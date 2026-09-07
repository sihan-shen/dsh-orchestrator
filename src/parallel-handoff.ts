import {
  MAX_AGGREGATE_PATH_BYTES,
  MAX_DAG_CHANGED_FILES,
  isWellFormedUnicode,
  parseRepoFilePath,
  repoPathContains,
  utf8ByteLength,
} from '@ds-plugins/dsh-scheduling-contracts'
import { sha256Utf8 as contextSha256Utf8 } from '@ds-plugins/dsh-context'
import type {
  OwnershipViolationSummaryV1,
  RepoFilePath,
  RepoPathDeclaration,
} from '@ds-plugins/dsh-scheduling-contracts'
import { MAX_HANDOFF_ITEMS, MAX_HANDOFF_STRING_BYTES } from './config.js'
import { createHandoff } from './handoff.js'
import type { HandoffV1, VerificationEvidenceV1 } from './types.js'

type JsonRecord = Record<string, unknown>

type OwnershipInvalidityV1 =
  | 'ill-formed-unicode'
  | 'over-byte-limit'
  | 'absolute'
  | 'backslash'
  | 'dot-segment'
  | 'empty-segment'
  | 'glob'
  | 'control'
  | 'other-grammar'

const GLOB_PATTERN = /[*?\[\]{}]/u
const CONTROL_PATTERN = /[\u0000-\u001F\u007F-\u009F]/u
const ABSOLUTE_DRIVE_PATTERN = /^[A-Za-z]:[/\\]/u
const ARRAY_INDEX_PATTERN = /^(?:0|[1-9]\d*)$/u
const textEncoder = new TextEncoder()

/** A canonical token committed by one ownership check. */
export type OwnershipViolationTokenV1 =
  | { readonly kind: 'path'; readonly path: string }
  | { readonly kind: 'invalid'; readonly invalidity: OwnershipInvalidityV1; readonly rawDigest: string }

/** A raw, bounded Handoff envelope retained before path ownership projection. */
export interface RawParallelHandoffEnvelopeV1 {
  readonly schemaVersion: 1
  readonly status: HandoffV1['status']
  readonly summary: string
  readonly changedFiles: readonly string[]
  readonly decisions: readonly string[]
  readonly verification: readonly VerificationEvidenceV1[]
  readonly blockers: readonly string[]
}

/** Result of declaration-layer ownership classification for one worker Handoff. */
export type OwnershipCheckV1 =
  | { readonly valid: true; readonly changedFiles: readonly RepoFilePath[] }
  | { readonly valid: false; readonly summary: OwnershipViolationSummaryV1; readonly tokens: readonly OwnershipViolationTokenV1[] }

function fail(path: string, message: string): never {
  throw new TypeError(`${path} ${message}`)
}

function isPlainRecord(value: object): boolean {
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

/**
 * Snapshot a JSON-like input without retaining caller-owned objects or
 * invoking accessors. Lone surrogates remain strings so ownership can digest
 * their exact JSON.stringify preimage later.
 */
function snapshotJsonValue(value: unknown, path: string, seen = new WeakSet<object>()): unknown {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return value
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return value
    fail(path, 'must be a JSON value')
  }
  if (typeof value !== 'object') fail(path, 'must be a JSON value')
  if (seen.has(value)) fail(path, 'must be a JSON value')
  if (!Array.isArray(value) && !isPlainRecord(value)) fail(path, 'must be a JSON value')

  seen.add(value)
  try {
    const descriptors = Object.getOwnPropertyDescriptors(value)
    if (Array.isArray(value)) {
      const length = descriptors.length?.value
      if (typeof length !== 'number' || !Number.isSafeInteger(length) || length < 0) {
        fail(`${path}.length`, 'must be a valid array length')
      }
      for (const key of Reflect.ownKeys(descriptors)) {
        if (key !== 'length' && (typeof key !== 'string' || !/^(?:0|[1-9]\d*)$/u.test(key))) {
          fail(path, 'must be a JSON value')
        }
        if (key !== 'length' && Number(key) >= length) fail(path, 'must be a JSON value')
      }
      const snapshot: unknown[] = []
      for (let index = 0; index < length; index += 1) {
        const descriptor = descriptors[String(index)]
        if (descriptor === undefined || !Object.prototype.hasOwnProperty.call(descriptor, 'value')) {
          fail(`${path}[${index}]`, 'must be a JSON value')
        }
        snapshot.push(snapshotJsonValue(descriptor.value, `${path}[${index}]`, seen))
      }
      return snapshot
    }

    const snapshot = Object.create(null) as JsonRecord
    for (const key of Reflect.ownKeys(descriptors)) {
      if (typeof key !== 'string') fail(path, 'must be a JSON value')
      const descriptor = descriptors[key]!
      if (!descriptor.enumerable) fail(`${path}.${key}`, 'must be JSON-serialized')
      if (!Object.prototype.hasOwnProperty.call(descriptor, 'value')) {
        fail(`${path}.${key}`, 'must not be an accessor')
      }
      snapshot[key] = snapshotJsonValue(descriptor.value, `${path}.${key}`, seen)
    }
    return snapshot
  } finally {
    seen.delete(value)
  }
}

function deepFreeze<T>(value: T): T {
  if (typeof value !== 'object' || value === null || Object.isFrozen(value)) return value
  for (const child of Object.values(value as JsonRecord)) deepFreeze(child)
  return Object.freeze(value)
}

function exactRecord(value: unknown, path: string, allowed: readonly string[]): JsonRecord {
  if (typeof value !== 'object' || value === null || Array.isArray(value) || !isPlainRecord(value)) {
    fail(path, 'must be a plain object')
  }
  const record = value as JsonRecord
  const allowedSet = new Set(allowed)
  for (const key of Reflect.ownKeys(record)) {
    if (typeof key !== 'string' || !allowedSet.has(key)) fail(`${path}.${String(key)}`, 'is not supported')
  }
  return record
}

function dataProperty(record: JsonRecord, name: string, path: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(record, name)
  if (descriptor === undefined) fail(`${path}.${name}`, 'is required')
  if (!Object.prototype.hasOwnProperty.call(descriptor, 'value')) {
    fail(`${path}.${name}`, 'must not be an accessor')
  }
  return descriptor.value
}

function preflightArray(
  value: unknown,
  path: string,
  item: (value: unknown, index: number) => void,
): void {
  if (!Array.isArray(value)) fail(path, 'must be an array')
  const lengthDescriptor = Object.getOwnPropertyDescriptor(value, 'length')
  const length = lengthDescriptor?.value
  if (
    lengthDescriptor === undefined
    || !Object.prototype.hasOwnProperty.call(lengthDescriptor, 'value')
    || typeof length !== 'number'
    || !Number.isSafeInteger(length)
    || length < 0
  ) {
    fail(`${path}.length`, 'must be a valid array length')
  }
  if (length > MAX_HANDOFF_ITEMS) fail(path, `must not contain more than ${MAX_HANDOFF_ITEMS} items`)

  for (const key of Reflect.ownKeys(value)) {
    if (key !== 'length' && (typeof key !== 'string' || !ARRAY_INDEX_PATTERN.test(key))) {
      fail(path, 'must be a JSON value')
    }
    if (key !== 'length' && Number(key) >= length) fail(path, 'must be a JSON value')
  }

  for (let index = 0; index < length; index += 1) {
    const descriptor = Object.getOwnPropertyDescriptor(value, String(index))
    if (descriptor === undefined || !Object.prototype.hasOwnProperty.call(descriptor, 'value')) {
      fail(`${path}[${index}]`, 'must be a JSON value')
    }
    item(descriptor.value, index)
  }
}

function preflightStringArray(value: unknown, path: string): void {
  preflightArray(value, path, (item, index) => boundedString(item, `${path}[${index}]`))
}

function preflightRawChangedFiles(value: unknown, path: string): void {
  preflightArray(value, path, (item, index) => rawChangedFile(item, `${path}[${index}]`))
}

function preflightVerificationEvidence(value: unknown, index: number): VerificationEvidenceV1['status'] {
  const path = `verification[${index}]`
  const evidence = exactRecord(value, path, [
    'schemaVersion',
    'commandName',
    'args',
    'exitCode',
    'status',
    'stdout',
    'stderr',
    'truncated',
    'durationMs',
  ])
  if (dataProperty(evidence, 'schemaVersion', path) !== 1) fail(`${path}.schemaVersion`, 'must be 1')

  const exitCode = dataProperty(evidence, 'exitCode', path)
  if (exitCode !== null && (typeof exitCode !== 'number' || !Number.isInteger(exitCode) || exitCode < 0)) {
    fail(`${path}.exitCode`, 'must be a non-negative integer or null')
  }
  const status = dataProperty(evidence, 'status', path)
  if (status !== 'passed' && status !== 'failed' && status !== 'timed-out' && status !== 'spawn-error') {
    fail(`${path}.status`, 'must be a supported verification status')
  }
  if (typeof dataProperty(evidence, 'truncated', path) !== 'boolean') {
    fail(`${path}.truncated`, 'must be a boolean')
  }
  const durationMs = dataProperty(evidence, 'durationMs', path)
  if (typeof durationMs !== 'number' || !Number.isInteger(durationMs) || durationMs < 0) {
    fail(`${path}.durationMs`, 'must be a non-negative integer')
  }

  boundedString(dataProperty(evidence, 'commandName', path), `${path}.commandName`)
  preflightStringArray(dataProperty(evidence, 'args', path), `${path}.args`)
  boundedString(dataProperty(evidence, 'stdout', path), `${path}.stdout`)
  boundedString(dataProperty(evidence, 'stderr', path), `${path}.stderr`)
  return status
}

function preflightRawHandoff(handoff: JsonRecord): void {
  if (dataProperty(handoff, 'schemaVersion', 'handoff') !== 1) fail('schemaVersion', 'must be 1')
  const status = dataProperty(handoff, 'status', 'handoff')
  if (status !== 'completed' && status !== 'blocked' && status !== 'failed') {
    fail('status', 'must be "completed", "blocked", or "failed"')
  }
  const summary = boundedString(dataProperty(handoff, 'summary', 'handoff'), 'summary')
  preflightRawChangedFiles(dataProperty(handoff, 'changedFiles', 'handoff'), 'changedFiles')
  preflightStringArray(dataProperty(handoff, 'decisions', 'handoff'), 'decisions')

  const verificationStatuses: VerificationEvidenceV1['status'][] = []
  preflightArray(dataProperty(handoff, 'verification', 'handoff'), 'verification', (item, index) => {
    verificationStatuses.push(preflightVerificationEvidence(item, index))
  })
  preflightStringArray(dataProperty(handoff, 'blockers', 'handoff'), 'blockers')
  if (
    status === 'completed'
    && verificationStatuses.some(verificationStatus => verificationStatus !== 'passed')
    && !summary.includes('[verification: failed]')
  ) {
    fail('summary', 'must include [verification: failed] when completed work has unsuccessful verification')
  }
}

function required(record: JsonRecord, name: string, path: string): unknown {
  if (!Object.prototype.hasOwnProperty.call(record, name)) fail(`${path}.${name}`, 'is required')
  return record[name]
}

function boundedString(value: unknown, path: string): string {
  if (typeof value !== 'string') fail(path, 'must be a string')
  if (value.includes('\0')) fail(path, 'must not contain a NUL byte')
  if (textEncoder.encode(value).byteLength > MAX_HANDOFF_STRING_BYTES) {
    fail(path, `must not exceed ${MAX_HANDOFF_STRING_BYTES} UTF-8 bytes`)
  }
  return value
}

function rawChangedFile(value: unknown, path: string): string {
  if (typeof value !== 'string') fail(path, 'must be a string')
  if (textEncoder.encode(value).byteLength > MAX_HANDOFF_STRING_BYTES) {
    fail(path, `must not exceed ${MAX_HANDOFF_STRING_BYTES} UTF-8 bytes`)
  }
  return value
}

function boundedArray(value: unknown, path: string): readonly unknown[] {
  if (!Array.isArray(value)) fail(path, 'must be an array')
  if (value.length > MAX_HANDOFF_ITEMS) fail(path, `must not contain more than ${MAX_HANDOFF_ITEMS} items`)
  return value
}

function stringArray(value: unknown, path: string): readonly string[] {
  return boundedArray(value, path).map((item, index) => boundedString(item, `${path}[${index}]`))
}

function verificationEvidence(value: unknown, index: number): VerificationEvidenceV1 {
  const path = `verification[${index}]`
  const evidence = exactRecord(value, path, [
    'schemaVersion',
    'commandName',
    'args',
    'exitCode',
    'status',
    'stdout',
    'stderr',
    'truncated',
    'durationMs',
  ])
  if (evidence.schemaVersion !== 1) fail(`${path}.schemaVersion`, 'must be 1')
  const exitCode = evidence.exitCode
  if (exitCode !== null && (typeof exitCode !== 'number' || !Number.isInteger(exitCode) || exitCode < 0)) {
    fail(`${path}.exitCode`, 'must be a non-negative integer or null')
  }
  const status = evidence.status
  if (status !== 'passed' && status !== 'failed' && status !== 'timed-out' && status !== 'spawn-error') {
    fail(`${path}.status`, 'must be a supported verification status')
  }
  if (typeof evidence.truncated !== 'boolean') fail(`${path}.truncated`, 'must be a boolean')
  const durationMs = evidence.durationMs
  if (typeof durationMs !== 'number' || !Number.isInteger(durationMs) || durationMs < 0) {
    fail(`${path}.durationMs`, 'must be a non-negative integer')
  }
  return {
    schemaVersion: 1,
    commandName: boundedString(required(evidence, 'commandName', path), `${path}.commandName`),
    args: stringArray(required(evidence, 'args', path), `${path}.args`),
    exitCode: exitCode as number | null,
    status,
    stdout: boundedString(required(evidence, 'stdout', path), `${path}.stdout`),
    stderr: boundedString(required(evidence, 'stderr', path), `${path}.stderr`),
    truncated: evidence.truncated,
    durationMs,
  }
}

/**
 * Parse a parallel worker Handoff while retaining every changed-file string
 * exactly as reported. This is intentionally separate from parseHandoff:
 * ownership classification must see the raw path before any normalization.
 */
export function parseParallelHandoffEnvelopeRaw(value: unknown): RawParallelHandoffEnvelopeV1 {
  const handoff = exactRecord(value, 'handoff', [
    'schemaVersion',
    'status',
    'summary',
    'changedFiles',
    'decisions',
    'verification',
    'blockers',
  ])
  preflightRawHandoff(handoff)

  const snapshot = snapshotJsonValue(value, 'handoff')
  const snapshotHandoff = exactRecord(snapshot, 'handoff', [
    'schemaVersion',
    'status',
    'summary',
    'changedFiles',
    'decisions',
    'verification',
    'blockers',
  ])
  if (snapshotHandoff.schemaVersion !== 1) fail('schemaVersion', 'must be 1')
  const status = snapshotHandoff.status
  if (status !== 'completed' && status !== 'blocked' && status !== 'failed') {
    fail('status', 'must be "completed", "blocked", or "failed"')
  }
  const summary = boundedString(required(snapshotHandoff, 'summary', 'handoff'), 'summary')
  const rawChangedFiles = boundedArray(required(snapshotHandoff, 'changedFiles', 'handoff'), 'changedFiles')
    .map((item, index) => rawChangedFile(item, `changedFiles[${index}]`))
  const decisions = stringArray(required(snapshotHandoff, 'decisions', 'handoff'), 'decisions')
  const verification = boundedArray(required(snapshotHandoff, 'verification', 'handoff'), 'verification')
    .map((item, index) => verificationEvidence(item, index))
  const blockers = stringArray(required(snapshotHandoff, 'blockers', 'handoff'), 'blockers')
  if (
    status === 'completed'
    && verification.some(evidence => evidence.status !== 'passed')
    && !summary.includes('[verification: failed]')
  ) {
    fail('summary', 'must include [verification: failed] when completed work has unsuccessful verification')
  }
  return deepFreeze({
    schemaVersion: 1,
    status,
    summary,
    changedFiles: rawChangedFiles,
    decisions,
    verification,
    blockers,
  })
}

function sha256Utf8(value: string): string {
  return contextSha256Utf8(value).slice('sha256:'.length)
}

function invalidToken(raw: string, invalidity: OwnershipInvalidityV1): OwnershipViolationTokenV1 {
  return deepFreeze({
    kind: 'invalid',
    invalidity,
    rawDigest: sha256Utf8(JSON.stringify(raw)),
  })
}

function pathSegmentsForClassification(raw: string): readonly string[] {
  return (raw.endsWith('/') ? raw.slice(0, -1) : raw).split('/')
}

function invalidityFor(raw: string): OwnershipInvalidityV1 | undefined {
  if (!isWellFormedUnicode(raw)) return 'ill-formed-unicode'
  if (utf8ByteLength(raw) > MAX_AGGREGATE_PATH_BYTES) return 'over-byte-limit'
  if (raw.startsWith('/') || ABSOLUTE_DRIVE_PATTERN.test(raw) || raw.startsWith('\\\\')) return 'absolute'
  if (raw.includes('\\')) return 'backslash'

  const segments = pathSegmentsForClassification(raw)
  if (segments.some(segment => segment === '.' || segment === '..')) return 'dot-segment'
  if (raw.length === 0 || segments.some(segment => segment.length === 0)) return 'empty-segment'
  if (GLOB_PATTERN.test(raw)) return 'glob'
  if (CONTROL_PATTERN.test(raw)) return 'control'

  try {
    parseRepoFilePath(raw)
    return undefined
  } catch {
    return 'other-grammar'
  }
}

/**
 * Classify one raw changed-file declaration. Undefined means a valid file
 * path contained by at least one authoritative write declaration.
 */
export function classifyOwnershipToken(
  raw: string,
  writePaths: readonly RepoPathDeclaration[],
): OwnershipViolationTokenV1 | undefined {
  const invalidity = invalidityFor(raw)
  if (invalidity !== undefined) return invalidToken(raw, invalidity)

  let path: RepoFilePath
  try {
    path = parseRepoFilePath(raw)
  } catch {
    return invalidToken(raw, 'other-grammar')
  }
  const owned = writePaths.some(owner => {
    try {
      return repoPathContains(owner, path)
    } catch {
      return false
    }
  })
  if (owned) return undefined
  return deepFreeze({ kind: 'path', path })
}

function tokenSortKey(token: OwnershipViolationTokenV1): string {
  return JSON.stringify(token)
}

/** Check raw changed files against declaration-layer ownership. */
export function checkOwnership(
  nodeId: string,
  changedFiles: readonly string[],
  writePaths: readonly RepoPathDeclaration[],
): OwnershipCheckV1 {
  const classified = changedFiles
    .map(raw => classifyOwnershipToken(raw, writePaths))
    .filter((token): token is OwnershipViolationTokenV1 => token !== undefined)
  const unique = [...new Map(classified.map(token => [tokenSortKey(token), token])).entries()]
    .sort(([left], [right]) => left < right ? -1 : left > right ? 1 : 0)
    .map(([, token]) => token)

  if (unique.length === 0) {
    const ownedFiles = changedFiles.map((raw, index) => parseRepoFilePath(raw, `changedFiles[${index}]`))
    return deepFreeze({ valid: true, changedFiles: ownedFiles })
  }

  const digest = sha256Utf8(JSON.stringify(unique))
  const samplePaths = unique
    .flatMap(token => token.kind === 'path' ? [token.path] : [])
    .sort()
    .slice(0, Math.min(MAX_DAG_CHANGED_FILES, unique.length))
  return deepFreeze({
    valid: false,
    tokens: unique,
    summary: {
      nodeId,
      count: unique.length,
      digest,
      samplePaths,
    },
  })
}

/** Construct a detached, frozen ordinary Handoff after ownership succeeds. */
export function constructParallelHandoff(
  envelope: RawParallelHandoffEnvelopeV1,
  changedFiles: readonly RepoFilePath[],
): HandoffV1 {
  const raw = parseParallelHandoffEnvelopeRaw(envelope)
  const authoritativeFiles = boundedArray(changedFiles, 'changedFiles')
    .map((file, index) => parseRepoFilePath(file, `changedFiles[${index}]`))
  return deepFreeze(createHandoff(
    raw.status,
    raw.summary,
    authoritativeFiles,
    raw.decisions,
    raw.verification,
    raw.blockers,
  ))
}
