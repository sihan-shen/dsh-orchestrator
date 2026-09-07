import { MAX_HANDOFF_ITEMS, MAX_HANDOFF_STRING_BYTES } from './config.js'
import type { HandoffV1, VerificationEvidenceV1 } from './types.js'

type JsonRecord = Record<string, unknown>

const textEncoder = new TextEncoder()

function fail(path: string, message: string): never {
  throw new TypeError(`${path} ${message}`)
}

function assertJsonValue(value: unknown, path: string, seen = new WeakSet<object>()): void {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return
    fail(path, 'must be a JSON value')
  }
  if (typeof value !== 'object') fail(path, 'must be a JSON value')
  if (seen.has(value)) fail(path, 'must be a JSON value')
  if (!Array.isArray(value) && Object.getPrototypeOf(value) !== Object.prototype && Object.getPrototypeOf(value) !== null) {
    fail(path, 'must be a JSON value')
  }
  seen.add(value)
  if (Array.isArray(value)) {
    for (const [index, item] of value.entries()) assertJsonValue(item, `${path}[${index}]`, seen)
  } else {
    for (const [key, item] of Object.entries(value)) assertJsonValue(item, `${path}.${key}`, seen)
  }
  seen.delete(value)
}

function record(value: unknown, path: string): JsonRecord {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) fail(path, 'must be an object')
  return value as JsonRecord
}

function onlyKeys(value: JsonRecord, path: string, keys: readonly string[]): void {
  for (const key of Object.keys(value)) {
    if (!keys.includes(key)) fail(`${path}.${key}`, 'is not supported')
  }
}

function boundedString(value: unknown, path: string): string {
  if (typeof value !== 'string') fail(path, 'must be a string')
  if (value.includes('\0')) fail(path, 'must not contain a NUL byte')
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

function normalizedWorkspaceRoot(workspaceRoot: string): string {
  if (workspaceRoot.length === 0 || workspaceRoot.includes('\0')) {
    throw new TypeError('workspaceRoot must be a non-empty path without NUL bytes')
  }
  return workspaceRoot.replaceAll('\\', '/')
}

function normalizedRelativePath(value: unknown, path: string, workspaceRoot: string): string {
  const source = boundedString(value, path).replaceAll('\\', '/')
  if (source.startsWith('/') || /^[A-Za-z]:/u.test(source)) fail(path, 'must be repository-relative')

  const segments: string[] = []
  for (const segment of source.split('/')) {
    if (segment === '' || segment === '.') continue
    if (segment === '..') fail(path, 'must not traverse outside the workspace')
    segments.push(segment)
  }
  if (segments.length === 0) fail(path, 'must name a file below the workspace root')

  const normalized = segments.join('/')
  const rootWithSeparator = workspaceRoot.endsWith('/') ? workspaceRoot : `${workspaceRoot}/`
  if (!`${rootWithSeparator}${normalized}`.startsWith(rootWithSeparator)) {
    fail(path, 'must resolve below the workspace root')
  }
  return normalized
}

function stringArray(value: unknown, path: string): readonly string[] {
  return boundedArray(value, path).map((item, index) => boundedString(item, `${path}[${index}]`))
}

function changedFiles(value: unknown, workspaceRoot: string): readonly string[] {
  return boundedArray(value, 'changedFiles').map((item, index) =>
    normalizedRelativePath(item, `changedFiles[${index}]`, workspaceRoot),
  )
}

function verificationEvidence(value: unknown, index: number): VerificationEvidenceV1 {
  const path = `verification[${index}]`
  const evidence = record(value, path)
  onlyKeys(evidence, path, [
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
  const commandName = boundedString(evidence.commandName, `${path}.commandName`)
  const args = stringArray(evidence.args, `${path}.args`)
  const exitCode = evidence.exitCode
  if (exitCode !== null && (!Number.isInteger(exitCode) || typeof exitCode !== 'number' || exitCode < 0)) {
    fail(`${path}.exitCode`, 'must be a non-negative integer or null')
  }
  const status = evidence.status
  if (status !== 'passed' && status !== 'failed' && status !== 'timed-out' && status !== 'spawn-error') {
    fail(`${path}.status`, 'must be a supported verification status')
  }
  const stdout = boundedString(evidence.stdout, `${path}.stdout`)
  const stderr = boundedString(evidence.stderr, `${path}.stderr`)
  if (typeof evidence.truncated !== 'boolean') fail(`${path}.truncated`, 'must be a boolean')
  const durationMs = evidence.durationMs
  if (!Number.isInteger(durationMs) || typeof durationMs !== 'number' || durationMs < 0) {
    fail(`${path}.durationMs`, 'must be a non-negative integer')
  }
  return { commandName, args, durationMs, exitCode, schemaVersion: 1, status, stderr, stdout, truncated: evidence.truncated }
}

/**
 * Build a Handoff from fields that have already passed the Handoff boundary.
 * The result stays mutable for legacy callers; parallel callers deep-freeze
 * their detached projection after supplying authoritative file paths.
 */
export function createHandoff(
  status: HandoffV1['status'],
  summary: string,
  changedFiles: readonly string[],
  decisions: readonly string[],
  verification: readonly VerificationEvidenceV1[],
  blockers: readonly string[],
): HandoffV1 {
  if (
    status === 'completed'
    && verification.some(evidence => evidence.status !== 'passed')
    && !summary.includes('[verification: failed]')
  ) {
    fail('summary', 'must include [verification: failed] when completed work has unsuccessful verification')
  }
  return {
    schemaVersion: 1,
    status,
    summary,
    changedFiles: [...changedFiles],
    decisions: [...decisions],
    verification: verification.map(evidence => ({
      schemaVersion: 1,
      commandName: evidence.commandName,
      args: [...evidence.args],
      exitCode: evidence.exitCode,
      status: evidence.status,
      stdout: evidence.stdout,
      stderr: evidence.stderr,
      truncated: evidence.truncated,
      durationMs: evidence.durationMs,
    })),
    blockers: [...blockers],
  }
}

function boundedFailureSummary(message: string): string {
  const safeMessage = message.replaceAll('\0', '\uFFFD')
  let result = ''
  let bytes = 0
  for (const character of safeMessage) {
    const characterBytes = textEncoder.encode(character).byteLength
    if (bytes + characterBytes > MAX_HANDOFF_STRING_BYTES) break
    result += character
    bytes += characterBytes
  }
  return result
}

/**
 * Parse and normalize a model-produced v1 handoff for parent-session use.
 * @param value - Untrusted JSON-like worker output.
 * @param workspaceRoot - Repository root used to constrain changed-file paths.
 * @returns A validated handoff with slash-normalized repository-relative paths.
 * @throws {TypeError} When output is not a valid bounded HandoffV1 record.
 */
export function parseHandoff(value: unknown, workspaceRoot: string): HandoffV1 {
  const root = normalizedWorkspaceRoot(workspaceRoot)
  assertJsonValue(value, 'handoff')
  const handoff = record(value, 'handoff')
  onlyKeys(handoff, 'handoff', [
    'schemaVersion',
    'status',
    'summary',
    'changedFiles',
    'decisions',
    'verification',
    'blockers',
  ])
  if (handoff.schemaVersion !== 1) fail('schemaVersion', 'must be 1')
  const status = handoff.status
  if (status !== 'completed' && status !== 'blocked' && status !== 'failed') {
    fail('status', 'must be "completed", "blocked", or "failed"')
  }
  const summary = boundedString(handoff.summary, 'summary')
  const verification = boundedArray(handoff.verification, 'verification').map(verificationEvidence)
  if (
    status === 'completed'
    && verification.some(evidence => evidence.status !== 'passed')
    && !summary.includes('[verification: failed]')
  ) {
    fail('summary', 'must include [verification: failed] when completed work has unsuccessful verification')
  }
  return createHandoff(
    status,
    summary,
    changedFiles(handoff.changedFiles, root),
    stringArray(handoff.decisions, 'decisions'),
    verification,
    stringArray(handoff.blockers, 'blockers'),
  )
}

/**
 * Construct the bounded fallback record used when worker output cannot be trusted.
 * @param message - Safe validation summary for the parent session.
 * @returns A failed handoff without transcript or tool-output fields.
 */
export function failedHandoff(message: string): HandoffV1 {
  return {
    schemaVersion: 1,
    status: 'failed',
    summary: boundedFailureSummary(message),
    changedFiles: [],
    decisions: [],
    verification: [],
    blockers: [],
  }
}

/**
 * Convert arbitrary worker output into the only result a parent session may consume.
 * @param value - Raw worker output, possibly invalid or non-JSON.
 * @param workspaceRoot - Repository root used to constrain changed-file paths.
 * @returns The parsed handoff or a fixed failed handoff without copied raw output.
 */
export function normalizeWorkerOutput(value: unknown, workspaceRoot: string): HandoffV1 {
  try {
    return parseHandoff(value, workspaceRoot)
  } catch {
    return failedHandoff('Worker output failed HandoffV1 validation.')
  }
}
