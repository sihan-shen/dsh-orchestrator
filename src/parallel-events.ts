import type { Session, SessionId } from '@deepseek-ai/dsh-session'
import {
  MAX_DAG_NODES,
  MAX_PARALLEL_STARTED_PAYLOAD_BYTES,
  MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES,
  MAX_PARALLEL_WORKER_REQUESTED_PAYLOAD_BYTES,
  MAX_PARALLEL_WORKER_REF_BYTES,
  MAX_PARALLEL_WORKER_ROUTE_FIELD_BYTES,
  MAX_PARALLEL_WORKER_TASK_BYTES,
  MAX_PARALLEL_WORKER_TOOL_BYTES,
  MAX_PARALLEL_WORKER_TOOL_COUNT,
  MAX_SCHEDULING_IDENTIFIER_BYTES,
  MAX_SCHEDULING_ITEMS,
  MAX_SCHEDULING_STRING_BYTES,
  assertSerializedPayloadLimit,
  isWellFormedUnicode,
  parseNodeId,
  parseParallelAggregateV1,
  parseScheduleFeedbackV1,
  serializedPayloadBytes,
  utf8ByteLength,
} from '@ds-plugins/dsh-scheduling-contracts'
import type {
  CorrelationTripleV1 as ContractCorrelationTripleV1,
  ExpectedEventBranch as ContractExpectedEventBranch,
  ParallelAggregateV1,
} from '@ds-plugins/dsh-scheduling-contracts'
import type { HandoffV1, WorkerSpecV1 } from './types.js'

/** Correlation fields shared by all parallel worker records. */
export interface CorrelationTripleV1 extends ContractCorrelationTripleV1 {}

/** Context supplied by replay or an append caller to discriminate event branches. */
export type ExpectedEventBranch = ContractExpectedEventBranch

export type LegacyWorkerRequestedV1 = WorkerSpecV1

export interface ParallelWorkerRequestedV1 extends WorkerSpecV1, CorrelationTripleV1 {}

export interface LegacyWorkerFinishedV1 {
  readonly schemaVersion: 1
  readonly childSessionId: SessionId
  readonly handoff: HandoffV1
}

export interface ParallelWorkerFinishedV1 extends CorrelationTripleV1 {
  readonly schemaVersion: 1
  readonly workerRef: string
  readonly handoff: HandoffV1
}

export interface PlannedParallelRequestV1 extends CorrelationTripleV1 {}

export interface ParallelStartedV1 {
  readonly schemaVersion: 1
  readonly dagId: string
  readonly requests: readonly PlannedParallelRequestV1[]
}

export type WorkerRequestedV1 = LegacyWorkerRequestedV1 | ParallelWorkerRequestedV1
export type WorkerFinishedEventV1 = LegacyWorkerFinishedV1 | ParallelWorkerFinishedV1

type RecordValue = Record<string, unknown>

const WORKER_REF_PATTERN = /^w:[0-9a-f]{32}$/u
const MAX_PARALLEL_FANOUT_ID_BYTES = 67
const MAX_PARALLEL_REQUEST_ID_BYTES = 94
const CORRELATION_KEYS = ['fanoutId', 'nodeId', 'requestId'] as const
const WORKER_SPEC_KEYS = [
  'schemaVersion',
  'task',
  'provider',
  'model',
  'reasoningEffort',
  'maxTokens',
  'allowedTools',
  'expectedOutput',
] as const
const WORKER_REQUESTED_KEYS = [...WORKER_SPEC_KEYS, ...CORRELATION_KEYS] as const
const WORKER_FINISHED_KEYS = [
  'schemaVersion',
  'childSessionId',
  'workerRef',
  'handoff',
  ...CORRELATION_KEYS,
] as const
const PARALLEL_STARTED_KEYS = ['schemaVersion', 'dagId', 'requests'] as const
const PLANNED_REQUEST_KEYS = [...CORRELATION_KEYS] as const
const textEncoder = new TextEncoder()

function fail(path: string, message: string): never {
  throw new TypeError(`${path} ${message}`)
}

function isPlainRecord(value: object): boolean {
  const prototype = Object.getPrototypeOf(value)
  return prototype === Object.prototype || prototype === null
}

function assertJsonValue(value: unknown, path: string, seen = new WeakSet<object>()): void {
  if (value === null || typeof value === 'string' || typeof value === 'boolean') return
  if (typeof value === 'number') {
    if (Number.isFinite(value)) return
    fail(path, 'must be a JSON value')
  }
  if (typeof value !== 'object') fail(path, 'must be a JSON value')
  if (seen.has(value)) fail(path, 'must be a JSON value')
  if (!Array.isArray(value) && !isPlainRecord(value)) fail(path, 'must be a JSON value')

  seen.add(value)
  if (Array.isArray(value)) {
    for (const key of Reflect.ownKeys(value)) {
      if (key !== 'length' && (typeof key !== 'string' || !/^(?:0|[1-9]\d*)$/u.test(key))) {
        fail(path, 'must be a JSON value')
      }
    }
    for (const [index, item] of value.entries()) assertJsonValue(item, `${path}[${index}]`, seen)
  } else {
    for (const key of Reflect.ownKeys(value)) {
      if (typeof key !== 'string') fail(path, 'must be a JSON value')
      assertJsonValue((value as RecordValue)[key], `${path}.${key}`, seen)
    }
  }
  seen.delete(value)
}

function deepFreeze<T>(value: T): T {
  if (typeof value !== 'object' || value === null || Object.isFrozen(value)) return value
  for (const child of Object.values(value as RecordValue)) deepFreeze(child)
  return Object.freeze(value)
}

function exactRecord(value: unknown, path: string, allowed: readonly string[]): RecordValue {
  if (typeof value !== 'object' || value === null || Array.isArray(value) || !isPlainRecord(value)) {
    fail(path, 'must be a plain object')
  }
  const record = value as RecordValue
  const allowedSet = new Set(allowed)
  for (const key of Reflect.ownKeys(record)) {
    if (typeof key !== 'string' || !allowedSet.has(key)) fail(`${path}.${String(key)}`, 'is not allowed')
  }
  return record
}

function required(record: RecordValue, name: string, path: string): unknown {
  if (!Object.prototype.hasOwnProperty.call(record, name)) fail(`${path}.${name}`, 'is required')
  return record[name]
}

function boundedText(value: unknown, path: string, maximum = MAX_SCHEDULING_STRING_BYTES): string {
  if (typeof value !== 'string' || value.trim() === '') fail(path, 'must be a non-empty string')
  if (value.includes('\0')) fail(path, 'must not contain a NUL byte')
  if (textEncoder.encode(value).byteLength > maximum) fail(path, `must not exceed ${maximum} UTF-8 bytes`)
  return value
}

function boundedIdentifier(value: unknown, path: string): string {
  if (typeof value !== 'string' || value.trim() === '') fail(path, 'must be a non-empty identifier')
  if (value.includes('\0')) fail(path, 'must not contain a NUL byte')
  if (textEncoder.encode(value).byteLength > MAX_SCHEDULING_IDENTIFIER_BYTES) {
    fail(path, `must not exceed ${MAX_SCHEDULING_IDENTIFIER_BYTES} UTF-8 bytes`)
  }
  return value
}

function boundedDerivedIdentifier(value: unknown, path: string, maximum: number): string {
  const parsed = boundedIdentifier(value, path)
  if (!isWellFormedUnicode(parsed)) fail(path, 'must contain well-formed Unicode')
  if (textEncoder.encode(parsed).byteLength > maximum) fail(path, `must not exceed ${maximum} UTF-8 bytes`)
  return parsed
}

function boundedInteger(value: unknown, path: string, minimum: number, maximum: number): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < minimum || value > maximum) {
    fail(path, `must be an integer between ${minimum} and ${maximum}`)
  }
  return value
}

function parseCorrelation(
  input: RecordValue,
  path: string,
  expectedBranch: ExpectedEventBranch | undefined,
): CorrelationTripleV1 | undefined {
  if (expectedBranch !== undefined && expectedBranch !== 'legacy' && expectedBranch !== 'parallel') {
    fail(`${path}.expectedBranch`, 'is unsupported')
  }

  const present = CORRELATION_KEYS.filter(key => Object.prototype.hasOwnProperty.call(input, key))
  if (present.length !== 0 && present.length !== CORRELATION_KEYS.length) {
    fail(path, 'must contain the complete correlation triple or none of it')
  }
  const correlated = present.length === CORRELATION_KEYS.length
  if (expectedBranch === 'legacy' && correlated) fail(path, 'legacy branch must not contain a correlation triple')
  if (expectedBranch === 'parallel' && !correlated) fail(path, 'parallel branch requires a correlation triple')
  if (!correlated) return undefined

  return {
    fanoutId: boundedDerivedIdentifier(input.fanoutId, `${path}.fanoutId`, MAX_PARALLEL_FANOUT_ID_BYTES),
    nodeId: parseNodeId(input.nodeId, `${path}.nodeId`),
    requestId: boundedDerivedIdentifier(input.requestId, `${path}.requestId`, MAX_PARALLEL_REQUEST_ID_BYTES),
  }
}

function parseWorkerSpec(value: unknown, expectedBranch: ExpectedEventBranch | undefined): WorkerSpecV1 & Partial<CorrelationTripleV1> {
  assertJsonValue(value, 'workerRequested')
  const input = exactRecord(value, 'workerRequested', WORKER_REQUESTED_KEYS)
  if (input.schemaVersion !== 1) fail('workerRequested.schemaVersion', 'must be 1')
  if (input.expectedOutput !== 'handoff-v1') fail('workerRequested.expectedOutput', 'must be "handoff-v1"')

  const parallel = expectedBranch === 'parallel' || CORRELATION_KEYS.some(key => Object.prototype.hasOwnProperty.call(input, key))
  const taskMaximum = parallel ? MAX_PARALLEL_WORKER_TASK_BYTES : MAX_SCHEDULING_STRING_BYTES
  const routeMaximum = parallel ? MAX_PARALLEL_WORKER_ROUTE_FIELD_BYTES : MAX_SCHEDULING_IDENTIFIER_BYTES
  const toolMaximum = parallel ? MAX_PARALLEL_WORKER_TOOL_BYTES : MAX_SCHEDULING_STRING_BYTES
  const toolCount = parallel ? MAX_PARALLEL_WORKER_TOOL_COUNT : MAX_SCHEDULING_ITEMS
  const allowedToolsValue = required(input, 'allowedTools', 'workerRequested')
  if (!Array.isArray(allowedToolsValue)) fail('workerRequested.allowedTools', 'must be an array')
  if (allowedToolsValue.length > toolCount) fail('workerRequested.allowedTools', `must not contain more than ${toolCount} items`)
  const allowedTools = allowedToolsValue.map((tool, index) => boundedText(tool, `workerRequested.allowedTools[${index}]`, toolMaximum))
  if (new Set(allowedTools).size !== allowedTools.length) fail('workerRequested.allowedTools', 'must not contain duplicates')
  if (parallel && allowedTools.includes('targeted_verify')) fail('workerRequested.allowedTools', 'must not include targeted_verify')

  const correlation = parseCorrelation(input, 'workerRequested', expectedBranch)
  return deepFreeze({
    schemaVersion: 1,
    task: boundedText(required(input, 'task', 'workerRequested'), 'workerRequested.task', taskMaximum),
    provider: boundedText(required(input, 'provider', 'workerRequested'), 'workerRequested.provider', routeMaximum),
    model: boundedText(required(input, 'model', 'workerRequested'), 'workerRequested.model', routeMaximum),
    ...(Object.prototype.hasOwnProperty.call(input, 'reasoningEffort')
      ? { reasoningEffort: boundedText(input.reasoningEffort, 'workerRequested.reasoningEffort', routeMaximum) }
      : {}),
    maxTokens: boundedInteger(required(input, 'maxTokens', 'workerRequested'), 'workerRequested.maxTokens', 1, 128_000),
    allowedTools,
    expectedOutput: 'handoff-v1',
    ...(correlation === undefined ? {} : correlation),
  })
}

function parseHandoff(value: unknown): HandoffV1 {
  const feedback = parseScheduleFeedbackV1({
    schemaVersion: 1,
    requestId: 'worker-event-handoff',
    outcome: 'failed',
    handoff: value,
  })
  return feedback.handoff as HandoffV1
}

function parseWorkerRef(value: unknown, path: string): string {
  if (typeof value !== 'string' || utf8ByteLength(value) > MAX_PARALLEL_WORKER_REF_BYTES || !WORKER_REF_PATTERN.test(value)) {
    fail(path, 'must be w: followed by 32 lowercase hexadecimal characters')
  }
  return value
}

function parseWorkerFinishedRecord(value: unknown, expectedBranch: ExpectedEventBranch | undefined): WorkerFinishedEventV1 {
  assertJsonValue(value, 'workerFinished')
  const input = exactRecord(value, 'workerFinished', WORKER_FINISHED_KEYS)
  if (input.schemaVersion !== 1) fail('workerFinished.schemaVersion', 'must be 1')

  const correlation = parseCorrelation(input, 'workerFinished', expectedBranch)
  const hasChildSessionId = Object.prototype.hasOwnProperty.call(input, 'childSessionId')
  const hasWorkerRef = Object.prototype.hasOwnProperty.call(input, 'workerRef')
  if (hasChildSessionId && (hasWorkerRef || correlation !== undefined)) {
    fail('workerFinished', 'must not combine legacy childSessionId with parallel fields')
  }
  if (hasWorkerRef !== (correlation !== undefined)) {
    fail('workerFinished', 'parallel worker-finished requires workerRef and the complete correlation triple')
  }
  if (!hasChildSessionId && !hasWorkerRef) {
    fail('workerFinished', 'must contain either legacy childSessionId or parallel workerRef')
  }
  if (expectedBranch === 'legacy' && !hasChildSessionId) fail('workerFinished', 'legacy branch requires childSessionId')
  if (expectedBranch === 'parallel' && !hasWorkerRef) fail('workerFinished', 'parallel branch requires workerRef')

  const handoff = parseHandoff(required(input, 'handoff', 'workerFinished'))
  if (hasChildSessionId) {
    const childSessionId = required(input, 'childSessionId', 'workerFinished')
    if (typeof childSessionId !== 'string') fail('workerFinished.childSessionId', 'must be a string')
    return deepFreeze({
      schemaVersion: 1,
      childSessionId: childSessionId as SessionId,
      handoff,
    })
  }
  return deepFreeze({
    schemaVersion: 1,
    workerRef: parseWorkerRef(required(input, 'workerRef', 'workerFinished'), 'workerFinished.workerRef'),
    handoff,
    ...correlation!,
  })
}

function assertEventPayloadLimit(value: unknown, maximum: number, label: string): void {
  try {
    assertSerializedPayloadLimit(value, maximum, label)
  } catch {
    throw new TypeError(`${label} exceeds payload ceiling of ${maximum} bytes`)
  }
}

function assertMeasuredEventPayloadLimit(value: unknown, maximum: number, label: string, measure: typeof serializedPayloadBytes): void {
  let measured: number
  try {
    measured = measure(value)
  } catch {
    throw new TypeError(`${label} payload must be JSON-serializable`)
  }
  if (!Number.isSafeInteger(measured) || measured < 0) {
    throw new TypeError(`${label} payload measurement must be a non-negative safe integer`)
  }
  if (measured > maximum) throw new TypeError(`${label} exceeds payload ceiling of ${maximum} bytes`)
}

function parsePlannedRequest(value: unknown, index: number): PlannedParallelRequestV1 {
  const path = `parallelStarted.requests[${index}]`
  assertJsonValue(value, path)
  const input = exactRecord(value, path, PLANNED_REQUEST_KEYS)
  const correlation = parseCorrelation(input, path, 'parallel')!
  return deepFreeze({
    fanoutId: correlation.fanoutId,
    nodeId: correlation.nodeId,
    requestId: correlation.requestId,
  })
}

/** Parse a strict legacy or correlated worker-requested event projection. */
export function parseWorkerRequestedV1(value: unknown, expectedBranch: 'legacy'): LegacyWorkerRequestedV1
export function parseWorkerRequestedV1(value: unknown, expectedBranch: 'parallel'): ParallelWorkerRequestedV1
export function parseWorkerRequestedV1(value: unknown, expectedBranch?: ExpectedEventBranch): LegacyWorkerRequestedV1 | ParallelWorkerRequestedV1
export function parseWorkerRequestedV1(
  value: unknown,
  expectedBranch?: ExpectedEventBranch,
): LegacyWorkerRequestedV1 | ParallelWorkerRequestedV1 {
  const parsed = parseWorkerSpec(value, expectedBranch) as LegacyWorkerRequestedV1 | ParallelWorkerRequestedV1
  if ('fanoutId' in parsed) assertEventPayloadLimit(parsed, MAX_PARALLEL_WORKER_REQUESTED_PAYLOAD_BYTES, 'parallel worker-requested')
  return parsed
}

/** Parse a strict legacy or correlated worker-finished event projection. */
export function parseWorkerFinishedV1(value: unknown, expectedBranch: 'legacy'): LegacyWorkerFinishedV1
export function parseWorkerFinishedV1(value: unknown, expectedBranch: 'parallel'): ParallelWorkerFinishedV1
export function parseWorkerFinishedV1(value: unknown, expectedBranch?: ExpectedEventBranch): LegacyWorkerFinishedV1 | ParallelWorkerFinishedV1
export function parseWorkerFinishedV1(
  value: unknown,
  expectedBranch?: ExpectedEventBranch,
): LegacyWorkerFinishedV1 | ParallelWorkerFinishedV1 {
  const parsed = parseWorkerFinishedRecord(value, expectedBranch)
  if ('workerRef' in parsed) assertEventPayloadLimit(parsed, MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES, 'parallel worker-finished')
  return parsed
}

/** Parse the bounded planned-request manifest anchoring one parallel DAG. */
export function parseParallelStartedV1(value: unknown): ParallelStartedV1 {
  assertJsonValue(value, 'parallelStarted')
  const input = exactRecord(value, 'parallelStarted', PARALLEL_STARTED_KEYS)
  if (input.schemaVersion !== 1) fail('parallelStarted.schemaVersion', 'must be 1')
  const requestsValue = required(input, 'requests', 'parallelStarted')
  if (!Array.isArray(requestsValue)) fail('parallelStarted.requests', 'must be an array')
  if (requestsValue.length === 0) fail('parallelStarted.requests', 'must not be empty')
  if (requestsValue.length > MAX_DAG_NODES) fail('parallelStarted.requests', `must not contain more than ${MAX_DAG_NODES} items`)

  const requests = requestsValue.map(parsePlannedRequest)
  const requestIds = new Set<string>()
  const nodeIds = new Set<string>()
  for (const request of requests) {
    if (requestIds.has(request.requestId)) fail('parallelStarted.requests', 'must have unique requestId values')
    if (nodeIds.has(request.nodeId)) fail('parallelStarted.requests', 'must have unique nodeId values')
    requestIds.add(request.requestId)
    nodeIds.add(request.nodeId)
  }

  const parsed = deepFreeze({
    schemaVersion: 1 as const,
    dagId: boundedDerivedIdentifier(required(input, 'dagId', 'parallelStarted'), 'parallelStarted.dagId', MAX_SCHEDULING_IDENTIFIER_BYTES),
    requests,
  })
  assertEventPayloadLimit(parsed, MAX_PARALLEL_STARTED_PAYLOAD_BYTES, 'parallel-started')
  return parsed
}

/** Append the planned parallel request manifest before any worker starts. */
export function appendParallelStarted(session: Session, value: ParallelStartedV1): number {
  return session.append('dsh-plugin/parallel-started', parseParallelStartedV1(value)).seq
}

/** Append one correlated parallel worker request after its payload ceiling is checked. */
export function appendParallelWorkerRequested(
  session: Session,
  value: ParallelWorkerRequestedV1,
  measure: typeof serializedPayloadBytes = serializedPayloadBytes,
): number {
  const parsed = parseWorkerRequestedV1(value, 'parallel') as ParallelWorkerRequestedV1
  assertMeasuredEventPayloadLimit(parsed, MAX_PARALLEL_WORKER_REQUESTED_PAYLOAD_BYTES, 'parallel worker-requested', measure)
  return session.append('dsh-plugin/worker-requested', parsed).seq
}

/** Append one correlated parallel worker result; raw child session ids never enter this shape. */
export function appendParallelWorkerFinished(session: Session, value: ParallelWorkerFinishedV1): number {
  const parsed = parseWorkerFinishedV1(value, 'parallel') as ParallelWorkerFinishedV1
  return session.append('dsh-plugin/worker-finished', parsed).seq
}

/** Append a validated level or cumulative parallel aggregate. */
export function appendParallelFinished(session: Session, value: ParallelAggregateV1): number {
  return session.append('dsh-plugin/parallel-finished', parseParallelAggregateV1(value)).seq
}
