import { describe, expect, it } from 'vitest'
import { Session, SessionId } from '@deepseek-ai/dsh-session'
import {
  MAX_DAG_NODES,
  MAX_PARALLEL_STARTED_PAYLOAD_BYTES,
  MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES,
  MAX_PARALLEL_WORKER_REQUESTED_PAYLOAD_BYTES,
  assertSerializedPayloadLimit,
  serializedPayloadBytes,
  type ParallelAggregateV1,
} from '@han_05/dsh-scheduling-contracts'
import {
  appendParallelFinished,
  appendParallelStarted,
  appendParallelWorkerFinished,
  appendParallelWorkerRequested,
  parseParallelStartedV1,
  parseWorkerFinishedV1,
  parseWorkerRequestedV1,
  type ParallelStartedV1,
  type ParallelWorkerFinishedV1,
  type ParallelWorkerRequestedV1,
  type ExpectedEventBranch,
} from '../src/parallel-events.ts'
import { appendWorkerRequested } from '../src/events.ts'
import type { HandoffV1, WorkerSpecV1 } from '../src/types.ts'

const textEncoder = new TextEncoder()
const WORKER_REF = `w:${'a'.repeat(32)}`

const triple = {
  fanoutId: 'root:dag:1:aggregate',
  nodeId: 'worker-a',
  requestId: 'root:dag:1:node:worker-a',
} as const

const workerSpec: WorkerSpecV1 = {
  schemaVersion: 1,
  task: 'Implement the bounded worker event contract.',
  provider: 'provider-disabled',
  model: 'model-disabled',
  reasoningEffort: 'high',
  maxTokens: 32_000,
  allowedTools: ['read_file', 'write_file'],
  expectedOutput: 'handoff-v1',
}

const legacyTargetedVerifyWorkerSpec: WorkerSpecV1 = {
  ...workerSpec,
  allowedTools: ['targeted_verify'],
}

const handoff: HandoffV1 = {
  schemaVersion: 1,
  status: 'completed',
  summary: 'Implemented the bounded worker event contract.',
  changedFiles: ['packages/dsh-orchestrator/src/parallel-events.ts'],
  decisions: ['Kept legacy and parallel event branches disjoint.'],
  verification: [],
  blockers: [],
}

const parallelWorkerRequest: ParallelWorkerRequestedV1 = { ...workerSpec, ...triple }
const parallelWorkerFinished: ParallelWorkerFinishedV1 = {
  schemaVersion: 1,
  workerRef: WORKER_REF,
  handoff,
  ...triple,
}

const parallelStarted: ParallelStartedV1 = {
  schemaVersion: 1,
  dagId: 'root:dag:1',
  requests: [triple],
}

const parallelAggregate: ParallelAggregateV1 = {
  schemaVersion: 1,
  dagId: 'root:dag:1',
  scope: 'dag',
  fanoutId: triple.fanoutId,
  nodeResults: [{
    schemaVersion: 1,
    nodeId: triple.nodeId,
    requestId: triple.requestId,
    workerRef: WORKER_REF,
    status: 'completed',
    reason: 'completed',
  }],
  aggregateStatus: 'completed',
  verificationOutcome: 'not-run-no-commands',
  ownershipViolations: [],
  projectedHandoff: handoff,
}

function nativeSerializedBytes(value: unknown): number {
  return textEncoder.encode(JSON.stringify(value)).byteLength
}

function utf8StringWithBytes(byteLength: number): string {
  return `${'界'.repeat(Math.floor(byteLength / 3))}${'x'.repeat(byteLength % 3)}`
}

function schemaValidParallelFinishedAt(targetBytes: number): ParallelWorkerFinishedV1 {
  const decisions = Array.from({ length: 15 }, () => utf8StringWithBytes(16_384))
  const value = {
    ...parallelWorkerFinished,
    handoff: {
      ...handoff,
      summary: 'UTF-8 boundary fixture.',
      changedFiles: [],
      decisions,
    },
  }
  const finalItemBytes = targetBytes - nativeSerializedBytes(value) - 3
  if (finalItemBytes < 1 || finalItemBytes > 16_384) {
    throw new Error(`cannot construct ${targetBytes}-byte worker-finished fixture`)
  }
  decisions.push(utf8StringWithBytes(finalItemBytes))
  if (nativeSerializedBytes(value) !== targetBytes) {
    throw new Error(`worker-finished fixture is not ${targetBytes} bytes`)
  }
  return value
}

function maximumLegalParallelStarted(): ParallelStartedV1 {
  const rootSessionId = 'r'.repeat(48)
  const dagId = `${rootSessionId}:dag:999`
  return {
    schemaVersion: 1,
    dagId,
    requests: Array.from({ length: MAX_DAG_NODES }, (_, index) => {
      const nodeId = `n${index.toString().padStart(2, '0')}${'x'.repeat(29)}`
      return {
        fanoutId: `${dagId}:aggregate`,
        nodeId,
        requestId: `${dagId}:node:${nodeId}`,
      }
    }),
  }
}

function escapedBoundedString(prefix: string, byteLength: number): string {
  return `${prefix}${'\u0001'.repeat(byteLength - textEncoder.encode(prefix).byteLength)}`
}

function maximumLegalParallelWorkerRequested(): ParallelWorkerRequestedV1 {
  const rootSessionId = 'r'.repeat(48)
  const dagId = `${rootSessionId}:dag:999`
  const nodeId = 'n'.repeat(32)
  return {
    schemaVersion: 1,
    task: escapedBoundedString('task-', 16_384),
    provider: escapedBoundedString('provider-', 256),
    model: escapedBoundedString('model-', 256),
    reasoningEffort: escapedBoundedString('effort-', 256),
    maxTokens: 128_000,
    allowedTools: Array.from({ length: 16 }, (_, index) =>
      escapedBoundedString(`tool-${index.toString().padStart(2, '0')}-`, 256)),
    expectedOutput: 'handoff-v1',
    fanoutId: `${dagId}:aggregate`,
    nodeId,
    requestId: `${dagId}:node:${nodeId}`,
  }
}

function expectDeepFrozen(value: unknown): void {
  if (typeof value !== 'object' || value === null) return
  expect(Object.isFrozen(value)).toBe(true)
  for (const child of Object.values(value)) expectDeepFrozen(child)
}

function payloadKeys(value: unknown): string[] {
  if (Array.isArray(value)) return value.flatMap(payloadKeys)
  if (value === null || typeof value !== 'object') return []
  return Object.entries(value).flatMap(([key, child]) => [key, ...payloadKeys(child)])
}

describe('strict worker event unions', () => {
  it('accepts legacy targeted_verify requests through parsing and append with the legacy shape', () => {
    const session = Session.create(SessionId('legacy-targeted-verify'))

    expect(parseWorkerRequestedV1(legacyTargetedVerifyWorkerSpec, 'legacy')).toEqual(legacyTargetedVerifyWorkerSpec)
    expect(appendWorkerRequested(session, legacyTargetedVerifyWorkerSpec)).toBe(0)
    expect(session.events[0]?.data).toEqual(legacyTargetedVerifyWorkerSpec)
    expect(session.events[0]?.data).not.toHaveProperty('fanoutId')
    expect(session.events[0]?.data).not.toHaveProperty('workerRef')
  })

  it('continues rejecting targeted_verify on the parallel worker-requested branch', () => {
    const parallelRequest = { ...legacyTargetedVerifyWorkerSpec, ...triple }
    const session = Session.create(SessionId('parallel-targeted-verify'))

    expect(() => parseWorkerRequestedV1(parallelRequest, 'parallel')).toThrow(/targeted_verify/u)
    expect(() => appendParallelWorkerRequested(session, parallelRequest)).toThrow(/targeted_verify/u)
    expect(session.events).toEqual([])
  })

  it('round-trips byte-compatible legacy requested and finished branches', () => {
    const requested = parseWorkerRequestedV1(workerSpec, 'legacy')
    const finished = parseWorkerFinishedV1({
      schemaVersion: 1,
      childSessionId: SessionId('child-session'),
      handoff,
    }, 'legacy')

    expect(requested).toEqual(workerSpec)
    expect(finished).toEqual({
      schemaVersion: 1,
      childSessionId: 'child-session',
      handoff,
    })
    expect(requested).not.toHaveProperty('fanoutId')
    expect(finished).not.toHaveProperty('workerRef')
  })

  it('round-trips complete parallel branches without a raw childSessionId', () => {
    const requested = parseWorkerRequestedV1(parallelWorkerRequest, 'parallel')
    const finished = parseWorkerFinishedV1(parallelWorkerFinished, 'parallel')

    expect(requested).toEqual(parallelWorkerRequest)
    expect(finished).toEqual(parallelWorkerFinished)
    expect(requested).not.toHaveProperty('workerRef')
    expect(requested).not.toHaveProperty('childSessionId')
    expect(finished).not.toHaveProperty('childSessionId')
    expectDeepFrozen(requested)
    expectDeepFrozen(finished)
  })

  it('uses expectedBranch context while structurally accepting either complete standalone branch', () => {
    const legacyFinished = { schemaVersion: 1, childSessionId: 'child-session', handoff }

    expect(parseWorkerRequestedV1(workerSpec)).toEqual(workerSpec)
    expect(parseWorkerRequestedV1(parallelWorkerRequest)).toEqual(parallelWorkerRequest)
    expect(parseWorkerFinishedV1(legacyFinished)).toEqual(legacyFinished)
    expect(parseWorkerFinishedV1(parallelWorkerFinished)).toEqual(parallelWorkerFinished)
    expect(() => parseWorkerRequestedV1(workerSpec, 'parallel')).toThrow(/parallel/u)
    expect(() => parseWorkerRequestedV1(parallelWorkerRequest, 'legacy')).toThrow(/legacy/u)
    expect(() => parseWorkerFinishedV1(legacyFinished, 'parallel')).toThrow(/parallel/u)
    expect(() => parseWorkerFinishedV1(parallelWorkerFinished, 'legacy')).toThrow(/legacy/u)
  })

  it('accepts an ExpectedEventBranch variable while retaining branch-specific overloads', () => {
    const parseRequestedAtBranch = (expectedBranch: ExpectedEventBranch) => parseWorkerRequestedV1(
      expectedBranch === 'legacy' ? workerSpec : parallelWorkerRequest,
      expectedBranch,
    )
    const parseFinishedAtBranch = (expectedBranch: ExpectedEventBranch) => parseWorkerFinishedV1(
      expectedBranch === 'legacy'
        ? { schemaVersion: 1, childSessionId: 'child-session', handoff }
        : parallelWorkerFinished,
      expectedBranch,
    )

    expect(parseRequestedAtBranch('legacy')).toEqual(workerSpec)
    expect(parseRequestedAtBranch('parallel')).toEqual(parallelWorkerRequest)
    expect(parseFinishedAtBranch('legacy')).toEqual({
      schemaVersion: 1,
      childSessionId: 'child-session',
      handoff,
    })
    expect(parseFinishedAtBranch('parallel')).toEqual(parallelWorkerFinished)
  })

  it.each([
    { fanoutId: triple.fanoutId },
    { nodeId: triple.nodeId },
    { requestId: triple.requestId },
    { fanoutId: triple.fanoutId, nodeId: triple.nodeId },
    { fanoutId: triple.fanoutId, requestId: triple.requestId },
    { nodeId: triple.nodeId, requestId: triple.requestId },
  ])('rejects partial correlation triple %#', partialTriple => {
    expect(() => parseWorkerRequestedV1({ ...workerSpec, ...partialTriple })).toThrow(/correlation|triple|branch/u)
    expect(() => parseWorkerFinishedV1({
      schemaVersion: 1,
      workerRef: WORKER_REF,
      handoff,
      ...partialTriple,
    })).toThrow(/correlation|triple|branch/u)
  })

  it.each([
    { ...parallelWorkerRequest, childSessionId: 'child-session' },
    { ...parallelWorkerRequest, workerRef: WORKER_REF },
    { ...parallelWorkerFinished, childSessionId: 'child-session' },
    { schemaVersion: 1, childSessionId: 'child-session', workerRef: WORKER_REF, handoff, ...triple },
  ])('rejects dual-shape payload %#', value => {
    const parse = 'task' in value ? parseWorkerRequestedV1 : parseWorkerFinishedV1
    expect(() => parse(value)).toThrow()
  })

  it.each([
    [{ ...triple, nodeId: 'bad:node' }, /nodeId/u],
    [{ ...triple, fanoutId: 'f'.repeat(68) }, /fanoutId/u],
    [{ ...triple, requestId: 'r'.repeat(95) }, /requestId/u],
  ])('rejects an invalid correlation triple %#', (invalidTriple, message) => {
    expect(() => parseWorkerRequestedV1({ ...workerSpec, ...invalidTriple }, 'parallel')).toThrow(message)
  })

  it.each([
    'worker-session',
    `w:${'A'.repeat(32)}`,
    `w:${'a'.repeat(31)}`,
    `w:${'a'.repeat(33)}`,
  ])('rejects invalid parallel workerRef %s', workerRef => {
    expect(() => parseWorkerFinishedV1({ ...parallelWorkerFinished, workerRef }, 'parallel')).toThrow(/workerRef/u)
  })

  it('rejects malformed Unicode in derived parallel identifiers while preserving legacy childSessionId compatibility', () => {
    const malformed = '\uD800'

    expect(() => parseWorkerRequestedV1({
      ...parallelWorkerRequest,
      fanoutId: `${triple.fanoutId}${malformed}`,
    }, 'parallel')).toThrow(/fanoutId/u)
    expect(() => parseWorkerFinishedV1({
      ...parallelWorkerFinished,
      requestId: `${triple.requestId}${malformed}`,
    }, 'parallel')).toThrow(/requestId/u)
    expect(() => parseParallelStartedV1({
      ...parallelStarted,
      dagId: `${parallelStarted.dagId}${malformed}`,
    })).toThrow(/dagId/u)

    expect(parseWorkerFinishedV1({
      schemaVersion: 1,
      childSessionId: `legacy-child${malformed}`,
      handoff,
    }, 'legacy')).toEqual({
      schemaVersion: 1,
      childSessionId: `legacy-child${malformed}`,
      handoff,
    })
  })
})

describe('parallel event manifests and payload ceilings', () => {
  it('parses a detached deeply frozen planned request manifest', () => {
    const input = {
      schemaVersion: 1 as const,
      dagId: parallelStarted.dagId,
      requests: [{ ...triple }],
    }
    const parsed = parseParallelStartedV1(input)

    expect(parsed).toEqual(parallelStarted)
    expect(parsed).not.toBe(input)
    expect(parsed.requests).not.toBe(input.requests)
    expect(parsed.requests[0]).not.toBe(input.requests[0])
    expectDeepFrozen(parsed)

    input.requests[0]!.requestId = 'mutated'
    input.requests.push({ ...triple, nodeId: 'worker-b', requestId: 'root:dag:1:node:worker-b' })
    expect(parsed).toEqual(parallelStarted)
  })

  it('keeps maximum legal parallel-started and worker-requested compositions below their ceilings', () => {
    const started = maximumLegalParallelStarted()
    const requested = maximumLegalParallelWorkerRequested()

    expect(parseParallelStartedV1(started)).toEqual(started)
    expect(parseWorkerRequestedV1(requested, 'parallel')).toEqual(requested)
    expect(nativeSerializedBytes(started)).toBeLessThanOrEqual(MAX_PARALLEL_STARTED_PAYLOAD_BYTES)
    expect(nativeSerializedBytes(requested)).toBeLessThanOrEqual(MAX_PARALLEL_WORKER_REQUESTED_PAYLOAD_BYTES)
    expect(serializedPayloadBytes(started)).toBe(nativeSerializedBytes(started))
    expect(serializedPayloadBytes(requested)).toBe(nativeSerializedBytes(requested))
  })

  it('enforces controlled raw payloads at the exact and one-over UTF-8 serialized boundary', () => {
    const exact = utf8StringWithBytes(MAX_PARALLEL_STARTED_PAYLOAD_BYTES - 2)
    const over = `${exact}x`

    expect(nativeSerializedBytes(exact)).toBe(MAX_PARALLEL_STARTED_PAYLOAD_BYTES)
    expect(nativeSerializedBytes(over)).toBe(MAX_PARALLEL_STARTED_PAYLOAD_BYTES + 1)
    expect(() => assertSerializedPayloadLimit(exact, MAX_PARALLEL_STARTED_PAYLOAD_BYTES, 'parallel-started')).not.toThrow()
    expect(() => assertSerializedPayloadLimit(over, MAX_PARALLEL_STARTED_PAYLOAD_BYTES, 'parallel-started')).toThrow(/parallel-started/u)
  })

  it('accepts a schema-valid parallel worker-finished payload at the exact ceiling and rejects one byte over', () => {
    const exact = schemaValidParallelFinishedAt(MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES)
    const over = schemaValidParallelFinishedAt(MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES + 1)

    expect(nativeSerializedBytes(exact)).toBe(MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES)
    expect(nativeSerializedBytes(over)).toBe(MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES + 1)
    expect(parseWorkerFinishedV1(exact, 'parallel')).toEqual(exact)
    expect(() => parseWorkerFinishedV1(over, 'parallel')).toThrow(/payload ceiling/u)
  })

  it('rejects an over-limit worker-requested measurement before writing Session state', () => {
    const session = Session.create(SessionId('parallel-worker-requested-limit'))

    expect(() => appendParallelWorkerRequested(
      session,
      parallelWorkerRequest,
      () => MAX_PARALLEL_WORKER_REQUESTED_PAYLOAD_BYTES + 1,
    )).toThrow(/payload ceiling/u)
    expect(session.events).toEqual([])
  })

  it('rejects an over-limit worker-finished payload before writing Session state', () => {
    const session = Session.create(SessionId('parallel-worker-finished-limit'))
    const over = schemaValidParallelFinishedAt(MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES + 1)

    expect(() => appendParallelWorkerFinished(session, over)).toThrow(/payload ceiling/u)
    expect(session.events).toEqual([])
  })
})

describe('parallel durable event append helpers', () => {
  it('appends planned, requested, finished, and aggregate snapshots in order', () => {
    const session = Session.create(SessionId('parallel-event-order'))
    const mutableStarted = { ...parallelStarted, requests: [{ ...triple }] }
    const mutableRequest = { ...parallelWorkerRequest, allowedTools: [...parallelWorkerRequest.allowedTools] }
    const mutableHandoff = {
      ...handoff,
      changedFiles: [...handoff.changedFiles],
      decisions: [...handoff.decisions],
      verification: [...handoff.verification],
      blockers: [...handoff.blockers],
    }
    const mutableFinished = { ...parallelWorkerFinished, handoff: mutableHandoff }
    const mutableAggregate = {
      ...parallelAggregate,
      nodeResults: parallelAggregate.nodeResults.map(result => ({ ...result })),
      ownershipViolations: [...parallelAggregate.ownershipViolations],
      projectedHandoff: mutableHandoff,
    }

    expect(appendParallelStarted(session, mutableStarted)).toBe(0)
    expect(appendParallelWorkerRequested(session, mutableRequest)).toBe(1)
    expect(appendParallelWorkerFinished(session, mutableFinished)).toBe(2)
    expect(appendParallelFinished(session, mutableAggregate)).toBe(3)
    expect(session.events.map(event => event.type)).toEqual([
      'dsh-plugin/parallel-started',
      'dsh-plugin/worker-requested',
      'dsh-plugin/worker-finished',
      'dsh-plugin/parallel-finished',
    ])
    expect(session.events.map(event => event.data)).toEqual([
      parallelStarted,
      parallelWorkerRequest,
      parallelWorkerFinished,
      parallelAggregate,
    ])

    mutableStarted.requests.push({ ...triple, nodeId: 'worker-b', requestId: 'root:dag:1:node:worker-b' })
    mutableRequest.allowedTools.push('shell')
    mutableHandoff.changedFiles.push('mutated-after-append.ts')
    mutableHandoff.decisions.push('Mutated after append.')

    expect(session.events.map(event => event.data)).toEqual([
      parallelStarted,
      parallelWorkerRequest,
      parallelWorkerFinished,
      parallelAggregate,
    ])
    for (const event of session.events) expectDeepFrozen(event.data)
    expect(JSON.parse(JSON.stringify(session.events))).toEqual(session.events)
    expect(payloadKeys(session.events.map(event => event.data))).not.toEqual(expect.arrayContaining([
      'authorization',
      'credential',
      'token',
      'transcript',
      'rawTranscript',
      'childSessionId',
    ]))
  })

  it.each([
    { ...parallelWorkerRequest, authorization: 'SECRET' },
    { ...parallelWorkerFinished, credential: 'SECRET' },
    { ...parallelStarted, token: 'SECRET' },
    { ...parallelAggregate, rawTranscript: 'SECRET' },
  ])('rejects sensitive or unknown payload fields before append %#', value => {
    const session = Session.create(SessionId('parallel-sensitive-field'))
    const append = 'requests' in value
      ? appendParallelStarted
      : 'nodeResults' in value
        ? appendParallelFinished
        : 'task' in value
          ? appendParallelWorkerRequested
          : appendParallelWorkerFinished

    expect(() => append(session, value as never)).toThrow()
    expect(session.events).toEqual([])
  })
})
