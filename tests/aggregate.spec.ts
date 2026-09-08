import { describe, expect, it } from 'vitest'
import {
  MAX_AGGREGATE_PATH_BYTES,
  MAX_AGGREGATE_PAYLOAD_BYTES,
  MAX_AGGREGATE_PROJECTED_HANDOFF_BYTES,
  MAX_AGGREGATE_VERIFICATION_METADATA_SERIALIZED_BYTES,
  MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES,
  MAX_AGGREGATE_VERIFICATION_TOTAL_BYTES,
  MAX_AGGREGATE_VIOLATION_BYTES,
  MAX_DAG_NODES,
  parseParallelAggregateV1,
  serializedPayloadBytes,
  type HandoffV1,
  type ParallelNodeResultV1,
  type VerificationEvidenceV1,
} from '@han_05/dsh-scheduling-contracts'
import {
  boundOwnershipViolationSection,
  buildParallelAggregate,
  projectAggregateHandoff,
  projectAggregateVerification,
  type BuildParallelAggregateInput,
} from '../src/aggregate.ts'

const WORKER_REF = `w:${'a'.repeat(32)}`
const textEncoder = new TextEncoder()
const MAX_AGGREGATE_VERIFICATION_RECORD_BYTES =
  MAX_AGGREGATE_VERIFICATION_METADATA_SERIALIZED_BYTES + MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES

const passedEvidence: VerificationEvidenceV1 = {
  schemaVersion: 1,
  commandName: 'typecheck',
  args: [],
  exitCode: 0,
  status: 'passed',
  stdout: '',
  stderr: '',
  truncated: false,
  durationMs: 10,
}

const failedEvidence: VerificationEvidenceV1 = {
  ...passedEvidence,
  exitCode: 1,
  status: 'failed',
}

const baseHandoff: HandoffV1 = {
  schemaVersion: 1,
  status: 'completed',
  summary: 'Worker completed its assigned change.',
  changedFiles: [],
  decisions: [],
  verification: [],
  blockers: [],
}

function completed(nodeId: string, workerRef = WORKER_REF): ParallelNodeResultV1 {
  return {
    schemaVersion: 1,
    nodeId,
    requestId: `root:dag:1:node:${nodeId}`,
    workerRef,
    status: 'completed',
    reason: 'completed',
  }
}

function failed(nodeId: string, workerRef = WORKER_REF): ParallelNodeResultV1 {
  return {
    ...completed(nodeId, workerRef),
    status: 'failed',
    reason: 'failed-result',
  }
}

function violated(nodeId: string, workerRef = WORKER_REF): ParallelNodeResultV1 {
  return {
    ...completed(nodeId, workerRef),
    status: 'ownership-violation',
    reason: 'violation',
  }
}

function notRun(nodeId: string, reason: 'no-route' | 'dependency-not-run' = 'no-route'): ParallelNodeResultV1 {
  return {
    schemaVersion: 1,
    nodeId,
    requestId: `root:dag:1:node:${nodeId}`,
    status: 'not-run',
    reason,
  }
}

function buildInput(overrides: Partial<BuildParallelAggregateInput> = {}): BuildParallelAggregateInput {
  const nodeResults = overrides.nodeResults ?? [completed('a')]
  const acceptedHandoffs = overrides.acceptedHandoffs ?? new Map(
    nodeResults
      .filter(result => result.status === 'completed')
      .map(result => [result.nodeId, baseHandoff] as const),
  )
  const ownershipViolations = overrides.ownershipViolations ?? nodeResults
    .filter(result => result.status === 'ownership-violation')
    .map(result => ownershipSummary(result.nodeId))
  return {
    dagId: 'root:dag:1',
    scope: 'dag',
    fanoutId: 'root:dag:1:aggregate',
    nodeResults,
    nodeOrder: nodeResults.map(result => result.nodeId),
    acceptedHandoffs,
    ownershipViolations,
    verificationOutcome: 'not-run-no-commands',
    ...overrides,
  }
}

function ownershipSummary(nodeId: string, samplePaths: readonly string[] = []): {
  readonly nodeId: string
  readonly count: number
  readonly digest: string
  readonly samplePaths: readonly string[]
} {
  return {
    nodeId,
    count: Math.max(1, samplePaths.length),
    digest: nodeId.charCodeAt(0).toString(16).padStart(64, '0'),
    samplePaths,
  }
}

describe('buildParallelAggregate status derivation', () => {
  it.each([
    [[completed('a'), failed('b', `w:${'b'.repeat(32)}`)], 'passed', 'failed'],
    [[completed('a'), violated('b', `w:${'b'.repeat(32)}`)], 'command-failed', 'blocked'],
    [[notRun('a')], 'not-run-no-accepted-nodes', 'blocked'],
    [[completed('a')], 'admission-rejected', 'failed'],
    [[completed('a')], 'command-failed', 'verification-failed'],
    [[completed('a')], 'not-run-no-commands', 'completed'],
  ] as const)('derives aggregate status %#', (nodeResults, verificationOutcome, aggregateStatus) => {
    const aggregate = buildParallelAggregate(buildInput({
      nodeResults,
      verificationOutcome,
      ...(verificationOutcome === 'passed' ? { verification: [passedEvidence] } : {}),
      ...(verificationOutcome === 'command-failed' ? { verification: [failedEvidence] } : {}),
    }))
    expect(aggregate.aggregateStatus).toBe(aggregateStatus)
  })
})

describe('aggregate input integrity', () => {
  it('rejects a missing canonical nodeOrder instead of preserving completion order', () => {
    const input = buildInput({
      nodeResults: [completed('b', `w:${'b'.repeat(32)}`), completed('a')],
      nodeOrder: ['a', 'b'],
      acceptedHandoffs: new Map([
        ['a', baseHandoff],
        ['b', baseHandoff],
      ]),
    })
    const withoutNodeOrder = { ...input } as { nodeOrder?: readonly string[] }
    delete withoutNodeOrder.nodeOrder

    expect(() => buildParallelAggregate(withoutNodeOrder as unknown as BuildParallelAggregateInput)).toThrow(/nodeOrder/u)
  })

  it('rejects a missing accepted Handoff for a completed node', () => {
    const input = buildInput({
      nodeResults: [completed('a'), completed('b', `w:${'b'.repeat(32)}`)],
      nodeOrder: ['a', 'b'],
      acceptedHandoffs: new Map([['a', baseHandoff]]),
    })

    expect(() => buildParallelAggregate(input)).toThrow(/acceptedHandoffs/u)
  })

  it('rejects an extra accepted Handoff key', () => {
    const input = buildInput({
      acceptedHandoffs: new Map([
        ['a', baseHandoff],
        ['unexpected', baseHandoff],
      ]),
    })

    expect(() => buildParallelAggregate(input)).toThrow(/acceptedHandoffs/u)
  })

  it('rejects a requestId key instead of a nodeId key', () => {
    const input = buildInput({
      acceptedHandoffs: new Map([['root:dag:1:node:a', baseHandoff]]),
    })

    expect(() => buildParallelAggregate(input)).toThrow(/acceptedHandoffs/u)
  })

  it('rejects a non-completed or ownership-unsafe accepted Handoff', () => {
    const nonCompleted = {
      ...baseHandoff,
      status: 'blocked',
    } as const
    const nonCompletedInput = buildInput({
      acceptedHandoffs: new Map([['a', nonCompleted]]),
    })
    expect(() => buildParallelAggregate(nonCompletedInput)).toThrow(/completed Handoff/u)

    const ownershipUnsafeInput = buildInput({
      nodeResults: [completed('a'), violated('b', `w:${'b'.repeat(32)}`)],
      nodeOrder: ['a', 'b'],
      acceptedHandoffs: new Map([
        ['a', baseHandoff],
        ['b', baseHandoff],
      ]),
    })
    expect(() => buildParallelAggregate(ownershipUnsafeInput)).toThrow(/acceptedHandoffs/u)
  })

  it('is detached and deterministic across completion-order permutations', () => {
    const firstNodeResults = [completed('b', `w:${'b'.repeat(32)}`), completed('a')]
    const firstHandoffs = new Map<string, HandoffV1>([
      ['b', { ...baseHandoff, summary: 'summary-b' }],
      ['a', { ...baseHandoff, summary: 'summary-a' }],
    ])
    const first = buildParallelAggregate(buildInput({
      nodeResults: firstNodeResults,
      nodeOrder: ['a', 'b'],
      acceptedHandoffs: firstHandoffs,
    }))
    const second = buildParallelAggregate(buildInput({
      nodeResults: [completed('a'), completed('b', `w:${'b'.repeat(32)}`)],
      nodeOrder: ['a', 'b'],
      acceptedHandoffs: new Map([
        ['a', { ...baseHandoff, summary: 'summary-a' }],
        ['b', { ...baseHandoff, summary: 'summary-b' }],
      ]),
    }))

    expect(first).toEqual(second)
    expect(JSON.stringify(first)).toBe(JSON.stringify(second))
    expect(first.nodeResults).not.toBe(firstNodeResults)
    expect(first.projectedHandoff).not.toBe(firstHandoffs.get('a'))
  })
})

describe('projected aggregate Handoff', () => {
  it('reserves the budget-rejected blocker before saturated ordinary blockers', () => {
    const acceptedHandoffs = new Map([
      ['a', {
        ...baseHandoff,
        blockers: Array.from({ length: 128 }, (_, index) => `ordinary-blocker-${index.toString().padStart(3, '0')}-${'x'.repeat(128)}`),
      }],
    ])
    const aggregate = buildParallelAggregate(buildInput({
      acceptedHandoffs,
      verificationOutcome: 'admission-rejected',
    }))

    expect(aggregate.projectedHandoff.blockers).toContain('[verification: budget-rejected]')
    expect(aggregate.projectedHandoffTruncated).toBe(true)
  })

  it('orders node results and retains a deterministic whole-item projection', () => {
    const nodeResults = [completed('b', `w:${'b'.repeat(32)}`), completed('a')]
    const acceptedHandoffs = new Map([
      ['b', { ...baseHandoff, summary: 'summary-b', changedFiles: ['z.ts', 'shared.ts'], decisions: ['decision-z'] }],
      ['a', { ...baseHandoff, summary: 'summary-a', changedFiles: ['a.ts', 'shared.ts'], decisions: ['decision-a'] }],
    ])
    const aggregate = buildParallelAggregate(buildInput({ nodeResults, nodeOrder: ['a', 'b'], acceptedHandoffs }))

    expect(aggregate.nodeResults.map(result => result.nodeId)).toEqual(['a', 'b'])
    expect(aggregate.projectedHandoff.summary.indexOf('summary-a')).toBeLessThan(aggregate.projectedHandoff.summary.indexOf('summary-b'))
    expect(aggregate.projectedHandoff.changedFiles).toEqual(['a.ts', 'shared.ts', 'z.ts'])
    expect(aggregate.projectedHandoff.decisions).toEqual(['decision-a', 'decision-z'])
    expect(aggregate.projectedHandoff).not.toHaveProperty('nodeResults')
  })

  it('uses the explicit canonical order for level scope and rejects incomplete mappings', () => {
    const nodeResults = [completed('b', `w:${'b'.repeat(32)}`), completed('a')]
    const acceptedHandoffs = new Map([
      ['a', baseHandoff],
      ['b', baseHandoff],
    ])
    const input = buildInput({
      scope: 'level',
      fanoutId: 'root:dag:1:level:0',
      levelId: 'root:dag:1:level:0',
      levelIndex: 0,
      nodeResults,
      nodeOrder: ['a', 'b'],
      acceptedHandoffs,
    })

    expect(buildParallelAggregate(input).nodeResults.map(result => result.nodeId)).toEqual(['a', 'b'])
    expect(() => buildParallelAggregate({ ...input, nodeOrder: ['a'] })).toThrow(/nodeOrder/u)
  })

  it('marks verification failure in the projected failed Handoff', () => {
    const aggregate = buildParallelAggregate(buildInput({
      verificationOutcome: 'command-failed',
      verification: [failedEvidence],
    }))

    expect(aggregate.projectedHandoff.status).toBe('failed')
    expect(aggregate.projectedHandoff.summary).toContain('[verification: failed]')
    expect(aggregate.projectedHandoff.verification).toEqual([failedEvidence])
  })

  it('retains the verification-failed marker when a source summary is truncated', () => {
    const aggregate = buildParallelAggregate(buildInput({
      acceptedHandoffs: new Map([['a', {
        ...baseHandoff,
        summary: `${'x'.repeat(8_000)} [verification: failed]`,
      }]]),
      verificationOutcome: 'command-failed',
      verification: [failedEvidence],
    }))

    expect(aggregate.projectedHandoff.summary).toContain('[verification: failed]')
    expect(aggregate.projectedHandoff.summary).toContain('[summary truncated]')
  })

  it('retains only complete changed-file items when the projected field is saturated', () => {
    const files = Array.from({ length: 8 }, (_, index) => `file-${index}-${'界'.repeat(2_000)}`)
    const acceptedHandoffs = new Map([
      ['a', { ...baseHandoff, changedFiles: files }],
    ])
    const projection = projectAggregateHandoff(buildInput({ acceptedHandoffs }), 'completed')

    expect(projection.truncated).toBe(true)
    expect(projection.handoff.changedFiles.every(file => files.includes(file))).toBe(true)
    expect(serializedPayloadBytes(projection.handoff)).toBeLessThanOrEqual(MAX_AGGREGATE_PROJECTED_HANDOFF_BYTES)
  })

  it('retains a legal whole path at the exact 1024-byte boundary', () => {
    const path = `src/${'x'.repeat(MAX_AGGREGATE_PATH_BYTES - 4)}`
    expect(textEncoder.encode(path)).toHaveLength(MAX_AGGREGATE_PATH_BYTES)

    const projection = projectAggregateHandoff(buildInput({
      acceptedHandoffs: new Map([['a', { ...baseHandoff, changedFiles: [path] }]]),
    }), 'completed')

    expect(projection.handoff.changedFiles).toEqual([path])
    expect(projection.truncated).toBe(false)
  })
})

describe('aggregate section budgets', () => {
  it('does not truncate small-metadata ASCII stdout at the raw output ceiling', () => {
    const evidence = { ...passedEvidence, stdout: 'x'.repeat(MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES) }
    const projected = projectAggregateVerification([evidence])

    expect(projected[0]!.stdout).toBe(evidence.stdout)
    expect(projected[0]!.stderr).toBe('')
    expect(projected[0]!.truncated).toBe(false)
    expect(textEncoder.encode(projected[0]!.stdout).byteLength).toBe(MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES)
    expect(serializedPayloadBytes(projected[0])).toBeGreaterThan(MAX_AGGREGATE_VERIFICATION_METADATA_SERIALIZED_BYTES)
    expect(serializedPayloadBytes(projected[0])).toBeLessThanOrEqual(MAX_AGGREGATE_VERIFICATION_RECORD_BYTES)
  })

  it('preserves verification metadata and command order while shortening escaped output', () => {
    const evidence = Array.from({ length: 4 }, (_, index) => ({
      ...passedEvidence,
      commandName: `check-${index}`,
      stdout: '\n'.repeat(8_192),
      stderr: '界'.repeat(4_096),
    }))
    const projected = projectAggregateVerification(evidence)

    expect(projected.map(item => item.commandName)).toEqual(['check-0', 'check-1', 'check-2', 'check-3'])
    expect(projected.map(item => item.args)).toEqual(evidence.map(item => item.args))
    expect(projected.some(item => item.truncated)).toBe(true)
    expect(serializedPayloadBytes(projected)).toBeLessThanOrEqual(MAX_AGGREGATE_VERIFICATION_TOTAL_BYTES)
    expect(projected.every(item => textEncoder.encode(item.stdout).byteLength + textEncoder.encode(item.stderr).byteLength <= 8_192)).toBe(true)
    expect(projected.every(item => serializedPayloadBytes(item) <= MAX_AGGREGATE_VERIFICATION_RECORD_BYTES)).toBe(true)
  })

  it('shortens escaped output until each complete serialized record fits its 12288-byte allowance', () => {
    const evidence = [{ ...passedEvidence, stdout: '\n'.repeat(MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES) }]
    const projected = projectAggregateVerification(evidence)

    expect(projected[0]!.truncated).toBe(true)
    expect(projected[0]!.stdout.length).toBeLessThan(evidence[0]!.stdout.length)
    expect(serializedPayloadBytes(projected[0])).toBeGreaterThan(MAX_AGGREGATE_VERIFICATION_METADATA_SERIALIZED_BYTES)
    expect(serializedPayloadBytes(projected[0])).toBeLessThanOrEqual(MAX_AGGREGATE_VERIFICATION_RECORD_BYTES)
    expect(textEncoder.encode(projected[0]!.stdout).byteLength + textEncoder.encode(projected[0]!.stderr).byteLength).toBeLessThanOrEqual(MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES)
    expect(serializedPayloadBytes(projected)).toBeLessThanOrEqual(MAX_AGGREGATE_VERIFICATION_TOTAL_BYTES)
  })

  it('rejects a node-results and identifier envelope fragment over its 16384-byte sub-budget', () => {
    const oversizedRequestId = 'x'.repeat(16_300)
    const input = buildInput({
      nodeResults: [{ ...completed('a'), requestId: oversizedRequestId }],
      nodeOrder: ['a'],
    })

    expect(() => buildParallelAggregate(input)).toThrow(/16384/u)
  })

  it('drops every ownership sample path when the complete sample section exceeds its budget', () => {
    const summaries = Array.from({ length: MAX_DAG_NODES }, (_, nodeIndex) => ownershipSummary(
      `n${nodeIndex.toString().padStart(2, '0')}`,
      Array.from({ length: 16 }, (_, pathIndex) => `n${nodeIndex}/path-${pathIndex}-${'x'.repeat(1_000)}`),
    ))
    const bounded = boundOwnershipViolationSection(summaries)

    expect(bounded.map(item => item.nodeId)).toEqual(summaries.map(item => item.nodeId))
    expect(bounded.map(item => item.count)).toEqual(summaries.map(item => item.count))
    expect(bounded.every(item => item.samplePaths.length === 0)).toBe(true)
    expect(serializedPayloadBytes(bounded)).toBeLessThanOrEqual(MAX_AGGREGATE_VIOLATION_BYTES)
  })

  it('keeps all ownership samples when the complete section fits', () => {
    const summaries = [
      ownershipSummary('b', ['b/path.ts']),
      ownershipSummary('a', ['a/path.ts']),
    ]
    const bounded = boundOwnershipViolationSection(summaries)

    expect(bounded).toEqual([
      summaries[1],
      summaries[0],
    ])
    expect(bounded[0]!.samplePaths).toEqual(['a/path.ts'])
  })

  it('builds a worst-case escaped aggregate that remains bounded and parser-valid', () => {
    const nodeResults = Array.from({ length: MAX_DAG_NODES }, (_, index) => {
      const nodeId = `n${index.toString().padStart(2, '0')}`
      const workerRef = `w:${index.toString(16).padStart(32, '0')}`
      return index === 0 ? completed(nodeId, workerRef) : violated(nodeId, workerRef)
    })
    const acceptedHandoffs = new Map(nodeResults
      .filter(result => result.status === 'completed')
      .map(result => [result.nodeId, {
        ...baseHandoff,
        summary: '\n'.repeat(4_000),
        changedFiles: Array.from({ length: 16 }, (_, fileIndex) => `file-${fileIndex}-${'界'.repeat(900)}`),
        decisions: Array.from({ length: 16 }, (_, decisionIndex) => `decision-${decisionIndex}-${'界'.repeat(900)}`),
        blockers: Array.from({ length: 16 }, (_, blockerIndex) => `blocker-${blockerIndex}-${'界'.repeat(900)}`),
      }] as const))
    const ownershipViolations = nodeResults
      .filter(result => result.status === 'ownership-violation')
      .map(result => ownershipSummary(
      result.nodeId,
      Array.from({ length: 16 }, (_, pathIndex) => `${result.nodeId}/path-${pathIndex}-${'x'.repeat(1_000)}`),
      ))
    const maximum = buildParallelAggregate(buildInput({
      nodeResults,
      acceptedHandoffs,
      ownershipViolations,
      verificationOutcome: 'passed',
      verification: Array.from({ length: 4 }, (_, index) => ({
        ...passedEvidence,
        commandName: `check-${index}`,
        args: Array.from({ length: 8 }, () => 'x'.repeat(256)),
        stdout: '\n'.repeat(8_192),
        stderr: '界'.repeat(4_096),
      })),
    }))

    expect(serializedPayloadBytes(maximum)).toBeLessThanOrEqual(MAX_AGGREGATE_PAYLOAD_BYTES)
    expect(() => parseParallelAggregateV1(maximum)).not.toThrow()
    expect(serializedPayloadBytes(maximum.verification)).toBeLessThanOrEqual(MAX_AGGREGATE_VERIFICATION_TOTAL_BYTES)
    expect(serializedPayloadBytes(maximum.ownershipViolations)).toBeLessThanOrEqual(MAX_AGGREGATE_VIOLATION_BYTES)
    expect(serializedPayloadBytes(maximum.projectedHandoff)).toBeLessThanOrEqual(MAX_AGGREGATE_PROJECTED_HANDOFF_BYTES)
    expect(maximum.ownershipViolations.every(item => item.samplePaths.length === 0)).toBe(true)
    expect(maximum.verification?.at(-1)?.truncated).toBe(true)
  })
})
