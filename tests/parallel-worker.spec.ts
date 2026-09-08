import { describe, expect, it, vi } from 'vitest'
import { Session, SessionId } from '@deepseek-ai/dsh-session'
import type { Agent } from '@deepseek-ai/dsh-agent'
import type { SubagentRun, SubagentRuntime, SubagentStartRequest } from '@deepseek-ai/dsh-subagent'
import {
  MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES,
  serializedPayloadBytes,
  parseTaskDagV1,
  type TaskNodeV1,
} from '@han_05/dsh-scheduling-contracts'
import type { HandoffV1 } from '../src/types.ts'
import type { ResolvedScheduleV1 } from '../src/scheduling.ts'
import {
  parallelWorkerSpec,
  runParallelWorker,
  type ParallelWorkerRunInput,
} from '../src/parallel-worker.ts'

const workerRef = `w:${'a'.repeat(32)}`
const handoff: HandoffV1 = {
  schemaVersion: 1,
  status: 'completed',
  summary: 'Updated the leaf file.',
  changedFiles: ['src/a.ts'],
  decisions: ['Kept the leaf bounded.'],
  verification: [],
  blockers: [],
}

const node: TaskNodeV1 = parseTaskDagV1({
  schemaVersion: 1,
  rootTaskId: 'root-task',
  nodes: [{
    schemaVersion: 1,
    nodeId: 'leaf-a',
    objective: 'Update the leaf file.',
    profile: { coding: 80, reasoning: 60, toolUse: 50, repoContext: 70, risk: 20, difficulty: 40 },
    constraints: {
      maxWorkers: 1,
      maxOutputTokens: 32_000,
      maxLatencyMs: 60_000,
      allowPaidFallback: false,
      requiredTools: ['read_file'],
    },
    readPaths: ['src/'],
    writePaths: ['src/a.ts'],
    dependsOn: [],
  }],
}).nodes[0]!

const resolvedSchedule: ResolvedScheduleV1 = {
  request: {} as never,
  decision: {
    schemaVersion: 1,
    mode: 'single-worker',
    route: { provider: 'provider-disabled', model: 'baseline-disabled', maxTokens: 32_000 },
    workerCount: 1,
    source: 'profile-fallback',
    policyVersion: 'profile-fallback-v1',
  },
}

interface FakeResult {
  readonly stopReason: 'completed' | 'aborted' | 'error' | 'max-tokens' | 'refusal'
  readonly structured?: unknown
  readonly output: readonly unknown[]
  readonly diagnostic?: string
}

interface RuntimeFixture {
  readonly input: ParallelWorkerRunInput
  readonly starts: SubagentStartRequest[]
  readonly dispose: ReturnType<typeof vi.fn>
  readonly abort: () => void
}

function validRun(result: Promise<FakeResult> = Promise.resolve({ stopReason: 'completed', structured: handoff, output: [] })): SubagentRun {
  return {
    id: SessionId('child-session-raw'),
    result: result as SubagentRun['result'],
    dispose: vi.fn(async () => undefined),
  } as SubagentRun
}

function fixture(start: (signal: AbortSignal) => Promise<SubagentRun> = async () => validRun()): RuntimeFixture {
  const controller = new AbortController()
  const starts: SubagentStartRequest[] = []
  const parent = { session: Session.create(SessionId('parallel-root')) } as Agent
  const dispose = vi.fn(async () => undefined)
  const startWithObservedDispose = async (_provider: 'spawn', request: SubagentStartRequest) => {
    starts.push(request)
    const run = await start(request.signal)
    run.dispose = dispose
    return run
  }
  const subagents = { start: startWithObservedDispose } as unknown as Pick<SubagentRuntime, 'start'>
  return {
    input: {
      node,
      dagId: 'dag-1',
      fanoutId: 'root:dag-1:fanout',
      requestId: 'root:dag-1:node:leaf-a',
      allowedTools: ['read_file'],
      resolvedSchedule,
      parent,
      signal: controller.signal,
      subagents,
      registerWorkerId: id => {
        expect(id).toBe(SessionId('child-session-raw'))
        return workerRef
      },
    },
    starts,
    dispose,
    abort: () => controller.abort(new Error('cancelled')),
  }
}

function workerFinishedEvents(fixtureValue: RuntimeFixture) {
  return fixtureValue.input.parent.session.events.filter(event => event.type === 'dsh-plugin/worker-finished')
}

function realisticThirtyKiBHandoff(): HandoffV1 {
  return {
    ...handoff,
    decisions: Array.from({ length: 2 }, () => 'x'.repeat(15_000)),
  }
}

function oversizedWorkerFinishedHandoff(): HandoffV1 {
  const oversized = {
    ...handoff,
    decisions: Array.from({ length: 17 }, () => 'x'.repeat(16_384)),
  }
  expect(serializedPayloadBytes({
    schemaVersion: 1,
    workerRef,
    handoff: oversized,
    fanoutId: 'root:dag-1:fanout',
    nodeId: 'leaf-a',
    requestId: 'root:dag-1:node:leaf-a',
  })).toBeGreaterThan(MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES)
  return oversized
}

describe('admitted parallel leaf worker', () => {
  it('records schedule/request before start and finished after publication without raw child id', async () => {
    const runtime = fixture()

    const terminal = await runParallelWorker(runtime.input)

    expect(runtime.input.parent.session.events.map(event => event.type)).toEqual([
      'dsh-plugin/schedule-selected',
      'dsh-plugin/worker-requested',
      'dsh-plugin/worker-finished',
    ])
    expect(runtime.starts[0]).toMatchObject({
      maxDepth: 1,
      toolFilter: { allow: ['read_file'] },
      agentOptions: { provider: 'provider-disabled', model: 'baseline-disabled' },
    })
    expect(runtime.starts[0]?.toolFilter?.allow).not.toContain('targeted_verify')
    expect(terminal).toMatchObject({
      nodeResult: { status: 'completed', reason: 'completed', workerRef },
      acceptedHandoff: { changedFiles: ['src/a.ts'] },
    })
    expect(runtime.input.parent.session.events[2]?.data).toEqual({
      schemaVersion: 1,
      workerRef,
      handoff,
      fanoutId: 'root:dag-1:fanout',
      nodeId: 'leaf-a',
      requestId: 'root:dag-1:node:leaf-a',
    })
    expect(runtime.dispose).toHaveBeenCalledTimes(1)
  })

  it('never publishes raw child ids, diagnostic text, or unstructured output', async () => {
    const runtime = fixture(async () => validRun(Promise.resolve({
      stopReason: 'completed',
      structured: handoff,
      output: [{ text: 'RAW_OUTPUT_SECRET' }],
      diagnostic: 'RAW_DIAGNOSTIC_SECRET',
    })))

    await runParallelWorker(runtime.input)

    const durable = JSON.stringify(runtime.input.parent.session.events)
    expect(durable).not.toContain('child-session-raw')
    expect(durable).not.toContain('RAW_OUTPUT_SECRET')
    expect(durable).not.toContain('RAW_DIAGNOSTIC_SECRET')
    expect(durable).not.toContain('childSessionId')
    expect(durable).not.toContain('diagnostic')
    expect(durable).not.toContain('output')
  })

  it('builds a leaf-only WorkerSpec from the resolved route', () => {
    expect(parallelWorkerSpec({
      node,
      dagId: 'dag-1',
      fanoutId: 'fanout-1',
      requestId: 'request-1',
      allowedTools: ['read_file'],
      resolvedSchedule,
      parent: {} as Agent,
      signal: new AbortController().signal,
      subagents: {} as Pick<SubagentRuntime, 'start'>,
      registerWorkerId: () => workerRef,
    })).toEqual({
      schemaVersion: 1,
      task: node.objective,
      provider: 'provider-disabled',
      model: 'baseline-disabled',
      maxTokens: 32_000,
      allowedTools: ['read_file'],
      expectedOutput: 'handoff-v1',
    })
  })

  it.each([
    ['abort-before-call', (runtime: RuntimeFixture) => { runtime.abort(); return Promise.reject(new Error('aborted before start')) }, { status: 'not-run', reason: 'cancelled-before-start', finished: 0 }],
    ['abort-while-pending', (runtime: RuntimeFixture) => new Promise<SubagentRun>((_resolve, reject) => {
      runtime.input.signal.addEventListener('abort', () => reject(new Error('aborted while start pending')), { once: true })
      queueMicrotask(runtime.abort)
    }), { status: 'not-run', reason: 'cancelled-before-start', finished: 0 }],
    ['abort-after-fulfillment', async (runtime: RuntimeFixture) => {
      await Promise.resolve()
      runtime.abort()
      return validRun()
    }, { status: 'blocked', reason: 'cancelled-after-start', workerRef, finished: 1 }],
    ['start-rejection', async () => { throw new Error('start failed') }, { status: 'failed', reason: 'start-failed', finished: 0 }],
  ] as const)('classifies %s at the publication boundary', async (_name, start, expected) => {
    const runtime = fixture(signal => start(runtime))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.nodeResult).toMatchObject({
      status: expected.status,
      reason: expected.reason,
      ...(expected.workerRef === undefined ? {} : { workerRef: expected.workerRef }),
    })
    expect(workerFinishedEvents(runtime)).toHaveLength(expected.finished)
  })

  it.each([
    ['fulfilled-aborted-result', async (runtime: RuntimeFixture) => {
      runtime.abort()
      return validRun(Promise.resolve({ stopReason: 'aborted', structured: handoff, output: [] }))
    }],
    ['rejected-result-after-abort', async (runtime: RuntimeFixture) => {
      const result = {
        then: (_onFulfilled: unknown, onRejected: (reason: unknown) => unknown) => ({
          then: (resolve: (value: unknown) => void) => {
            resolve(onRejected(new Error('child cancelled')))
            queueMicrotask(runtime.abort)
          },
        }),
      } as unknown as SubagentRun['result']
      return validRun(result)
    }],
  ] as const)('classifies a published %s as cancelled-after-start', async (_name, start) => {
    const runtime = fixture(signal => start(runtime))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.nodeResult).toMatchObject({
      status: 'blocked',
      reason: 'cancelled-after-start',
      workerRef,
    })
    expect(workerFinishedEvents(runtime)).toHaveLength(1)
    expect(runtime.dispose).toHaveBeenCalledTimes(1)
  })

  it('classifies a fulfilled aborted result as cancelled-after-start without parent cancellation', async () => {
    const runtime = fixture(async () => validRun(Promise.resolve({
      stopReason: 'aborted',
      structured: handoff,
      output: [],
    })))

    const terminal = await runParallelWorker(runtime.input)

    expect(runtime.input.signal.aborted).toBe(false)
    expect(terminal.nodeResult).toMatchObject({
      status: 'blocked',
      reason: 'cancelled-after-start',
      workerRef,
    })
    expect(workerFinishedEvents(runtime)).toHaveLength(1)
    expect(runtime.dispose).toHaveBeenCalledTimes(1)
  })

  it('checks raw changed-file ownership before normalizing the accepted handoff', async () => {
    const runtime = fixture(async () => validRun(Promise.resolve({
      stopReason: 'completed',
      structured: { ...handoff, changedFiles: ['src/not-declared.ts'] },
      output: [],
    })))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.nodeResult).toMatchObject({ status: 'ownership-violation', reason: 'violation', workerRef })
    expect(terminal.acceptedHandoff).toBeUndefined()
    expect(terminal.ownershipViolation).toMatchObject({ nodeId: 'leaf-a', count: 1 })
    expect(workerFinishedEvents(runtime)).toHaveLength(1)
    expect(JSON.stringify(runtime.input.parent.session.events)).not.toContain('not-declared.ts')
  })

  it('rejects an oversized but valid Handoff without worker-finished publication', async () => {
    const runtime = fixture(async () => validRun(Promise.resolve({
      stopReason: 'completed',
      structured: oversizedWorkerFinishedHandoff(),
      output: [],
    })))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.nodeResult).toMatchObject({ status: 'failed', reason: 'handoff-payload-too-large', workerRef })
    expect(terminal.acceptedHandoff).toBeUndefined()
    expect(workerFinishedEvents(runtime)).toHaveLength(0)
    expect(runtime.dispose).toHaveBeenCalledTimes(1)
  })

  it('publishes a realistic 30 KiB Handoff losslessly under the worker-finished ceiling', async () => {
    const largeHandoff = realisticThirtyKiBHandoff()
    const runtime = fixture(async () => validRun(Promise.resolve({
      stopReason: 'completed',
      structured: largeHandoff,
      output: [],
    })))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.acceptedHandoff).toEqual(largeHandoff)
    expect(workerFinishedEvents(runtime)).toHaveLength(1)
    expect(workerFinishedEvents(runtime)[0]?.data).toMatchObject({ handoff: largeHandoff })
  })

  it.each([
    ['blocked', 'blocked', 'blocked-result'],
    ['failed', 'failed', 'failed-result'],
  ] as const)('maps a completed stop with structured %s Handoff to %s/%s', async (handoffStatus, status, reason) => {
    const structured = { ...handoff, status: handoffStatus }
    const runtime = fixture(async () => validRun(Promise.resolve({
      stopReason: 'completed',
      structured,
      output: [],
    })))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.nodeResult).toMatchObject({ status, reason, workerRef })
    expect(terminal.acceptedHandoff).toBeUndefined()
    expect(workerFinishedEvents(runtime)).toHaveLength(1)
    expect(workerFinishedEvents(runtime)[0]?.data).toMatchObject({ handoff: structured })
  })

  it('disposes a published run exactly once when its result rejects', async () => {
    const runtime = fixture(async () => validRun(Promise.reject(new Error('child failed'))))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.nodeResult).toMatchObject({ status: 'failed', reason: 'failed-result', workerRef })
    expect(workerFinishedEvents(runtime)).toHaveLength(1)
    expect(runtime.dispose).toHaveBeenCalledTimes(1)
  })

  it('disposes a published run exactly once and preserves a workerRef registration collision', async () => {
    const collision = new Error('WORKER_REF_COLLISION')
    const runtime = fixture()
    const input = {
      ...runtime.input,
      registerWorkerId: () => { throw collision },
    }

    await expect(runParallelWorker(input)).rejects.toBe(collision)

    expect(runtime.dispose).toHaveBeenCalledTimes(1)
    expect(workerFinishedEvents(runtime)).toHaveLength(0)
    expect(JSON.stringify(runtime.input.parent.session.events)).not.toContain('child-session-raw')
  })

  it('disposes a published run exactly once and preserves a settlement exception', async () => {
    const settlementFailure = new Error('settlement failed')
    const run = validRun()
    Object.defineProperty(run, 'result', {
      configurable: true,
      get: () => { throw settlementFailure },
    })
    const runtime = fixture(async () => run)

    await expect(runParallelWorker(runtime.input)).rejects.toBe(settlementFailure)

    expect(runtime.dispose).toHaveBeenCalledTimes(1)
    expect(workerFinishedEvents(runtime)).toHaveLength(0)
  })

  it('disposes a published run exactly once and preserves a worker-finished append exception', async () => {
    const appendFailure = new Error('append failed')
    const runtime = fixture()
    const session = runtime.input.parent.session
    const originalAppend = session.append.bind(session) as (...args: unknown[]) => unknown
    Object.defineProperty(session, 'append', {
      configurable: true,
      value: vi.fn((...args: unknown[]) => {
        if (args[0] === 'dsh-plugin/worker-finished') throw appendFailure
        return originalAppend(...args)
      }),
    })

    await expect(runParallelWorker(runtime.input)).rejects.toBe(appendFailure)

    expect(runtime.dispose).toHaveBeenCalledTimes(1)
    expect(workerFinishedEvents(runtime)).toHaveLength(0)
  })

  it.each([
    ['cancellation', async (runtime: RuntimeFixture) => {
      runtime.abort()
      return validRun()
    }, { status: 'blocked', reason: 'cancelled-after-start', finished: 1 }],
    ['ownership violation', async () => validRun(Promise.resolve({
      stopReason: 'completed',
      structured: { ...handoff, changedFiles: ['src/not-declared.ts'] },
      output: [],
    })), { status: 'ownership-violation', reason: 'violation', finished: 1 }],
    ['oversized Handoff', async () => validRun(Promise.resolve({
      stopReason: 'completed',
      structured: oversizedWorkerFinishedHandoff(),
      output: [],
    })), { status: 'failed', reason: 'handoff-payload-too-large', finished: 0 }],
  ] as const)('does not let dispose failure replace %s classification or event cardinality', async (_name, start, expected) => {
    const runtime = fixture(signal => start(runtime))
    runtime.dispose.mockRejectedValueOnce(new Error('dispose failed'))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.nodeResult).toMatchObject({ status: expected.status, reason: expected.reason, workerRef })
    expect(runtime.dispose).toHaveBeenCalledTimes(1)
    expect(workerFinishedEvents(runtime)).toHaveLength(expected.finished)
  })

  it('converts an ordinary completed result to one bounded failed-result event when dispose fails', async () => {
    const runtime = fixture()
    runtime.dispose.mockRejectedValueOnce(new Error('RAW_DISPOSE_SECRET'))

    const terminal = await runParallelWorker(runtime.input)

    expect(terminal.nodeResult).toMatchObject({ status: 'failed', reason: 'failed-result', workerRef })
    expect(terminal.acceptedHandoff).toBeUndefined()
    expect(runtime.dispose).toHaveBeenCalledTimes(1)
    expect(workerFinishedEvents(runtime)).toHaveLength(1)
    expect(workerFinishedEvents(runtime)[0]?.data).toMatchObject({
      handoff: { status: 'failed', summary: 'Worker cleanup failed before completion.' },
    })
    expect(JSON.stringify(runtime.input.parent.session.events)).not.toContain('RAW_DISPOSE_SECRET')
  })
})
