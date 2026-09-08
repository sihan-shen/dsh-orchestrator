import { Context } from '@deepseek-ai/cordis'
import type { Agent } from '@deepseek-ai/dsh-agent'
import { Session, SessionId } from '@deepseek-ai/dsh-session'
import type { SubagentRun, SubagentStartRequest } from '@deepseek-ai/dsh-subagent'
import { parseTaskDagV1, type TaskDagV1 } from '@han_05/dsh-scheduling-contracts'
import { describe, expect, it, vi } from 'vitest'
import { createBudgetControllerRegistry } from '../src/budgets.ts'
import { appendParallelFinished } from '../src/parallel-events.ts'
import {
  mountParallelExecutionService,
  ParallelGenerationDisposedError,
} from '../src/parallel.ts'
import type { SchedulerResolver } from '../src/scheduling.ts'
import type { HandoffV1, OrchestratorConfig } from '../src/types.ts'

Object.defineProperty(Session.prototype, 'events', { configurable: true, get(this: Session) { return this.snapshotEvents() } })

function deferred<T>() {
  let resolve = (_value: T | PromiseLike<T>) => undefined
  const promise = new Promise<T>(settle => { resolve = settle })
  return { promise, resolve }
}

const config: OrchestratorConfig = {
  workspaceRoot: '.',
  mode: 'single-worker',
  worker: { provider: 'provider-disabled', model: 'model-disabled', maxTokens: 32_000 },
  budgets: { maxWorkers: 2, maxPluginToolActions: 4, toolTimeoutMs: 60_000 },
  verification: { commands: [], timeoutMs: 60_000, maxOutputBytes: 4_096 },
  parallel: {
    maxParallelWorkers: 2,
    verification: { schemaVersion: 1, scope: 'dag', commands: [] },
    workerToolAllowlist: [],
    routeToolFilters: {},
  },
}

const taskDag: TaskDagV1 = parseTaskDagV1({
  schemaVersion: 1,
  rootTaskId: 'task-13-root',
  nodes: [{
    schemaVersion: 1,
    nodeId: 'worker-a',
    objective: 'Complete the lifecycle fixture.',
    profile: { coding: 50, reasoning: 50, toolUse: 50, repoContext: 50, risk: 20, difficulty: 40 },
    constraints: {
      maxWorkers: 1,
      maxOutputTokens: 32_000,
      maxLatencyMs: 60_000,
      allowPaidFallback: false,
      requiredTools: [],
    },
    readPaths: ['src/worker-a.ts'],
    writePaths: ['src/worker-a.ts'],
    dependsOn: [],
  }],
})

const handoff: HandoffV1 = {
  schemaVersion: 1,
  status: 'completed',
  summary: 'Lifecycle fixture completed.',
  changedFiles: ['src/worker-a.ts'],
  decisions: [],
  verification: [],
  blockers: [],
}

interface MountOptions {
  readonly result?: Promise<Awaited<SubagentRun['result']>>
  readonly disposeRun?: () => Promise<void>
  readonly afterCumulativeAppend?: () => void
}

function mount(options: MountOptions = {}) {
  const ctx = new Context()
  const session = Session.create(SessionId('parallel-lifecycle-root'))
  const parent = { session } as Agent
  const starts: SubagentStartRequest[] = []
  const startObserved = deferred<void>()
  const disposeRun = vi.fn(options.disposeRun ?? (async () => undefined))
  const budgetRegistry = createBudgetControllerRegistry(config.budgets, () => () => undefined)
  const schedulerResolver: SchedulerResolver = { current: () => undefined }
  const mounted = mountParallelExecutionService(ctx, {
    config,
    budgetRegistry,
    schedulerResolver,
    subagents: {
      getProvider() {
        return {
          name: 'spawn',
          inheritsParentContext: false,
          capabilities: { agentOptions: true, outputSchema: true, depthLimit: true, toolFilter: true, persona: false },
          async start() { throw new Error('not used') },
        }
      },
      async start(_provider, request) {
        starts.push(request)
        startObserved.resolve()
        return {
          id: SessionId('parallel-lifecycle-child'),
          result: options.result ?? Promise.resolve({ stopReason: 'completed', structured: handoff, output: [] }),
          dispose: disposeRun,
        } as SubagentRun
      },
    },
    appendAggregate(target, aggregate) {
      appendParallelFinished(target, aggregate)
      if (aggregate.scope === 'dag') options.afterCumulativeAppend?.()
    },
  })
  return {
    ...mounted,
    ctx,
    session,
    parent,
    starts,
    startObserved: startObserved.promise,
    disposeRun,
    request: () => ({ dag: taskDag, parent, signal: new AbortController().signal }),
  }
}

describe('parallel execution generation lifecycle', () => {
  it('exports a stable generation-disposed error contract', () => {
    expect(new ParallelGenerationDisposedError()).toMatchObject({
      name: 'ParallelGenerationDisposedError',
      code: 'GENERATION_DISPOSED',
    })
  })

  it('retains strict run-envelope validation before allocation', async () => {
    const generation = mount()

    await expect(generation.service.run({
      ...generation.request(),
      policy: { scope: 'dag', commands: [] },
    } as never)).rejects.toThrow(/policy.*(?:unknown|not supported)/u)

    expect(generation.session.events).toEqual([])
    await generation.dispose()
  })

  it('aborts and drains an active run before unregistering without a synthetic final aggregate', async () => {
    const cleanup = deferred<void>()
    const generation = mount({
      result: new Promise(() => undefined),
      disposeRun: async () => cleanup.promise,
    })
    const running = generation.service.run(generation.request())
    await generation.startObserved

    let disposalSettled = false
    const disposing = generation.dispose().then(() => { disposalSettled = true })
    await vi.waitFor(() => expect(generation.disposeRun).toHaveBeenCalledTimes(1))

    expect(generation.starts[0]?.signal.aborted).toBe(true)
    expect(generation.starts[0]?.signal.reason).toBe('generation-disposed')
    expect(disposalSettled).toBe(false)
    expect((generation.ctx as unknown as { get(name: string): unknown }).get('parallelExecution')).toBe(generation.service)
    expect(generation.session.events.filter(event => event.type === 'dsh-plugin/parallel-finished')).toEqual([])

    cleanup.resolve()
    await expect(running).rejects.toMatchObject({
      name: 'ParallelGenerationDisposedError',
      code: 'GENERATION_DISPOSED',
    })
    await disposing
    expect((generation.ctx as unknown as { get(name: string): unknown }).get('parallelExecution')).toBeUndefined()
  })

  it('returns the committed result when disposal begins after the cumulative append', async () => {
    const committed = deferred<void>()
    const generation = mount({ afterCumulativeAppend: () => committed.resolve() })
    const running = generation.service.run(generation.request())
    await committed.promise

    const disposing = generation.dispose()
    await expect(running).resolves.toMatchObject({ finalAggregate: { scope: 'dag' } })
    await disposing
  })

  it('admits no new run, DAG allocation, or worker start after disposal begins', async () => {
    const generation = mount()
    const disposing = generation.dispose()

    await expect(generation.service.run(generation.request())).rejects.toMatchObject({
      name: 'ParallelGenerationDisposedError',
      code: 'GENERATION_DISPOSED',
    })
    await disposing

    expect(generation.session.events).toEqual([])
    expect(generation.starts).toEqual([])
  })
})
