import { Context } from '@deepseek-ai/cordis'
import { agentEvents, type Agent } from '@deepseek-ai/dsh-agent'
import type {} from '@deepseek-ai/dsh-subagent'
import type {} from '@deepseek-ai/dsh-tools'
import { describe, expect, it, vi } from 'vitest'
import { ReasoningEffortId } from '@deepseek-ai/dsh-llm'
import SessionStore, { Session, SessionId } from '@deepseek-ai/dsh-session'
import { apply as applyAdaptiveScheduler, createAdaptiveScheduler } from '@han_05/dsh-adaptive-scheduler'
import { BudgetController } from '../src/budgets.ts'
import { mountRootScheduling, type ResolvedScheduleV1, type SchedulerResolver } from '../src/scheduling.ts'
import { createDelegateWorkerTool, HANDOFF_V1_JSON_SCHEMA, mountSingleWorkerMode, runWorker, SINGLE_WORKER_STARTUP_TIMEOUT_MS } from '../src/worker.ts'
import type { HandoffV1, OrchestratorConfig } from '../src/types.ts'

const workspaceRoot = '/workspace/ds-plugins'

const config: OrchestratorConfig = {
  workspaceRoot,
  mode: 'single-worker',
  worker: {
    provider: 'openai-codex',
    model: 'gpt-5.6-codex',
    reasoningEffort: 'high',
    maxTokens: 32_000,
  },
  budgets: {
    maxWorkers: 1,
    maxPluginToolActions: 2,
    toolTimeoutMs: 60_000,
  },
  verification: {
    commands: [{
      name: 'typecheck',
      executable: 'pnpm',
      fixedArgs: ['typecheck'],
      allowedArgs: 'none',
    }],
    timeoutMs: 60_000,
    maxOutputBytes: 4_096,
  },
}

const parallelConfig: OrchestratorConfig = {
  ...config,
  parallel: {
    maxParallelWorkers: 1,
    verification: { schemaVersion: 1, scope: 'dag', commands: [] },
    workerToolAllowlist: [],
    routeToolFilters: {},
  },
}

const adaptiveConfig: OrchestratorConfig = {
  ...config,
  worker: {
    ...config.worker,
    provider: 'profile-disabled',
    model: 'profile-disabled',
    maxTokens: 64_000,
  },
  scheduling: {
    allowInvalidDecisionFallback: false,
    allowedRoutes: [{ provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000, reasoningEffort: 'high' }],
    rootProfile: { coding: 50, reasoning: 50, toolUse: 50, repoContext: 50, risk: 50, difficulty: 50 },
    workerProfile: { coding: 80, reasoning: 70, toolUse: 60, repoContext: 80, risk: 30, difficulty: 60 },
    maxLatencyMs: 60_000,
    allowPaidFallback: false,
  },
}

const adaptiveRoutes = [
  { provider: 'provider-disabled', model: 'baseline-disabled', maxTokens: 32_000, modelFamily: 'deepseek' },
  { provider: 'provider-disabled', model: 'fallback-disabled', maxTokens: 32_000, modelFamily: 'deepseek' },
  { provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000, reasoningEffort: 'high', modelFamily: 'deepseek' },
] as const

const scopedAdaptiveConfig: OrchestratorConfig = {
  ...adaptiveConfig,
  scheduling: {
    ...adaptiveConfig.scheduling!,
    allowedRoutes: adaptiveRoutes,
  },
}

const scopedSchedulerConfig = {
  policyVersion: 'v0.3.0',
  catalog: [
    { alias: 'baseline', route: adaptiveRoutes[0], tier: 'baseline', taskTypes: ['code-fix', 'unknown'], toolFilter: ['targeted_verify'], paid: false, reliability: 70 },
    { alias: 'fallback', route: adaptiveRoutes[1], tier: 'fallback', taskTypes: ['code-fix', 'unknown'], toolFilter: ['targeted_verify'], paid: false, reliability: 80 },
    { alias: 'strong', route: adaptiveRoutes[2], tier: 'strong', taskTypes: ['code-fix', 'unknown'], toolFilter: ['targeted_verify'], paid: false, reliability: 100 },
  ],
  baselines: { 'code-fix': 'baseline', 'code-new': 'strong', research: 'baseline', summarize: 'baseline', review: 'strong', 'tool-heavy': 'strong', unknown: 'baseline' },
  stickyTtlMs: 900_000,
  idleTtlMs: 300_000,
  errorWindowMs: 300_000,
  cooldownMs: 60_000,
  escalationTtlMs: 600_000,
  maxEscalationsPerTask: 1,
  maxRounds: 2,
  historyWindowSize: 8,
  historyMinSamples: 3,
} as const

const validHandoff: HandoffV1 = {
  schemaVersion: 1,
  status: 'completed',
  summary: 'Updated the focused worker file.',
  changedFiles: ['packages/dsh-orchestrator/src/worker.ts'],
  decisions: ['Kept the delegation foreground-only.'],
  verification: [],
  blockers: [],
}

const failedVerification = {
  schemaVersion: 1 as const,
  commandName: 'typecheck',
  args: [],
  exitCode: 1,
  status: 'failed' as const,
  stdout: '',
  stderr: 'verification failed',
  truncated: false,
  durationMs: 1,
}

const profileResolvedSchedule = {
  request: {} as never,
  decision: {
    schemaVersion: 1,
    mode: 'single-worker',
    route: config.worker,
    workerCount: 1,
    source: 'profile-fallback',
    policyVersion: 'profile-fallback-v1',
  },
} as ResolvedScheduleV1

const noSchedulerResolver = { current: () => undefined }

function emptyBudgetSnapshot() {
  return {
    maxWorkers: 1,
    admittedWorkers: 0,
    maxPluginToolActions: 2,
    admittedPluginToolActions: 0,
    remainingWorkers: 1,
    remainingPluginToolActions: 2,
  }
}

interface FakeRequest {
  readonly prompt: readonly { readonly type: string; readonly text?: string }[]
  readonly parent: unknown
  readonly signal: AbortSignal
  readonly agentOptions?: {
    readonly provider?: string
    readonly model?: string
    readonly reasoningEffort?: string
    readonly maxTokens?: number
  }
  readonly outputSchema?: unknown
  readonly maxDepth?: number
  readonly toolFilter?: unknown
}

interface FakeResult {
  readonly stopReason: 'completed' | 'aborted' | 'error' | 'max-tokens' | 'refusal'
  readonly structured?: unknown
  readonly output: readonly unknown[]
  readonly diagnostic?: string
}

interface FakeRun {
  readonly id: ReturnType<typeof SessionId>
  readonly result: Promise<FakeResult>
  dispose(): Promise<void>
}

type Start = (request: FakeRequest) => Promise<FakeRun>

class FakeSubagents {
  readonly requests: FakeRequest[] = []
  readonly providers: string[] = []
  starts = 0

  constructor(private readonly startRun: Start) {}

  getProvider() {
    return {
      name: 'spawn',
      inheritsParentContext: false,
      capabilities: { agentOptions: true, outputSchema: true, depthLimit: true, toolFilter: true, persona: false },
      start: this.startRun,
    }
  }

  start(provider: string, request: FakeRequest): Promise<FakeRun> {
    this.starts += 1
    this.providers.push(provider)
    this.requests.push(request)
    return this.startRun(request)
  }
}

function publishedRun(
  result: Promise<FakeResult>,
  id = 'child-worker-session',
  disposeRun: () => Promise<void> = async () => undefined,
): { readonly run: FakeRun; readonly dispose: ReturnType<typeof vi.fn> } {
  const dispose = vi.fn(disposeRun)
  return { run: { id: SessionId(id), result, dispose }, dispose }
}

function rootSession(id = 'worker-root-session') {
  return Session.create(SessionId(id), undefined, {
    version: 0,
    id: SessionId(id),
    createdAt: 0,
    cwd: workspaceRoot,
    isSeeded: false,
  })
}

function parentFor(session = rootSession()) {
  const injected: unknown[] = []
  return {
    parent: {
      session,
      inject(message: unknown) { injected.push(message) },
    },
    injected,
  }
}

function workerOptions(overrides: Partial<Parameters<typeof runWorker>[0]> = {}) {
  const { parent, injected } = parentFor()
  const run = publishedRun(Promise.resolve({
    stopReason: 'completed',
    structured: validHandoff,
    output: [{ type: 'text', text: 'SECRET_TRANSCRIPT_MARKER' }],
  }))
  const subagents = new FakeSubagents(async () => run.run)
  return {
    options: {
      config,
      resolvedSchedule: profileResolvedSchedule,
      parent,
      task: 'Update the focused worker file.',
      allowedTools: ['read_file', 'write_file'],
      signal: new AbortController().signal,
      subagents,
      ...overrides,
    },
    injected,
    run,
    subagents,
  }
}

function injectedText(message: unknown): string {
  const candidate = message as { readonly content?: readonly { readonly type?: string; readonly text?: string }[] }
  const first = candidate.content?.[0]
  if (first?.type !== 'text' || typeof first.text !== 'string') throw new Error('expected one injected text message')
  return first.text
}

function toolRegistry() {
  const tools = new Map<string, { readonly name: string }>()
  return {
    register(tool: { readonly name: string }) {
      if (tools.has(tool.name)) throw new Error(`duplicate tool: ${tool.name}`)
      tools.set(tool.name, tool)
      return () => { tools.delete(tool.name) }
    },
    get(name: string) {
      return tools.get(name)
    },
  }
}

async function mountedSingleWorkerMode(
  modeConfig: OrchestratorConfig = config,
  schedulerResolver: SchedulerResolver = noSchedulerResolver,
) {
  const ctx = new Context()
  const sessionStore = await ctx.plugin(SessionStore)
  const tools = toolRegistry()
  ctx.provide('tools', tools as never)
  ctx.provide('subagents', new FakeSubagents(async () => {
    throw new Error('worker should not start while testing root route evidence')
  }) as never)
  const budgetRegistry = {
    forRootSession: () => ({
      admitPluginTool: () => ({ allowed: true as const }),
      admitWorker: () => ({ allowed: true as const }),
      snapshot: emptyBudgetSnapshot,
    }),
  }
  const fiber = await ctx.plugin(child => {
    mountRootScheduling(child, modeConfig, budgetRegistry, schedulerResolver)
    return mountSingleWorkerMode(child, modeConfig, budgetRegistry, schedulerResolver)
  })
  return { ctx, sessionStore, fiber }
}

describe('one-shot worker runtime', () => {
  it('starts one bounded foreground child, awaits its result, disposes it, and records only the validated handoff', async () => {
    let settle = (_result: FakeResult) => undefined
    const result = new Promise<FakeResult>(resolve => { settle = resolve })
    const run = publishedRun(result)
    const { parent, injected } = parentFor()
    const subagents = new FakeSubagents(async () => run.run)

    const running = runWorker({
      config,
      resolvedSchedule: profileResolvedSchedule,
      parent,
      task: 'Update the focused worker file.',
      allowedTools: ['read_file', 'write_file'],
      signal: new AbortController().signal,
      subagents,
    })

    await Promise.resolve()
    expect(subagents.providers).toEqual(['spawn'])
    expect(subagents.requests).toHaveLength(1)
    const [request] = subagents.requests
    expect(request?.maxDepth).toBe(1)
    expect(request?.agentOptions).toMatchObject({
      provider: config.worker.provider,
      model: config.worker.model,
      maxTokens: config.worker.maxTokens,
    })
    expect(request?.outputSchema).toEqual(HANDOFF_V1_JSON_SCHEMA)
    expect(request?.toolFilter).toEqual({ allow: ['read_file', 'write_file'] })
    const prompt = request?.prompt.map(block => block.text ?? '').join('\n') ?? ''
    expect(prompt).toContain('Update the focused worker file.')
    expect(prompt).toContain('HandoffV1')
    expect(prompt).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(run.dispose).not.toHaveBeenCalled()

    settle({
      stopReason: 'completed',
      structured: validHandoff,
      output: [{ type: 'text', text: 'SECRET_TRANSCRIPT_MARKER' }],
    })

    await expect(running).resolves.toEqual(validHandoff)
    expect(run.dispose).toHaveBeenCalledTimes(1)
    expect(parent.session.snapshotEvents().map(event => event.type)).toEqual([
      'dsh-plugin/worker-requested',
      'dsh-plugin/worker-finished',
    ])
    expect(parent.session.snapshotEvents()[1]).toMatchObject({
      data: { childSessionId: SessionId('child-worker-session'), handoff: validHandoff },
    })
    expect(injected).toEqual([])
  })

  it.each([
    ['aborted', 'blocked'],
    ['error', 'failed'],
    ['max-tokens', 'blocked'],
    ['refusal', 'blocked'],
  ] as const)('normalizes a %s child result without copying transcript or diagnostic data', async (stopReason, status) => {
    const run = publishedRun(Promise.resolve({
      stopReason,
      output: [{ type: 'text', text: 'SECRET_TRANSCRIPT_MARKER' }],
      diagnostic: 'SECRET_TRANSCRIPT_MARKER',
    }))
    const { parent, injected } = parentFor()
    const handoff = await runWorker({
      config,
      resolvedSchedule: profileResolvedSchedule,
      parent,
      task: 'Bounded task.',
      allowedTools: ['read_file'],
      signal: new AbortController().signal,
      subagents: new FakeSubagents(async () => run.run),
    })

    expect(handoff.status).toBe(status)
    expect(JSON.stringify(handoff)).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(parent.session.snapshotEvents().filter(event => event.type === 'dsh-plugin/worker-finished')).toHaveLength(1)
    expect(JSON.stringify(parent.session.snapshotEvents())).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(injected).toEqual([])
    expect(run.dispose).toHaveBeenCalledTimes(1)
  })

  it.each([
    ['invalid structured output', { schemaVersion: 2 }, [{ type: 'text', text: 'SECRET_TRANSCRIPT_MARKER' }]],
    ['raw-only output', undefined, [{ type: 'text', text: 'SECRET_TRANSCRIPT_MARKER' }]],
  ] as const)('returns a scrubbed failed handoff for %s', async (_label, structured, output) => {
    const run = publishedRun(Promise.resolve({ stopReason: 'completed', structured, output }))
    const { options, injected } = workerOptions({ subagents: new FakeSubagents(async () => run.run) })

    const handoff = await runWorker(options)

    expect(handoff).toMatchObject({ status: 'failed', changedFiles: [], verification: [] })
    expect(JSON.stringify(handoff)).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(injected).toEqual([])
    expect(run.dispose).toHaveBeenCalledTimes(1)
  })

  it('normalizes a disposal rejection into a scrubbed failed handoff and appends completion evidence', async () => {
    const run = publishedRun(
      Promise.resolve({ stopReason: 'completed', structured: validHandoff, output: [] }),
      'child-worker-dispose-rejection',
      async () => { throw new Error('SECRET_TRANSCRIPT_MARKER') },
    )
    const { parent, injected } = parentFor()

    const handoff = await runWorker({
      config,
      resolvedSchedule: profileResolvedSchedule,
      parent,
      task: 'Bounded task.',
      allowedTools: ['read_file'],
      signal: new AbortController().signal,
      subagents: new FakeSubagents(async () => run.run),
    })

    expect(handoff).toMatchObject({ status: 'failed', changedFiles: [], verification: [] })
    expect(JSON.stringify(handoff)).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(parent.session.snapshotEvents().map(event => event.type)).toEqual([
      'dsh-plugin/worker-requested',
      'dsh-plugin/worker-finished',
    ])
    expect(JSON.stringify(parent.session.snapshotEvents())).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(injected).toEqual([])
    expect(run.dispose).toHaveBeenCalledTimes(1)
  })

  it('normalizes start infrastructure rejection without publishing a child event or injection', async () => {
    const { parent, injected } = parentFor()
    const subagents = new FakeSubagents(async () => { throw new Error('SECRET_TRANSCRIPT_MARKER') })

    const handoff = await runWorker({
      config,
      resolvedSchedule: profileResolvedSchedule,
      parent,
      task: 'Bounded task.',
      allowedTools: ['read_file'],
      signal: new AbortController().signal,
      subagents,
    })

    expect(handoff).toMatchObject({ status: 'failed', changedFiles: [], verification: [] })
    expect(JSON.stringify(handoff)).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(parent.session.snapshotEvents().map(event => event.type)).toEqual(['dsh-plugin/worker-requested'])
    expect(injected).toEqual([])
  })

  it('rejects a route override when the provider lacks agentOptions before publication or start', async () => {
    const { options, subagents } = workerOptions()
    subagents.getProvider = () => ({
      name: 'spawn',
      inheritsParentContext: false,
      capabilities: { agentOptions: false, outputSchema: true, depthLimit: true, toolFilter: true, persona: false },
      start: async () => { throw new Error('must not start') },
    })

    await expect(runWorker(options)).resolves.toMatchObject({ status: 'failed' })
    expect(subagents.starts).toBe(0)
    expect(options.parent.session.snapshotEvents()).toEqual([])
  })

  it('returns a blocked handoff before publication when the caller is already cancelled', async () => {
    const controller = new AbortController()
    controller.abort(new Error('SECRET_TRANSCRIPT_MARKER'))
    const { parent, injected } = parentFor()
    const subagents = new FakeSubagents(async () => { throw new Error('must not start') })

    const handoff = await runWorker({
      config,
      resolvedSchedule: profileResolvedSchedule,
      parent,
      task: 'Bounded task.',
      allowedTools: ['read_file'],
      signal: controller.signal,
      subagents,
    })

    expect(handoff).toMatchObject({ status: 'blocked', changedFiles: [], verification: [] })
    expect(JSON.stringify(handoff)).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(subagents.requests).toEqual([])
    expect(parent.session.snapshotEvents()).toEqual([])
    expect(injected).toEqual([])
  })

  it('records the published child handoff but does not inject after caller cancellation', async () => {
    const controller = new AbortController()
    let settle = (_result: FakeResult) => undefined
    const result = new Promise<FakeResult>(resolve => { settle = resolve })
    const run = publishedRun(result)
    const { parent, injected } = parentFor()
    const subagents = new FakeSubagents(async () => run.run)
    const running = runWorker({
      config,
      resolvedSchedule: profileResolvedSchedule,
      parent,
      task: 'Bounded task.',
      allowedTools: ['read_file'],
      signal: controller.signal,
      subagents,
    })

    await Promise.resolve()
    controller.abort(new Error('SECRET_TRANSCRIPT_MARKER'))
    settle({
      stopReason: 'aborted',
      output: [{ type: 'text', text: 'SECRET_TRANSCRIPT_MARKER' }],
      diagnostic: 'SECRET_TRANSCRIPT_MARKER',
    })

    await expect(running).resolves.toMatchObject({ status: 'blocked' })
    expect(parent.session.snapshotEvents().map(event => event.type)).toEqual([
      'dsh-plugin/worker-requested',
      'dsh-plugin/worker-finished',
    ])
    expect(JSON.stringify(parent.session.snapshotEvents())).not.toContain('SECRET_TRANSCRIPT_MARKER')
    expect(injected).toEqual([])
    expect(run.dispose).toHaveBeenCalledTimes(1)
  })
})

describe('delegate_worker tool', () => {
  it.each([8, 16])('admits and runs one legacy delegation under a parallel cumulative budget of %i', async maxWorkers => {
    const parallelConfig: OrchestratorConfig = {
      ...config,
      budgets: { ...config.budgets, maxWorkers },
      parallel: {
        maxParallelWorkers: Math.min(8, maxWorkers),
        verification: { schemaVersion: 1, scope: 'dag', commands: [] },
        workerToolAllowlist: ['read_file', 'write_file'],
        routeToolFilters: {},
      },
    }
    const { parent } = parentFor(rootSession(`worker-parallel-root-${maxWorkers}`))
    const run = publishedRun(Promise.resolve({ stopReason: 'completed', structured: validHandoff, output: [] }))
    const subagents = new FakeSubagents(async () => run.run)
    const controller = new BudgetController(parallelConfig.budgets, () => undefined)
    const tool = createDelegateWorkerTool({
      config: parallelConfig,
      subagents,
      budgetRegistry: { forRootSession: () => controller },
      schedulerResolver: noSchedulerResolver,
    })

    await expect(tool.execute(
      { task: 'Bounded task.', allowedTools: ['read_file'] },
      { signal: new AbortController().signal, agent: parent, deferContext: () => undefined } as never,
    )).resolves.toEqual(validHandoff)

    expect(subagents.starts).toBe(1)
    expect(controller.snapshot()).toMatchObject({
      maxWorkers,
      admittedWorkers: 1,
      admittedPluginToolActions: 1,
    })
    expect(run.dispose).toHaveBeenCalledTimes(1)
  })

  it('keeps root sticky and cooldown state when its worker invocation completes', async () => {
    const { parent } = parentFor(rootSession('shared-root-worker-scope'))
    const rootId = String(parent.session.id)
    let now = 1_000
    const scheduler = createAdaptiveScheduler({
      ...scopedSchedulerConfig,
      idleTtlMs: 100,
      cooldownMs: 1_000,
    }, { generation: 'scope-test', now: () => now })
    const rootRequest = {
      schemaVersion: 1 as const,
      target: 'root' as const,
      taskId: rootId,
      objective: 'Fix parser failure',
      profile: scopedAdaptiveConfig.scheduling!.rootProfile,
      constraints: {
        maxWorkers: 0 as const,
        maxOutputTokens: 64_000,
        maxLatencyMs: 60_000,
        allowPaidFallback: false,
        requiredTools: ['targeted_verify'],
      },
    }
    const signal = new AbortController().signal
    await scheduler.schedule(rootRequest, emptyBudgetSnapshot(), signal)
    scheduler.recordFailure({ requestId: rootId, code: 'TIMEOUT' })
    await expect(scheduler.schedule(rootRequest, emptyBudgetSnapshot(), signal)).resolves.toMatchObject({
      route: { model: 'fallback-disabled' },
    })
    const run = publishedRun(Promise.resolve({ stopReason: 'completed', structured: validHandoff, output: [] }))
    const tool = createDelegateWorkerTool({
      config: scopedAdaptiveConfig,
      subagents: new FakeSubagents(async () => run.run),
      budgetRegistry: { forRootSession: () => new BudgetController(scopedAdaptiveConfig.budgets, () => undefined) },
      schedulerResolver: { current: () => scheduler },
    })

    await tool.execute(
      { task: 'Fix parser failure', allowedTools: ['targeted_verify'] },
      { signal, agent: parent, deferContext: () => undefined } as never,
    )

    await expect(scheduler.schedule(rootRequest, emptyBudgetSnapshot(), signal)).resolves.toMatchObject({
      explanationCode: 'STICKY_ROUTE',
      route: { model: 'fallback-disabled' },
    })
    now += 101
    await expect(scheduler.schedule(rootRequest, emptyBudgetSnapshot(), signal)).resolves.toMatchObject({
      explanationCode: 'TRANSIENT_FALLBACK',
      route: { model: 'fallback-disabled' },
    })
  })

  it('records failed worker feedback in history before in-process child disposal', async () => {
    const ctx = new Context()
    const historyConfig = {
      ...scopedSchedulerConfig,
      catalog: [scopedSchedulerConfig.catalog[1], scopedSchedulerConfig.catalog[0], scopedSchedulerConfig.catalog[2]],
      historyMinSamples: 1,
    }
    const schedulerFiber = await ctx.plugin(applyAdaptiveScheduler, historyConfig)
    const scheduler = ctx.get('adaptiveScheduler')!
    const { parent } = parentFor(rootSession('history-worker-root'))
    const child = Session.create(SessionId('history-worker-child'), undefined, {
      version: 0,
      id: SessionId('history-worker-child'),
      createdAt: 0,
      cwd: workspaceRoot,
      parentSession: parent.session.id,
      isSeeded: false,
    })
    const run = publishedRun(
      Promise.resolve({ stopReason: 'error', output: [] }),
      String(child.id),
      async () => { ctx.emit('session/disposed' as never, child as never) },
    )
    const tool = createDelegateWorkerTool({
      config: scopedAdaptiveConfig,
      subagents: new FakeSubagents(async () => run.run),
      budgetRegistry: { forRootSession: () => new BudgetController(scopedAdaptiveConfig.budgets, () => undefined) },
      schedulerResolver: { current: () => scheduler },
    })

    await expect(tool.execute(
      { task: 'Fix parser failure', allowedTools: ['targeted_verify'] },
      { signal: new AbortController().signal, agent: parent, deferContext: () => undefined } as never,
    )).resolves.toMatchObject({ status: 'failed' })

    await expect(scheduler.schedule({
      schemaVersion: 1,
      target: 'worker',
      taskId: 'history-probe',
      objective: 'Fix parser failure',
      profile: scopedAdaptiveConfig.scheduling!.workerProfile,
      constraints: {
        maxWorkers: 1,
        maxOutputTokens: 64_000,
        maxLatencyMs: 60_000,
        allowPaidFallback: false,
        requiredTools: ['targeted_verify'],
      },
    }, emptyBudgetSnapshot(), new AbortController().signal)).resolves.toMatchObject({
      explanationCode: 'HISTORY_ORDERED_CANDIDATE',
      route: { model: 'fallback-disabled' },
    })
    await schedulerFiber.dispose()
  })

  it('rejects an invalid scheduler decision without consuming either budget', async () => {
    const controller = new BudgetController(config.budgets, () => undefined)
    const { parent } = parentFor()
    const subagents = new FakeSubagents(async () => { throw new Error('worker must not start') })
    const tool = createDelegateWorkerTool({
      config: adaptiveConfig,
      subagents,
      budgetRegistry: { forRootSession: () => controller },
      schedulerResolver: {
        current: () => ({
          schedule: async () => ({
            schemaVersion: 1,
            mode: 'single-worker' as const,
            route: { provider: 'unconfigured', model: 'bad', maxTokens: 32_000 },
            workerCount: 1 as const,
            source: 'scheduler' as const,
            policyVersion: 'bad',
          }),
        }),
      },
    })
    const before = controller.snapshot()

    await expect(tool.execute(
      { task: 'Fix parser', allowedTools: ['targeted_verify'] },
      { signal: new AbortController().signal, agent: parent } as never,
    )).rejects.toThrow('SCHEDULE_DECISION_INVALID')
    expect(controller.snapshot()).toEqual(before)
    expect(subagents.starts).toBe(0)
    expect(parent.session.snapshotEvents()).toEqual([])
  })

  it('abandons a schedule that resolves after cancellation before changing budget or publishing events', async () => {
    const controller = new BudgetController(adaptiveConfig.budgets, () => undefined)
    const before = controller.snapshot()
    const { parent } = parentFor()
    const subagents = new FakeSubagents(async () => { throw new Error('worker must not start') })
    const cancellation = new Error('cancelled while scheduling')
    const aborted = new AbortController()
    let release: (() => void) | undefined
    const completed: string[] = []
    let scheduledTaskId = ''
    const scheduler = {
      schedule: async (request: { readonly taskId: string }) => new Promise(resolve => {
        scheduledTaskId = request.taskId
        release = () => resolve({
          schemaVersion: 1 as const,
          mode: 'single-worker' as const,
          route: { provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000, reasoningEffort: 'high' as const },
          workerCount: 1 as const,
          source: 'scheduler' as const,
          policyVersion: 'v0.3.0',
        })
      }),
      complete: (requestId: string) => { completed.push(requestId) },
    }
    const tool = createDelegateWorkerTool({
      config: adaptiveConfig,
      subagents,
      budgetRegistry: { forRootSession: () => controller },
      schedulerResolver: { current: () => scheduler },
    })

    const running = tool.execute(
      { task: 'Fix parser', allowedTools: ['targeted_verify'] },
      { signal: aborted.signal, agent: parent } as never,
    )
    await Promise.resolve()
    aborted.abort(cancellation)
    release?.()

    await expect(running).rejects.toBe(cancellation)
    expect(controller.snapshot()).toEqual(before)
    expect(parent.session.snapshotEvents()).toEqual([])
    expect(subagents.starts).toBe(0)
    expect(scheduledTaskId).toMatch(/^worker:[0-9a-f-]{36}$/)
    expect(scheduledTaskId).not.toBe(String(parent.session.id))
    expect(completed).toEqual([scheduledTaskId])
  })

  it.each([
    ['completed', 'completed', validHandoff, 'completed', 'completed'],
    ['verification-failed', 'completed', { ...validHandoff, summary: '[verification: failed] Updated the focused worker file.', verification: [failedVerification] }, 'verification-failed', 'completed'],
    ['blocked', 'aborted', undefined, 'blocked', 'blocked'],
    ['failed', 'error', undefined, 'failed', 'failed'],
  ] as const)('records the selected route before the worker and reports %s feedback', async (_label, stopReason, structured, expectedOutcome, expectedStatus) => {
    const { parent } = parentFor()
    const run = publishedRun(Promise.resolve({ stopReason, structured, output: [] }))
    const subagents = new FakeSubagents(async () => run.run)
    const controller = new BudgetController(adaptiveConfig.budgets, () => undefined)
    const observed: unknown[] = []
    const completed: string[] = []
    const scheduler = {
      schedule: async (request: unknown, budget: unknown) => {
        observed.push({ request, budget })
        return {
          schemaVersion: 1 as const,
          mode: 'single-worker' as const,
          route: { provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000, reasoningEffort: 'high' },
          workerCount: 1 as const,
          source: 'scheduler' as const,
          policyVersion: 'v0.3.0',
        }
      },
      observe: (feedback: unknown) => { observed.push(feedback) },
      complete: (requestId: string) => { completed.push(requestId) },
    }
    const tool = createDelegateWorkerTool({
      config: adaptiveConfig,
      subagents,
      budgetRegistry: { forRootSession: () => controller },
      schedulerResolver: { current: () => scheduler },
    })
    const deferContext = vi.fn()

    await expect(tool.execute(
      { task: 'Fix parser', allowedTools: ['targeted_verify'] },
      { signal: new AbortController().signal, agent: parent, deferContext } as never,
    )).resolves.toMatchObject({ status: expectedStatus })

    expect(parent.session.snapshotEvents().map(event => event.type)).toEqual([
      'dsh-plugin/schedule-selected',
      'dsh-plugin/worker-requested',
      'dsh-plugin/worker-finished',
    ])
    expect(parent.session.snapshotEvents()[1]).toMatchObject({
      data: { provider: 'provider-disabled', model: 'strong-disabled', reasoningEffort: 'high', maxTokens: 64_000 },
    })
    expect(subagents.requests[0]).toMatchObject({
      agentOptions: { provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000 },
      maxDepth: 1,
      toolFilter: { allow: ['targeted_verify'] },
    })
    expect(subagents.requests[0]?.agentOptions).toMatchObject({ reasoningEffort: 'high' })
    expect(observed).toHaveLength(2)
    const scheduledTaskId = (observed[0] as { request: { taskId: string } }).request.taskId
    expect(scheduledTaskId).toMatch(/^worker:[0-9a-f-]{36}$/)
    expect(scheduledTaskId).not.toBe(String(parent.session.id))
    expect(observed).toEqual([
      expect.objectContaining({
        request: expect.objectContaining({
          taskId: scheduledTaskId,
          affinity: { workerId: `${parent.session.id}:worker:1` },
        }),
        budget: expect.objectContaining({ admittedWorkers: 0, admittedPluginToolActions: 0 }),
      }),
      expect.objectContaining({
        schemaVersion: 1,
        requestId: scheduledTaskId,
        outcome: expectedOutcome,
        handoff: expect.objectContaining({ status: expectedStatus }),
      }),
    ])
    expect(completed).toEqual([scheduledTaskId])
  })

  it('reports a budget rejection to the scheduler without recording a selection or starting a worker', async () => {
    const { parent } = parentFor()
    const controller = new BudgetController({ ...adaptiveConfig.budgets, maxPluginToolActions: 0 }, () => undefined)
    const subagents = new FakeSubagents(async () => { throw new Error('worker must not start') })
    const observed: unknown[] = []
    const completed: string[] = []
    let scheduledTaskId = ''
    const scheduler = {
      schedule: async (request: { readonly taskId: string }) => {
        scheduledTaskId = request.taskId
        return {
          schemaVersion: 1 as const,
          mode: 'single-worker' as const,
          route: { provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000, reasoningEffort: 'high' },
          workerCount: 1 as const,
          source: 'scheduler' as const,
          policyVersion: 'v0.3.0',
        }
      },
      observe: (feedback: unknown) => { observed.push(feedback) },
      complete: (requestId: string) => { completed.push(requestId) },
    }
    const tool = createDelegateWorkerTool({
      config: adaptiveConfig,
      subagents,
      budgetRegistry: { forRootSession: () => controller },
      schedulerResolver: { current: () => scheduler },
    })

    await expect(tool.execute(
      { task: 'Fix parser', allowedTools: ['targeted_verify'] },
      { signal: new AbortController().signal, agent: parent } as never,
    )).rejects.toThrow('PLUGIN_TOOL_LIMIT')

    expect(observed).toEqual([{
      schemaVersion: 1,
      requestId: scheduledTaskId,
      outcome: 'budget-rejected',
      budgetRejection: { code: 'PLUGIN_TOOL_LIMIT', limit: 0, observed: 1 },
    }])
    expect(parent.session.snapshotEvents()).toEqual([])
    expect(subagents.starts).toBe(0)
    expect(scheduledTaskId).toMatch(/^worker:[0-9a-f-]{36}$/)
    expect(scheduledTaskId).not.toBe(String(parent.session.id))
    expect(completed).toEqual([scheduledTaskId])
  })

  it('admits one delegation before starting, binds a scrubbed handoff to its tool result, and rejects the second without invoking the provider', async () => {
    const { parent, injected } = parentFor()
    const first = publishedRun(Promise.resolve({ stopReason: 'completed', structured: validHandoff, output: [] }))
    const subagents = new FakeSubagents(async () => first.run)
    const admitPluginTool = vi.fn(() => ({ allowed: true as const }))
    const admitWorker = vi.fn()
      .mockReturnValueOnce({ allowed: true as const })
      .mockReturnValueOnce({ allowed: false as const, code: 'WORKER_LIMIT', limit: 1, observed: 2 })
    const tool = createDelegateWorkerTool({
      config,
      subagents,
      budgetRegistry: { forRootSession: () => ({ admitPluginTool, admitWorker, snapshot: emptyBudgetSnapshot }) },
      schedulerResolver: noSchedulerResolver,
    })
    const deferContext = vi.fn()

    await expect(tool.execute(
      { task: 'Bounded task.', allowedTools: ['read_file'] },
      { signal: new AbortController().signal, agent: parent, deferContext } as never,
    )).resolves.toEqual(validHandoff)
    await expect(tool.execute(
      { task: 'Different bounded task.', allowedTools: ['read_file'] },
      { signal: new AbortController().signal, agent: parent } as never,
    )).rejects.toThrow(/WORKER_LIMIT/)

    expect(admitPluginTool).toHaveBeenCalledTimes(2)
    expect(admitWorker).toHaveBeenCalledTimes(2)
    expect(subagents.requests).toHaveLength(1)
    expect(first.dispose).toHaveBeenCalledTimes(1)
    expect(injected).toEqual([])
    expect(deferContext).toHaveBeenCalledTimes(1)
    const [context] = deferContext.mock.calls[0] ?? []
    expect(context).toMatchObject({
      source: {
        kind: 'plugin',
        plugin: 'ds-orchestrator',
        form: 'notice',
        summary: validHandoff.summary,
      },
    })
    const projection = JSON.parse(injectedText(context))
    expect(projection).toEqual({
      status: validHandoff.status,
      summary: validHandoff.summary,
      changedFiles: validHandoff.changedFiles,
      decisions: validHandoff.decisions,
      verification: validHandoff.verification,
      blockers: validHandoff.blockers,
    })
    expect(injectedText(context)).not.toContain('SECRET_TRANSCRIPT_MARKER')
  })

  it('validates tool input and pre-aborted cancellation before charging either budget', async () => {
    const { parent } = parentFor()
    const subagents = new FakeSubagents(async () => { throw new Error('must not start') })
    const admitPluginTool = vi.fn(() => ({ allowed: true as const }))
    const admitWorker = vi.fn(() => ({ allowed: true as const }))
    const tool = createDelegateWorkerTool({
      config,
      subagents,
      budgetRegistry: { forRootSession: () => ({ admitPluginTool, admitWorker, snapshot: emptyBudgetSnapshot }) },
      schedulerResolver: noSchedulerResolver,
    })
    const aborted = new AbortController()
    const cancellation = new Error('already cancelled')
    aborted.abort(cancellation)

    await expect(tool.execute(
      { task: '', allowedTools: ['read_file'] },
      { signal: new AbortController().signal, agent: parent } as never,
    )).rejects.toThrow(/task/i)
    await expect(tool.execute(
      { task: 'Bounded task.', allowedTools: ['read_file', 'read_file'] },
      { signal: new AbortController().signal, agent: parent } as never,
    )).rejects.toThrow(/allowedTools/i)
    await expect(tool.execute(
      { task: 'Bounded task.', allowedTools: ['read_file'], unexpected: true },
      { signal: new AbortController().signal, agent: parent } as never,
    )).rejects.toThrow(/unknown|unsupported/i)
    await expect(tool.execute(
      { task: 'Bounded task.', allowedTools: ['read_file'] },
      { signal: aborted.signal, agent: parent } as never,
    )).rejects.toBe(cancellation)

    expect(admitPluginTool).not.toHaveBeenCalled()
    expect(admitWorker).not.toHaveBeenCalled()
    expect(subagents.requests).toEqual([])
  })
})

describe('single-worker service lifecycle', () => {
  it('waits for a late subagents service before the fixed startup deadline', async () => {
    vi.useFakeTimers()
    const ctx = new Context()
    const tools = toolRegistry()
    ctx.provide('tools', tools as never)
    const budgetRegistry = {
      forRootSession: () => ({
        admitPluginTool: () => ({ allowed: true as const }),
        admitWorker: () => ({ allowed: true as const }),
        snapshot: emptyBudgetSnapshot,
      }),
    }
    const fiber = ctx.plugin(child => mountSingleWorkerMode(child, config, budgetRegistry, noSchedulerResolver))

    expect(tools.get('delegate_worker')).toBeUndefined()
    await vi.advanceTimersByTimeAsync(SINGLE_WORKER_STARTUP_TIMEOUT_MS - 1)
    ctx.provide('subagents', new FakeSubagents(async () => {
      throw new Error('worker should not start in this lifecycle test')
    }) as never)
    await fiber
    expect(tools.get('delegate_worker')).toBeDefined()
    expect((ctx as unknown as { get(name: string): unknown }).get('parallelExecution')).toBeUndefined()
    expect(tools.get('context_repo_map')).toBeUndefined()
    expect(tools.get('context_symbol_query')).toBeUndefined()
    expect(tools.get('context_expand_source')).toBeUndefined()

    await fiber.dispose()
    expect(tools.get('delegate_worker')).toBeUndefined()
    vi.useRealTimers()
  })

  it('mounts the internal parallel service only inside a parallel-enabled subagents generation', async () => {
    const ctx = new Context()
    const tools = toolRegistry()
    ctx.provide('tools', tools as never)
    const firstSubagents = await ctx.plugin(child => child.provide('subagents', new FakeSubagents(async () => {
      throw new Error('worker should not start in this lifecycle test')
    }) as never))
    const budgetRegistry = {
      forRootSession: () => ({
        admitPluginTool: () => ({ allowed: true as const }),
        admitWorker: () => ({ allowed: true as const }),
        admitFanout: () => ({ allowed: true as const }),
        snapshot: emptyBudgetSnapshot,
      }),
    }

    const fiber = await ctx.plugin(child => mountSingleWorkerMode(
      child,
      parallelConfig,
      budgetRegistry,
      noSchedulerResolver,
    ))

    const firstService = (ctx as unknown as { get(name: string): unknown }).get('parallelExecution')
    expect(firstService).toMatchObject({
      run: expect.any(Function),
    })
    expect(tools.get('delegate_worker')).toBeDefined()
    expect(tools.get('parallel_worker')).toBeUndefined()

    await firstSubagents.dispose()
    expect((ctx as unknown as { get(name: string): unknown }).get('parallelExecution')).toBeUndefined()
    const nextSubagents = await ctx.plugin(child => child.provide('subagents', new FakeSubagents(async () => {
      throw new Error('replacement worker should not start in this lifecycle test')
    }) as never))
    await vi.waitFor(() => {
      expect((ctx as unknown as { get(name: string): unknown }).get('parallelExecution')).toMatchObject({
        run: expect.any(Function),
      })
    })
    expect((ctx as unknown as { get(name: string): unknown }).get('parallelExecution')).not.toBe(firstService)

    await fiber.dispose()
    expect((ctx as unknown as { get(name: string): unknown }).get('parallelExecution')).toBeUndefined()
    await nextSubagents.dispose()
  })

  it('rejects Single Worker startup after the fixed missing-subagents deadline', async () => {
    vi.useFakeTimers()
    const ctx = new Context()
    ctx.provide('tools', toolRegistry() as never)
    const fiber = ctx.plugin(child => mountSingleWorkerMode(child, config, {
      forRootSession: () => ({ admitPluginTool: () => ({ allowed: true as const }), admitWorker: () => ({ allowed: true as const }), snapshot: emptyBudgetSnapshot }),
    }, noSchedulerResolver))
    await vi.advanceTimersByTimeAsync(SINGLE_WORKER_STARTUP_TIMEOUT_MS)
    await expect(fiber).rejects.toThrow(/single-worker.*subagents.*timeout/i)
    vi.useRealTimers()
  })

  it('cancels a pending Single Worker startup on disposal without mounting late', async () => {
    vi.useFakeTimers()
    const ctx = new Context()
    const tools = toolRegistry()
    ctx.provide('tools', tools as never)
    const fiber = ctx.plugin(child => mountSingleWorkerMode(child, config, {
      forRootSession: () => ({ admitPluginTool: () => ({ allowed: true as const }), admitWorker: () => ({ allowed: true as const }), snapshot: emptyBudgetSnapshot }),
    }, noSchedulerResolver))
    await fiber.dispose()
    await vi.advanceTimersByTimeAsync(SINGLE_WORKER_STARTUP_TIMEOUT_MS)
    ctx.provide('subagents', new FakeSubagents(async () => { throw new Error('must not mount') }) as never)
    expect(tools.get('delegate_worker')).toBeUndefined()
    vi.useRealTimers()
  })

  it('routes a Single Worker root request before its actual header and ignores malformed actual routes', async () => {
    const scheduler = {
      schedule: async () => ({
        schemaVersion: 1 as const,
        mode: 'direct' as const,
        route: {
          provider: 'provider-disabled',
          model: 'strong-disabled',
          maxTokens: 64_000,
          reasoningEffort: 'high',
        },
        workerCount: 0 as const,
        source: 'scheduler' as const,
        policyVersion: 'v0.3.0',
      }),
    }
    const mounted = await mountedSingleWorkerMode(adaptiveConfig, { current: () => scheduler })
    const root = mounted.ctx.sessions.create(SessionId('worker-scheduled-root'), {
      meta: { cwd: workspaceRoot },
    })
    const agent = { id: root.id, session: root } as Agent

    const result = await agentEvents(mounted.ctx, agent).waterfall(
      'agent/request',
      { turn: 1, step: 1, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled', temperature: 0.2, stop: ['<stop>'] }),
    )
    expect(result).toMatchObject({
      provider: 'provider-disabled',
      model: 'strong-disabled',
      maxTokens: 64_000,
      reasoningEffort: ReasoningEffortId('high'),
      temperature: 0.2,
      stop: ['<stop>'],
    })
    expect(root.snapshotEvents().map(event => event.type)).toEqual(['dsh-plugin/schedule-selected'])

    root.append('request/header', { header: { config: result }, reason: 'initial' })
    const missingModel = mounted.ctx.sessions.create(SessionId('worker-scheduled-missing-model'), {
      meta: { cwd: workspaceRoot },
    })
    missingModel.append('request/header', {
      header: { config: { provider: 'provider-disabled' } },
      reason: 'initial',
    } as never)
    const malformedConfig = mounted.ctx.sessions.create(SessionId('worker-scheduled-malformed'), {
      meta: { cwd: workspaceRoot },
    })
    malformedConfig.append('request/header', {
      header: { config: 'not-a-route' },
      reason: 'initial',
    } as never)
    await Promise.resolve()

    expect(root.snapshotEvents().map(event => event.type)).toEqual([
      'dsh-plugin/schedule-selected',
      'request/header',
      'dsh-plugin/run-started',
    ])
    expect(root.snapshotEvents()[2]).toMatchObject({ data: { provider: 'provider-disabled', model: 'strong-disabled' } })
    expect(missingModel.snapshotEvents().filter(event => event.type === 'dsh-plugin/run-started')).toEqual([])
    expect(malformedConfig.snapshotEvents().filter(event => event.type === 'dsh-plugin/run-started')).toEqual([])

    await mounted.fiber.dispose()
    await mounted.sessionStore.dispose()
  })

  it('records the actual root request route and ignores malformed route snapshots', async () => {
    const mounted = await mountedSingleWorkerMode()
    const routed = mounted.ctx.sessions.create(SessionId('worker-resolved-route'), {
      meta: { cwd: workspaceRoot },
    })
    routed.append('request/header', {
      header: { config: { provider: 'deepseek', model: 'deepseek-reasoner' } },
      reason: 'initial',
    })
    const missingModel = mounted.ctx.sessions.create(SessionId('worker-missing-route-field'), {
      meta: { cwd: workspaceRoot },
    })
    missingModel.append('request/header', {
      header: { config: { provider: 'deepseek' } },
      reason: 'initial',
    } as never)
    const malformedConfig = mounted.ctx.sessions.create(SessionId('worker-malformed-route-shape'), {
      meta: { cwd: workspaceRoot },
    })
    malformedConfig.append('request/header', {
      header: { config: 'not-a-route' },
      reason: 'initial',
    } as never)
    await Promise.resolve()

    expect(routed.snapshotEvents().filter(event => event.type === 'dsh-plugin/run-started')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({ mode: 'single-worker', provider: 'deepseek', model: 'deepseek-reasoner' }),
      }),
    ])
    expect(missingModel.snapshotEvents().filter(event => event.type === 'dsh-plugin/run-started')).toEqual([])
    expect(malformedConfig.snapshotEvents().filter(event => event.type === 'dsh-plugin/run-started')).toEqual([])

    await mounted.fiber.dispose()
    await mounted.sessionStore.dispose()
  })
})
