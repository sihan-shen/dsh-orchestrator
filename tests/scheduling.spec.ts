import { describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { agentEvents, type Agent } from '@deepseek-ai/dsh-agent'
import { ReasoningEffortId } from '@deepseek-ai/dsh-llm'
import { Session, SessionId, type SessionEvent } from '@deepseek-ai/dsh-session'
import { createAdaptiveScheduler } from '@ds-plugins/dsh-adaptive-scheduler'
import {
  buildCapabilityRequest,
  fixedProfileSchedule,
  mountAdaptiveSchedulerResolver,
  mountRootScheduling,
  resolveSchedule,
  restoreScheduleSelected,
  scheduleSelectedFrom,
  validateScheduleDecisionForConfig,
  type ResolveScheduleInput,
} from '../src/scheduling.ts'
import type { OrchestratorConfig } from '../src/types.ts'

const routes = [
  { provider: 'provider-disabled', model: 'baseline-disabled', maxTokens: 32_000 },
  { provider: 'provider-disabled', model: 'fallback-disabled', maxTokens: 32_000 },
  { provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000, reasoningEffort: 'high' },
  {
    provider: 'provider-disabled',
    model: 'metadata-disabled',
    maxTokens: 32_000,
    reasoningEffort: 'high',
    promptProfile: 'coding-v1',
    modelFamily: 'deepseek',
  },
] as const

const scheduling = {
  allowInvalidDecisionFallback: false,
  allowedRoutes: routes,
  rootProfile: { coding: 50, reasoning: 50, toolUse: 50, repoContext: 50, risk: 50, difficulty: 50 },
  workerProfile: { coding: 80, reasoning: 70, toolUse: 60, repoContext: 80, risk: 30, difficulty: 60 },
  maxLatencyMs: 60_000,
  allowPaidFallback: false,
} as const

const config: OrchestratorConfig = {
  workspaceRoot: '.',
  mode: 'single-worker',
  worker: { provider: 'provider-disabled', model: 'baseline-disabled', maxTokens: 64_000 },
  budgets: { maxWorkers: 1, maxPluginToolActions: 24, toolTimeoutMs: 60_000 },
  verification: { commands: [], timeoutMs: 60_000, maxOutputBytes: 65_536 },
  scheduling,
}

function parallelWorkerConfig(maxWorkers: 8 | 16): OrchestratorConfig {
  return {
    ...config,
    budgets: { ...config.budgets, maxWorkers },
    parallel: {
      maxParallelWorkers: Math.min(8, maxWorkers),
      verification: { schemaVersion: 1, scope: 'dag', commands: [] },
      workerToolAllowlist: ['read_file', 'write_file'],
      routeToolFilters: {},
    },
  }
}

const workerInput: ResolveScheduleInput = {
  target: 'worker',
  taskId: 'session-1',
  objective: 'Fix the scheduling adapter',
  requiredTools: ['targeted_verify'],
  affinity: { workerId: 'session-1:worker:1' },
  budget: {
    maxWorkers: 1,
    admittedWorkers: 0,
    maxPluginToolActions: 24,
    admittedPluginToolActions: 0,
    remainingWorkers: 1,
    remainingPluginToolActions: 24,
  },
  signal: new AbortController().signal,
}

const validDecision = {
  schemaVersion: 1,
  mode: 'single-worker',
  route: routes[0],
  workerCount: 1,
  source: 'scheduler',
  policyVersion: 'v0.3.0',
  explanationCode: 'TASK_BASELINE',
} as const

const validSelectedEvent = {
  schemaVersion: 1,
  target: 'root',
  source: 'scheduler',
  provider: 'provider-disabled',
  model: 'baseline-disabled',
  maxTokens: 32_000,
  policyVersion: 'v0.3.0',
} as const

describe('orchestrator scheduling adapter', () => {
  it.each([8, 16])('keeps the legacy worker capability width at one under a parallel cumulative budget of %i', maxWorkers => {
    const request = buildCapabilityRequest(parallelWorkerConfig(maxWorkers), workerInput)
    expect(request).toMatchObject({ target: 'worker' })
    expect(request.constraints.maxWorkers).toBe(1)
  })

  it('projects a bounded capability request from the target and deployment policy', () => {
    expect(buildCapabilityRequest(config, workerInput)).toEqual({
      schemaVersion: 1,
      target: 'worker',
      taskId: 'session-1',
      objective: 'Fix the scheduling adapter',
      profile: scheduling.workerProfile,
      constraints: {
        maxWorkers: 1,
        maxOutputTokens: 64_000,
        maxLatencyMs: 60_000,
        allowPaidFallback: false,
        allowedProviders: ['provider-disabled'],
        requiredTools: ['targeted_verify'],
      },
      affinity: { workerId: 'session-1:worker:1' },
    })
  })

  it('uses the fixed worker route when the optional scheduler is absent', async () => {
    const resolver = { current: () => undefined }
    await expect(resolveSchedule(config, resolver, workerInput)).resolves.toMatchObject({
      decision: { source: 'profile-fallback', route: config.worker },
    })
  })

  it('accepts a valid scheduler response and returns its selected route', async () => {
    const scheduler = { schedule: async () => validDecision }
    await expect(resolveSchedule(config, { current: () => scheduler }, workerInput)).resolves.toMatchObject({
      decision: validDecision,
      scheduler,
    })
  })

  it('hard-validates the target mode and workerCount pair', () => {
    const rootDecision = {
      ...validDecision,
      mode: 'direct' as const,
      workerCount: 0 as const,
    }

    expect(validateScheduleDecisionForConfig(validDecision, config, 'worker')).toEqual(validDecision)
    expect(validateScheduleDecisionForConfig(rootDecision, config, 'root')).toEqual(rootDecision)
    expect(() => validateScheduleDecisionForConfig(validDecision, config, 'root')).toThrow(/target shape/u)
    expect(() => validateScheduleDecisionForConfig(rootDecision, config, 'worker')).toThrow(/target shape/u)
  })

  it('hard-rejects unconfigured routes and token ceilings', () => {
    expect(() => validateScheduleDecisionForConfig({
      ...validDecision,
      route: { ...routes[0], provider: 'not-configured' },
    }, config, 'worker')).toThrow(/route is not configured/u)

    expect(() => validateScheduleDecisionForConfig({
      ...validDecision,
      route: { ...routes[0], maxTokens: routes[0].maxTokens + 1 },
    }, config, 'worker')).toThrow(/route is not configured/u)

    const workerCeilingConfig: OrchestratorConfig = {
      ...config,
      worker: { ...config.worker, maxTokens: 32_000 },
      scheduling: {
        ...scheduling,
        allowedRoutes: [{ provider: 'provider-disabled', model: 'wide-disabled', maxTokens: 128_000 }],
      },
    }
    expect(() => validateScheduleDecisionForConfig({
      ...validDecision,
      route: { provider: 'provider-disabled', model: 'wide-disabled', maxTokens: 32_001 },
    }, workerCeilingConfig, 'worker')).toThrow(/route is not configured/u)
  })

  it.each([
    ['reasoningEffort', 'low'],
    ['promptProfile', 'review-v1'],
    ['modelFamily', 'other-family'],
  ] as const)('hard-rejects configured routes with mismatched %s metadata', (field, value) => {
    const metadataConfig: OrchestratorConfig = {
      ...config,
      scheduling: { ...scheduling, allowedRoutes: [routes[3]] },
    }
    const metadataDecision = { ...validDecision, route: routes[3] }

    expect(validateScheduleDecisionForConfig(metadataDecision, metadataConfig, 'worker')).toEqual(metadataDecision)
    expect(() => validateScheduleDecisionForConfig({
      ...metadataDecision,
      route: { ...routes[3], [field]: value },
    }, metadataConfig, 'worker')).toThrow(/route is not configured/u)
  })

  it('hard-rejects route metadata that the configured route omits', () => {
    expect(() => validateScheduleDecisionForConfig({
      ...validDecision,
      route: { ...routes[0], reasoningEffort: 'high' },
    }, config, 'worker')).toThrow(/route is not configured/u)
  })

  it('tracks an optional adaptive scheduler service without making it required', () => {
    const ctx = new Context()
    expect(ctx.get('adaptiveScheduler')).toBeUndefined()
    const mounted = mountAdaptiveSchedulerResolver(ctx)
    expect(mounted.current()).toBeUndefined()
    const fakeScheduler = { schedule: async () => validDecision }
    const dispose = ctx.provide('adaptiveScheduler', fakeScheduler as never)
    expect(mounted.current()).toBe(fakeScheduler)
    dispose()
    expect(mounted.current()).toBeUndefined()
  })

  it('rejects an invalid scheduler route without consuming the supplied budget', async () => {
    const beforeBudget = workerInput.budget
    const invalid = { ...validDecision, route: { ...validDecision.route, provider: 'not-configured' } }
    await expect(resolveSchedule(config, {
      current: () => ({ schedule: async () => invalid }),
    }, workerInput)).rejects.toThrow('SCHEDULE_DECISION_INVALID')
    expect(workerInput.budget).toEqual(beforeBudget)
  })

  it('falls back to the fixed profile only when invalid-decision fallback is enabled', async () => {
    const invalid = { ...validDecision, route: { ...validDecision.route, provider: 'not-configured' } }
    await expect(resolveSchedule({
      ...config,
      scheduling: { ...scheduling, allowInvalidDecisionFallback: true },
    }, {
      current: () => ({ schedule: async () => invalid }),
    }, workerInput)).resolves.toMatchObject({
      decision: { source: 'profile-fallback', route: config.worker },
    })
  })

  it('does not fall back to the fixed profile when cancellation races with invalid scheduling', async () => {
    const cancellation = new Error('cancelled while scheduling')
    const aborted = new AbortController()
    let release: (() => void) | undefined
    const pending = resolveSchedule({
      ...config,
      scheduling: { ...scheduling, allowInvalidDecisionFallback: true },
    }, {
      current: () => ({
        schedule: async () => new Promise(resolve => {
          release = () => resolve({
            ...validDecision,
            route: { ...validDecision.route, provider: 'not-configured' },
          })
        }),
      }),
    }, { ...workerInput, signal: aborted.signal })

    await Promise.resolve()
    aborted.abort(cancellation)
    release?.()

    await expect(pending).rejects.toBe(cancellation)
  })

  it.each([
    ['reasoningEffort', 'low'],
    ['promptProfile', 'review-v1'],
    ['modelFamily', 'other-family'],
  ] as const)('rejects a scheduler response with mismatched %s route metadata', async (field, value) => {
    const metadataRoute = routes[3]
    const invalid = {
      ...validDecision,
      route: { ...metadataRoute, [field]: value },
    }
    await expect(resolveSchedule(config, {
      current: () => ({ schedule: async () => invalid }),
    }, workerInput)).rejects.toThrow('SCHEDULE_DECISION_INVALID')
  })

  it('converts decisions into provenance events and restores only configured routes', () => {
    expect(scheduleSelectedFrom(validDecision, 'worker')).toMatchObject({
      target: 'worker',
      source: 'scheduler',
      provider: 'provider-disabled',
      model: 'baseline-disabled',
      maxTokens: 32_000,
      policyVersion: 'v0.3.0',
    })

    const restored = restoreScheduleSelected([
      { type: 'dsh-plugin/schedule-selected', data: validSelectedEvent } as SessionEvent,
    ], { ...config, mode: 'direct', budgets: { ...config.budgets, maxWorkers: 0 } }, 'root')
    expect(restored).toMatchObject({
      source: 'scheduler',
      route: { provider: 'provider-disabled', model: 'baseline-disabled' },
      explanationCode: 'DURABLE_STICKY_RESTORE',
    })
    expect(restoreScheduleSelected([
      { type: 'dsh-plugin/schedule-selected', data: { ...validSelectedEvent, model: 'not-configured' } } as SessionEvent,
    ], config, 'root')).toBeUndefined()
  })

  it('rejects durable selections when scheduling policy is absent', () => {
    const noSchedulingConfig: OrchestratorConfig = {
      ...config,
      mode: 'direct',
      budgets: { ...config.budgets, maxWorkers: 0 },
      scheduling: undefined,
    }
    expect(restoreScheduleSelected([
      {
        type: 'dsh-plugin/schedule-selected',
        data: { ...validSelectedEvent, provider: 'unconfigured', model: 'unconfigured', target: 'root' },
      } as SessionEvent,
    ], noSchedulingConfig, 'root')).toBeUndefined()
  })

  it('rejects durable selections that omit configured route metadata', () => {
    const metadataConfig: OrchestratorConfig = {
      ...config,
      mode: 'direct',
      budgets: { ...config.budgets, maxWorkers: 0 },
      scheduling: { ...scheduling, allowedRoutes: [routes[3]] },
    }
    expect(restoreScheduleSelected([
      {
        type: 'dsh-plugin/schedule-selected',
        data: {
          ...validSelectedEvent,
          provider: routes[3].provider,
          model: routes[3].model,
          maxTokens: routes[3].maxTokens,
          reasoningEffort: routes[3].reasoningEffort,
          promptProfile: routes[3].promptProfile,
          target: 'root',
        },
      } as SessionEvent,
    ], metadataConfig, 'root')).toBeUndefined()
  })

  it('preserves all configured route metadata when a selected route is replayed', () => {
    const metadataConfig: OrchestratorConfig = {
      ...config,
      mode: 'direct',
      budgets: { ...config.budgets, maxWorkers: 0 },
      scheduling: { ...scheduling, allowedRoutes: [routes[3]] },
    }
    const decision = {
      ...validDecision,
      mode: 'direct' as const,
      route: routes[3],
      workerCount: 0 as const,
    }
    const selected = scheduleSelectedFrom(decision, 'root')
    expect(selected).toMatchObject({
      reasoningEffort: 'high',
      promptProfile: 'coding-v1',
      modelFamily: 'deepseek',
    })
    expect(restoreScheduleSelected([
      { type: 'dsh-plugin/schedule-selected', data: selected },
    ], metadataConfig, 'root')).toMatchObject({ route: routes[3] })
  })

  it('rejects scheduler route metadata when the profile omits those fields', async () => {
    const hardRouteConfig: OrchestratorConfig = {
      ...config,
      scheduling: {
        ...scheduling,
        allowedRoutes: [{ provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000 }],
      },
    }
    const schedulerRoute = {
      provider: 'provider-disabled',
      model: 'strong-disabled',
      maxTokens: 64_000,
      reasoningEffort: 'high' as const,
      promptProfile: 'coding-strong-v1',
      modelFamily: 'deepseek',
    }
    const scheduler = {
      schedule: async () => ({
        schemaVersion: 1 as const,
        mode: 'single-worker' as const,
        route: schedulerRoute,
        workerCount: 1 as const,
        source: 'scheduler' as const,
        policyVersion: 'v0.3.0',
        explanationCode: 'TASK_BASELINE' as const,
      }),
    }

    await expect(resolveSchedule(hardRouteConfig, { current: () => scheduler }, workerInput)).rejects.toThrow('SCHEDULE_DECISION_INVALID')
  })

  it('derives fixed-profile mode from the scheduling target', () => {
    expect(fixedProfileSchedule(config.worker, config.mode, 'root')).toMatchObject({
      mode: 'direct',
      workerCount: 0,
      source: 'profile-fallback',
    })
  })

  it.each([
    'QUOTA_EXHAUSTED',
    'NON_TRANSIENT_FAILURE',
    'LOCAL_TOOL_BUDGET',
    'SAFETY_GATE',
  ])('preserves scheduler operational error %s even when invalid fallback is enabled', async code => {
    const operational = Object.assign(new Error(code), { code })
    await expect(resolveSchedule({
      ...config,
      scheduling: { ...scheduling, allowInvalidDecisionFallback: true },
    }, {
      current: () => ({ schedule: async () => { throw operational } }),
    }, workerInput)).rejects.toBe(operational)
  })

  it('preserves scheduler operational errors when invalid fallback is disabled', async () => {
    const operational = Object.assign(new Error('QUOTA_EXHAUSTED'), { code: 'QUOTA_EXHAUSTED' })
    await expect(resolveSchedule(config, {
      current: () => ({ schedule: async () => { throw operational } }),
    }, workerInput)).rejects.toBe(operational)
  })

  it('routes root requests through the official waterfall and records the selected route first', async () => {
    const ctx = new Context()
    const root = Session.create(SessionId('root-official-seam'))
    const agent = { id: root.id, session: root } as Agent
    const rootConfig: OrchestratorConfig = {
      ...config,
      mode: 'direct',
      budgets: { ...config.budgets, maxWorkers: 0 },
    }
    const scheduler = {
      schedule: async () => ({
        ...validDecision,
        mode: 'direct' as const,
        route: routes[2],
        workerCount: 0 as const,
      }),
    }
    const dispose = mountRootScheduling(
      ctx,
      rootConfig,
      { forRootSession: () => ({ snapshot: () => workerInput.budget }) as never },
      { current: () => scheduler },
    )

    const result = await agentEvents(ctx, agent).waterfall(
      'agent/request',
      { turn: 1, step: 1, signal: new AbortController().signal },
      async () => ({
        provider: 'profile-disabled',
        model: 'profile-disabled',
        temperature: 0.2,
        stop: ['<stop>'],
      }),
    )

    expect(result).toMatchObject({
      provider: 'provider-disabled',
      model: 'strong-disabled',
      maxTokens: 64_000,
      reasoningEffort: ReasoningEffortId('high'),
      temperature: 0.2,
      stop: ['<stop>'],
    })
    expect(root.events.map(event => event.type)).toEqual(['dsh-plugin/schedule-selected'])
    dispose()
  })

  it('degrades a single-worker root request to a direct fixed-profile route when the scheduler is absent', async () => {
    const ctx = new Context()
    const root = Session.create(SessionId('root-profile-fallback'))
    const agent = { id: root.id, session: root } as Agent
    const dispose = mountRootScheduling(
      ctx,
      config,
      { forRootSession: () => ({ snapshot: () => workerInput.budget }) as never },
      { current: () => undefined },
    )

    const result = await agentEvents(ctx, agent).waterfall(
      'agent/request',
      { turn: 1, step: 1, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled' }),
    )

    expect(result).toMatchObject({
      provider: config.worker.provider,
      model: config.worker.model,
      maxTokens: config.worker.maxTokens,
    })
    expect(root.events).toHaveLength(1)
    expect(root.events[0]).toMatchObject({
      type: 'dsh-plugin/schedule-selected',
      data: { target: 'root', source: 'profile-fallback' },
    })
    dispose()
  })

  it('re-enters the scheduler for every step in one root session', async () => {
    const ctx = new Context()
    const root = Session.create(SessionId('root-multiple-steps'))
    const agent = { id: root.id, session: root } as Agent
    let calls = 0
    const scheduler = {
      schedule: async () => {
        calls++
        return {
          ...validDecision,
          mode: 'direct' as const,
          route: calls === 1 ? routes[0] : routes[2],
          workerCount: 0 as const,
        }
      },
    }
    const dispose = mountRootScheduling(
      ctx,
      config,
      { forRootSession: () => ({ snapshot: () => workerInput.budget }) as never },
      { current: () => scheduler },
    )

    const first = await agentEvents(ctx, agent).waterfall(
      'agent/request',
      { turn: 1, step: 1, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled' }),
    )
    const second = await agentEvents(ctx, agent).waterfall(
      'agent/request',
      { turn: 1, step: 2, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled' }),
    )

    expect(first.model).toBe('baseline-disabled')
    expect(second.model).toBe('strong-disabled')
    expect(calls).toBe(2)
    expect(root.events.filter(event => event.type === 'dsh-plugin/schedule-selected')).toHaveLength(2)
    dispose()
  })

  it('lets failure and TTL state affect the next root request', async () => {
    const ctx = new Context()
    const root = Session.create(SessionId('root-changing-state'))
    root.append('dsh-plugin/schedule-selected', {
      ...validSelectedEvent,
      model: 'fallback-disabled',
    })
    const agent = { id: root.id, session: root } as Agent
    let now = root.events[0]!.time
    const scheduler = createAdaptiveScheduler({
      policyVersion: 'v0.3.0',
      catalog: [
        { alias: 'baseline', route: routes[0], tier: 'baseline', taskTypes: ['unknown'], toolFilter: [], paid: false, reliability: 70 },
        { alias: 'fallback', route: routes[1], tier: 'fallback', taskTypes: ['unknown'], toolFilter: [], paid: false, reliability: 80 },
        { alias: 'strong', route: routes[2], tier: 'strong', taskTypes: ['unknown'], toolFilter: [], paid: false, reliability: 100 },
      ],
      baselines: { 'code-fix': 'baseline', 'code-new': 'baseline', research: 'baseline', summarize: 'baseline', review: 'baseline', 'tool-heavy': 'baseline', unknown: 'baseline' },
      stickyTtlMs: 100,
      idleTtlMs: 1_000,
      errorWindowMs: 1_000,
      cooldownMs: 100,
      escalationTtlMs: 1_000,
      maxEscalationsPerTask: 1,
      maxRounds: 2,
      historyWindowSize: 8,
      historyMinSamples: 2,
    }, { now: () => now, generation: 'root-integration' })
    const dispose = mountRootScheduling(
      ctx,
      config,
      { forRootSession: () => ({ snapshot: () => workerInput.budget }) as never },
      { current: () => scheduler },
    )
    const request = () => agentEvents(ctx, agent).waterfall(
      'agent/request',
      { turn: 1, step: 1, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled' }),
    )

    expect((await request()).model).toBe('fallback-disabled')
    now += 100
    expect((await request()).model).toBe('baseline-disabled')
    scheduler.recordFailure({ requestId: String(root.id), code: 'TIMEOUT' })
    expect((await request()).model).toBe('fallback-disabled')
    dispose()
  })

  it('hydrates a durable root selection once for a remounted scheduler, then schedules normally', async () => {
    const ctx = new Context()
    const root = Session.create(SessionId('root-durable-restore'))
    root.append('dsh-plugin/schedule-selected', {
      schemaVersion: 1,
      target: 'root',
      source: 'scheduler',
      provider: 'provider-disabled',
      model: 'strong-disabled',
      maxTokens: 64_000,
      reasoningEffort: 'high',
      policyVersion: 'v0.3.0',
    })
    const agent = { id: root.id, session: root } as Agent
    const rootConfig: OrchestratorConfig = {
      ...config,
      mode: 'direct',
      budgets: { ...config.budgets, maxWorkers: 0 },
    }
    const selectedAt = root.events[0]!.time
    const hydrated: unknown[][] = []
    let calls = 0
    const remountedScheduler = {
      hydrate: (...args: unknown[]) => { hydrated.push(args) },
      schedule: async () => {
        calls++
        return {
          ...validDecision,
          mode: 'direct' as const,
          route: routes[2],
          workerCount: 0 as const,
        }
      },
    }
    let currentScheduler = remountedScheduler
    const dispose = mountRootScheduling(
      ctx,
      rootConfig,
      { forRootSession: () => ({ snapshot: () => workerInput.budget }) as never },
      { current: () => currentScheduler },
    )

    const result = await agentEvents(ctx, agent).waterfall(
      'agent/request',
      { turn: 2, step: 1, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled' }),
    )

    expect(result).toMatchObject({ provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000 })
    await agentEvents(ctx, agent).waterfall(
      'agent/request',
      { turn: 2, step: 2, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled' }),
    )
    expect(hydrated).toHaveLength(1)
    expect(hydrated[0]?.[2]).toBe(selectedAt)
    expect(calls).toBe(2)

    const nextGenerationHydrated: unknown[][] = []
    currentScheduler = {
      hydrate: (...args: unknown[]) => { nextGenerationHydrated.push(args) },
      schedule: remountedScheduler.schedule,
    }
    await agentEvents(ctx, agent).waterfall(
      'agent/request',
      { turn: 2, step: 3, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled' }),
    )
    expect(nextGenerationHydrated).toHaveLength(1)
    expect(calls).toBe(3)
    dispose()
  })
})
