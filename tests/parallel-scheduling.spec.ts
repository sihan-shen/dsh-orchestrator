import { describe, expect, it } from 'vitest'
import {
  parseTaskDagV1,
  type AdaptiveSchedulerService,
  type BudgetViewV1,
  type CapabilityRequestV1,
  type RouteDecisionV1,
  type ScheduleDecisionV1,
  type TaskNodeV1,
} from '@han_05/dsh-scheduling-contracts'
import {
  buildParallelCapabilityRequest,
  resolveParallelNodeSchedule,
  type ParallelScheduleContextV1,
} from '../src/parallel-scheduling.ts'
import type { SchedulerResolver } from '../src/scheduling.ts'
import type { OrchestratorConfig } from '../src/types.ts'

const profile = {
  coding: 80,
  reasoning: 70,
  toolUse: 60,
  repoContext: 80,
  risk: 30,
  difficulty: 60,
} as const

const baselineRoute = {
  provider: 'provider-disabled',
  model: 'baseline-disabled',
  maxTokens: 64_000,
} as const

const strongRoute = {
  provider: 'strong-provider-disabled',
  model: 'strong-disabled',
  maxTokens: 96_000,
  reasoningEffort: 'high',
  promptProfile: 'coding-v1',
  modelFamily: 'deepseek',
} as const

const parallelConfig: OrchestratorConfig = {
  workspaceRoot: '.',
  mode: 'single-worker',
  worker: baselineRoute,
  budgets: { maxWorkers: 8, maxPluginToolActions: 24, toolTimeoutMs: 60_000 },
  verification: { commands: [], timeoutMs: 60_000, maxOutputBytes: 65_536 },
  scheduling: {
    allowInvalidDecisionFallback: false,
    allowedRoutes: [baselineRoute, strongRoute],
    rootProfile: profile,
    workerProfile: profile,
    maxLatencyMs: 60_000,
    allowPaidFallback: false,
  },
  parallel: {
    maxParallelWorkers: 4,
    verification: { schemaVersion: 1, scope: 'dag', commands: [] },
    workerToolAllowlist: ['read_file', 'write_file'],
    routeToolFilters: {
      '["provider-disabled","baseline-disabled",null,null,null]': ['read_file'],
    },
  },
}

function node(nodeId: string, overrides: Partial<TaskNodeV1['constraints']> = {}): TaskNodeV1 {
  return parseTaskDagV1({
    schemaVersion: 1,
    rootTaskId: `root-${nodeId}`,
    nodes: [{
      schemaVersion: 1,
      nodeId,
      objective: `Work on ${nodeId}.`,
      profile,
      constraints: {
        maxWorkers: 1,
        maxOutputTokens: 64_000,
        maxLatencyMs: 60_000,
        allowPaidFallback: false,
        requiredTools: [],
        ...overrides,
      },
      readPaths: ['src/'],
      writePaths: [`src/${nodeId}.ts`],
      dependsOn: [],
    }],
  }).nodes[0]!
}

const budget: BudgetViewV1 = {
  maxWorkers: 8,
  admittedWorkers: 2,
  maxPluginToolActions: 24,
  admittedPluginToolActions: 3,
  remainingWorkers: 6,
  remainingPluginToolActions: 21,
}

function context(
  requestId = 'request-a',
  signal: AbortSignal = new AbortController().signal,
): ParallelScheduleContextV1 {
  return {
    dagId: 'dag-1',
    requestId,
    workerAffinityId: `affinity-${requestId}`,
    budget,
    signal,
  }
}

function decision(route: RouteDecisionV1): ScheduleDecisionV1 {
  return {
    schemaVersion: 1,
    mode: 'single-worker',
    route,
    workerCount: 1,
    source: 'scheduler',
    policyVersion: 'v0.4-test',
    explanationCode: 'TASK_BASELINE',
  }
}

interface SchedulerCapture {
  calls: number
  readonly requests: CapabilityRequestV1[]
  readonly budgets: BudgetViewV1[]
  readonly signals: AbortSignal[]
}

function capturingResolver(
  selected: ScheduleDecisionV1 | ((request: CapabilityRequestV1) => Promise<ScheduleDecisionV1>),
): { readonly capture: SchedulerCapture; readonly resolver: SchedulerResolver } {
  const capture: SchedulerCapture = { calls: 0, requests: [], budgets: [], signals: [] }
  const scheduler: AdaptiveSchedulerService = {
    async schedule(request, currentBudget, signal) {
      capture.calls++
      capture.requests.push(request)
      capture.budgets.push(currentBudget)
      capture.signals.push(signal)
      return typeof selected === 'function' ? selected(request) : selected
    },
  }
  return { capture, resolver: { current: () => scheduler } }
}

describe('parallel scheduling', () => {
  it('intersects node tokens, latency, paid fallback, providers, and tools without widening', async () => {
    const input = node('narrow', {
      maxOutputTokens: 8_000,
      maxLatencyMs: 5_000,
      allowPaidFallback: true,
      allowedProviders: ['strong-provider-disabled', 'not-configured'],
      requiredTools: ['read_file'],
    })
    const selectedRoute = { ...strongRoute, maxTokens: 8_000 }
    const { capture, resolver } = capturingResolver(decision(selectedRoute))
    const scheduleContext = context('request-narrow')

    const result = await resolveParallelNodeSchedule(parallelConfig, resolver, input, scheduleContext)
    if (result.kind !== 'executable') throw new Error('expected an executable parallel node')

    expect(result).toMatchObject({
      kind: 'executable',
      node: input,
      allowedTools: ['read_file'],
      request: {
        schemaVersion: 1,
        target: 'worker',
        taskId: 'request-narrow',
        objective: 'Work on narrow.',
        profile,
        constraints: {
          maxWorkers: 1,
          maxOutputTokens: 8_000,
          maxLatencyMs: 5_000,
          allowPaidFallback: false,
          allowedProviders: ['strong-provider-disabled'],
          requiredTools: ['read_file'],
        },
        affinity: { workerId: 'affinity-request-narrow' },
      },
      resolvedSchedule: {
        decision: { source: 'scheduler', route: selectedRoute },
      },
    })
    expect(result.allowedTools).toEqual(['read_file'])
    expect(capture.calls).toBe(1)
    expect(capture.requests).toEqual([result.request])
    expect(capture.budgets).toEqual([budget])
    expect(capture.signals).toEqual([scheduleContext.signal])
  })

  it('keeps deployment ceilings and node denial when the node requests wider limits', () => {
    const config: OrchestratorConfig = {
      ...parallelConfig,
      worker: { ...parallelConfig.worker, maxTokens: 16_000 },
      scheduling: {
        ...parallelConfig.scheduling!,
        maxLatencyMs: 4_000,
        allowPaidFallback: true,
      },
    }
    const input = node('deployment-narrow', {
      maxOutputTokens: 64_000,
      maxLatencyMs: 8_000,
      allowPaidFallback: false,
      allowedProviders: ['provider-disabled', 'not-configured'],
      requiredTools: ['read_file'],
    })

    expect(buildParallelCapabilityRequest(config, input, context('request-deployment-narrow'))).toMatchObject({
      constraints: {
        maxWorkers: 1,
        maxOutputTokens: 16_000,
        maxLatencyMs: 4_000,
        allowPaidFallback: false,
        allowedProviders: ['provider-disabled'],
        requiredTools: ['read_file'],
      },
    })
  })

  it.each([
    {
      label: 'zero worker constraint',
      config: parallelConfig,
      input: node('zero', { maxWorkers: 0 }),
      reason: 'zero-worker-constraint',
    },
    {
      label: 'empty provider intersection',
      config: parallelConfig,
      input: node('provider', { allowedProviders: ['not-configured'] }),
      reason: 'no-route',
    },
    {
      label: 'deployment-disallowed tool',
      config: {
        ...parallelConfig,
        parallel: { ...parallelConfig.parallel!, workerToolAllowlist: ['read_file'] },
      },
      input: node('tool', { requiredTools: ['write_file'] }),
      reason: 'tool-unauthorized',
    },
    {
      label: 'targeted verification tool',
      config: parallelConfig,
      input: node('targeted', { requiredTools: ['targeted_verify'] }),
      reason: 'tool-unauthorized',
    },
  ] as const)('classifies $label before scheduling or admission', async ({ config, input, reason }) => {
    const { capture, resolver } = capturingResolver(decision(baselineRoute))
    const scheduleContext = context(`request-${input.nodeId}`)

    await expect(resolveParallelNodeSchedule(config, resolver, input, scheduleContext)).resolves.toEqual({
      kind: 'not-run',
      node: input,
      requestId: `request-${input.nodeId}`,
      reason,
    })
    expect(capture.calls).toBe(0)
    expect(scheduleContext.budget).toBe(budget)
    expect(scheduleContext.budget).toEqual({
      maxWorkers: 8,
      admittedWorkers: 2,
      maxPluginToolActions: 24,
      admittedPluginToolActions: 3,
      remainingWorkers: 6,
      remainingPluginToolActions: 21,
    })
  })

  it('uses requiredTools as the exact spawned allowlist after the scheduler accepts them', async () => {
    const input = node('scheduler-tools', { requiredTools: ['read_file', 'write_file'] })
    const selectedRoute = { ...strongRoute, maxTokens: 64_000 }
    const { resolver } = capturingResolver(decision(selectedRoute))

    const result = await resolveParallelNodeSchedule(
      parallelConfig,
      resolver,
      input,
      context('request-scheduler-tools'),
    )
    if (result.kind !== 'executable') throw new Error('expected scheduler-authorized tools to be executable')

    expect(result).toMatchObject({
      kind: 'executable',
      resolvedSchedule: { decision: { source: 'scheduler', route: selectedRoute } },
    })
    expect(result.allowedTools).toEqual(['read_file', 'write_file'])
    expect(result.request.constraints.requiredTools).toEqual(['read_file', 'write_file'])
  })

  it('uses routeToolFilters for scheduler-absent fixed-route authorization', async () => {
    const resolver: SchedulerResolver = { current: () => undefined }
    const authorized = await resolveParallelNodeSchedule(
      parallelConfig,
      resolver,
      node('fixed-authorized', { requiredTools: ['read_file'] }),
      context('request-fixed-authorized'),
    )
    if (authorized.kind !== 'executable') throw new Error('expected fixed-route-authorized tools to be executable')

    expect(authorized).toMatchObject({
      kind: 'executable',
      resolvedSchedule: {
        decision: { source: 'profile-fallback', route: baselineRoute },
      },
    })
    expect(authorized.allowedTools).toEqual(['read_file'])

    const unauthorized = node('fixed-unauthorized', { requiredTools: ['write_file'] })
    await expect(resolveParallelNodeSchedule(
      parallelConfig,
      resolver,
      unauthorized,
      context('request-fixed-unauthorized'),
    )).resolves.toEqual({
      kind: 'not-run',
      node: unauthorized,
      requestId: 'request-fixed-unauthorized',
      reason: 'tool-unauthorized',
    })
  })

  it('keeps a fixed worker route executable when its provider is absent from allowedRoutes', async () => {
    const fixedRoute = {
      provider: 'fixed-only-provider-disabled',
      model: 'fixed-only-model-disabled',
      maxTokens: 64_000,
    } as const
    const config: OrchestratorConfig = {
      ...parallelConfig,
      worker: fixedRoute,
      scheduling: {
        ...parallelConfig.scheduling!,
        allowedRoutes: [strongRoute],
      },
      parallel: {
        ...parallelConfig.parallel!,
        routeToolFilters: {
          [JSON.stringify([fixedRoute.provider, fixedRoute.model, null, null, null])]: ['read_file'],
        },
      },
    }

    const result = await resolveParallelNodeSchedule(
      config,
      { current: () => undefined },
      node('fixed-provider', { requiredTools: ['read_file'] }),
      context('request-fixed-provider'),
    )

    expect(result).toMatchObject({
      kind: 'executable',
      resolvedSchedule: { decision: { source: 'profile-fallback', route: fixedRoute } },
    })
  })

  it('completes a scheduler invocation when parallel hard validation rejects its route', async () => {
    const completed: string[] = []
    const scheduler: AdaptiveSchedulerService = {
      schedule: async () => decision({ ...strongRoute, maxTokens: 64_000 }),
      complete: requestId => completed.push(requestId),
    }

    const result = await resolveParallelNodeSchedule(
      parallelConfig,
      { current: () => scheduler },
      node('scheduler-rejected-route', { maxOutputTokens: 8_000 }),
      context('request-scheduler-rejected-route'),
    )

    expect(result).toMatchObject({
      kind: 'not-run',
      reason: 'no-route',
      requestId: 'request-scheduler-rejected-route',
    })
    expect(completed).toEqual(['request-scheduler-rejected-route'])
  })

  it('does not complete a scheduler invocation twice for profile fallback', async () => {
    const completed: string[] = []
    const scheduler: AdaptiveSchedulerService = {
      schedule: async () => decision({ ...baselineRoute, provider: 'not-configured' }),
      complete: requestId => completed.push(requestId),
    }

    const result = await resolveParallelNodeSchedule({
      ...parallelConfig,
      scheduling: { ...parallelConfig.scheduling!, allowInvalidDecisionFallback: true },
    }, { current: () => scheduler }, node('scheduler-fallback', { requiredTools: ['read_file'] }), context('request-scheduler-fallback'))

    expect(result).toMatchObject({
      kind: 'executable',
      resolvedSchedule: { decision: { source: 'profile-fallback', route: baselineRoute } },
    })
    expect(completed).toEqual(['request-scheduler-fallback'])
  })

  it('rethrows the original cancellation and never converts it to a no-route fallback', async () => {
    const cancellation = new Error('cancelled while scheduling parallel node')
    const controller = new AbortController()
    let release: (() => void) | undefined
    const { resolver } = capturingResolver(() => new Promise(resolve => {
      release = () => resolve(decision({ ...baselineRoute, provider: 'not-configured' }))
    }))
    const pending = resolveParallelNodeSchedule({
      ...parallelConfig,
      scheduling: { ...parallelConfig.scheduling!, allowInvalidDecisionFallback: true },
    }, resolver, node('cancelled'), context('request-cancelled', controller.signal))

    await Promise.resolve()
    controller.abort(cancellation)
    release?.()

    await expect(pending).rejects.toBe(cancellation)
  })
})
