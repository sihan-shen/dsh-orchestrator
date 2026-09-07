import {
  MAX_SCHEDULING_LATENCY_MS,
  parseCapabilityRequestV1,
} from '@ds-plugins/dsh-scheduling-contracts'
import type {
  CapabilityRequestV1,
  BudgetViewV1,
  ScheduleDecisionV1,
  TaskNodeV1,
} from '@ds-plugins/dsh-scheduling-contracts'
import { routeToolFilterKey } from './config.js'
import {
  resolveScheduleFromRequest,
  type ResolvedScheduleV1,
  type SchedulerResolver,
  validateScheduleDecisionForConfig,
} from './scheduling.js'
import type { OrchestratorConfig } from './types.js'

export interface ParallelScheduleContextV1 {
  readonly dagId: string
  readonly requestId: string
  readonly workerAffinityId: string
  readonly budget: BudgetViewV1
  readonly signal: AbortSignal
}

export interface ExecutableParallelNodeV1 {
  readonly kind: 'executable'
  readonly node: TaskNodeV1
  readonly request: CapabilityRequestV1
  readonly resolvedSchedule: ResolvedScheduleV1
  readonly allowedTools: readonly string[]
}

export interface RejectedParallelNodeV1 {
  readonly kind: 'not-run'
  readonly node: TaskNodeV1
  readonly requestId: string
  readonly reason: 'zero-worker-constraint' | 'no-route' | 'tool-unauthorized'
}

export type ParallelNodeClassificationV1 = ExecutableParallelNodeV1 | RejectedParallelNodeV1

function cloneDetached<T>(value: T): T {
  if (Array.isArray(value)) return value.map(item => cloneDetached(item)) as T
  if (typeof value !== 'object' || value === null) return value
  const result: Record<string, unknown> = {}
  for (const [key, child] of Object.entries(value as Record<string, unknown>)) {
    result[key] = cloneDetached(child)
  }
  return result as T
}

function deepFreeze<T>(value: T): T {
  if (typeof value !== 'object' || value === null || Object.isFrozen(value)) return value
  for (const child of Object.values(value as Record<string, unknown>)) deepFreeze(child)
  return Object.freeze(value)
}

function detachedFrozen<T>(value: T): T {
  return deepFreeze(cloneDetached(value))
}

function notRun(
  node: TaskNodeV1,
  requestId: string,
  reason: RejectedParallelNodeV1['reason'],
): RejectedParallelNodeV1 {
  return detachedFrozen({ kind: 'not-run', node, requestId, reason })
}

function deploymentProviders(config: OrchestratorConfig): readonly string[] {
  const providers = [
    ...(config.scheduling?.allowedRoutes.map(route => route.provider) ?? []),
    config.worker.provider,
  ]
  return [...new Set(providers)]
}

function intersectProviders(config: OrchestratorConfig, node: TaskNodeV1): readonly string[] {
  const allowed = node.constraints.allowedProviders
  const providers = deploymentProviders(config)
  return allowed === undefined
    ? providers
    : providers.filter(provider => allowed.includes(provider))
}

/** Build the worker request after narrowing every deployment/node capability constraint. */
export function buildParallelCapabilityRequest(
  config: OrchestratorConfig,
  node: TaskNodeV1,
  context: ParallelScheduleContextV1,
): CapabilityRequestV1 {
  const scheduling = config.scheduling
  const providers = intersectProviders(config, node)
  return parseCapabilityRequestV1({
    schemaVersion: 1,
    target: 'worker',
    taskId: context.requestId,
    objective: node.objective,
    profile: node.profile,
    constraints: {
      // Parallel fan-out is owned by the Orchestrator; every capability request is one worker.
      maxWorkers: 1,
      maxOutputTokens: Math.min(config.worker.maxTokens, node.constraints.maxOutputTokens),
      maxLatencyMs: Math.min(scheduling?.maxLatencyMs ?? MAX_SCHEDULING_LATENCY_MS, node.constraints.maxLatencyMs),
      allowPaidFallback: (scheduling?.allowPaidFallback ?? false) && node.constraints.allowPaidFallback,
      allowedProviders: providers,
      requiredTools: [...node.constraints.requiredTools],
    },
    affinity: { workerId: context.workerAffinityId },
  })
}

function selectedRouteMatchesNode(
  config: OrchestratorConfig,
  node: TaskNodeV1,
  request: CapabilityRequestV1,
  decision: ScheduleDecisionV1,
): boolean {
  if (decision.route.maxTokens > request.constraints.maxOutputTokens) return false
  if (request.constraints.allowedProviders !== undefined
    && !request.constraints.allowedProviders.includes(decision.route.provider)) return false
  if (decision.source === 'scheduler' && config.scheduling !== undefined) {
    try {
      validateScheduleDecisionForConfig(decision, config, 'worker')
    } catch {
      return false
    }
  }
  if (decision.source === 'scheduler') return true
  return node.constraints.requiredTools.length === 0 || node.constraints.requiredTools.every(tool =>
    (config.parallel?.routeToolFilters[routeToolFilterKey(decision.route)] ?? []).includes(tool),
  )
}

function completeRejectedSchedulerDecision(resolved: ResolvedScheduleV1): void {
  if (resolved.decision.source === 'scheduler') {
    resolved.scheduler?.complete?.(resolved.request.taskId)
  }
}

/** Resolve one parallel node and fail closed before any worker admission occurs. */
export async function resolveParallelNodeSchedule(
  config: OrchestratorConfig,
  resolver: SchedulerResolver,
  node: TaskNodeV1,
  context: ParallelScheduleContextV1,
): Promise<ParallelNodeClassificationV1> {
  if (node.constraints.maxWorkers === 0) return notRun(node, context.requestId, 'zero-worker-constraint')

  const providers = intersectProviders(config, node)
  if (providers.length === 0) return notRun(node, context.requestId, 'no-route')

  const parallel = config.parallel
  if (parallel === undefined) return notRun(node, context.requestId, 'no-route')
  if (node.constraints.requiredTools.includes('targeted_verify')
    || !node.constraints.requiredTools.every(tool => parallel.workerToolAllowlist.includes(tool))) {
    return notRun(node, context.requestId, 'tool-unauthorized')
  }

  const request = buildParallelCapabilityRequest(config, node, context)
  const resolved = await resolveScheduleFromRequest(config, resolver, request, context.budget, context.signal)
  if (resolved.decision.route.maxTokens > request.constraints.maxOutputTokens
    || (request.constraints.allowedProviders !== undefined
      && !request.constraints.allowedProviders.includes(resolved.decision.route.provider))) {
    completeRejectedSchedulerDecision(resolved)
    return notRun(node, context.requestId, 'no-route')
  }
  if (resolved.decision.source === 'scheduler' && config.scheduling !== undefined) {
    try {
      validateScheduleDecisionForConfig(resolved.decision, config, 'worker')
    } catch {
      completeRejectedSchedulerDecision(resolved)
      return notRun(node, context.requestId, 'no-route')
    }
  }
  if (!selectedRouteMatchesNode(config, node, request, resolved.decision)) {
    completeRejectedSchedulerDecision(resolved)
    return notRun(node, context.requestId, 'tool-unauthorized')
  }

  return deepFreeze({
    kind: 'executable',
    node: detachedFrozen(node),
    request,
    resolvedSchedule: Object.freeze({ ...resolved }),
    allowedTools: Object.freeze([...node.constraints.requiredTools]),
  })
}
