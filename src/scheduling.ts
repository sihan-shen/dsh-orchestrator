import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-agent'
import { ReasoningEffortId, type LlmCallConfig } from '@deepseek-ai/dsh-llm'
import type { Session, SessionEvent } from '@deepseek-ai/dsh-session'
import {
  MAX_SCHEDULING_LATENCY_MS,
  parseBudgetViewV1,
  parseCapabilityRequestV1,
  parseScheduleDecisionV1,
  parseScheduleSelectedV1,
} from '@ds-plugins/dsh-scheduling-contracts'
import type {
  AdaptiveSchedulerService,
  BudgetViewV1,
  CapabilityProfileV1,
  CapabilityRequestV1,
  RouteDecisionV1,
  ScheduleDecisionV1,
  ScheduleSelectedV1,
} from '@ds-plugins/dsh-scheduling-contracts'
import type { BudgetControllerRegistry } from './budgets.js'
import { appendScheduleSelected } from './events.js'
import type { HandoffV1, OrchestratorConfig } from './types.js'

/** Resolves the currently active optional scheduler service generation. */
export interface SchedulerResolver {
  current(): AdaptiveSchedulerService | undefined
}

/** Inputs projected into one scheduler capability request. */
export interface ResolveScheduleInput {
  readonly target: 'root' | 'worker'
  readonly taskId: string
  readonly objective: string
  readonly requiredTools: readonly string[]
  readonly affinity?: CapabilityRequestV1['affinity']
  readonly priorHandoff?: HandoffV1
  readonly budget: BudgetViewV1
  readonly signal: AbortSignal
}

/** The validated request, selected decision, and scheduler generation that produced it. */
export interface ResolvedScheduleV1 {
  readonly request: CapabilityRequestV1
  readonly decision: ScheduleDecisionV1
  readonly scheduler?: AdaptiveSchedulerService
}

/** Error raised when a scheduler response is not acceptable to the Orchestrator boundary. */
export class SchedulingValidationError extends Error {
  readonly code: 'SCHEDULE_DECISION_INVALID'
  readonly cause: unknown

  constructor(code: 'SCHEDULE_DECISION_INVALID', options: { readonly cause?: unknown } = {}) {
    super(code)
    this.name = 'SchedulingValidationError'
    this.code = code
    this.cause = options.cause
  }
}

const DEFAULT_PROFILE: CapabilityProfileV1 = {
  coding: 50,
  reasoning: 50,
  toolUse: 50,
  repoContext: 50,
  risk: 50,
  difficulty: 50,
}

const PROFILE_FALLBACK_POLICY = 'profile-fallback-v1'
const ROUTE_IDENTITY_AND_METADATA_KEYS = [
  'provider',
  'model',
  'reasoningEffort',
  'promptProfile',
  'modelFamily',
] as const

function throwIfAborted(signal: AbortSignal): void {
  if (signal.aborted) throw signal.reason ?? new DOMException('Scheduling cancelled', 'AbortError')
}

/** Project one bounded capability request without exposing runtime/provider state. */
export function buildCapabilityRequest(config: OrchestratorConfig, input: ResolveScheduleInput): CapabilityRequestV1 {
  const scheduling = config.scheduling
  const profile = input.target === 'root'
    ? scheduling?.rootProfile ?? DEFAULT_PROFILE
    : scheduling?.workerProfile ?? DEFAULT_PROFILE
  const allowedProviders = scheduling === undefined
    ? undefined
    : [...new Set(scheduling.allowedRoutes.map(route => route.provider))]
  return parseCapabilityRequestV1({
    schemaVersion: 1,
    target: input.target,
    taskId: input.taskId,
    objective: input.objective,
    profile,
    constraints: {
      // The model-facing delegate remains one-shot; the cumulative parallel budget
      // belongs to the root BudgetController, never to a per-worker capability request.
      maxWorkers: input.target === 'worker' ? 1 : 0,
      maxOutputTokens: config.worker.maxTokens,
      maxLatencyMs: scheduling?.maxLatencyMs ?? MAX_SCHEDULING_LATENCY_MS,
      allowPaidFallback: scheduling?.allowPaidFallback ?? false,
      ...(allowedProviders === undefined ? {} : { allowedProviders }),
      requiredTools: [...input.requiredTools],
    },
    ...(input.affinity === undefined ? {} : { affinity: input.affinity }),
    ...(input.priorHandoff === undefined ? {} : { priorHandoff: input.priorHandoff }),
  })
}

/** Produce the deployment's fixed route when no scheduler decision is available. */
export function fixedProfileSchedule(
  worker: OrchestratorConfig['worker'],
  _mode: OrchestratorConfig['mode'],
  target: 'root' | 'worker',
): ScheduleDecisionV1 {
  const expectedMode = target === 'worker' ? 'single-worker' : 'direct'
  return parseScheduleDecisionV1({
    schemaVersion: 1,
    mode: expectedMode,
    route: {
      provider: worker.provider,
      model: worker.model,
      maxTokens: worker.maxTokens,
      ...(worker.reasoningEffort === undefined ? {} : { reasoningEffort: worker.reasoningEffort }),
    },
    workerCount: target === 'worker' ? 1 : 0,
    source: 'profile-fallback',
    policyVersion: PROFILE_FALLBACK_POLICY,
    explanationCode: 'PROFILE_FALLBACK',
  })
}

function routeIsAllowed(config: OrchestratorConfig, route: RouteDecisionV1): boolean {
  const scheduling = config.scheduling
  if (scheduling === undefined) return false
  const allowed = scheduling.allowedRoutes.find(candidate =>
    ROUTE_IDENTITY_AND_METADATA_KEYS.every(key => candidate[key] === route[key]),
  )
  return allowed !== undefined
    && route.maxTokens <= allowed.maxTokens
    && route.maxTokens <= config.worker.maxTokens
}

export function validateScheduleDecisionForConfig(
  decision: ScheduleDecisionV1,
  config: OrchestratorConfig,
  target: 'root' | 'worker',
): ScheduleDecisionV1 {
  const expectedMode = target === 'worker' ? 'single-worker' : 'direct'
  const expectedWorkers = target === 'worker' ? 1 : 0
  if (decision.mode !== expectedMode || decision.workerCount !== expectedWorkers || decision.source !== 'scheduler') {
    throw new TypeError('decision target shape is invalid')
  }
  if (!routeIsAllowed(config, decision.route)) throw new TypeError('decision route is not configured')
  return decision
}

/** Resolve one already-projected capability request through the optional scheduler. */
export async function resolveScheduleFromRequest(
  config: OrchestratorConfig,
  resolver: SchedulerResolver,
  request: CapabilityRequestV1,
  budget: BudgetViewV1,
  signal: AbortSignal,
): Promise<ResolvedScheduleV1> {
  throwIfAborted(signal)
  const scheduler = config.scheduling === undefined ? undefined : resolver.current()
  if (scheduler === undefined) {
    return Object.freeze({ request, decision: fixedProfileSchedule(config.worker, config.mode, request.target) })
  }
  const rawDecision = await scheduler.schedule(request, parseBudgetViewV1(budget), signal)
  if (signal.aborted) {
    if (request.target === 'worker') scheduler.complete?.(request.taskId)
    throwIfAborted(signal)
  }
  try {
    const decision = validateScheduleDecisionForConfig(
      parseScheduleDecisionV1(rawDecision),
      config,
      request.target,
    )
    return Object.freeze({ request, decision, scheduler })
  } catch (error) {
    if (request.target === 'worker') scheduler.complete?.(request.taskId)
    if (config.scheduling?.allowInvalidDecisionFallback === true) {
      return Object.freeze({
        request,
        decision: fixedProfileSchedule(config.worker, config.mode, request.target),
        scheduler,
      })
    }
    throw new SchedulingValidationError('SCHEDULE_DECISION_INVALID', { cause: error })
  }
}

/** Resolve scheduler-first while keeping invalid responses outside final budget admission. */
export async function resolveSchedule(
  config: OrchestratorConfig,
  resolver: SchedulerResolver,
  input: ResolveScheduleInput,
): Promise<ResolvedScheduleV1> {
  const request = buildCapabilityRequest(config, input)
  return resolveScheduleFromRequest(config, resolver, request, input.budget, input.signal)
}

interface RestoredScheduleSelection {
  readonly decision: ScheduleDecisionV1
  readonly selectedAt?: number
}

function restoreScheduleSelection(
  events: readonly SessionEvent[],
  config: OrchestratorConfig,
  target: 'root' | 'worker',
): RestoredScheduleSelection | undefined {
  if (config.scheduling === undefined) return undefined
  for (let index = events.length - 1; index >= 0; index--) {
    const event = events[index]
    if (event?.type !== 'dsh-plugin/schedule-selected') continue
    let selected: ScheduleSelectedV1
    try {
      selected = parseScheduleSelectedV1(event.data)
    } catch {
      continue
    }
    if (selected.target !== target) continue
    const route: RouteDecisionV1 = {
      provider: selected.provider,
      model: selected.model,
      maxTokens: selected.maxTokens,
      ...(selected.reasoningEffort === undefined ? {} : { reasoningEffort: selected.reasoningEffort }),
      ...(selected.promptProfile === undefined ? {} : { promptProfile: selected.promptProfile }),
      ...(selected.modelFamily === undefined ? {} : { modelFamily: selected.modelFamily }),
    }
    if (!routeIsAllowed(config, route)) continue
    return {
      decision: parseScheduleDecisionV1({
        schemaVersion: 1,
        mode: target === 'worker' ? 'single-worker' : 'direct',
        route,
        workerCount: target === 'worker' ? 1 : 0,
        source: selected.source,
        policyVersion: selected.policyVersion ?? PROFILE_FALLBACK_POLICY,
        explanationCode: 'DURABLE_STICKY_RESTORE',
      }),
      ...(Number.isSafeInteger(event.time) && event.time >= 0 ? { selectedAt: event.time } : {}),
    }
  }
  return undefined
}

/** Track an optional adaptive scheduler generation using Cordis's unloadable injection lifecycle. */
export function mountAdaptiveSchedulerResolver(ctx: Context): SchedulerResolver {
  let current: AdaptiveSchedulerService | undefined
  const injectOptional = (ctx as Context & { inject?: Context['inject'] }).inject
  if (typeof injectOptional === 'function') {
    injectOptional.call(ctx, ['adaptiveScheduler'], schedulerCtx => {
      const scheduler = schedulerCtx.get('adaptiveScheduler') as AdaptiveSchedulerService | undefined
      if (scheduler === undefined) return
      schedulerCtx.effect(() => {
        current = scheduler
        return () => {
          if (current === scheduler) current = undefined
        }
      }, 'ds-orchestrator: optional adaptive scheduler')
    })
  }
  return {
    current: () => current ?? (ctx.get('adaptiveScheduler') as AdaptiveSchedulerService | undefined),
  }
}

/** Project a validated decision into its durable, bounded selection provenance. */
export function scheduleSelectedFrom(decision: ScheduleDecisionV1, target: 'root' | 'worker'): ScheduleSelectedV1 {
  const selectedDecision = parseScheduleDecisionV1(decision)
  const expectedMode = target === 'worker' ? 'single-worker' : 'direct'
  const expectedWorkers = target === 'worker' ? 1 : 0
  if (selectedDecision.mode !== expectedMode || selectedDecision.workerCount !== expectedWorkers) {
    throw new TypeError('schedule selection target shape is invalid')
  }
  return parseScheduleSelectedV1({
    schemaVersion: 1,
    target,
    source: selectedDecision.source,
    provider: selectedDecision.route.provider,
    model: selectedDecision.route.model,
    maxTokens: selectedDecision.route.maxTokens,
    ...(selectedDecision.route.reasoningEffort === undefined ? {} : { reasoningEffort: selectedDecision.route.reasoningEffort }),
    ...(selectedDecision.route.promptProfile === undefined ? {} : { promptProfile: selectedDecision.route.promptProfile }),
    ...(selectedDecision.route.modelFamily === undefined ? {} : { modelFamily: selectedDecision.route.modelFamily }),
    policyVersion: selectedDecision.policyVersion,
  })
}

/** Restore the newest configured durable selection for one scheduling target. */
export function restoreScheduleSelected(
  events: readonly SessionEvent[],
  config: OrchestratorConfig,
  target: 'root' | 'worker',
): ScheduleDecisionV1 | undefined {
  return restoreScheduleSelection(events, config, target)?.decision
}

/** Enforce the selected root route at DSH's official agent/request waterfall seam. */
export function mountRootScheduling(
  ctx: Context,
  config: OrchestratorConfig,
  budgetRegistry: Pick<BudgetControllerRegistry, 'forRootSession'>,
  schedulerResolver: SchedulerResolver,
): () => void {
  if (config.scheduling === undefined) return () => undefined
  const hydratedSessions = new WeakMap<object, WeakSet<Session>>()
  return ctx.on('agent/request', async ({ agent, signal }, next): Promise<LlmCallConfig> => {
    const base = await next()
    const rootSessionId = agent.session.header.parentSession ?? agent.session.id
    if (agent.session.header.parentSession !== undefined) return base
    const input: ResolveScheduleInput = {
      target: 'root',
      taskId: String(rootSessionId),
      objective: 'root request',
      requiredTools: [],
      budget: budgetRegistry.forRootSession(rootSessionId).snapshot(),
      signal,
    }
    const scheduler = schedulerResolver.current()
    if (scheduler !== undefined) {
      let sessions = hydratedSessions.get(scheduler)
      if (sessions === undefined) {
        sessions = new WeakSet<Session>()
        hydratedSessions.set(scheduler, sessions)
      }
      if (!sessions.has(agent.session)) {
        sessions.add(agent.session)
        const restored = restoreScheduleSelection(agent.session.events, config, 'root')
        if (restored?.selectedAt !== undefined) {
          scheduler.hydrate?.(buildCapabilityRequest(config, input), restored.decision, restored.selectedAt)
        }
      }
    }
    const resolved = await resolveSchedule(config, { current: () => scheduler }, input)
    appendScheduleSelected(agent.session, scheduleSelectedFrom(resolved.decision, 'root'))
    const { reasoningEffort: _oldEffort, maxTokens: _oldMaxTokens, ...rest } = base
    return {
      ...rest,
      provider: resolved.decision.route.provider,
      model: resolved.decision.route.model,
      maxTokens: resolved.decision.route.maxTokens,
      ...(resolved.decision.route.reasoningEffort === undefined
        ? {}
        : { reasoningEffort: ReasoningEffortId(resolved.decision.route.reasoningEffort) }),
    }
  })
}
