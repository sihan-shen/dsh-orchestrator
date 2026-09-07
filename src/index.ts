import type { Context } from '@deepseek-ai/cordis'
import type {} from '@deepseek-ai/dsh-session'
import type {} from '@deepseek-ai/dsh-subagent'
import type {} from '@deepseek-ai/dsh-subprocess'
import type {} from '@deepseek-ai/dsh-tools'
import { mountBudgetControllerRegistry } from './budgets.js'
import { Config } from './config.js'
import { mountDirectMode } from './direct.js'
import { appendBudgetRejected } from './events.js'
import { mountTargetedVerificationTool } from './verification.js'
import { mountSingleWorkerMode } from './worker.js'
import { mountAdaptiveSchedulerResolver, mountRootScheduling } from './scheduling.js'
import type { OrchestratorConfig } from './types.js'

export { BudgetController, createBudgetControllerRegistry, mountBudgetControllerRegistry } from './budgets.js'
export type {
  BudgetAllowed,
  BudgetControllerRegistry,
  BudgetDecision,
  BudgetRejected,
  BudgetRejection,
  BudgetRejectionCode,
  BudgetRejectionRecorder,
  BudgetRejectionRecorderFactory,
  MountedBudgetControllerRegistry,
  PluginToolAction,
} from './budgets.js'
export { Config, parseConfig, routeToolFilterKey } from './config.js'
export {
  CONTEXT_PROMPT_ORDER,
  CONTEXT_PROMPT_SECTION,
  CONTEXT_TOOL_NAMES,
  mountContextIntegration,
} from './context.js'
export { DIRECT_PROMPT_ORDER, DIRECT_PROMPT_SECTION, mountDirectMode } from './direct.js'
export { compareDagValidationIssues, validateTaskDagV1 } from './dag.js'
export {
  appendBudgetRejected,
  appendScheduleSelected,
  appendRunStarted,
  appendVerificationFinished,
  appendWorkerFinished,
  appendWorkerRequested,
} from './events.js'
export {
  appendParallelFinished,
  appendParallelStarted,
  appendParallelWorkerFinished,
  appendParallelWorkerRequested,
  parseParallelStartedV1,
  parseWorkerFinishedV1,
  parseWorkerRequestedV1,
} from './parallel-events.js'
export type {
  BudgetRejectedInput,
  BudgetRejectedV1,
  RunStartedInput,
  RunStartedV1,
  WorkerFinishedV1,
} from './events.js'
export type {
  CorrelationTripleV1,
  ExpectedEventBranch,
  LegacyWorkerFinishedV1,
  LegacyWorkerRequestedV1,
  ParallelStartedV1,
  ParallelWorkerFinishedV1,
  ParallelWorkerRequestedV1,
  PlannedParallelRequestV1,
  WorkerFinishedEventV1,
  WorkerRequestedV1,
} from './parallel-events.js'
export { failedHandoff, normalizeWorkerOutput, parseHandoff } from './handoff.js'
export {
  boundOwnershipViolationSection,
  buildParallelAggregate,
  projectAggregateHandoff,
  projectAggregateVerification,
} from './aggregate.js'
export type { BuildParallelAggregateInput } from './aggregate.js'
export {
  checkOwnership,
  classifyOwnershipToken,
  constructParallelHandoff,
  parseParallelHandoffEnvelopeRaw,
} from './parallel-handoff.js'
export type {
  OwnershipCheckV1,
  OwnershipViolationTokenV1,
  RawParallelHandoffEnvelopeV1,
} from './parallel-handoff.js'
export {
  createTargetedVerificationTool,
  mountTargetedVerificationTool,
  VerificationService,
  validateParallelVerificationPolicy,
  validateVerificationArguments,
  VERIFICATION_CLEANUP_ALLOWANCE_MS,
  VERIFICATION_TERMINATION_GRACE_MS,
} from './verification.js'
export {
  createDelegateWorkerTool,
  HANDOFF_V1_JSON_SCHEMA,
  mountSingleWorkerMode,
  parseDelegateWorkerInput,
  runWorker,
  workerSpec,
  SINGLE_WORKER_STARTUP_TIMEOUT_MS,
} from './worker.js'
export type {
  DelegateWorkerInput,
  DelegateWorkerToolOptions,
  RunWorkerOptions,
} from './worker.js'
export type {
  TargetedVerificationToolOptions,
  VerificationEvidenceAppender,
  VerificationServiceOptions,
} from './verification.js'
export type {
  ContextBlockV1,
  ContextCompiler,
  HandoffV1,
  OrchestratorConfig,
  OrchestratorSchedulingConfig,
  ParallelConfigV1,
  VerificationCommand,
  VerificationEvidenceV1,
  WorkerSpecV1,
} from './types.js'
export {
  buildCapabilityRequest,
  fixedProfileSchedule,
  mountAdaptiveSchedulerResolver,
  mountRootScheduling,
  resolveSchedule,
  resolveScheduleFromRequest,
  restoreScheduleSelected,
  scheduleSelectedFrom,
  SchedulingValidationError,
  validateScheduleDecisionForConfig,
} from './scheduling.js'
export {
  buildParallelCapabilityRequest,
  resolveParallelNodeSchedule,
} from './parallel-scheduling.js'
export { parallelWorkerSpec, runParallelWorker } from './parallel-worker.js'
export type { ParallelWorkerRunInput, ParallelWorkerTerminalV1 } from './parallel-worker.js'
export {
  allocateDagId,
  createParallelExecutionRuntime,
  mountParallelExecutionService,
  ParallelGenerationDisposedError,
  ParallelRunValidationError,
} from './parallel.js'
export type {
  MountedParallelExecutionService,
  ParallelExecutionService,
  ParallelRunRequestV1,
  ParallelRunResultV1,
  ParallelRuntimeOptions,
} from './parallel.js'
export {
  foldFinalLevelVerification,
  runParallelVerification,
} from './parallel-verification.js'
export type {
  ParallelVerificationResultV1,
  ParallelVerificationRunnerOptions,
} from './parallel-verification.js'
export type {
  ResolveScheduleInput,
  ResolvedScheduleV1,
  SchedulerResolver,
} from './scheduling.js'
export type {
  ExecutableParallelNodeV1,
  ParallelNodeClassificationV1,
  ParallelScheduleContextV1,
  RejectedParallelNodeV1,
} from './parallel-scheduling.js'

/** Stable Cordis plugin name for the DSH v0.1 orchestration bundle. */
export const name = 'ds-orchestrator'

/** Required services for the v0.1 Direct runtime. */
export const inject = ['systemPrompt', 'tools', 'sessions', 'subprocess']

/**
 * Mount the v0.1 bundle entry point.
 * @param ctx - Cordis context that owns session lifecycle and teardown.
 * @param config - Validated deployment configuration whose budget limits are enforced.
 */
export const apply = (ctx: Context, config: OrchestratorConfig): void | Promise<void> => {
  const budgets = mountBudgetControllerRegistry(ctx, config.budgets, rootSessionId => rejection => {
    const session = ctx.sessions.get(rootSessionId)
    if (session === undefined) return
    appendBudgetRejected(session, {
      reason: rejection.code,
      limit: rejection.limit,
      observed: rejection.observed,
    })
  })
  const schedulerResolver = mountAdaptiveSchedulerResolver(ctx)
  mountRootScheduling(ctx, config, budgets.registry, schedulerResolver)
  mountTargetedVerificationTool(ctx, {
    workspaceRoot: config.workspaceRoot,
    verification: config.verification,
    subprocess: ctx.subprocess,
    budgetRegistry: budgets.registry,
  })
  if (config.mode === 'direct') {
    mountDirectMode(ctx, config)
  } else {
    return mountSingleWorkerMode(ctx, config, budgets.registry, schedulerResolver)
  }
}

apply.Config = Config
apply.inject = inject
