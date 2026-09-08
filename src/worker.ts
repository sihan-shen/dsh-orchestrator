import type { Context } from '@deepseek-ai/cordis'
import type { Agent, AgentOptions } from '@deepseek-ai/dsh-agent'
import { boundContextSummary, createUserMessage } from '@deepseek-ai/dsh-llm'
import type { Session, SessionId } from '@deepseek-ai/dsh-session'
import type { SubagentCapabilities, SubagentRun, SubagentRuntime, SubagentStartRequest } from '@deepseek-ai/dsh-subagent'
import type {} from '@deepseek-ai/dsh-subprocess'
import type { ObjectJsonSchema, ToolDefinition } from '@deepseek-ai/dsh-tools'
import type { ScheduleFeedbackV1, RouteDecisionV1 } from '@han_05/dsh-scheduling-contracts'
import { parseScheduleFeedbackV1 } from '@han_05/dsh-scheduling-contracts'
import type { BudgetControllerRegistry, BudgetRejected } from './budgets.js'
import { MAX_HANDOFF_ITEMS, MAX_HANDOFF_STRING_BYTES } from './config.js'
import { mountContextIntegration } from './context.js'
import { appendRunStarted, appendScheduleSelected, appendVerificationFinished, appendWorkerFinished, appendWorkerRequested } from './events.js'
import { failedHandoff, normalizeWorkerOutput } from './handoff.js'
import { appendParallelFinished } from './parallel-events.js'
import { mountParallelExecutionService } from './parallel.js'
import { resolveSchedule, scheduleSelectedFrom, type ResolvedScheduleV1, type SchedulerResolver } from './scheduling.js'
import { parseRequestRoute, type HandoffV1, type OrchestratorConfig, type WorkerSpecV1 } from './types.js'
import { VerificationService } from './verification.js'

/** Exact structured result contract requested from every v0.1 child worker. */
export const HANDOFF_V1_JSON_SCHEMA: ObjectJsonSchema = {
  type: 'object',
  additionalProperties: false,
  properties: {
    schemaVersion: { type: 'integer', const: 1 },
    status: { type: 'string', enum: ['completed', 'blocked', 'failed'] },
    summary: { type: 'string' },
    changedFiles: { type: 'array', items: { type: 'string' } },
    decisions: { type: 'array', items: { type: 'string' } },
    verification: {
      type: 'array',
      items: {
        type: 'object',
        additionalProperties: false,
        properties: {
          schemaVersion: { type: 'integer', const: 1 },
          commandName: { type: 'string' },
          args: { type: 'array', items: { type: 'string' } },
          exitCode: { oneOf: [{ type: 'integer' }, { type: 'null' }] },
          status: { type: 'string', enum: ['passed', 'failed', 'timed-out', 'spawn-error'] },
          stdout: { type: 'string' },
          stderr: { type: 'string' },
          truncated: { type: 'boolean' },
          durationMs: { type: 'integer' },
        },
        required: ['schemaVersion', 'commandName', 'args', 'exitCode', 'status', 'stdout', 'stderr', 'truncated', 'durationMs'],
      },
    },
    blockers: { type: 'array', items: { type: 'string' } },
  },
  required: ['schemaVersion', 'status', 'summary', 'changedFiles', 'decisions', 'verification', 'blockers'],
}

/** Maximum time a Single Worker deployment waits for the required subagents service. */
export const SINGLE_WORKER_STARTUP_TIMEOUT_MS = 5_000

/** The bounded caller input allowed by the foreground delegation tool. */
export interface DelegateWorkerInput {
  readonly task: string
  readonly allowedTools: readonly string[]
}

/** Dependencies required to run one already-admitted foreground worker. */
export interface RunWorkerOptions extends DelegateWorkerInput {
  readonly config: OrchestratorConfig
  readonly resolvedSchedule: ResolvedScheduleV1
  readonly parent: Agent
  readonly signal: AbortSignal
  readonly subagents: Pick<SubagentRuntime, 'start' | 'getProvider'>
}

/** Dependencies required to define the model-facing delegation tool. */
export interface DelegateWorkerToolOptions {
  readonly config: OrchestratorConfig
  readonly subagents: Pick<SubagentRuntime, 'start' | 'getProvider'>
  readonly budgetRegistry: Pick<BudgetControllerRegistry, 'forRootSession'>
  readonly schedulerResolver: SchedulerResolver
}

const workerPromptInstruction = [
  'Return only a JSON HandoffV1 object matching the required output schema.',
  'Do not return a transcript, credentials, tool output, or diagnostic text.',
].join(' ')

const textEncoder = new TextEncoder()

function throwReason(signal: AbortSignal): never {
  throw signal.reason ?? new DOMException('Worker cancelled', 'AbortError')
}

function boundedString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.trim() === '') throw new TypeError(`${name} must be a non-empty string`)
  if (value.includes('\0')) throw new TypeError(`${name} must not contain a NUL byte`)
  if (textEncoder.encode(value).byteLength > MAX_HANDOFF_STRING_BYTES) {
    throw new TypeError(`${name} must not exceed ${MAX_HANDOFF_STRING_BYTES} UTF-8 bytes`)
  }
  return value
}

/** Validate tool-shaped worker input before any budget admission or child publication. */
export function parseDelegateWorkerInput(value: unknown): DelegateWorkerInput {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    throw new TypeError('delegate_worker arguments must be an object')
  }
  const input = value as { task?: unknown; allowedTools?: unknown }
  for (const key of Object.keys(input)) {
    if (key !== 'task' && key !== 'allowedTools') {
      throw new TypeError(`delegate_worker arguments contain unsupported property: ${key}`)
    }
  }
  if (!Array.isArray(input.allowedTools)) throw new TypeError('delegate_worker allowedTools must be an array')
  if (input.allowedTools.length > MAX_HANDOFF_ITEMS) {
    throw new TypeError(`delegate_worker allowedTools must not contain more than ${MAX_HANDOFF_ITEMS} items`)
  }
  const allowedTools = input.allowedTools.map((tool, index) => boundedString(tool, `delegate_worker allowedTools[${index}]`))
  if (new Set(allowedTools).size !== allowedTools.length) {
    throw new TypeError('delegate_worker allowedTools must not contain duplicates')
  }
  if (allowedTools.includes('delegate_worker')) {
    throw new TypeError('delegate_worker must not allow recursive delegation')
  }
  return { task: boundedString(input.task, 'delegate_worker task'), allowedTools }
}

function blockedHandoff(message: string): HandoffV1 {
  return { ...failedHandoff(message), status: 'blocked' }
}

function failedForStopReason(stopReason: string): HandoffV1 {
  switch (stopReason) {
    case 'aborted':
      return blockedHandoff('Worker was cancelled before completion.')
    case 'max-tokens':
      return blockedHandoff('Worker reached its configured token limit before completion.')
    case 'refusal':
      return blockedHandoff('Worker declined the delegated task.')
    case 'error':
      return failedHandoff('Worker failed before completion.')
    default:
      return failedHandoff('Worker ended with an unsupported stop reason.')
  }
}

function failedStart(signal: AbortSignal): HandoffV1 {
  return signal.aborted
    ? blockedHandoff('Worker was cancelled before publication.')
    : failedHandoff('Worker could not be started.')
}

export function workerSpec(input: DelegateWorkerInput, resolvedRoute: RouteDecisionV1): WorkerSpecV1 {
  return {
    schemaVersion: 1,
    task: input.task,
    provider: resolvedRoute.provider,
    model: resolvedRoute.model,
    ...(resolvedRoute.reasoningEffort === undefined ? {} : { reasoningEffort: resolvedRoute.reasoningEffort }),
    maxTokens: resolvedRoute.maxTokens,
    allowedTools: [...input.allowedTools],
    expectedOutput: 'handoff-v1',
  }
}

/** Build the one-shot child request shared by legacy and parallel workers. */
export function workerStartRequest(
  spec: WorkerSpecV1,
  parent: Agent,
  signal: AbortSignal,
  capabilities: SubagentCapabilities,
): SubagentStartRequest {
  const agentOptions: AgentOptions = {
    provider: spec.provider,
    model: spec.model,
    maxTokens: spec.maxTokens,
  }
  return {
    prompt: [{
      type: 'text',
      text: `${spec.task}\n\n${workerPromptInstruction}`,
    }],
    parent,
    signal,
    ...(capabilities.agentOptions ? { agentOptions } : {}),
    ...(capabilities.outputSchema ? { outputSchema: HANDOFF_V1_JSON_SCHEMA } : {}),
    ...(capabilities.depthLimit ? { maxDepth: 1 } : {}),
    ...(capabilities.toolFilter ? { toolFilter: { allow: [...spec.allowedTools] } } : {}),
  }
}

function acceptedHandoff(result: Awaited<SubagentRun['result']>, workspaceRoot: string): HandoffV1 {
  if (result.stopReason !== 'completed') return failedForStopReason(result.stopReason)
  return normalizeWorkerOutput(result.structured, workspaceRoot)
}

function handoffProjection(handoff: HandoffV1): string {
  return JSON.stringify({
    status: handoff.status,
    summary: handoff.summary,
    changedFiles: handoff.changedFiles,
    decisions: handoff.decisions,
    verification: handoff.verification,
    blockers: handoff.blockers,
  })
}

function handoffContext(handoff: HandoffV1) {
  const text = handoffProjection(handoff)
  return createUserMessage({
    content: [{ type: 'text', text }],
    source: {
      kind: 'plugin',
      plugin: 'ds-orchestrator',
      form: 'notice',
      summary: boundContextSummary(handoff.summary),
    },
  })
}

function budgetFeedback(requestId: string, rejection: BudgetRejected): ScheduleFeedbackV1 {
  return parseScheduleFeedbackV1({
    schemaVersion: 1,
    requestId,
    outcome: 'budget-rejected',
    budgetRejection: {
      code: rejection.code,
      limit: rejection.limit,
      observed: rejection.observed,
    },
  })
}

function handoffFeedback(requestId: string, handoff: HandoffV1): ScheduleFeedbackV1 {
  const outcome = handoff.status === 'completed'
    ? handoff.verification.some(item => item.status !== 'passed') ? 'verification-failed' : 'completed'
    : handoff.status
  return parseScheduleFeedbackV1({
    schemaVersion: 1,
    requestId,
    outcome,
    handoff,
    verification: handoff.verification,
  })
}

/**
 * Start one already-admitted foreground child, persist only validated evidence, and return the bounded handoff.
 * @param options - Validated deployment config, bounded input, root parent, and the caller's cancellation signal.
 * @returns A valid HandoffV1 even when child startup or execution fails.
 */
export async function runWorker(options: RunWorkerOptions): Promise<HandoffV1> {
  const input = parseDelegateWorkerInput({ task: options.task, allowedTools: options.allowedTools })
  if (options.signal.aborted) return blockedHandoff('Worker was cancelled before publication.')

  const spec = workerSpec(input, options.resolvedSchedule.decision.route)
  const provider = options.subagents.getProvider('spawn')
  if (provider?.capabilities.agentOptions === false) {
    return failedHandoff('Worker provider does not support route overrides.')
  }
  appendWorkerRequested(options.parent.session, spec)

  let run: SubagentRun | undefined
  try {
    run = await options.subagents.start(
      'spawn',
      workerStartRequest(spec, options.parent, options.signal, provider?.capabilities ?? {
        agentOptions: true,
        outputSchema: true,
        depthLimit: true,
        toolFilter: true,
        persona: false,
      }),
    )
  } catch {
    return failedStart(options.signal)
  }

  let handoff: HandoffV1
  try {
    const result = await run.result
    handoff = acceptedHandoff(result, options.config.workspaceRoot)
  } catch {
    handoff = options.signal.aborted
      ? blockedHandoff('Worker was cancelled before completion.')
      : failedHandoff('Worker failed before completion.')
  }
  try {
    await run.dispose()
  } catch {
    handoff = failedHandoff('Worker cleanup failed before completion.')
  }

  appendWorkerFinished(options.parent.session, run.id, handoff)
  return handoff
}

function handoffText(value: unknown): string {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return 'Worker handoff unavailable.'
  const handoff = value as Record<string, unknown>
  const status = typeof handoff.status === 'string' ? handoff.status : 'unknown'
  const summary = typeof handoff.summary === 'string' ? handoff.summary : ''
  return `Worker handoff: ${status}\n${summary}`
}

/** Create the foreground-only model tool that performs budget admission before child start. */
export function createDelegateWorkerTool(options: DelegateWorkerToolOptions): ToolDefinition {
  if (options.config.mode !== 'single-worker') {
    throw new TypeError('delegate_worker requires mode "single-worker"')
  }
  return {
    name: 'delegate_worker',
    description: 'Start one foreground worker and return its structured handoff, never a transcript.',
    parameters: {
      type: 'object',
      additionalProperties: false,
      properties: {
        task: { type: 'string' },
        allowedTools: { type: 'array', items: { type: 'string' } },
      },
      required: ['task', 'allowedTools'],
    },
    output: {
      schema: HANDOFF_V1_JSON_SCHEMA,
      render: (_args, value) => [{ type: 'text', text: handoffText(value) }],
    },
    timeoutMs: options.config.budgets.toolTimeoutMs,
    async execute(rawArgs, exec) {
      const input = parseDelegateWorkerInput(rawArgs)
      if (exec.signal.aborted) throwReason(exec.signal)
      const parent = exec.agent
      if (parent === undefined) throw new Error('delegate_worker requires an active parent agent')
      const rootSessionId = parent.session.header.parentSession ?? parent.session.id
      const workerTaskId = `worker:${crypto.randomUUID()}`
      const budget = options.budgetRegistry.forRootSession(rootSessionId)
      const resolvedSchedule = await resolveSchedule(options.config, options.schedulerResolver, {
        target: 'worker',
        taskId: workerTaskId,
        objective: input.task,
        requiredTools: input.allowedTools,
        affinity: { workerId: `${rootSessionId}:worker:1` },
        budget: budget.snapshot(),
        signal: exec.signal,
      })
      try {
        if (exec.signal.aborted) throwReason(exec.signal)
        const action = budget.admitPluginTool('delegate_worker')
        if (!action.allowed) {
          resolvedSchedule.scheduler?.observe?.(budgetFeedback(resolvedSchedule.request.taskId, action))
          throw new Error(`delegate_worker rejected by budget: ${action.code}`)
        }
        const worker = budget.admitWorker()
        if (!worker.allowed) {
          resolvedSchedule.scheduler?.observe?.(budgetFeedback(resolvedSchedule.request.taskId, worker))
          throw new Error(`delegate_worker rejected by budget: ${worker.code}`)
        }
        appendScheduleSelected(parent.session, scheduleSelectedFrom(resolvedSchedule.decision, 'worker'))
        const handoff = await runWorker({ ...input, config: options.config, resolvedSchedule, parent, signal: exec.signal, subagents: options.subagents })
        resolvedSchedule.scheduler?.observe?.(handoffFeedback(resolvedSchedule.request.taskId, handoff))
        if (!exec.signal.aborted) exec.deferContext(handoffContext(handoff))
        return handoff
      } finally {
        resolvedSchedule.scheduler?.complete?.(resolvedSchedule.request.taskId)
      }
    },
    presentCall: rawArgs => {
      try {
        const input = parseDelegateWorkerInput(rawArgs)
        return {
          card: 'generic',
          title: 'Delegate foreground worker',
          kind: 'execute',
          rawInput: input,
        }
      } catch {
        return undefined
      }
    },
  }
}

/** Register the Single Worker tool only while the configured mode is active. */
export function mountSingleWorkerMode(
  ctx: Context,
  config: OrchestratorConfig,
  budgetRegistry: Pick<BudgetControllerRegistry, 'forRootSession'>,
  schedulerResolver: SchedulerResolver,
): Promise<void> {
  if (config.mode !== 'single-worker') throw new TypeError('mountSingleWorkerMode requires mode "single-worker"')
  mountContextIntegration(ctx, config)
  return new Promise((resolve, reject) => {
    let settled = false
    let timeout: ReturnType<typeof setTimeout> | undefined
    const settle = (callback: () => void) => {
      if (settled) return false
      settled = true
      if (timeout !== undefined) clearTimeout(timeout)
      callback()
      return true
    }
    const injection = ctx.inject(['subagents'], workerCtx => {
      settle(resolve)
      workerCtx.effect(() => {
        const pending = new Map<SessionId, Session>()
        let active = true
        const disposeTool = workerCtx.tools.register(createDelegateWorkerTool({ config, subagents: workerCtx.subagents, budgetRegistry, schedulerResolver }))
        const parallelContext = ctx.extend({ fiber: workerCtx.fiber })
        const parallel = config.parallel === undefined
          ? undefined
          : mountParallelExecutionService(parallelContext, {
            config,
            budgetRegistry,
            schedulerResolver,
            subagents: workerCtx.subagents,
            appendAggregate: appendParallelFinished,
            verificationServiceFor: session => new VerificationService({
              workspaceRoot: config.workspaceRoot,
              verification: config.verification,
              subprocess: workerCtx.subprocess,
              appendEvidence: evidence => appendVerificationFinished(session, evidence),
            }),
          })
        const disposeEvents = workerCtx.on('session/event', (session, event) => {
          if (event.type !== 'request/header' || session.header.parentSession !== undefined) return
          // The durable run record is derived only from this actual request snapshot.
          const route = parseRequestRoute(event.data.header)
          if (route === undefined) return
          if (session.snapshotEvents().some(entry => entry.type === 'dsh-plugin/run-started') || pending.has(session.id)) return
          pending.set(session.id, session)
          queueMicrotask(() => {
            if (!active || pending.get(session.id) !== session) return
            pending.delete(session.id)
            appendRunStarted(session, { mode: 'single-worker', provider: route.provider, model: route.model })
          })
        })
        const disposeSessions = workerCtx.on('session/disposed', session => {
          if (pending.get(session.id) === session) pending.delete(session.id)
        })
        return async () => {
          const drainingParallel = parallel?.dispose()
          active = false
          disposeTool()
          disposeEvents()
          disposeSessions()
          pending.clear()
          await drainingParallel
        }
      }, 'ds-orchestrator: single worker mode')
    })
    let injectionDispose: Promise<void> | undefined
    const disposeInjection = () => injectionDispose ??= injection.dispose()
    timeout = setTimeout(() => {
      if (!settle(() => reject(new TypeError('single-worker subagents startup timeout')))) return
      void disposeInjection()
    }, SINGLE_WORKER_STARTUP_TIMEOUT_MS)
    ctx.effect(() => async () => {
      settle(resolve)
      await disposeInjection()
    }, 'ds-orchestrator: single worker startup')
  })
}
