import type { Agent } from '@deepseek-ai/dsh-agent'
import type { Context } from '@deepseek-ai/cordis'
import type { Session, SessionId } from '@deepseek-ai/dsh-session'
import type { SubagentRuntime } from '@deepseek-ai/dsh-subagent'
import { sha256Utf8 } from '@han_05/dsh-context'
import {
  MAX_DAG_ID_ORDINAL,
  MAX_DAG_LEVELS,
  MAX_DAG_NODES,
  parseParallelAggregateV1,
} from '@han_05/dsh-scheduling-contracts'
import type {
  DagValidationIssueV1,
  DagValidationLimitsV1,
  OwnershipViolationSummaryV1,
  ParallelAggregateV1,
  ParallelNodeResultV1,
  ParallelVerificationPolicyV1,
  TaskDagV1,
  TaskNodeV1,
} from '@han_05/dsh-scheduling-contracts'
import { buildParallelAggregate } from './aggregate.js'
import type { BudgetController, BudgetControllerRegistry } from './budgets.js'
import { validateTaskDagV1 } from './dag.js'
import {
  appendParallelFinished,
  appendParallelStarted,
  parseParallelStartedV1,
} from './parallel-events.js'
import type { PlannedParallelRequestV1 } from './parallel-events.js'
import {
  resolveParallelNodeSchedule,
  type ExecutableParallelNodeV1,
  type ParallelNodeClassificationV1,
} from './parallel-scheduling.js'
import {
  foldFinalLevelVerification,
  runParallelVerification,
  type ParallelVerificationResultV1,
} from './parallel-verification.js'
import { runParallelWorker, type ParallelWorkerTerminalV1 } from './parallel-worker.js'
import type { SchedulerResolver } from './scheduling.js'
import type { HandoffV1, OrchestratorConfig } from './types.js'
import type { VerificationService } from './verification.js'

const ROOT_SESSION_ID_MAX_BYTES = 48
const WORKER_REF_DIGEST_PATTERN = /^[0-9a-f]{32}$/u
const RUN_REQUEST_KEYS = ['dag', 'parent', 'signal'] as const
const GENERATION_DISPOSED_REASON = 'generation-disposed'

type ParallelRunValidationCode =
  | 'INVALID_ROOT_SESSION_ID'
  | 'DAG_ID_EXHAUSTED'
  | 'WORKER_REF_COLLISION'
  | 'INVALID_DAG'

export interface ParallelRunRequestV1 {
  readonly dag: TaskDagV1
  readonly parent: Agent
  readonly signal: AbortSignal
}

export interface ParallelExecutionService {
  run(request: ParallelRunRequestV1): Promise<ParallelRunResultV1>
}

declare module '@deepseek-ai/cordis' {
  interface Context {
    parallelExecution: ParallelExecutionService
  }
}

export interface ParallelRunResultV1 {
  readonly dagId: string
  readonly aggregates: readonly ParallelAggregateV1[]
  readonly finalAggregate: ParallelAggregateV1
}

export class ParallelRunValidationError extends Error {
  readonly code: ParallelRunValidationCode
  readonly issues?: readonly DagValidationIssueV1[]

  constructor(code: ParallelRunValidationCode, issues?: readonly DagValidationIssueV1[]) {
    super(code)
    this.name = 'ParallelRunValidationError'
    this.code = code
    if (issues !== undefined) {
      this.issues = Object.freeze(issues.map(issue => Object.freeze({ ...issue } as DagValidationIssueV1)))
    }
  }
}

/** Stable caller-visible error raised when the owning service generation is disposed. */
export class ParallelGenerationDisposedError extends Error {
  readonly code = 'GENERATION_DISPOSED' as const

  constructor() {
    super('parallel execution generation disposed')
    this.name = 'ParallelGenerationDisposedError'
  }
}

export interface ParallelRuntimeOptions {
  readonly config: OrchestratorConfig
  readonly budgetRegistry: Pick<BudgetControllerRegistry, 'forRootSession'>
  readonly schedulerResolver: SchedulerResolver
  readonly subagents: Pick<SubagentRuntime, 'start'>
  readonly appendAggregate: typeof appendParallelFinished
  /** Bind durable verification evidence to the parent session for one run. */
  readonly verificationServiceFor?: (session: Session) => VerificationService
  readonly workerRefDigest?: (workerId: string) => string
  /** Internal service-generation cancellation source installed by the lifecycle mount. */
  readonly generationSignal?: AbortSignal
}

export interface MountedParallelExecutionService {
  readonly service: ParallelExecutionService
  readonly dispose: () => Promise<void>
}

interface ManifestEntryV1 extends PlannedParallelRequestV1 {
  readonly levelIndex: number
}

interface ExecutableEntryV1 {
  readonly manifest: ManifestEntryV1
  readonly classification: ExecutableParallelNodeV1
}

interface DagRunStateV1 {
  readonly dagId: string
  readonly nodeOrder: readonly string[]
  readonly manifests: ReadonlyMap<string, ManifestEntryV1>
  readonly nodes: ReadonlyMap<string, TaskNodeV1>
  readonly nodeResults: Map<string, ParallelNodeResultV1>
  readonly acceptedHandoffs: Map<string, HandoffV1>
  readonly ownershipViolations: Map<string, OwnershipViolationSummaryV1>
  readonly workerRefs: Map<string, string>
  readonly aggregates: ParallelAggregateV1[]
}

interface SuccessfulClassificationV1 {
  readonly classification: ParallelNodeClassificationV1
  readonly manifest: ManifestEntryV1
}

type ClassificationOutcomeV1 =
  | SuccessfulClassificationV1
  | { readonly error: unknown }

type WorkerOutcomeV1 =
  | { readonly terminal: ParallelWorkerTerminalV1 }
  | { readonly error: unknown }

interface RunAbortScope {
  readonly signal: AbortSignal
  dispose(): void
}

function isRecord(value: unknown): value is Record<PropertyKey, unknown> {
  return typeof value === 'object' && value !== null && !Array.isArray(value)
}

function dataProperty(record: Record<PropertyKey, unknown>, key: string, path: string): unknown {
  const descriptor = Object.getOwnPropertyDescriptor(record, key)
  if (descriptor === undefined) throw new TypeError(`${path}.${key} is required`)
  if (!Object.prototype.hasOwnProperty.call(descriptor, 'value')) {
    throw new TypeError(`${path}.${key} must be a data property`)
  }
  return descriptor.value
}

function isAbortSignal(value: unknown): value is AbortSignal {
  return isRecord(value)
    && typeof value.aborted === 'boolean'
    && typeof value.addEventListener === 'function'
    && typeof value.removeEventListener === 'function'
}

function parseParallelRunEnvelope(value: unknown): ParallelRunRequestV1 {
  if (!isRecord(value)) throw new TypeError('parallel run request must be an object')
  const allowed = new Set<string>(RUN_REQUEST_KEYS)
  for (const key of Reflect.ownKeys(value)) {
    if (typeof key !== 'string' || !allowed.has(key)) {
      throw new TypeError(`parallel run request.${String(key)} is unknown`)
    }
  }

  const dag = dataProperty(value, 'dag', 'parallel run request')
  const parent = dataProperty(value, 'parent', 'parallel run request')
  const signal = dataProperty(value, 'signal', 'parallel run request')
  if (!isRecord(dag) || !Array.isArray(dag.nodes)) {
    throw new TypeError('parallel run request.dag must be an already-parsed TaskDagV1')
  }
  if (!isRecord(parent) || !isRecord(parent.session)) {
    throw new TypeError('parallel run request.parent must have a session')
  }
  const session = parent.session
  if (typeof session.id !== 'string' || !Array.isArray(session.events) || !isRecord(session.header)) {
    throw new TypeError('parallel run request.parent.session is invalid')
  }
  if (!isAbortSignal(signal)) throw new TypeError('parallel run request.signal must be an AbortSignal')

  return { dag: dag as unknown as TaskDagV1, parent: parent as unknown as Agent, signal }
}

function runAbortScope(caller: AbortSignal, generation: AbortSignal | undefined): RunAbortScope {
  const controller = new AbortController()
  const sources = generation === undefined ? [caller] : [caller, generation]
  const listeners: Array<readonly [AbortSignal, () => void]> = []
  for (const source of sources) {
    const forward = () => {
      if (!controller.signal.aborted) controller.abort(source.reason)
    }
    if (source.aborted) {
      forward()
      break
    }
    source.addEventListener('abort', forward, { once: true })
    listeners.push([source, forward])
  }
  return {
    signal: controller.signal,
    dispose() {
      for (const [source, listener] of listeners) source.removeEventListener('abort', listener)
    },
  }
}

function throwIfGenerationDisposed(signal: AbortSignal | undefined): void {
  if (signal?.aborted) throw new ParallelGenerationDisposedError()
}

function guardedSubagents(
  subagents: Pick<SubagentRuntime, 'start'>,
  signal: AbortSignal,
): Pick<SubagentRuntime, 'start'> {
  return {
    start(provider, request) {
      if (signal.aborted) return Promise.reject(signal.reason)
      return subagents.start(provider, request)
    },
  }
}

function rootSessionId(session: Session): SessionId {
  return session.header.parentSession ?? session.id
}

function assertRootSessionId(session: Session): SessionId {
  const rootId = rootSessionId(session)
  const valid = rootId.length > 0
    && rootId.length <= ROOT_SESSION_ID_MAX_BYTES
    && rootId.trim().length > 0
    && !rootId.includes('\0')
    && [...rootId].every(character => character.codePointAt(0)! <= 0x7f)
  if (!valid) throw new ParallelRunValidationError('INVALID_ROOT_SESSION_ID')
  return rootId
}

function anchoredOrdinal(dagId: string, rootId: string): number | undefined {
  const prefix = `${rootId}:dag:`
  if (!dagId.startsWith(prefix)) return undefined
  const suffix = dagId.slice(prefix.length)
  if (!/^(?:[1-9]|[1-9]\d{1,2})$/u.test(suffix)) return undefined
  const ordinal = Number(suffix)
  return ordinal <= MAX_DAG_ID_ORDINAL ? ordinal : undefined
}

/** Allocate the next same-session DAG ordinal from parser-valid durable anchors. */
export function allocateDagId(session: Session): { readonly dagId: string; readonly ordinal: number } {
  const rootId = assertRootSessionId(session)
  let maximum = 0
  for (const event of session.events) {
    let dagId: string
    try {
      if (event.type === 'dsh-plugin/parallel-started') {
        dagId = parseParallelStartedV1(event.data).dagId
      } else if (event.type === 'dsh-plugin/parallel-finished') {
        dagId = parseParallelAggregateV1(event.data).dagId
      } else {
        continue
      }
    } catch {
      continue
    }
    const ordinal = anchoredOrdinal(dagId, rootId)
    if (ordinal !== undefined) maximum = Math.max(maximum, ordinal)
  }

  const ordinal = maximum + 1
  if (ordinal > MAX_DAG_ID_ORDINAL) throw new ParallelRunValidationError('DAG_ID_EXHAUSTED')
  return Object.freeze({ dagId: `${rootId}:dag:${ordinal}`, ordinal })
}

function dagValidationLimits(config: OrchestratorConfig): DagValidationLimitsV1 {
  return {
    schemaVersion: 1,
    maxNodes: MAX_DAG_NODES,
    maxLevels: MAX_DAG_LEVELS,
    maxWidth: config.parallel!.maxParallelWorkers,
    maxCumulativeWorkers: config.budgets.maxWorkers,
  }
}

function plannedManifest(
  dagId: string,
  levels: readonly (readonly string[])[],
  scope: 'level' | 'dag',
): readonly ManifestEntryV1[] {
  const entries: ManifestEntryV1[] = []
  for (const [levelIndex, level] of levels.entries()) {
    const fanoutId = scope === 'level' ? `${dagId}:level:${levelIndex}` : `${dagId}:aggregate`
    for (const nodeId of level) {
      entries.push(Object.freeze({
        fanoutId,
        levelIndex,
        nodeId,
        requestId: `${dagId}:node:${nodeId}`,
      }))
    }
  }
  return Object.freeze(entries)
}

function syntheticNotRun(manifest: ManifestEntryV1, reason: ParallelNodeResultV1['reason']): ParallelNodeResultV1 {
  return Object.freeze({
    schemaVersion: 1,
    nodeId: manifest.nodeId,
    requestId: manifest.requestId,
    status: 'not-run',
    reason,
  })
}

function createRunState(dagId: string, dag: TaskDagV1, manifest: readonly ManifestEntryV1[]): DagRunStateV1 {
  return {
    dagId,
    nodeOrder: Object.freeze(manifest.map(entry => entry.nodeId)),
    manifests: new Map(manifest.map(entry => [entry.nodeId, entry])),
    nodes: new Map(dag.nodes.map(node => [node.nodeId, node])),
    nodeResults: new Map(),
    acceptedHandoffs: new Map(),
    ownershipViolations: new Map(),
    workerRefs: new Map(),
    aggregates: [],
  }
}

function applyDependencyReadiness(state: DagRunStateV1, level: readonly string[]): readonly string[] {
  const ready: string[] = []
  for (const nodeId of level) {
    const node = state.nodes.get(nodeId)!
    const manifest = state.manifests.get(nodeId)!
    const dependenciesReady = node.dependsOn.every(dependency =>
      state.nodeResults.get(dependency)?.status === 'completed',
    )
    if (dependenciesReady) ready.push(nodeId)
    else state.nodeResults.set(nodeId, syntheticNotRun(manifest, 'dependency-not-run'))
  }
  return ready
}

function completeSchedule(classification: ExecutableParallelNodeV1): void {
  classification.resolvedSchedule.scheduler?.complete?.(classification.resolvedSchedule.request.taskId)
}

async function classifyReadyNodes(
  options: ParallelRuntimeOptions,
  state: DagRunStateV1,
  readyNodeIds: readonly string[],
  budget: BudgetController,
  signal: AbortSignal,
): Promise<readonly ExecutableEntryV1[]> {
  const budgetView = budget.snapshot()
  const outcomes = await Promise.all(readyNodeIds.map(async (nodeId): Promise<ClassificationOutcomeV1> => {
    const manifest = state.manifests.get(nodeId)!
    try {
      const classification = await resolveParallelNodeSchedule(
        options.config,
        options.schedulerResolver,
        state.nodes.get(nodeId)!,
        {
          dagId: state.dagId,
          requestId: manifest.requestId,
          workerAffinityId: manifest.requestId,
          budget: budgetView,
          signal,
        },
      )
      return { classification, manifest }
    } catch (error) {
      return { error }
    }
  }))

  const firstError = outcomes.find((outcome): outcome is { readonly error: unknown } => 'error' in outcome)
  if (firstError !== undefined) {
    for (const outcome of outcomes) {
      if ('classification' in outcome && outcome.classification.kind === 'executable') {
        completeSchedule(outcome.classification)
      }
    }
    throw firstError.error
  }

  const executable: ExecutableEntryV1[] = []
  for (const outcome of outcomes as readonly SuccessfulClassificationV1[]) {
    if (outcome.classification.kind === 'not-run') {
      state.nodeResults.set(
        outcome.manifest.nodeId,
        syntheticNotRun(outcome.manifest, outcome.classification.reason),
      )
    } else {
      executable.push({ manifest: outcome.manifest, classification: outcome.classification })
    }
  }
  return executable
}

function completeSchedules(executable: readonly ExecutableEntryV1[]): void {
  let firstError: unknown
  for (const item of executable) {
    try {
      completeSchedule(item.classification)
    } catch (error) {
      if (firstError === undefined) firstError = error
    }
  }
  if (firstError !== undefined) throw firstError
}

function applyAdmissionStop(
  state: DagRunStateV1,
  executable: readonly ExecutableEntryV1[],
  remainingLevels: readonly (readonly string[])[],
): void {
  for (const item of executable) {
    state.nodeResults.set(item.manifest.nodeId, syntheticNotRun(item.manifest, 'admission-rejected'))
  }
  for (const level of remainingLevels) {
    for (const nodeId of level) {
      if (state.nodeResults.has(nodeId)) continue
      const manifest = state.manifests.get(nodeId)!
      state.nodeResults.set(nodeId, syntheticNotRun(manifest, 'admission-rejected'))
    }
  }
}

function applyLevelVerificationStop(
  state: DagRunStateV1,
  remainingLevels: readonly (readonly string[])[],
): void {
  for (const level of remainingLevels) {
    for (const nodeId of level) {
      if (state.nodeResults.has(nodeId)) continue
      const manifest = state.manifests.get(nodeId)!
      state.nodeResults.set(nodeId, syntheticNotRun(manifest, 'level-verification-stopped'))
    }
  }
}

function workerRefRegistrar(
  state: DagRunStateV1,
  digestWorkerId: (workerId: string) => string,
): (workerId: SessionId) => string {
  return workerId => {
    const fullWorkerId = String(workerId)
    const digest = digestWorkerId(fullWorkerId)
    if (!WORKER_REF_DIGEST_PATTERN.test(digest)) {
      throw new TypeError('workerRefDigest must return 32 lowercase hexadecimal characters')
    }
    const workerRef = `w:${digest}`
    if (state.workerRefs.has(workerRef) || [...state.workerRefs.values()].includes(fullWorkerId)) {
      throw new ParallelRunValidationError('WORKER_REF_COLLISION')
    }
    state.workerRefs.set(workerRef, fullWorkerId)
    return workerRef
  }
}

async function runExecutableLevel(
  options: ParallelRuntimeOptions,
  state: DagRunStateV1,
  executable: readonly ExecutableEntryV1[],
  parent: Agent,
  signal: AbortSignal,
  subagents: Pick<SubagentRuntime, 'start'>,
  registerWorkerId: (workerId: SessionId) => string,
): Promise<void> {
  const outcomes = await Promise.all(executable.map(async (item): Promise<WorkerOutcomeV1> => {
    let terminal: ParallelWorkerTerminalV1 | undefined
    let failure: unknown
    try {
      terminal = await runParallelWorker({
        node: item.classification.node,
        dagId: state.dagId,
        fanoutId: item.manifest.fanoutId,
        requestId: item.manifest.requestId,
        allowedTools: item.classification.allowedTools,
        resolvedSchedule: item.classification.resolvedSchedule,
        parent,
        signal,
        subagents,
        registerWorkerId,
      })
    } catch (error) {
      failure = error
    }
    try {
      completeSchedule(item.classification)
    } catch (error) {
      if (failure === undefined) failure = error
    }
    return failure === undefined ? { terminal: terminal! } : { error: failure }
  }))

  const firstError = outcomes.find((outcome): outcome is { readonly error: unknown } => 'error' in outcome)
  if (firstError !== undefined) throw firstError.error

  for (const [index, outcome] of outcomes.entries()) {
    const terminal = (outcome as { readonly terminal: ParallelWorkerTerminalV1 }).terminal
    const nodeId = executable[index]!.manifest.nodeId
    state.nodeResults.set(nodeId, terminal.nodeResult)
    if (terminal.acceptedHandoff !== undefined) state.acceptedHandoffs.set(nodeId, terminal.acceptedHandoff)
    if (terminal.ownershipViolation !== undefined) state.ownershipViolations.set(nodeId, terminal.ownershipViolation)
  }
}

function selectedMap<T>(source: ReadonlyMap<string, T>, nodeIds: readonly string[]): ReadonlyMap<string, T> {
  const selected = new Map<string, T>()
  for (const nodeId of nodeIds) {
    const value = source.get(nodeId)
    if (value !== undefined) selected.set(nodeId, value)
  }
  return selected
}

function nodeResultsFor(state: DagRunStateV1, nodeIds: readonly string[]): readonly ParallelNodeResultV1[] {
  return nodeIds.map(nodeId => {
    const result = state.nodeResults.get(nodeId)
    if (result === undefined) throw new Error(`parallel node ${nodeId} has no terminal result`)
    return result
  })
}

function buildAggregate(
  state: DagRunStateV1,
  scope: 'level' | 'dag',
  nodeIds: readonly string[],
  verification: ParallelVerificationResultV1,
  levelIndex?: number,
): ParallelAggregateV1 {
  const fanoutId = scope === 'level' ? `${state.dagId}:level:${levelIndex}` : `${state.dagId}:aggregate`
  return buildParallelAggregate({
    dagId: state.dagId,
    scope,
    fanoutId,
    ...(scope === 'level' ? { levelId: fanoutId, levelIndex } : {}),
    nodeResults: nodeResultsFor(state, nodeIds),
    nodeOrder: nodeIds,
    acceptedHandoffs: selectedMap(state.acceptedHandoffs, nodeIds),
    ownershipViolations: nodeIds.flatMap(nodeId => {
      const summary = state.ownershipViolations.get(nodeId)
      return summary === undefined ? [] : [summary]
    }),
    verificationOutcome: verification.outcome,
    ...(verification.evidence.length === 0 ? {} : { verification: verification.evidence }),
  })
}

function appendAggregate(
  options: ParallelRuntimeOptions,
  state: DagRunStateV1,
  session: Session,
  aggregate: ParallelAggregateV1,
): void {
  options.appendAggregate(session, aggregate)
  state.aggregates.push(aggregate)
}

function defaultWorkerRefDigest(workerId: string): string {
  return sha256Utf8(workerId).slice('sha256:'.length, 'sha256:'.length + 32)
}

function acceptedNodeCount(state: DagRunStateV1, nodeIds: readonly string[]): number {
  return nodeIds.reduce((count, nodeId) => count + (state.acceptedHandoffs.has(nodeId) ? 1 : 0), 0)
}

function isActualVerificationInvocation(result: ParallelVerificationResultV1): boolean {
  return result.outcome !== 'not-run-no-commands' && result.outcome !== 'not-run-no-accepted-nodes'
}

async function verifyAcceptedNodes(
  policy: ParallelVerificationPolicyV1,
  budget: BudgetController,
  service: VerificationService | undefined,
  signal: AbortSignal,
  acceptedCount: number,
): Promise<ParallelVerificationResultV1> {
  if (policy.commands.length === 0) return { outcome: 'not-run-no-commands', evidence: [] }
  if (service === undefined) throw new TypeError('parallel verification requires verificationServiceFor')
  return runParallelVerification({ policy, controller: budget, service, signal }, acceptedCount)
}

/** Create the bounded DAG executor with integrated verification. */
export function createParallelExecutionRuntime(options: ParallelRuntimeOptions): ParallelExecutionService {
  const parallel = options.config.parallel
  if (parallel === undefined) throw new TypeError('parallel execution requires config.parallel')
  if (parallel.verification.commands.length !== 0 && options.verificationServiceFor === undefined) {
    throw new TypeError('parallel verification commands require verificationServiceFor')
  }
  const digestWorkerId = options.workerRefDigest ?? defaultWorkerRefDigest

  return Object.freeze({
    async run(requestValue: ParallelRunRequestV1): Promise<ParallelRunResultV1> {
      const request = parseParallelRunEnvelope(requestValue)
      const abortScope = runAbortScope(request.signal, options.generationSignal)
      let committed = false
      try {
        throwIfGenerationDisposed(options.generationSignal)
        const rootId = assertRootSessionId(request.parent.session)
        const validation = validateTaskDagV1(request.dag, dagValidationLimits(options.config))
        if (!validation.valid) {
          throw new ParallelRunValidationError('INVALID_DAG', validation.issues)
        }

        throwIfGenerationDisposed(options.generationSignal)
        const { dagId } = allocateDagId(request.parent.session)
        const manifest = plannedManifest(dagId, validation.levels, parallel.verification.scope)
        appendParallelStarted(request.parent.session, {
          schemaVersion: 1,
          dagId,
          requests: manifest.map(({ fanoutId, nodeId, requestId }) => ({ fanoutId, nodeId, requestId })),
        })

        const state = createRunState(dagId, request.dag, manifest)
        const budget = options.budgetRegistry.forRootSession(rootId)
        const registerWorkerId = workerRefRegistrar(state, digestWorkerId)
        const verificationService = parallel.verification.commands.length === 0
          ? undefined
          : options.verificationServiceFor!(request.parent.session)
        const verificationInvocations: ParallelVerificationResultV1[] = []
        const runSubagents = guardedSubagents(options.subagents, abortScope.signal)

        for (const [levelIndex, level] of validation.levels.entries()) {
          throwIfGenerationDisposed(options.generationSignal)
          const readyNodeIds = applyDependencyReadiness(state, level)
          const executable = await classifyReadyNodes(options, state, readyNodeIds, budget, abortScope.signal)
          throwIfGenerationDisposed(options.generationSignal)
          if (executable.length === 0) continue

          const admission = budget.admitFanout(executable.length)
          if (!admission.allowed) {
            applyAdmissionStop(state, executable, validation.levels.slice(levelIndex + 1))
            completeSchedules(executable)
            break
          }

          throwIfGenerationDisposed(options.generationSignal)
          await runExecutableLevel(
            options,
            state,
            executable,
            request.parent,
            abortScope.signal,
            runSubagents,
            registerWorkerId,
          )
          throwIfGenerationDisposed(options.generationSignal)

          if (parallel.verification.scope === 'level') {
            const verification = await verifyAcceptedNodes(
              parallel.verification,
              budget,
              verificationService,
              abortScope.signal,
              acceptedNodeCount(state, level),
            )
            throwIfGenerationDisposed(options.generationSignal)
            if (isActualVerificationInvocation(verification)) verificationInvocations.push(verification)
            appendAggregate(
              options,
              state,
              request.parent.session,
              buildAggregate(state, 'level', level, verification, levelIndex),
            )
            if (verification.outcome === 'command-failed' || verification.outcome === 'admission-rejected') {
              applyLevelVerificationStop(state, validation.levels.slice(levelIndex + 1))
              break
            }
          }
        }

        const finalVerification = parallel.verification.scope === 'level'
          ? foldFinalLevelVerification(parallel.verification, verificationInvocations)
          : await verifyAcceptedNodes(
            parallel.verification,
            budget,
            verificationService,
            abortScope.signal,
            acceptedNodeCount(state, state.nodeOrder),
          )
        throwIfGenerationDisposed(options.generationSignal)
        const finalAggregate = buildAggregate(state, 'dag', state.nodeOrder, finalVerification)
        const result = Object.freeze({
          dagId,
          aggregates: Object.freeze([...state.aggregates, finalAggregate]),
          finalAggregate,
        })
        throwIfGenerationDisposed(options.generationSignal)
        appendAggregate(options, state, request.parent.session, finalAggregate)
        committed = true
        return result
      } catch (error) {
        if (!committed && options.generationSignal?.aborted) {
          throw new ParallelGenerationDisposedError()
        }
        throw error
      } finally {
        abortScope.dispose()
      }
    },
  })
}

/** Register one generation-scoped internal parallel service and drain it before unregistering. */
export function mountParallelExecutionService(
  ctx: Context,
  options: ParallelRuntimeOptions,
): MountedParallelExecutionService {
  const generation = new AbortController()
  const active = new Set<Promise<unknown>>()
  let disposing = false
  const runtime = createParallelExecutionRuntime({ ...options, generationSignal: generation.signal })
  const service: ParallelExecutionService = Object.freeze({
    run(request: ParallelRunRequestV1) {
      if (disposing) return Promise.reject(new ParallelGenerationDisposedError())
      const promise = Promise.resolve().then(() => runtime.run(request))
      active.add(promise)
      void promise.then(
        () => { active.delete(promise) },
        () => { active.delete(promise) },
      )
      return promise
    },
  })
  const lifecycle = ctx.effect(function* () {
    const unregister = ctx.provide('parallelExecution', service)
    yield unregister
    yield async () => {
      disposing = true
      generation.abort(GENERATION_DISPOSED_REASON)
      await Promise.allSettled([...active])
    }
  }, 'ds-orchestrator: parallel execution generation')

  return Object.freeze({
    service,
    async dispose() {
      await lifecycle()
    },
  })
}
