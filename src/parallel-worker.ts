import type { Agent } from '@deepseek-ai/dsh-agent'
import type { SessionId } from '@deepseek-ai/dsh-session'
import type { SubagentRun, SubagentRuntime } from '@deepseek-ai/dsh-subagent'
import {
  MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES,
  serializedPayloadBytes,
  type OwnershipViolationSummaryV1,
  type ParallelNodeResultV1,
  type ScheduleSelectedV1,
  type TaskNodeV1,
} from '@han_05/dsh-scheduling-contracts'
import { appendScheduleSelected } from './events.js'
import {
  appendParallelWorkerFinished,
  appendParallelWorkerRequested,
  type ParallelWorkerFinishedV1,
  type ParallelWorkerRequestedV1,
} from './parallel-events.js'
import {
  checkOwnership,
  constructParallelHandoff,
  parseParallelHandoffEnvelopeRaw,
} from './parallel-handoff.js'
import { scheduleSelectedFrom, type ResolvedScheduleV1 } from './scheduling.js'
import { failedHandoff } from './handoff.js'
import type { HandoffV1, WorkerSpecV1 } from './types.js'
import { workerSpec, workerStartRequest } from './worker.js'

export interface ParallelWorkerRunInput {
  readonly node: TaskNodeV1
  readonly dagId: string
  readonly fanoutId: string
  readonly requestId: string
  readonly allowedTools: readonly string[]
  readonly resolvedSchedule: ResolvedScheduleV1
  readonly parent: Agent
  readonly signal: AbortSignal
  readonly subagents: Pick<SubagentRuntime, 'start'>
  readonly registerWorkerId: (workerId: SessionId) => string
}

export interface ParallelWorkerTerminalV1 {
  readonly nodeResult: ParallelNodeResultV1
  readonly acceptedHandoff?: HandoffV1
  readonly ownershipViolation?: OwnershipViolationSummaryV1
}

function correlation(input: ParallelWorkerRunInput) {
  return { fanoutId: input.fanoutId, nodeId: input.node.nodeId, requestId: input.requestId }
}

function parallelWorkerRequest(input: ParallelWorkerRunInput, spec: WorkerSpecV1): ParallelWorkerRequestedV1 {
  return { ...spec, ...correlation(input) }
}

function parallelScheduleSelected(input: ParallelWorkerRunInput): ScheduleSelectedV1 {
  return { ...scheduleSelectedFrom(input.resolvedSchedule.decision, 'worker'), ...correlation(input) }
}

function nodeResult(
  input: ParallelWorkerRunInput,
  status: ParallelNodeResultV1['status'],
  reason: ParallelNodeResultV1['reason'],
  workerRef?: string,
): ParallelNodeResultV1 {
  return {
    schemaVersion: 1,
    nodeId: input.node.nodeId,
    requestId: input.requestId,
    status,
    reason,
    ...(workerRef === undefined ? {} : { workerRef }),
  }
}

function beforePublication(input: ParallelWorkerRunInput, reason: 'cancelled-before-start' | 'start-failed'): ParallelWorkerTerminalV1 {
  return { nodeResult: nodeResult(input, reason === 'cancelled-before-start' ? 'not-run' : 'failed', reason) }
}

interface SettledParallelWorkerTerminalV1 extends ParallelWorkerTerminalV1 {
  readonly handoff: HandoffV1
}

function blockedHandoff(summary: string): HandoffV1 {
  return { ...failedHandoff(summary), status: 'blocked' }
}

function resultHandoff(stopReason: string): HandoffV1 {
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

function safeFailureTerminal(
  input: ParallelWorkerRunInput,
  workerRef: string,
  reason: 'blocked-result' | 'failed-result' | 'cancelled-after-start' | 'handoff-payload-too-large',
  handoff: HandoffV1,
): SettledParallelWorkerTerminalV1 {
  return {
    nodeResult: nodeResult(input, reason === 'cancelled-after-start' || reason === 'blocked-result' ? 'blocked' : 'failed', reason, workerRef),
    handoff,
  }
}

function settleHandoff(input: ParallelWorkerRunInput, workerRef: string, result: Awaited<SubagentRun['result']>): SettledParallelWorkerTerminalV1 {
  if (input.signal.aborted || result.stopReason === 'aborted') {
    return safeFailureTerminal(input, workerRef, 'cancelled-after-start', blockedHandoff('Worker was cancelled after publication.'))
  }
  if (result.stopReason !== 'completed') {
    return safeFailureTerminal(input, workerRef, result.stopReason === 'error' ? 'failed-result' : 'blocked-result', resultHandoff(result.stopReason))
  }

  let raw
  try {
    raw = parseParallelHandoffEnvelopeRaw(result.structured)
  } catch {
    return safeFailureTerminal(input, workerRef, 'failed-result', failedHandoff('Worker output failed HandoffV1 validation.'))
  }
  const ownership = checkOwnership(input.node.nodeId, raw.changedFiles, input.node.writePaths)
  if (!ownership.valid) {
    return {
      nodeResult: nodeResult(input, 'ownership-violation', 'violation', workerRef),
      ownershipViolation: ownership.summary,
      handoff: failedHandoff('Worker Handoff violated its declared ownership.'),
    }
  }

  const acceptedHandoff = constructParallelHandoff(raw, ownership.changedFiles)
  if (acceptedHandoff.status === 'blocked') {
    return safeFailureTerminal(input, workerRef, 'blocked-result', acceptedHandoff)
  }
  if (acceptedHandoff.status === 'failed') {
    return safeFailureTerminal(input, workerRef, 'failed-result', acceptedHandoff)
  }
  return {
    nodeResult: nodeResult(input, 'completed', 'completed', workerRef),
    acceptedHandoff,
    handoff: acceptedHandoff,
  }
}

function finishedEvent(input: ParallelWorkerRunInput, terminal: SettledParallelWorkerTerminalV1, workerRef: string): ParallelWorkerFinishedV1 {
  return {
    ...correlation(input),
    schemaVersion: 1,
    workerRef,
    handoff: terminal.handoff,
  }
}

interface WorkerFinishedPublicationV1 {
  readonly terminal: SettledParallelWorkerTerminalV1
  readonly event?: ParallelWorkerFinishedV1
}

function workerFinishedPublication(
  input: ParallelWorkerRunInput,
  terminal: SettledParallelWorkerTerminalV1,
  workerRef: string,
): WorkerFinishedPublicationV1 {
  const event = finishedEvent(input, terminal, workerRef)
  if (serializedPayloadBytes(event) > MAX_PARALLEL_WORKER_FINISHED_PAYLOAD_BYTES) {
    return {
      terminal: safeFailureTerminal(
        input,
        workerRef,
        'handoff-payload-too-large',
        failedHandoff('[handoff exceeds parallel worker-finished payload ceiling]'),
      ),
    }
  }
  return { terminal, event }
}

function disposalFailureCanReplace(terminal: SettledParallelWorkerTerminalV1): boolean {
  return terminal.nodeResult.reason !== 'cancelled-after-start'
    && terminal.nodeResult.reason !== 'violation'
    && terminal.nodeResult.reason !== 'handoff-payload-too-large'
}

function publicTerminal(terminal: SettledParallelWorkerTerminalV1): ParallelWorkerTerminalV1 {
  return {
    nodeResult: terminal.nodeResult,
    ...(terminal.acceptedHandoff === undefined ? {} : { acceptedHandoff: terminal.acceptedHandoff }),
    ...(terminal.ownershipViolation === undefined ? {} : { ownershipViolation: terminal.ownershipViolation }),
  }
}

async function settlePublishedRun(input: ParallelWorkerRunInput, run: SubagentRun, workerRef: string): Promise<SettledParallelWorkerTerminalV1> {
  let removeAbortListener: () => void = () => undefined
  const aborted = new Promise<{ readonly kind: 'aborted' }>(resolve => {
    if (input.signal.aborted) {
      resolve({ kind: 'aborted' })
      return
    }
    const onAbort = () => resolve({ kind: 'aborted' as const })
    input.signal.addEventListener('abort', onAbort, { once: true })
    removeAbortListener = () => input.signal.removeEventListener('abort', onAbort)
  })
  try {
    const outcome = await Promise.race([
      run.result.then(result => ({ kind: 'result' as const, result }), () => ({ kind: 'rejected' as const })),
      aborted,
    ])
    if (outcome.kind === 'aborted') {
      return safeFailureTerminal(input, workerRef, 'cancelled-after-start', blockedHandoff('Worker was cancelled after publication.'))
    }
    if (outcome.kind === 'rejected') {
      if (input.signal.aborted) {
        return safeFailureTerminal(input, workerRef, 'cancelled-after-start', blockedHandoff('Worker was cancelled after publication.'))
      }
      return safeFailureTerminal(input, workerRef, 'failed-result', failedHandoff('Worker failed before completion.'))
    }
    return settleHandoff(input, workerRef, outcome.result)
  } finally {
    removeAbortListener()
  }
}

/** Build the bounded one-shot spec for an already admitted leaf node. */
export function parallelWorkerSpec(input: ParallelWorkerRunInput): WorkerSpecV1 {
  if (input.allowedTools.includes('targeted_verify') || input.allowedTools.includes('delegate_worker')) {
    throw new TypeError('parallel workers must not allow verification or recursive delegation tools')
  }
  return workerSpec({ task: input.node.objective, allowedTools: input.allowedTools }, input.resolvedSchedule.decision.route)
}

/** Execute one already-admitted parallel leaf and publish only correlated durable evidence. */
export async function runParallelWorker(input: ParallelWorkerRunInput): Promise<ParallelWorkerTerminalV1> {
  const spec = parallelWorkerSpec(input)
  appendScheduleSelected(input.parent.session, parallelScheduleSelected(input))
  appendParallelWorkerRequested(input.parent.session, parallelWorkerRequest(input, spec))

  let run: SubagentRun
  try {
    run = await input.subagents.start('spawn', workerStartRequest(spec, input.parent, input.signal))
  } catch {
    return beforePublication(input, input.signal.aborted ? 'cancelled-before-start' : 'start-failed')
  }

  let disposalStarted = false
  const disposeOnce = async () => {
    if (disposalStarted) return
    disposalStarted = true
    await run.dispose()
  }

  try {
    const workerRef = input.registerWorkerId(run.id)
    let publication = workerFinishedPublication(input, await settlePublishedRun(input, run, workerRef), workerRef)
    try {
      await disposeOnce()
    } catch {
      if (disposalFailureCanReplace(publication.terminal)) {
        publication = workerFinishedPublication(
          input,
          safeFailureTerminal(input, workerRef, 'failed-result', failedHandoff('Worker cleanup failed before completion.')),
          workerRef,
        )
      }
    }
    if (publication.event !== undefined) {
      appendParallelWorkerFinished(input.parent.session, publication.event)
    }
    return publicTerminal(publication.terminal)
  } finally {
    try {
      await disposeOnce()
    } catch {
      // Preserve the primary registration, settlement, or append failure.
    }
  }
}
