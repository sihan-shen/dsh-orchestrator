import {
  MAX_AGGREGATE_PATH_BYTES,
  MAX_AGGREGATE_PAYLOAD_BYTES,
  MAX_AGGREGATE_PROJECTED_HANDOFF_BYTES,
  MAX_AGGREGATE_VERIFICATION_METADATA_SERIALIZED_BYTES,
  MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES,
  MAX_AGGREGATE_VERIFICATION_TOTAL_BYTES,
  MAX_AGGREGATE_VIOLATION_BYTES,
  MAX_VERIFICATION_COMMANDS,
  deriveAggregateStatus,
  parseParallelAggregateV1,
  serializedPayloadBytes,
  utf8ByteLength,
} from '@ds-plugins/dsh-scheduling-contracts'
import type {
  OwnershipViolationSummaryV1,
  ParallelAggregateV1,
  ParallelNodeResultV1,
  VerificationOutcomeV1,
} from '@ds-plugins/dsh-scheduling-contracts'
import { MAX_HANDOFF_ITEMS } from './config.js'
import type { HandoffV1, VerificationEvidenceV1 } from './types.js'

const VERIFICATION_FAILED_MARKER = '[verification: failed]'
const VERIFICATION_BUDGET_REJECTED_MARKER = '[verification: budget-rejected]'
const SUMMARY_TRUNCATED_MARKER = '[summary truncated]'
const MAX_AGGREGATE_NODE_RESULTS_AND_ENVELOPE_BYTES = 16_384
const MAX_AGGREGATE_VERIFICATION_RECORD_BYTES =
  MAX_AGGREGATE_VERIFICATION_METADATA_SERIALIZED_BYTES + MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES

export interface BuildParallelAggregateInput {
  readonly dagId: string
  readonly scope: 'level' | 'dag'
  readonly fanoutId: string
  readonly levelId?: string
  readonly levelIndex?: number
  readonly nodeResults: readonly ParallelNodeResultV1[]
  /** Canonical level-then-lexical node order supplied by the DAG executor. */
  readonly nodeOrder: readonly string[]
  readonly acceptedHandoffs: ReadonlyMap<string, HandoffV1>
  readonly ownershipViolations: readonly OwnershipViolationSummaryV1[]
  readonly verificationOutcome: VerificationOutcomeV1
  readonly verification?: readonly VerificationEvidenceV1[]
}

interface TruncationResult<T> {
  readonly items: readonly T[]
  readonly truncated: boolean
}

function lexicalCompare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0
}

function deepFreeze<T>(value: T): T {
  if (typeof value !== 'object' || value === null || Object.isFrozen(value)) return value
  for (const child of Object.values(value as Record<string, unknown>)) deepFreeze(child)
  return Object.freeze(value)
}

function takeUtf8Prefix(value: string, maximumBytes: number): string {
  if (maximumBytes <= 0) return ''
  let result = ''
  let bytes = 0
  for (const character of value) {
    const characterBytes = utf8ByteLength(character)
    if (bytes + characterBytes > maximumBytes) break
    result += character
    bytes += characterBytes
  }
  return result
}

function cloneVerification(
  evidence: VerificationEvidenceV1,
  stdout: string = evidence.stdout,
  stderr: string = evidence.stderr,
): VerificationEvidenceV1 {
  const sourceOutputBytes = utf8ByteLength(evidence.stdout) + utf8ByteLength(evidence.stderr)
  const retainedOutputBytes = utf8ByteLength(stdout) + utf8ByteLength(stderr)
  return {
    schemaVersion: 1,
    commandName: evidence.commandName,
    args: [...evidence.args],
    exitCode: evidence.exitCode,
    status: evidence.status,
    stdout,
    stderr,
    truncated: evidence.truncated || retainedOutputBytes < sourceOutputBytes,
    durationMs: evidence.durationMs,
  }
}

function verificationMetadata(evidence: VerificationEvidenceV1): Record<string, unknown> {
  return {
    schemaVersion: 1,
    commandName: evidence.commandName,
    args: [...evidence.args],
    exitCode: evidence.exitCode,
    status: evidence.status,
    durationMs: evidence.durationMs,
  }
}

function outputForBudget(evidence: VerificationEvidenceV1, maximumBytes: number): { readonly stdout: string; readonly stderr: string } {
  const stdoutBytes = Math.min(utf8ByteLength(evidence.stdout), maximumBytes)
  const stdout = takeUtf8Prefix(evidence.stdout, stdoutBytes)
  const stderr = takeUtf8Prefix(evidence.stderr, Math.max(0, maximumBytes - utf8ByteLength(stdout)))
  return { stdout, stderr }
}

function verificationSectionFits(evidence: readonly VerificationEvidenceV1[]): boolean {
  return serializedPayloadBytes(evidence) <= MAX_AGGREGATE_VERIFICATION_TOTAL_BYTES
}

function verificationOutputFits(evidence: VerificationEvidenceV1): boolean {
  return utf8ByteLength(evidence.stdout) + utf8ByteLength(evidence.stderr) <= MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES
}

function verificationRecordFits(evidence: VerificationEvidenceV1): boolean {
  return serializedPayloadBytes(evidence) <= MAX_AGGREGATE_VERIFICATION_RECORD_BYTES
}

function verificationCandidateFits(
  candidate: VerificationEvidenceV1,
  section: readonly VerificationEvidenceV1[],
): boolean {
  return verificationOutputFits(candidate)
    && verificationRecordFits(candidate)
    && verificationSectionFits(section)
}

/** Project ordered verification evidence under serialized section and output budgets. */
export function projectAggregateVerification(
  evidence: readonly VerificationEvidenceV1[],
): readonly VerificationEvidenceV1[] {
  if (evidence.length > MAX_VERIFICATION_COMMANDS) {
    throw new TypeError(`aggregate verification must not contain more than ${MAX_VERIFICATION_COMMANDS} items`)
  }

  for (const [index, item] of evidence.entries()) {
    const metadataBytes = serializedPayloadBytes(verificationMetadata(item))
    if (metadataBytes > MAX_AGGREGATE_VERIFICATION_METADATA_SERIALIZED_BYTES) {
      throw new TypeError(`aggregate verification[${index}] metadata exceeds ${MAX_AGGREGATE_VERIFICATION_METADATA_SERIALIZED_BYTES} bytes`)
    }
  }

  const projected = evidence.map(item => cloneVerification(item, '', ''))
  if (projected.some(item => !verificationRecordFits(item))) {
    throw new TypeError('aggregate verification metadata cannot fit its serialized record budget')
  }
  if (!verificationSectionFits(projected)) {
    throw new TypeError('aggregate verification metadata exceeds its serialized budget')
  }

  for (const [index, source] of evidence.entries()) {
    const sourceOutputBytes = utf8ByteLength(source.stdout) + utf8ByteLength(source.stderr)
    const maximumOutputBytes = Math.min(MAX_AGGREGATE_VERIFICATION_OUTPUT_BYTES, sourceOutputBytes)
    let low = 0
    let high = maximumOutputBytes
    let best = cloneVerification(source, '', '')

    while (low <= high) {
      const candidateBytes = Math.floor((low + high) / 2)
      const output = outputForBudget(source, candidateBytes)
      const candidate = cloneVerification(source, output.stdout, output.stderr)
      const candidateSection = [...projected]
      candidateSection[index] = candidate
      if (verificationCandidateFits(candidate, candidateSection)) {
        best = candidate
        low = candidateBytes + 1
      } else {
        high = candidateBytes - 1
      }
    }
    projected[index] = best
  }

  return deepFreeze(projected)
}

function canonicalOwnershipSummary(summary: OwnershipViolationSummaryV1): OwnershipViolationSummaryV1 {
  const samplePaths = [...new Set(summary.samplePaths)]
    .filter(path => utf8ByteLength(path) <= MAX_AGGREGATE_PATH_BYTES)
    .sort(lexicalCompare)
  return {
    nodeId: summary.nodeId,
    count: summary.count,
    digest: summary.digest,
    samplePaths,
  }
}

/** Bound ownership samples globally, retaining either every complete sample set or none. */
export function boundOwnershipViolationSection(
  summaries: readonly OwnershipViolationSummaryV1[],
): readonly OwnershipViolationSummaryV1[] {
  const canonical = [...summaries]
    .map(canonicalOwnershipSummary)
    .sort((left, right) => lexicalCompare(left.nodeId, right.nodeId))

  if (serializedPayloadBytes(canonical) <= MAX_AGGREGATE_VIOLATION_BYTES) {
    return deepFreeze(canonical)
  }

  return deepFreeze(canonical.map(summary => ({
    ...summary,
    samplePaths: [],
  })))
}

function orderedNodeResults(input: BuildParallelAggregateInput): readonly ParallelNodeResultV1[] {
  if (!Array.isArray(input.nodeOrder)) {
    throw new TypeError('nodeOrder is required and must be a complete canonical sequence')
  }
  if (input.nodeOrder.length !== input.nodeResults.length) {
    throw new TypeError('nodeOrder must contain exactly one entry for every node result')
  }

  const results = new Map<string, ParallelNodeResultV1>()
  for (const result of input.nodeResults) {
    if (results.has(result.nodeId)) throw new TypeError(`nodeResults contains duplicate nodeId ${result.nodeId}`)
    results.set(result.nodeId, result)
  }

  const ordered: ParallelNodeResultV1[] = []
  const seen = new Set<string>()
  for (const nodeId of input.nodeOrder) {
    if (seen.has(nodeId)) throw new TypeError(`nodeOrder contains duplicate nodeId ${nodeId}`)
    const result = results.get(nodeId)
    if (result === undefined) throw new TypeError(`nodeOrder references unknown nodeId ${nodeId}`)
    seen.add(nodeId)
    ordered.push(result)
  }
  if (seen.size !== results.size) throw new TypeError('nodeOrder must cover every node result')
  return ordered
}

function assertAcceptedHandoffs(
  input: BuildParallelAggregateInput,
  nodeResults: readonly ParallelNodeResultV1[],
): void {
  const completedNodeIds = new Set(
    nodeResults.filter(result => result.status === 'completed').map(result => result.nodeId),
  )
  if (input.acceptedHandoffs.size !== completedNodeIds.size) {
    throw new TypeError('acceptedHandoffs keys must exactly match completed nodeResults')
  }
  for (const [nodeId, handoff] of input.acceptedHandoffs) {
    if (!completedNodeIds.has(nodeId)) {
      throw new TypeError(`acceptedHandoffs contains a non-completed or unknown nodeId ${nodeId}`)
    }
    if (handoff.status !== 'completed') {
      throw new TypeError(`acceptedHandoffs[${nodeId}] must be a completed Handoff`)
    }
  }
  for (const nodeId of completedNodeIds) {
    if (!input.acceptedHandoffs.has(nodeId)) {
      throw new TypeError(`acceptedHandoffs is missing completed nodeId ${nodeId}`)
    }
  }
}

function acceptedHandoffsInNodeOrder(
  input: BuildParallelAggregateInput,
  nodeResults: readonly ParallelNodeResultV1[],
): readonly HandoffV1[] {
  assertAcceptedHandoffs(input, nodeResults)
  const handoffs: HandoffV1[] = []
  for (const result of nodeResults) {
    if (result.status !== 'completed') continue
    handoffs.push(input.acceptedHandoffs.get(result.nodeId)!)
  }
  return handoffs
}

function sortedUniqueStrings(values: Iterable<string>): readonly string[] {
  return [...new Set(values)].sort(lexicalCompare)
}

function retainWholeItems<T>(items: readonly T[], maximumBytes: number): TruncationResult<T> {
  const retained: T[] = []
  let truncated = false
  for (const item of items) {
    if (retained.length >= MAX_HANDOFF_ITEMS) {
      truncated = true
      break
    }
    const candidate = [...retained, item]
    if (serializedPayloadBytes(candidate) > maximumBytes) {
      truncated = true
      break
    }
    retained.push(item)
  }
  return { items: retained, truncated }
}

function retainSummary(summary: string): { readonly value: string; readonly truncated: boolean } {
  if (serializedPayloadBytes(summary) <= 4_096) return { value: summary, truncated: false }

  const characters = [...summary]
  let low = 0
  let high = characters.length
  let best = SUMMARY_TRUNCATED_MARKER
  while (low <= high) {
    const prefixLength = Math.floor((low + high) / 2)
    const prefix = characters.slice(0, prefixLength).join('')
    const candidate = prefix.length === 0
      ? SUMMARY_TRUNCATED_MARKER
      : `${prefix}\n${SUMMARY_TRUNCATED_MARKER}`
    if (serializedPayloadBytes(candidate) <= 4_096) {
      best = candidate
      low = prefixLength + 1
    } else {
      high = prefixLength - 1
    }
  }
  return { value: best, truncated: true }
}

function retainBlockers(
  ordinaryBlockers: readonly string[],
  verificationOutcome: VerificationOutcomeV1,
): TruncationResult<string> {
  if (verificationOutcome !== 'admission-rejected') {
    return retainWholeItems(ordinaryBlockers, 4_096)
  }

  const marker = VERIFICATION_BUDGET_REJECTED_MARKER
  const intended = ordinaryBlockers.includes(marker)
    ? ordinaryBlockers
    : [marker, ...ordinaryBlockers]
  const retained: string[] = [marker]
  let truncated = false
  for (const blocker of ordinaryBlockers) {
    if (blocker === marker) continue
    if (retained.length >= MAX_HANDOFF_ITEMS) {
      truncated = true
      break
    }
    const candidate = [...retained, blocker]
    if (serializedPayloadBytes(candidate) > 4_096) {
      truncated = true
      break
    }
    retained.push(blocker)
  }
  if (retained.length < intended.length) truncated = true
  return {
    items: retained.sort(lexicalCompare),
    truncated,
  }
}

/** Build the deterministic, bounded parent-visible Handoff projection. */
export function projectAggregateHandoff(
  input: BuildParallelAggregateInput,
  aggregateStatus: ParallelAggregateV1['aggregateStatus'],
): { readonly handoff: HandoffV1; readonly truncated: boolean } {
  const nodeResults = orderedNodeResults(input)
  const acceptedHandoffs = acceptedHandoffsInNodeOrder(input, nodeResults)

  const summaries = acceptedHandoffs.map(handoff => handoff.summary)
  let intendedSummary = summaries.join('\n')
  if (acceptedHandoffs.length === 0 && aggregateStatus === 'blocked') {
    intendedSummary = 'No accepted parallel nodes.'
  }
  if (aggregateStatus === 'verification-failed' && !intendedSummary.startsWith(VERIFICATION_FAILED_MARKER)) {
    intendedSummary = intendedSummary.length === 0
      ? VERIFICATION_FAILED_MARKER
      : `${VERIFICATION_FAILED_MARKER}\n${intendedSummary}`
  }
  const summary = retainSummary(intendedSummary)

  const changedFiles = sortedUniqueStrings(acceptedHandoffs.flatMap(handoff => handoff.changedFiles))
  const decisions = sortedUniqueStrings(acceptedHandoffs.flatMap(handoff => handoff.decisions))
  const blockers = sortedUniqueStrings(acceptedHandoffs.flatMap(handoff => handoff.blockers))
  const retainedChangedFiles = retainWholeItems(changedFiles, 8_192)
  const retainedDecisions = retainWholeItems(decisions, 4_096)
  const retainedBlockers = retainBlockers(blockers, input.verificationOutcome)

  const intendedVerification = (input.verification ?? []).map(evidence => cloneVerification(evidence))
  const retainedVerification = retainWholeItems(intendedVerification, 2_048)

  const handoff: HandoffV1 = {
    schemaVersion: 1,
    status: aggregateStatus === 'completed' ? 'completed' : aggregateStatus === 'blocked' ? 'blocked' : 'failed',
    summary: summary.value,
    changedFiles: retainedChangedFiles.items,
    decisions: retainedDecisions.items,
    verification: retainedVerification.items,
    blockers: retainedBlockers.items,
  }
  if (serializedPayloadBytes(handoff) > MAX_AGGREGATE_PROJECTED_HANDOFF_BYTES) {
    throw new TypeError('projected aggregate Handoff exceeds its serialized budget')
  }

  return {
    handoff: deepFreeze(handoff),
    truncated: summary.truncated
      || retainedChangedFiles.truncated
      || retainedDecisions.truncated
      || retainedBlockers.truncated
      || retainedVerification.truncated,
  }
}

function assertNodeResultsAndEnvelopeBudget(
  input: BuildParallelAggregateInput,
  nodeResults: readonly ParallelNodeResultV1[],
  aggregateStatus: ParallelAggregateV1['aggregateStatus'],
): void {
  const fragment = {
    schemaVersion: 1 as const,
    dagId: input.dagId,
    scope: input.scope,
    fanoutId: input.fanoutId,
    ...(input.scope === 'level' ? { levelId: input.levelId, levelIndex: input.levelIndex } : {}),
    nodeResults,
    aggregateStatus,
    verificationOutcome: input.verificationOutcome,
  }
  const actualBytes = serializedPayloadBytes(fragment)
  if (actualBytes > MAX_AGGREGATE_NODE_RESULTS_AND_ENVELOPE_BYTES) {
    throw new TypeError(`aggregate nodeResults and envelope fragment exceeds ${MAX_AGGREGATE_NODE_RESULTS_AND_ENVELOPE_BYTES} bytes`)
  }
}

/** Construct and parser-validate one deterministic ParallelAggregateV1. */
export function buildParallelAggregate(input: BuildParallelAggregateInput): ParallelAggregateV1 {
  const nodeResults = orderedNodeResults(input)
  const verification = input.verification === undefined
    ? undefined
    : projectAggregateVerification(input.verification)
  const ownershipViolations = boundOwnershipViolationSection(input.ownershipViolations)
  const aggregateStatus = deriveAggregateStatus(nodeResults, input.verificationOutcome)
  assertNodeResultsAndEnvelopeBudget(input, nodeResults, aggregateStatus)
  const projection = projectAggregateHandoff({
    ...input,
    nodeResults,
    ownershipViolations,
    verification,
  }, aggregateStatus)

  const rawAggregate = {
    schemaVersion: 1 as const,
    dagId: input.dagId,
    scope: input.scope,
    fanoutId: input.fanoutId,
    ...(input.scope === 'level' ? { levelId: input.levelId, levelIndex: input.levelIndex } : {}),
    nodeResults,
    aggregateStatus,
    verificationOutcome: input.verificationOutcome,
    ...(verification === undefined || verification.length === 0 ? {} : { verification }),
    ownershipViolations,
    projectedHandoff: projection.handoff,
    ...(projection.truncated ? { projectedHandoffTruncated: true as const } : {}),
  }

  const aggregate = parseParallelAggregateV1(rawAggregate)
  if (serializedPayloadBytes(aggregate) > MAX_AGGREGATE_PAYLOAD_BYTES) {
    throw new TypeError('parallel aggregate exceeds its serialized payload budget')
  }
  return aggregate
}
