import type {
  CapabilityProfileV1,
  ParallelVerificationPolicyV1,
  RouteDecisionV1,
} from '@ds-plugins/dsh-scheduling-contracts'

/** Caller-argument policy for one deployment-controlled verification program. */
export type VerificationAllowedArgs = 'none' | 'orchestrator-test-paths'

/** A named verification program whose executable, fixed prefix, and caller arguments are deployment controlled. */
export interface VerificationCommand {
  readonly name: string
  readonly executable: string
  readonly fixedArgs: readonly string[]
  readonly allowedArgs: VerificationAllowedArgs
}

/** Deployment-controlled policy and tool authority for economical parallel workers. */
export interface ParallelConfigV1 {
  readonly maxParallelWorkers: number
  readonly verification: ParallelVerificationPolicyV1
  readonly workerToolAllowlist: readonly string[]
  readonly routeToolFilters: Readonly<Record<string, readonly string[]>>
}

/** Configuration validated before the orchestrator plugin is loaded. */
export interface OrchestratorConfig {
  /** Deployment-controlled repository root used by direct verification processes. */
  readonly workspaceRoot: string
  readonly mode: 'direct' | 'single-worker'
  readonly worker: {
    readonly provider: string
    readonly model: string
    readonly reasoningEffort?: string
    readonly maxTokens: number
  }
  readonly budgets: {
    readonly maxWorkers: number
    readonly maxPluginToolActions: number
    readonly toolTimeoutMs: number
  }
  readonly verification: {
    readonly commands: readonly VerificationCommand[]
    readonly timeoutMs: number
    readonly maxOutputBytes: number
  }
  /** Optional deployment-controlled parallel execution policy. */
  readonly parallel?: ParallelConfigV1
  /** Optional deterministic capability-to-route scheduling policy. */
  readonly scheduling?: OrchestratorSchedulingConfig
}

/** Deployment-controlled hard bounds and capability profiles for adaptive scheduling. */
export interface OrchestratorSchedulingConfig {
  readonly allowInvalidDecisionFallback: boolean
  readonly allowedRoutes: readonly RouteDecisionV1[]
  readonly rootProfile: CapabilityProfileV1
  readonly workerProfile: CapabilityProfileV1
  readonly maxLatencyMs: number
  readonly allowPaidFallback: boolean
}

/** One immutable, provenance-carrying context disclosure returned by the optional compiler service. */
export interface ContextBlockV1 {
  readonly schemaVersion: 1
  readonly blockId: string
  readonly kind: 'repo-map' | 'symbol' | 'source-window' | 'tool-result'
  readonly workspaceFingerprint: string
  readonly snapshotId: string
  readonly adapterId: string
  readonly adapterVersion: string
  readonly compilerPolicyVersion: string
  readonly sources: readonly { readonly path: string; readonly contentHash: string }[]
  readonly contentHash: string
  readonly text: string
  readonly byteLength: number
  readonly truncated: boolean
}

/** Optional trusted service exposed by the v0.2c code-intelligence lifecycle. */
export interface ContextCompiler {
  repoMap(
    request: { snapshotId: string; limit: number; cursor?: string },
    signal: AbortSignal,
    sessionKey?: string,
  ): Promise<ContextBlockV1>
  symbolQuery(
    request: { snapshotId: string; query: string; limit: number; cursor?: string },
    signal: AbortSignal,
    sessionKey?: string,
  ): Promise<ContextBlockV1>
  expandSource(
    request: { blockId: string; path: string; sourceHash: string; startOffset: number; endOffset: number },
    signal: AbortSignal,
    sessionKey?: string,
  ): Promise<ContextBlockV1>
  /** Select the compiler owned by the current Harness Session when supported. */
  forSession?(session: object | undefined): Promise<ContextCompiler>
}

/** Canonical provider route reconstructed from a root request-header snapshot. */
export interface RequestRouteV1 {
  readonly provider: string
  readonly model: string
}

/**
 * Project the root request's actual provider route without accepting a deployment fallback.
 * @param value - Untrusted request-header payload observed from the session event log.
 * @returns The resolved route only when both canonical identifiers are non-empty strings.
 */
export function parseRequestRoute(value: unknown): RequestRouteV1 | undefined {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) return undefined
  const header = value as { readonly config?: unknown }
  if (typeof header.config !== 'object' || header.config === null || Array.isArray(header.config)) return undefined
  const config = header.config as { readonly provider?: unknown; readonly model?: unknown }
  if (typeof config.provider !== 'string' || config.provider.trim() === '') return undefined
  if (typeof config.model !== 'string' || config.model.trim() === '') return undefined
  return { provider: config.provider, model: config.model }
}

/** Bounded child-session request persisted by a later orchestration task. */
export interface WorkerSpecV1 {
  readonly schemaVersion: 1
  readonly task: string
  readonly provider: string
  readonly model: string
  readonly reasoningEffort?: string
  readonly maxTokens: number
  readonly allowedTools: readonly string[]
  readonly expectedOutput: 'handoff-v1'
}

/** Result of one configured verification command. */
export interface VerificationEvidenceV1 {
  readonly schemaVersion: 1
  readonly commandName: string
  readonly args: readonly string[]
  readonly exitCode: number | null
  readonly status: 'passed' | 'failed' | 'timed-out' | 'spawn-error'
  readonly stdout: string
  readonly stderr: string
  readonly truncated: boolean
  readonly durationMs: number
}

/** Validated, bounded child result visible to the parent session. */
export interface HandoffV1 {
  readonly schemaVersion: 1
  readonly status: 'completed' | 'blocked' | 'failed'
  readonly summary: string
  readonly changedFiles: readonly string[]
  readonly decisions: readonly string[]
  readonly verification: readonly VerificationEvidenceV1[]
  readonly blockers: readonly string[]
}
