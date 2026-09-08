import {
  MAX_DAG_NODES,
  MAX_PARALLEL_WORKERS,
  MAX_SCHEDULING_ITEMS,
  MAX_SCHEDULING_LATENCY_MS,
  parseRouteDecisionV1,
} from '@han_05/dsh-scheduling-contracts'
import type { RouteDecisionV1 } from '@han_05/dsh-scheduling-contracts'
import { validateParallelVerificationPolicy } from './verification.js'
import type { OrchestratorConfig, ParallelConfigV1, VerificationAllowedArgs, VerificationCommand } from './types.js'

/** Maximum number of plugin-owned tool actions admitted in one run. */
export const MAX_PLUGIN_TOOL_ACTIONS = 32

/** Maximum finite timeout for one plugin-owned external action. */
export const MAX_TOOL_TIMEOUT_MS = 600_000

/** Maximum bytes retained from one verification command's combined output. */
export const MAX_VERIFICATION_OUTPUT_BYTES = 1_048_576

/** Maximum output tokens admitted for one foreground worker. */
export const MAX_WORKER_TOKENS = 128_000

/** Maximum UTF-8 byte length for one handoff string field. */
export const MAX_HANDOFF_STRING_BYTES = 16_384

/** Maximum items retained in a handoff array field. */
export const MAX_HANDOFF_ITEMS = 128

/** Maximum UTF-8 payload accepted from one trusted context-compiler result. */
export const MAX_CONTEXT_BLOCK_BYTES = 65_536

type RecordValue = Record<string, unknown>

function fail(path: string, message: string): never {
  throw new TypeError(`${path} ${message}`)
}

function record(value: unknown, path: string): RecordValue {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) {
    fail(path, 'must be an object')
  }
  return value as RecordValue
}

function onlyKeys(value: RecordValue, path: string, keys: readonly string[]): void {
  for (const key of Object.keys(value)) {
    if (!keys.includes(key)) fail(`${path}.${key}`, 'is not supported')
  }
}

function nonEmptyString(value: unknown, path: string): string {
  if (typeof value !== 'string' || value.trim() === '') fail(path, 'must be a non-empty string')
  return value
}

function positiveInteger(value: unknown, path: string, maximum?: number): number {
  if (!Number.isInteger(value) || typeof value !== 'number' || value <= 0) {
    fail(path, 'must be a positive integer')
  }
  if (maximum !== undefined && value > maximum) fail(path, `must not exceed ${maximum}`)
  return value
}

function nonNegativeInteger(value: unknown, path: string, maximum?: number): number {
  if (typeof value !== 'number' || !Number.isInteger(value) || value < 0) {
    fail(path, 'must be a non-negative integer')
  }
  if (maximum !== undefined && value > maximum) fail(path, `must not exceed ${maximum}`)
  return value
}

function stringList(value: unknown, path: string): readonly string[] {
  if (!Array.isArray(value)) fail(path, 'must be an array of strings')
  if (value.length > MAX_SCHEDULING_ITEMS) fail(path, `must not contain more than ${MAX_SCHEDULING_ITEMS} items`)
  const result = value.map((item, index) => nonEmptyString(item, `${path}[${index}]`))
  if (result.includes('targeted_verify')) {
    fail(path, 'must not include targeted_verify')
  }
  if (new Set(result).size !== result.length) fail(path, 'must not contain duplicate tools')
  return result
}

function routeToolFilterKeyValue(value: string, path: string): string {
  let tuple: unknown
  try {
    tuple = JSON.parse(value)
  } catch {
    fail(path, 'must be a canonical route-tool-filter key')
  }
  if (!Array.isArray(tuple) || tuple.length !== 5) fail(path, 'must be a canonical route-tool-filter key')
  if (typeof tuple[0] !== 'string' || tuple[0].trim() === '' || typeof tuple[1] !== 'string' || tuple[1].trim() === '') {
    fail(path, 'must be a canonical route-tool-filter key')
  }
  for (const item of tuple.slice(2)) {
    if (item !== null && (typeof item !== 'string' || item.trim() === '')) {
      fail(path, 'must be a canonical route-tool-filter key')
    }
  }
  if (JSON.stringify(tuple) !== value) fail(path, 'must be a canonical route-tool-filter key')
  return value
}

function parallelConfig(value: unknown, verification: OrchestratorConfig['verification'], maxWorkers: number): ParallelConfigV1 {
  const parallel = record(value, 'parallel')
  onlyKeys(parallel, 'parallel', ['maxParallelWorkers', 'verification', 'workerToolAllowlist', 'routeToolFilters'])
  const maxParallelWorkers = nonNegativeInteger(parallel.maxParallelWorkers, 'parallel.maxParallelWorkers', MAX_PARALLEL_WORKERS)
  if (maxParallelWorkers > maxWorkers) fail('parallel.maxParallelWorkers', 'must not exceed budgets.maxWorkers')
  const workerToolAllowlist = stringList(parallel.workerToolAllowlist, 'parallel.workerToolAllowlist')
  if (typeof parallel.routeToolFilters !== 'object' || parallel.routeToolFilters === null || Array.isArray(parallel.routeToolFilters)) {
    fail('parallel.routeToolFilters', 'must be an object of route keys to tool arrays')
  }
  const routeToolFilters: Record<string, readonly string[]> = {}
  for (const [key, tools] of Object.entries(parallel.routeToolFilters as Record<string, unknown>)) {
    const canonicalKey = routeToolFilterKeyValue(key, `parallel.routeToolFilters.${key}`)
    const parsedTools = stringList(tools, `parallel.routeToolFilters.${key}`)
    if (parsedTools.some(tool => !workerToolAllowlist.includes(tool))) {
      fail(`parallel.routeToolFilters.${key}`, 'must only contain workerToolAllowlist tools')
    }
    routeToolFilters[canonicalKey] = Object.freeze(parsedTools)
  }
  const parsedVerification = validateParallelVerificationPolicy(parallel.verification, verification)
  return {
    maxParallelWorkers,
    verification: parsedVerification,
    workerToolAllowlist: Object.freeze(workerToolAllowlist),
    routeToolFilters: Object.freeze(routeToolFilters),
  }
}

/** Canonical key for a route-specific parallel worker tool filter. */
export function routeToolFilterKey(route: RouteDecisionV1): string {
  return JSON.stringify([
    route.provider,
    route.model,
    route.reasoningEffort ?? null,
    route.promptProfile ?? null,
    route.modelFamily ?? null,
  ])
}

function verificationCommand(value: unknown, index: number): VerificationCommand {
  const path = `verification.commands[${index}]`
  const command = record(value, path)
  onlyKeys(command, path, ['name', 'executable', 'fixedArgs', 'allowedArgs'])
  const name = nonEmptyString(command.name, `${path}.name`)
  const executable = nonEmptyString(command.executable, `${path}.executable`)
  if (/\s/u.test(executable)) fail(`${path}.executable`, 'must be one path/name token')
  if (!Array.isArray(command.fixedArgs)) fail(`${path}.fixedArgs`, 'must be an array of literal arguments')
  const fixedArgs = command.fixedArgs.map((argument, argumentIndex) =>
    nonEmptyString(argument, `${path}.fixedArgs[${argumentIndex}]`),
  )
  const allowedArgs = command.allowedArgs
  if (allowedArgs !== 'none' && allowedArgs !== 'orchestrator-test-paths') {
    fail(`${path}.allowedArgs`, 'must be "none" or "orchestrator-test-paths"')
  }
  return { name, executable, fixedArgs, allowedArgs: allowedArgs as VerificationAllowedArgs }
}

const SCHEDULING_KEYS = ['allowInvalidDecisionFallback', 'allowedRoutes', 'rootProfile', 'workerProfile', 'maxLatencyMs', 'allowPaidFallback'] as const
const PROFILE_KEYS = ['coding', 'reasoning', 'toolUse', 'repoContext', 'risk', 'difficulty'] as const

function booleanValue(value: unknown, path: string): boolean {
  if (typeof value !== 'boolean') fail(path, 'must be a boolean')
  return value
}

function capabilityProfile(value: unknown, path: string) {
  const profile = record(value, path)
  onlyKeys(profile, path, PROFILE_KEYS)
  const result = {} as Record<(typeof PROFILE_KEYS)[number], number>
  for (const key of PROFILE_KEYS) {
    const score = profile[key]
    if (typeof score !== 'number' || !Number.isFinite(score) || score < 0 || score > 100) {
      fail(`${path}.${key}`, 'must be a finite number between 0 and 100')
    }
    result[key] = score
  }
  return result
}

function schedulingConfig(value: unknown): NonNullable<OrchestratorConfig['scheduling']> {
  const scheduling = record(value, 'scheduling')
  onlyKeys(scheduling, 'scheduling', SCHEDULING_KEYS)
  if (!Array.isArray(scheduling.allowedRoutes)) fail('scheduling.allowedRoutes', 'must be an array')
  if (scheduling.allowedRoutes.length === 0) fail('scheduling.allowedRoutes', 'must not be empty')
  if (scheduling.allowedRoutes.length > MAX_SCHEDULING_ITEMS) fail('scheduling.allowedRoutes', `must not contain more than ${MAX_SCHEDULING_ITEMS} items`)
  const allowedRoutes = scheduling.allowedRoutes.map((route, index) => {
    try {
      return parseRouteDecisionV1(route)
    } catch (error) {
      fail(`scheduling.allowedRoutes[${index}]`, error instanceof Error ? error.message : 'is invalid')
    }
  })
  const routeKeys = new Set(allowedRoutes.map(route => `${route.provider}\u0000${route.model}`))
  if (routeKeys.size !== allowedRoutes.length) fail('scheduling.allowedRoutes', 'must not contain duplicate provider/model routes')
  return {
    allowInvalidDecisionFallback: booleanValue(scheduling.allowInvalidDecisionFallback, 'scheduling.allowInvalidDecisionFallback'),
    allowedRoutes,
    rootProfile: capabilityProfile(scheduling.rootProfile, 'scheduling.rootProfile'),
    workerProfile: capabilityProfile(scheduling.workerProfile, 'scheduling.workerProfile'),
    maxLatencyMs: positiveInteger(scheduling.maxLatencyMs, 'scheduling.maxLatencyMs', MAX_SCHEDULING_LATENCY_MS),
    allowPaidFallback: booleanValue(scheduling.allowPaidFallback, 'scheduling.allowPaidFallback'),
  }
}

/**
 * Validate deployment configuration before the plugin starts.
 * @param value - Raw Cordis configuration.
 * @returns The validated configuration without unknown fields.
 * @throws {TypeError} When a field violates the v0.1 configuration rules.
 */
export function parseConfig(value: unknown): OrchestratorConfig {
  const config = record(value, 'config')
  onlyKeys(config, 'config', ['workspaceRoot', 'mode', 'worker', 'budgets', 'verification', 'scheduling', 'parallel'])

  const workspaceRoot = nonEmptyString(config.workspaceRoot, 'workspaceRoot')
  if (workspaceRoot.includes('\0')) fail('workspaceRoot', 'must not contain NUL bytes')

  const mode = config.mode
  if (mode !== 'direct' && mode !== 'single-worker') fail('mode', 'must be "direct" or "single-worker"')

  const worker = record(config.worker, 'worker')
  onlyKeys(worker, 'worker', ['provider', 'model', 'reasoningEffort', 'maxTokens'])
  const provider = nonEmptyString(worker.provider, 'worker.provider')
  const model = nonEmptyString(worker.model, 'worker.model')
  const reasoningEffort = worker.reasoningEffort === undefined
    ? undefined
    : nonEmptyString(worker.reasoningEffort, 'worker.reasoningEffort')
  const maxTokens = positiveInteger(worker.maxTokens, 'worker.maxTokens', MAX_WORKER_TOKENS)

  const budgets = record(config.budgets, 'budgets')
  onlyKeys(budgets, 'budgets', ['maxWorkers', 'maxPluginToolActions', 'toolTimeoutMs'])
  const maxWorkers = budgets.maxWorkers
  const hasParallel = config.parallel !== undefined
  if (hasParallel) {
    if (mode !== 'single-worker') fail('parallel', 'requires mode "single-worker"')
    if (!Number.isInteger(maxWorkers) || typeof maxWorkers !== 'number' || maxWorkers < 1 || maxWorkers > MAX_DAG_NODES) {
      fail('budgets.maxWorkers', `must be an integer from 1 to ${MAX_DAG_NODES} when parallel is configured`)
    }
  } else {
    if (mode === 'direct' && maxWorkers !== 0) fail('mode "direct"', 'requires budgets.maxWorkers to be 0')
    if (mode === 'single-worker' && maxWorkers !== 1) {
      fail('mode "single-worker"', 'requires budgets.maxWorkers to be exactly one (1)')
    }
    if (maxWorkers !== 0 && maxWorkers !== 1) fail('budgets.maxWorkers', 'must be 0 or 1')
  }
  const maxPluginToolActions = positiveInteger(
    budgets.maxPluginToolActions,
    'budgets.maxPluginToolActions',
    MAX_PLUGIN_TOOL_ACTIONS,
  )
  const toolTimeoutMs = positiveInteger(budgets.toolTimeoutMs, 'budgets.toolTimeoutMs', MAX_TOOL_TIMEOUT_MS)

  const verification = record(config.verification, 'verification')
  onlyKeys(verification, 'verification', ['commands', 'timeoutMs', 'maxOutputBytes'])
  if (!Array.isArray(verification.commands)) fail('verification.commands', 'must be an array')
  const commands = verification.commands.map(verificationCommand)
  const commandNames = new Set<string>()
  for (const command of commands) {
    if (commandNames.has(command.name)) {
      fail('verification.commands', `contains duplicate command name ${JSON.stringify(command.name)}`)
    }
    commandNames.add(command.name)
  }
  const timeoutMs = positiveInteger(verification.timeoutMs, 'verification.timeoutMs', MAX_TOOL_TIMEOUT_MS)
  const maxOutputBytes = positiveInteger(
    verification.maxOutputBytes,
    'verification.maxOutputBytes',
    MAX_VERIFICATION_OUTPUT_BYTES,
  )

  const parallel = hasParallel ? parallelConfig(config.parallel, {
    commands,
    timeoutMs,
    maxOutputBytes,
  }, maxWorkers as number) : undefined

  return {
    workspaceRoot,
    mode,
    worker: { provider, model, ...(reasoningEffort === undefined ? {} : { reasoningEffort }), maxTokens },
    budgets: { maxWorkers, maxPluginToolActions, toolTimeoutMs },
    verification: { commands, timeoutMs, maxOutputBytes },
    ...(parallel === undefined ? {} : { parallel }),
    ...(config.scheduling === undefined ? {} : { scheduling: schedulingConfig(config.scheduling) }),
  }
}

/** Cordis standard-schema entry that delegates loading validation to {@link parseConfig}. */
export const Config = {
  '~standard': {
    version: 1 as const,
    vendor: '@han_05/dsh-orchestrator',
    validate(value: unknown) {
      try {
        return { value: parseConfig(value) }
      } catch (error) {
        return {
          issues: [{ message: error instanceof Error ? error.message : 'invalid configuration' }],
        }
      }
    },
  },
}
