import type { Context } from '@deepseek-ai/cordis'
import { createUserMessage } from '@deepseek-ai/dsh-llm'
import type { Session } from '@deepseek-ai/dsh-session'
import type { ToolDefinition, ToolRunContext } from '@deepseek-ai/dsh-tools'
import { parseContextBlockV1 } from '@ds-plugins/dsh-context'
import { MAX_CONTEXT_BLOCK_BYTES } from './config.js'
import type { ContextBlockV1, ContextCompiler, OrchestratorConfig } from './types.js'

declare module '@deepseek-ai/cordis' {
  interface Context {
    contextCompiler: ContextCompiler
  }
}

/** Stable optional context-policy section shared by Direct and Single Worker modes. */
export const CONTEXT_PROMPT_SECTION = 'ds-plugins:context-policy'

/** Keep context policy after the orchestration contract and before later tool-specific guidance. */
export const CONTEXT_PROMPT_ORDER = 110

/** Exact bounded read-only context surface supplied by the optional integration. */
export const CONTEXT_TOOL_NAMES = [
  'context_repo_map',
  'context_symbol_query',
  'context_expand_source',
] as const

const CONTEXT_PROMPT = [
  'Start with the smallest useful Context Block: use repository maps and symbol queries before source expansion.',
  'Treat Context Block provenance as required: expand source only from a returned block ID with its exact path and source hash.',
  'Keep source windows bounded and request only the offsets needed for the task.',
].join('\n')

const textEncoder = new TextEncoder()
const sha256Pattern = /^sha256:[0-9a-f]{64}$/

interface SystemPromptRegistry {
  section(section: { readonly name: string; readonly order: number; readonly text: string }): () => void
}

interface ContextToolRegistry {
  register(tool: ToolDefinition): () => void
  get(name: string): ToolDefinition | undefined
}

interface SessionRegistry {
  get(id: Session['id']): Session | undefined
}

type MutableToolDefinition = ToolDefinition & {
  execute: ToolDefinition['execute']
  timeoutMs?: number
}

interface PendingContextCall {
  readonly controller: AbortController
  readonly session: Session | undefined
  readonly root: Session | undefined
  settled: Promise<void>
}

interface ContextLifecycle {
  readonly pending: Set<PendingContextCall>
  readonly rootFor: (session: Session) => Session | undefined
  readonly isDisposed: (session: Session) => boolean
  readonly isActive: () => boolean
}

function throwReason(signal: AbortSignal): never {
  throw signal.reason ?? new DOMException('Context compilation cancelled', 'AbortError')
}

function object(value: unknown, name: string): Record<string, unknown> {
  if (typeof value !== 'object' || value === null || Array.isArray(value)) throw new TypeError(`${name} must be an object`)
  return value as Record<string, unknown>
}

function exactKeys(value: Record<string, unknown>, allowed: readonly string[], name: string): void {
  for (const key of Object.keys(value)) if (!allowed.includes(key)) throw new TypeError(`${name}.${key} is not supported`)
}

function nonEmptyString(value: unknown, name: string): string {
  if (typeof value !== 'string' || value.trim() === '' || value.includes('\0')) throw new TypeError(`${name} must be a non-empty string without NUL bytes`)
  return value
}

function positiveLimit(value: unknown): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 1 || value > 50) {
    throw new RangeError('limit must be an integer between 1 and 50')
  }
  return value
}

function optionalCursor(value: unknown): string | undefined {
  if (value === undefined) return undefined
  if (typeof value !== 'string' || textEncoder.encode(value).byteLength > 1_024) throw new RangeError('cursor exceeds 1024 UTF-8 bytes')
  return value
}

function repoMapRequest(value: unknown): { snapshotId: string; limit: number; cursor?: string } {
  const input = object(value, 'context_repo_map arguments')
  exactKeys(input, ['snapshotId', 'limit', 'cursor'], 'context_repo_map arguments')
  const cursor = optionalCursor(input.cursor)
  return {
    snapshotId: nonEmptyString(input.snapshotId, 'snapshotId'),
    limit: positiveLimit(input.limit),
    ...(cursor === undefined ? {} : { cursor }),
  }
}

function symbolRequest(value: unknown): { snapshotId: string; query: string; limit: number; cursor?: string } {
  const input = object(value, 'context_symbol_query arguments')
  exactKeys(input, ['snapshotId', 'query', 'limit', 'cursor'], 'context_symbol_query arguments')
  if (typeof input.query !== 'string' || textEncoder.encode(input.query).byteLength > 256) throw new RangeError('query exceeds 256 UTF-8 bytes')
  const cursor = optionalCursor(input.cursor)
  return {
    snapshotId: nonEmptyString(input.snapshotId, 'snapshotId'),
    query: input.query,
    limit: positiveLimit(input.limit),
    ...(cursor === undefined ? {} : { cursor }),
  }
}

function safePath(value: unknown): string {
  const path = nonEmptyString(value, 'path')
  if (path.includes('\\') || path.startsWith('/') || /^[A-Za-z]:/.test(path)) throw new TypeError('path must be repository-relative POSIX syntax')
  if (path.split('/').some(part => part === '' || part === '.' || part === '..')) throw new TypeError('path contains an unsafe segment')
  return path
}

function nonNegativeOffset(value: unknown, name: string): number {
  if (typeof value !== 'number' || !Number.isSafeInteger(value) || value < 0) throw new RangeError(`${name} must be a non-negative integer`)
  return value
}

function expansionRequest(value: unknown): {
  blockId: string
  path: string
  sourceHash: string
  startOffset: number
  endOffset: number
} {
  const input = object(value, 'context_expand_source arguments')
  exactKeys(input, ['blockId', 'path', 'sourceHash', 'startOffset', 'endOffset'], 'context_expand_source arguments')
  const blockId = nonEmptyString(input.blockId, 'blockId')
  const sourceHash = nonEmptyString(input.sourceHash, 'sourceHash')
  if (!sha256Pattern.test(blockId)) throw new TypeError('blockId must be a sha256 hash')
  if (!sha256Pattern.test(sourceHash)) throw new TypeError('sourceHash must be a sha256 hash')
  const startOffset = nonNegativeOffset(input.startOffset, 'startOffset')
  const endOffset = nonNegativeOffset(input.endOffset, 'endOffset')
  if (endOffset < startOffset) throw new RangeError('endOffset must be greater than or equal to startOffset')
  if (endOffset - startOffset > MAX_CONTEXT_BLOCK_BYTES) throw new RangeError('source window exceeds the per-block bound')
  return { blockId, path: safePath(input.path), sourceHash, startOffset, endOffset }
}

function contextMessage(block: ContextBlockV1) {
  return createUserMessage({
    content: [{ type: 'text', text: JSON.stringify(block) }],
    source: {
      kind: 'plugin',
      plugin: 'ds-orchestrator',
      form: 'notice',
      summary: `Context Block: ${block.kind}`,
    },
  })
}

function sessionOf(exec: ToolRunContext): Session | undefined {
  return exec.agent?.session
}

function sessionKeyFactory(generation: string, rootFor: (session: Session) => Session | undefined): (exec: ToolRunContext) => string {
  const keys = new WeakMap<Session, string>()
  let nextSession = 0
  return exec => {
    const session = sessionOf(exec)
    if (session !== undefined) {
      const rootSessionId = session.header.parentSession ?? session.id
      const owner = rootFor(session) ?? session
      const existing = keys.get(owner)
      if (existing !== undefined) return existing
      const key = `orchestrator-context:${generation}:${nextSession++}:${rootSessionId}`
      keys.set(owner, key)
      return key
    }
    return `orchestrator-context:${generation}:call:${exec.rootCallId}`
  }
}

function output() {
  return {
    schema: { type: 'object' as const },
    render: (_args: unknown, value: unknown) => [{ type: 'text' as const, text: JSON.stringify(value) }],
  }
}

function createContextTools(
  compiler: ContextCompiler,
  timeoutMs: number,
  keyFor: (exec: ToolRunContext) => string,
): readonly ToolDefinition[] {
  const compilerFor = async (exec: ToolRunContext): Promise<ContextCompiler> => compiler.forSession === undefined
    ? compiler
    : compiler.forSession(exec.agent?.session)
  return [{
    name: 'context_repo_map',
    description: 'Compile a bounded repository map Context Block from the current immutable snapshot.',
    parameters: {
      type: 'object', additionalProperties: false,
      properties: { snapshotId: { type: 'string' }, limit: { type: 'integer', minimum: 1, maximum: 50 }, cursor: { type: 'string' } },
      required: ['snapshotId', 'limit'],
    },
    output: output(),
    timeoutMs,
    async execute(value, exec) {
      if (exec.signal.aborted) throwReason(exec.signal)
      return (await compilerFor(exec)).repoMap(repoMapRequest(value), exec.signal, keyFor(exec))
    },
  }, {
    name: 'context_symbol_query',
    description: 'Compile bounded symbol matches into a Context Block from the current immutable snapshot.',
    parameters: {
      type: 'object', additionalProperties: false,
      properties: { snapshotId: { type: 'string' }, query: { type: 'string' }, limit: { type: 'integer', minimum: 1, maximum: 50 }, cursor: { type: 'string' } },
      required: ['snapshotId', 'query', 'limit'],
    },
    output: output(),
    timeoutMs,
    async execute(value, exec) {
      if (exec.signal.aborted) throwReason(exec.signal)
      return (await compilerFor(exec)).symbolQuery(symbolRequest(value), exec.signal, keyFor(exec))
    },
  }, {
    name: 'context_expand_source',
    description: 'Expand one bounded provenance-checked source window from a prior Context Block.',
    parameters: {
      type: 'object', additionalProperties: false,
      properties: {
        blockId: { type: 'string' }, path: { type: 'string' }, sourceHash: { type: 'string' },
        startOffset: { type: 'integer', minimum: 0 }, endOffset: { type: 'integer', minimum: 0 },
      },
      required: ['blockId', 'path', 'sourceHash', 'startOffset', 'endOffset'],
    },
    output: output(),
    timeoutMs,
    async execute(value, exec) {
      if (exec.signal.aborted) throwReason(exec.signal)
      return (await compilerFor(exec)).expandSource(expansionRequest(value), exec.signal, keyFor(exec))
    },
  }]
}

function wrapContextTool(
  tool: MutableToolDefinition,
  mountedExecute: ToolDefinition['execute'],
  lifecycle: ContextLifecycle,
  timeoutMs: number,
): () => void {
  const originalExecute = tool.execute
  const originalTimeoutMs = tool.timeoutMs
  const wrappedExecute: ToolDefinition['execute'] = async function (args, exec) {
    if (exec.signal.aborted) throwReason(exec.signal)
    const session = sessionOf(exec)
    const root = session === undefined ? undefined : lifecycle.rootFor(session)
    if (!lifecycle.isActive()) throw new Error('context integration generation is disposed')
    if (session !== undefined && lifecycle.isDisposed(session)) throw new Error('context Session is disposed')
    if (root !== undefined && lifecycle.isDisposed(root)) throw new Error('context root Session is disposed')

    const controller = new AbortController()
    const forwardCallerAbort = () => controller.abort(exec.signal.reason)
    exec.signal.addEventListener('abort', forwardCallerAbort, { once: true })
    const call: PendingContextCall = {
      controller,
      session,
      root,
      settled: Promise.resolve(),
    }
    lifecycle.pending.add(call)
    const operation = (async () => {
      try {
        const callExec = { ...exec, signal: controller.signal } as ToolRunContext
        const result = await mountedExecute.call(tool, args, callExec)
        if (exec.signal.aborted) throwReason(exec.signal)
        if (controller.signal.aborted) throwReason(controller.signal)
        if (!lifecycle.isActive()) throw new Error('context integration generation was disposed before completion')
        if (session !== undefined && lifecycle.isDisposed(session)) throw new Error('context Session was disposed before completion')
        if (root !== undefined && lifecycle.isDisposed(root)) throw new Error('context root Session was disposed before completion')
        const block = parseContextBlockV1(result)
        exec.deferContext(contextMessage(block))
        return block
      } finally {
        exec.signal.removeEventListener('abort', forwardCallerAbort)
        lifecycle.pending.delete(call)
      }
    })()
    call.settled = operation.then(() => undefined, () => undefined)
    return operation
  }
  tool.execute = wrappedExecute
  tool.timeoutMs = timeoutMs
  return () => {
    if (tool.execute === wrappedExecute) tool.execute = originalExecute
    if (tool.timeoutMs === timeoutMs) {
      if (originalTimeoutMs === undefined) delete tool.timeoutMs
      else tool.timeoutMs = originalTimeoutMs
    }
  }
}

/**
 * Mount context prompt/tool policy only while the optional compiler service exists.
 * The parent context owns the injection, and each service generation owns its prompt, tools, and session listener.
 */
export function mountContextIntegration(ctx: Context, config: OrchestratorConfig): void {
  const injectOptional = (ctx as Context & { inject?: Context['inject'] }).inject
  if (typeof injectOptional !== 'function') return
  const injection = injectOptional.call(ctx, ['contextCompiler'], contextCtx => {
    const compiler = contextCtx.get('contextCompiler') as ContextCompiler | undefined
    if (compiler === undefined) return
    const systemPrompt = contextCtx.get('systemPrompt') as SystemPromptRegistry | undefined
    const tools = contextCtx.get('tools') as ContextToolRegistry | undefined
    const sessions = contextCtx.get('sessions') as SessionRegistry | undefined
    if (systemPrompt === undefined || tools === undefined) throw new Error('context integration requires systemPrompt and tools services')
    const generation = crypto.randomUUID()

    contextCtx.effect(() => {
      let active = true
      const disposedSessions = new WeakSet<Session>()
      const roots = new WeakMap<Session, Session>()
      const pending = new Set<PendingContextCall>()
      const rootFor = (session: Session): Session | undefined => {
        if (session.header.parentSession === undefined) return session
        const existing = roots.get(session)
        if (existing !== undefined) return existing
        const root = sessions?.get(session.header.parentSession)
        if (root !== undefined) roots.set(session, root)
        return root
      }
      const lifecycle: ContextLifecycle = {
        pending,
        rootFor,
        isDisposed: session => disposedSessions.has(session),
        isActive: () => active,
      }
      const disposePrompt = systemPrompt.section({
        name: CONTEXT_PROMPT_SECTION,
        order: CONTEXT_PROMPT_ORDER,
        text: CONTEXT_PROMPT,
      })
      const disposeCreated = contextCtx.on('session/created', session => { rootFor(session) })
      const disposeSessions = contextCtx.on('session/disposed', session => {
        disposedSessions.add(session)
        for (const call of pending) {
          if (call.session === session || call.root === session) {
            call.controller.abort(new Error('context Session owner was disposed'))
          }
        }
      })
      const localTools = createContextTools(
        compiler,
        config.budgets.toolTimeoutMs,
        sessionKeyFactory(generation, rootFor),
      )
      const disposers: Array<() => void> = []
      for (const localTool of localTools) {
        const existing = tools.get(localTool.name) as MutableToolDefinition | undefined
        const tool = existing ?? localTool as MutableToolDefinition
        if (existing === undefined) disposers.push(tools.register(tool))
        disposers.push(wrapContextTool(
          tool,
          localTool.execute,
          lifecycle,
          config.budgets.toolTimeoutMs,
        ))
      }
      return async () => {
        active = false
        for (const dispose of disposers.reverse()) dispose()
        disposeCreated()
        disposeSessions()
        disposePrompt()
        const settling = [...pending].map(call => {
          call.controller.abort(new Error('context integration generation was disposed'))
          return call.settled
        })
        await Promise.all(settling)
      }
    }, 'ds-orchestrator: optional context integration')
  })

  ctx.effect(() => () => injection.dispose(), 'ds-orchestrator: optional context injection')
}
