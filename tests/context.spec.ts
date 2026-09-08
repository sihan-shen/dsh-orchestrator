import { Context } from '@deepseek-ai/cordis'
import { Session, SessionId } from '@deepseek-ai/dsh-session'
import type { ToolDefinition } from '@deepseek-ai/dsh-tools'
import { createContextBlockV1, parseContextBlockV1 } from '@han_05/dsh-context'
import { createContextTools as createTask3ContextTools } from '@han_05/dsh-code-intelligence'
import { describe, expect, it, vi } from 'vitest'
import {
  CONTEXT_PROMPT_ORDER,
  CONTEXT_PROMPT_SECTION,
  CONTEXT_TOOL_NAMES,
  mountContextIntegration,
} from '../src/context.ts'
import type { ContextCompiler, OrchestratorConfig } from '../src/types.ts'

const config: OrchestratorConfig = {
  workspaceRoot: '/workspace/ds-plugins',
  mode: 'direct',
  worker: {
    provider: 'openai-codex',
    model: 'gpt-5.6-codex',
    maxTokens: 32_000,
  },
  budgets: {
    maxWorkers: 0,
    maxPluginToolActions: 2,
    toolTimeoutMs: 60_000,
  },
  verification: {
    commands: [],
    timeoutMs: 60_000,
    maxOutputBytes: 4_096,
  },
}

const contextBlock = createContextBlockV1({
  schemaVersion: 1,
  workspaceRoot: process.cwd(),
  kind: 'repo-map',
  workspaceFingerprint: 'sha256:workspace',
  snapshotId: 'snapshot-1',
  adapterId: 'typescript-ast-fallback',
  adapterVersion: '1',
  compilerPolicyVersion: 'dsh-context-compiler-v1',
  sources: [{
    path: 'packages/dsh-orchestrator/src/context.ts',
    contentHash: `sha256:${'1'.repeat(64)}`,
  }],
  text: JSON.stringify({ schemaVersion: 1, snapshotId: 'snapshot-1', items: [] }),
  truncated: false,
})

const uuidGenerationPattern = /^orchestrator-context:[0-9a-f]{8}-[0-9a-f]{4}-4[0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}:\d+:/

interface PromptSection {
  readonly name: string
  readonly order: number
  readonly text: string
}

function promptRegistry() {
  const sections = new Map<string, PromptSection>()
  return {
    section(section: PromptSection) {
      if (sections.has(section.name)) throw new Error(`duplicate prompt section: ${section.name}`)
      sections.set(section.name, section)
      return () => { sections.delete(section.name) }
    },
    get(name: string) {
      return sections.get(name)
    },
    names() {
      return [...sections.keys()]
    },
  }
}

function toolRegistry() {
  const tools = new Map<string, ToolDefinition>()
  return {
    register(tool: ToolDefinition) {
      if (tools.has(tool.name)) throw new Error(`duplicate tool: ${tool.name}`)
      tools.set(tool.name, tool)
      return () => {
        if (tools.get(tool.name) === tool) tools.delete(tool.name)
      }
    },
    get(name: string) {
      return tools.get(name)
    },
    names() {
      return [...tools.keys()]
    },
  }
}

function compiler(result = contextBlock) {
  return {
    repoMap: vi.fn(async () => result),
    symbolQuery: vi.fn(async () => ({ ...result, kind: 'symbol' as const })),
    expandSource: vi.fn(async () => ({ ...result, kind: 'source-window' as const })),
  } satisfies ContextCompiler
}

function sessionAgent(id = 'context-root', parentSession?: string) {
  const session = Session.create(SessionId(id), undefined, {
    version: 0,
    id: SessionId(id),
    createdAt: 0,
    cwd: '/workspace/ds-plugins',
    ...(parentSession === undefined ? {} : { parentSession: SessionId(parentSession) }),
  })
  return { session }
}

function rootAgent(id = 'context-root') {
  return sessionAgent(id)
}

function deferred<T>() {
  let resolve!: (value: T | PromiseLike<T>) => void
  let reject!: (reason?: unknown) => void
  const promise = new Promise<T>((resolvePromise, rejectPromise) => {
    resolve = resolvePromise
    reject = rejectPromise
  })
  return { promise, resolve, reject }
}

async function flushMount(): Promise<void> {
  await Promise.resolve()
  await Promise.resolve()
}

describe('optional orchestrator context lifecycle', () => {
  it('leaves prompts and tools unchanged while the optional compiler service is absent', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)

    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    expect(prompts.names()).toEqual([])
    expect(tools.names()).toEqual([])

    await fiber.dispose()
  })

  it('registers three bounded read-only tools and one provider-neutral policy section only with the service', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    const service = compiler()
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const disposeService = ctx.provide('contextCompiler', service as never)

    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    expect(tools.names()).toEqual(CONTEXT_TOOL_NAMES)
    for (const name of CONTEXT_TOOL_NAMES) {
      const tool = tools.get(name)
      expect(tool?.timeoutMs).toBe(config.budgets.toolTimeoutMs)
      expect(tool?.parameters).toMatchObject({ type: 'object', additionalProperties: false })
      expect(JSON.stringify(tool)).not.toMatch(/write|shell|provider|credential|transcript/i)
    }
    expect(prompts.names()).toEqual([CONTEXT_PROMPT_SECTION])
    expect(prompts.get(CONTEXT_PROMPT_SECTION)).toMatchObject({ order: CONTEXT_PROMPT_ORDER })
    const prompt = prompts.get(CONTEXT_PROMPT_SECTION)?.text ?? ''
    expect(prompt).toContain('smallest useful Context Block')
    expect(prompt).toContain('exact path and source hash')
    expect(prompt).not.toContain(config.worker.provider)
    expect(prompt).not.toContain(config.worker.model)

    await fiber.dispose()
    expect(tools.names()).toEqual([])
    expect(prompts.names()).toEqual([])
    disposeService()
  })

  it('defers only the parsed Context Block on the owning tool result and passes one stable session key', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    const service = compiler()
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const disposeService = ctx.provide('contextCompiler', service as never)
    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    const tool = tools.get('context_repo_map')
    if (tool === undefined) throw new Error('context_repo_map was not registered')
    const deferContext = vi.fn()
    await expect(tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: rootAgent(),
        rootCallId: 'root-call-1',
        deferContext,
      } as never,
    )).resolves.toEqual(contextBlock)

    expect(service.repoMap).toHaveBeenCalledTimes(1)
    const [, , sessionKey] = service.repoMap.mock.calls[0] ?? []
    expect(sessionKey).toMatch(uuidGenerationPattern)
    expect(deferContext).toHaveBeenCalledTimes(1)
    const message = deferContext.mock.calls[0]?.[0] as {
      readonly source?: unknown
      readonly content?: readonly { readonly type?: string; readonly text?: string }[]
    }
    expect(message.source).toEqual({
      kind: 'plugin',
      plugin: 'ds-orchestrator',
      form: 'notice',
      summary: 'Context Block: repo-map',
    })
    expect(message.content).toHaveLength(1)
    const text = message.content?.[0]?.text
    if (text === undefined) throw new Error('deferred Context Block text is missing')
    expect(parseContextBlockV1(JSON.parse(text))).toEqual(contextBlock)
    expect(text).not.toContain('SECRET_TRANSCRIPT_MARKER')

    await fiber.dispose()
    disposeService()
  })

  it('selects a session-scoped compiler from the calling tool Session', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    const fallback = compiler()
    const selected = compiler()
    const service = {
      ...fallback,
      forSession: vi.fn(async () => selected),
    } satisfies ContextCompiler
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const disposeService = ctx.provide('contextCompiler', service as never)
    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()
    const tool = tools.get('context_repo_map')
    if (tool === undefined) throw new Error('context_repo_map was not registered')
    const agent = rootAgent('session-scoped-context')

    await tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent,
        rootCallId: 'session-scoped-context-call',
        deferContext: vi.fn(),
      } as never,
    )

    expect(service.forSession).toHaveBeenCalledWith(agent.session)
    expect(selected.repoMap).toHaveBeenCalledTimes(1)
    expect(fallback.repoMap).not.toHaveBeenCalled()
    await fiber.dispose()
    disposeService()
  })

  it.each([
    ['blockId', { ...contextBlock, blockId: `sha256:${'2'.repeat(64)}` }],
    ['contentHash', { ...contextBlock, contentHash: `sha256:${'3'.repeat(64)}` }],
  ])('rejects a forged derived %s before deferring context', async (_field, forgedBlock) => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    const service = compiler(forgedBlock)
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const disposeService = ctx.provide('contextCompiler', service as never)
    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    const tool = tools.get('context_repo_map')
    if (tool === undefined) throw new Error('context_repo_map was not registered')
    const deferContext = vi.fn()
    await expect(tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: rootAgent('forged-context-root'),
        rootCallId: 'forged-call',
        deferContext,
      } as never,
    )).rejects.toThrow(/does not match/)
    expect(deferContext).not.toHaveBeenCalled()

    await fiber.dispose()
    disposeService()
  })

  it('decorates Task 3 tools without duplicate registration and restores their execution on disposal', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    const service = compiler()
    const originalExecute = vi.fn(async () => contextBlock)
    const existing = {
      name: 'context_repo_map',
      description: 'Compile a bounded repository map Context Block.',
      parameters: { type: 'object', additionalProperties: false, properties: {}, required: [] },
      output: { schema: { type: 'object' }, render: () => [] },
      execute: originalExecute,
    } as ToolDefinition
    tools.register(existing)
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const disposeService = ctx.provide('contextCompiler', service as never)

    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()
    expect(tools.get('context_repo_map')).toBe(existing)
    expect(tools.names().filter(name => name === 'context_repo_map')).toHaveLength(1)

    const deferContext = vi.fn()
    await existing.execute({ snapshotId: 'snapshot-1', limit: 10 }, {
      signal: new AbortController().signal,
      agent: rootAgent(),
      rootCallId: 'decorated-call',
      deferContext,
    } as never)
    expect(originalExecute).not.toHaveBeenCalled()
    expect(service.repoMap).toHaveBeenCalledTimes(1)
    expect(service.repoMap.mock.calls[0]?.[2]).toMatch(uuidGenerationPattern)
    expect(deferContext).toHaveBeenCalledTimes(1)

    await fiber.dispose()
    await existing.execute({ snapshotId: 'snapshot-1', limit: 10 }, {
      signal: new AbortController().signal,
      agent: rootAgent(),
      rootCallId: 'restored-call',
      deferContext,
    } as never)
    expect(originalExecute).toHaveBeenCalledTimes(1)
    expect(deferContext).toHaveBeenCalledTimes(1)
    disposeService()
  })

  it('composes with Task 3 service-first registration in one provider effect without duplicate tools', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    const service = compiler()
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const orchestratorFiber = await ctx.plugin(child => mountContextIntegration(child, config))

    const providerFiber = await ctx.plugin(child => {
      child.effect(() => {
        const disposeService = child.provide('contextCompiler', service as never)
        const disposers = createTask3ContextTools(service).map(tool => tools.register(tool))
        return () => {
          for (const dispose of disposers.reverse()) dispose()
          disposeService()
        }
      })
    })
    await flushMount()

    expect(tools.names()).toEqual(CONTEXT_TOOL_NAMES)
    expect(prompts.names()).toEqual([CONTEXT_PROMPT_SECTION])
    const tool = tools.get('context_repo_map')
    if (tool === undefined) throw new Error('Task 3 context tool was not retained')
    const deferContext = vi.fn()
    await tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: rootAgent('task-3-provider-root'),
        rootCallId: 'task-3-provider-call',
        deferContext,
      } as never,
    )
    expect(deferContext).toHaveBeenCalledTimes(1)

    await providerFiber.dispose()
    await flushMount()
    expect(tools.names()).toEqual([])
    expect(prompts.names()).toEqual([])
    await orchestratorFiber.dispose()
  })

  it('aborts pending compiler work and awaits its settlement during disposal', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    const started = deferred<void>()
    const release = deferred<void>()
    let compilerSignal: AbortSignal | undefined
    let cacheWrites = 0
    const service = {
      ...compiler(),
      repoMap: vi.fn(async (_request, signal: AbortSignal) => {
        compilerSignal = signal
        started.resolve()
        await release.promise
        if (signal.aborted) throw signal.reason ?? new DOMException('cancelled', 'AbortError')
        cacheWrites += 1
        return contextBlock
      }),
    } satisfies ContextCompiler
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const disposeService = ctx.provide('contextCompiler', service as never)
    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    const tool = tools.get('context_repo_map')
    if (tool === undefined) throw new Error('context_repo_map was not registered')
    const deferContext = vi.fn()
    const pending = tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: rootAgent('pending-disposal-root'),
        rootCallId: 'pending-disposal-call',
        deferContext,
      } as never,
    )
    await started.promise

    let disposalSettled = false
    const disposing = fiber.dispose().then(() => { disposalSettled = true })
    await Promise.resolve()
    await Promise.resolve()
    const disposalAwaitedCompiler = !disposalSettled
    const compilerWasAborted = compilerSignal?.aborted === true
    release.resolve()
    const [pendingOutcome] = await Promise.allSettled([pending, disposing])

    expect(compilerWasAborted).toBe(true)
    expect(disposalAwaitedCompiler).toBe(true)
    expect(pendingOutcome.status).toBe('rejected')
    expect(cacheWrites).toBe(0)
    expect(deferContext).not.toHaveBeenCalled()
    disposeService()
  })

  it('uses a unique UUID generation after a simulated HMR module reload', async () => {
    async function captureKey(mount: typeof mountContextIntegration): Promise<string | undefined> {
      const ctx = new Context()
      const prompts = promptRegistry()
      const tools = toolRegistry()
      const service = compiler()
      ctx.provide('systemPrompt', prompts as never)
      ctx.provide('tools', tools as never)
      const disposeService = ctx.provide('contextCompiler', service as never)
      const fiber = await ctx.plugin(child => mount(child, config))
      await flushMount()
      const tool = tools.get('context_repo_map')
      if (tool === undefined) throw new Error('context_repo_map was not registered')
      await tool.execute(
        { snapshotId: 'snapshot-1', limit: 10 },
        {
          signal: new AbortController().signal,
          agent: rootAgent('hmr-context-root'),
          rootCallId: 'hmr-call',
          deferContext: vi.fn(),
        } as never,
      )
      const key = service.repoMap.mock.calls[0]?.[2]
      await fiber.dispose()
      disposeService()
      return key
    }

    vi.resetModules()
    const firstModule = await import('../src/context.ts')
    const firstKey = await captureKey(firstModule.mountContextIntegration)
    vi.resetModules()
    const secondModule = await import('../src/context.ts')
    const secondKey = await captureKey(secondModule.mountContextIntegration)

    expect(firstKey).toMatch(uuidGenerationPattern)
    expect(secondKey).toMatch(uuidGenerationPattern)
    expect(secondKey).not.toBe(firstKey)
  })

  it('drops pending results from a disposed generation before a same-id replacement remount', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    let settleOld = (_value: typeof contextBlock) => undefined
    const oldResult = new Promise<typeof contextBlock>(resolve => { settleOld = resolve })
    const oldCompiler = {
      ...compiler(),
      repoMap: vi.fn(async () => oldResult),
    } satisfies ContextCompiler
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const disposeOldService = ctx.provide('contextCompiler', oldCompiler as never)
    const oldFiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    const oldTool = tools.get('context_repo_map')
    if (oldTool === undefined) throw new Error('old context tool was not registered')
    const oldDeferred = vi.fn()
    const pending = oldTool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: rootAgent('reused-context-root'),
        rootCallId: 'old-call',
        deferContext: oldDeferred,
      } as never,
    )

    const disposingOld = oldFiber.dispose()
    settleOld(contextBlock)
    await disposingOld
    disposeOldService()
    const nextCompiler = compiler()
    const disposeNextService = ctx.provide('contextCompiler', nextCompiler as never)
    const nextFiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    await expect(pending).rejects.toThrow(/disposed|generation/i)
    expect(oldDeferred).not.toHaveBeenCalled()

    const nextTool = tools.get('context_repo_map')
    if (nextTool === undefined) throw new Error('replacement context tool was not registered')
    const nextDeferred = vi.fn()
    await nextTool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: rootAgent('reused-context-root'),
        rootCallId: 'next-call',
        deferContext: nextDeferred,
      } as never,
    )
    expect(nextCompiler.repoMap).toHaveBeenCalledTimes(1)
    expect(nextDeferred).toHaveBeenCalledTimes(1)

    await nextFiber.dispose()
    disposeNextService()
  })

  it('isolates a same-id replacement Session from pending compiler work in the active generation', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    let settleOld = (_value: typeof contextBlock) => undefined
    const oldResult = new Promise<typeof contextBlock>(resolve => { settleOld = resolve })
    const service = compiler()
    service.repoMap
      .mockImplementationOnce(async () => oldResult)
      .mockResolvedValue(contextBlock)
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    const disposeService = ctx.provide('contextCompiler', service as never)
    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    const tool = tools.get('context_repo_map')
    if (tool === undefined) throw new Error('context_repo_map was not registered')
    const oldAgent = rootAgent('same-id-context-root')
    const oldDeferred = vi.fn()
    const pending = tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: oldAgent,
        rootCallId: 'same-id-old-call',
        deferContext: oldDeferred,
      } as never,
    )
    ctx.emit('session/disposed', oldAgent.session)

    const replacementDeferred = vi.fn()
    await tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: rootAgent('same-id-context-root'),
        rootCallId: 'same-id-replacement-call',
        deferContext: replacementDeferred,
      } as never,
    )
    settleOld(contextBlock)
    await expect(pending).rejects.toThrow(/disposed|generation/i)

    const firstKey = service.repoMap.mock.calls[0]?.[2]
    const replacementKey = service.repoMap.mock.calls[1]?.[2]
    expect(firstKey).not.toBe(replacementKey)
    expect(oldDeferred).not.toHaveBeenCalled()
    expect(replacementDeferred).toHaveBeenCalledTimes(1)

    await fiber.dispose()
    disposeService()
  })

  it('invalidates child work owned by a disposed root while allowing its same-id replacement', async () => {
    const ctx = new Context()
    const prompts = promptRegistry()
    const tools = toolRegistry()
    const releaseOld = deferred<void>()
    let oldSignal: AbortSignal | undefined
    const service = compiler()
    service.repoMap
      .mockImplementationOnce(async (_request, signal) => {
        oldSignal = signal
        await releaseOld.promise
        if (signal.aborted) throw signal.reason ?? new DOMException('cancelled', 'AbortError')
        return contextBlock
      })
      .mockResolvedValue(contextBlock)
    const liveRoots = new Map<string, Session>()
    ctx.provide('systemPrompt', prompts as never)
    ctx.provide('tools', tools as never)
    ctx.provide('sessions', { get: (id: Session['id']) => liveRoots.get(id) } as never)
    const disposeService = ctx.provide('contextCompiler', service as never)
    const fiber = await ctx.plugin(child => mountContextIntegration(child, config))
    await flushMount()

    const tool = tools.get('context_repo_map')
    if (tool === undefined) throw new Error('context_repo_map was not registered')
    const oldRoot = rootAgent('owned-root').session
    const oldChild = sessionAgent('owned-child', 'owned-root')
    liveRoots.set(oldRoot.id, oldRoot)
    const oldDeferred = vi.fn()
    const pendingChild = tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: oldChild,
        rootCallId: 'old-child-call',
        deferContext: oldDeferred,
      } as never,
    )
    await Promise.resolve()

    ctx.emit('session/disposed', oldRoot)
    const replacementRoot = rootAgent('owned-root')
    liveRoots.set(replacementRoot.session.id, replacementRoot.session)
    const staleChildDeferred = vi.fn()
    const staleChildOutcome = await tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: oldChild,
        rootCallId: 'stale-child-call',
        deferContext: staleChildDeferred,
      } as never,
    ).then(() => 'resolved', () => 'rejected')
    const replacementDeferred = vi.fn()
    const replacementOutcome = await tool.execute(
      { snapshotId: 'snapshot-1', limit: 10 },
      {
        signal: new AbortController().signal,
        agent: replacementRoot,
        rootCallId: 'replacement-root-call',
        deferContext: replacementDeferred,
      } as never,
    ).then(() => 'resolved', () => 'rejected')
    releaseOld.resolve()
    const pendingOutcome = await pendingChild.then(() => 'resolved', () => 'rejected')

    expect(oldSignal?.aborted).toBe(true)
    expect(staleChildOutcome).toBe('rejected')
    expect(pendingOutcome).toBe('rejected')
    expect(replacementOutcome).toBe('resolved')
    expect(service.repoMap).toHaveBeenCalledTimes(2)
    expect(oldDeferred).not.toHaveBeenCalled()
    expect(staleChildDeferred).not.toHaveBeenCalled()
    expect(replacementDeferred).toHaveBeenCalledTimes(1)

    await fiber.dispose()
    disposeService()
  })
})
