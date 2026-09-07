import { describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import { agentEvents, type Agent } from '@deepseek-ai/dsh-agent'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import { apply, inject } from '../src/index.ts'
import type { OrchestratorConfig } from '../src/types.ts'

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
    commands: [{
      name: 'typecheck',
      executable: 'pnpm',
      fixedArgs: ['typecheck'],
      allowedArgs: 'none',
    }],
    timeoutMs: 60_000,
    maxOutputBytes: 4_096,
  },
}

const adaptiveConfig: OrchestratorConfig = {
  ...config,
  worker: { provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000 },
  scheduling: {
    allowInvalidDecisionFallback: false,
    allowedRoutes: [{ provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000 }],
    rootProfile: { coding: 50, reasoning: 50, toolUse: 50, repoContext: 50, risk: 50, difficulty: 50 },
    workerProfile: { coding: 50, reasoning: 50, toolUse: 50, repoContext: 50, risk: 50, difficulty: 50 },
    maxLatencyMs: 60_000,
    allowPaidFallback: false,
  },
}

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
    assembledPrompt() {
      return [...sections.values()]
        .sort((left, right) => left.order - right.order || left.name.localeCompare(right.name))
        .map(section => section.text)
        .join('\n\n')
    },
    sectionNames() {
      return [...sections.keys()]
    },
  }
}

function toolRegistry() {
  const tools = new Map<string, { readonly name: string }>()
  return {
    register(tool: { readonly name: string }) {
      if (tools.has(tool.name)) throw new Error(`duplicate tool: ${tool.name}`)
      tools.set(tool.name, tool)
      return () => { tools.delete(tool.name) }
    },
    get(name: string) {
      return tools.get(name)
    },
  }
}

async function mountedDirectMode(modeConfig: OrchestratorConfig = config) {
  const ctx = new Context()
  const sessionStore = await ctx.plugin(SessionStore)
  const prompts = promptRegistry()
  const tools = toolRegistry()
  ctx.provide('systemPrompt', prompts as never)
  ctx.provide('tools', tools as never)
  ctx.provide('subprocess', { spawn: () => { throw new Error('verification must not run in this test') } } as never)

  const fiber = await ctx.plugin(apply, modeConfig)
  return { ctx, sessionStore, fiber, prompts, tools }
}

function appendRootRequest(ctx: Context, sessionId: string) {
  const session = ctx.sessions.create(SessionId(sessionId), {
    meta: { cwd: '/workspace/ds-plugins' },
  })
  session.append('request/header', {
    header: { config: { provider: 'openai-codex', model: 'gpt-5.6-codex' } },
    reason: 'initial',
  })
  return session
}

function appendRootHeader(ctx: Context, sessionId: string, header: unknown) {
  const session = ctx.sessions.create(SessionId(sessionId), {
    meta: { cwd: '/workspace/ds-plugins' },
  })
  session.append('request/header', { header, reason: 'initial' } as never)
  return session
}

describe('Direct orchestrator mode', () => {
  it('keeps the optional scheduler injection non-blocking and preserves the four required injections', async () => {
    const ctxWithoutScheduler = new Context()
    const sessionStore = await ctxWithoutScheduler.plugin(SessionStore)
    const prompts = promptRegistry()
    const tools = toolRegistry()
    ctxWithoutScheduler.provide('systemPrompt', prompts as never)
    ctxWithoutScheduler.provide('tools', tools as never)
    ctxWithoutScheduler.provide('subprocess', { spawn: () => { throw new Error('verification must not run in this test') } } as never)

    const startedAt = performance.now()
    const fiber = await ctxWithoutScheduler.plugin(apply, config)
    expect(performance.now() - startedAt).toBeLessThan(100)
    expect(inject).toEqual(['systemPrompt', 'tools', 'sessions', 'subprocess'])

    await fiber.dispose()
    await sessionStore.dispose()
  })

  it('registers only targeted verification, a bounded prompt section, and one durable root run record', async () => {
    const { ctx, sessionStore, fiber, prompts, tools } = await mountedDirectMode()

    expect(inject).toEqual(['systemPrompt', 'tools', 'sessions', 'subprocess'])
    expect(tools.get('targeted_verify')).toBeDefined()
    expect(tools.get('delegate_worker')).toBeUndefined()
    expect(tools.get('context_repo_map')).toBeUndefined()
    expect(tools.get('context_symbol_query')).toBeUndefined()
    expect(tools.get('context_expand_source')).toBeUndefined()
    expect(prompts.sectionNames()).toEqual(['ds-plugins:orchestrator'])
    const assembledPrompt = prompts.assembledPrompt()
    expect(assembledPrompt).toContain('Complete the coding task in this session. Use targeted_verify only for configured checks.')
    expect(assembledPrompt).toContain('Report only verification that was actually run')
    expect(assembledPrompt).toContain('Do not claim that an execution receipt proves correctness.')
    expect(assembledPrompt).not.toContain('openai-codex')
    expect(assembledPrompt).not.toContain('gpt-5.6-codex')
    expect(assembledPrompt).not.toContain('SECRET_TRANSCRIPT_MARKER')

    const root = appendRootRequest(ctx, 'direct-root')
    root.append('request/header', {
      header: { config: { provider: 'openai-codex', model: 'gpt-5.6-codex' } },
      reason: 'resume',
    })
    await Promise.resolve()

    expect(root.events.filter(event => event.type === 'dsh-plugin/run-started')).toEqual([
      expect.objectContaining({
        data: {
          schemaVersion: 1,
          mode: 'direct',
          provider: 'openai-codex',
          model: 'gpt-5.6-codex',
        },
      }),
    ])

    await fiber.dispose()
    expect(tools.get('targeted_verify')).toBeUndefined()
    expect(prompts.sectionNames()).toEqual([])
    await sessionStore.dispose()
  })

  it('orders root schedule selection before request/header and records only that actual route', async () => {
    const mounted = await mountedDirectMode(adaptiveConfig)
    const root = mounted.ctx.sessions.create(SessionId('direct-scheduled-root'), {
      meta: { cwd: '/workspace/ds-plugins' },
    })
    const agent = { id: root.id, session: root } as Agent

    const result = await agentEvents(mounted.ctx, agent).waterfall(
      'agent/request',
      { turn: 1, step: 1, signal: new AbortController().signal },
      async () => ({ provider: 'profile-disabled', model: 'profile-disabled', temperature: 0.2 }),
    )
    expect(result).toMatchObject({ provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000, temperature: 0.2 })
    expect(root.events.map(event => event.type)).toEqual(['dsh-plugin/schedule-selected'])

    root.append('request/header', { header: { config: result }, reason: 'initial' })
    await Promise.resolve()
    expect(root.events.map(event => event.type)).toEqual([
      'dsh-plugin/schedule-selected',
      'request/header',
      'dsh-plugin/run-started',
    ])
    expect(root.events[2]).toMatchObject({ data: { provider: 'provider-disabled', model: 'strong-disabled' } })

    await mounted.fiber.dispose()
    await mounted.sessionStore.dispose()
  })

  it('remounts without duplicating its prompt, tool, or root-session listener', async () => {
    const first = await mountedDirectMode()
    await first.fiber.dispose()

    const secondFiber = await first.ctx.plugin(apply, config)
    expect(first.prompts.sectionNames()).toEqual(['ds-plugins:orchestrator'])
    expect(first.tools.get('targeted_verify')).toBeDefined()

    const root = appendRootRequest(first.ctx, 'direct-remount-root')
    await Promise.resolve()
    expect(root.events.filter(event => event.type === 'dsh-plugin/run-started')).toHaveLength(1)

    await secondFiber.dispose()
    await first.sessionStore.dispose()
  })

  it('records the actual request route and ignores malformed route snapshots instead of falling back to worker configuration', async () => {
    const mounted = await mountedDirectMode()

    const routed = appendRootHeader(mounted.ctx, 'direct-resolved-route', {
      config: { provider: 'deepseek', model: 'deepseek-reasoner' },
    })
    const missingModel = appendRootHeader(mounted.ctx, 'direct-missing-route-field', {
      config: { provider: 'deepseek' },
    })
    const malformedConfig = appendRootHeader(mounted.ctx, 'direct-malformed-route-shape', {
      config: 'not-a-route',
    })
    await Promise.resolve()

    expect(routed.events.filter(event => event.type === 'dsh-plugin/run-started')).toEqual([
      expect.objectContaining({
        data: expect.objectContaining({ mode: 'direct', provider: 'deepseek', model: 'deepseek-reasoner' }),
      }),
    ])
    expect(missingModel.events.filter(event => event.type === 'dsh-plugin/run-started')).toEqual([])
    expect(malformedConfig.events.filter(event => event.type === 'dsh-plugin/run-started')).toEqual([])

    await mounted.fiber.dispose()
    await mounted.sessionStore.dispose()
  })

  it('preserves one durable run record when HMR remounts an active root session', async () => {
    const first = await mountedDirectMode()
    const root = appendRootRequest(first.ctx, 'direct-hmr-root')
    await Promise.resolve()
    expect(root.events.filter(event => event.type === 'dsh-plugin/run-started')).toHaveLength(1)

    await first.fiber.dispose()
    const secondFiber = await first.ctx.plugin(apply, config)
    root.append('request/header', {
      header: { config: { provider: 'openai-codex', model: 'gpt-5.6-codex' } },
      reason: 'resume',
    })
    await Promise.resolve()

    expect(root.events.filter(event => event.type === 'dsh-plugin/run-started')).toHaveLength(1)

    await secondFiber.dispose()
    await first.sessionStore.dispose()
  })

  it('cancels a disposed root queued record before the same id begins a new lifecycle', async () => {
    const mounted = await mountedDirectMode()
    const disposed = mounted.ctx.sessions.prepare(SessionId('direct-reused-root'), {
      meta: { cwd: '/workspace/ds-plugins' },
    })
    const detach = mounted.ctx.sessions.enter(disposed)
    mounted.ctx.sessions.announce(disposed)
    disposed.append('request/header', {
      header: { config: { provider: 'openai-codex', model: 'gpt-5.6-codex' } },
      reason: 'initial',
    })
    detach()

    const replacement = appendRootRequest(mounted.ctx, 'direct-reused-root')
    await Promise.resolve()

    expect(disposed.events.filter(event => event.type === 'dsh-plugin/run-started')).toHaveLength(0)
    expect(replacement.events.filter(event => event.type === 'dsh-plugin/run-started')).toHaveLength(1)

    await mounted.fiber.dispose()
    await mounted.sessionStore.dispose()
  })
})
