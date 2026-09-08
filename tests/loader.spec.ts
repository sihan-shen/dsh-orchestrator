import { existsSync } from 'node:fs'
import { copyFile, cp, mkdtemp, mkdir, rm, symlink, writeFile } from 'node:fs/promises'
import { createRequire } from 'node:module'
import { tmpdir } from 'node:os'
import { join, resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import { SessionId } from '@deepseek-ai/dsh-session'
import type { SubagentProvider, SubagentRuntime } from '@deepseek-ai/dsh-subagent'
import { SINGLE_WORKER_STARTUP_TIMEOUT_MS } from '@han_05/dsh-orchestrator'

const repositoryRoot = resolve(fileURLToPath(new URL('.', import.meta.url)), '../../..')
const sourceProfileDir = join(repositoryRoot, 'profiles/v0.1')
const sourceProfileManifest = join(sourceProfileDir, 'package.json')
const sourceProfilePatch = join(sourceProfileDir, 'cordis.patch.yml')
const sourceProfileModules = join(sourceProfileDir, 'node_modules')

interface ProfileLayer {
  readonly patches: readonly Record<string, unknown>[]
}

interface LoadedProfile {
  readonly dir: string
  readonly layers: readonly ProfileLayer[]
  readonly patches: readonly Record<string, unknown>[]
}

interface ProfileEntry {
  readonly id?: string
  readonly name?: string
  readonly config?: unknown
}

interface BootedContext {
  readonly fiber: { dispose(): Promise<void> }
  readonly loader: {
    entries(): Iterable<{ readonly id: string; readonly options: { readonly name: string } }>
    remove(id: string): Promise<void>
  }
  inject(
    dependencies: readonly string[],
    callback: (context: {
      readonly tools: { get(name: string): unknown }
      readonly sessions: { create(id: ReturnType<typeof SessionId>, options: unknown): unknown }
      readonly subagents?: SubagentRuntime
    }) => void,
  ): Promise<unknown>
  get(name: 'tools'): { get(name: string): unknown } | undefined
}

interface AppBoot {
  healProfilesModuleFallback(installAnchor: string, home: string): void
  loadProfile(options: { installAnchor: string; profile?: string; home?: string }): Promise<LoadedProfile>
  composeEntries(layers: readonly (readonly Record<string, unknown>[])[]): ProfileEntry[]
  boot(
    binName: string,
    configPath: string,
    patches: readonly Record<string, unknown>[],
  ): Promise<BootedContext>
}

interface ProfileLoadOptions {
  readonly mode: 'direct' | 'single-worker'
  readonly enableSubagents?: boolean
  readonly parallelOverlay?: boolean
  readonly profile?: 'v0.1' | 'v0.3-adaptive'
  readonly telemetryStorageRoot?: string
  readonly telemetryDirect?: boolean
}

interface LoadedProfileRuntime {
  readonly root: string
  readonly resolvedOrchestratorEntry: string
  readonly context: BootedContext
  dispose(): Promise<void>
}

function actualAppBoot(): Promise<AppBoot> {
  return import('@deepseek-ai/dsh-app-boot') as Promise<AppBoot>
}

function profileRequire(profileDir: string) {
  return createRequire(join(profileDir, 'package.json'))
}

function disabledRows(entries: readonly ProfileEntry[], enabled: readonly string[] = []): Record<string, unknown>[] {
  const required = new Set(['session', 'subprocess', 'tools', 'system-prompt', 'ds-orchestrator'])
  for (const id of enabled) required.add(id)
  return entries.flatMap(entry => entry.id === undefined || required.has(entry.id)
    ? []
    : [{ id: entry.id, disabled: true }])
}

function singleWorkerOverlay(entries: readonly ProfileEntry[]): Record<string, unknown> {
  const orchestrator = entries.find(entry => entry.id === 'ds-orchestrator')
  if (orchestrator?.config === undefined || typeof orchestrator.config !== 'object' || Array.isArray(orchestrator.config)) {
    throw new Error('actual profile did not compose the ds-orchestrator configuration')
  }
  const config = structuredClone(orchestrator.config) as Record<string, unknown>
  const budgets = config.budgets
  if (budgets === undefined || typeof budgets !== 'object' || Array.isArray(budgets)) {
    throw new Error('actual profile ds-orchestrator configuration has no budgets')
  }
  return {
    id: 'ds-orchestrator',
    config: {
      ...config,
      mode: 'single-worker',
      budgets: { ...(budgets as Record<string, unknown>), maxWorkers: 1 },
    },
  }
}

function parallelOverlay(entries: readonly ProfileEntry[]): Record<string, unknown> {
  const overlay = singleWorkerOverlay(entries)
  const config = overlay.config as Record<string, unknown>
  const routes = (config.scheduling as { allowedRoutes?: readonly Record<string, unknown>[] } | undefined)?.allowedRoutes ?? []
  const routeToolFilters = Object.fromEntries(routes.flatMap(route => {
    if (route === undefined) return []
    return [[JSON.stringify([route.provider, route.model, route.reasoningEffort ?? null, route.promptProfile ?? null, route.modelFamily ?? null]), ['read_file', 'write_file']]]
  }))
  return {
    ...overlay,
    config: {
      ...config,
      budgets: { ...((config.budgets ?? {}) as Record<string, unknown>), maxWorkers: 4 },
      parallel: {
        maxParallelWorkers: 4,
        verification: { schemaVersion: 1, scope: 'dag', commands: [{ name: 'test:profile', args: [] }] },
        workerToolAllowlist: ['read_file', 'write_file'],
        routeToolFilters,
      },
    },
  }
}

function adaptiveCatalogOverlay(entries: readonly ProfileEntry[]): Record<string, unknown> {
  const scheduler = entries.find(entry => entry.id === 'dsh-adaptive-scheduler')
  if (scheduler?.config === undefined || typeof scheduler.config !== 'object' || Array.isArray(scheduler.config)) throw new Error('missing adaptive scheduler config')
  const config = structuredClone(scheduler.config) as Record<string, unknown>
  config.catalog = (config.catalog as Record<string, unknown>[]).map(entry => ({ ...entry, toolFilter: ['read_file', 'write_file'] }))
  return { id: 'dsh-adaptive-scheduler', config }
}

async function copyActualProfile(root: string, profileName: 'v0.1' | 'v0.3-adaptive'): Promise<string> {
  const sourceDir = join(repositoryRoot, 'profiles', profileName)
  const profileDir = join(root, 'profiles', profileName)
  await mkdir(profileDir, { recursive: true })
  await Promise.all([
    copyFile(join(sourceDir, 'package.json'), join(profileDir, 'package.json')),
    copyFile(join(sourceDir, 'cordis.patch.yml'), join(profileDir, 'cordis.patch.yml')),
  ])
  if (profileName === 'v0.1') {
    await symlink(sourceProfileModules, join(profileDir, 'node_modules'), process.platform === 'win32' ? 'junction' : 'dir')
  } else {
    await mkdir(join(profileDir, 'node_modules'), { recursive: true })
    await mkdir(join(profileDir, 'node_modules', '@ds-plugins'), { recursive: true })
    await symlink(
      join(sourceProfileModules, '@deepseek-ai'),
      join(profileDir, 'node_modules', '@deepseek-ai'),
      process.platform === 'win32' ? 'junction' : 'dir',
    )
    await symlink(
      join(sourceProfileModules, '@ds-plugins', 'dsh-orchestrator'),
      join(profileDir, 'node_modules', '@ds-plugins', 'dsh-orchestrator'),
      process.platform === 'win32' ? 'junction' : 'dir',
    )
    await cp(join(repositoryRoot, 'packages/dsh-adaptive-scheduler'), join(root, 'packages/dsh-adaptive-scheduler'), {
      recursive: true,
      filter: (source) => !source.split(/[\\/]/).includes('node_modules'),
    })
    await cp(join(repositoryRoot, 'packages/dsh-scheduling-contracts'), join(root, 'packages/dsh-scheduling-contracts'), {
      recursive: true,
      filter: (source) => !source.split(/[\\/]/).includes('node_modules'),
    })
    await symlink(
      join(root, 'packages/dsh-adaptive-scheduler'),
      join(profileDir, 'node_modules', '@ds-plugins', 'dsh-adaptive-scheduler'),
      process.platform === 'win32' ? 'junction' : 'dir',
    )
    await symlink(
      join(root, 'packages/dsh-scheduling-contracts'),
      join(profileDir, 'node_modules', '@ds-plugins', 'dsh-scheduling-contracts'),
      process.platform === 'win32' ? 'junction' : 'dir',
    )
    await mkdir(join(root, 'packages/dsh-adaptive-scheduler/node_modules/@ds-plugins'), { recursive: true })
    await symlink(
      join(root, 'packages/dsh-scheduling-contracts'),
      join(root, 'packages/dsh-adaptive-scheduler/node_modules/@han_05/dsh-scheduling-contracts'),
      process.platform === 'win32' ? 'junction' : 'dir',
    )
    await symlink(
      join(repositoryRoot, 'packages/dsh-telemetry'),
      join(profileDir, 'node_modules', '@ds-plugins', 'dsh-telemetry'),
      process.platform === 'win32' ? 'junction' : 'dir',
    )
  }
  await writeFile(join(profileDir, 'cordis.yml'), '[]\n')
  return profileDir
}

export async function loadActualProfile(options: ProfileLoadOptions): Promise<LoadedProfileRuntime> {
  const root = await mkdtemp(join(tmpdir(), 'dsh-orchestrator-profile-'))
  let rootAdaptiveLink: string | undefined
  try {
    const profileName = options.profile ?? 'v0.1'
    const profileDir = await copyActualProfile(root, profileName)
    const appBoot = await actualAppBoot()
    const resolver = profileRequire(profileDir)
    const dshBaseManifest = resolver.resolve('@deepseek-ai/dsh-base/package.json')
    appBoot.healProfilesModuleFallback(dshBaseManifest, root)
    const profile = await appBoot.loadProfile({
      installAnchor: dshBaseManifest,
      profile: profileName,
      home: root,
    })
    const profilePatches = [
      ...profile.layers.flatMap(layer => layer.patches),
      ...profile.patches,
    ]
    const entries = appBoot.composeEntries([profilePatches])
    const telemetryOrchestrator = options.telemetryDirect
      ? (() => {
          const entry = entries.find(item => item.id === 'ds-orchestrator')
          if (entry?.config === undefined || typeof entry.config !== 'object' || Array.isArray(entry.config)) throw new Error('missing orchestrator config for telemetry overlay')
          const config = structuredClone(entry.config) as Record<string, unknown>
          const budgets = config.budgets
          return {
            id: 'ds-orchestrator',
            config: {
              ...config,
              mode: 'direct',
              budgets: { ...((budgets ?? {}) as Record<string, unknown>), maxWorkers: 0 },
            },
          }
        })()
      : undefined
    const patches = [
      ...profilePatches,
      ...(options.telemetryStorageRoot === undefined ? [] : [{ insert: [{ id: 'dsh-telemetry', name: '@han_05/dsh-telemetry', config: { enabled: true, storageRoot: options.telemetryStorageRoot } }] }]),
      ...disabledRows(entries, [
        ...(options.enableSubagents ? ['subagent'] : []),
        ...(profileName === 'v0.3-adaptive' ? ['dsh-adaptive-scheduler'] : []),
      ]),
      ...(options.parallelOverlay
      ? [parallelOverlay(entries), adaptiveCatalogOverlay(entries)]
        : options.mode === 'single-worker'
          ? [singleWorkerOverlay(entries)]
          : []),
      ...(telemetryOrchestrator === undefined ? [] : [telemetryOrchestrator]),
    ]
    if (profileName === 'v0.3-adaptive') {
      const rootPluginModules = join(repositoryRoot, 'node_modules', '@ds-plugins')
      await mkdir(rootPluginModules, { recursive: true })
      const candidate = join(rootPluginModules, 'dsh-adaptive-scheduler')
      if (!existsSync(candidate)) {
        await symlink(join(root, 'packages/dsh-adaptive-scheduler'), candidate, process.platform === 'win32' ? 'junction' : 'dir')
        rootAdaptiveLink = candidate
      }
    }
    const resolvedOrchestratorEntry = resolver.resolve('@han_05/dsh-orchestrator')
    const context = await appBoot.boot(
      'dsh-orchestrator-loader-test',
      join(profile.dir, 'cordis.yml'),
      patches,
    )
    return {
      root,
      resolvedOrchestratorEntry,
      context,
      async dispose() {
        try {
          await context.fiber.dispose()
        } finally {
          if (rootAdaptiveLink !== undefined) await rm(rootAdaptiveLink, { force: true })
          await rm(root, { recursive: true, force: true })
        }
      },
    }
  } catch (error) {
    if (rootAdaptiveLink !== undefined) await rm(rootAdaptiveLink, { force: true })
    await rm(root, { recursive: true, force: true })
    throw error
  }
}

export async function injectedServices(context: BootedContext, requireSubagents = false): Promise<{
  readonly tools: { get(name: string): unknown }
  readonly sessions: { create(id: ReturnType<typeof SessionId>, options: unknown): unknown }
  readonly subagents?: SubagentRuntime
}> {
  let services: {
    readonly tools: { get(name: string): unknown }
    readonly sessions: { create(id: ReturnType<typeof SessionId>, options: unknown): unknown }
    readonly subagents?: SubagentRuntime
  } | undefined
  await context.inject(requireSubagents ? ['tools', 'sessions', 'subagents'] : ['tools', 'sessions'], child => {
    services = child
  })
  if (services === undefined) throw new Error('actual profile did not inject its required services')
  return services
}

export async function injectedTelemetry(context: BootedContext): Promise<{ flush(): Promise<void>; stats(): unknown; dispose(): Promise<void> }> {
  let telemetry: { flush(): Promise<void>; stats(): unknown; dispose(): Promise<void> } | undefined
  await context.inject(['telemetry'], child => {
    telemetry = (child as unknown as { telemetry?: typeof telemetry }).telemetry
  })
  if (telemetry === undefined) throw new Error('actual profile did not inject telemetry')
  return telemetry
}

export function fakeSpawnProvider(): SubagentProvider {
  return {
    name: 'spawn',
    inheritsParentContext: false,
    capabilities: { agentOptions: true, outputSchema: true, depthLimit: true, toolFilter: true, persona: true },
    async start(request) {
      return {
        id: SessionId(`keyless-child:${request.parent.id}`),
        localAgent: undefined,
        result: Promise.resolve({
          stopReason: 'completed',
          output: [],
          structured: {
            schemaVersion: 1,
            status: 'completed',
            summary: 'Keyless worker completed.',
            changedFiles: [],
            decisions: [],
            verification: [],
            blockers: [],
          },
        }),
        async dispose() {},
      }
    },
  }
}

async function withActualProfile<T>(options: ProfileLoadOptions, callback: (runtime: LoadedProfileRuntime) => Promise<T>): Promise<T> {
  const runtime = await loadActualProfile(options)
  try {
    return await callback(runtime)
  } finally {
    await runtime.dispose()
  }
}

describe('built DSH v0.1 profile Loader composition', () => {
  it('requires the test command to build the profile-resolved package entry', () => {
    const entry = profileRequire(sourceProfileDir).resolve('@han_05/dsh-orchestrator')
    expect(entry).toMatch(/[\\/]packages[\\/]dsh-orchestrator[\\/]lib[\\/]index\.mjs$/)
  })

  it('boots the actual Direct profile through the bare built package entry and disposes it through the root Include', async () => {
    let root = ''
    await withActualProfile({ mode: 'direct' }, async (runtime) => {
      root = runtime.root
      expect(runtime.resolvedOrchestratorEntry).toMatch(/[\\/]packages[\\/]dsh-orchestrator[\\/]lib[\\/]index\.mjs$/)
      expect(runtime.resolvedOrchestratorEntry).not.toMatch(/[\\/]src[\\/]/)
      const entry = [...runtime.context.loader.entries()].find(candidate => candidate.id === 'include:ds-orchestrator')
      expect(entry?.options.name).toBe('@han_05/dsh-orchestrator')
      const services = await injectedServices(runtime.context)
      expect(services.tools.get('targeted_verify')).toBeDefined()
      expect(services.tools.get('delegate_worker')).toBeUndefined()
      expect((runtime.context as unknown as { get(name: string): unknown }).get('parallelExecution')).toBeUndefined()

      await runtime.context.loader.remove('include')
      expect(runtime.context.get('tools')?.get('targeted_verify')).toBeUndefined()
    })
    expect(existsSync(root)).toBe(false)
  })

  it('fails Single Worker profile boot observably when the required subagents service is unavailable', async () => {
    await expect(loadActualProfile({ mode: 'single-worker' })).rejects.toThrow(/single-worker.*subagents.*timeout/i)
  }, SINGLE_WORKER_STARTUP_TIMEOUT_MS + 2_000)

  it('uses the actual Loader-provided subagents service for one keyless Single Worker child', async () => {
    await withActualProfile({ mode: 'single-worker', enableSubagents: true }, async (runtime) => {
      const services = await injectedServices(runtime.context, true)
      const service = services.subagents
      if (service === undefined) throw new Error('actual profile did not inject the official subagents service')
      const unregister = service.registerProvider(fakeSpawnProvider())
      try {
        const delegate = services.tools.get('delegate_worker') as {
          execute(
            input: unknown,
            exec: { readonly signal: AbortSignal; readonly agent: unknown; deferContext(value: unknown): void },
          ): Promise<unknown>
        } | undefined
        expect(delegate).toBeDefined()
        expect((runtime.context as unknown as { get(name: string): unknown }).get('parallelExecution')).toBeUndefined()

        const session = services.sessions.create(SessionId('loader-single-worker-root'), { meta: { cwd: repositoryRoot } }) as {
          readonly id: ReturnType<typeof SessionId>
          readonly header: { readonly parentSession?: unknown }
        }
        const contexts: unknown[] = []
        await expect(delegate?.execute(
          { task: 'Return the bounded keyless handoff.', allowedTools: ['read_file'] },
          {
            signal: new AbortController().signal,
            agent: { id: session.id, session },
            deferContext(value) { contexts.push(value) },
          },
        )).resolves.toMatchObject({ status: 'completed', summary: 'Keyless worker completed.' })
        expect(contexts).toHaveLength(1)
      } finally {
        unregister()
      }
    })
  })

  it('boots the scheduler-present v0.3 adaptive profile with the required orchestrator injections', async () => {
    await withActualProfile({ profile: 'v0.3-adaptive', mode: 'single-worker', enableSubagents: true }, async (runtime) => {
      const services = await injectedServices(runtime.context, true)
      expect(services.tools.get('targeted_verify')).toBeDefined()
      expect(services.tools.get('delegate_worker')).toBeDefined()
      expect((runtime.context as unknown as { get(name: string): unknown }).get('parallelExecution')).toBeUndefined()
      expect(services.tools.get('parallel_worker')).toBeUndefined()
      expect((runtime.context as unknown as { get(name: string): unknown }).get('adaptiveScheduler')).toBeDefined()
    })
  })

  it('registers only the internal parallel service when a test overlay supplies parallel config', async () => {
    await withActualProfile({
      profile: 'v0.3-adaptive',
      mode: 'single-worker',
      enableSubagents: true,
      parallelOverlay: true,
    }, async (runtime) => {
      const services = await injectedServices(runtime.context, true)
      expect((runtime.context as unknown as { get(name: string): unknown }).get('parallelExecution')).toMatchObject({
        run: expect.any(Function),
      })
      expect(services.tools.get('delegate_worker')).toBeDefined()
      expect(services.tools.get('parallel_worker')).toBeUndefined()
    })
  })

  it('keeps the existing worker constraints while exposing parallelExecution only through the overlay', async () => {
    await withActualProfile({ profile: 'v0.3-adaptive', mode: 'single-worker', enableSubagents: true }, async (runtime) => {
      expect((runtime.context as unknown as { get(name: string): unknown }).get('parallelExecution')).toBeUndefined()
    })
    await withActualProfile({ profile: 'v0.3-adaptive', mode: 'single-worker', enableSubagents: true, parallelOverlay: true }, async (runtime) => {
      const service = (runtime.context as unknown as { get(name: string): unknown }).get('parallelExecution')
      expect(service).toMatchObject({ run: expect.any(Function) })
      expect((await injectedServices(runtime.context, true)).tools.get('delegate_worker')).toBeDefined()
    })
  })
})
