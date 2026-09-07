import { readFileSync } from 'node:fs'
import { resolve } from 'node:path'
import { fileURLToPath } from 'node:url'
import { describe, expect, it } from 'vitest'
import * as yaml from 'js-yaml'
import { parseConfig, routeToolFilterKey } from '../src/config.ts'

const validConfig = {
  workspaceRoot: '.',
  mode: 'direct',
  worker: {
    provider: 'openai-codex',
    model: 'gpt-5.6-codex',
    maxTokens: 32_000,
  },
  budgets: {
    maxWorkers: 0,
    maxPluginToolActions: 24,
    toolTimeoutMs: 60_000,
  },
  verification: {
    commands: [
      {
        name: 'typecheck',
        executable: 'pnpm',
        fixedArgs: ['typecheck'],
        allowedArgs: 'none',
      },
    ],
    timeoutMs: 120_000,
    maxOutputBytes: 65_536,
  },
} as const

const validScheduling = {
  allowInvalidDecisionFallback: false,
  allowedRoutes: [
    { provider: 'provider-disabled', model: 'baseline-disabled', maxTokens: 32_000 },
    { provider: 'provider-disabled', model: 'strong-disabled', maxTokens: 64_000, reasoningEffort: 'high' },
  ],
  rootProfile: { coding: 50, reasoning: 50, toolUse: 50, repoContext: 50, risk: 50, difficulty: 50 },
  workerProfile: { coding: 80, reasoning: 70, toolUse: 60, repoContext: 80, risk: 30, difficulty: 60 },
  maxLatencyMs: 60_000,
  allowPaidFallback: false,
} as const

const singleWorkerConfig = {
  ...validConfig,
  mode: 'single-worker',
  budgets: { ...validConfig.budgets, maxWorkers: 1 },
} as const

const parallelConfig = {
  maxParallelWorkers: 4,
  verification: {
    schemaVersion: 1,
    scope: 'dag',
    commands: [{ name: 'typecheck', args: [] }],
  },
  workerToolAllowlist: ['read_file', 'write_file'],
  routeToolFilters: {
    '["provider-disabled","baseline-disabled",null,null,null]': ['read_file'],
  },
} as const

function configWith(patch: Record<string, unknown>) {
  return {
    ...validConfig,
    budgets: {
      ...validConfig.budgets,
      ...patch,
    },
  }
}

function configWithVerificationExecutable(executable: string) {
  return {
    ...validConfig,
    verification: {
      ...validConfig.verification,
      commands: [
        {
          ...validConfig.verification.commands[0],
          executable,
        },
      ],
    },
  }
}

describe('parseConfig', () => {
  it('returns valid bounded configuration', () => {
    expect(parseConfig(validConfig)).toEqual(validConfig)
  })

  it('accepts the optional bounded scheduling configuration', () => {
    expect(parseConfig({ ...validConfig, scheduling: validScheduling })).toEqual({ ...validConfig, scheduling: validScheduling })
  })

  it('accepts parallel only with single-worker mode and cumulative workers up to 16', () => {
    const parsed = parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 16 },
      parallel: parallelConfig,
    })

    expect(parsed.parallel).toEqual(parallelConfig)
    expect(parsed.budgets.maxWorkers).toBe(16)
  })

  it.each([0, 8])('accepts maxParallelWorkers boundary %i when it fits the cumulative budget', maxParallelWorkers => {
    const parsed = parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 16 },
      parallel: { ...parallelConfig, maxParallelWorkers },
    })

    expect(parsed.parallel?.maxParallelWorkers).toBe(maxParallelWorkers)
  })

  it('rejects maxParallelWorkers above the contracts worker-width ceiling', () => {
    expect(() => parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 16 },
      parallel: { ...parallelConfig, maxParallelWorkers: 9 },
    })).toThrow(/maxParallelWorkers/u)
  })

  it('detaches and deep-freezes parsed parallel tool allowlists and route filters', () => {
    const input = {
      maxParallelWorkers: 4,
      verification: {
        schemaVersion: 1 as const,
        scope: 'dag' as const,
        commands: [{ name: 'typecheck', args: [] }],
      },
      workerToolAllowlist: ['read_file', 'write_file'],
      routeToolFilters: {
        '["provider-disabled","baseline-disabled",null,null,null]': ['read_file', 'write_file'],
      },
    }
    const parsed = parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 8 },
      parallel: input,
    })
    const parsedParallel = parsed.parallel
    if (parsedParallel === undefined) throw new Error('parallel config must parse')
    const key = Object.keys(parsedParallel.routeToolFilters)[0]!

    expect(parsedParallel.workerToolAllowlist).not.toBe(input.workerToolAllowlist)
    expect(parsedParallel.routeToolFilters).not.toBe(input.routeToolFilters)
    expect(parsedParallel.routeToolFilters[key]).not.toBe(input.routeToolFilters[key])
    expect(Object.isFrozen(parsedParallel.workerToolAllowlist)).toBe(true)
    expect(Object.isFrozen(parsedParallel.routeToolFilters)).toBe(true)
    expect(Object.isFrozen(parsedParallel.routeToolFilters[key])).toBe(true)
  })

  it('rejects parallel configs whose route filters exceed the allowlist or duplicate tools', () => {
    const key = '["provider-disabled","baseline-disabled",null,null,null]'
    expect(() => parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 8 },
      parallel: {
        ...parallelConfig,
        routeToolFilters: { [key]: ['read_file', 'edit_file'] },
      },
    })).toThrow(/workerToolAllowlist/u)
    expect(() => parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 8 },
      parallel: {
        ...parallelConfig,
        workerToolAllowlist: ['read_file', 'read_file'],
      },
    })).toThrow(/duplicate/u)
  })

  it('rejects non-canonical route-tool-filter keys with spacing and alternate escapes', () => {
    expect(() => parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 8 },
      parallel: {
        ...parallelConfig,
        routeToolFilters: {
          '[ "provider-disabled","baseline-disabled",null,null,null]': ['read_file'],
        },
      },
    })).toThrow(/canonical/u)

    const quotedProvider = 'quote"provider'
    expect(routeToolFilterKey({ provider: quotedProvider, model: 'model', maxTokens: 1 }))
      .toBe('["quote\\"provider","model",null,null,null]')
    expect(() => parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 8 },
      parallel: {
        ...parallelConfig,
        routeToolFilters: {
          '["quote\\u0022provider","model",null,null,null]': ['read_file'],
        },
      },
    })).toThrow(/canonical/u)
  })

  it('builds the canonical route-tool-filter key with null optional fields', () => {
    expect(routeToolFilterKey({
      provider: 'provider-disabled',
      model: 'baseline-disabled',
      maxTokens: 32_000,
    })).toBe('["provider-disabled","baseline-disabled",null,null,null]')
    expect(routeToolFilterKey({
      provider: 'provider-disabled',
      model: 'strong-disabled',
      maxTokens: 64_000,
      reasoningEffort: 'high',
      promptProfile: 'coding-strong-v1',
      modelFamily: 'deepseek',
    })).toBe('["provider-disabled","strong-disabled","high","coding-strong-v1","deepseek"]')
  })

  it.each([
    {
      ...singleWorkerConfig,
      mode: 'direct',
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 0 },
      parallel: parallelConfig,
    },
    {
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 17 },
      parallel: parallelConfig,
    },
    {
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 4 },
      parallel: { ...parallelConfig, maxParallelWorkers: 5 },
    },
    {
      ...singleWorkerConfig,
      parallel: { ...parallelConfig, workerToolAllowlist: ['targeted_verify'] },
    },
    {
      ...singleWorkerConfig,
      parallel: { ...parallelConfig, workerToolAllowlist: 'read_file' },
    },
    {
      ...singleWorkerConfig,
      parallel: { ...parallelConfig, routeToolFilters: { bad: 'read_file' } },
    },
    {
      ...singleWorkerConfig,
      parallel: { ...parallelConfig, extra: true },
    },
  ])('rejects invalid parallel deployment %#', value => expect(() => parseConfig(value)).toThrow())

  it('preserves old mode rules when parallel is absent', () => {
    expect(() => parseConfig({
      ...singleWorkerConfig,
      budgets: { ...singleWorkerConfig.budgets, maxWorkers: 2 },
    })).toThrow(/exactly one/u)
  })

  it('rejects unknown scheduling keys and out-of-range capability profiles', () => {
    expect(() => parseConfig({ ...validConfig, scheduling: { ...validScheduling, unexpected: true } })).toThrow(/scheduling\.unexpected/)
    expect(() => parseConfig({
      ...validConfig,
      scheduling: { ...validScheduling, workerProfile: { ...validScheduling.workerProfile, risk: 101 } },
    })).toThrow(/scheduling\.workerProfile\.risk/)
  })

  it.each(['', '/workspace\0ds-plugins'])('rejects an invalid deployment workspace root', workspaceRoot => {
    expect(() => parseConfig({ ...validConfig, workspaceRoot })).toThrow(/workspaceRoot/i)
  })

  it('accepts the v0.1 profile configuration at Cordis load time', () => {
    const root = resolve(fileURLToPath(new URL('.', import.meta.url)), '../../..')
    const patch = yaml.load(readFileSync(resolve(root, 'profiles/v0.1/cordis.patch.yml'), 'utf8'))
    if (!Array.isArray(patch)) throw new TypeError('profile patch must be a list')
    const entry = patch.find(value => typeof value === 'object' && value !== null && 'id' in value && value.id === 'ds-orchestrator')
    if (entry === undefined || typeof entry !== 'object' || entry === null || !('config' in entry)) {
      throw new TypeError('profile patch must configure ds-orchestrator')
    }

    expect(parseConfig(entry.config)).toMatchObject({
      workspaceRoot: '.',
      mode: 'direct',
      budgets: { maxWorkers: 0 },
      verification: {
        commands: [
          { name: 'typecheck', executable: 'pnpm', fixedArgs: ['typecheck'], allowedArgs: 'none' },
          {
            name: 'test:profile',
            executable: 'pnpm',
            fixedArgs: ['test:profile'],
            allowedArgs: 'orchestrator-test-paths',
          },
        ],
      },
    })
  })

  it('requires direct mode to disable workers', () => {
    expect(() => parseConfig({ ...validConfig, budgets: { ...validConfig.budgets, maxWorkers: 1 } }))
      .toThrow(/direct.*maxWorkers.*0/)
  })

  it('requires single-worker mode to admit exactly one worker', () => {
    expect(() => parseConfig({ ...validConfig, mode: 'single-worker', budgets: { ...validConfig.budgets, maxWorkers: 0 } }))
      .toThrow(/single-worker.*maxWorkers.*1/)
  })

  it('rejects a non-positive tool timeout', () => {
    expect(() => parseConfig(configWith({ toolTimeoutMs: 0 }))).toThrow(/toolTimeoutMs/)
  })

  it('rejects a non-integral plugin tool action limit', () => {
    expect(() => parseConfig(configWith({ maxPluginToolActions: 1.5 }))).toThrow(/maxPluginToolActions/)
  })

  it('rejects shell strings as verification executables', () => {
    expect(() => parseConfig(configWithVerificationExecutable('sh -c'))).toThrow(/executable/)
  })

  it('rejects duplicate verification command names', () => {
    expect(() => parseConfig({
      ...validConfig,
      verification: {
        ...validConfig.verification,
        commands: [
          validConfig.verification.commands[0],
          { name: 'typecheck', executable: 'pnpm', fixedArgs: ['test'], allowedArgs: 'none' },
        ],
      },
    })).toThrow(/verification\.commands.*duplicate.*typecheck/i)
  })

  it.each([
    ['provider', { ...validConfig, worker: { ...validConfig.worker, provider: '' } }],
    ['model', { ...validConfig, worker: { ...validConfig.worker, model: '' } }],
  ])('rejects an empty worker %s', (_field, config) => {
    expect(() => parseConfig(config)).toThrow(/worker\.(provider|model)/)
  })

  it('accepts the documented worker token ceiling and rejects an excessive value', () => {
    expect(parseConfig({
      ...validConfig,
      worker: { ...validConfig.worker, maxTokens: 128_000 },
    }).worker.maxTokens).toBe(128_000)
    expect(() => parseConfig({
      ...validConfig,
      worker: { ...validConfig.worker, maxTokens: 128_001 },
    })).toThrow(/worker\.maxTokens.*128000/i)
  })
})
