import { readFileSync } from 'node:fs'
import { createHash } from 'node:crypto'
import { fileURLToPath } from 'node:url'
import { resolve } from 'node:path'
import { describe, expect, it } from 'vitest'
import * as yaml from 'js-yaml'
import { entryListSchema } from '@deepseek-ai/cordis-plugin-include'
import { parseConfig } from '../src/config.ts'

const communityCandidates = [
  'dsh-lsp-actions',
  'dsh-telemetry-redactor',
  'dsh-verification-receipt',
  'dsh-engineering-workflow',
  'dsh-openai-oauth',
  'dsh-project-memory',
  'dsh-task-relay',
  'dsh-subagent-model-router',
  'DSH-Subagent-Model-Router',
  'CypherNaught-0x/DSH-Subagent-Model-Router',
  'dsh-tier-router',
  'dsh-codex-harness',
  'dsh-codex-shim',
  'dsh-minimal-first-turn',
  'dsh-proactive',
  'dsh-trace',
] as const

describe('DSH v0.1 profile', () => {
  it('matches the committed byte and digest preservation fixture', async () => {
    const root = resolve(fileURLToPath(new URL('.', import.meta.url)), '../../..')
    const fixture = JSON.parse(readFileSync(resolve(root, 'tests/replay/fixtures/v0.1-profile-bytes.json'), 'utf8')) as Record<string, { bytes: number; sha256: string }>
    for (const [relativePath, expected] of Object.entries(fixture)) {
      const bytes = readFileSync(resolve(root, relativePath))
      expect({ bytes: bytes.byteLength, sha256: createHash('sha256').update(bytes).digest('hex') }).toEqual(expected)
    }
  })
  it('composes only the base and orchestrator bundles through a direct-mode user patch', () => {
    const root = resolve(fileURLToPath(new URL('.', import.meta.url)), '../../..')
    const profile = resolve(root, 'profiles/v0.1')
    const manifest = JSON.parse(readFileSync(resolve(profile, 'package.json'), 'utf8')) as {
      dsh?: { profile?: { bundles?: string[] } }
    }
    const bundle = resolve(root, 'packages/dsh-orchestrator')
    const bundleManifest = JSON.parse(readFileSync(resolve(bundle, 'package.json'), 'utf8')) as {
      dsh?: { bundle?: { patch?: string } }
    }
    const bundlePatch = yaml.load(
      readFileSync(resolve(bundle, bundleManifest.dsh?.bundle?.patch ?? ''), 'utf8'),
      { schema: entryListSchema },
    )
    if (!Array.isArray(bundlePatch)) throw new TypeError('bundle patch must be a patch list')
    const profilePatch = yaml.load(
      readFileSync(resolve(profile, 'cordis.patch.yml'), 'utf8'),
      { schema: entryListSchema },
    )
    if (!Array.isArray(profilePatch)) throw new TypeError('profile patch must be a patch list')
    const rows = bundlePatch.flatMap((operation): Record<string, unknown>[] =>
      typeof operation === 'object' && operation !== null
        ? (operation as { insert?: Record<string, unknown>[] }).insert ?? []
        : [],
    )
    for (const operation of profilePatch) {
      if (typeof operation !== 'object' || operation === null || !('id' in operation)) continue
      const update = operation as { id: string; config?: unknown }
      const row = rows.find(candidate => candidate.id === update.id)
      if (row !== undefined && update.config !== undefined) row.config = update.config
    }

    expect(manifest.dsh?.profile?.bundles).toEqual([
      '@deepseek-ai/dsh-base',
      '@han_05/dsh-orchestrator',
    ])
    expect(rows.some(row => row.id === 'ds-orchestrator')).toBe(true)
    expect(rows.find(row => row.id === 'ds-orchestrator')?.name)
      .toBe('@han_05/dsh-orchestrator')

    const orchestrator = rows.find(row => row.id === 'ds-orchestrator')
    expect(orchestrator?.config).toMatchObject({
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

    const profileSources = [JSON.stringify(manifest), JSON.stringify(rows)]
    for (const candidate of communityCandidates) {
      for (const source of profileSources) expect(source).not.toContain(candidate)
    }
  })
})

describe('DSH v0.3 adaptive profile', () => {
  it('composes the exact base, orchestrator, and adaptive scheduler bundles without changing v0.1', () => {
    const root = resolve(fileURLToPath(new URL('.', import.meta.url)), '../../..')
    const v01Profile = resolve(root, 'profiles/v0.1')
    const v03Profile = resolve(root, 'profiles/v0.3-adaptive')
    const v01ManifestBefore = readFileSync(resolve(v01Profile, 'package.json'), 'utf8')
    const v01PatchBefore = readFileSync(resolve(v01Profile, 'cordis.patch.yml'), 'utf8')
    const v03Manifest = JSON.parse(readFileSync(resolve(v03Profile, 'package.json'), 'utf8')) as {
      dsh?: { profile?: { bundles?: string[] } }
    }
    const v03Patch = yaml.load(readFileSync(resolve(v03Profile, 'cordis.patch.yml'), 'utf8'), { schema: entryListSchema })

    expect(v03Manifest.dsh?.profile?.bundles).toEqual([
      '@deepseek-ai/dsh-base',
      '@han_05/dsh-orchestrator',
      '@han_05/dsh-adaptive-scheduler',
    ])
    expect(v03Patch).toEqual(expect.arrayContaining([
      expect.objectContaining({ id: 'ds-orchestrator' }),
      expect.objectContaining({ id: 'dsh-adaptive-scheduler' }),
    ]))
    const v03Orchestrator = v03Patch.find(value =>
      typeof value === 'object' && value !== null && 'id' in value && value.id === 'ds-orchestrator',
    )
    if (v03Orchestrator === undefined || typeof v03Orchestrator !== 'object' || v03Orchestrator === null || !('config' in v03Orchestrator)) {
      throw new TypeError('v0.3 profile patch must configure ds-orchestrator')
    }
    expect(parseConfig(v03Orchestrator.config)).toMatchObject({
      mode: 'single-worker',
      budgets: { maxWorkers: 1 },
    })
    expect(readFileSync(resolve(v01Profile, 'package.json'), 'utf8')).toBe(v01ManifestBefore)
    expect(readFileSync(resolve(v01Profile, 'cordis.patch.yml'), 'utf8')).toBe(v01PatchBefore)
  })
})
