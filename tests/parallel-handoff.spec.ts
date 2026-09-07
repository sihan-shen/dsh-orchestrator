import { createHash } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import {
  MAX_AGGREGATE_PATH_BYTES,
  MAX_DAG_CHANGED_FILES,
  parseRepoPathDeclaration,
  type RepoFilePath,
} from '@ds-plugins/dsh-scheduling-contracts'
import { MAX_HANDOFF_ITEMS, MAX_HANDOFF_STRING_BYTES } from '../src/config.ts'
import {
  checkOwnership,
  classifyOwnershipToken,
  constructParallelHandoff,
  parseParallelHandoffEnvelopeRaw,
  type RawParallelHandoffEnvelopeV1,
} from '../src/parallel-handoff.ts'
import { normalizeWorkerOutput, parseHandoff } from '../src/handoff.ts'

const workspaceRoot = '/workspace/project'
const ownedDirectory = parseRepoPathDeclaration('src/')

const passedVerification = {
  schemaVersion: 1,
  commandName: 'typecheck',
  args: [],
  exitCode: 0,
  status: 'passed',
  stdout: '',
  stderr: '',
  truncated: false,
  durationMs: 1_000,
} as const

const failedVerification = {
  ...passedVerification,
  exitCode: 1,
  status: 'failed',
} as const

const validHandoff = {
  schemaVersion: 1,
  status: 'completed',
  summary: 'Implemented declaration-layer ownership.',
  changedFiles: ['src/owned.ts'],
  decisions: ['Classify raw changed files before legacy normalization.'],
  verification: [passedVerification],
  blockers: [],
} as const

const DOT_SEGMENT_RAW_DIGEST = 'a03099a887e73b018a8b3d957ee24239c3be69876f14843c18f79647c8008f7d'
const LONE_SURROGATE_RAW_DIGEST = '8c0c59dd0d275aadcd462a5fe12eb352cbdfeaf961eae4f85a4660521df7d2f5'
const CANONICAL_TOKEN_PREIMAGE = '[{"kind":"invalid","invalidity":"dot-segment","rawDigest":"a03099a887e73b018a8b3d957ee24239c3be69876f14843c18f79647c8008f7d"},{"kind":"path","path":"outside.ts"}]'
const CANONICAL_TOKEN_DIGEST = 'b8ae1b19baf4d87e8a472d3170a6c889935786ae8e6240a49793a17834bdc583'

function handoffWith(patch: Record<string, unknown>) {
  return { ...validHandoff, ...patch }
}

function expectDeepFrozen(value: unknown): void {
  if (typeof value !== 'object' || value === null) return
  expect(Object.isFrozen(value)).toBe(true)
  for (const child of Object.values(value)) expectDeepFrozen(child)
}

function sha256Hex(value: string): string {
  return createHash('sha256').update(value, 'utf8').digest('hex')
}

describe('raw parallel Handoff envelope', () => {
  it('preserves dot segments, backslashes, NULs, and controls before ownership classification', () => {
    const changedFiles = [
      'src/./owned.ts',
      'src\\owned.ts',
      'src/../outside.ts',
      '\u0000bad.ts',
      'src/\u0080control.ts',
    ]
    const input = { ...validHandoff, changedFiles }
    const envelope = parseParallelHandoffEnvelopeRaw(input)

    expect(envelope.changedFiles).toEqual(changedFiles)
    expect(envelope.changedFiles).not.toBe(changedFiles)
    expect(checkOwnership('node-a', envelope.changedFiles, [ownedDirectory])).toMatchObject({
      valid: false,
      summary: { nodeId: 'node-a', count: 5, samplePaths: [] },
    })

    changedFiles[0] = 'src/mutated.ts'
    changedFiles.push('src/late.ts')
    expect(envelope.changedFiles).toEqual([
      'src/./owned.ts',
      'src\\owned.ts',
      'src/../outside.ts',
      '\u0000bad.ts',
      'src/\u0080control.ts',
    ])
    expectDeepFrozen(envelope)
  })

  it('accepts the Handoff raw-string bound and rejects one byte over without path normalization', () => {
    const exact = 'x'.repeat(MAX_HANDOFF_STRING_BYTES)
    const over = `${exact}x`

    expect(parseParallelHandoffEnvelopeRaw(handoffWith({ changedFiles: [exact] })).changedFiles).toEqual([exact])
    expect(() => parseParallelHandoffEnvelopeRaw(handoffWith({ changedFiles: [over] }))).toThrow(/changedFiles\[0\].*UTF-8 bytes/u)
  })

  it('bounds invalid ownership tokens through the raw changedFiles item ceiling', () => {
    const exact = Array.from({ length: MAX_HANDOFF_ITEMS }, (_, index) => `../bad-${index.toString().padStart(3, '0')}.ts`)
    const envelope = parseParallelHandoffEnvelopeRaw(handoffWith({ changedFiles: exact }))
    const result = checkOwnership('node-a', envelope.changedFiles, [ownedDirectory])

    expect(result.valid).toBe(false)
    if (result.valid) throw new Error('expected bounded ownership violations')
    expect(result.summary).toMatchObject({ count: MAX_HANDOFF_ITEMS, samplePaths: [] })
    expect(result.tokens).toHaveLength(MAX_HANDOFF_ITEMS)
    expect(() => parseParallelHandoffEnvelopeRaw(handoffWith({
      changedFiles: [...exact, '../one-too-many.ts'],
    }))).toThrow(/changedFiles.*items/u)
  })

  it.each(['transcript', 'provider', 'credential', 'authorization', 'unknown'])
    ('rejects the unknown or sensitive %s field without copying its value', field => {
      const marker = `SECRET_${field.toUpperCase()}_VALUE`
      let thrown: unknown

      try {
        parseParallelHandoffEnvelopeRaw({ ...validHandoff, [field]: marker })
      } catch (error) {
        thrown = error
      }

      expect(thrown).toBeInstanceOf(TypeError)
      expect(String(thrown)).not.toContain(marker)
    })

  it('rejects an unknown top-level value before traversing its nested payload', () => {
    let getterCalls = 0
    const sensitiveValue = Object.create(null) as Record<string, unknown>
    Object.defineProperty(sensitiveValue, 'payload', {
      enumerable: true,
      get() {
        getterCalls += 1
        throw new Error('sensitive getter must not run')
      },
    })

    expect(() => parseParallelHandoffEnvelopeRaw(handoffWith({ transcript: sensitiveValue })))
      .toThrow(/handoff\.transcript is not supported/u)
    expect(getterCalls).toBe(0)
  })

  it('rejects an oversized changedFiles array before inspecting its entries', () => {
    let getterCalls = 0
    const changedFiles = Array.from({ length: MAX_HANDOFF_ITEMS + 1 }, (_, index) => `src/file-${index}.ts`)
    Object.defineProperty(changedFiles, '0', {
      enumerable: true,
      get() {
        getterCalls += 1
        throw new Error('changedFiles getter must not run')
      },
    })

    expect(() => parseParallelHandoffEnvelopeRaw(handoffWith({ changedFiles })))
      .toThrow(new RegExp(`changedFiles must not contain more than ${MAX_HANDOFF_ITEMS} items`, 'u'))
    expect(getterCalls).toBe(0)
  })

  it('rejects unknown and sensitive fields inside verification evidence', () => {
    expect(() => parseParallelHandoffEnvelopeRaw(handoffWith({
      verification: [{ ...passedVerification, transcript: 'SECRET_TRANSCRIPT' }],
    }))).toThrow(/transcript|supported/u)
    expect(() => parseParallelHandoffEnvelopeRaw(handoffWith({
      verification: [{ ...passedVerification, credential: 'SECRET_CREDENTIAL' }],
    }))).toThrow(/credential|supported/u)
  })

  it('rejects an unknown verification value before traversing an accessor or cycle', () => {
    let getterCalls = 0
    const unknownValue = Object.create(null) as Record<string, unknown>
    Object.defineProperty(unknownValue, 'payload', {
      enumerable: true,
      get() {
        getterCalls += 1
        throw new Error('verification getter must not run')
      },
    })
    unknownValue.self = unknownValue

    expect(() => parseParallelHandoffEnvelopeRaw(handoffWith({
      verification: [{ ...passedVerification, transcript: unknownValue }],
    }))).toThrow(/verification\[0\]\.transcript is not supported/u)
    expect(getterCalls).toBe(0)
  })
})

describe('ownership invalidity classification', () => {
  it.each([
    [`\ud800${'x'.repeat(1_025)}/../\\*.ts\u0000`, 'ill-formed-unicode'],
    [`/${'x'.repeat(1_025)}\\../*.ts\u0000`, 'over-byte-limit'],
    ['/abs\\x/../b//*.ts\u0000', 'absolute'],
    ['a\\b/../c//*.ts\u0000', 'backslash'],
    ['a/../b//*.ts\u0000', 'dot-segment'],
    ['a//*.ts\u0000', 'empty-segment'],
    ['a/*.ts\u0000', 'glob'],
    ['a/\u0080control.ts/', 'control'],
    ['a/', 'other-grammar'],
  ] as const)('classifies %j as %s using exact precedence', (raw, invalidity) => {
    expect(classifyOwnershipToken(raw, [ownedDirectory])).toMatchObject({
      kind: 'invalid',
      invalidity,
      rawDigest: expect.stringMatching(/^[0-9a-f]{64}$/u),
    })
  })

  it.each([
    ['/absolute.ts', 'absolute'],
    ['C:/absolute.ts', 'absolute'],
    ['C:\\absolute.ts', 'absolute'],
    ['\\\\server\\share.ts', 'absolute'],
    ['a//b.ts', 'empty-segment'],
    ['', 'empty-segment'],
  ] as const)('applies the exact %s predicate to %j', (raw, invalidity) => {
    expect(classifyOwnershipToken(raw, [ownedDirectory])).toMatchObject({ kind: 'invalid', invalidity })
  })

  it.each(['*', '?', '[', ']', '{', '}'])('classifies glob metacharacter %s before later grammar failures', metacharacter => {
    expect(classifyOwnershipToken(`outside/${metacharacter}.ts\u0000`, [ownedDirectory])).toMatchObject({
      kind: 'invalid',
      invalidity: 'glob',
    })
  })

  it.each(['\u0000', '\u001f', '\u007f', '\u0080', '\u009f'])
    ('classifies control code point %j without throwing', control => {
      expect(classifyOwnershipToken(`outside/${control}name.ts`, [ownedDirectory])).toMatchObject({
        kind: 'invalid',
        invalidity: 'control',
      })
    })

  it('hashes JSON.stringify(rawString), including a lone surrogate, without exposing the raw value', () => {
    const token = classifyOwnershipToken('\ud800', [ownedDirectory])

    expect(JSON.stringify('\ud800')).toBe('"\\ud800"')
    expect(sha256Hex('"\\ud800"')).toBe(LONE_SURROGATE_RAW_DIGEST)
    expect(token).toEqual({
      kind: 'invalid',
      invalidity: 'ill-formed-unicode',
      rawDigest: LONE_SURROGATE_RAW_DIGEST,
    })
    expect(JSON.stringify(token)).not.toContain('\ud800')
  })

  it('accepts a 1024-byte owned path and classifies 1025 bytes before containment', () => {
    const exact = `src/${'x'.repeat(MAX_AGGREGATE_PATH_BYTES - 4)}`
    const over = `${exact}x`

    expect(new TextEncoder().encode(exact)).toHaveLength(MAX_AGGREGATE_PATH_BYTES)
    expect(new TextEncoder().encode(over)).toHaveLength(MAX_AGGREGATE_PATH_BYTES + 1)
    expect(classifyOwnershipToken(exact, [ownedDirectory])).toBeUndefined()
    expect(classifyOwnershipToken(over, [ownedDirectory])).toMatchObject({
      kind: 'invalid',
      invalidity: 'over-byte-limit',
    })
  })

  it('classifies grammar-valid outside paths while accepting exact-file and directory ownership', () => {
    expect(classifyOwnershipToken('src/exact.ts', [parseRepoPathDeclaration('src/exact.ts')])).toBeUndefined()
    expect(classifyOwnershipToken('src/nested/owned.ts', [ownedDirectory])).toBeUndefined()
    expect(classifyOwnershipToken('src/exact.tsx', [parseRepoPathDeclaration('src/exact.ts')])).toEqual({
      kind: 'path',
      path: 'src/exact.tsx',
    })
    expect(classifyOwnershipToken('Src/case.ts', [ownedDirectory])).toEqual({
      kind: 'path',
      path: 'Src/case.ts',
    })
  })
})

describe('ownership digest commitments', () => {
  it('deduplicates complete tokens, sorts canonical JSON, and hashes the exact canonical preimage', () => {
    const result = checkOwnership(
      'node-a',
      ['outside.ts', '../bad', 'outside.ts', '../bad'],
      [ownedDirectory],
    )

    expect(result.valid).toBe(false)
    if (result.valid) throw new Error('expected ownership violations')
    expect(sha256Hex('"../bad"')).toBe(DOT_SEGMENT_RAW_DIGEST)
    expect(JSON.stringify(result.tokens)).toBe(CANONICAL_TOKEN_PREIMAGE)
    expect(result.tokens).toEqual([
      { kind: 'invalid', invalidity: 'dot-segment', rawDigest: DOT_SEGMENT_RAW_DIGEST },
      { kind: 'path', path: 'outside.ts' },
    ])
    expect(result.summary).toEqual({
      nodeId: 'node-a',
      count: 2,
      digest: CANONICAL_TOKEN_DIGEST,
      samplePaths: ['outside.ts'],
    })
  })

  it('retains only the first sixteen sorted whole valid paths while all tokens affect count and digest', () => {
    const outsidePaths = [
      'outside/17.ts', 'outside/03.ts', 'outside/11.ts', 'outside/00.ts', 'outside/16.ts', 'outside/08.ts',
      'outside/01.ts', 'outside/14.ts', 'outside/06.ts', 'outside/10.ts', 'outside/04.ts', 'outside/13.ts',
      'outside/02.ts', 'outside/15.ts', 'outside/07.ts', 'outside/12.ts', 'outside/05.ts', 'outside/09.ts',
    ]
    const result = checkOwnership('node-a', [...outsidePaths, '../invalid.ts'], [ownedDirectory])

    expect(result.valid).toBe(false)
    if (result.valid) throw new Error('expected ownership violations')
    expect(result.summary.count).toBe(19)
    expect(result.summary.samplePaths).toEqual([
      'outside/00.ts', 'outside/01.ts', 'outside/02.ts', 'outside/03.ts',
      'outside/04.ts', 'outside/05.ts', 'outside/06.ts', 'outside/07.ts',
      'outside/08.ts', 'outside/09.ts', 'outside/10.ts', 'outside/11.ts',
      'outside/12.ts', 'outside/13.ts', 'outside/14.ts', 'outside/15.ts',
    ])
    expect(result.summary.samplePaths).toHaveLength(MAX_DAG_CHANGED_FILES)
    expect(result.summary.samplePaths).not.toContain('../invalid.ts')
    expect(result.tokens).toHaveLength(19)
  })

  it('returns detached deeply frozen success and failure projections', () => {
    const acceptedInput = ['src/a.ts', 'src/nested/b.ts']
    const accepted = checkOwnership('node-a', acceptedInput, [ownedDirectory])
    expect(accepted).toEqual({ valid: true, changedFiles: ['src/a.ts', 'src/nested/b.ts'] })
    expect(accepted.valid).toBe(true)
    if (!accepted.valid) throw new Error('expected accepted ownership')
    expect(accepted.changedFiles).not.toBe(acceptedInput)
    expectDeepFrozen(accepted)

    acceptedInput[0] = 'outside/mutated.ts'
    acceptedInput.push('outside/late.ts')
    expect(accepted.changedFiles).toEqual(['src/a.ts', 'src/nested/b.ts'])

    const rejectedInput = ['outside/a.ts', '../bad.ts']
    const rejected = checkOwnership('node-b', rejectedInput, [ownedDirectory])
    expect(rejected.valid).toBe(false)
    if (rejected.valid) throw new Error('expected rejected ownership')
    expect(rejected.tokens).not.toBe(rejectedInput)
    expectDeepFrozen(rejected)

    rejectedInput[0] = 'src/now-owned.ts'
    rejectedInput.push('src/late.ts')
    expect(rejected.summary).toMatchObject({ nodeId: 'node-b', count: 2, samplePaths: ['outside/a.ts'] })
  })
})

describe('parallel Handoff construction', () => {
  it('projects only ownership-accepted canonical files into an ordinary Handoff', () => {
    const envelope = parseParallelHandoffEnvelopeRaw(validHandoff)
    const ownership = checkOwnership('node-a', envelope.changedFiles, [ownedDirectory])
    expect(ownership.valid).toBe(true)
    if (!ownership.valid) throw new Error('expected accepted ownership')

    expect(constructParallelHandoff(envelope, ownership.changedFiles)).toEqual(validHandoff)
  })

  it('accepts 128 authoritative changedFiles and rejects 129', () => {
    const envelope = parseParallelHandoffEnvelopeRaw(validHandoff)
    const atLimit = Array.from({ length: 128 }, (_, index) => `src/file-${index}.ts` as RepoFilePath)

    expect(constructParallelHandoff(envelope, atLimit).changedFiles).toHaveLength(128)
    expect(() => constructParallelHandoff(envelope, [...atLimit, 'src/file-128.ts'])).toThrow(/changedFiles.*items|128/u)
  })

  it('keeps an outside-path result as a violation and rejects a noncanonical forged projection', () => {
    const envelope = parseParallelHandoffEnvelopeRaw(handoffWith({ changedFiles: ['outside.ts'] }))
    const ownership = checkOwnership('node-a', envelope.changedFiles, [ownedDirectory])

    expect(ownership).toMatchObject({
      valid: false,
      summary: { nodeId: 'node-a', count: 1, samplePaths: ['outside.ts'] },
    })
    expect(ownership).not.toHaveProperty('changedFiles')
    expect(() => constructParallelHandoff(
      envelope,
      ['src/./forged.ts' as RepoFilePath],
    )).toThrow(/changedFiles|repository path|file/u)
  })

  it('reapplies legacy Handoff bounds and completed failed-verification marker rules', () => {
    const envelope = parseParallelHandoffEnvelopeRaw(validHandoff)

    expect(() => constructParallelHandoff({
      ...envelope,
      summary: 'x'.repeat(MAX_HANDOFF_STRING_BYTES + 1),
    } as RawParallelHandoffEnvelopeV1, ['src/owned.ts' as RepoFilePath])).toThrow(/summary/u)
    expect(() => constructParallelHandoff({
      ...envelope,
      decisions: Array.from({ length: MAX_HANDOFF_ITEMS + 1 }, () => 'decision'),
    } as RawParallelHandoffEnvelopeV1, ['src/owned.ts' as RepoFilePath])).toThrow(/decisions/u)
    expect(() => constructParallelHandoff({
      ...envelope,
      summary: 'Verification did not pass.',
      verification: [failedVerification],
    }, ['src/owned.ts' as RepoFilePath])).toThrow(/\[verification: failed\]/u)
    expect(constructParallelHandoff({
      ...envelope,
      summary: 'Completed with acknowledgement. [verification: failed]',
      verification: [failedVerification],
    }, ['src/owned.ts' as RepoFilePath])).toMatchObject({
      status: 'completed',
      summary: 'Completed with acknowledgement. [verification: failed]',
      verification: [failedVerification],
    })
  })

  it('proves declaration-layer classification runs before legacy slash and dot normalization', () => {
    const raw = parseParallelHandoffEnvelopeRaw(handoffWith({
      changedFiles: ['src/./owned.ts', 'src\\owned.ts', 'src/../outside.ts', '\u0000bad'],
    }))

    expect(raw.changedFiles).toEqual(['src/./owned.ts', 'src\\owned.ts', 'src/../outside.ts', '\u0000bad'])
    expect(checkOwnership('node-a', raw.changedFiles, [ownedDirectory])).toMatchObject({
      valid: false,
      summary: { count: 4, samplePaths: [] },
    })
    expect(parseHandoff(handoffWith({ changedFiles: ['src/./owned.ts', 'src\\owned.ts'] }), workspaceRoot).changedFiles)
      .toEqual(['src/owned.ts', 'src/owned.ts'])
    expect(normalizeWorkerOutput({ transcript: 'SECRET_TRANSCRIPT' }, workspaceRoot)).toEqual({
      schemaVersion: 1,
      status: 'failed',
      summary: 'Worker output failed HandoffV1 validation.',
      changedFiles: [],
      decisions: [],
      verification: [],
      blockers: [],
    })
  })
})
