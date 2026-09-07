import { describe, expect, it } from 'vitest'
import { MAX_HANDOFF_ITEMS, MAX_HANDOFF_STRING_BYTES } from '../src/config.ts'
import { failedHandoff, normalizeWorkerOutput, parseHandoff } from '../src/handoff.ts'

const workspaceRoot = '/workspace/project'

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

const validHandoff = {
  schemaVersion: 1,
  status: 'completed',
  summary: 'Implemented the requested configuration validation.',
  changedFiles: ['src\\config.ts'],
  decisions: ['Validation rejects unknown keys.'],
  verification: [passedVerification],
  blockers: [],
} as const

function handoffWith(patch: Record<string, unknown>) {
  return { ...validHandoff, ...patch }
}

describe('parseHandoff', () => {
  it('accepts completed JSON output and normalizes repository-relative paths', () => {
    expect(parseHandoff(validHandoff, workspaceRoot)).toEqual({
      ...validHandoff,
      changedFiles: ['src/config.ts'],
    })
  })

  it('retains legacy slash, dot-segment, and 1025-byte path normalization behavior', () => {
    const legacyLongPath = `src/${'x'.repeat(1_021)}`

    expect(new TextEncoder().encode(legacyLongPath)).toHaveLength(1_025)
    expect(parseHandoff(handoffWith({
      changedFiles: ['src/./nested//file.ts', 'src\\nested\\windows.ts', legacyLongPath],
    }), workspaceRoot).changedFiles).toEqual([
      'src/nested/file.ts',
      'src/nested/windows.ts',
      legacyLongPath,
    ])
  })

  it.each([
    ['schema version', handoffWith({ schemaVersion: 2 }), /schemaVersion/],
    ['status', handoffWith({ status: 'unknown' }), /status/],
    ['oversized summary', handoffWith({ summary: 'x'.repeat(MAX_HANDOFF_STRING_BYTES + 1) }), /summary/],
    ['too many decisions', handoffWith({ decisions: Array.from({ length: MAX_HANDOFF_ITEMS + 1 }, () => 'decision') }), /decisions/],
    ['absolute changed file', handoffWith({ changedFiles: ['/etc/passwd'] }), /changedFiles/],
    ['parent traversal', handoffWith({ changedFiles: ['../secret.txt'] }), /changedFiles/],
    ['Windows parent traversal', handoffWith({ changedFiles: ['..\\secret.txt'] }), /changedFiles/],
    ['NUL byte', handoffWith({ changedFiles: ['src/\0secret.ts'] }), /changedFiles/],
    ['unknown root key', { ...validHandoff, extra: true }, /extra/],
    ['non-JSON value', handoffWith({ decisions: [new Date() as unknown as string] }), /decisions/],
  ])('rejects invalid %s', (_name, value, message) => {
    expect(() => parseHandoff(value, workspaceRoot)).toThrow(message)
  })

  it('requires a completed handoff to use the failed-verification acknowledgement marker', () => {
    expect(() => parseHandoff(handoffWith({
      summary: 'Completed the requested change.',
      verification: [{ ...passedVerification, status: 'failed', exitCode: 1 }],
    }), workspaceRoot)).toThrow(/\[verification: failed\]/)
  })
})

describe('worker output normalization', () => {
  it('does not copy invalid worker output into the parent handoff', () => {
    const raw = 'SECRET_TRANSCRIPT_MARKER'
    const handoff = normalizeWorkerOutput(raw, workspaceRoot)

    expect(handoff.status).toBe('failed')
    expect(JSON.stringify(handoff)).not.toContain(raw)
  })

  it.each(['unknown', 'sensitive', 'transcript', 'provider', 'credential', 'authorization'])
    ('rejects and scrubs the %s worker-output field', field => {
      const marker = `SECRET_${field.toUpperCase()}_MARKER`
      const handoff = normalizeWorkerOutput({ ...validHandoff, [field]: marker }, workspaceRoot)
      const serialized = JSON.stringify(handoff)

      expect(handoff).toEqual({
        schemaVersion: 1,
        status: 'failed',
        summary: 'Worker output failed HandoffV1 validation.',
        changedFiles: [],
        decisions: [],
        verification: [],
        blockers: [],
      })
      expect(serialized).not.toContain(marker)
      expect(serialized).not.toContain(field)
    })

  it('returns a bounded failed handoff for a validation message', () => {
    expect(failedHandoff('Worker output was invalid.')).toEqual({
      schemaVersion: 1,
      status: 'failed',
      summary: 'Worker output was invalid.',
      changedFiles: [],
      decisions: [],
      verification: [],
      blockers: [],
    })
  })
})
