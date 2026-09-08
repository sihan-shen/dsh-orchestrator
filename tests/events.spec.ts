import { describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import SessionStore, { Session, SessionId, SessionLogOffset } from '@deepseek-ai/dsh-session'
import {
  appendBudgetRejected,
  appendRunStarted,
  appendScheduleSelected,
  appendVerificationFinished,
  appendWorkerFinished,
  appendWorkerRequested,
} from '../src/events.ts'
import type { HandoffV1, VerificationEvidenceV1, WorkerSpecV1 } from '../src/types.ts'

const workerSpec: WorkerSpecV1 = {
  schemaVersion: 1,
  task: 'Add durable orchestrator events.',
  provider: 'openai-codex',
  model: 'codex-model',
  maxTokens: 16_000,
  allowedTools: ['read', 'edit'],
  expectedOutput: 'handoff-v1',
}

const verification: VerificationEvidenceV1 = {
  schemaVersion: 1,
  commandName: 'typecheck',
  args: ['typecheck'],
  exitCode: 0,
  status: 'passed',
  stdout: 'typecheck passed',
  stderr: '',
  truncated: false,
  durationMs: 1_200,
}

const handoff: HandoffV1 = {
  schemaVersion: 1,
  status: 'completed',
  summary: 'Added durable orchestrator events.',
  changedFiles: ['src/events.ts'],
  decisions: ['Events are required during replay.'],
  verification: [verification],
  blockers: [],
}

function payloadKeys(value: unknown): string[] {
  if (Array.isArray(value)) return value.flatMap(payloadKeys)
  if (value === null || typeof value !== 'object') return []
  return Object.entries(value).flatMap(([key, entry]) => [key, ...payloadKeys(entry)])
}

describe('durable orchestrator events', () => {
  it('appends a detached schedule-selected event without sensitive fields', () => {
    const session = Session.create(SessionId('dsh-schedule-selected'))
    const input = {
      schemaVersion: 1,
      target: 'worker',
      source: 'scheduler',
      provider: 'provider-disabled',
      model: 'baseline-disabled',
      maxTokens: 32_000,
      policyVersion: 'v0.3.0',
    } as const

    expect(appendScheduleSelected(session, input)).toBe(0)
    ;(input as { model: string }).model = 'mutated'
    expect(session.snapshotEvents()[0]).toMatchObject({
      type: 'dsh-plugin/schedule-selected',
      data: { model: 'baseline-disabled' },
    })
    expect(JSON.stringify(session.snapshotEvents()[0])).not.toContain('credential')
    expect(() => session.append('dsh-plugin/schedule-selected', input)).not.toThrow()
    expect(JSON.parse(JSON.stringify(session.snapshotEvents()))).toEqual(session.snapshotEvents())
  })

  it('appends required orchestration records in order and returns their sequence numbers', () => {
    const session = Session.create(SessionId('dsh-events'))

    expect(appendRunStarted(session, {
      mode: 'single-worker',
      provider: 'openai-codex',
      model: 'codex-model',
    })).toBe(0)
    expect(appendWorkerRequested(session, workerSpec)).toBe(1)
    expect(appendWorkerFinished(session, SessionId('child-session'), handoff)).toBe(2)
    expect(appendBudgetRejected(session, {
      reason: 'WORKER_LIMIT',
      limit: 1,
      observed: 2,
    })).toBe(3)
    expect(appendVerificationFinished(session, verification)).toBe(4)

    expect(session.snapshotEvents().map(event => event.type)).toEqual([
      'dsh-plugin/run-started',
      'dsh-plugin/worker-requested',
      'dsh-plugin/worker-finished',
      'dsh-plugin/budget-rejected',
      'dsh-plugin/verification-finished',
    ])
    expect(session.snapshotEvents().map(event => event.ignorable)).toEqual([
      undefined,
      undefined,
      undefined,
      undefined,
      undefined,
    ])
    expect(session.snapshotEvents()).toMatchObject([
      {
        data: {
          schemaVersion: 1,
          mode: 'single-worker',
          provider: 'openai-codex',
          model: 'codex-model',
        },
      },
      { data: workerSpec },
      {
        data: {
          schemaVersion: 1,
          childSessionId: 'child-session',
          handoff,
        },
      },
      {
        data: {
          schemaVersion: 1,
          reason: 'WORKER_LIMIT',
          limit: 1,
          observed: 2,
        },
      },
      { data: verification },
    ])
    expect(session.snapshotEvents()[1]?.data).toEqual(workerSpec)
    expect(session.snapshotEvents()[2]?.data).toEqual({
      schemaVersion: 1,
      childSessionId: 'child-session',
      handoff,
    })
    expect(session.snapshotEvents()[1]?.data).not.toHaveProperty('fanoutId')
    expect(session.snapshotEvents()[1]?.data).not.toHaveProperty('workerRef')
    expect(session.snapshotEvents()[2]?.data).not.toHaveProperty('fanoutId')
    expect(session.snapshotEvents()[2]?.data).not.toHaveProperty('workerRef')
  })

  it('projects a seeded child from inheritedEventCount rather than its full seed length', () => {
    const parent = Session.create(SessionId('seed-parent'))
    appendRunStarted(parent, { mode: 'direct', provider: 'parent-provider', model: 'parent-model' })
    appendWorkerRequested(parent, workerSpec)
    const child = Session.create(SessionId('seed-child'), parent.snapshotEvents(), {
      version: 0,
      id: SessionId('seed-child'),
      createdAt: 0,
      isSeeded: true,
      parentSession: parent.id,
    }, SessionLogOffset(1))
    appendWorkerRequested(child, workerSpec)

    expect(child.header.isSeeded).toBe(true)
    expect(child.inheritedEventCount).toBe(SessionLogOffset(1))
    expect(child.snapshotEvents()).toHaveLength(4)
    expect(child.ownEvents().map(event => event.type)).toEqual([
      'dsh-plugin/worker-requested',
      'session/end-seed',
      'dsh-plugin/worker-requested',
    ])
    expect(child.ownEvents().some(event => event.type === 'dsh-plugin/run-started')).toBe(false)

    const unseeded = Session.create(SessionId('unseeded-child'))
    expect(unseeded.header.isSeeded).toBe(false)
    expect(unseeded.inheritedEventCount).toBe(SessionLogOffset(0))
  })

  it('records JSON-safe snapshots without sensitive payload fields', () => {
    const session = Session.create(SessionId('dsh-event-snapshots'))
    const mutableWorker = {
      ...workerSpec,
      allowedTools: [...workerSpec.allowedTools],
      authorization: 'must-not-be-recorded',
    }
    const mutableVerification = {
      ...verification,
      args: [...verification.args],
      token: 'must-not-be-recorded',
    }
    const mutableHandoff = {
      ...handoff,
      changedFiles: [...handoff.changedFiles],
      decisions: [...handoff.decisions],
      verification: [mutableVerification],
      blockers: [...handoff.blockers],
      rawTranscript: 'must-not-be-recorded',
    }

    appendRunStarted(session, {
      mode: 'single-worker',
      provider: 'openai-codex',
      model: 'codex-model',
      token: 'must-not-be-recorded',
    })
    appendWorkerRequested(session, mutableWorker)
    appendWorkerFinished(session, SessionId('child-session'), mutableHandoff)
    appendBudgetRejected(session, {
      reason: 'WORKER_LIMIT',
      limit: 1,
      observed: 2,
      transcript: 'must-not-be-recorded',
    })
    appendVerificationFinished(session, mutableVerification)

    mutableWorker.allowedTools.push('shell')
    mutableVerification.args.push('--all')
    mutableHandoff.changedFiles.push('src/after-append.ts')
    mutableHandoff.decisions.push('Mutated after append.')

    expect(session.snapshotEvents()[1]?.data).toMatchObject({ allowedTools: ['read', 'edit'] })
    expect(session.snapshotEvents()[2]?.data).toMatchObject({
      handoff: {
        changedFiles: ['src/events.ts'],
        decisions: ['Events are required during replay.'],
        verification: [{ args: ['typecheck'] }],
      },
    })
    expect(session.snapshotEvents()[4]?.data).toMatchObject({ args: ['typecheck'] })
    expect(JSON.parse(JSON.stringify(session.snapshotEvents()))).toEqual(session.snapshotEvents())
    expectDeepFrozen(session.snapshotEvents()[1]?.data)
    expectDeepFrozen(session.snapshotEvents()[2]?.data)

    const keys = payloadKeys(session.snapshotEvents().map(event => event.data))
    expect(keys).not.toContain('authorization')
    expect(keys).not.toContain('token')
    expect(keys).not.toContain('transcript')
    expect(keys).not.toContain('rawTranscript')
  })

  it('disposes an event projection before remounting it without duplicate delivery', async () => {
    const ctx = new Context()
    await ctx.plugin(SessionStore)
    const session = ctx.sessions.create(SessionId('dsh-event-projection'))
    const firstProjection: string[] = []
    const disposeFirst = ctx.effect(() => ctx.on('session/event', (_session, event) => {
      if (event.type.startsWith('dsh-plugin/')) firstProjection.push(event.type)
    }), 'dsh event projection')

    appendRunStarted(session, {
      mode: 'direct',
      provider: 'openai-codex',
      model: 'codex-model',
    })
    expect(firstProjection).toEqual(['dsh-plugin/run-started'])

    disposeFirst()
    appendWorkerRequested(session, workerSpec)
    expect(firstProjection).toEqual(['dsh-plugin/run-started'])

    const secondProjection: string[] = []
    const disposeSecond = ctx.effect(() => ctx.on('session/event', (_session, event) => {
      if (event.type.startsWith('dsh-plugin/')) secondProjection.push(event.type)
    }), 'dsh event projection remount')

    appendVerificationFinished(session, verification)
    expect(firstProjection).toEqual(['dsh-plugin/run-started'])
    expect(secondProjection).toEqual(['dsh-plugin/verification-finished'])

    disposeSecond()
  })
})

function expectDeepFrozen(value: unknown): void {
  if (typeof value !== 'object' || value === null) return
  expect(Object.isFrozen(value)).toBe(true)
  for (const child of Object.values(value)) expectDeepFrozen(child)
}
