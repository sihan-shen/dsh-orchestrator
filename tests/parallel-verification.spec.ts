import { describe, expect, it } from 'vitest'
import type { ParallelVerificationPolicyV1 } from '@han_05/dsh-scheduling-contracts'
import { BudgetController, type BudgetRejection } from '../src/budgets.ts'
import {
  foldFinalLevelVerification,
  runParallelVerification,
  type ParallelVerificationResultV1,
} from '../src/parallel-verification.ts'
import type { VerificationEvidenceV1 } from '../src/types.ts'
import type { VerificationService } from '../src/verification.ts'

const commandNames = ['typecheck', 'test:profile', 'lint'] as const

function policy(commandCount: number, scope: 'level' | 'dag' = 'dag'): ParallelVerificationPolicyV1 {
  return {
    schemaVersion: 1,
    scope,
    commands: commandNames.slice(0, commandCount).map(name => ({ name, args: [] })),
  }
}

function evidence(
  commandName: string,
  status: VerificationEvidenceV1['status'],
): VerificationEvidenceV1 {
  return {
    schemaVersion: 1,
    commandName,
    args: [],
    exitCode: status === 'passed' ? 0 : status === 'failed' ? 1 : null,
    status,
    stdout: `${commandName}:${status}`,
    stderr: '',
    truncated: false,
    durationMs: 1,
  }
}

class FakeVerificationService {
  readonly calls: Array<{ readonly name: string; readonly args: readonly string[] }> = []

  constructor(private readonly results: readonly VerificationEvidenceV1[]) {}

  async run(name: string, args: readonly string[], signal: AbortSignal): Promise<VerificationEvidenceV1> {
    if (signal.aborted) throw signal.reason
    const result = this.results[this.calls.length]
    if (result === undefined) throw new Error(`missing verification result for ${name}`)
    this.calls.push({ name, args: [...args] })
    return result
  }
}

function runnerFixture(
  results: readonly VerificationEvidenceV1[],
  options: { readonly commandCount?: number; readonly actionLimit?: number } = {},
) {
  const rejections: BudgetRejection[] = []
  const controller = new BudgetController({
    maxWorkers: 8,
    maxPluginToolActions: options.actionLimit ?? 8,
    toolTimeoutMs: 60_000,
  }, rejection => rejections.push(rejection))
  const service = new FakeVerificationService(results)
  return {
    controller,
    rejections,
    service,
    options: {
      policy: policy(options.commandCount ?? results.length),
      controller,
      service: service as unknown as VerificationService,
      signal: new AbortController().signal,
    },
  }
}

describe('integrated parallel verification runner', () => {
  it('runs configured commands in order, admits one targeted_verify action each, and stops at the first non-passing evidence', async () => {
    const passed = evidence('typecheck', 'passed')
    const timedOut = evidence('test:profile', 'timed-out')
    const neverRun = evidence('lint', 'passed')
    const test = runnerFixture([passed, timedOut, neverRun])

    await expect(runParallelVerification(test.options, 2)).resolves.toEqual({
      outcome: 'command-failed',
      evidence: [passed, timedOut],
    })
    expect(test.service.calls.map(call => call.name)).toEqual(['typecheck', 'test:profile'])
    expect(test.controller.snapshot()).toMatchObject({ admittedPluginToolActions: 2 })
    expect(test.rejections).toEqual([])
  })

  it('preserves passing evidence before admission rejection and emits only the controller rejection', async () => {
    const passed = evidence('typecheck', 'passed')
    const test = runnerFixture([passed], { commandCount: 2, actionLimit: 1 })

    await expect(runParallelVerification(test.options, 1)).resolves.toEqual({
      outcome: 'admission-rejected',
      evidence: [passed],
    })
    expect(test.service.calls).toEqual([{ name: 'typecheck', args: [] }])
    expect(test.controller.snapshot()).toMatchObject({ admittedPluginToolActions: 1 })
    expect(test.rejections).toEqual([{ code: 'PLUGIN_TOOL_LIMIT', limit: 1, observed: 2 }])
  })

  it.each([
    [0, 1, 'not-run-no-commands'],
    [2, 0, 'not-run-no-accepted-nodes'],
  ] as const)('maps commands=%i accepted=%i to %s without admission or service calls', async (commands, accepted, outcome) => {
    const test = runnerFixture([], { commandCount: commands })

    await expect(runParallelVerification(test.options, accepted)).resolves.toEqual({ outcome, evidence: [] })
    expect(test.service.calls).toEqual([])
    expect(test.controller.snapshot()).toMatchObject({ admittedPluginToolActions: 0 })
    expect(test.rejections).toEqual([])
  })
})

describe('level verification fold', () => {
  const first: ParallelVerificationResultV1 = {
    outcome: 'passed',
    evidence: [evidence('typecheck', 'passed')],
  }
  const last: ParallelVerificationResultV1 = {
    outcome: 'command-failed',
    evidence: [evidence('test:profile', 'failed')],
  }

  it('returns only the last actual invocation', () => {
    expect(foldFinalLevelVerification(policy(2, 'level'), [first, last])).toEqual(last)
  })

  it('uses exact zero-command and no-actual-invocation outcomes', () => {
    expect(foldFinalLevelVerification(policy(0, 'level'), [first])).toEqual({
      outcome: 'not-run-no-commands',
      evidence: [],
    })
    expect(foldFinalLevelVerification(policy(2, 'level'), [])).toEqual({
      outcome: 'not-run-no-accepted-nodes',
      evidence: [],
    })
  })
})
