import type {
  ParallelVerificationPolicyV1,
  VerificationOutcomeV1,
} from '@ds-plugins/dsh-scheduling-contracts'
import type { BudgetController } from './budgets.js'
import type { VerificationEvidenceV1 } from './types.js'
import type { VerificationService } from './verification.js'

export interface ParallelVerificationResultV1 {
  readonly outcome: VerificationOutcomeV1
  readonly evidence: readonly VerificationEvidenceV1[]
}

export interface ParallelVerificationRunnerOptions {
  readonly policy: ParallelVerificationPolicyV1
  readonly controller: BudgetController
  readonly service: VerificationService
  readonly signal: AbortSignal
}

/** Run deployment-captured integrated verification in policy order. */
export async function runParallelVerification(
  options: ParallelVerificationRunnerOptions,
  acceptedNodeCount: number,
): Promise<ParallelVerificationResultV1> {
  if (options.policy.commands.length === 0) {
    return { outcome: 'not-run-no-commands', evidence: [] }
  }
  if (acceptedNodeCount === 0) {
    return { outcome: 'not-run-no-accepted-nodes', evidence: [] }
  }

  const evidence: VerificationEvidenceV1[] = []
  for (const command of options.policy.commands) {
    const admission = options.controller.admitPluginTool('targeted_verify')
    if (!admission.allowed) return { outcome: 'admission-rejected', evidence }

    const item = await options.service.run(command.name, command.args, options.signal)
    evidence.push(item)
    if (item.status !== 'passed') return { outcome: 'command-failed', evidence }
  }

  return { outcome: 'passed', evidence }
}

/** Select the final verification state for a cumulative level-scope aggregate. */
export function foldFinalLevelVerification(
  policy: ParallelVerificationPolicyV1,
  invocations: readonly ParallelVerificationResultV1[],
): ParallelVerificationResultV1 {
  if (policy.commands.length === 0) return { outcome: 'not-run-no-commands', evidence: [] }
  return invocations.at(-1) ?? { outcome: 'not-run-no-accepted-nodes', evidence: [] }
}
