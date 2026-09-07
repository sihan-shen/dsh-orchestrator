import { describe, expect, it } from 'vitest'
import { Context } from '@deepseek-ai/cordis'
import SessionStore, { SessionId } from '@deepseek-ai/dsh-session'
import {
  BudgetController,
  createBudgetControllerRegistry,
  mountBudgetControllerRegistry,
  type BudgetRejection,
} from '../src/budgets.ts'
import type { OrchestratorConfig } from '../src/types.ts'

const budgets: OrchestratorConfig['budgets'] = {
  maxWorkers: 1,
  maxPluginToolActions: 2,
  toolTimeoutMs: 30_000,
}

function rejections(): { readonly values: BudgetRejection[]; readonly record: (rejection: BudgetRejection) => void } {
  const values: BudgetRejection[] = []
  return {
    values,
    record(rejection) {
      values.push(rejection)
    },
  }
}

function seededController(
  seed: { readonly workers: number; readonly actions: number; readonly workerLimit: number; readonly actionLimit: number },
) {
  const recorder = rejections()
  const controller = new BudgetController({
    maxWorkers: seed.workerLimit,
    maxPluginToolActions: seed.actionLimit,
    toolTimeoutMs: 30_000,
  }, recorder.record)
  for (let index = 0; index < seed.workers; index += 1) expect(controller.admitWorker()).toEqual({ allowed: true })
  for (let index = 0; index < seed.actions; index += 1) expect(controller.admitPluginTool('targeted_verify')).toEqual({ allowed: true })
  return { controller, recorder }
}

describe('deterministic budget admission', () => {
  it('commits one action and N workers atomically', () => {
    const recorder = rejections()
    const controller = new BudgetController({ maxWorkers: 8, maxPluginToolActions: 4, toolTimeoutMs: 30_000 }, recorder.record)

    expect(controller.admitFanout(3)).toEqual({ allowed: true })
    expect(controller.snapshot()).toMatchObject({ admittedWorkers: 3, admittedPluginToolActions: 1 })
  })

  it('rolls back both counters and records only the more-negative rejection, worker on ties', () => {
    const { controller, recorder } = seededController({ workers: 7, actions: 4, workerLimit: 8, actionLimit: 4 })

    expect(controller.admitFanout(3)).toEqual({ allowed: false, code: 'WORKER_LIMIT', limit: 8, observed: 10 })
    expect(controller.snapshot()).toMatchObject({ admittedWorkers: 7, admittedPluginToolActions: 4 })
    expect(recorder.values).toHaveLength(1)

    const tie = seededController({ workers: 6, actions: 4, workerLimit: 8, actionLimit: 4 })
    expect(tie.controller.admitFanout(3)).toEqual({ allowed: false, code: 'WORKER_LIMIT', limit: 8, observed: 9 })
    expect(tie.recorder.values).toHaveLength(1)

    const action = seededController({ workers: 5, actions: 4, workerLimit: 8, actionLimit: 4 })
    expect(action.controller.admitFanout(3)).toEqual({ allowed: false, code: 'PLUGIN_TOOL_LIMIT', limit: 4, observed: 5 })
    expect(action.controller.snapshot()).toMatchObject({ admittedWorkers: 5, admittedPluginToolActions: 4 })
    expect(action.recorder.values).toHaveLength(1)
  })

  it('gives DISPOSED precedence and rejects zero or non-integral fanout without recording', () => {
    const { controller, recorder } = seededController({ workers: 0, actions: 0, workerLimit: 8, actionLimit: 4 })
    controller.dispose()

    expect(controller.admitFanout(1)).toMatchObject({ code: 'DISPOSED' })
    expect(() => controller.admitFanout(0)).toThrow()
    expect(() => controller.admitFanout(1.5)).toThrow()
    expect(recorder.values).toHaveLength(1)
  })

  it('rejects fanout widths above the contracts ceiling without recording or changing counters', () => {
    const recorder = rejections()
    const controller = new BudgetController({ maxWorkers: 8, maxPluginToolActions: 4, toolTimeoutMs: 30_000 }, recorder.record)

    expect(() => controller.admitFanout(9)).toThrow(/1\.\.8/u)
    expect(controller.snapshot()).toMatchObject({ admittedWorkers: 0, admittedPluginToolActions: 0 })
    expect(recorder.values).toEqual([])
  })

  it('exposes an immutable read-only scheduling budget snapshot', () => {
    const recorder = rejections()
    const controller = new BudgetController({ ...budgets, maxPluginToolActions: 24 }, recorder.record)

    expect(controller.snapshot()).toEqual({
      maxWorkers: 1,
      admittedWorkers: 0,
      maxPluginToolActions: 24,
      admittedPluginToolActions: 0,
      remainingWorkers: 1,
      remainingPluginToolActions: 24,
    })
    controller.admitPluginTool('delegate_worker')
    expect(controller.snapshot()).toMatchObject({ admittedPluginToolActions: 1, remainingPluginToolActions: 23 })
    expect('admitWorker' in controller.snapshot()).toBe(false)
    expect(Object.isFrozen(controller.snapshot())).toBe(true)
  })

  it('rejects the first worker in Direct mode and records its exact rejected observation', () => {
    const recorder = rejections()
    const controller = new BudgetController({ ...budgets, maxWorkers: 0 }, recorder.record)

    expect(controller.admitWorker()).toEqual({
      allowed: false,
      code: 'WORKER_LIMIT',
      limit: 0,
      observed: 1,
    })
    expect(recorder.values).toEqual([{ code: 'WORKER_LIMIT', limit: 0, observed: 1 }])
  })

  it('admits exactly one worker and keeps rejection observations stable without incrementing', () => {
    const recorder = rejections()
    const controller = new BudgetController(budgets, recorder.record)

    expect(controller.admitWorker()).toEqual({ allowed: true })
    expect(controller.admitWorker()).toEqual({
      allowed: false,
      code: 'WORKER_LIMIT',
      limit: 1,
      observed: 2,
    })
    expect(controller.admitWorker()).toEqual({
      allowed: false,
      code: 'WORKER_LIMIT',
      limit: 1,
      observed: 2,
    })
    expect(recorder.values).toEqual([
      { code: 'WORKER_LIMIT', limit: 1, observed: 2 },
      { code: 'WORKER_LIMIT', limit: 1, observed: 2 },
    ])
  })

  it('counts only the two registered plugin tools and rejects excess actions without incrementing', () => {
    const recorder = rejections()
    const controller = new BudgetController(budgets, recorder.record)

    expect(controller.admitPluginTool('targeted_verify')).toEqual({ allowed: true })
    expect(controller.admitPluginTool('delegate_worker')).toEqual({ allowed: true })
    expect(controller.admitPluginTool('targeted_verify')).toEqual({
      allowed: false,
      code: 'PLUGIN_TOOL_LIMIT',
      limit: 2,
      observed: 3,
    })
    expect(controller.admitPluginTool('targeted_verify')).toEqual({
      allowed: false,
      code: 'PLUGIN_TOOL_LIMIT',
      limit: 2,
      observed: 3,
    })
    expect(() => controller.admitPluginTool('arbitrary-tool' as never)).toThrow(/unknown plugin tool action/i)
    expect(recorder.values).toEqual([
      { code: 'PLUGIN_TOOL_LIMIT', limit: 2, observed: 3 },
      { code: 'PLUGIN_TOOL_LIMIT', limit: 2, observed: 3 },
    ])
  })

  it('rejects disposed admissions with the configured limit and a non-incrementing next observation', () => {
    const recorder = rejections()
    const controller = new BudgetController(budgets, recorder.record)

    expect(controller.admitWorker()).toEqual({ allowed: true })
    expect(controller.admitPluginTool('targeted_verify')).toEqual({ allowed: true })
    controller.dispose()

    expect(controller.admitWorker()).toEqual({
      allowed: false,
      code: 'DISPOSED',
      limit: 1,
      observed: 2,
    })
    expect(controller.admitPluginTool('targeted_verify')).toEqual({
      allowed: false,
      code: 'DISPOSED',
      limit: 2,
      observed: 2,
    })
    expect(recorder.values).toEqual([
      { code: 'DISPOSED', limit: 1, observed: 2 },
      { code: 'DISPOSED', limit: 2, observed: 2 },
    ])
  })

  it('isolates counters by root session id and starts fresh after a registry lifecycle ends', () => {
    const recorder = rejections()
    const registry = createBudgetControllerRegistry(budgets, () => recorder.record)
    const rootA = SessionId('budget-root-a')
    const rootB = SessionId('budget-root-b')

    expect(registry.forRootSession(rootA).admitWorker()).toEqual({ allowed: true })
    expect(registry.forRootSession(rootA).admitWorker()).toMatchObject({
      allowed: false,
      code: 'WORKER_LIMIT',
    })
    expect(registry.forRootSession(rootB).admitWorker()).toEqual({ allowed: true })

    registry.disposeSession(rootA)
    expect(registry.forRootSession(rootA).admitWorker()).toEqual({ allowed: true })
  })

  it('removes terminal session state and clears all controllers when its Cordis effect is disposed', async () => {
    const ctx = new Context()
    const sessionStore = await ctx.plugin(SessionStore)
    const recorder = rejections()
    const mounted = mountBudgetControllerRegistry(ctx, budgets, () => recorder.record)
    const session = ctx.sessions.prepare(SessionId('budget-lifecycle'))
    const detach = ctx.sessions.enter(session)
    ctx.sessions.announce(session)

    const controller = mounted.registry.forRootSession(session.id)
    expect(controller.admitWorker()).toEqual({ allowed: true })
    detach()
    expect(mounted.registry.forRootSession(session.id).admitWorker()).toEqual({ allowed: true })

    await mounted.dispose()
    expect(mounted.registry.forRootSession(session.id).admitWorker()).toEqual({
      allowed: false,
      code: 'DISPOSED',
      limit: 1,
      observed: 1,
    })

    const remounted = mountBudgetControllerRegistry(ctx, budgets, () => recorder.record)
    expect(remounted.registry.forRootSession(session.id).admitWorker()).toEqual({ allowed: true })
    await remounted.dispose()
    await sessionStore.dispose()
  })
})
