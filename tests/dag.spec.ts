import { describe, expect, it } from 'vitest'
import type { DagValidationIssueV1, DagValidationLimitsV1 } from '@ds-plugins/dsh-scheduling-contracts'
import { parseTaskDagV1 } from '@ds-plugins/dsh-scheduling-contracts'
import { compareDagValidationIssues, validateTaskDagV1 } from '../src/dag.ts'

const profile = {
  coding: 50,
  reasoning: 50,
  toolUse: 50,
  repoContext: 50,
  risk: 50,
  difficulty: 50,
} as const

const constraints = {
  maxWorkers: 1,
  maxOutputTokens: 32_000,
  maxLatencyMs: 60_000,
  allowPaidFallback: false,
  requiredTools: [],
} as const

interface NodeOptions {
  readonly dependsOn?: readonly string[]
  readonly readPaths?: readonly string[]
  readonly writePaths?: readonly string[]
}

function node(nodeId: string, dependsOnOrOptions: readonly string[] | NodeOptions = []) {
  const options = Array.isArray(dependsOnOrOptions)
    ? { dependsOn: dependsOnOrOptions }
    : dependsOnOrOptions as NodeOptions
  return {
    schemaVersion: 1,
    nodeId,
    objective: `Work on ${nodeId}.`,
    profile,
    constraints,
    readPaths: options.readPaths ?? [],
    writePaths: options.writePaths ?? [`src/${nodeId}.ts`],
    dependsOn: options.dependsOn ?? [],
  }
}

function dag(nodes: readonly ReturnType<typeof node>[]) {
  return { schemaVersion: 1, rootTaskId: 'root-1', nodes }
}

function limits(overrides: Partial<DagValidationLimitsV1> = {}) {
  return {
    schemaVersion: 1,
    maxNodes: 16,
    maxLevels: 4,
    maxWidth: 8,
    maxCumulativeWorkers: 16,
    ...overrides,
  } as const
}

function concurrentOverlapDag() {
  return dag([
    node('c', { readPaths: ['shared/wr.ts'], writePaths: ['shared/rw.ts'] }),
    node('a', { writePaths: ['shared/ww.ts', 'shared/wr.ts'] }),
    node('b', { readPaths: ['shared/rw.ts'], writePaths: ['shared/ww.ts'] }),
  ])
}

function wideDag(count: number) {
  return dag(Array.from({ length: count }, (_, index) => node(`n${index}`)))
}

function chainDag(count: number) {
  return dag(Array.from({ length: count }, (_, index) => node(`n${index}`, index === 0 ? [] : [`n${index - 1}`])))
}

describe('validateTaskDagV1', () => {
  it('omits all duplicate declarations and transitive descendants of direct missing dependencies', () => {
    const result = validateTaskDagV1(parseTaskDagV1(dag([
      node('dup'),
      node('dup'),
      node('missing-root', ['ghost']),
      node('downstream', ['missing-root']),
      node('ok'),
    ])), limits())

    expect(result.issues).toEqual([
      { code: 'duplicate-node', nodeId: 'dup' },
      { code: 'missing-dependency', nodeId: 'missing-root', dependsOn: 'ghost' },
    ])
    expect(result.levels).toEqual([['ok']])
    expect(result.levelCount).toBe(0)
  })

  it('emits one cycle issue per non-trivial SCC node, omits descendants, and does not double-report self cycles', () => {
    const result = validateTaskDagV1(parseTaskDagV1(dag([
      node('a', ['b']),
      node('b', ['a']),
      node('c', ['b']),
      node('self', ['self']),
    ])), limits())

    expect(result.issues).toEqual([
      { code: 'self-dependency', nodeId: 'self' },
      { code: 'cycle', nodeId: 'a' },
      { code: 'cycle', nodeId: 'b' },
    ])
    expect(result.levels).toEqual([])
    expect(result.levelCount).toBe(0)
  })

  it('orders each ready level lexically and reports the full dependency depth', () => {
    const result = validateTaskDagV1(parseTaskDagV1(dag([
      node('d', ['z', 'a']),
      node('z'),
      node('c', ['z']),
      node('a'),
      node('b', ['a']),
    ])), limits())

    expect(result.issues).toEqual([])
    expect(result.levels).toEqual([
      ['a', 'z'],
      ['b', 'c', 'd'],
    ])
    expect(result.levelCount).toBe(2)
  })

  it('prunes descendants of duplicate and self-dependent declarations without fabricating issues', () => {
    const result = validateTaskDagV1(parseTaskDagV1(dag([
      node('dup'),
      node('dup'),
      node('dup-child', ['dup']),
      node('self', ['self']),
      node('self-child', ['self']),
      node('ok'),
    ])), limits())

    expect(result.issues).toEqual([
      { code: 'duplicate-node', nodeId: 'dup' },
      { code: 'self-dependency', nodeId: 'self' },
    ])
    expect(result.levels).toEqual([['ok']])
    expect(result.levelCount).toBe(0)
  })

  it('retains direct missing diagnostics on a node also pruned by a duplicate dependency', () => {
    const result = validateTaskDagV1(parseTaskDagV1(dag([
      node('dup'),
      node('dup'),
      node('mixed', ['dup', 'ghost']),
    ])), limits())

    expect(result.issues).toEqual([
      { code: 'duplicate-node', nodeId: 'dup' },
      { code: 'missing-dependency', nodeId: 'mixed', dependsOn: 'ghost' },
    ])
    expect(result.levels).toEqual([])
    expect(result.levelCount).toBe(0)
  })

  it('retains direct missing diagnostics on a node also pruned by self-dependency', () => {
    const result = validateTaskDagV1(parseTaskDagV1(dag([
      node('mixed', ['mixed', 'ghost']),
    ])), limits())

    expect(result.issues).toEqual([
      { code: 'self-dependency', nodeId: 'mixed' },
      { code: 'missing-dependency', nodeId: 'mixed', dependsOn: 'ghost' },
    ])
    expect(result.levels).toEqual([])
    expect(result.levelCount).toBe(0)
  })

  it('compares only same-ready-level pairs with ww before wr before rw', () => {
    const result = validateTaskDagV1(parseTaskDagV1(concurrentOverlapDag()), limits())

    expect(result.issues.filter(issue => issue.code === 'overlapping-access')).toEqual([
      { code: 'overlapping-access', nodeA: 'a', nodeB: 'b', mode: 'ww' },
      { code: 'overlapping-access', nodeA: 'a', nodeB: 'c', mode: 'wr' },
      { code: 'overlapping-access', nodeA: 'b', nodeB: 'c', mode: 'rw' },
    ])
  })

  it('uses lexical file/directory containment and ignores overlaps across computed levels', () => {
    const result = validateTaskDagV1(parseTaskDagV1(dag([
      node('writer', { writePaths: ['src/'] }),
      node('peer', { writePaths: ['src/peer.ts'] }),
      node('reader', { dependsOn: ['writer'], readPaths: ['src/writer.ts'], writePaths: ['out/reader.ts'] }),
    ])), limits())

    expect(result.levels).toEqual([
      ['peer', 'writer'],
      ['reader'],
    ])
    expect(result.issues).toEqual([
      { code: 'overlapping-access', nodeA: 'peer', nodeB: 'writer', mode: 'ww' },
    ])
  })

  it('reports each graph-wide limit against the computed levels and declaration count', () => {
    expect(validateTaskDagV1(parseTaskDagV1(wideDag(9)), limits({ maxWidth: 8 })).issues).toContainEqual(
      { code: 'level-width-exceeded', level: 0, width: 9, limit: 8 },
    )
    expect(validateTaskDagV1(parseTaskDagV1(chainDag(5)), limits({ maxLevels: 4 })).issues).toContainEqual(
      { code: 'too-many-levels', levelCount: 5, limit: 4 },
    )
    expect(validateTaskDagV1(parseTaskDagV1(wideDag(16)), limits({ maxCumulativeWorkers: 15 })).issues).toContainEqual(
      { code: 'cumulative-worker-limit-exceeded', count: 16, limit: 15 },
    )
    expect(validateTaskDagV1(parseTaskDagV1(wideDag(4)), limits({ maxNodes: 3 })).issues).toContainEqual(
      { code: 'too-many-nodes', count: 4, limit: 3 },
    )
  })

  it('sorts every issue variant by its complete deterministic comparator key', () => {
    const issues: DagValidationIssueV1[] = [
      { code: 'level-width-exceeded', level: 1, width: 4, limit: 3 },
      { code: 'overlapping-access', nodeA: 'z', nodeB: 'a', mode: 'rw' },
      { code: 'cumulative-worker-limit-exceeded', count: 8, limit: 4 },
      { code: 'missing-dependency', nodeId: 'b', dependsOn: 'z' },
      { code: 'too-many-levels', levelCount: 4, limit: 2 },
      { code: 'cycle', nodeId: 'a' },
      { code: 'duplicate-node', nodeId: 'b' },
      { code: 'too-many-nodes', count: 7, limit: 3 },
      { code: 'self-dependency', nodeId: 'c' },
      { code: 'overlapping-access', nodeA: 'a', nodeB: 'z', mode: 'ww' },
      { code: 'missing-dependency', nodeId: 'b', dependsOn: 'a' },
    ]

    expect(issues.sort(compareDagValidationIssues)).toEqual([
      { code: 'duplicate-node', nodeId: 'b' },
      { code: 'self-dependency', nodeId: 'c' },
      { code: 'missing-dependency', nodeId: 'b', dependsOn: 'a' },
      { code: 'missing-dependency', nodeId: 'b', dependsOn: 'z' },
      { code: 'cycle', nodeId: 'a' },
      { code: 'overlapping-access', nodeA: 'a', nodeB: 'z', mode: 'ww' },
      { code: 'overlapping-access', nodeA: 'z', nodeB: 'a', mode: 'rw' },
      { code: 'too-many-nodes', count: 7, limit: 3 },
      { code: 'too-many-levels', levelCount: 4, limit: 2 },
      { code: 'level-width-exceeded', level: 1, width: 4, limit: 3 },
      { code: 'cumulative-worker-limit-exceeded', count: 8, limit: 4 },
    ])
  })

  it('returns a detached deeply frozen validation projection', () => {
    const result = validateTaskDagV1(parseTaskDagV1(dag([node('b'), node('a')])), limits())

    expect(result).toEqual({ schemaVersion: 1, valid: true, issues: [], levels: [['a', 'b']], levelCount: 1 })
    expect(Object.isFrozen(result)).toBe(true)
    expect(Object.isFrozen(result.issues)).toBe(true)
    expect(Object.isFrozen(result.levels)).toBe(true)
    expect(Object.isFrozen(result.levels[0])).toBe(true)
  })
})
