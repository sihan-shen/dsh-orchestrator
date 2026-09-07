import {
  parseDagValidationLimitsV1,
  repoPathContains,
} from '@ds-plugins/dsh-scheduling-contracts'
import type {
  DagValidationIssueV1,
  DagValidationLimitsV1,
  DagValidationV1,
  RepoFilePath,
  RepoPathDeclaration,
  TaskDagV1,
  TaskNodeV1,
} from '@ds-plugins/dsh-scheduling-contracts'

type EligibleNodes = Map<string, TaskNodeV1>

const ISSUE_RANK: Readonly<Record<DagValidationIssueV1['code'], number>> = {
  'duplicate-node': 1,
  'self-dependency': 2,
  'missing-dependency': 3,
  cycle: 4,
  'overlapping-access': 5,
  'too-many-nodes': 6,
  'too-many-levels': 7,
  'level-width-exceeded': 8,
  'cumulative-worker-limit-exceeded': 9,
}

const OVERLAP_MODE_RANK: Readonly<Record<'ww' | 'wr' | 'rw', number>> = {
  ww: 1,
  wr: 2,
  rw: 3,
}

function lexicalCompare(left: string, right: string): number {
  return left < right ? -1 : left > right ? 1 : 0
}

function compareKeyPart(left: string | number, right: string | number): number {
  if (typeof left === 'number' && typeof right === 'number') return left - right
  return lexicalCompare(String(left), String(right))
}

function issueKey(issue: DagValidationIssueV1): readonly (string | number)[] {
  const rank = ISSUE_RANK[issue.code]
  switch (issue.code) {
    case 'duplicate-node':
    case 'self-dependency':
    case 'cycle':
      return [rank, issue.nodeId]
    case 'missing-dependency':
      return [rank, issue.nodeId, issue.dependsOn]
    case 'overlapping-access': {
      const [nodeA, nodeB] = [issue.nodeA, issue.nodeB].sort(lexicalCompare)
      return [rank, nodeA!, nodeB!, OVERLAP_MODE_RANK[issue.mode]]
    }
    case 'too-many-nodes':
    case 'cumulative-worker-limit-exceeded':
      return [rank, issue.limit, issue.count]
    case 'too-many-levels':
      return [rank, issue.limit, issue.levelCount]
    case 'level-width-exceeded':
      return [rank, issue.level, issue.limit, issue.width]
  }
}

export function compareDagValidationIssues(a: DagValidationIssueV1, b: DagValidationIssueV1): number {
  const left = issueKey(a)
  const right = issueKey(b)
  for (let index = 0; index < Math.max(left.length, right.length); index += 1) {
    if (left[index] === undefined) return -1
    if (right[index] === undefined) return 1
    const result = compareKeyPart(left[index], right[index])
    if (result !== 0) return result
  }
  return 0
}

function uniqueDeclarations(nodes: readonly TaskNodeV1[], issues: DagValidationIssueV1[]): {
  readonly declaredIds: ReadonlySet<string>
  readonly duplicateIds: ReadonlySet<string>
  readonly eligible: EligibleNodes
} {
  const declarations = new Map<string, TaskNodeV1[]>()
  for (const node of nodes) {
    const matches = declarations.get(node.nodeId)
    if (matches === undefined) declarations.set(node.nodeId, [node])
    else matches.push(node)
  }

  const eligible: EligibleNodes = new Map()
  const duplicateIds = new Set<string>()
  for (const nodeId of [...declarations.keys()].sort(lexicalCompare)) {
    const matches = declarations.get(nodeId)!
    if (matches.length === 1) {
      eligible.set(nodeId, matches[0]!)
    } else {
      duplicateIds.add(nodeId)
      issues.push({ code: 'duplicate-node', nodeId })
    }
  }
  return { declaredIds: new Set(declarations.keys()), duplicateIds, eligible }
}

function pruneRootsAndDescendants(eligible: EligibleNodes, roots: ReadonlySet<string>): void {
  if (roots.size === 0) return
  const excluded = new Set(roots)
  let changed = true
  while (changed) {
    changed = false
    for (const [nodeId, node] of eligible) {
      if (excluded.has(nodeId)) continue
      if (node.dependsOn.some(dependency => excluded.has(dependency))) {
        excluded.add(nodeId)
        changed = true
      }
    }
  }
  for (const nodeId of excluded) eligible.delete(nodeId)
}

function selfDependencyRoots(eligible: EligibleNodes, issues: DagValidationIssueV1[]): ReadonlySet<string> {
  const roots = new Set<string>()
  for (const nodeId of [...eligible.keys()].sort(lexicalCompare)) {
    if (eligible.get(nodeId)!.dependsOn.includes(nodeId)) {
      roots.add(nodeId)
      issues.push({ code: 'self-dependency', nodeId })
    }
  }
  return roots
}

function missingDependencyRoots(
  eligible: EligibleNodes,
  declaredIds: ReadonlySet<string>,
  issues: DagValidationIssueV1[],
): ReadonlySet<string> {
  const roots = new Set<string>()
  for (const nodeId of [...eligible.keys()].sort(lexicalCompare)) {
    const node = eligible.get(nodeId)!
    for (const dependency of node.dependsOn) {
      if (!declaredIds.has(dependency)) {
        roots.add(nodeId)
        issues.push({ code: 'missing-dependency', nodeId, dependsOn: dependency })
      }
    }
  }
  return roots
}

function tarjanScc(eligible: EligibleNodes): readonly (readonly string[])[] {
  let nextIndex = 0
  const indices = new Map<string, number>()
  const lowLinks = new Map<string, number>()
  const stack: string[] = []
  const onStack = new Set<string>()
  const components: string[][] = []

  const visit = (nodeId: string): void => {
    const index = nextIndex
    nextIndex += 1
    indices.set(nodeId, index)
    lowLinks.set(nodeId, index)
    stack.push(nodeId)
    onStack.add(nodeId)

    const dependencies = [...new Set(eligible.get(nodeId)!.dependsOn)]
      .filter(dependency => eligible.has(dependency))
      .sort(lexicalCompare)
    for (const dependency of dependencies) {
      if (!indices.has(dependency)) {
        visit(dependency)
        lowLinks.set(nodeId, Math.min(lowLinks.get(nodeId)!, lowLinks.get(dependency)!))
      } else if (onStack.has(dependency)) {
        lowLinks.set(nodeId, Math.min(lowLinks.get(nodeId)!, indices.get(dependency)!))
      }
    }

    if (lowLinks.get(nodeId) !== indices.get(nodeId)) return
    const component: string[] = []
    let member: string
    do {
      member = stack.pop()!
      onStack.delete(member)
      component.push(member)
    } while (member !== nodeId)
    components.push(component.sort(lexicalCompare))
  }

  for (const nodeId of [...eligible.keys()].sort(lexicalCompare)) {
    if (!indices.has(nodeId)) visit(nodeId)
  }
  return components
}

function removeCyclesAndDescendants(
  eligible: EligibleNodes,
  components: readonly (readonly string[])[],
  issues: DagValidationIssueV1[],
): void {
  const roots = new Set<string>()
  for (const component of components) {
    if (component.length < 2) continue
    for (const nodeId of component) {
      roots.add(nodeId)
      issues.push({ code: 'cycle', nodeId })
    }
  }
  pruneRootsAndDescendants(eligible, roots)
}

function deterministicLevels(eligible: EligibleNodes): string[][] {
  const dependencyCount = new Map<string, number>()
  const dependents = new Map<string, Set<string>>()
  for (const [nodeId, node] of eligible) {
    const dependencies = new Set(node.dependsOn.filter(dependency => eligible.has(dependency)))
    dependencyCount.set(nodeId, dependencies.size)
    for (const dependency of dependencies) {
      const children = dependents.get(dependency)
      if (children === undefined) dependents.set(dependency, new Set([nodeId]))
      else children.add(nodeId)
    }
  }

  let ready = [...eligible.keys()].filter(nodeId => dependencyCount.get(nodeId) === 0).sort(lexicalCompare)
  const levels: string[][] = []
  while (ready.length > 0) {
    const level = ready
    levels.push(level)
    const next = new Set<string>()
    for (const nodeId of level) {
      for (const dependent of dependents.get(nodeId) ?? []) {
        const remaining = dependencyCount.get(dependent)! - 1
        dependencyCount.set(dependent, remaining)
        if (remaining === 0) next.add(dependent)
      }
    }
    ready = [...next].sort(lexicalCompare)
  }
  return levels
}

function declarationsOverlap(left: RepoPathDeclaration, right: RepoPathDeclaration): boolean {
  if (!left.endsWith('/')) return repoPathContains(right, left as RepoFilePath)
  if (!right.endsWith('/')) return repoPathContains(left, right as RepoFilePath)
  return left.startsWith(right) || right.startsWith(left)
}

function pathSetsOverlap(left: readonly RepoPathDeclaration[], right: readonly RepoPathDeclaration[]): boolean {
  return left.some(leftPath => right.some(rightPath => declarationsOverlap(leftPath, rightPath)))
}

function overlapMode(nodeA: TaskNodeV1, nodeB: TaskNodeV1): 'ww' | 'wr' | 'rw' | undefined {
  if (pathSetsOverlap(nodeA.writePaths, nodeB.writePaths)) return 'ww'
  if (pathSetsOverlap(nodeA.writePaths, nodeB.readPaths)) return 'wr'
  if (pathSetsOverlap(nodeA.readPaths, nodeB.writePaths)) return 'rw'
  return undefined
}

function addConcurrentOverlapIssues(
  levels: readonly (readonly string[])[],
  eligible: ReadonlyMap<string, TaskNodeV1>,
  issues: DagValidationIssueV1[],
): void {
  for (const level of levels) {
    for (let left = 0; left < level.length; left += 1) {
      for (let right = left + 1; right < level.length; right += 1) {
        const nodeA = level[left]!
        const nodeB = level[right]!
        const mode = overlapMode(eligible.get(nodeA)!, eligible.get(nodeB)!)
        if (mode !== undefined) issues.push({ code: 'overlapping-access', nodeA, nodeB, mode })
      }
    }
  }
}

function addGraphLimitIssues(
  declarationCount: number,
  levels: readonly (readonly string[])[],
  limits: DagValidationLimitsV1,
  issues: DagValidationIssueV1[],
): void {
  if (declarationCount > limits.maxNodes) {
    issues.push({ code: 'too-many-nodes', count: declarationCount, limit: limits.maxNodes })
  }
  if (levels.length > limits.maxLevels) {
    issues.push({ code: 'too-many-levels', levelCount: levels.length, limit: limits.maxLevels })
  }
  for (let level = 0; level < levels.length; level += 1) {
    const width = levels[level]!.length
    if (width > limits.maxWidth) {
      issues.push({ code: 'level-width-exceeded', level, width, limit: limits.maxWidth })
    }
  }
  if (declarationCount > limits.maxCumulativeWorkers) {
    issues.push({
      code: 'cumulative-worker-limit-exceeded',
      count: declarationCount,
      limit: limits.maxCumulativeWorkers,
    })
  }
}

function deepFreezeDagValidation(value: DagValidationV1): DagValidationV1 {
  for (const issue of value.issues) Object.freeze(issue)
  for (const level of value.levels) Object.freeze(level)
  Object.freeze(value.issues)
  Object.freeze(value.levels)
  return Object.freeze(value)
}

export function validateTaskDagV1(dag: TaskDagV1, limits: DagValidationLimitsV1): DagValidationV1 {
  const parsedLimits = parseDagValidationLimitsV1(limits)
  const issues: DagValidationIssueV1[] = []
  const { declaredIds, duplicateIds, eligible } = uniqueDeclarations(dag.nodes, issues)
  const selfRoots = selfDependencyRoots(eligible, issues)
  const missingRoots = missingDependencyRoots(eligible, declaredIds, issues)
  pruneRootsAndDescendants(eligible, duplicateIds)
  pruneRootsAndDescendants(eligible, selfRoots)
  pruneRootsAndDescendants(eligible, missingRoots)
  removeCyclesAndDescendants(eligible, tarjanScc(eligible), issues)
  const levels = deterministicLevels(eligible)
  addConcurrentOverlapIssues(levels, eligible, issues)
  addGraphLimitIssues(dag.nodes.length, levels, parsedLimits, issues)
  issues.sort(compareDagValidationIssues)
  const hadExclusion = issues.some(issue =>
    issue.code === 'duplicate-node'
    || issue.code === 'self-dependency'
    || issue.code === 'missing-dependency'
    || issue.code === 'cycle')
  return deepFreezeDagValidation({
    schemaVersion: 1,
    valid: issues.length === 0,
    issues,
    levels,
    levelCount: hadExclusion ? 0 : levels.length,
  })
}
