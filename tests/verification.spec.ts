import { describe, expect, it, vi } from 'vitest'
import { Session, SessionId } from '@deepseek-ai/dsh-session'
import {
  createTargetedVerificationTool,
  VERIFICATION_CLEANUP_ALLOWANCE_MS,
  validateParallelVerificationPolicy,
  validateVerificationArguments,
  VerificationService,
} from '../src/verification.ts'
import { apply } from '../src/index.ts'

interface FakeOutcome {
  readonly exitCode: number | null
  readonly signal: string | null
}

interface FakeSpawnSpec {
  readonly argv: readonly string[]
  readonly cwd: string
  readonly stdio: {
    readonly stdin: 'ignore'
    readonly stdout: { readonly maxBytes: number }
    readonly stderr: { readonly maxBytes: number }
  }
  readonly graceMs: number
  readonly signal?: AbortSignal
}

interface FakeHandle {
  readonly done: Promise<FakeOutcome>
  readonly collected: {
    readonly stdout?: { readFrom(offset: number): { text: string; lossy: boolean } }
    readonly stderr?: { readFrom(offset: number): { text: string; lossy: boolean } }
  }
  terminate(): void
  waitForExit(signal?: AbortSignal): Promise<boolean>
}

type HandleFactory = (spec: FakeSpawnSpec) => FakeHandle

class FakeSubprocess {
  readonly spawns: FakeSpawnSpec[] = []

  constructor(private readonly next: HandleFactory) {}

  spawn(spec: FakeSpawnSpec): FakeHandle {
    this.spawns.push(spec)
    return this.next(spec)
  }
}

function handle(
  outcome: Promise<FakeOutcome>,
  options: { readonly stdout?: string; readonly stderr?: string; readonly stdoutLossy?: boolean; readonly stderrLossy?: boolean } = {},
): FakeHandle {
  return {
    done: outcome,
    collected: {
      stdout: {
        readFrom: () => ({ text: options.stdout ?? '', lossy: options.stdoutLossy ?? false }),
      },
      stderr: {
        readFrom: () => ({ text: options.stderr ?? '', lossy: options.stderrLossy ?? false }),
      },
    },
    terminate: () => undefined,
    waitForExit: async () => true,
  }
}

const workspaceRoot = '/workspace/ds-plugins'
const verification = {
  commands: [
    { name: 'typecheck', executable: 'pnpm', fixedArgs: ['typecheck'], allowedArgs: 'none' },
    { name: 'test:orchestrator', executable: 'pnpm', fixedArgs: ['exec', 'vitest', 'run'], allowedArgs: 'orchestrator-test-paths' },
  ],
  timeoutMs: 20,
  maxOutputBytes: 8,
} as const

function service(subprocess: FakeSubprocess, finished: unknown[] = []) {
  return new VerificationService({
    workspaceRoot,
    verification,
    subprocess,
    appendEvidence(evidence) {
      finished.push(evidence)
    },
  })
}

describe('targeted verification service', () => {
  it('exports the configured command argument validator', () => {
    const command = verification.commands[1]
    expect(validateVerificationArguments(command, ['packages/dsh-orchestrator/tests/ok.spec.ts']))
      .toEqual(['packages/dsh-orchestrator/tests/ok.spec.ts'])
    expect(() => validateVerificationArguments(verification.commands[0], ['--all']))
      .toThrow(/accepts no caller arguments/u)
  })

  it('validates parallel verification policies against deployment commands and arguments', () => {
    const policy = {
      schemaVersion: 1 as const,
      scope: 'dag' as const,
      commands: [{ name: 'typecheck', args: [] }],
    }
    expect(validateParallelVerificationPolicy(policy, verification)).toEqual(policy)
    expect(() => validateParallelVerificationPolicy({
      ...policy,
      commands: [{ name: 'shell', args: [] }],
    }, verification)).toThrow(/unknown parallel verification command/u)
    expect(() => validateParallelVerificationPolicy({
      ...policy,
      commands: [{ name: 'typecheck', args: ['--all'] }],
    }, verification)).toThrow(/accepts no caller arguments/u)
  })

  it('runs a configured command directly and records a passed evidence event', async () => {
    const subprocess = new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 0, signal: null }), {
      stdout: 'all checks passed\n',
    }))
    const finished: unknown[] = []

    const evidence = await service(subprocess, finished).run('typecheck', [], new AbortController().signal)

    expect(evidence).toMatchObject({
      schemaVersion: 1,
      commandName: 'typecheck',
      args: [],
      exitCode: 0,
      status: 'passed',
      stderr: '',
      truncated: true,
    })
    expect(Buffer.byteLength(evidence.stdout)).toBeLessThanOrEqual(verification.maxOutputBytes)
    expect(finished).toEqual([evidence])
    expect(subprocess.spawns).toHaveLength(1)
    const [spawnSpec] = subprocess.spawns
    expect(spawnSpec?.argv).toEqual(['pnpm', 'typecheck'])
    expect(spawnSpec?.cwd).toBe(workspaceRoot)
    expect(spawnSpec).not.toHaveProperty('shell')
    expect(spawnSpec).not.toHaveProperty('env')
  })

  it('records a non-zero process outcome as failed', async () => {
    const finished: unknown[] = []
    const evidence = await service(new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 1, signal: null }), {
      stderr: 'typecheck failed',
    })), finished).run('typecheck', [], new AbortController().signal)

    expect(evidence).toMatchObject({ status: 'failed', exitCode: 1 })
    expect(finished).toEqual([evidence])
  })

  it('owns a timeout, terminates through the subprocess signal, and records timed-out evidence', async () => {
    let received: FakeSpawnSpec | undefined
    const finished: unknown[] = []
    const subprocess = new FakeSubprocess(spec => {
      received = spec
      return handle(new Promise(resolve => {
        spec.signal?.addEventListener('abort', () => resolve({ exitCode: null, signal: 'SIGTERM' }), { once: true })
      }))
    })

    const evidence = await service(subprocess, finished).run('typecheck', [], new AbortController().signal)

    expect(received?.signal?.aborted).toBe(true)
    expect(evidence).toMatchObject({ status: 'timed-out', exitCode: null })
    expect(finished).toEqual([evidence])
  })

  it('bounds a non-quiescing process cleanup to the advertised timeout allowance', async () => {
    vi.useFakeTimers()
    try {
      const finished: unknown[] = []
      const waitForExit = vi.fn(async (signal?: AbortSignal) => new Promise<boolean>(resolve => {
        signal?.addEventListener('abort', () => resolve(false), { once: true })
      }))
      const subprocess = new FakeSubprocess(() => ({
        ...handle(new Promise<FakeOutcome>(() => undefined)),
        waitForExit,
      }))

      const running = service(subprocess, finished).run('typecheck', [], new AbortController().signal)

      await vi.advanceTimersByTimeAsync(verification.timeoutMs + VERIFICATION_CLEANUP_ALLOWANCE_MS)

      await expect(running).resolves.toMatchObject({ status: 'timed-out', exitCode: null })
      expect(waitForExit).toHaveBeenCalledTimes(1)
      expect(waitForExit.mock.calls[0]?.[0]?.aborted).toBe(true)
      expect(finished).toMatchObject([{ status: 'timed-out', exitCode: null }])
    } finally {
      vi.useRealTimers()
    }
  })

  it('maps a spawn-level failure to spawn-error and records it', async () => {
    const finished: unknown[] = []
    const evidence = await service(new FakeSubprocess(() => handle(Promise.reject(new Error('spawn failed')))), finished)
      .run('typecheck', [], new AbortController().signal)

    expect(evidence).toMatchObject({ status: 'spawn-error', exitCode: null })
    expect(finished).toEqual([evidence])
  })

  it('propagates caller cancellation after recording the actual terminated process outcome', async () => {
    const controller = new AbortController()
    const reason = new Error('caller cancelled verification')
    const finished: unknown[] = []
    const subprocess = new FakeSubprocess(spec => handle(new Promise(resolve => {
      spec.signal?.addEventListener('abort', () => resolve({ exitCode: null, signal: 'SIGTERM' }), { once: true })
    })))
    const running = service(subprocess, finished).run('typecheck', [], controller.signal)

    controller.abort(reason)

    await expect(running).rejects.toBe(reason)
    expect(finished).toMatchObject([{ status: 'failed', exitCode: null }])
  })

  it('preserves a caller cancellation that occurs before the deadline when process settlement crosses it', async () => {
    const controller = new AbortController()
    const reason = new Error('caller cancelled before deadline')
    const finished: unknown[] = []
    const subprocess = new FakeSubprocess(spec => handle(new Promise(resolve => {
      spec.signal?.addEventListener('abort', () => {
        setTimeout(() => resolve({ exitCode: null, signal: 'SIGTERM' }), verification.timeoutMs + 10)
      }, { once: true })
    })))
    const running = service(subprocess, finished).run('typecheck', [], controller.signal)

    controller.abort(reason)

    await expect(running).rejects.toBe(reason)
    expect(finished).toMatchObject([{ status: 'failed', exitCode: null }])
    expect(finished).not.toMatchObject([{ status: 'timed-out' }])
  })

  it('propagates caller cancellation that occurs while cleanup waits for process quiescence', async () => {
    const controller = new AbortController()
    const reason = new Error('caller cancelled during cleanup')
    const finished: unknown[] = []
    let signalWaitStarted = () => undefined
    const waitStarted = new Promise<void>(resolve => { signalWaitStarted = resolve })
    let releaseWait = (_value: boolean) => undefined
    const quiescent = new Promise<boolean>(resolve => { releaseWait = resolve })
    const subprocess = new FakeSubprocess(() => ({
      ...handle(Promise.resolve({ exitCode: 0, signal: null })),
      waitForExit: async () => {
        signalWaitStarted()
        return quiescent
      },
    }))
    const running = service(subprocess, finished).run('typecheck', [], controller.signal)

    await waitStarted
    controller.abort(reason)
    releaseWait(true)

    await expect(running).rejects.toBe(reason)
    expect(finished).toMatchObject([{ status: 'passed', exitCode: 0 }])
  })

  it('bounds UTF-8 stdout and stderr together without leaving an invalid code point', async () => {
    const evidence = await service(new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 0, signal: null }), {
      stdout: '€abc',
      stderr: 'éxyz',
    })), []).run('typecheck', [], new AbortController().signal)

    expect(Buffer.byteLength(evidence.stdout) + Buffer.byteLength(evidence.stderr)).toBeLessThanOrEqual(verification.maxOutputBytes)
    expect(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.from(evidence.stdout))).toBe(evidence.stdout)
    expect(new TextDecoder('utf-8', { fatal: true }).decode(Buffer.from(evidence.stderr))).toBe(evidence.stderr)
    expect(evidence.truncated).toBe(true)
  })

  it.each([
    ['typecheck caller arguments', 'typecheck', ['--all']],
    ['NUL argument', 'test:orchestrator', ['packages/dsh-orchestrator/tests/ok.spec.ts\0--runInBand']],
    ['POSIX traversal', 'test:orchestrator', ['packages/dsh-orchestrator/tests/../outside.spec.ts']],
    ['Windows traversal', 'test:orchestrator', ['packages\\dsh-orchestrator\\tests\\..\\outside.spec.ts']],
    ['absolute POSIX path', 'test:orchestrator', ['/tmp/ok.spec.ts']],
    ['absolute Windows path', 'test:orchestrator', ['C:\\temp\\ok.spec.ts']],
    ['non-test path', 'test:orchestrator', ['packages/dsh-orchestrator/src/index.ts']],
  ])('rejects %s before a process is admitted or a verification event exists', async (_label, command, args) => {
    const subprocess = new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 0, signal: null })))
    const finished: unknown[] = []

    await expect(service(subprocess, finished).run(command, args, new AbortController().signal)).rejects.toThrow(/verification/i)
    expect(subprocess.spawns).toEqual([])
    expect(finished).toEqual([])
  })

  it('rejects an unknown command before a process is admitted or a verification event exists', async () => {
    const subprocess = new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 0, signal: null })))
    const finished: unknown[] = []

    await expect(service(subprocess, finished).run('shell', [], new AbortController().signal)).rejects.toThrow(/unknown verification command/i)
    expect(subprocess.spawns).toEqual([])
    expect(finished).toEqual([])
  })
})

describe('targeted_verify tool definition', () => {
  it('uses a generic tool card, admits the budget before spawning, and does not record verification after budget rejection', async () => {
    const session = Session.create(SessionId('verification-tool-session'), undefined, {
      version: 0,
      id: SessionId('verification-tool-session'),
      createdAt: 0,
      cwd: `${workspaceRoot}/packages/dsh-orchestrator`,
      isSeeded: false,
    })
    const subprocess = new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 0, signal: null })))
    const admitPluginTool = vi.fn(() => ({ allowed: false as const, code: 'PLUGIN_TOOL_LIMIT', limit: 0, observed: 1 }))
    const options = {
      verification,
      workspaceRoot,
      subprocess,
      budgetRegistry: { forRootSession: () => ({ admitPluginTool }) },
    }
    const tool = createTargetedVerificationTool(options)

    expect(tool.name).toBe('targeted_verify')
    expect(tool.timeoutMs).toBeGreaterThanOrEqual(verification.timeoutMs + VERIFICATION_CLEANUP_ALLOWANCE_MS)
    expect(tool.presentCall?.({ command: 'typecheck', args: [] })).toMatchObject({
      card: 'generic',
      title: 'Run targeted verification',
      kind: 'execute',
    })

    await expect(tool.execute(
      { command: 'typecheck', args: [] },
      { signal: new AbortController().signal, agent: { session } } as never,
    )).rejects.toThrow(/PLUGIN_TOOL_LIMIT/)
    expect(admitPluginTool).toHaveBeenCalledWith('targeted_verify')
    expect(subprocess.spawns).toEqual([])
    expect(session.snapshotEvents().filter(event => event.type === 'dsh-plugin/verification-finished')).toEqual([])
  })

  it('prevalidates requests before budget admission and uses the supplied repository root', async () => {
    const session = Session.create(SessionId('verification-valid-tool-session'), undefined, {
      version: 0,
      id: SessionId('verification-valid-tool-session'),
      createdAt: 0,
      cwd: `${workspaceRoot}/packages/dsh-orchestrator`,
      isSeeded: false,
    })
    const subprocess = new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 0, signal: null })))
    const admitPluginTool = vi.fn(() => ({ allowed: true as const }))
    const options = {
      verification,
      workspaceRoot,
      subprocess,
      budgetRegistry: { forRootSession: () => ({ admitPluginTool }) },
    }
    const tool = createTargetedVerificationTool(options)
    const aborted = new AbortController()
    const cancellation = new Error('already cancelled')
    aborted.abort(cancellation)

    await expect(tool.execute(
      { command: 'shell', args: [] },
      { signal: new AbortController().signal, agent: { session } } as never,
    )).rejects.toThrow(/unknown verification command/i)
    await expect(tool.execute(
      { command: 'typecheck', args: ['--all'] },
      { signal: new AbortController().signal, agent: { session } } as never,
    )).rejects.toThrow(/accepts no caller arguments/i)
    await expect(tool.execute(
      { command: 'typecheck', args: [] },
      { signal: aborted.signal, agent: { session } } as never,
    )).rejects.toBe(cancellation)

    expect(admitPluginTool).not.toHaveBeenCalled()
    expect(subprocess.spawns).toEqual([])
    expect(session.snapshotEvents().filter(event => event.type === 'dsh-plugin/verification-finished')).toEqual([])

    await expect(tool.execute(
      { command: 'typecheck', args: [] },
      { signal: new AbortController().signal, agent: { session } } as never,
    )).resolves.toMatchObject({ status: 'passed' })

    expect(admitPluginTool).toHaveBeenCalledTimes(1)
    expect(subprocess.spawns).toHaveLength(1)
    expect(subprocess.spawns[0]?.cwd).toBe(workspaceRoot)
    expect(session.snapshotEvents().filter(event => event.type === 'dsh-plugin/verification-finished')).toHaveLength(1)
  })

  it.each(['', '/workspace\0ds-plugins'])('rejects an invalid trusted repository root at tool construction', value => {
    const subprocess = new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 0, signal: null })))
    const admitPluginTool = vi.fn(() => ({ allowed: true as const }))
    const options = {
      verification,
      workspaceRoot: value,
      subprocess,
      budgetRegistry: { forRootSession: () => ({ admitPluginTool }) },
    }

    expect(() => createTargetedVerificationTool(options)).toThrow(/workspaceRoot/i)
  })
})

describe('bundle targeted verification registration', () => {
  it('registers targeted_verify through apply with the deployment-configured repository root', async () => {
    const registrations: unknown[] = []
    const promptSections: unknown[] = []
    const subprocess = new FakeSubprocess(() => handle(Promise.resolve({ exitCode: 0, signal: null })))
    const ctx = {
      sessions: { get: () => undefined },
      subprocess,
      tools: {
        register(tool: unknown) {
          registrations.push(tool)
          return () => undefined
        },
      },
      effect(callback: () => unknown) {
        return callback()
      },
      get(name: string) {
        if (name === 'systemPrompt') {
          return {
            section(section: unknown) {
              promptSections.push(section)
              return () => undefined
            },
          }
        }
        return undefined
      },
      on() {
        return () => undefined
      },
    }
    const config = {
      workspaceRoot,
      mode: 'direct' as const,
      worker: { provider: 'openai-codex', model: 'gpt-5.6-codex', maxTokens: 32_000 },
      budgets: { maxWorkers: 0 as const, maxPluginToolActions: 1, toolTimeoutMs: 60_000 },
      verification,
    }
    const session = Session.create(SessionId('verification-apply-session'), undefined, {
      version: 0,
      id: SessionId('verification-apply-session'),
      createdAt: 0,
      cwd: `${workspaceRoot}/nested-session-directory`,
      isSeeded: false,
    })

    apply(ctx as never, config as never)

    const [registered] = registrations as ReturnType<typeof createTargetedVerificationTool>[]
    expect(registered?.name).toBe('targeted_verify')
    expect(promptSections).toHaveLength(1)
    if (registered === undefined) return

    await expect(registered.execute(
      { command: 'typecheck', args: [] },
      { signal: new AbortController().signal, agent: { session } } as never,
    )).resolves.toMatchObject({ status: 'passed' })
    expect(subprocess.spawns[0]?.cwd).toBe(workspaceRoot)
  })
})
