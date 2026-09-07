# `@ds-plugins/dsh-orchestrator` v0.1

`@ds-plugins/dsh-orchestrator` is an out-of-tree DeepSeek Harness (DSH) bundle for a small, evidence-first coding-agent loop. It composes with the official `@deepseek-ai/dsh-base` bundle and never replaces the DSH agent loop or provider implementation.

## Status and compatibility

- Parent repository: [DS-Plugins](https://github.com/sihan-shen/DS-Plugins)
- DSH dependency line: `0.1.1-rc.2`
- Cordis peer dependency: `4.0.1`
- Shared plugin dependencies: `@ds-plugins/dsh-context` `^0.2.0` and
  `@ds-plugins/dsh-scheduling-contracts` `^0.3.0`
- Availability: the bundle is used by the parent repository's `v0.1` and
  `v0.2c-context` profiles. It is prepared for standalone publication but is
  not yet published to npm; it requires the scheduling-contracts package to be
  published first (or supplied from a local workspace).

## Install and standalone development

After the shared contract packages are published, install it with its DSH host
dependencies:

```sh
pnpm add @ds-plugins/dsh-orchestrator
```

The bundle is loaded by the host through the included
[`cordis.patch.yml`](./cordis.patch.yml). For a standalone checkout:

```sh
pnpm install
pnpm typecheck
pnpm test
pnpm run test:package-entry
pnpm pack --dry-run
```

The standalone test configuration intentionally excludes parent-repository
profile/Loader coverage and integration tests that compose unpublished sibling
plugins. Those remain covered in the parent repository.

## Scope

v0.1 has two mutually exclusive modes:

- `direct`: one root agent; it registers `targeted_verify` and records `dsh-plugin/run-started`.
- `single-worker`: the root agent also receives one foreground `delegate_worker` action. It can start exactly one serial child and receives only a validated `HandoffV1`, never a child transcript.

The repository profile at [`profiles/v0.1`](https://github.com/sihan-shen/DS-Plugins/tree/main/profiles/v0.1) is a source-workspace profile. Its workspace dependency is intentionally not a standalone published-profile installation recipe. To use a packed or published bundle elsewhere, create a normal DSH profile with `@deepseek-ai/dsh-base` plus this bundle, then copy the equivalent `ds-orchestrator` configuration below into that profile's `cordis.patch.yml`.

The profile deliberately provides composition only. Use it beneath a DSH surface such as the official Headless or Web bundle; it does not add its own UI or provider.

## Repository development profile

```sh
nix develop --command pnpm install
nix develop --command pnpm test:profile
```

The checked-in profile selects Direct mode. Change `mode` and the matching `budgets.maxWorkers` together to select Single Worker mode:

Single Worker mode requires the official `subagents` service. Startup waits up to 5 seconds for that service and then fails explicitly if it is unavailable; this is a dependency error, not a Loader hang.

```yaml
- id: ds-orchestrator
  config:
    workspaceRoot: .
    mode: single-worker
    worker:
      provider: openai-codex
      model: gpt-5.6-codex
      maxTokens: 32000
    budgets:
      maxWorkers: 1
      maxPluginToolActions: 24
      toolTimeoutMs: 60000
    verification:
      commands:
        - name: typecheck
          executable: pnpm
          fixedArgs: [typecheck]
          allowedArgs: none
      timeoutMs: 120000
      maxOutputBytes: 65536
```

`openai-codex` is an official DSH `dsh-llm-pi-ai` OAuth route. Sign in through the official DSH authorization surface first. This package does not implement OAuth, read token files, substitute a third-party OAuth plugin, or silently fall back to a paid API.

## Configuration contract

All top-level fields are required, unknown keys are rejected, and values are validated when the plugin loads.

| Field | Rule |
| --- | --- |
| `workspaceRoot` | Non-empty, no NUL. It is deployment-controlled and is the only working directory used by verification; a session `cwd` cannot replace it. |
| `mode` | Exactly `direct` or `single-worker`. |
| `worker.provider`, `worker.model` | Non-empty strings. |
| `worker.reasoningEffort` | Optional non-empty string retained in the WorkerSpec/event. The pinned Agent API does not receive it as an undocumented option. |
| `worker.maxTokens` | Positive integer, maximum **128000**. The checked-in profile uses **32000**. |
| `budgets.maxWorkers` | `0` for Direct, `1` for Single Worker; no other value is valid. |
| `budgets.maxPluginToolActions` | Positive integer, maximum **32**. Counts only `targeted_verify` and `delegate_worker`, not all Harness tools or steps. |
| `budgets.toolTimeoutMs` | Positive integer, maximum **600000 ms**. |
| `verification.commands` | A command list with unique names. An executable is one non-whitespace token and `fixedArgs` is a literal string array. |
| `verification.commands[].allowedArgs` | `none` or `orchestrator-test-paths`. |
| `verification.timeoutMs` | Positive integer, maximum **600000 ms**. |
| `verification.maxOutputBytes` | Positive integer, maximum **1048576 bytes**. |

## Targeted verification

`targeted_verify` runs only a named command from `verification.commands`. It invokes direct argv (`executable`, `fixedArgs`, approved caller args) at `workspaceRoot`; it never accepts a shell string.

- `allowedArgs: none` permits no caller args.
- `allowedArgs: orchestrator-test-paths` permits only POSIX repository-relative files under `packages/dsh-orchestrator/tests/`, with no empty, `.` or `..` segments, backslashes, drive prefixes, absolute paths, NULs, or non-test extensions.
- A timeout owns process cancellation and waits for process-tree quiescence. Captured stdout/stderr are bounded with a UTF-8-safe proportional tail cap and become `VerificationEvidenceV1`.
- Caller cancellation is reported as cancellation, not overwritten as a timeout. Invalid and pre-aborted calls are rejected before consuming the plugin-action budget.

`VerificationEvidenceV1` holds `schemaVersion`, command name/args, exit code, status, bounded stdout/stderr, truncation flag, and duration. A completed Handoff with any non-passing evidence must contain the exact marker `[verification: failed]` in its summary.

## Handoff and canonical events

The parent accepts only JSON `HandoffV1`:

```text
schemaVersion, status, summary, changedFiles, decisions, verification, blockers
```

String fields are capped at **16384 UTF-8 bytes** and every Handoff array at **128 items**. Changed-file paths are slash-normalized, repository-relative, and reject traversal or absolute Windows/POSIX forms. Invalid child output becomes a fixed failed Handoff without copying raw output.

Canonical session evidence consists of versioned `dsh-plugin/` events: `run-started`, `worker-requested`, `worker-finished`, `budget-rejected`, and `verification-finished`. `run-started` projects the actual root `request/header` provider/model route, never the worker deployment fallback; malformed route snapshots produce no record. The other events project bounded worker specs, validated Handoffs, admission counters, and verification evidence. They exclude credentials, authorization headers, tokens, raw model output, and worker transcripts.

## Provider-smoke boundary

`pnpm test:provider` is retained as a safe compatibility command, but provider execution is intentionally unavailable:

```sh
nix develop --command pnpm test:provider
# DISABLED: local OpenAI Codex provider smoke is intentionally unavailable; keyless verification only.
```

The command exits successfully whether or not `DSH_RUN_OPENAI_CODEX_SMOKE` or `DSH_HARNESS_ROOT` is set. It does not locate or execute a Harness checkout, load an OpenAI/Codex provider, inspect Git state, create a temporary profile, access credentials, or make a network request. The profile therefore does not carry the smoke-only `dsh-headless` dependency.

Keyless Loader/replay tests remain the repository's executable coverage for Direct, Single Worker, Handoff, budgets, and targeted verification. They do not constitute provider or real coding-task acceptance.

## v0.1 limitations

- One serial worker only; no parallel writes.
- The action budget counts plugin-owned tools only, not a total Harness tool or step budget.
- No adaptive routing, retry policy, model fallback, or paid-provider fallback.
- No community runtime dependency.
- The Direct and Single Worker modes and keyless Loader/replay coverage can be verified without a provider. This repository intentionally has no real-provider acceptance path.
