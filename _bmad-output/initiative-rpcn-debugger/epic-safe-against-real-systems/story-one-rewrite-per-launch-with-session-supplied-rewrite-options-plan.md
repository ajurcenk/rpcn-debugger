---
title: 'One rewrite per launch, with Session-supplied rewrite options'
type: 'refactor'
ticket: '1'
created: '2026-10-23'
status: 'built'
baseline_revision: 'f31c2e732a865251914e8f122b986c920850ce9d'
route: 'full'
route_source: 'auto'
risk: 'medium'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** This is E3 entry 1, the opening enabler (retro A5, S6), and it covers CAP-2. Two gaps block the rest of E3:
- `Engine.Check` reads and rewrites the pipeline, and the Session keeps Check's LineMap and Translator. `Engine.Start` then reads and rewrites the file a second time. So an edit between launch and configurationDone desyncs the running configs from the Session's maps.
- The rewrite takes no options. E3 needs the Session to pass it the debug-group base `rpcn-debug-<os-user>` (D5), and later entries pass more.

**Approach:**
- `Check` produces the configs once. `core.CheckResult` gains an opaque `Prepared any` that the Session stores and hands back in `StartOptions.Prepared`. `Start` runs exactly those configs and never reads the file.
- The Session fills a new `StartOptions.DebugGroup` with `rpcn-debug-<os-user>`, taken from the process user. It travels through the engine's `Loader` into `rewrite.Rewrite(src, rewrite.Options{DebugGroup})`, and `Plan` records the options it was given.
- Nothing uses the option yet; entry 2 does.

## Boundaries & Constraints

**Always:**
- Layering is unchanged:
  - `core` stays stdlib-only and never inspects `Prepared`.
  - `engine` and `rewrite` don't import each other. The `Loader` (`debugger/serve.go`) is still the only bridge.
- `<os-user>` comes from the process user (`os/user.Current()`, falling back to `$USER`/`$USERNAME`, then `user`). It is sanitized to `[a-z0-9._-]` and lowercased, so `DOMAIN\Jane Doe` becomes `domain-jane-doe`. It never comes from the pipeline's env or `envFile`.
- `Check` keeps parsing and linting without `Build()`. `Start` with a nil `Prepared` (a caller that skipped Check) still loads the file itself, so engine tests and the `Engine` contract keep working.
- Behaviour is otherwise unchanged: every rewrite golden is byte-identical and the equivalence corpus stays green.

**Never:**
- No caching of configs by path inside the adapter; the result is carried explicitly.
- No group override yet, and no new launch key: `DebugGroup` is not user-settable in this entry.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Edit after launch | Launch, then the pipeline file is rewritten (a different processor), then configurationDone | The run steps the launched pipeline: stops and lines match the LineMap from launch | none |
| File deleted after launch | Launch, the file is removed, configurationDone | The run proceeds from the launched configs | none |
| Start without Check | An engine caller calls `Start` with `Prepared` nil | It loads and rewrites the file as before | load errors as today |
| User name | Process user `DOMAIN\Jane Doe`, or lookup fails with no `$USER` | `rpcn-debug-domain-jane-doe`; `rpcn-debug-user` | none |

</frozen-after-approval>

## Code Map

- `debugger/core/port.go`:
  - `CheckResult` (add `Prepared any`, documented as opaque, passed back to Start);
  - `StartOptions` (add `Prepared any` and `DebugGroup string`);
  - the `Engine` doc comments for Check and Start (Start runs `opts.Prepared` when set).
- `debugger/core/session.go`:
  - `launch` (~289–303): Check is called with `StartOptions{Pipeline, Cwd, Lookup}`; add `DebugGroup`, store `res.Prepared` with the other launch state;
  - `configurationDone` (~331): builds the Start options; add `Prepared` and `DebugGroup`.
  - A small helper for the debug-group name (with a unit test), in `session.go` or a new `debuggroup.go`.
- `debugger/engine/engine.go`:
  - `Loader` type (`func(pipeline string) (Configs, core.LineMap, error)`) gains a `LoadOptions{DebugGroup string}` argument;
  - `Check` (~78) returns the loaded `Configs` as `Prepared`;
  - `Start` (~104) uses `opts.Prepared.(Configs)` when set, else loads.
- `debugger/serve.go` `loadPipeline`: passes the options into `rewrite.Rewrite`.
- `debugger/rewrite/rewrite.go`: `Rewrite(src []byte)` becomes `Rewrite(src []byte, opts Options)`, with `Options{DebugGroup string}` stored on `Plan` (an exported read-only field or method).
- Callers of `rewrite.Rewrite` to update: `debugger/logging_test.go`, `debugger/engine/engine_test.go` (its `load` helper, line ~36), `debugger/rewrite/*_test.go`, `test/dapdrive/dapdrive.go` (`userPaths`), and `test/equivalence` if it calls it.
- Fakes implementing `core.Engine` in `core`/`dap` tests compile unchanged, because they return `CheckResult{}` with a nil `Prepared`.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/port.go` -- add `CheckResult.Prepared`, `StartOptions.Prepared`/`DebugGroup`, and update the docs -- the port contract.
- [x] `debugger/core/session.go` (plus a helper file if cleaner) -- compute the debug group, pass it to Check and Start, keep `Prepared` -- the Session owns the options.
- [x] `debugger/engine/engine.go` -- `Loader` with `LoadOptions`; `Check` returns `Prepared`; `Start` runs it -- one rewrite per launch.
- [x] `debugger/serve.go` -- `loadPipeline(path, opts)` passes `rewrite.Options` -- the bridge.
- [x] `debugger/rewrite/rewrite.go` -- `Rewrite(src, opts)`, with the options kept on `Plan` -- the option channel.
- [x] Callers and tests -- update the `Rewrite` and `Loader` calls; add the matrix tests (a `debugger` or `test/dapdrive` test for the edit after launch and the deleted file, an engine test for Start without Check, a unit test for the name sanitizing, and a rewrite test that `Plan` holds the given option).

**Acceptance Criteria:**
- Given a launched session whose pipeline file is then changed, when configurationDone runs, then the stops, lines and delivered output are those of the launched pipeline.
- Given `task ci`, then it exits 0 with every rewrite golden byte-identical.

## Implementation Notes

- The debug-group helper is `debugger/core/debuggroup.go` (`debugGroup()`, with the testable `debugGroupName(name, getenv)`); unexported, since only the Session uses it.
- `rewrite.Plan.Options` is an exported field (documented read-only).
- `engine.Adapter.Start` switches on `opts.Prepared`: `Configs` runs it, nil loads the file, and any other type is an error (a caller bug, never a silent reload).
- `test/dapdrive.Options` gained `AfterLaunch func() error`, run between the launch response and configurationDone; `TestOneRewritePerLaunch` (edited and deleted file) uses it, and fails when Start ignores Prepared.

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 2, false 0, maybe-false 0. 1 deferred, 1 rejected. The reviewer found no bugs and no unmet criteria. A mutation (`Start` ignoring `Prepared`, via `-overlay`) makes `TestOneRewritePerLaunch` fail in both subtests.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | CLAUDE.md's Layout doesn't mention `core/debuggroup.go`, `rewrite.Options`/`Plan.Options` or `dapdrive.Options.AfterLaunch` | low | defer | Fixes to agent-context files are deferred; appended to deferred-work.md for E3's refactor sweep |
| 2 | `TestDebugGroupIgnoresLaunchEnv` can only fail if the Session builds the group from its own lookup | low | reject | True, but a stronger test would need an injected name source, more than a direct correction. `TestDebugGroupName` covers the env fallback |

## Verification

**Commands:**
- `go test -count=1 ./debugger/... ./test/...` -- expected: PASS.
- `task ci` -- expected: exit 0.

Run 2026-10-23 by the orchestrator after review: `task ci` exit 0.
