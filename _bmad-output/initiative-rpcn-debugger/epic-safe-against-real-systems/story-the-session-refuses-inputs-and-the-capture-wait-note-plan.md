---
title: 'The Session refuses inputs, and the capture wait note'
type: 'feature'
ticket: '4'
created: '2026-10-23'
status: done
baseline_revision: '54142f42e33aeb4badb4cebaa032218a4474da0b'
route: 'full'
route_source: 'auto'
risk: 'high'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** This is E3 entry 4 (CAP-2, Done when 1; D1 and D5). Entry 3 classes every input, but nothing acts on it yet, so a debug session still captures from an SQS queue or an `http_server` without asking. `stdin` is refused by a rewrite special case with the wrong code. And a capture from a quiet input waits silently.

**Approach:**
- **Findings reach the Session.** `InputClass` and `InputFinding` move to `debugger/core`, and `rewrite` uses them. `Plan.Inputs` travels through `loadPipeline` and `engine.Configs` to `core.CheckResult.Inputs`.
- **The Session decides, once, at launch after Check:**
  - any `rejected` finding refuses the launch with `INPUT_REJECTED` (1011), at that input's path and line, with its effect text and a replay hint ("…; replay a saved capture instead, with the `replay` launch key (coming with epic-edit-restart-replay)");
  - otherwise, any `side-effecting` or `unknown` finding without `allowSideEffects: true` refuses with `SIDE_EFFECTS_NOT_ALLOWED` (1012), at the first such input, with every such input's path and effect listed;
  - with `allowSideEffects: true`, each such input gets one launch warning naming its effect.
  - Both codes join the `core.Error` registry.
- **`stdin`:** `rewrite`'s `rejectStdin` goes away. `stdin` is the table's rejected row, so it gets 1011 with its effect "stdin carries the debugger's DAP stream".
- **`inputClasses`** (a launch key) maps a component name to `replay-safe`, `side-effecting` or `rejected`.
  - The Session passes it as a rewrite option, and the rewrite applies it before composites take their worst child's class, so a `broker` follows its overridden children.
  - A finding records when the override loosened its class; the Session warns once per such input ("inputClasses marks <component> at <path> replay-safe; the debugger can't prove it").
  - A name that isn't an input component is `LAUNCH_INVALID`, and so is a value outside the set.
- **Both keys** (`allowSideEffects` bool, default false; `inputClasses`) join `dap.LaunchConfig`, the `Launch` command and the generated schema.
- **Wait note:** if the run has started and `Captured` hasn't been called 5 s later, the Session emits one console note: "Waiting for the input: nothing captured yet after 5 s." The delay is a Session field that tests can shorten.

## Boundaries & Constraints

**Always:**
- The Session alone refuses (D1). The rewrite only classifies, with the overrides it was given.
- `core` stays stdlib-only, and `dap` imports only `core`.
- Every existing test and corpus pipeline still launches. They use replay-safe inputs (`generate`, `file`, the tracer's `redpanda` with an overridden group), so a change in what launches is a regression to look at, not to paper over.
- The refusal names the user's file line and path, as launch errors do (2.12).
- Existing goldens stay byte-identical, except where `stdin` stops being a rewrite error.

**Never:**
- No change to the classes themselves (entry 3). No group override for other fields (entry 5).
- No refusal inside the rewrite.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Safe | `generate` | Launches; no warning | none |
| Rejected | `stdin`, or `http_server` | Launch fails | 1011 at the input's line, with the effect and the replay hint |
| Side-effecting | `aws_sqs`, no `allowSideEffects` | Launch fails | 1012 at the input, listing its effect |
| Unknown | a component with no table row | As side-effecting | 1012 |
| Opt-in | `aws_sqs` with `allowSideEffects: true` | Launches; one warning naming the effect | none |
| Rejected beats opt-in | `http_server` with `allowSideEffects: true` | Launch fails | 1011 |
| Loosen | `aws_sqs` with `inputClasses: {aws_sqs: replay-safe}` | Launches; one warning that the class was loosened | none |
| Tighten | `generate` with `inputClasses: {generate: rejected}` | Launch fails | 1011 |
| Composite follows | `broker: [generate, aws_sqs]` with `{aws_sqs: replay-safe}` | Launches; the broker is replay-safe | none |
| Bad override | `{no_such: replay-safe}` or `{aws_sqs: maybe}` | Launch fails | `LAUNCH_INVALID` naming the key |
| Silent input | a capture that delivers nothing within the (shortened) delay | One wait note, then the run continues as before | none |

</frozen-after-approval>

## Code Map

- `debugger/rewrite/inputs.go`: `InputClass`, `InputFinding` (exported, ~37) and the class ranking. Move the types to core; keep the ranking where `rewrite` needs it. `inputwalk.go`: the walker, `resourceClasses`, where overrides apply before the worst-class rollup.
- `debugger/rewrite/rewrite.go`: `Options` (add `InputClasses map[string]InputClass` or similar) and `Rewrite`; the `rejectStdin` calls (~108, ~112). `stdio.go` `rejectStdin`: delete it (and `find`, if nothing else uses it).
- `debugger/serve.go` `loadPipeline`; `debugger/engine/engine.go` `Configs`, `LoadOptions` (from 3.1), `Check` (fills `CheckResult.Inputs`).
- `debugger/core/port.go`: `CheckResult`, `StartOptions` (from 3.1: `DebugGroup`, `Prepared`; add the overrides). `core/errors.go`: the registry; add 1011 and 1012 with docs. `core/protocol.go` `Launch`: add `AllowSideEffects`, `InputClasses`.
- `debugger/core/session.go` `launch` (~280–310): after `engine.Check`, decide and emit warnings (Emit outside the lock, as the existing warnings loop does). `run`/`configurationDone`, plus the `Captured` arriver method, for the wait timer.
- `debugger/dap/launch.go`: `LaunchConfig` and its tag reader (`launchField`, `Schema()`). It has no enum support for map values yet; add an `enum` tag that validates values and emits `additionalProperties.enum`. Map `LaunchConfig` to `core.Launch` wherever the existing keys are mapped.
- `cmd/gen-launch-schema` and `vscode/package.json`: regenerate with `task gen:schema`; `task lint:schema` checks drift.
- Tests:
  - DAP-level: `test/dapdrive` (`Run`, `Options.Launch`) or `debugger/serve_test.go`. That file has the existing stdin test, which now expects 1011.
  - Session: unit tests with a fake engine that returns findings, in `core`.
  - Rewrite: override application, including the composite rollup.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/` (port.go, errors.go, protocol.go, a new inputs file if cleaner) -- the input class and finding types, 1011 and 1012, the `Launch` fields -- the contract.
- [x] `debugger/rewrite/` -- use the core types, apply `inputClasses` before the rollup and record loosening, validate names, delete `rejectStdin` -- classification with overrides.
- [x] `debugger/engine/engine.go`, `debugger/serve.go` -- carry the findings and overrides -- plumbing.
- [x] `debugger/core/session.go` -- the refusal, the opt-in and loosening warnings, the wait note -- the decision.
- [x] `debugger/dap/launch.go` plus the schema regeneration -- `allowSideEffects`, `inputClasses` with value enums -- the launch contract.
- [x] Tests for every matrix row, with the existing stdin test updated.

**Acceptance Criteria:**
- Given each matrix scenario, when launched through `debugger.Serve`, then the launch fails or succeeds with exactly the code, location and warnings in the matrix.
- Given `task ci`, then it exits 0. The schema check passes, and every existing test and corpus case still launches.

## Implementation Notes

- `core/inputs.go` holds `InputClass` (`ClassReplaySafe`, …), `OverrideClasses`, `InputFinding` (plus `Inherited`, `Override`, `Loosened`) and the Session's decision (`inputDecision`). `rewrite` keeps its names as aliases (`InputClass = core.InputClass`, `ReplaySafe = core.ClassReplaySafe`, …) and its `rank` as a plain function, so the table and walker code is unchanged.
- `Inherited` marks a finding whose class is a child's or a named resource's. The refusal and the opt-in warnings skip those, so `broker: [generate, aws_sqs]` is refused at the `aws_sqs` child, and gets one warning, not two.
- An override replaces the component's own class after the kept-group check and before the rollup. Overriding to replay-safe clears the effect. A tighter or different class with no table effect gets "inputClasses marks <component> <class>", and the same class keeps the table's effect.
- Name validation is in `engine.Adapter.loadOptions` (`env.GetInputConfig`), since only the engine has the env; value validation is in the Session (`checkInputClasses`) and in `dap` (the new `enum` tag).
- `dap.LaunchConfig` gained `bool` support (`allowSideEffects`, `default:"false"`) and the `enum` tag for string-map values (schema `additionalProperties.enum`).
- The wait note is a `time.AfterFunc` started after `ThreadStarted`. It is stopped when `Start` returns, and `run` waits for a note in flight, so the note never follows `Terminated`.
- An unreferenced `input_resources` entry gets no finding (the walker follows only `resource:` references). Benthos adds input resources lazily (`LazyAdd`), so an unreferenced `stdin` resource is never built and never reads the DAP stream. `rejectStdin` used to refuse it anyway.
- `debugger/serve_test.go` now imports benthos `components/io` and connect `components/aws` (through `serve_inputs_test.go`), so `stdin`, `http_server` and `aws_sqs` lint there.

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 3, low 4, false 0, maybe-false 0. 6 patched by the implementer, 1 patched by the orchestrator. The implementer had already flagged #1 before the review.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `inputClasses` can loosen `stdin` (to replay-safe, or to side-effecting with `allowSideEffects`), so the capture reads the DAP stream (AD-15). The user had already approved that broken rows stay out of configuration | medium | patch | `core.FixedInputs` (stdin, dynamic, redpanda_migrator): the Session refuses any other class with `LAUNCH_INVALID`, and the rewrite ignores such overrides as a second guard. Session and Serve tests |
| 2 | With `rejectStdin` gone, a `stdin` nested under a component the walker doesn't descend into gets no finding, so `allowSideEffects` lets it through | medium | patch | The walker scans the subtree of any input it doesn't descend into for `stdin` keys and adds a rejected finding at each. New golden `inputs-stdin-unknown`; a Serve row gives 1011 at `input.rpcn_test_unknown.child.stdin` |
| 3 | An override that changes the class replaces the table's effect text, and a test passed only by matching the component name | medium | patch | The table text is kept, with the override note in brackets; the test asserts "waits for clients (inputClasses marks http_server side-effecting)" |
| 4 | The Silent input matrix row isn't driven through `debugger.Serve` | low | patch | An unexported `withCaptureWait` Serve option. To reach the Session it adds an exported, test-only `core.Session.SetCaptureWait`; `debugger`'s public API is unchanged. `TestServeCaptureWaitNote` sees exactly one note on the DAP stream |
| 5 | The `Options.InputClasses` doc says the Session validates names | low | patch | Corrected: the Session checks values, the engine names |
| 6 | The 1013 reservation comment sits between 1011 and 1012 | low | patch | Moved after 1012 |
| 7 | The spine's Launch keys row doesn't list `allowSideEffects`/`inputClasses` as built | low | patch (orchestrator) | Spine row updated, AD-5 gains the per-session overrides and the three fixed rows; memlog entry; lint clean |

## Verification

**Commands:**
- `go test -count=1 ./debugger/... ./test/...` -- expected: PASS.
- `task ci` -- expected: exit 0.

Run 2026-10-23 by the orchestrator after the review patches: `task ci` exit 0.
