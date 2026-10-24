---
title: 'output: real and none'
type: 'feature'
ticket: '7'
created: '2026-10-23'
status: 'built'
baseline_revision: '1016836dcde2b1d3fcf1955d0623a000df9dbcb5'
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

**Problem:** This is E3 entry 7 (CAP-11, Done when 3; D2). A session always ends in the dry-run sink, so a user who wants the debugged batch written to the real output, or wants nothing recorded at all, has no option.

**Approach:**
- **`output` launch key:** `dry-run` (the default), `real` or `none`. It goes through `dap.LaunchConfig`, `core.Launch`/`StartOptions`, `engine.LoadOptions` and `rewrite.Options`, the pattern from 3.4/3.5. The `enum` tag is extended from string maps to plain string keys.
- **The rewrite emits the step stream's output config** (`Plan.Output`) as `rpcn_debug_sink` YAML, and the engine adds it with `AddOutputYAML` as given. The engine never assembles or parses an output string.
  - Under `real`, the sink config carries `mode: real` and `output:`, which is the user's `output` block **without its top-level `processors`** (they already ran in the output stepper). Every `stdout`/`stderr` output in it is replaced by `drop: {}`.
  - Under `dry-run` or `none`, it carries only the mode.
- **`rpcn_debug_sink`:** its spec gains a `mode` enum and an optional `output` field (`NewOutputField`).
  - Under `real` it builds the user's output as an `OwnedOutput`, primes it on Connect, and in each `WriteBatch` writes the batch to it synchronously and then calls `Deliver`. A failed write returns the error, so the run fails as a delivery failure today; outcomes and `OUTPUT_FAILED` are entry 8. It closes the output on Close.
  - Under `none` it writes nothing.
  - `Deliver` is called in every mode, and `MaxInFlight` stays 1.
- **Warnings follow the mode:**
  - `real` gets one launch warning naming the output component and line ("output: real writes each delivered batch to … at output (line N)").
  - The `stdout`/`stderr` warning says "replaced by drop under output: real" in `real`, and keeps its dry-run text otherwise.
  - The composite-output-processor ("not run in dry-run") warning applies only to `dry-run` and `none`, because under `real` the user's output runs its per-output processors as in production.
  - Under `real`, output-level `batching` (anywhere in the emitted output tree) is removed, with one launch warning per field that the captured batch is written as one batch. A count with no period would otherwise never flush the single captured batch, and the session would hang. This matches `dry-run`, which applies no output batching either (user decision 2026-10-23, from the review).
- **Lint errors** in the emitted output config map to the user's file lines, like the other config parts (a new position part).

## Boundaries & Constraints

**Always:**
- The default (`dry-run`) behaves exactly as today. Every existing golden, test and corpus case is unchanged, and AD-8 still compares what the sink received.
- `output_resources` are carried as today, so a real output that references a resource output works.
- The capture side is untouched: never-ack still holds whatever the output mode.

**Never:**
- No Delivered outcome field, `OUTPUT_FAILED` code, Session batch numbering or mode-aware console texts; those are entry 8.
- No Restart/replay guard (`allowRepeatWrites`); that's epic-edit-restart-replay.
- No change to the input side.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Default | no `output` key | As today: delivered batches shown, nothing written | none |
| Real | `output: real`, a `redpanda` output to topic `out`, `output.processors` adding a counter field | `out` receives exactly the shown batches, once, with the counter at 1 (processors applied once); one `real` warning | none |
| None | `output: none`, the same pipeline | Batches shown in the Debug Console; `out` receives nothing | none |
| stdout under real | `output: real` with `broker.outputs: [redpanda, stdout]` | `stdout` replaced by `drop`, with the real-mode warning; Redpanda gets the batch | none |
| Composite under real | `output: real` with a `switch` output with per-case processors | No "not run in dry-run" warning; the case processors run in the real output | none |
| Write fails | `output: real` with an output that refuses the write | The run fails as a delivery failure | as today |
| Unreachable | `output: real` to an unreachable broker | The output retries its connection as in production; Pause and Stop still work. Entry 8 bounds it with a write timeout and `OUTPUT_FAILED` (user decision 2026-10-23) | none in this entry |
| Bad value | `output: maybe` | Launch fails | `LAUNCH_INVALID` naming `output` |

</frozen-after-approval>

## Code Map

- `debugger/engine/sink.go`: `registerSink` (spec with `Summary` only, `MaxInFlight 1`), and `sink.WriteBatch`, which records, then calls `Deliver`. Add `mode`/`output` fields; the sink holds an optional `*service.OwnedOutput` (`conf.FieldOutput("output")`, `Prime`, its batch write, `Close`).
- `debugger/engine/engine.go` `configureStep`: today `b.AddOutputYAML(SinkName + ": {}")`; use `Configs.Output` from the rewrite. `Configs`/`LoadOptions`; `configError` for mapping lint errors by part.
- `debugger/rewrite/rewrite.go`:
  - `Plan` gains `Output`; `Options` gains the mode;
  - the output section (~176–200): `plan.Delivery`, `warnStdout`, the "is not run in dry-run" `warnNestedLists`, `warnBatching`;
  - `encodeMapped` and `plan.positions` with the `core.Part*` keys (`core/translate.go`): add an output part.
- `debugger/rewrite/stdio.go`: `warnStdout` (text and walk), `warnBatching`/`outputConfig`; `walkKeys` for replacing `stdout`/`stderr` in the emitted copy. Never edit the parsed tree in place if the step config also reads it; copy as `overrideGroups` does.
- `debugger/core` (protocol `Launch`, port `StartOptions`), `session.go`, `debugger/serve.go`, `debugger/dap/launch.go` (`enum` tag: string maps only today, extend to strings), `server.go`, then `task gen:schema`.
- Tests:
  - rewrite goldens or table tests for the emitted sink YAML per mode, the processors strip, the stdout replacement, and the mode-dependent warnings;
  - engine tests with a fake output for write order (write, then `Deliver`) and failure;
  - a `debugger.Serve` test for the bad value and the warnings;
  - a new integration test in `test/integration/` for Real and None against Redpanda. Reuse `startRedpanda`, `dapdrive.Run` and kadm/kgo as `neverack_test.go` does.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/` -- `Plan.Output` per mode, the processors strip, `stdout`/`stderr` → `drop`, mode-aware warnings, the output position part -- D2.
- [x] `debugger/engine/` -- the sink's `mode`/`output`, the write-then-`Deliver` order, `configureStep` using `Plan.Output`, lint mapping -- the forward.
- [x] `debugger/core/`, `session.go`, `serve.go`, `dap/launch.go` plus the schema -- the `output` key -- the launch contract.
- [x] Tests for every matrix row, with Real and None as integration tests.

**Acceptance Criteria:**
- Given `output: real` to a Redpanda topic, when a session runs to the end, then the topic holds exactly the batches the Debug Console showed, once, with `output.processors` applied once.
- Given `task ci`, then it exits 0, with every existing golden, test and corpus case unchanged.

## Implementation Notes

- `core.OutputMode` (`core/output.go`: `dry-run`, `real`, `none`, `""` read as dry-run) travels `dap.LaunchConfig.Output` → `core.Launch.Output` (the Session also rejects an unknown value, `LAUNCH_INVALID` naming `output`) → `StartOptions.Output` → `engine.LoadOptions.Output` → `rewrite.Options.Output`. The `enum` tag now also applies to plain strings (decode check and schema `enum`).
- `rewrite/output.go`: `sinkConfig` builds `Plan.Output` (encoded with a position map under the new `core.PartOutput`; the synthesized sink lines map to the `output:` key); `dropStdio` deep-copies (via `resolve`) and swaps `stdout`/`stderr` for `drop: {}`; `warnReal` gives the one real warning. Under real, the engine path `root.output.rpcn_debug_sink.output` translates to `output`, and `output_resources` also get the stdio replacement in `Plan.Resources` (so `output: {resource: x}` with a stdout resource can't write to the DAP stream); other modes carry resources as before.
- `engine/sink.go`: spec with `mode` (enum, default dry-run) and optional `output` (`NewOutputField`). Under real the constructor builds the `OwnedOutput`; `Connect` primes it once; `WriteBatch` records, snapshots, writes (a copy) synchronously, then `Deliver`s; a failed write returns `write to the output: …` (the run fails as "deliver the batch: …") and does not `Deliver`. `Close` closes it (priming first if needed, since closing an unprimed OwnedOutput waits for ctx). `configureStep` adds `Configs.Output` as given (falling back to `rpcn_debug_sink: {}` when empty) and maps its lint errors through `configError(core.PartOutput, "output", …)`.
- Write-fails row: an unreachable broker does not fail the write: the output retries its connection (as production does) until the session ends, so the test uses a deterministic refusal instead (a fake output in `engine/output_test.go`, a `file` output whose path is a directory in `serve_output_test.go`). Bounding the write is left to entry 8 (`OUTPUT_FAILED`).
- The console text still says "Delivered batch i … to dry-run output" in every mode (mode-aware texts are entry 8).

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 4, low 3, false 0, maybe-false 0, plus 1 frozen-intent deviation decided by the user. All patched. One more hang was found and fixed while testing.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | Under `real`, output `batching` with a count and no period never flushes the single captured batch, so the session hangs (reproduced) | medium | user decided, then patch | User 2026-10-23: under `real` every output-level `batching` (including composite children and `output_resources`) is removed with one warning each; the frozen Approach was amended. The reproduced pipeline now completes and writes once |
| 2 | The "Write fails" row (unreachable broker fails the run) isn't met: the output retries like production | intent | user decided | User 2026-10-23: the row was split into a refused write (fails, tested) and Unreachable (retries; Pause/Stop work). Entry 8 gains a write timeout ending in `OUTPUT_FAILED` (tickets.toml updated) |
| 3 | A `redpanda_common` output, or `redpanda` without `seed_brokers`, under `real` needs the shared client a debug session never has, so it retries forever | medium | patch | `PIPELINE_INVALID` at the output's line under `real` (`noSharedClientOut`); dry-run unaffected; tests |
| 4 | `dropStdio` replaces only `stdout`/`stderr` components, so `file: {path: /dev/stdout}` writes into the DAP stream (AD-15) | medium | patch | `file` with `/dev/stdout`, `/dev/stderr`, `/dev/fd/1`, `/dev/fd/2`, `/proc/self/fd/1` or `/proc/self/fd/2` is replaced by `drop` with a warning; tests |
| 5 | A failed `Prime` is cached, so the output never connects; `Close` doesn't close it | medium | patch | Prime is retried on each Connect until it succeeds; Close closes a built output; `sink_test.go` |
| 6 | `warnReal` names a component that was replaced by drop, and gives "to the  output" for an empty one | low | patch | Says "has nothing to write to" when nothing is written; otherwise names the copy's component |
| 7 | Matrix tests missing: per-case processors running under `real`, and exactly one `real` warning | low | patch | A Serve test with a 2-case switch whose case mapping is written to a file; the integration test counts the warnings |
| 8 | (implementer, while testing) An output that fails to init under `real` hung the session: `step` waited only on the producer | medium | patch | `step` also watches the stream's Run and fails the run; a Serve test. The producer goroutine leaks in that case and is deferred (deferred-work.md) |

## Verification

**Commands:**
- `go test -count=1 ./debugger/... ./test/dapdrive/ ./test/equivalence/` -- expected: PASS, with no existing golden changed.
- `task test:integration` -- expected: PASS, with the new Real/None test.
- `task ci` -- expected: exit 0.

Run 2026-10-23 by the orchestrator after the review patches: `task ci` exit 0 (integration included), no golden changed, no container left behind.
