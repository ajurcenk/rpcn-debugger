---
title: 'The session stays alive after the run ends'
type: 'feature'
ticket: '5'
created: '2026-11-03'
status: 'done'
baseline_revision: 'a20dbb0fc6f3557883c8571c32aac445de197247'
route: 'full'
route_source: 'auto'
risk: 'medium'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** When a run ends, the Session sends Terminated and VS Code closes the session. So the developer can't Restart after seeing the delivered batch, which is the moment they most want to fix the YAML (D5, AD-20).

**Approach:** Once a batch is captured, the end of a run no longer terminates. The Session emits the run-finished note, exits the root thread and waits for Restart or Stop. Clients detect the end of a run from the root thread's ThreadExited, which is no new protocol member.

## Boundaries & Constraints

**Always:**
- The end of a run after a capture covers all batches delivered, STRICT_REJECTED, OUTPUT_FAILED and a run failure. It emits the run's own end notes as today, then the console note "Run finished: Restart re-steps the same batch with the edited pipeline; Stop ends the session.", then ThreadExited for the root thread, and no Terminated.
- A launch that captures nothing, or fails or ends before the capture (NOTHING_CAPTURED, a capture error, Terminate), still ends with Terminated, as today.
- Restart after the run ends re-steps the kept batch exactly as Restart from a stop does (entry 2): batch 1 again, the entry stop, the same notes.
- A DAP `terminate` request gets its response and then a `terminated` event. A `disconnect` request ends the session as today.
- Log records after the run ends stay muted until a Restart starts a new run.
- dapdrive ends a run on the root thread's ThreadExited when no restart is in flight, or on `terminated`. An optional hook may answer the end of a run with Restart. The equivalence harness keeps working through dapdrive, unchanged.

**Never:**
- No new protocol command or event (AD-3 stays closed; no spine change), and no new custom DAP event.
- No saveCapture, replay or SetMessage work.
- No change to `LaunchConfig`, the schema or `vscode/src` beyond its test.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Delivered | the run delivers its batch | Delivered, the note, ThreadExited 1; no Terminated | none |
| Restart after end | after the note | the entry stop on the same batch, delivered again as batch 1 | none |
| Nothing captured | the input closes empty | the NOTHING_CAPTURED note, then Terminated | none |
| Run failure after capture | the step stream fails | the failure on stderr, the note, ThreadExited 1; no Terminated | none |
| STRICT_REJECTED / OUTPUT_FAILED | the outcome note | the outcome note, the run-finished note, no Terminated | none |
| Stop after end | DAP terminate | the response, then `terminated` | none |
| Step after end | Next or Continue with no live thread | refused, as for any unknown or unstopped thread today | core.Error |

</frozen-after-approval>

## Code Map

- `debugger/core/session.go`:
  - `run()` tail (around lines 455–495): split on whether the batch was captured (`kept != nil` after the capture). Uncaptured runs keep today's tail and end with Terminated. Captured runs emit the run-finished note, then the root ThreadExited, set `runDone`, and emit no Terminated.
  - `restart()`: drop the `runDone` refusals (before the Check and after the wait). A run that ended during the wait just proceeds. Reset `runDone = false` along with the stepping state.
  - The `runDone` comment becomes "the run ended: Log sends nothing more until a Restart".
- `debugger/core/delivery.go`: no change expected.
- `debugger/dap/server.go`: `TerminateRequest` (line 250) sends the response, then a `TerminatedEvent`, via `s.send`. Disconnect is unchanged.
- `test/dapdrive/dapdrive.go`, `run()`:
  - Handle `thread` events. A reason `exited` with threadId 1, while no restart is in flight, ends the run: disconnect and return.
  - A successful restart sets in-flight until the next `thread` started with id 1.
  - Add `Options.AfterRun func(Result) Command`. If it returns `Restart`, send it, record a failure in `RestartErrors` and end, and on success reset the delivered numbering and paths as at a stop. `""` or nil ends.
  - `terminated` still ends the run.
- Tests that wait for `terminated` at the end of a run move to the root ThreadExited (and the note where useful), keeping their assertions:
  - core: `session_test.go`, `stepping_test.go`, `inputs_test.go`, `restart_test.go` (`TestRestartAsTheRunEnds` now expects the Restart to succeed);
  - dap: `stepping_test.go`, `delivered_test.go`;
  - `debugger/serve_test.go` and `serve_output_test.go`;
  - `vscode/src/test/adapter.test.ts` (F5 path: wait for the `thread` exited event, then disconnect).
  - The NOTHING_CAPTURED and pre-capture-failure tests still expect `terminated`.
- Leave alone: `engine/`, `rewrite/`, `test/equivalence/` (dapdrive covers it), `CLAUDE.md`.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/session.go` -- the captured or uncaptured run tail, the note, Restart after the end, the `runDone` reset -- D5.
- [x] `debugger/dap/server.go` -- a terminated event after the terminate response -- Stop ends the session.
- [x] `test/dapdrive/dapdrive.go` -- end on the root ThreadExited, `AfterRun`, restart in flight -- for every later entry's tests.
- [x] Tests listed in the Code Map -- move them to the new end of a run; a core test per matrix row; a dapdrive test for the verify line; a dap test for terminate -- coverage.

**Acceptance Criteria:**
- Given a dapdrive run to the end, when `AfterRun` answers Restart once, then the output holds the note, no `terminated` event arrives before the disconnect, and `Delivered` holds the batch twice, each numbered 1.
- Given a NOTHING_CAPTURED launch, when it runs, then it ends with `terminated`.
- Given `task ci`, when it runs, then it is green, including `task test:equivalence`.

## Implementation Notes

- **`core/session.go`:**
  - A run that captured its batch ends with its own end notes, then `RunFinishedNote` (exported for clients and tests), then the root ThreadExited, and sends no Terminated.
  - An uncaptured run ends with the root ThreadExited, then Terminated.
  - `restart()` no longer refuses once the run has ended, and resets `runDone`.
- **`dap/server.go`:** a terminate request writes its response, then a terminated event, unless one already went out (tracked under `s.mu`).
- **dapdrive:**
  - A root exit ends the run only when this run's output held `RunFinishedNote` and no restart is in flight. Otherwise it reads on until `terminated`.
  - New `Options.AfterRun` and `Result.Terminated`.
- **Tests:**
  - Every end-of-run test that waited for `terminated` now waits for the note and the root exit.
  - New core tests: `TestRunEndsAfterCapture` (delivered, run failure, strict and output failure; Next and Continue refused; Restart re-steps) and `TestLogAfterRunEnds`.
  - New dap tests: `TestTerminate` and `TestTerminateAfterNothingCaptured`.
  - New dapdrive tests: `TestAfterRunRestart` and `TestNothingCapturedEnds`.
  - The extension's F5 tests wait for the thread exit and then disconnect.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 3, false 0, maybe-false 0.
- **medium, patch (fixed):** dapdrive ended the run, or asked `AfterRun`, on the root exit of an uncaptured launch, before its `terminated`. It now ends only after `RunFinishedNote`, and records `Result.Terminated`.
- **medium, patch (fixed):** acceptance criteria 1 and 2 were not asserted through dapdrive. `TestNothingCapturedEnds` now asserts `Terminated`, no `AfterRun` call and no restart error, and `TestAfterRunRestart` asserts `!Terminated`.
- **low, patch (fixed):** a Stop after an uncaptured run, which had already terminated, got two terminated events. The server now sends at most one, and `TestTerminateAfterNothingCaptured` covers it.
- **low, patch (fixed):** the F5 extension test's title still said "terminates".
- **low, patch (fixed, merged with the dapdrive finding):** the second run's missing `terminated` couldn't be asserted, because `Result` had no `Terminated` field.

## Design Notes

Detecting the end of a run from the root ThreadExited keeps AD-3 closed: the run's own end notes and the run-finished note come before it, and a Restart's ThreadExited follows its response, so a client that knows it asked for a restart can tell the two apart. VS Code needs nothing: with no threads it shows the session as running, with Restart and Stop on the toolbar.

## Verification

**Commands:**
- `go test -race ./debugger/... ./test/dapdrive/... ./test/equivalence/...` -- expected: pass.
- `task ci` -- expected: exit 0.

**Manual checks:**
- In VS Code with `testdata/tracer/restart.yaml`: Continue to the end. The note shows and the session stays. Edit the mapping, press Restart, and the run stops at entry with the same `captured_at`. Stop ends the session.
