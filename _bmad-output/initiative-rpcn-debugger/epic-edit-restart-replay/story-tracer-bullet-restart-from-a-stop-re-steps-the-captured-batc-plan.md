---
title: 'Tracer bullet: Restart from a stop re-steps the captured batch'
type: 'feature'
ticket: '2'
created: '2026-11-02'
status: 'built'
baseline_revision: '82fd73fabeb2406e3b11035b4a66ae1075b8a5df'
route: 'full'
route_source: 'auto'
risk: 'high'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** A developer who spots a wrong mapping at a stop has to relaunch, which re-reads the input and may capture a different batch. `Restart` is NOT_SUPPORTED today (AD-20, CAP-10).

**Approach:** Split the Engine port into capture and step-from-a-batch. The Session keeps the captured batch, and on Restart re-checks only the pipeline file with the launch's options, replaces the current run, and re-steps the same batch from the entry stop.

## Boundaries & Constraints

**Always:**
- The captured batch is never modified: each step works on a deep copy, so every Restart sees the batch as captured.
- Restart re-reads only the pipeline file, with the launch keys as launched. The first Restart of a session gets the console note "Restart re-reads only the pipeline file; changes to launch.json need a new launch." (D4)
- Restart does not lint the input block, re-run the input-class decision or capture again. Edits to `input.processors` are stepped.
- Restart discards the threads, frames, site counters and env clones, keeps the breakpoints and exception filters, resets the delivered-batch number, and stops at the entry stop, as F5 does. Each live thread gets ThreadExited, then the root thread starts again.
- Under `output: real`, the restarted run is `dry-run`, with the console note "Restart runs as output: dry-run, so the batch is not written to the output again." (entry 7 adds the opt-in)
- The restart response comes before every event the new run emits.

**Never:**
- No Restart after the run has ended (entry 5); it is NOT_SUPPORTED. Restart before a batch is captured is also NOT_SUPPORTED.
- No breakpoint re-resolution, no saveCapture or replay, and no change to `LaunchConfig` or the schema.
- No new core.Error code.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Restart at a stop | stopped; mapping edited | entry stop on the same batch; new mapping in effect; delivered as batch 1; input not read again | none |
| Input edited too | input path changed to a missing file | the run re-steps the kept batch | none |
| Broken YAML | stopped; file has a lint or YAML error | the restart request fails with PIPELINE_INVALID at UserPath/Line; the current stop stays, and a later Next or Restart works | core.Error |
| output: real | launched with `real` | the restarted run delivers `recorded` (dry-run), with the note | none |
| Too early | Restart during capture | NOT_SUPPORTED; the capture goes on | core.Error |
| Run ended | Restart after Terminated | NOT_SUPPORTED | core.Error |
| Terminate during Restart | Terminate while the old run stops | the session ends; no new run starts | none |

</frozen-after-approval>

## Code Map

- `debugger/core/port.go`:
  - `Engine.Start` is replaced by `Capture(ctx, opts) (CapturedBatch, error)` and `Step(ctx, opts, batch any, a Arriver) error`. `CapturedBatch{Batch any; Kept, Total int}` holds an opaque batch: core stays stdlib only.
  - `Arriver.Captured` leaves the port; the Session calls its own `Captured`.
  - Add `StartOptions.Restart bool`: Check skips the capture config's lint.
- `debugger/core/session.go`:
  - Keep `captured CapturedBatch` and add a run generation `gen`.
  - `run` takes the batch (nil means capture first). After `Start`'s replacement returns, a run whose gen is stale returns silently (no strict note, failure, ThreadExited or Terminated).
  - New `restart()`, wired in `Do` in place of `notSupported("Restart")`.
- `debugger/core/threads.go`, `stop.go`: reset helpers for threads, frames, `rootSites`, `budgetWarned`, `entryPending` and `pausePending`. `frameSeq` and `stopSeq` keep counting, so no frame or stop id is reused.
- `debugger/core/delivery.go`: `delivered = 0` on restart (fixes the field comment). `Captured` is no longer an Arriver method.
- `debugger/engine/engine.go`, `capture.go`:
  - `Start` splits into `Capture` (`newRun`, `r.capture`) and `Step` (`newRun` on `opts.Prepared`, then `r.step(ctx, b.DeepCopy())`). `run.step` already feeds a batch through `AddBatchProducerFunc`: that is the in-memory source.
  - `Check` skips `configureCapture` when `opts.Restart` is set.
  - `nopArriver.Captured` goes.
- `debugger/dap/server.go`:
  - `InitializeResponse` sets `SupportsRestartRequest`.
  - `RestartRequest` must NOT go through `respondLocked`: `do` waits for the old run, whose teardown emits, and `frontend.Emit` takes `s.mu`, so that would deadlock. Instead, the frontend holds back events while a restart runs, and flushes them after the response (Design Notes).
- `test/dapdrive/dapdrive.go`:
  - Add the command `Restart Command = "restart"`, sent with `{}` arguments.
  - A failed restart is appended to a new `Result.RestartErrors`, and the script is asked again at the same stop.
  - The delivered-batch number check restarts from 1 after a successful restart.
- Tests with fake engines: `core/session_test.go`, `stepping_test.go`, `inputs_test.go`, `dap/session_test.go`, `engine/engine_test.go`, `output_test.go`, `stepper_test.go`. Move them to Capture/Step. In engine tests, use a local `start` helper (Capture, then Step).
- `testdata/tracer/restart.yaml` (+ `restart-data.txt`): an F5 sample with a `file` input and a mapping to edit at a stop.
- Leave alone: `rewrite/` (Check already re-rewrites), `LaunchConfig`, `vscode/`, `CLAUDE.md`.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/port.go` -- the Capture/Step port, `CapturedBatch`, `StartOptions.Restart`; drop `Arriver.Captured` -- the AD-20 split.
- [x] `debugger/engine/engine.go`, `capture.go` -- implement Capture and Step (a deep copy per Step); Check honours Restart -- the in-memory re-step.
- [x] `debugger/core/session.go`, `threads.go`, `stop.go`, `delivery.go` -- keep the batch; the generation-guarded run; `restart()` (Design Notes) -- the Session side.
- [x] `debugger/dap/server.go` -- `supportsRestartRequest`, the held-events restart handler -- DAP.
- [x] `test/dapdrive/dapdrive.go` -- `Restart`, `RestartErrors`, per-run numbering -- for entries 5 and 7.
- [x] Tests (above files) -- port migration; new core tests for each matrix row except VS Code; a dap test that the restart response precedes the new stopped event; dapdrive tests for the verify line.
- [x] `testdata/tracer/restart.yaml` -- the F5 sample.

**Acceptance Criteria:**
- Given a `file` input pipeline stopped at a mapping, when the script rewrites the data file and the mapping, then restarts and continues, then the delivered batch is batch 1, carries the original data and shows the new mapping's output.
- Given a restart with broken YAML, when it fails, then `RestartErrors` names the line, and a following Continue delivers with the old mapping.
- Given `task ci`, when it runs, then it is green and the equivalence corpus is unchanged.

## Implementation Notes

- **Port:** `Engine.Start` is now `Capture` + `Step`, with `CapturedBatch{Batch any; Kept, Total int}`. `Arriver.Captured` is gone: the Session's `capture` and `keepCaptured` (in `delivery.go`) keep the batch and own the waiting timer. `Step` builds a new run on fresh env clones and steps `b.DeepCopy()`.
- **Deviation, wind-down:** a Restart releases the old run before cancelling it. Cancel-and-wait took about 8.7 s, because a stopped Arrive only returns when its stream shuts down. Instead, a per-run `replaced` channel makes the old run's current and later Arrives fail, so it delivers nothing and ends in about 10 ms. A run still alive after `restartWindDown` (2 s, e.g. a slow write under `output: real`) is cancelled.
- **Deviation, extension test:** the `vscode/src/test/adapter.test.ts` initialize assertion now also expects `supportsRestartRequest`, because `task ci` runs it.
- **Also changed to compile:**
  - the fake engines in `debugger/logging_test.go` and `debugger/rewrite/translate_test.go`;
  - the "nothing delivered" note, which now uses the run's own output mode, so a restarted `real` run says dry-run.
- **Sample:** after review, `testdata/tracer/restart.yaml` uses a `generate` input with `captured_at = now()`. A relative `file` path resolved against the adapter's cwd, which the extension never sets. The dapdrive tests write their own file-input pipeline to a temp dir.
- **Tests:**
  - `debugger/core/restart_test.go` has one test per matrix row, plus `TestRestartAsTheRunEnds` for the race found in review.
  - `debugger/dap/session_test.go` checks the advertised capability, that the response comes before the new run's events, and that a failed restart leaves the stop.
  - `engine_test.go` checks that a Restart Check skips the capture lint and that two Steps of one batch both see it as captured.
  - `test/dapdrive/dapdrive_test.go` covers the acceptance criteria, broken YAML at line 8 and lint at line 9.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 4, false 1, maybe-false 0.
- **medium, patch (fixed):** `restart()` re-checked only `ended` after waiting. A run that finished inside the window emitted Terminated, and then a new run started, with `runDone` stuck. It now refuses on `runDone` too, and `TestRestartAsTheRunEnds` reproduces the race (it fails without the fix).
- **medium, patch (fixed):** the sample's relative data path depended on the adapter's cwd, which `vscode/src/extension.ts` doesn't set. The sample now uses `generate`.
- **low, patch (fixed):** the "yaml" case of `TestRestartBrokenPipeline` asserted only "line". It now asserts "line 8".
- **low, patch (fixed):** dapdrive's internal `restarted` flag was on the public `Result`. It is now a local value returned by `answer`.
- **low, rejected:** `Deliver` and `Log` have no generation guard. A batch the old run was already delivering when the Restart came, or its teardown logs, can show just after the restart response. The window is narrow, and the Delivered is truthful (that batch was delivered). Suppressing it needs new guards.
- **low, rejected:** a Restart Check still runs the full rewrite, so an input block that can't be rewritten at all (no `input`, an undefined resource in the input tree, `redpanda_common`) fails the Restart. The error is PIPELINE_INVALID at its line. A field-level lint error in the input is skipped, as the intent says. Skipping the input in the rewrite would change `rewrite/`.
- **false:** "a Terminate during a Restart can't happen over DAP." DAP requests are handled one at a time, so a terminate waits behind the restart, and nothing goes wrong. The Session-level race is covered by `TestTerminateDuringRestart`.

## Design Notes

`restart()`, in order:
1. Refuse when not captured, or when the run is done or the session has ended.
2. Run `engine.Check` with the launch's options, `Restart: true`, and `Output` set to dry-run when the launch said real. On an error, return it, translated as `launch` does; nothing else changes.
3. Bump `gen`, cancel the run and wait for its `done`. If the session ended meanwhile, stop here.
4. Under the lock: ThreadExited for each started, unexited thread (emitted after the unlock). Reset the stepping state and set `delivered = 0`. Swap in the new `lines`, `translate`, `prepared` and `delivery`. Set `entryPending` and the root thread started.
5. Emit the notes (D4 once, output real), then `res.Warnings` as at launch. Start `run` with the kept batch, which uses no waiting timer.

DAP hold (server.go): `restart` sets `s.holding = true` under `s.mu`, calls `s.do` unlocked, and then, under `s.mu`, writes the response, writes the queued events in order, and clears `holding`. While `holding` is set, `Emit` queues instead of writing.

## Verification

**Commands:**
- `go test -race ./debugger/... ./test/dapdrive/...` -- expected: pass.
- `task ci` -- expected: exit 0.

**Manual checks:**
- In VS Code with `testdata/tracer/restart.yaml`: stop at the mapping, edit it, press Restart. The run stops at entry again, and the Debug Console shows the D4 note and the new output.
