---
title: 'replay launch key'
type: 'feature'
ticket: '4'
created: '2026-11-03'
status: 'built'
baseline_revision: '4e8a81a6082a22ef857da4fcc6b8928fcb083034'
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

**Problem:** A capture saved with `saveCapture` can't be debugged again without the broker or the data it came from. Pipelines whose input is rejected, such as `http_server`, can't be debugged at all (AD-5, AD-11, CAP-10).

**Approach:** Add the launch key `replay: <path>`. The launch reads the capture file, and the run steps its batch through entry 2's step-from-a-batch path, with no capture and no input-class check.

## Boundaries & Constraints

**Always:**
- **Path and read:** `replay` is any path, relative to `cwd`. The launch reads it with the engine's `ReadCapture`. A missing file, a directory, a file that doesn't decode, or a newer major `schemaVersion` is LAUNCH_INVALID naming `"replay"`, the path and the reason.
- **Conflicting key:** `replay` with `saveCapture` is LAUNCH_INVALID.
- **No input:** a replay never reads the input. No broker or listener is contacted, and the capture stream is never built. Check lints only what Step runs, as for Restart. The input-class decision, its refusals and its warnings are skipped, so rejected inputs replay. The other launch warnings (processor effects, split state, stdio) still show.
- **What runs:**
  - `maxMessages` applies to the file's batch: it keeps the first n and gives the usual "captured k of n" note against the file's total.
  - A console note at the start says "Replaying <absolute path>: <k> messages; the input is not read.".
  - The waiting note is never sent.
- **After the run:** a replay ends like a captured run (entry 5): the run-finished note, then the session stays. Restart re-steps the replayed batch.
- **output: real:** the replay runs as `dry-run`, with the console note "Replay runs as output: dry-run, so the batch is not written to the output again." (entry 7 adds the opt-in)
- **Schema:** the launch schema is regenerated.

**Never:**
- No `inputOverride` (entry 8) and no `allowRepeatWrites` (entry 7).
- No new error code.
- No change to the capture-file format.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Replay | a file saved by an earlier launch | entry stop on the saved batch; the replay note; delivered output equals the original launch's | none |
| Rejected input | `http_server` pipeline + saved file | steps; its address is never bound | none |
| With saveCapture | both keys | launch fails | LAUNCH_INVALID |
| Missing / bad file | no file, a dir, bad JSON, `schemaVersion` "2.0" | launch fails, naming the reason | LAUNCH_INVALID |
| maxMessages | file of 10, maxMessages 3 | 3 stepped; "captured 3 of 10" | none |
| output: real | `real` + replay | dry-run delivery (`recorded`) and the note | none |
| Restart | after the replayed run ends | re-steps the same batch as batch 1 | none |
| Broker down | Kafka capture saved, container stopped | replay steps and delivers | none |

</frozen-after-approval>

## Code Map

- **`debugger/core/port.go`:**
  - Rename `StartOptions.Restart` to `StepOnly` ("Check lints only what Step runs: Restart and replay"). Update `session.go`, `engine.go` and the tests using it (`restart_test.go:158,161`, `engine_test.go:956,960`).
  - Add `Engine.LoadCapture(path string) (CapturedBatch, error)`.
- **`debugger/engine/capturefile.go`:** `Adapter.LoadCapture` wraps `ReadCapture`. It returns `Kept = len(batch)`, `Total`, and the `service.MessageBatch` as Batch.
- **`debugger/core/protocol.go`:** add `Launch.Replay string`.
- **`debugger/core/session.go`:**
  - **`launch`:** if `Replay` is set, refuse it together with `SaveCapture`, resolve its path against `cwd`, and stat it the way envFile is checked. Run `Check` with `StepOnly` and with `Output` set to dry-run when the launch said real. Skip `inputDecision`, then `engine.LoadCapture` (an error is LAUNCH_INVALID), and keep the result.
  - **`configurationDone`:** when a replay batch is kept, apply maxMessages (truncate the opaque batch through the engine: either `LoadCapture` takes `maxMessages` and returns Kept and Total, or a small engine helper; prefer passing maxMessages to `LoadCapture`). Emit the replay note and the `real` note. Start `run` with that batch, so there is no capture, no autosave and no waiting timer.
  - The run-finished, Restart and gen logic from entries 2 and 5 is reused unchanged. Restart's Check already uses `StepOnly`.
- **`debugger/dap/launch.go`:** a `Replay string` field with `json:"replay"`, `min:"1"`, and a desc (a capture file saved with saveCapture; any path, relative to cwd; the input isn't read and the input-class check is skipped). `server.go` maps it into `core.Launch`. Regenerate `vscode/package.json`.
- **Fake engines:** add a `LoadCapture` stub.
- **Tests:**
  - core: each matrix row except Broker down, with fake engines.
  - dapdrive: the AD-11 round trip. Launch a `generate` pipeline with `saveCapture`, then launch with `replay`. The first stop's batches and `Delivered` (including `Raw` and `Lossless`) are equal.
  - dapdrive: an `http_server` pipeline on a fixed free port. Replay a file saved from a generate pipeline, and `net.Listen` on that port succeeds during the stop.
  - `test/integration/replay_test.go`: capture from the Redpanda container with `saveCapture` (reuse `redpanda_test.go`'s setup), stop the container, replay and deliver.
- **`debugger/rewrite/` (amended in review):** the input walker's warnings (group override, `auto_replay_nacks`, start-at-latest) go to a new `Plan.InputWarnings`, not `Plan.Warnings`. `serve.go`'s loader passes them as `engine.Configs.InputWarnings`. `Check` appends them to `CheckResult.Warnings` only without `StepOnly`, so neither a replay nor a Restart shows them.
- **Leave alone:** `vscode/src`, `CLAUDE.md`, and `testdata/tracer/restart.yaml` (it has the user's uncommitted edit; never stage it).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/port.go`, `protocol.go`; `engine/engine.go`, `capturefile.go` -- `StepOnly`, `LoadCapture` (with maxMessages), `Launch.Replay` -- the port.
- [x] `debugger/core/session.go` -- replay at launch (validation, check, load) and at configurationDone (notes, run with the batch) -- the Session side.
- [x] `debugger/dap/launch.go`, `server.go`; `task gen:schema` -- the launch key.
- [x] Tests -- core matrix rows, the dapdrive round trip and `http_server` replay, `test/integration/replay_test.go`, fake-engine stubs -- coverage.
- [x] `testdata/tracer/replay.yaml` -- an `http_server` F5 sample whose comment says to set `"replay": ".rpcn-debug/<file>"`, with a file saved from `restart.yaml` -- the manual check.

**Acceptance Criteria:**
- Given a saved capture, when it is replayed, then the first stop's batches and the delivered batch are byte-identical to the saving launch's.
- Given `task ci`, when it runs, then it is green, including the integration replay test and `lint:schema`.

## Implementation Notes

- **Port:**
  - `StartOptions.Restart` is renamed `StepOnly`.
  - New `Engine.LoadCapture(path, maxMessages)` returns `Kept` and `Total`, where `Total` includes the file's skipped messages.
- **Session:**
  - **Launch:** `replay` with `saveCapture` is LAUNCH_INVALID. The path resolves against `cwd` and is checked with a `checkFile` helper, which envFile now shares. Check runs with `StepOnly`, and with dry-run output when the launch said real. `inputDecision` is skipped.
  - **Errors:** a load failure is LAUNCH_INVALID `"replay": <err>`, where the engine's error names the file once.
  - **Run:** it emits the replay note and the dry-run note, then keeps the batch through `keepCaptured`, which gives the "captured k of n" note. It never captures, saves or sends the waiting note.
  - **For entry 7:** a replay launched with `real` stores dry-run as `s.output`. Entry 7 needs the launched mode to decide `allowRepeatWrites`.
- **Input-walker warnings (plan change in review):** the input walker's warnings move to `Plan.InputWarnings` and then `Configs.InputWarnings`. `Check` appends them after `conf.Warnings`, and only without `StepOnly`. On a normal launch they now come after the other warnings, with the text unchanged. The goldens render both lists under one header and are unchanged.
- **DAP:** `LaunchConfig.Replay` (`min:"1"`); the schema is regenerated.
- **Sample:** `testdata/tracer/replay.yaml` (`http_server`).
- **Tests:**
  - `core/replay_test.go`: every matrix row except Broker down.
  - `engine`:
    - `TestLoadCaptureMaxMessages`: truncation, a missing file, bad JSON, a newer major version.
    - `TestCheckStepOnlyDropsInputWarnings`.
  - dapdrive:
    - `TestReplayRoundTrip`: the first stop and `Delivered` are identical, including raw bytes and lossless metadata.
    - `TestReplayRejectedInput`: the `http_server` port stays free.
  - `test/integration/replay_test.go` `TestReplayBrokerDown`: replays with the container stopped.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 2, false 0, maybe-false 0.
- **medium, patch (fixed; the plan's Code Map was amended, with the user's choice to patch in place):** a replay, and a Restart, still showed the capture-only input-walker warnings (group override, start-at-latest, `auto_replay_nacks`). They now travel separately as `InputWarnings`, and a `StepOnly` Check drops them. `TestCheckStepOnlyDropsInputWarnings` covers it.
- **low, patch (fixed):** the replay error named the path twice. It is now `"replay": <err>`, and `TestReplayBadFile` asserts the path appears once.
- **low, patch (fixed):** the real `Adapter.LoadCapture` wasn't tested on bad JSON or a newer major version. Both are added to `TestLoadCaptureMaxMessages`.

## Plan Change Log

- **Review pass 1 (quick):** a replay still showed the capture-only input-walker warnings, which the frozen Always says are skipped. The Code Map had put `rewrite/` off limits, so the warnings couldn't be told apart. Amended: a Code Map bullet carries them as `Plan.InputWarnings` and `Configs.InputWarnings`, and a `StepOnly` Check drops them. That also stops Restart repeating them. The user chose to patch in place rather than revert: the rest of the implementation is kept as is.

## Verification

**Commands:**
- `go test -race ./debugger/... ./test/dapdrive/...` -- expected: pass.
- `task ci` -- expected: exit 0.

**Manual checks:**
- In VS Code, save a capture from `restart.yaml`, then F5 `replay.yaml` with `"replay": ".rpcn-debug/<file>"`. It steps the saved batch, and no port is opened.
