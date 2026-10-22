---
title: 'Delivery at rpcn_debug_sink (dry-run)'
type: 'feature'
ticket: '8'
created: '2026-10-21'
status: 'built'
baseline_revision: '5231a100f822651614f2265c5a1867ea10665926'
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

**Problem:** `rpcn_debug_sink` already collects the delivered batches (MaxInFlight 1, no batch policy), but nobody sees them. The session ends silently after the last stop, so the user can't tell what the pipeline would have written. A pipeline whose output sets `batching` gives no hint that the batch boundaries shown in the debugger differ from production.

**Approach:**
- The sink reports each batch it receives as `core.Delivered{Snapshot}` through the Session, built from a deep copy exactly as received (after `output.processors`, AD-21).
- The DAP server shows each delivered batch in the Debug Console as an Output event (content, metadata, error per message; decision 2026-10-16).
- It also sends a machine-readable custom DAP event that VS Code ignores and the test driver reads, so 2.9 compares structured data, not console text.
- The rewrite warns at launch when the output tree configures `batching`.

## Boundaries & Constraints

**Always:**
- AD-21:
  - The step config's output is always `rpcn_debug_sink`. It receives batches after `output.processors` (already wrapped as the last top-level stepper, 2.5).
  - In dry-run it records batches exactly as received (MaxInFlight 1), and applies no output-level batching.
  - It emits one `Delivered{Snapshot}` per batch, in the order received.
- The Snapshot is the AD-11 shape built by the existing engine conversion (lossless scalar metadata, `__` keys internal, errors).
  - Its position names the user path `output` and the line of the top-level `output:` key.
  - It never references engine objects (AD-19).
- The Debug Console text per batch:
  - a header `Delivered batch i (n messages) to dry-run output`;
  - per message: its content (text, or base64 when not UTF-8), then metadata as `key = value` lines, internal keys marked, then `error: …` when set.
  - A message's content longer than 4 KiB is cut, with a note saying how many bytes were left out. Batches are numbered from 1 within the session.
- The custom event is `rpcnDelivered`, with body `{batch: i, snapshot: <AD-11 Snapshot JSON>}`.
  - It's a frontend detail of `debugger/dap`. No core protocol member is added; Delivered already exists in AD-3.
  - `test/dapdrive` collects these events into `Result.Delivered`, replacing its derivation from the last stop.
- Batching warning (AD-21): if any output in the output tree (including `broker`/`switch` children and `output_resources`) has a non-null `batching` field, one launch warning says "output batching isn't applied in dry-run: the batches shown are the pipeline's own, and production batch boundaries may differ", naming the path and line.
- A run that delivers nothing (every message deleted) still ends cleanly, with the Debug Console saying "Nothing was delivered".

**Never:**
- No `output: real`/`none` and no forwarding to the user's output (epic-safe-against-real-systems). No webview. No capture files.
- No change to what is delivered: the sink only observes (AD-8).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| One batch | Tracer sample, Continue | One `Delivered batch 1 (1 messages)` Output with its content and metadata, one `rpcnDelivered` event, then terminated | (none) |
| group_by split | `testdata/tracer/parents/group_by.yaml` | One Delivered per output batch, in the engine's order; the batch boundaries match the reference run | (none) |
| Output processors | `nested.yaml` | The delivered content includes `from_output: true` (after `output.processors`) | (none) |
| Message error | A message whose error survives to the output | Its `error:` line appears under that message | (none) |
| Nothing delivered | A mapping `root = deleted()` | No Delivered; Output "Nothing was delivered"; terminated | (none) |
| Batching warning | `output: {drop: {}, batching: {count: 5}}` | One launch warning naming `output.batching` and its line; the delivery is unbatched | (none) |
| No warning | Output without `batching` | No batching warning | (none) |
| Large content | 10 KiB message | Console shows the first 4 KiB plus "… 6144 more bytes"; the event carries the full content | (none) |

</frozen-after-approval>

## Code Map

- `debugger/engine/sink.go`: `sink` collects copies in `got`. `WriteBatch` should also call a delivery port with the snapshot batches (`snapshotBatches` in `engine/snapshot.go` / `stepper.go`). Keep `batches()` for the engine's own log line (`engine.go:104`).
- `debugger/core/port.go` `Arriver`: add `Deliver(ctx, batch Batch)` or similar. The Session builds the Snapshot (position: user path `output`, the `output:` key line from the LineMap or a rewrite-provided position) and emits `Delivered`.
- `debugger/core/protocol.go:248`: `Delivered{Snapshot}` exists.
- `debugger/core/session.go`:
  - emit Delivered in order;
  - count delivered batches;
  - at run end, emit "Nothing was delivered" when there are none;
  - keep the existing order "…, thread exited, terminated".
- `debugger/dap/server.go` around line 421 currently drops Delivered. Render the Output text, and send the custom `rpcnDelivered` event: go-dap has no type for it, so write it as a generic `dap.Event` with a JSON body.
- `debugger/rewrite`: the stdio walk (`stdio.go`, `warnNestedLists`) already visits the output tree and `output_resources`. Add the batching check there, and record the `output:` key line for the Delivered position.
- `test/dapdrive/dapdrive.go`: `Result.Delivered` is derived from the last stop today. Collect it from `rpcnDelivered` events, as `[]Batch` with messages, metadata and errors. Update `dapdrive_test.go` `sameAsReference`.
- Fake `Arriver`s in tests (engine, core, dap, stepper_test) need the new method.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/engine/sink.go` (+ test): report each batch through the delivery port, in order, as deep-copied snapshot batches.
- [x] `debugger/core/port.go`, `session.go` (+ tests): `Deliver`, the Snapshot position, Delivered emission, the batch counter and "Nothing was delivered".
- [x] `debugger/dap/server.go` (+ tests): the console rendering (with the 4 KiB cut) and the `rpcnDelivered` custom event.
- [x] `debugger/rewrite` (+ goldens): the batching warning and the `output:` key position.
- [x] `test/dapdrive/`: Delivered from events, with the parent tests comparing it to `RunReference`.
- [x] `README.md`: one line on delivery in the Debug Console.

**Acceptance Criteria:**
- Given the tracer sample in VS Code, when the user continues to the end, then the Debug Console shows `Delivered batch 1 (1 messages) to dry-run output` with the message content and its metadata, and the session ends with no dialog.
- Given `testdata/tracer/parents/group_by.yaml`, when the user continues to the end, then one Delivered entry per output batch appears.
- Given a pipeline whose output sets `batching`, when the user presses F5, then the Debug Console shows the batching warning at launch.
- Given `task ci`, when it runs, then all tests pass and every dapdrive parent case's events match the reference run.

## Plan Change Log

## Implementation Notes

- Port: `core.Arriver.Deliver(ctx, Batch)`; `core.CheckResult.Delivery` (and `engine.Configs.Delivery`, `rewrite.Plan.Delivery`) carry the `output:` key position. Delivered Snapshots sit at user path `output`, that line, the root thread, FrameID 0 and StepperID -1 (no stepper).
- The sink calls `Deliver` from `WriteBatch` with `snapshotBatches` of the received batch; it still keeps its `got` copies for the engine's log line.
- "Nothing was delivered to dry-run output" is emitted only when the run ends without error (not after NOTHING_CAPTURED or a failed run).
- DAP: the batch number is counted in the server (`atomic.Int64`), since Delivered carries only the Snapshot. The Output event is followed by `rpcnDelivered`, a struct embedding `dap.Event` with a JSON body. Console cut: 4096 bytes of the raw content (text cut backs off to a UTF-8 boundary; base64 is of the cut prefix), then `… N more bytes`. Multi-line content and metadata are indented.
- Batching warning: one per non-null `batching` field found in `output` and `output_resources` (walks broker/switch children; `processors` lists skipped), each naming its path and line. Golden render gained a `# delivery` section; new cases `batching.yaml`, `batching-top.yaml`.
- `testdata/tracer/parents/for_each.yaml` was back to `count: 3`: commit 5231a10 had set `count: 10`, which made `TestParents/for_each` fail at HEAD (the debugger captures one batch of 3; the reference run delivered 4 batches). The file's own comment says one batch of 3.
- Tests: engine (`TestSinkDeliversInOrder`, `TestSinkNothingDelivered`, delivery in the sample test), core (`TestDelivered`, `TestNothingDelivered`), dap (`delivered_test.go`: rendering, 4 KiB cut, event JSON, nothing delivered), dapdrive (tracer, group_by, error, deleted, batching warning, large content), serve tests read the new events.

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 3, false 0, maybe-false 0, plus the pending manual check. Two were patched and one deferred.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | The batching warning fires on any nested key named `batching` (e.g. an HTTP header) | low | patch | `warnBatching` now follows output structure (the output block's own field; the component config's `batching`; `outputs[]`, `output`, `cases[].output`, list-form fallback). `output_resources` entries go through `resourcePath`. New golden `batching-header` (no warning). Every `batching` golden warning is kept |
| 2 | Non-UTF-8 byte metadata prints as unmarked base64 in the Debug Console | low | patch | Bytes metadata without `text` gets ` (base64)`, as content does |
| 3 | The delivered counters (Session and DAP) never reset per run | low | defer | Latent until Restart (AD-20, epic-edit-restart-replay). The comment is corrected and the reset deferred (deferred-work.md) |
| 4 | ACs 1 and 3 (VS Code) aren't verified | — | manual | The user's VS Code check, next |

Orchestrator note: the diff restores `testdata/tracer/parents/for_each.yaml` to `count: 3`. Bookkeeping commit 5231a10 had swept in a test edit (`count: 10`) through `git commit -a`, which broke `TestParents/for_each`. Commits now stage explicit paths.

## Design Notes

Delivery reaches the user twice on purpose: once as readable Debug Console text (the user's decision), and once as a structured custom event that tooling (`test/dapdrive`, a future webview under AD-12) consumes. The event isn't an AD-3 protocol member: it's DAP's rendering of the existing `Delivered` event, the same way `stopped` renders `Paused`.

## Verification

**Commands:**
- `go test -race ./debugger/... ./test/dapdrive/...`: expected PASS.
- `task ci`: expected exit 0.

**Manual checks:**
- In VS Code, continue to the end on the tracer sample and on `group_by.yaml`, and check the Delivered text. F5 on a pipeline with `batching` shows the warning.
