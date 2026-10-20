---
title: 'Stepper modes, nested parents and ctx propagation'
type: 'feature'
ticket: '7'
created: '2026-10-20'
status: done
baseline_revision: 'a01020c6a1a287c93bb0ddfe6737b75a278b2ea2'
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

**Problem:** `rpcn_debug_stepper` runs every child list like a plain ExecuteAll. It ignores the `mode`, `strict` and `parent` fields that 2.5 writes. So:
- a `try` remainder runs on messages that already failed;
- a strict pipeline's later processors run on failed batches;
- the debugger's output can differ from the real run (against AD-7 and AD-8).

Frames carry no repeat label, so the five `for_each` entries look identical. Nothing detects a lost ctx. And no event tells the user about retry attempts, messages `try` skipped, or a pause inside a retry backoff that changes timing.

**Approach:**
- The stepper acts on its fields:
  - `all` mirrors `ExecuteAll`, including strict skip when `strict: true`;
  - `try` mirrors `ExecuteTryAll`, with multi-batch in and out.

  It deep-copies the batch at every Arrive.
- It computes AD-10 repeat labels (`[msg i/n]`, `[attempt i/max]`, `[loop i]`, `[group i]`) from a per-entry counter in its parent frame, and reports them through StepStarted and AttemptStarted.
- It emits MessagesSkipped for messages `try` doesn't run, and logs `CTX_LOST` when a non-root stepper finds no frame in ctx.
- It emits the AD-8 time-budget warning when a stop happens inside a `retry`.
- A reusable scripted DAP driver, which 2.9 also uses, proves each in-scope AD-7 parent end to end.

## Boundaries & Constraints

**Always:**
- Mode semantics are copied from benthos v4.81.0 `internal/component/processor/execute.go` (AD-7):
  - **`all`:** for each child in order, run every current batch through it and collect the outputs. With `strict: true`, a batch whose first message has an error passes through without running the child. Stop on a processor error, or when `ctx.Err()` is set after a child.
  - **`try`:** the same loop, but a batch whose first message has an error always passes through untouched.
  - **Ordering:** in both modes the outer loop is over children and the inner loop over batches, exactly as the engine does. The output batches keep the engine's order.
- The stepper never changes message content, metadata, errors or batch boundaries beyond what its children do (AD-8). At every Arrive it passes a deep copy (AD-19).
- Repeat labels (AD-10):
  - The stepper computes them; the Session only displays them.
  - The counter is per call site in the parent frame (AD-17): the Nth entry of this stepper from the same parent stop.
  - `[msg i/n]` applies under `for_each` (one entry per message), with `n` the message count at the parent stop.
  - `[attempt i/max]` applies under `retry`, with `max` from `max_retries`, shown as `[attempt i]` when that is 0 (unbounded).
  - `[loop i]` applies under `while`, and `[group i]` under `group_by`.
  - `branch`, `switch` cases, `try`, `catch` and top-level steppers get no label.
  - The rewrite tells each stepper which label kind it needs, and for retry the max, as new stepper fields.
  - Verify how many times, and with which batch, each parent enters its child list against the benthos v4.81.0 sources in `internal/impl/pure/processor_*.go`. The AD-7 "entered" column is the expectation.
- Events (AD-3, already in the closed set):
  - `StepStarted{repeatLabel}` when an entry begins;
  - `AttemptStarted` for each `retry` entry;
  - `MessagesSkipped{indices, reason}` for messages a `try`-mode stepper passes through because they already failed.
- `CTX_LOST` (AD-17): a stepper whose `parent` isn't -1 but finds no frame in ctx logs `CTX_LOST` (the registry code) at Warn through the AD-15 handler, then runs on the root thread.
- Time budget (AD-8): the first stop inside a stepper under a `retry` gives one warning Output per session and stepper. It says that pausing inside a retry changes its backoff and elapsed-time behaviour, so equivalence isn't guaranteed while paused there.
- The scripted DAP driver is a non-test, importable package `test/dapdrive`:
  - it starts `debugger.Serve` over in-memory pipes with the stock environment, launches a pipeline and runs a script (e.g. Step In at every stop until terminated);
  - it returns each stop's line, user path, frame names and labels, plus the delivered state.

  2.9 imports it.
- All earlier tests and the tracer/nested F5 paths keep passing, and the equivalence case still matches its reference.

**Never:**
- No concurrent parents (`workflow`, `parallel`, parallel `retry`), which stay opaque until epic-concurrency. No breakpoints or exception stops. No SetMessage.
- No equivalence comparison or corpus (2.9), and no delivery display (2.8).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| try skips the failed | `try: [throw on msg 1, mapping]` with 2 single-message batches | The mapping runs only on msg 0; msg 1 passes through with its error; MessagesSkipped names msg 1 (reason `try`) | (none) |
| strict skip | `error_handling.strict: true`, processor 0 fails a batch | Later steppers' children don't run on that batch, the same as the reference run | (none) |
| catch | A failed message, then `catch: [mapping]` | The catch stepper is entered once per caught message; the error is cleared after, as in the engine | (none) |
| for_each labels | `for_each` over a batch of 3 | 3 entries labelled `[msg 1/3]`, `[msg 2/3]`, `[msg 3/3]` in the call stack | (none) |
| retry labels | `retry: {max_retries: 2}` whose child fails twice, then succeeds | Entries `[attempt 1/2]` … plus AttemptStarted per attempt; one time-budget warning | (none) |
| while labels | `while` that loops 3 times | `[loop 1]` … `[loop 3]` | (none) |
| group_by labels | 2 groups | `[group 1]`, `[group 2]`; output batches split as in the engine | (none) |
| ctx lost | A stepper config with `parent: 3` called with a ctx that has no frame (unit test) | `CTX_LOST` logged at Warn; it runs on the root thread | (none) |
| Driver | `test/dapdrive` with Step In on `testdata/tracer/nested.yaml` | Every stop's line and path in order, with frame stacks | (none) |
| Equivalence unchanged | The existing equivalence case | Same output as `RunReference` | (none) |

</frozen-after-approval>

## Code Map

- `debugger/engine/stepper.go`:
  - `stepperSpec()` has `id`, `parent`, `mode`, `strict`, `concurrent`, `lines` and `processors` (2.5).
  - `ProcessBatch` enters its frame with `arriver.Enter(ctx, id, len(procs))`, loops the children with Arrive before each, adds the end stop, and exits.
  - Replace the loop body with mode-aware execution (see the benthos `execute.go` loops, quoted in the 2.7 investigation: lines 34–62 for ExecuteAll with `IsStrict`, 70–98 for ExecuteTryAll).
  - Add the label fields to the spec (e.g. `label: msg|attempt|loop|group` and `max_attempts`).
- `debugger/rewrite/walk.go` `processor()`: writes the stepper fields for each parent kind. Add the label kind per parent (for_each → msg, retry → attempt with `max_retries`, while → loop, group_by → group). Goldens regenerate.
- `debugger/core/session.go` `Enter(ctx, stepperID, children)` (line ~494): the frame record has `RepeatLabel` (empty) and `CallSite`.
  - Add a way to get the per-call-site entry counter and the parent stop's message count. For example, `Enter` takes the label kind and returns the computed label, or a `core.EntryIndex(ctx)` helper. The label is still computed in the stepper, from Session-provided counters, per AD-10.
  - Emit `StepStarted{RepeatLabel}` and `AttemptStarted`, and add `MessagesSkipped`.
  - Frames show `RepeatLabel` in DAP stackTrace names (`debugger/dap/server.go` stackTrace builds names from the frame's `Name`; append the label).
- `debugger/core/errors.go`: `CodeCtxLost` exists (2.2). Log it through `Session.Log` at Warn (2.4).
- `debugger/logging.go` / `Session.Log`: the warning path for CTX_LOST and the time-budget warning (or `core.Output` directly for the latter).
- New package `test/dapdrive/`: a Go DAP client over `io.Pipe` to `debugger.Serve(standardEnv, …)`. Reuse the helpers in `debugger/serve_test.go` and `debugger/dap/*_test.go` (send, next, wait-for-event) as the model, but in a non-test package. The stock env is `schema.Standard(...).Environment()` with `components/all`, as in `test/equivalence/equivalence.go` `standardEnv()`.
- Tests:
  - `debugger/engine/stepper_test.go` (new) unit-tests the modes against `service.ExecuteProcessors` with plain processors on the same input, which is the reference.
  - `test/dapdrive/dapdrive_test.go` runs one scripted pipeline per in-scope AD-7 parent (new `testdata/tracer/parents/*.yaml`) and asserts lines and labels.
- Benthos parent sources to read for entry counts: `internal/impl/pure/processor_for_each.go`, `processor_while.go`, `processor_retry.go`, `processor_group_by.go`, `processor_catch.go`, `processor_try.go`, `processor_switch.go`, `processor_branch.go` in the module cache (`go list -m -f '{{.Dir}}' github.com/redpanda-data/benthos/v4`).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/engine/stepper.go` + `stepper_test.go`: the `all`/`try`/strict modes matching `ExecuteProcessors`/the engine on the same input (multi-batch in and out), MessagesSkipped, deep copies, `CTX_LOST`.
- [x] `debugger/rewrite/walk.go` + goldens: label kind and retry max on the stepper config.
- [x] `debugger/core/session.go`, `frames.go`, `protocol.go` (+ tests): per-call-site entry counters and the parent message count for labels; StepStarted, AttemptStarted and MessagesSkipped emission; the time-budget warning once per stepper.
- [x] `debugger/dap/server.go` (+ test): labels in stack frame names, and Output for the new events.
- [x] `test/dapdrive/` + `testdata/tracer/parents/*.yaml`: the scripted driver, and a test per in-scope AD-7 parent (lines plus labels).
- [x] `README.md`: one line on labels.

**Acceptance Criteria:**
- Given `testdata/tracer/parents/for_each.yaml` in VS Code, when the user steps in, then the call stack shows `[msg 1/3]`, `[msg 2/3]`, `[msg 3/3]` on the for_each frames in turn.
- Given a pipeline with `try` whose first child fails one message, when the user steps through, then the second child never stops on that message, and the Debug Console reports it as skipped.
- Given `task ci`, when it runs, then every scripted parent test, the mode unit tests and the existing equivalence case pass.

## Plan Change Log

## Implementation Notes

- Port: `Arriver.Enter(ctx, core.Entry) (ctx, func(memo any))` replaces `Enter(ctx, id, children)`. `Entry` carries the stepper's `Label func(core.Site) string`, a shared counter name `Site` (group_by groups share one per parent stop, so two groups read `[group 1]`, `[group 2]`) and `Attempt`. The Session keeps the per-call-site counters in the parent frame (key: parent stop, stepper or shared Site), calls `Label` once with `{Entries, Messages, Memo}` and only displays the result. New `Arriver.Skip` reports skipped messages; the Session emits `MessagesSkipped` (with user path, file, line). `FrameRef.InRetry` is inherited down the stack and drives the once-per-stepper time-budget warning on the first stop.
- Retry attempt numbers: retry calls the stepper once per attempt per message with no marker, so an entry is attempt k+1 only when the previous entry at the call site failed, attempts remain (`max_attempts` = `max_retries`, verified in `processor_retry.go`: `retries == maxRetries` stops after `max_retries` attempts) and its input (Snapshot JSON) is identical; otherwise attempt 1. The memo passes through the exit func.
- Rewrite: new stepper fields `repeat: msg|attempt|loop|group` (only when non-empty) and `max_attempts` (retry only, `max_retries` node copied verbatim, so `${VAR}` resolves in the engine). `catch` steppers get `strict: false`, because `ExecuteCatchAll` ignores strict. New golden `strict-parents`.
- Stepper: does not Arrive before a child that runs on no batch (all skipped), so try's second child never stops on a failed message; each skipped message is reported once per entry at the first child it skips. A `try` list gets the `msg` repeat kind (try enters it once per message, AD-7), so a try skip names the message by its place in the parent's batch ("Skipped message 2 of 3", matching `[msg 2/3]`, via `Arriver.Skip(…, of)`); other skips (strict) name messages by index across the entry's batches, and the text states the engine rule (the batch's first message failed).
- Counters per call: the stepper wraps every child call in `core.MarkCall(ctx, len(batch))`, and the Session keys entry counters by that call, so `[msg i/n]`, `[loop i]`, `[group i]` and attempts restart for each batch a parent stop holds, and `n` is the called batch's size. Known limit: within one `while` call, a child that splits the batch makes one loop show as several entries (`[loop 1]`, `[loop 2]`, …), since the engine gives no loop marker.
- dapdrive compares delivered metadata too (keys, AD-11 types, display values). Only `backoff_duration` (retry) is compared by key and type alone: it sums backoff sleeps that the engine randomizes (default randomization factor 0.5, not settable from the retry config).
- DAP: `MessagesSkipped` and `AttemptStarted` render as console Output (the skip with source/line); `StepStarted`/`StepFinished` stay unrendered (labels show in frame names).
- `test/dapdrive`: `Run(ctx, Options{Pipeline, Launch, Script, Env, LogOutput, MaxStops}) (Result, error)`; `Result{Stops, Output, Delivered}`. A stop's user path is resolved from the rewrite's LineMap by (frame list path, line). `Delivered` is the last stop's batches when it is a top-level end stop (no Delivered event until 2.8). Tests compare each parent sample's delivery against `equivalence.Case.RunReference`.
- The `generate-mapping` equivalence case can't be compared through the driver: its input emits 3 single-message batches and the debugger steps only the first captured one (2.9's concern). `task test:equivalence` still passes.

- 2026-10-20 VS Code check (user): for_each showed `[msg 1/3]`…`[msg 3/3]`; try labelled its frames, skipped the failed message and reported it in the Debug Console; retry showed attempt labels, the attempt lines and one time-budget warning. All ACs met.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 6, false 0, maybe-false 0. Four were patched, two deferred, two rejected.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | A try skip is reported as message 0 (try runs one single-message batch per message), and try frames have no label | medium | patch | try lists get the `msg` repeat kind, and skips name the message's place in the parent batch ("Skipped message 2 of 3 …"). `Arriver.Skip` and `MessagesSkipped` gain `Of` |
| 2 | The strict/batch skip text says every listed message "already failed" | low | patch | Reworded to the engine rule: the batch's first message failed |
| 3 | `[loop i]`/`[group i]`/`[msg i/n]` keep counting across separate parent calls when the parent stop holds several batches | medium | patch (partial) | `core.MarkCall` marks each child call and the counter is keyed by it (`TestLabelsRestartPerCall`). Remaining limit, a split inside one while call, deferred (deferred-work.md) |
| 4 | Retry attempts are inferred by identical input, so an identical next message after `max_elapsed_time` continues the count | low | defer | Label-only, needs an engine marker (deferred-work.md) |
| 5 | The dapdrive delivery check omits metadata (AD-8) | low | patch | Metadata keys, types and values are compared. Retry's `backoff_duration` value is excluded, because the engine randomizes backoff |
| 6 | No end-to-end strict test | low | defer | Belongs in the 2.9 corpus strict row (deferred-work.md) |
| 7 | dapdrive can report an end stop as child 0 when a flow list sits on one line | low | reject | The samples and corpus use block lists; the driver is test tooling |
| 8 | Entries without a parent frame share a session-wide counter that is never reset | low | reject | It only matters for CTX_LOST steppers with a repeat kind; top-level entries have no label |

## Design Notes

The stepper reproduces the engine loop instead of calling `service.ExecuteProcessors`, because it must Arrive between children. That makes the loop the place where equivalence can break, so the unit tests compare it against `ExecuteProcessors` (ExecuteAll semantics) and against a `try` processor built from the same children, on the same inputs. The strict flag comes from the rewrite's `strict` field, not from ctx: benthos carries strict mode in ctx through `processor.WithStrict`/`IsStrict`, which are internal (AD-2).

## Verification

**Commands:**
- `go test -race ./debugger/... ./test/dapdrive/...`: expected PASS.
- `task ci`: expected exit 0.

**Manual checks:**
- In VS Code, step through `testdata/tracer/parents/for_each.yaml` and a retry/try sample. The call stack shows the labels, and the Debug Console shows skipped messages and the retry time-budget warning.
