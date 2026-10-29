---
title: 'Delivered outcomes, Session-owned batch numbers, and strict from errored messages'
type: 'feature'
ticket: '8'
created: '2026-10-29'
status: done
baseline_revision: 'cfc0e7842f491fdd2286aaed76a06110a8580605'
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

**Problem:** This is E3 entry 8 (CAP-11, D3; user decision 2026-10-23 from the 3.7 review). Five problems remain after 3.7:
- **Strict is decided by text.** `STRICT_REJECTED` comes from matching benthos's error text (`strictRejection` in `engine.go`). Under `output: real`, a mixed strict batch whose write then fails folds both into the same error, so a real write failure is reported as the harmless strict note.
- **No outcome or bound on writes.** A failed write is a generic delivery failure, and an unreachable output retries forever.
- **Two counters.** The batch number is counted twice: in `dap/server.go` (`delivered`) and in the Session.
- **Console texts say "dry-run output" in every mode.**
- **A goroutine leak.** The producer goroutine leaks when the step stream fails (3.7).

**Approach:**
- **Errored messages at the output, structurally.** One counter per run, seeded with the captured batch's errored messages; each top-level stepper entry (parent -1) adds the errored messages it returns and subtracts those it was given. After the run the counter holds the errored messages that reached the output, also when a stepper splits the batch or strict skips later steppers (user decision 2026-10-29, amending the first wording, which overwrote a field per stepper).
  - Benthos's own strict output wrapper is left on. `SetStrict(false)` would also turn off strict inside opaque parents, which read it from the context (`processor.IsStrict`).
- **Outcome from the sink.** `rpcn_debug_sink` records each batch's outcome: `recorded` (dry-run), `written` (real, after the write succeeds), `discarded` (none) or `failed` (real, with the write error). `Deliver` is called once per batch it receives, after the write, carrying the outcome and error.
  - Under `real`, each write gets a deadline (30 s, a test-shortenable field). A write still unfinished then is `failed`, with an error saying it timed out.
- **The engine decides from facts, not text:**
  - any `failed` outcome means the run ends with `OUTPUT_FAILED` (1013, new in the registry), naming the output and the error;
  - otherwise, under strict with errored messages at the output, it's `STRICT_REJECTED` (as today);
  - otherwise any producer error is a delivery failure, as today.

  The `strictRejection` text match is deleted.
- **Session owns the numbering.** `core.Delivered` gains `Batch` (1-based, numbered by the Session), `Outcome` and `Error`. DAP displays `Batch` and its own counter is removed. The `rpcnDelivered` event body gains `outcome` and `error`.
- **Mode-aware texts.** The Session's and DAP's console texts follow the mode:
  - dry-run: "…to dry-run output";
  - real: "…written to the output";
  - none: "…not written (output: none)";
  - failed: "…failed to write: <error>".

  "Nothing was delivered" names the mode too.
- **The producer runs on a context** that is cancelled once the step stream has stopped, so it can't outlive the run (fixes the 3.7 leak).

## Boundaries & Constraints

**Always:**
- AD-8 holds: the delivered batches under `dry-run` are unchanged, the strict and strict-interpolated corpus cases stay green, and no golden changes.
- `core` stays stdlib-only. The outcome values are `core` constants, and `dap` only displays them.
- The new code goes in the `core.Error` registry with its doc; `OUTPUT_FAILED` names the output path and line where known.
- Never-ack is untouched.

**Never:**
- No change to which messages are written or delivered: benthos's strict wrapper still decides that.
- No Restart counter reset (epic-edit-restart-replay owns it).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Dry-run | default | "Delivered batch 1 (n messages) to dry-run output"; outcome `recorded` | none |
| Real ok | `output: real`, the write succeeds | "…written to the output"; outcome `written` | none |
| None | `output: none` | "…not written (output: none)"; outcome `discarded` | none |
| Real refused | `output: real`, the output refuses the write | One Delivered, outcome `failed`, with the error | `OUTPUT_FAILED` |
| Real timeout | `output: real`, unreachable broker, deadline shortened in the test | Outcome `failed` ("timed out") | `OUTPUT_FAILED` |
| Strict all failed | strict, every message errored | Nothing delivered | `STRICT_REJECTED` note (as today) |
| Strict mixed, write ok | strict, mixed batch, real write succeeds | The passing messages delivered and `written` | `STRICT_REJECTED` note |
| Strict mixed, write fails | strict, mixed batch, the real write fails | `failed` | `OUTPUT_FAILED`, not the strict note |
| Reconnect | an output whose first write returns not-connected, then succeeds | Exactly one Delivered for the batch | none |

</frozen-after-approval>

## Code Map

- `debugger/engine/sink.go`:
  - `WriteBatch` (records, writes under `real` via the `OwnedOutput`, then `Deliver`), `Connect`/`Prime`, `Close` (from 3.7);
  - add the outcome, the per-write deadline, and a run-visible record of a failed outcome.
  - Benthos retries a `WriteBatch` that returned not-connected; make sure only one `Deliver` happens per batch.
- `debugger/engine/stepper.go` `ProcessBatch` (~140–224): the stepper's `parent` (-1 is top level) and its `out` batches. It needs a pointer to the run's errored-at-output field, wired at registration (`registerStepper`) the way the arriver is.
- `debugger/engine/engine.go`:
  - `run` struct; `step` (~330–380): `produce(context.Background(), batch)`, `prodErr`, the `strictRejection` const and its `strings.Contains`;
  - `stopTimeout`. Replace the text match with the decision above, and give the producer a cancellable context.
- `debugger/core/protocol.go` `Delivered` (~266); `core/errors.go` (add 1013); the `core.Arriver.Deliver` signature in `port.go`.
- `debugger/core/session.go`:
  - `Deliver` (~504, `s.delivered++`);
  - the end-of-run texts (~425–440: the strict note, "Nothing was delivered to dry-run output");
  - where the run error becomes an Output or failure;
  - the Session's output mode (from 3.7 launch).
- `debugger/dap/server.go`: the `delivered atomic.Int64` (~69), `frontend.delivered`, `deliveredText`, `deliveredBody`; plus `debugger/dap/delivered_test.go`.
- Tests:
  - engine tests with a fake output: ok, refusal, timeout, reconnect, and strict mixed with a failing write;
  - the strict corpus cases (`testdata/equivalence/strict*`);
  - Serve tests for the texts per mode and `OUTPUT_FAILED`.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/` -- `Delivered.Batch`/`Outcome`/`Error`, the outcome constants, 1013, the Session's numbering and mode-aware texts, run-failure handling -- the contract and the Session.
- [x] `debugger/engine/` -- the outcome and deadline in the sink, the errored-at-output record in top-level steppers, the fact-based decision, the cancellable producer context, the deleted text match -- the engine.
- [x] `debugger/dap/server.go` -- display `Batch`, no counter, the `rpcnDelivered` body with outcome and error -- the frontend.
- [x] Tests for every matrix row.

**Acceptance Criteria:**
- Given each matrix scenario, then the Delivered events (count, number, outcome), the console text and the run's end code match the matrix.
- Given `task ci`, then it exits 0, with every golden and corpus case unchanged and `strictRejection` gone from the code.

## Implementation Notes

- **Errored-at-output count.** A plain overwrite by the last top-level stepper is wrong in two cases the engine produces: a top-level stepper that splits the batch is followed by one call of the next stepper per batch, and under strict benthos's `ExecuteAll` passes a batch whose first message failed by the later top-level steppers without calling them. So each top-level entry (parent -1 and no frame in ctx, which excludes `processor_resources` steppers called from inside) adds the errored messages it returns and subtracts those it was given, on one `atomic.Int64` per run, seeded with the captured batch's count. After the run it holds the errored messages that reached the output. `engine/output_test.go` `TestSinkStrict` covers a skipped `output.processors` stepper.
- **One Deliver per batch.** When the user's output reports not-connected, the sink returns `service.ErrNotConnected` unwrapped and records nothing, so benthos reconnects and retries the same batch. The write deadline (30 s, `Adapter.SetWriteTimeout` for tests) runs from the batch's first attempt, across retries. The failure error the sink returns is flattened (`%v`), so an `ErrNotConnected` in its chain can't make benthos retry a batch that has already been reported. Through `OwnedOutput`, a not-connected user output is retried inside benthos, and the sink sees one write.
- **On the deadline** the batch is already queued in the `OwnedOutput`, which goes on retrying it. The sink closes the output right away with plain `OwnedOutput.Close`, within `stopTimeout`, so it takes no more batches. Close doesn't end the write in flight, and the public API has no way to end it, so the error says `the write timed out after <d>; the output keeps retrying until the session's streams stop, so it may still write the batch`. A timed-out run ends about `stopTimeout` (5 s) after the deadline. A failed batch is reported, but it isn't recorded in the sink's received batches, so the run's "delivered" log doesn't count it.
- **Texts.** Headers: `Delivered batch i (n messages) to dry-run output`, `…, written to the output`, `…, not written (output: none)`, `…, failed to write: <error>`. The nothing-delivered note: `Nothing was delivered to dry-run output`, `Nothing was delivered, so nothing was written to the output` (real), `Nothing was delivered (output: none)`. `OUTPUT_FAILED` reads `the output failed to write a delivered batch: <error> (at output, line N)` and reaches the user through the Session's existing `The debug run failed:` stderr line.
- **Spine.** AD-3's "Target: today `debugger/dap` also counts" was met by this entry; the orchestrator replaced it with the built rule (Session-numbered `Batch`, plus `Outcome` and `Error`) and added a memlog event.

## Plan Change Log

- 2026-10-29 (implementation): the frozen Approach says each top-level stepper overwrites the errored-at-output field. That is wrong when a top-level stepper splits the batch (the next is called once per batch) and under strict, where later top-level steppers are skipped for a batch whose first message failed. Instead, each top-level entry adds the errored messages it returns and subtracts those it was given, on one per-run counter seeded with the captured batch's count. The intent (decide `STRICT_REJECTED` structurally) is unchanged; confirmed by the user on 2026-10-29, and the frozen Approach amended. KEEP: the rest of the decision rule.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 3, false 0, maybe-false 0, plus 1 frozen-intent change confirmed by the user. The orchestrator raised 1 more finding afterwards.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | A write reported as timed out can still be written later: the transaction is already queued in the OwnedOutput, which keeps retrying until stream stop | medium | patch | On the deadline the sink closes the output (bounded by `stopTimeout`). The error says honestly that the output keeps retrying until the streams stop, so it may still write the batch; the README says the same. The outcome stays `failed`/`OUTPUT_FAILED` |
| 2 | The frozen Approach (overwrite the field per stepper) was replaced during implementation by an add-and-subtract counter | intent | user decided | User 2026-10-29: accepted; the frozen Approach was amended (it fixes splits and strict-skipped steppers; `TestSinkStrict`) |
| 3 | The engine's delivered count includes failed batches | low | patch | Failed batches are reported via `Deliver` but not recorded in `s.got`; dry-run/none never fail, so AD-8 is unchanged |
| 4 | The spine was edited while the plan says "not changed" | low | patch | The plan note was stale: the orchestrator updated AD-3 (the met target) on purpose; the note now says so |
| 5 | A leftover blank line in `stepper.run` | low | patch | Removed |
| 6 | (orchestrator) The first fix stopped the in-flight write through `OwnedOutput.XUnwrapper()` and reflection into benthos's internal output (`TriggerCloseNow`); benthos documents `XUnwrapper` as "for internal use only, do not use this", which breaks AD-2's intent | medium | patch | Removed: a plain bounded `Close`, with the honest error text above. A timed-out run ends about 5 s after the deadline |

## Verification

**Commands:**
- `go test -count=1 ./debugger/... ./test/dapdrive/ ./test/equivalence/` -- expected: PASS.
- `grep -rn strictRejection debugger/` -- expected: no match.
- `task ci` -- expected: exit 0.

Run 2026-10-29 by the orchestrator after the review patches: `task ci` exit 0; `strictRejection` and `XUnwrapper` absent from `debugger/`; no golden or corpus change.
