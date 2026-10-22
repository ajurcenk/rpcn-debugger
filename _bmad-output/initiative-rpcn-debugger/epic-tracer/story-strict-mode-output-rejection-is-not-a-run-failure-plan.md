---
title: 'Strict-mode output rejection is not a run failure'
type: 'bugfix'
ticket: '14'
created: '2026-10-22'
status: done
baseline_revision: 'f2e77f9a4c2043c06de0e467ec9046db56bf462b'
route: 'oneshot'
route_source: 'auto'
risk: 'low'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** Under `error_handling.strict`, the engine's output nacks every message that failed a processing step: benthos v4.81 `internal/component/output/async_writer.go` wraps each one as `rejected due to failed processing: …`. The step stream's producer gets that nack back as an error, the engine returns `deliver the batch: …`, and the Session prints `The debug run failed`. This is expected strict behaviour, and the delivered batches match the reference run (found in the 2.9 strict corpus case).

**Approach:**
- When the step stream runs strict and the producer's error is that strict rejection, the engine returns a new registry error, `STRICT_REJECTED`.
- The Session handles it as a normal end of run:
  - a console note saying the output rejected the messages that failed a processing step, as in production (where the input would see them nacked);
  - the usual delivered and "Nothing was delivered" handling;
  - no `The debug run failed` line.
- A non-strict processor error, or any other producer error, still reports as before.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because this is about 40 lines across `debugger/core/errors.go`, `debugger/engine/engine.go` and `debugger/core/session.go`, plus tests.

- The rejection is detected by its text (`rejected due to failed processing`) together with `Configs.Strict`. Benthos returns its internal `batch.Error`, which isn't reachable through the public API (AD-2). Each release pins one Connect version (AD-14), so a wording change shows up in the strict equivalence case.
- An interpolated strict value isn't applied to the output (`SetStrict` gets false, 2.5), so no rejection can happen there.

Implementation (2026-10-22):
- Files:
  - `debugger/core/errors.go` + `errors_test.go`: the new code `STRICT_REJECTED` = 1010, pinned.
  - `debugger/engine/engine.go` `step`: under `Configs.Strict`, a producer error containing the rejection text returns `STRICT_REJECTED`.
  - `debugger/core/session.go` run end: `STRICT_REJECTED` emits the console note, then is treated as success (the nothing-delivered check still applies).
  - `debugger/core/session_test.go` `TestStrictRejectionIsNotAFailure`.
  - `test/equivalence/equivalence_test.go`: every run must be free of "The debug run failed".
  - `testdata/equivalence/strict/expect.yaml`: requires the note.

- 2026-10-22: the full `task ci` passed with Docker running again; the integration step had been blocked by a stopped Docker daemon, unrelated to this change.

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 4, false 0, maybe-false 0. Three were patched and one deferred.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | In a mixed strict batch, a write failure of the passing messages folds into the same batch.Error and would read as the expected rejection | low | defer | Latent: `rpcn_debug_sink.WriteBatch` never fails. It becomes reachable with `output: real`, so deferred to epic-safe-against-real-systems (deferred-work.md) |
| 2 | No engine-level test of the mapping (the Session test fakes `STRICT_REJECTED`) | low | patch | `engine_test.go` `TestStrictRejection`: a real pipeline with a throwing processor gives `STRICT_REJECTED` under strict and nil without it |
| 3 | The registry docs say every code is a Session failure | low | patch | The `Code` doc now allows an engine-to-Session outcome that never reaches a frontend as an error |
| 4 | The note drops the rejection text | low | patch | The note appends the translated rejection, e.g. `…: rejected due to failed processing: boom`. Covered in `TestStrictRejectionIsNotAFailure` |
