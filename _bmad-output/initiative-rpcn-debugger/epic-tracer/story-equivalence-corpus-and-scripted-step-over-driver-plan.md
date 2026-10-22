---
title: 'Equivalence corpus and scripted Step Over driver'
type: 'feature'
ticket: '9'
created: '2026-10-21'
status: done
baseline_revision: '2d287b8ed3ad030bc2f7d61f846fe1dd88366d1a'
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

**Problem:** AD-8 says the debugger must not change the output, and CAP-12 is the user-facing promise. Today the only evidence is the 2.7 parent samples. Nothing in `task ci` runs a corpus of every in-scope AD-7 row, plus strict mode, with and without pauses, and fails when the debugger's delivered batches differ from the same pipeline run without the rewrite.

**Approach:**
- Grow `testdata/equivalence/` into the AD-8 corpus: one case per in-scope AD-7 row, plus strict mode, `processor_resources`, an opaque parent, and the input and output processor lists.
- The harness runs each case's reference (no debugger) and runs the debugger through `test/dapdrive` three times: Continue only (no pauses after the entry stop), Next at every stop, and Step In at every stop.
- Each run must equal the reference on content, metadata, errors, batch boundaries and order.
- Every case asserts that ctx reached every child: no `CTX_LOST`, and nested frames are seen (AD-17).
- `task ci` runs it all.

## Boundaries & Constraints

**Always:**
- **Same batch (user-approved decision, 2026-10-21).** Each corpus case's input yields exactly one batch of at most 10 messages (the `maxMessages` default), so the debugger's captured batch is the reference run's whole input. The harness fails a case whose debugger run reports truncation (the "captured N of M" line) or whose reference run receives more than one input batch. Case files say so in a header comment.
- **Equal (AD-8):** the ordered list of delivered batches matches. Each message matches on content bytes; metadata keys, types and values; and error text. The one exception is retry's `backoff_duration` value (randomized by the engine), compared by key and type only, as in 2.7.
- **Runs per case:**
  1. Continue at the entry stop (no further pauses).
  2. `next` at every stop until terminated.
  3. `stepIn` at every stop until terminated.

  AD-8 requires the first two; the third covers nested pause paths.
- **Ctx proof (AD-17):** in every case, no run's output contains `CTX_LOST`. In every case with a parent, the Step In run has at least one stop with two or more frames inside that parent.
- **Corpus rows (in scope):**
  - `pipeline` (top level), `input-processors`, `output-processors`;
  - `branch`, `switch`, `try`, `catch`, `for_each`, `while`, `group_by`, `retry` (non-parallel);
  - `strict` (strict mode with a failing processor; the reference also runs strict);
  - `processor-resources` (a resource with a parent inside, used via `resource:`);
  - `opaque-parallel` (a `parallel` parent run as one opaque step);
  - `nested` (branch in switch in for_each).

  Each case is deterministic: no `now()`, `uuid`, `random` or timing-dependent processors except retry's backoff.
- The comparison helper moves into the `test/equivalence` package (exported), and `test/dapdrive`'s tests reuse it, so there's one definition of "equal".
- Failures print the case, the run and a diff of the delivered batches.

**Never:**
- No concurrent parents beyond running them opaque (epic-concurrency adds their wrapped rows). No breakpoints, SetMessage or replay inputs.
- No change to the debugger's behaviour; this entry only adds evidence. A failing case is a bug to report and fix in its own entry, not to paper over in the harness.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Case passes | Any corpus case | 3 runs, each equal to the reference; no CTX_LOST; nested frames seen | (none) |
| Content differs | A run delivers different content (test-only injected fault) | The test fails, naming the case, the run and the diff | Fails |
| Boundaries differ | group_by splits differently | The test fails, showing the batch lists | Fails |
| Metadata differs | A key's value or type differs | The test fails | Fails |
| Truncated case | A case whose input batch is >10 messages | The test fails: the case breaks the same-batch rule | Fails |
| Multi-batch input | A case whose input yields 2 batches | The test fails: the case breaks the same-batch rule | Fails |
| CTX_LOST | Output contains CTX_LOST | The test fails | Fails |
| strict | A failing processor under `error_handling.strict: true` | The debugger matches the reference (including what the strict output rejects) | (none) |

</frozen-after-approval>

## Code Map

- `test/equivalence/equivalence.go`: `Case{Name, Dir, Pipeline}`, `Cases(dir)`, `RunReference(ctx) ([]service.MessageBatch, error)` (with `rpcn_reference_sink`, strict carried as written, private HTTP mux).
  - Add a reference input-batch counter for the same-batch rule, e.g. a reference sink option or a count of input batches via an `input.processors` probe. **Prefer** counting what `RunReference` already knows, or wrapping the input's batches at the engine level, **without** changing what the pipeline does.
  - Export `Compare(ref []service.MessageBatch, got []dapdrive.Batch) error`, moved from `test/dapdrive/dapdrive_test.go` `sameAsReference`/`refMeta`/`renderMessage`, along with the `backoff_duration` rule.
- `test/equivalence/equivalence_test.go`: `TestCases` runs references only. Add `TestEquivalence`: for each case, the reference, then three `dapdrive.Run` with `dapdrive.Options{Pipeline, Script}` scripts (`Always(Continue)` after the entry, `Always(Next)`, `Always(StepIn)`), then `Compare`, the CTX_LOST check and the frames ≥2 check.
- `test/dapdrive/dapdrive.go`: `Run(ctx, Options{Pipeline, Launch, Script, Env, LogOutput, MaxStops})` returns `Result{Stops, Output, Delivered}`. `Stop.Frames` has the frame names. Reuse it unchanged if possible.
- `test/dapdrive/dapdrive_test.go`: switch `sameAsReference` to `equivalence.Compare`. There's no import cycle: dapdrive's non-test code doesn't import equivalence.
- `testdata/equivalence/generate-mapping/pipeline.yaml`: currently `count: 3` with default batch_size 1 (3 batches), so make it one batch of 3. Add the case folders listed above. `testdata/tracer/parents/*.yaml` are good starting points, with the same-batch rule applied.
- `Taskfile.yml` `test:equivalence`: `-run '^TestCases$'`. Run `TestCases|TestEquivalence` with `-v` so the cases are listed.
- `deferred-work.md`: strict end-to-end (from 2.7) is delivered here.

## Tasks & Acceptance

**Execution:**
- [x] `test/equivalence/equivalence.go` (+ test): `Compare` (moved), the same-batch rule check, and `TestEquivalence` with three runs, the CTX_LOST check and the nested-frames check. Fault-injection unit tests for `Compare` (content, metadata, boundaries) and the rule (more than 10 messages, 2 input batches).
- [x] `testdata/equivalence/*/pipeline.yaml`: every row listed under Always, each deterministic and single-batch, with a header comment.
- [x] `test/dapdrive/dapdrive_test.go`: use `equivalence.Compare`.
- [x] `Taskfile.yml`: run `TestEquivalence` in `test:equivalence`. `README.md`: one line on the corpus rule (single batch, at most 10 messages).

**Acceptance Criteria:**
- Given `task ci`, when it runs, then `test:equivalence` lists every corpus case and all three runs per case pass.
- Given a deliberately broken stepper (a local experiment that skips `try` mode), when `TestEquivalence` runs, then the `try` case fails with a diff.

## Plan Change Log

- 2026-10-21: `test/dapdrive/dapdrive_test.go` is now an external test package (`dapdrive_test`). The Code Map's "no import cycle" holds only for that: an in-package test of `dapdrive` importing `equivalence` (which imports `dapdrive` for `Compare`'s signature) is a cycle. Every identifier it used was already exported.
- 2026-10-21: the `strict` case reads `testdata/equivalence/strict/input.json` with a `file` input (`auto_replay_nacks: false`) instead of `generate`. `generate` always wraps its reader in a nack preserver (benthos `input_generate.go`, even with `auto_replay_nacks: false`), so the reference replays the message the strict output rejects as a second input batch, which the debugger (never acking) can't match; the same-batch rule caught it. The path is relative to the test packages' cwd (`../../testdata/...`).

## Implementation Notes

- `Case.Reference(ctx) (Reference{Batches, InputBatches}, error)`; `RunReference` wraps it. The input count comes from wrapping the input plugin (every key but `label`/`processors`) in a registered `rpcn_reference_input` batch input whose `input` field is an `OwnedInput`: batches and acks pass through unchanged, sizes are counted before `input.processors`.
- `test/equivalence/compare.go`: `Compare` (moved from dapdrive_test, same rendering and `backoff_duration` rule; non-UTF-8 reference content is base64, as dapdrive shows it), a per-batch `-`/`+` diff, and `CheckSameBatch(ref, output)` (one input batch of at most `MaxMessages` = 10, no "captured N of M (maxMessages)" line).
- `Compare` is structural (batch count, message count, raw content bytes, error, metadata map of AD-11 type + lossless value; `refMeta` ports `engine.metaEntry`, json.Number and NaN/Inf included). dapdrive's delivered `Message` carries `Raw` and `Base64`, and `Meta` carries `Lossless`, from the Snapshot.
- `TestEquivalence`: per case, the reference, then `continue`/`next`/`stepIn` subtests via `dapdrive.Run` with `Always(cmd)`. Each checks the same-batch rule, `Compare`, no `CTX_LOST` in the Serve log or the output events, an `entry` first stop, a single stop for Continue, and the case's `expect.yaml`: reference `batches` and `messages` (also checked by `TestCases`), Step In `stops` (user path + frame count), `no_stops_under`, and `output` substrings for every run.
- Corpus: 16 cases (the 15 listed rows plus the existing `generate-mapping`, now one batch of 3). `strict` splits so the failed message heads the batch [3, 4]: message 4 skips the last processor and message 3 is rejected. Its file input path is `${EQ_CASE_DIR}/input.json`; the harness sets EQ_CASE_DIR with t.Setenv.
- Observed (not changed): on a strict reject the debugger prints "The debug run failed: deliver the batch: rejected due to failed processing: …", while the delivered batches still match the reference.
- Fault experiments (local, reverted): dropping `try` mode from `stepper.skips` fails the `try` case in all three runs with a diff; dropping the strict skip fails the `strict` case.
- Verification: `go test -race -count=1 ./test/equivalence/ ./test/dapdrive/` PASS; `task ci` exit 0, `test:equivalence` listing all 16 cases x 3 runs.

## Review Triage Log

Pass 1 (quick): high 0, medium 4, low 4, false 0, maybe-false 0, plus 3 rejected. Patched as below.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `Compare` matched on joined, unescaped strings, so [] equalled [[]] and separator-containing content could merge or split | medium | patch | Structural comparison: batch count, message count, raw content (`bytes.Equal`), error, metadata map. `TestCompareFaults` adds [] vs [[]] and separator cases |
| 2 | Comparison went through the AD-11 display form (text vs base64 confusable); `refMeta` lacked json.Number and NaN/Inf | low | patch | dapdrive carries `Raw`/`Base64`/`Lossless` from the Snapshot, and `refMeta` is aligned with `engine.metaEntry` (`TestCompareRaw`, `TestRefMeta`). The AD-11 type class stays the contract |
| 3–7 | The nested-parent check was switched on by rewrite output; any 2-frame stop satisfied it; opaque-parallel, stop locations and reference shapes went unasserted | medium | patch | Each case's required `expect.yaml` (KnownFields) declares `batches`, `messages`, `stops` (path plus frames), `no_stops_under` and `output`. `hasNestedParent` and `caseChecks` are gone. Each parent's child path, the nested 2/3/4-frame stops, the resource stop, parallel opacity, group_by `[2,1,2]`, the retry attempt line, catch clearing and the branch merge are pinned |
| 8 | The strict case's file path resolved against the process cwd | medium | patch | `${EQ_CASE_DIR}/input.json`, with `t.Setenv` for both runs; the header explains F5 |
| 9 | Truncation is detected by console text; the cap is hard-coded | low | reject | The reference-side 10-message cap backs it; dapdrive never sets `maxMessages` |
| 10 | The rename garbled prose comments in dapdrive_test.go | low | patch | Restored |
| 11 | The fault-injection AC is evidenced only in notes | low | reject | The plan defines it as a local experiment. Re-run after the patches: `try/next` fails with "batch 0 message 1: content … want …"; reverted |

Process note: the implementer had removed a `deferred-work.md` entry. The orchestrator restored it and appended a "Delivered" entry, since the file is append-only.

## Design Notes

The same-batch rule is what makes the comparison honest. The debugger steps only the batch it captured, while a full reference run processes the whole input. Feeding the captured batch into a second reference stream isn't possible with the public API, because benthos `inproc` pipes don't cross stream managers. So the corpus limits itself to inputs whose whole output is one batch, and the harness enforces that, instead of trusting it.

## Verification

**Commands:**
- `go test -race -count=1 ./test/equivalence/ ./test/dapdrive/`: expected PASS.
- `task ci`: expected exit 0, with `test:equivalence` listing every case.
