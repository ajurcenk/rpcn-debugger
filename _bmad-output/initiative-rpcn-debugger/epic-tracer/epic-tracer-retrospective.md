---
epic: epic-tracer
date: 2026-10-22
verdict: accepted-with-open-items
criteria: declared
headless: false
---

# Retrospective: epic-tracer (E2), "Step a captured batch through a pipeline in VSCode"

## Epic summary

- **Epic:** `_bmad-output/initiative-rpcn-debugger/epic-tracer/` (id 2). It covers CAP-1, CAP-3, CAP-4, CAP-11 and CAP-12, and its Done when has 6 items.
- **Tickets:** 14, all `done`, none still `built`. `pending_tickets` is empty. The build order (by plan baseline) was 2.1, 2.2, 2.5, 2.3, 2.6, 2.4, 2.13, 2.7, 2.8, 2.12, 2.9, 2.14, 2.10, 2.11.
  - 2.12, 2.13 and 2.14 were added mid-epic from review findings, by user decisions recorded in the epic Notes.
- **Ranges:** each plan's `baseline_revision`, up to the next plan's baseline. The last range (2.11) ends at the close commit `3ab2d74`, an inferred cut (no later work exists).

  | Ticket | Range | Commits | Prod Go net | Test net |
  |---|---|---|---|---|
  | 2.1 | 1926a78..1399b01 | 2 | 1145 | 1563 |
  | 2.2 | 1399b01..b4e9c33 | 2 | 964 | 836 |
  | 2.5 | b4e9c33..61ffd0a | 2 | 684 | 1481 |
  | 2.3 | 61ffd0a..b861d14 | 2 | 675 | 588 |
  | 2.6 | b861d14..1446496 | 2 | 539 | 950 |
  | 2.4 | 1446496..16d518b | 2 | 607 | 816 |
  | 2.13 | 16d518b..a01020c | 2 | 167 | 246 |
  | 2.7 | a01020c..5231a10 | 2 | 1074 | 1392 |
  | 2.8 | 5231a10..fce1700 | 2 | 293 | 767 |
  | 2.12 | fce1700..2d287b8 | 2 | 389 | 444 |
  | 2.9 | 2d287b8..f2e77f9 | 2 | 401 | 927 |
  | 2.14 | f2e77f9..0d6b3c8 | 3 | 31 | 59 |
  | 2.10 | 0d6b3c8..2ef8e0d | 2 | 39 | 0 |
  | 2.11 | 2ef8e0d..3ab2d74 | 2 | 0 | 105 |

  Whole epic: `1926a78..3ab2d74`, 29 commits, 0 merges (merge_count 0, merges_measured 0). Evidence JSON came from `git_evidence.py`, one run per range.
- **Evidence inventory:**
  - **Available:**
    - the epic file;
    - `tickets.toml`;
    - 14 plans, each with a Review Triage Log;
    - `deferred-work.md`;
    - the git history;
    - the initiative file;
    - the spec (CAP ids);
    - the architecture spine;
    - this session's conversation transcript (the build sessions ran as subagents of one orchestrating session).
  - **Missing:**
    - no story files (no ticket was pulled);
    - no previous retrospective (epic-platform-baseline has none);
    - the subagent transcripts were not read individually, so process lessons rest on the plans, the triage logs, the commits and the orchestrating session's record.

## Findings

Each finding carries its source. **[verified]** means the orchestrator re-checked it against the primary source; the rest are sub-agent reports with cited sources.

### Cross-ticket seams (defects no single-ticket review could see)

| # | Finding | Source / reproduction | Seam | Disposition |
|---|---|---|---|---|
| S1 | **A `processor_resources` entry referenced from the input tree (`input.broker.inputs[].processors`, `input_resources`) is stepped during capture.** The entry stop fires inside the capture stream before `Captured`, and it re-steps for re-sent, never-captured messages. The launch warning promises "runs during capture, not stepped" | `engine/capture.go` `configureCapture` adds the wrapped `Resources`. Repro `scratchpad/retro/e1_input_resource.yaml`: first stop `entry line=17 processor_resources.r.branch.processors[0]` **[verified]** | 2.13/2.5 × 2.1/2.6 × 2.3 | fix now (medium) |
| S2 | **An interpolated `error_handling.strict` resolving to true delivers what production rejects (AD-8).** The steppers skip, but the step stream isn't strict, so the failed message reaches the sink | `rewrite.go` `strict()` gives `Plan.Strict=false` for an interpolation; `engine.go` `SetStrict(r.conf.Strict)`. Repro `e2_interp_strict.yaml` with `STRICT=true`: delivered `{"a":1}` with error "boom" **[verified]**. The literal form (`e2b_strict.yaml`) delivers nothing | 2.5 × 2.4 × 2.8 × 2.14 | fix now (medium) |
| S3 | Variables hides batches past 64 and messages past 1024 after processors fan out (`unarchive`, `split`), with no notice. The `maxMessages` cap of 1024 assumes the captured size bounds every stop | `dap/variables.go` `encodeRef`; repro `e10_many.yaml` (1100 messages; 110 batches, 64–109 unexpandable) | 2.3 × 2.6 × 2.5/2.7 | fix (low-medium): show a "not expandable" hint or widen the reference |
| S4 | A lint error at the end of a `\|` Bloblang block is put on the following node (wrong line and path) or comes back unmapped | `rewrite/positions.go` `FilePosition`; repros `e4`/`e5`/`e6_lint_*.yaml` | 2.12 × 2.5/2.13 | fix (low): clamp to the block's last line |
| S5 | Lint errors in a tainted, unwrapped resource map only if it is a one-liner (a special case of S4); the only resource lint test is single-line | `e3_lint_tainted.yaml`; `engine/positions_test.go` "bad resource" | 2.12 × 2.5 | fix with S4 |
| S6 | `Start` re-reads and re-rewrites the pipeline instead of reusing Check's Configs. An edit between launch and configurationDone desyncs ids from the LineMap and Translator | `engine.go` `Start` calls `a.load` again; `session.go` keeps Check's maps | 2.1 × 2.5 × 2.6/2.4 | fix (low, latent; the user edits samples while testing) |
| S7 | Nested repeat parents lose the outer index in the skip text ("Skipped message 1 of 1" for try inside for_each) | `e8_combo.yaml` | 2.7 | accept (cosmetic; the stack shows the outer label) |

Seams checked clean: a combined for_each > try > resource > batching > strict pipeline (frames, labels, Skip, STRICT_REJECTED, equivalence for the non-strict variant); log translation after the 2.10 table; core.Error codes engine → Session → DAP; per-stop DAP ids with delivery and labels; the Arriver port across every implementation; Pause or terminate during capture; the 2.13 ref and taint walks over the shared `childLists`; the extension fill and schema against `LaunchConfig`.

### Spec-to-implementation reconciliation

- **Done when 1–6 are met at HEAD.** 4 and 5 are met under narrowed readings (see Acceptance verdict).
- **R1. No E2 decision was reconciled into the architecture spine.** It still says `updated: '2026-10-14'`, and `.memlog.md` ends with spike 1.6's entries **[verified]**. Every deviation below lives only in the epic Notes, plan notes or deferred-work.md. Disposition: reconcile the spec (action item A6).
- **Spine wording the as-built has outgrown** (reconcile spec; each is recorded in a plan, but none in the spine):
  - **AD-3:** `core.Error.Inline` (2.12); `STRICT_REJECTED` is an engine-to-Session outcome, not a failure (2.14); `MessagesSkipped`/`AttemptStarted` gained fields (2.7); batch numbering counted in both Session and DAP (2.8, against AD-3's state rule).
  - **AD-6:** interpolated `retry.parallel` is opaque; resources under *unknown* parents stay unwrapped; undefined `resource:` refs are rejected (2.13); the interpolated-strict handling (2.5).
  - **AD-6/AD-21 composite outputs:** their per-case processors are **not run** in dry-run (warning "is not run in dry-run", `rewrite.go:180` **[verified]**). AD-6 says they "run as one opaque step". That changes delivered output for broker or switch outputs with processors. Decide (A7).
  - **AD-7:** `catch` steppers are written `strict: false` (2.7). Add a column.
  - **AD-8:** the time-budget warning fires only inside `retry` (`session.go:734-742` **[verified]**); AD-8 also lists branch/workflow timeouts and rate limits. Decide (A7). The corpus layout is `{pipeline.yaml, expect.yaml, input.json}` with the same-batch rule (user decision 2.9), not `{input.snapshot.json, expected.snapshot.json}`.
  - **AD-10:** Step Over at the for_each message end leaves the for_each; the top-level "parent's next stop" is the next top-level sibling (2.6).
  - **AD-11 / CAP-4:** "internal" is a child node under metadata, not a DAP scope.
  - **AD-12:** the extension also fills `type`/`request`/`name` for F5 with no launch.json (2.1); `contributes.breakpoints` for yaml (2.2); a custom **event** `rpcnDelivered` (AD-12 allows custom requests) (2.8).
  - **AD-15:** two logger instances; DAP gets Warn and above; the Session's own Output events bypass slog and never reach stderr (2.4 decision).
  - **AD-17:** the Session's `Enter`, not the stepper, calls Fork/Exit; the root thread starts at ConfigurationDone.
  - **AD-22:** the dotenv grammar (2.4). License is not implemented (owned by epic-local-release; `LICENSE_REQUIRED` is defined, never raised **[verified]**).
  - **Unspecified limits:** `maxMessages` ≤ 1024; DAP refs of 32 frames / 64 batches / 64 stops (2.6).
- **CAP status:** CAP-4 is met. CAP-1, CAP-3, CAP-11 and CAP-12 are partially met, the rest owned by later epics (RPC plugins and user debug main → local-release; concurrent rows → concurrency; `output: real` → safe-against-real-systems).
- **Deferred items without a clear owner:** retry/while label inference (no owner); the strict Warn cosmetics (owner vague). The `allThreadsContinued` item and the Restart counter reset aren't named in their owner epics' Done when; the mixed-batch strict risk isn't named in epic-safe's file. Disposition: A8.

### Aggregate views

- **Architecture delta:**
  - Production layering is clean: core is stdlib-only; dap → core plus go-dap; engine and rewrite don't import each other; yaml.v3 only in rewrite (and test/equivalence); go-dap only in dap; no cycles.
  - **But nothing enforces it except AD-2's depguard rule** (`.golangci.yml`). There are no depguard rules for core, dap, yaml.v3 or go-dap, and no arch test. Disposition: A12.
  - `test/dapdrive` calls `rewrite.Rewrite` to rebuild stop paths (`dapdrive.go:575`), so the driver's expectations depend on the code under test. Accept: 2.9's `expect.yaml` now declares the paths independently.
- **God-class growth:**
  - `debugger/core/session.go` is 887 lines, touched by 9 tickets, with 33 fields, 28 lock sites and 13 responsibilities (dispatch, launch/env I/O, run lifecycle and messages, delivery, log relay, threads, frames/sites, skips, stop predicate, snapshots, budget warning, resume/terminate).
  - `dap/server.go` is 601 lines, with presentation text mixed into the server.
  - `rewrite/walk.go` and `rewrite.go` (≈150-line `Rewrite`) also have separable parts.
  - Disposition: defer to a split in the next refactor sweep (A13).
- **Duplication map:**
  - The stepper config contract (name, modes, repeat kinds, field names) is duplicated as literals in `rewrite/walk.go` and `engine/stepper.go`, not shared through core like `core.StepperPart`.
  - The content/metadata display rule exists in 4 places (dap variables, dap console, dapdrive, serve_test).
  - The same YAML error regex is in rewrite and engine.
  - Translation happens in 4 places across 3 packages (AD-6 says one handler).
  - The sink and the reference collector are near-identical.
  - The pipeline is read and rewritten twice per session (S6).
  - Six fake engines and five DAP test clients across packages.
  - Disposition: defer the contract sharing and display helper to A13; accept the test-fake duplication.
- **Pattern divergence:**
  - The engine returns mostly plain `fmt.Errorf`, which the Session classifies afterwards; rewrite returns only `core.Error`.
  - `CodeRunFailed` is used only as the DAP fallback; `CodeLicenseRequired` is unused.
  - Warnings reach the user by two routes (core Output vs slog).
  - Disposition: accept, and fold into A13 where cheap.

### Process lessons

- **Review yield.** 75 triage rows across 13 quick reviews (2.11 had none): high 1 (2.5 merge keys dropping processors), medium 22, low 49, false 1; 56 patched, 9 deferred, 6 rejected. Every feature ticket's review found real behaviour bugs.
  - Recurring categories:
    - test-harness or verification weakness (~14);
    - translation, paths and positions (~11);
    - incomplete structural walks (8);
    - YAML edge cases: alias, null, merge keys, interpolation, re-quoting (7);
    - docs accuracy (8);
    - labels (5);
    - lifecycle and timing (5).
  - Source: the 14 plans' Review Triage Logs.
  - Lesson: the quick lens is worth its cost. YAML edge cases and translation recur, so E3 plans should name them up front.
- **Plan size.** Every full-route plan ran ~2,000–2,950 tokens (1.2–1.8× the 1,600 guideline); only the oneshots and the sweep were under. The sizes came down after 2.7. Plan facts were wrong in 2.12 (a `count: x` lint row), 2.9 (`generate` nack replay; the import cycle), 2.6 (the literal stop rule against its own matrix) and 2.7 (an unverifiable equivalence boundary). Lesson: probe engine behaviour while planning, not only while implementing. Decide whether ~2,500 is the house norm or entries should be sliced smaller (A9).
- **Scope growth.** 2.12 and 2.13 came from the 2.4 review, and 2.14 from a 2.9 implementation observation. All three were user decisions recorded in the epic Notes. Lesson: reviews surface whole entries' worth of scope. Keep the "new entry, not sweep" rule (it worked).
- **Process slips:**
  - commit 5231a10's `git commit -a` swept in a user test edit and was pushed, so `task ci` wasn't run after the bookkeeping commit (fixed: memory note `commit-explicit-paths`);
  - a subagent deleted an append-only `deferred-work.md` entry (2.9; restored; only a plan rule records it);
  - Docker was down during 2.14 (no preflight);
  - the 2.9 triage header's counts don't match its table.

  None has a mechanical gate. Disposition: A10, A11.
- **Human in the loop.**
  - The user ran VS Code checks on 10 of 14 tickets (only 2.1 and 2.11 were hitl).
  - Only the 2.2 check found something the tests missed (the YAML `contributes.breakpoints`).
  - 2.6 was first presented without its manual check, and the review flagged that.
  - Lesson: keep the manual check for UI-facing ACs; for engine-only entries the scripted DAP driver now covers most of what manual checks found.
- **What went well:**
  - equivalence fault injection caught broken `try`/strict modes (2.9);
  - the same-batch rule caught a real reference mismatch;
  - the strict corpus case surfaced the 2.14 UX bug;
  - the 2.10 refactor kept every golden byte-identical;
  - reviewers reproduced bugs before routing them;
  - engine formats were probed before being trusted (2.4, 2.12);
  - the deferred-work loop closed with "Delivered" entries;
  - oneshot kept the small fixes cheap;
  - the history is clean (no reverts).

## Behavior verification

- The user ran the 2.11 acceptance checklist in VS Code with the installed `.vsix` on 2026-10-22, using `testdata/tracer/acceptance.yaml` (every in-scope parent, metadata, batching) and `acceptance-strict.yaml`. Every item passed.
- In this retro, the orchestrator ran the full DAP flow through `test/dapdrive` against the real engine for S1 and S2 (both reproduced, as above). The seam sub-agent ran 12 further scripted sessions (`scratchpad/retro/e*.yaml`).
- `go test ./debugger/... ./test/equivalence/ ./test/dapdrive/ ./cmd/...` passed at HEAD (re-run by the spec sub-agent). `task ci` passed at 2.10 with Docker.

## Previous-retro follow-through

There is nothing to follow through on: no previous retrospective file exists. `epic-platform-baseline/` has no `epic-platform-baseline-retrospective.md`.

## Action items

These are proposed, not applied. "New entry" means story-shaped work for `bmad-ticket` or `bmad-build`.

| # | Action | Kind | Owner | From |
|---|---|---|---|---|
| A1 | Resolve an interpolated `error_handling.strict` through the session's env lookup before `SetStrict` and the `STRICT_REJECTED` gate, so the step stream and output match production; add an interpolated-strict equivalence case | remediation, new bug entry | dev loop (bmad-build), **before E3** (user decision 2026-10-22) | S2 |
| A2 | Stop stepping resources inside the capture stream: give the capture stream the unwrapped resources (or a no-stop arriver) so input-tree processors run unstepped as the warning says; add a golden plus a dapdrive case | remediation, new bug entry | dev loop, **before E3** (user decision 2026-10-22) | S1 |
| A3 | Clamp lint positions just past a `\|` block's last line to that line (fixes S4 and S5); add multi-line resource lint tests | remediation, small entry | dev loop | S4, S5 |
| A4 | Variables: show "N more batches/messages not expandable (limit 64/1024)" instead of silent leaves, or widen the reference encoding | remediation, small entry | dev loop | S3 |
| A5 | Have Check hand its Configs (rewrite, LineMap, Translator) to Start, so the file is read once per launch | remediation, small entry | dev loop | S6 |
| A6 | Reconcile the architecture spine with E2's decisions and deviations (the R1 list) through `bmad-architecture` Update, then refresh the spec companion if needed | spec reconciliation | user plus bmad-architecture | R1 |
| A7 | **Decided 2026-10-22.** (a) Keep composite-output processors not run in dry-run, with the warning; amend AD-6 at A6 (dry-run delivers what reaches `output`, before per-output processing; `output: real` in E3 runs them). Append a deferred-work candidate, no owner: step `fan_out`/`switch` output cases in dry-run, one sink batch per case. (b) Implement the AD-8 time-budget warning for branch/workflow timeouts and rate limits too (AD-8 stays as written) | (a) spec reconciliation + deferred-work; (b) new entry | (a) A6; (b) dev loop, with A1/A2 before E3 | R1 |
| A8 | Give owners to the orphaned deferred items: the retry/while label inference (accept as a known limit, or upstream); the strict Warn cosmetics (pick an epic). Name `allThreadsContinued` in epic-concurrency's Done when, the Restart counter reset in epic-edit-restart-replay, and the mixed-batch strict risk in epic-safe-against-real-systems | backlog hygiene | user plus bmad-ticket (E3 inception covers its part) | spec reconciliation §5 |
| A9 | Decide plan sizing for E3: slice entries so plans land near 1,600 tokens, or set ~2,500 as the house norm. Plans should probe engine behaviour for the cases their matrix names | process | user | Process lessons |
| A10 | Make two rules durable beyond memory: "stage explicit paths, never `commit -a`" and "`deferred-work.md` is append-only". Options: a CLAUDE.md rule (needs approval) or a pre-commit check that `deferred-work.md` only grows | process | user | Process lessons |
| A11 | Make `test:integration` fail fast with a clear "Docker is not running" message (a preflight) | process, tiny entry | dev loop | Process lessons |
| A12 | Enforce the layering mechanically: depguard rules for core stdlib-only, dap → core only, yaml.v3 only in rewrite/test, go-dap only in dap | process/gate, small entry | dev loop | Aggregate views |
| A13 | Next refactor sweep: split `core/session.go` along its seams (launch/env, run lifecycle and messages, threads/frames, stepping); share the stepper config contract via core; one content/metadata display helper | tech debt, sweep scope | E3's refactor sweep | Aggregate views |

## Acceptance verdict

**Machine verdict: accepted-with-open-items** (criteria declared, Done when 1–6). `pending_tickets` is empty.

Evidence:
1. **Met:** capture, stepping, labels, Variables and strict are covered by `TestParents`, `TestServeNestedStepping`, `TestServePause`, `TestEquivalence/strict`, and the user's 2.11 checklist.
2. **Met:** delivery and the batching warning (`TestDeliveredGroupBy`, `TestBatchingWarning`, the 2.11 checklist).
3. **Met:** the protocol set, the pinned registry 1001–1010, the Snapshot, `lint:schema` in ci, and unverified breakpoints.
4. **Met, with a narrowing:** the time-budget warning covers `retry` only (A7b).
5. **Met as reinterpreted:** single-batch corpus (the user's 2.9 decision); CTX_LOST is proven by the ctx checks in every case, not by dedicated case files.
6. **Met:** pushed `3ab2d74`, `task ci` green, the 2.11 checklist from the `.vsix`.

Open items that don't block acceptance but stay tracked: S1 and S2 (medium defects at ticket seams, found only by this retro), S3–S6, and R1 (spine not reconciled).

**Human decision: accepted** by the user on 2026-10-22 (accepted-with-open-items confirmed).

## Open questions

Answered by the user on 2026-10-22:

- **A7(a), composite-output processors:** keep "not run in dry-run" with the warning, and amend AD-6. Routing for round_robin, greedy and fallback can't be reproduced without the real output; `output: real` covers it. Stepping `fan_out`/`switch` cases goes to deferred-work as a candidate.
- **A7(b), time budget:** show the warning for branch/workflow timeouts and rate limits as well, as AD-8 says.
- **A1/A2:** fix S1 and S2 as bugs before E3 starts.

None remain open.
