---
title: 'Refactor sweep'
type: 'refactor'
ticket: '10'
created: '2026-10-30'
status: 'built'
baseline_revision: '73cfe8b3b9bb1d2516ce3a3426be7f309005d69c'
route: 'full'
route_source: 'auto'
risk: 'low'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** E3 entries 1–9 left stale docs, open bookkeeping and duplicated walks:
- CLAUDE.md's Project state describes E2 only, and its Layout lacks every file E3 added.
- `deferred-work.md` still lists items that later entries closed: the 3.7 producer leak, the Session-owned batch number, and the strict decision by text (all 3.8).
- Benthos's strict-mode Warn reaches the Debug Console naming the internal `rpcn_debug_sink`.
- The README doesn't say that a killed session makes the next Kafka capture wait up to ~60 s.
- `debugger/rewrite` has two forward fixed points over `processor_resources` (`taintedResources`, effects' `reached`) and seven hand-written resource-label lookups.

**Approach:** cleanup with no change to stepping, configs or goldens.
- CLAUDE.md Project state and Layout refreshed to the code as of 3.9.
- `deferred-work.md`: append-only "Delivered" entries for the three 3.8 items.
- README: the stale-member wait, in the debug-group paragraph.
- Strict Warn (user decision 2026-10-30): the AD-15 handler keeps benthos's strict-mode rejection record ("Rejecting N of M message(s) for output 'rpcn_debug_sink' …") off the Debug Console; stderr still gets it. The Session's strict note stays the one console line. Then mark the deferred item Delivered.
- Code scope (user decision 2026-10-30, small helpers): one forward-closure function used by `taintedResources` and effects' `reached`, with the reference finder, filters and the multilevel hook passed by each caller; one resource-label helper for the hand-written `label` lookups; `walker.resources` uses `resourcePath`. No shared processor-list finder.

</frozen-after-approval>

## Boundaries & Constraints

**Always:**
- Every rewrite golden byte-identical; every test passes unchanged except tests of removed private helpers; equivalence corpus green.
- `deferred-work.md` append-only; each Delivered entry names the entry that delivered it.
- CLAUDE.md: only Project state and Layout (Commands/Rules only if wrong; BMAD section untouched).

**Never:**
- No new features, launch keys or warnings. No change to which references or lists a walk counts: the per-site semantics are deliberate (refs.go counts processor `resource` only; effects counts `resource`/`cache`/`rate_limit`; refs.go accepts interpolated labels).
- No retro A13 (session.go split, stepper contract in core); left out of E3 by the user.

## Code Map

- `CLAUDE.md` lines 5–16 (Project state) and Layout. Add: `core/debuggroup.go`, `core/inputs.go` (`FixedInputs`), `core/output.go` (`OutputMode`, `Delivered`/`Outcome`); `rewrite.Options`/`Plan.Options`, `Rewrite(src, opts)`, `CheckResult.Prepared`; rewrite `inputs.go`/`inputwalk.go` (input table, group override), `output.go` (sink YAML, `output: real`), `effects.go` (processor effects, state split), `state.go` (stateful try/catch, buffer), the `topLevel` allowlist in `rewrite.go`; `dapdrive.Options.AfterLaunch`; `test/integration/neverack_test.go`, `output_test.go`; `testdata/tracer/kafka.yaml`, `acceptance*.yaml`. Project state: never-ack Kafka capture under the debug group, input classes and refusals (1011/1012), `output` dry-run/real/none, Delivered outcomes (1013), launch warnings.
- `_bmad-output/initiative-rpcn-debugger/deferred-work.md` lines 49–51, 85–87, 133–134: the three items to mark Delivered.
- `README.md` line 90: the debug-group paragraph.
- `debugger/logging.go` `translateRecord`: where engine text is translated; the strict Warn arrives with `path=root.output`, message starting "Rejecting N of M message(s) for output 'rpcn_debug_sink'". Its embedded `(line N)` belongs to the failing processor, not the output, so it can't be shifted from the record.
- `debugger/rewrite`: `walk.go:taintedResources` (415) with `refs` (477); `effects.go:reached` (~439) with `labelRefs` (412); label lookups at `state.go:29`, `inputwalk.go:64`, `effects.go:522,548`, `walk.go:366`, `refs.go:27,58`; `walker.resources` re-implements `refs.go:resourcePath`.

## Tasks & Acceptance

**Execution:**
- [x] `CLAUDE.md` -- refresh Project state and Layout -- agents start from it.
- [x] `_bmad-output/initiative-rpcn-debugger/deferred-work.md` -- three Delivered entries -- bookkeeping.
- [x] `README.md` -- stale-member wait sentence -- deferred docs item.
- [x] `debugger/logging.go` -- skip the frontend (not stderr) for the strict rejection record, matched narrowly (WARN, message prefix "Rejecting " and "for output 'rpcn_debug_sink'"); test in `debugger/logging_test.go` that stderr keeps it and the Session gets no Log -- strict Warn decision.
- [x] `debugger/rewrite/walk.go`, `effects.go`, and the label-lookup sites -- the closure and label helpers -- code scope decision.

**Acceptance Criteria:**
- Given the strict corpus case under the debugger, when it runs, then the Debug Console has the Session's strict note and no "Rejecting … 'rpcn_debug_sink'" line, and stderr still has it.
- Given the sweep, when `go test ./debugger/rewrite/` runs without `-update`, then every golden passes byte-identical, and `task ci` exits 0 with the corpus green.

## Implementation Notes

- Strict Warn: `strictRejection` in `debugger/logging.go` matches the raw record (level exactly WARN, prefix "Rejecting ", contains "for output 'rpcn_debug_sink'"); only the frontend send is skipped. Beside the unit test, `test/equivalence` `checkRun` now fails a run whose Debug Console shows that record, and one that shows the strict note while the log lacks the record (fails without the logging change on the strict cases).
- Closure: `closure(bodies, seed, find, also)` in `refs.go`; `taintedResources` passes `refs` (now callback-based, as are `scanList`/`scanProcessor`) and no hook; `reached` passes `labelRefs` and the multilevel-levels hook. `resourceLabel(e)` returns the scalar label node as written (empty or interpolated included); each site keeps its own extra checks (`!= ""`, `!isInterpolated`). `walker.resources` takes its path from `resourcePath`.
- deferred-work.md: besides the three 3.8 items and the strict Warn, Delivered entries for the README half of the stale-member item (the wait note half left open) and for the two CLAUDE.md Layout items from 3.1/3.2, which this sweep delivers.

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 1, false 0, maybe-false 0. Both patched by the implementer.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | AD-15 still says every engine record at Warn and above goes to DAP Output; the strict-record filter contradicts the binding rule, and no memlog entry records the decision | medium | patch | AD-15 names the one exception (benthos's strict rejection record for `rpcn_debug_sink` stays on stderr; user decision 2026-10-30); memlog entry 83; spine lint clean |
| 2 | The new stale-member deferred entry starts "Delivered —" though its wait-note half is still open | low | patch | The entry now says "Partly delivered —"; a new open entry carries the wait-note half |

## Verification

**Commands:**
- `go test -count=1 ./debugger/... ./test/equivalence/` -- expected: PASS without `-update`.
- `git diff --stat debugger/rewrite/testdata` -- expected: empty.
- `task ci` -- expected: exit 0.

Run 2026-10-30 by the orchestrator after the review patches: `task ci` exit 0; `git diff --stat debugger/rewrite/testdata` empty.
