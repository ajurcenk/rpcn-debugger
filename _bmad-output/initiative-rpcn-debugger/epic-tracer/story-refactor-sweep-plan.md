---
title: 'Refactor sweep'
type: 'refactor'
ticket: '10'
created: '2026-10-22'
status: 'built'
baseline_revision: '0d6b3c85499a50fec22c6e418375e97cf86ea53a'
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

**Problem:** The E2 entries left one structural duplication and stale docs:
- The AD-7 child-list table (branch/while/retry → `processors`; try/catch/for_each → the list itself; switch/group_by → `cases[].processors`) is hand-copied in four walks in `debugger/rewrite`: `walker.processor`, `childLists`, `refChecker.processor` and the taint scan. A future AD-7 row added to one walk and missed by the others would silently drop stepping, resource checks or taint. The 2.13 review found the copies already drifting.
- CLAUDE.md's Project state and Layout describe the code as of 2.1–2.2. Agents start from it.
- Several `deferred-work.md` items were delivered by later entries, but the file still lists them as open.

**Approach:** Cleanup only, with no behaviour change:
- One table in `debugger/rewrite` gives, per AD-7 parent kind, its child lists with their user-path suffixes. All four walks read it.
- CLAUDE.md's Project state and Layout are refreshed to the current code.
- "Delivered" entries are appended to `deferred-work.md` for the items later entries closed.

</frozen-after-approval>

## Boundaries & Constraints

**Always:**
- No behaviour change:
  - every rewrite golden (`debugger/rewrite/testdata/*.golden`) is byte-identical before and after;
  - every test passes unchanged, except tests that call removed private helpers directly;
  - the equivalence corpus is green.
- The table owns: the parent kind, the mode (`all`/`try`), the repeat kind (2.7) and the child lists with their path suffixes (`.processors`, the list itself, `[i].processors`). Each walk derives its iteration from it. The classification (`classify`: leaf/parent/concurrent/unknown) stays one function.
- `deferred-work.md` is append-only. Add one "Delivered" entry per item closed by a later entry:
  - the CLAUDE.md update (1.7);
  - the Serve log destination (2.4 `WithLogOutput`);
  - YAML aliases (2.5);
  - `core.Error` Line/UserPath (2.12);
  - the strict reject message (2.14).

  Each names the entry that delivered it. Items owned by other epics stay as they are.
- CLAUDE.md:
  - Edit only the Project state and Layout parts: what the tracer does now (capture, stepping with labels, delivery, equivalence), the packages, `test/dapdrive`, the corpus rule (single batch, at most 10 messages, `expect.yaml`), and `testdata/tracer/`.
  - Commands and Rules stay as they are unless they're wrong. The BMAD skills section stays untouched.
  - The user approved CLAUDE.md upkeep in sweeps (1.7).

**Never:**
- No new features, and no fixes for deferred items owned by other epics. Scope found here that isn't cleanup becomes a new entry.

## Code Map

- `debugger/rewrite/walk.go`:
  - `walker.processor` (switch on `branch`/`while`/`retry` → `w.inner`; `try`/`catch`/`for_each` → `w.list`; `switch`/`group_by` → per-case `w.inner`);
  - `childLists(name, v)`;
  - the taint scan (`taintedResources`, `scanList`, `scanProcessor`, `refs`);
  - `classify` and the repeat/mode assignment from 2.7.
- `debugger/rewrite/refs.go` `refChecker.processor` (2.13): the same switch, with user paths.
- Goldens: `debugger/rewrite/testdata/` (about 40 cases). `go test ./debugger/rewrite/ -run TestGolden` without `-update` must pass.
- `CLAUDE.md` Project state and Layout (lines 5–16).
- `_bmad-output/initiative-rpcn-debugger/deferred-work.md`: append only.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/walk.go`, `refs.go`: one AD-7 parent table plus a child-list iterator yielding `(list node, user-path suffix, case index)`. The four walks use it, and the duplicate switch code is removed.
- [x] `CLAUDE.md`: refresh Project state and Layout.
- [x] `_bmad-output/initiative-rpcn-debugger/deferred-work.md`: the five "Delivered" entries.

**Acceptance Criteria:**
- Given the refactor, when `go test ./debugger/rewrite/` runs without `-update`, then every golden passes byte-identical, and `task ci` exits 0 with the equivalence corpus green.
- Given `debugger/rewrite`, when searching for the AD-7 parent names (`"branch"`, `"for_each"`, `"group_by"` …) in switch cases, then they appear in the one table (and `classify`), not in four walks.

## Plan Change Log

## Implementation Notes

- `walk.go` now holds `parents`, the one AD-7 table: per parent name, its `shape` (`ownList`: the config is the list; `inner`: its `processors` field; `cases`: each element's `processors` field) and its stepper `kind` (mode, repeat, noStrict). `parentKind` reads it (adding retry's `max_attempts` from `max_retries`), and `classify` keeps only the concurrent checks (`retry` with `parallel`, `workflow`, `parallel`) before a table lookup.
- `childLists(p, i)` is an `iter.Seq[childList]` over the child lists of the parent at key index `i` of processor mapping `p`. Each item carries the list node, its slot in the config, the user-path and engine-path suffixes (`""`, `.processors`, `[c].processors` / `.c.processors`; the case index is in the suffix), the end-of-entry key line, and, for a holder of the wrong kind, `bad`/`want`. `walker.processor` turns `bad` into the same PIPELINE_INVALID error as before and stops; `refChecker.processor` and the taint scan (`scanProcessor`) skip it and go on, as their lenient copies did. `walker.inner` and the old `childLists` are removed.
- One theoretical difference: a parent mapping with duplicate `processors` keys (yaml.v3 node parsing accepts them; the engine's config decode rejects them) was wrapped for every key by the walker but scanned for only the first by the resource check and taint scan. All walks now see every key, which can only add a resource check or taint in a config the engine rejects anyway.
- The 2.13 deferred item for this table is closed by this entry itself; the plan names five "Delivered" entries for items closed by earlier entries, so it gets none.
- CLAUDE.md: also added the existing `gen:schema` and `lint:schema` tasks to Commands (they were missing, and `task ci` runs `lint:schema`).

- Orchestrator: a sixth "Delivered" entry was appended for the 2.13 AD-7 table item, which this entry itself closes, so the file shows it closed. This supersedes the note above that it gets none; it's beyond the five the plan lists, and harmless.

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 4, false 0, maybe-false 0. All four were patched by the orchestrator. The reviewer found no hidden behaviour change in the walker, refChecker or taint refactor beyond the documented duplicate-`processors`-key case.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | Six "Delivered" entries against the plan's five, contradicting the Implementation Notes | low | patch | Implementation Notes now record the deliberate sixth entry (the 2.13 table item this entry closes) |
| 2 | CLAUDE.md says TestEquivalence compares against `Case.RunReference` | low | patch | It uses `Case.Reference` (plus input batches for the same-batch rule); reworded |
| 3 | CLAUDE.md places threads in `frames.go` | low | patch | Threads live in `session.go`; frames and entry marks in `frames.go`; reworded |
| 4 | The `childList` doc says the check and taint scan "skip" a bad holder, but they rely on nil-list guards | low | patch | The `bad` field doc states the contract: check `bad` or tolerate a nil list |

## Verification

**Commands:**
- `go test -count=1 ./debugger/rewrite/`: expected PASS, with no golden diff (`git status debugger/rewrite/testdata` clean).
- `task ci`: expected exit 0.

**Results (2026-10-22):**
- `go test -count=1 ./debugger/rewrite/`: PASS; `git status debugger/rewrite/testdata` clean (no golden changed, none regenerated).
- `task ci`: exit 0 (lint 0 issues, unit, equivalence `TestCases`/`TestEquivalence` green, integration, extension build/test/package).
