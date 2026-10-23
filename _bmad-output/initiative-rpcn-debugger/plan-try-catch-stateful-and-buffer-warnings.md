---
title: 'Launch warnings for stateful try/catch children and a dropped buffer'
type: 'feature'
ticket: ''
created: '2026-10-22'
status: 'built'
baseline_revision: '9215d935e5104bacb32d3173386d4f964bc2389a'
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

**Problem:** Two places where the debugger's output can differ from production with no notice. Both are user decisions of 2026-10-22, from the A6 spine gate; see the spine's AD-5, AD-7 and AD-8 and the two entries in `deferred-work.md`.

1. The engine runs `try` and `catch` processor-major: each child over every message, then the next child (benthos `internal/component/processor/execute.go`, `ExecuteTryAll`/`ExecuteCatchAll`). The debugger steps them message-major: one message through every child. That's the same unless children share state across messages. For example, child A writes a cache key and child B reads it: under processor-major, B on message 1 already sees message 2's write.
2. The rewrite drops the top-level `buffer` section silently. A buffer can re-batch, so production batch boundaries may differ.

**Approach:** Two launch warnings from `debugger/rewrite`, in the style of the existing output-batching warning, and no change to stepping.

- **Stateful `try`/`catch`:** a `try` or `catch` list with **two or more** children, at least one of which is **stateful**, gets one warning at the list's key line. It says the engine runs the children over every message one child at a time, while the debugger steps one message through all of them, so children that share state across messages may give different output.
  - **Stateful** means a closed table in rewrite: `cache`, `cached`, `dedupe`, `redis`, `redis_script`, `sql_raw`, `sql_insert` and `sql_select`.
  - A child is also stateful when it is a `resource:` whose `processor_resources` entry is stateful, or a parent whose child lists, at any depth, hold a stateful child. Guard against reference cycles.
  - `rate_limit` is left out: it changes only timing, not output (AD-8, as narrowed on 2026-10-22).
  - A single child is never warned about, because the two orders are then the same.
- **`buffer`:** a top-level `buffer` that is set (not null and not `none`) gets one warning at its key line. It says the buffer is dropped, so the captured batch goes straight to the processors and production batch boundaries may differ.
- **Spine:** drop `rate_limit` from AD-7's example list, and add "Delivered" entries to `deferred-work.md` for both items.

</frozen-after-approval>

## Code Map

- `debugger/rewrite/rewrite.go` `Rewrite`: the top-level flow. The warnings are emitted after the wrap, in rewrite order, like `warnNestedLists`/`warnBatching` (~176–190). The `buffer` check goes here, reading `field(root, "buffer")`.
- `debugger/rewrite/walk.go`:
  - `parents` (the AD-7 table: `try` and `catch` are `ownList`);
  - `childLists(p, i)` (the iterator every walk uses);
  - `walker.processor`, where each try/catch list is known with its user path and key line;
  - `w.warn(line, path, format, …)`.
- Resources: the walker has `processor_resources` (see `w.resources`, `taintedResources`, `refs.go`), so a `resource:` child can be resolved by label. Reuse that lookup; don't add a third resource walk.
- `debugger/rewrite/stdio.go` holds the existing warnings (`warnBatching` wording is the model: "…may differ").
- Goldens: `debugger/rewrite/testdata/<name>.{yaml,golden}`. Add cases and regenerate with `go test ./debugger/rewrite/ -update`; every existing golden must stay byte-identical.
- Corpus: `testdata/equivalence/<name>/{pipeline.yaml, expect.yaml}`. `expect.yaml` `output:` lists Debug Console lines every run must show. The stateless `try` case already exists (`testdata/equivalence/try`).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/walk.go` (or a new `debugger/rewrite/state.go`) -- the stateful table, the "holds stateful" check (resources, nesting, cycle guard), and the warning on try/catch lists -- item 1.
- [x] `debugger/rewrite/rewrite.go` -- the `buffer` warning -- item 2.
- [x] `debugger/rewrite/testdata/` -- golden cases:
  - try with `cache` + `mapping` (warned);
  - catch with `resource: c`, where `c` is a `cached` resource, + `mapping` (warned);
  - try with a single `dedupe` (not warned);
  - try with `rate_limit` + `mapping` (not warned);
  - try holding a `branch` that holds a `cache` (warned);
  - `buffer: memory: {}` (warned);
  - `buffer: none: {}` and no buffer (not warned).
- [x] `testdata/equivalence/try-stateful/` -- a `try` whose children are a `cache` `set` keyed per message and a `cache` `get` of the same key. That is order-insensitive, so equivalence holds; `expect.yaml` `output:` requires the try/catch warning.
- [x] `testdata/equivalence/buffer/` -- a case with a `buffer` that keeps the batch as captured, if one exists (probe `memory: {}` against the reference run), with `expect.yaml` requiring the buffer warning. If no buffer config passes the same-batch rule, put a dapdrive test asserting the warning in its place, and record why in Implementation Notes.
- [x] `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md` -- AD-7's example list loses `rate_limit` -- consistency with AD-8.
- [x] `_bmad-output/initiative-rpcn-debugger/deferred-work.md` -- two "Delivered" entries (append only).

**Acceptance Criteria:**
- Given a pipeline whose `try` holds a `cache` and a `mapping`, when it is launched, then the Debug Console shows one warning at that `try`'s line naming the order difference, and stepping is unchanged.
- Given `buffer: memory: {}`, when it is launched, then the Debug Console shows one warning at the `buffer` line.
- Given `task ci`, then it exits 0 with every pre-existing golden byte-identical.

## Implementation Notes

- The stateful table, the check and both warnings live in a new `debugger/rewrite/state.go`. `walker.processor` calls `warnStatefulOrder` for each `try`/`catch` key before wrapping its children, so the warning precedes any nested ones; lists inside `processor_resources` get it too (the walker visits them), tainted resources don't (they run opaque, processor-major like the engine).
- Resource lookup: `resourceBodies` was factored out of `taintedResources`; `Rewrite` builds it once and passes it to `taintedResources` and `statefulResources`, so there is no third resource walk. `statefulResources` scans each entry once (direct statefulness plus the labels it references), then propagates statefulness to referrers to a fixed point (reverse BFS) into `walker.statefulRes`; `resource:` children read that set. That is linear in the resources and ends reference cycles (golden `stateful-cycle`), replacing a first path-only DFS guard that was exponential over resources referenced by several routes (review finding).
- "Holds stateful" also looks inside opaque parents (concurrent and unknown): any `processors` list in their config, the `processors` processor's own list, and a workflow's `branch_resources`.
- `buffer`: null, an empty mapping, and a mapping with only `none` (and `label`) are the engine default and aren't warned. The warning is emitted after the output-batching ones.
- Corpus `buffer`: `memory: {}` (no `batch_policy`) passes the same-batch rule and equivalence, so no dapdrive fallback test was needed.
- Spine: besides dropping `rate_limit` from AD-7's example list, the two "(target: …)" clauses in AD-5 (`buffer`) and AD-7 (stateful try/catch) were removed, since both are now built, and AD-7 now says "a list of two or more children".

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 1, false 0, maybe-false 0. Both patched.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `holdsStateful`/`resourceStateful` search resources depth-first with a path-only guard and no per-label result, so resources referenced by several routes make the search exponential (2.55 s for 22 layers; a launch with ~30 layers would hang for minutes) | medium | patch | `statefulResources` now scans each entry once and propagates statefulness to referrers by reverse BFS to a fixed point, so a cycle (a → b → a) is right too. Measured afterwards with a throwaway test: 30 layers in 2.2 ms |
| 2 | No golden pins the reference-cycle guard | low | patch | `stateful-cycle.{yaml,golden}`: a two-child `try` warned through `b` → `a` → `cache` |

The orchestrator also reset `status` from `done` (set by the implementer) to `in-review`: only the user marks an entry done. The implementer's extra spine edit, removing the "(target: …)" clauses in AD-5 and AD-7 now that both warnings exist, was kept.

## Verification

- 2026-10-23: `go test ./debugger/rewrite/` PASS with every pre-existing golden byte-identical (only new `.golden` files from `-update`); `task ci` exit 0 (lint 0 issues, unit, equivalence incl. `try-stateful` and `buffer`, integration, extension build/test/package).

**Commands:**
- `go test -count=1 ./debugger/... ./test/...` -- expected: PASS.
- `task ci` -- expected: exit 0.

After the review patches (2026-10-23): `task ci` exit 0; no pre-existing golden changed.
