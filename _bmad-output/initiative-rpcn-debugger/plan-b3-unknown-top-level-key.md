---
title: 'An unknown top-level key fails the launch'
type: 'bugfix'
ticket: ''
created: '2026-10-30'
status: 'built'
baseline_revision: 'dddf2a124d4382d3d82f4473b25409029ced78a9'
route: 'oneshot'
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

**Problem:** E3 retro finding R3 (action B3). A top-level key outside the allowlist (`topLevel` plus `*_resources`), such as a misspelled `pipline:`, is dropped silently: no error and no warning. It reaches no engine config, so the engine's lint never sees it, and its processors never run, although the `topLevel` comment says such keys are "left to the engine's lint". `rpk connect lint` reports "field pipline not recognised", and `rpk connect run` refuses to start on lint errors by default.

**Approach:** (User decision 2026-10-30: a launch error, not a warning.) A top-level key that is neither in `topLevel` nor ends in `_resources` is `PIPELINE_INVALID` at its line and user path, "field <key> not recognised", the same text as Connect's lint, checked before anything else in the rewrite. Test fixtures that keep YAML anchors under an extra top-level key (which Connect's lint also refuses) move them into allowlisted sections, keeping what they test. AD-5's top-level sentence and the README say so.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because the check is a few lines at the start of `Rewrite`.

- `rewrite.go`: `knownSection(key)` (in `topLevel` or ends in `_resources`); `Rewrite` returns `invalid(k.Line, k.Value, "field %s not recognised", k.Value)` for the first unknown key, right after resolving aliases. The `topLevel` comment no longer says other keys are left to the engine's lint.
- Fixtures: `rewrite/testdata/alias.yaml` (the anchor moves to the input's `mapping`, aliased from the pipeline) and `merge.yaml` (each anchor defined inline where it is merged) were restructured; `merge.golden`'s content is identical apart from line numbers, and `alias.golden` changes in lines and its input mapping; what they test (an alias across subtrees, merge keys, the lost `root.lost`) holds. `rewrite/positions_test.go` and `engine/positions_test.go` rename their anchor keys to same-width allowlisted keys (`base`→`http`, `bad: &bad`→`tests: &b`, `shared`→`tracer`) so every expected line and column is unchanged. `merge.yaml` was never Connect-lint-clean (`<<` as an input), before or after; it tests the rewrite's merge handling only.
- A scan of every pipeline YAML under `testdata/`, `debugger/**/testdata/` and `test/` found no other unknown top-level key (only `expect.yaml` files, which aren't pipelines).
- Tests: `TestUnknownTopLevelKey` (PIPELINE_INVALID at `pipline`, line 3; a `*_resources` section passes) and `TestServeUnknownTopLevelKey` (through `debugger.Serve`). With the check disabled both fail.
- Docs: AD-5's top-level sentence (memlog entry 84; spine lint clean), README (the allowlist paragraph; plus B2's "also when the output reaches it through `output_resources`").

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 2, false 0, maybe-false 0.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `merge.yaml`'s restructure added `request_map`/`result_map` to the pipeline branch (it merged `&bb` where it was defined), changing the fixture's meaning, and the plan note said goldens change in lines only | low | patch | `&bb` is defined inline in the output branch where it is merged; `merge.golden` equals the old one apart from line numbers; note corrected |
| 2 | An empty or complex top-level key gives "field  not recognised" with an empty path | low | reject | Rare (Connect's own lint is no clearer), and a better message needs a new branch |

## Verification

**Commands:**
- `go test -count=1 ./debugger/...` -- expected: PASS.
- `task ci` -- expected: exit 0.

Run 2026-10-30 after the review patch: `task ci` exit 0.
