---
title: 'output: real refuses shared-client outputs reached through output_resources'
type: 'bugfix'
ticket: ''
created: '2026-10-30'
status: 'built'
baseline_revision: '2840c598f872305fa0106338f586c20bed881c8d'
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

**Problem:** E3 retro finding R2 (action B2). Under `output: real`, an output that needs the top-level `redpanda` block's shared client (`redpanda_common`, or `redpanda` without `seed_brokers`) is refused at launch (`PIPELINE_INVALID`) only when it is in the top-level `output` block. The same output reached through `output: {resource: o}` isn't checked: the launch goes on, the output can't connect, and the run ends after the 30 s write deadline with a generic `OUTPUT_FAILED`.

**Approach:** Under `output: real`, every `output_resources` entry the output tree reaches through `resource:` (to a fixed point) gets the same shared-client check, with the same error at the entry's line and user path. A `resource:` that can't be resolved at launch (interpolated, or naming no literal label) may reach any entry, so then every entry is checked, as B1 does for inputs. An entry nothing reaches is never built and isn't checked. Dry-run and none are unchanged, and so is every golden.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because the fix is one helper in `debugger/rewrite/output.go` plus tests.

- `output.go`: the shared-client cases moved from `realOutput` into `sharedClient(k, v, p)`; `sharedResources(out, outRes)` finds the reached entries with `closure` (3.10's fixed point) over `resource:` string fields (`walkPairs`, which skips processor lists) and runs `sharedClient` over each reached entry, or every entry when a reference can't be resolved.
- `rewrite.go`: `sharedResources` runs under real before `realOutput(outRes, …)`.
- Tests: `TestPlanOutputRealSharedClient` gains "resource", "resource chain" (through a `fallback` resource), "runtime resource" (`${O:o}`), "unmatched resource", and an unreferenced resource that is not refused; `TestServeOutputLaunch` gains "shared client through a resource under real" (`PIPELINE_INVALID` at `output_resources.o.redpanda_common`). With the `sharedResources` call disabled, the four reached cases fail. Goldens unchanged.
- Review patch: references are found by `outputRefs`, which follows only output positions (each block's component; the child outputs of `broker`, `fallback`, `reject_errored`, `drop_on`, `retry`, `switch` cases and `dynamic`, the benthos v4.84.0 fields built with `NewOutputField`/`NewOutputListField`/`NewOutputMapField`; connect adds none), instead of any `resource` key. Tests: a header named `resource` reaches nothing (both a matching and an unknown value), and a `switch` case's `resource:` is followed.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 0, false 1, maybe-false 0.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | Any map key named `resource` (e.g. an `http_client` header) counts as a reference, so an entry nothing reaches is refused | medium | patch | `outputRefs` follows output positions only; header tests |
| 2 | The diff's `baseline_revision` is the wrong hash | false | reject | The staged diff was taken before the fix; the file on disk has `2840c598…` |

## Verification

Run 2026-10-30 after the review patch: `task ci` exit 0; goldens unchanged.
