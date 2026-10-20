---
title: 'Rewrite rejects undefined resource references'
type: 'bugfix'
ticket: '13'
created: '2026-10-20'
status: 'built'
baseline_revision: '16d518b79d8250642463e4b6a4b014be52e8a8ad'
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

**Problem:** A pipeline that uses `- resource: <label>` (or a workflow `branch_resources` name) without a matching `processor_resources` label launches, then hangs: benthos retries the resource lookup for each message, and the session never stops or terminates. This was found in the 2.4 review and has existed since 2.1.

**Approach:** The rewrite collects the `processor_resources` labels and checks every processor-position `resource:` reference and every `branch_resources` name against them. It walks the input, pipeline and output processor lists, every resource, and the insides of opaque parents. An undefined one fails the launch with `PIPELINE_INVALID`, naming the label, the referencing user path and its line. Interpolated labels or references (`${VAR}`) are skipped, because only the engine can resolve them.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because this is about 60 lines in `debugger/rewrite`, plus goldens.

- Only a processor whose component key is `resource` counts: a `- resource: x` list item. `resource:` inside other components (`cache`, `rate_limit`, inputs, outputs) names other resource kinds and is ignored.

Implementation (2026-10-20):
- Files: new `debugger/rewrite/refs.go` (`checkResourceRefs`, called in `Rewrite` after the resources are validated and before the taint scan); goldens `undefined-resource`, `undefined-resource-in-resource`, `undefined-branch-resource`, `resource-other-kinds`; `debugger/serve_test.go` `TestServeUndefinedResource` (launch fails, no hang).
- The walk follows the AD-7 parents' child lists like the main walker, and inside opaque parents it searches every `processors` list and `branch_resources`.
- If any `processor_resources` label is interpolated, the check is skipped entirely. An interpolated reference is skipped individually.
- No existing golden changed.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 3, false 0, maybe-false 0. Three were patched, one deferred to the sweep.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | References inside the bare-list `processors` processor are missed, including the common `processor_resources: [{label: a, processors: [...]}]` and lists nested under `parallel` | medium | patch | Reproduced (err=nil). Inside opaque processor subtrees, a list of mappings is now checked as a processor list (`undefined-resource-processors`, `undefined-resource-parallel` goldens) |
| 2 | Processor lists nested inside the input tree and `input_resources` aren't checked | low | patch | The input tree and `input_resources` are searched, but only `processors:` lists count there, so `broker.inputs: [{resource: in1}]` (an input resource) isn't flagged (`undefined-resource-input`, `input-resource-ref` goldens). The orchestrator caught that false-positive risk in the first patch |
| 3 | The resource path for a missing, empty or non-scalar label differs from the walker's | low | patch | `resourcePath` uses `section.<label>`, or `section[i]` as the walker does |
| 4 | The AD-7 child-list table is duplicated across `walker.processor`, `childLists`, `refChecker.processor` and the taint scan | low | defer | A real drift risk, but a refactor, not a bug. Deferred to the 2.10 refactor sweep (deferred-work.md) |
