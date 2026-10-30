---
title: 'Never-ack for input resources named at run time'
type: 'bugfix'
ticket: ''
created: '2026-10-30'
status: 'built'
baseline_revision: 'd0ba69162e7351b9195a1e374b1d221b754d2d87'
route: 'oneshot'
route_source: 'auto'
risk: 'high'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** E3 retro finding R1 (action B1). The input walker overrides consumer groups and forces `auto_replay_nacks` only on the `input_resources` it reaches through a literal `resource:`. An input that names its resource at run time (`input: {resource: ${IN:a}}`) is classed Unknown, but the resource it reaches keeps `consumer_group: prod` and `auto_replay_nacks: false`. With `allowSideEffects: true`, which the refusal asks for, the capture joins the production group and its nack counts as an ack, so offsets are committed. That breaks never-ack, which E3 calls non-configurable.

**Approach:** When the walk meets a `resource:` it can't resolve at launch (interpolated), every `input_resources` entry the walk didn't reach gets the same treatment as a reached one: the group override (`<base>-<k>`, continuing k) and the forced `auto_replay_nacks`, each with its usual warning, in file order. The input's class stays Unknown, and the entries add no findings of their own, so refusals don't change. Without such a reference, unreached entries stay as they are (benthos builds input resources lazily, so they never connect), and every existing golden is byte-identical.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because the fix is one pass in `debugger/rewrite/inputwalk.go` plus tests.

- `inputwalk.go`: `inputWalker.dynamic` is set when `resource()` meets an interpolated (or non-scalar) reference in the main walk (not the local `resourceClasses` walk). `unreached()` runs after the main walk: when `dynamic`, each unreached input resource, in file order (`order`), is walked with `block` for its group override, forced `auto_replay_nacks` and latest-offset warning; the findings it adds are dropped, so the Session's refusals are unchanged. Nested literal references inside it are walked too (marked emitted).
- `rewrite.go`: `iw.unreached()` after `iw.block(capture, "input")`.
- Tests: `TestGroupOverrideMatrix` rows "runtime resource" (two resources, `dbg-0`/`dbg-1`, nacks forced, class Unknown) and "unreferenced resource" (no runtime reference: left as it is); golden `inputs-runtime-resource`; `TestServeRuntimeResource` (with `allowSideEffects`, the group replaced and nacks forced through `debugger.Serve`). Mutation: with `unreached()` disabled, all three fail. Every existing golden is byte-identical.
- Review patch: an entry whose label is interpolated is now walked too (`inputWalker.all`, every entry in file order), and a literal `resource:` that names no literal label also sets `dynamic`, since it may name such an entry. Matrix row "interpolated label" (`resource: a`, label `${L:a}`): group replaced and nacks forced; it fails without `unreached()`.

## Review Triage Log

Pass 1 (quick): high 1, medium 1, low 2, false 0, maybe-false 0.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | An input resource with an interpolated label is never in `order`, so a runtime (or unmatched literal) reference reaching it keeps the production group and `auto_replay_nacks: false` | high | patch | `all` holds every entry; an unmatched literal reference sets `dynamic`; matrix row "interpolated label" |
| 2 | `unreached` drops the findings it computes, so a runtime reference to a rejected resource (stdin, http_server) is only Unknown and `allowSideEffects` lets it through | medium | defer | Predates B1 (the referring input was already only Unknown); rolling Rejected into the referrer changes refusals, which the frozen intent rules out. Appended to deferred-work.md |
| 3 | Group numbering follows walker order, not strict file order, when an unreached entry names a later one | low | reject | AD-5/D5 number groups in walker order; the guarantee is distinctness, which holds |
| 4 | The `emitted` and `resource()` comments no longer match | low | patch | Rewritten |

## Verification

**Commands:**
- `go test -count=1 ./debugger/...` -- expected: PASS; every existing golden byte-identical.
- `task ci` -- expected: exit 0.

Run 2026-10-30 after the review patch: `task ci` exit 0; existing goldens unchanged.
