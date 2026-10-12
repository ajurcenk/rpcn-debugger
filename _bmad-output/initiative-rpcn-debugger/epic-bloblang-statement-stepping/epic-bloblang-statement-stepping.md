---
type: epic
title: "Step inside a mapping"
parent: initiative-rpcn-debugger
covers: [CAP-7]
after: []
assignee: ""
risk: high
---

# Step inside a mapping

## Description

Statement-by-statement stepping through Bloblang mappings by prefix replay, as a view whose real processor result stays authoritative.

## Outcome

A developer finds the exact faulty Bloblang line; this is the middle of the spec's success signal (CAP-7).

## Done when

1. Step In on `mapping`, `mutation` and `bloblang`, and on branch `request_map`/`result_map`, steps per statement using U-BLOBL's `Statements()`. The base-line rule applies; folded and quoted mappings run as one step, with a warning (AD-6).
2. root, this, meta and vars are shown after each statement through the Snapshot `bloblang` field: root is unassigned for `mapping` and starts from the input for `mutation`. Values are labelled recomputed or environment-dependent. Tracing is capped at 200 statements.
3. Statement breakpoints work, with LineMap statement lines filled by the rewrite. The trace-vs-real check flags any difference.
4. The spike's 16-line mapping steps as 9 statements with correct state, and the equivalence corpus is still green.
5. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

Not a Bloblang interpreter. No upstream statement hook (spine Deferred).

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-7
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-6, AD-10, AD-11, AD-13, AD-18
- spike evidence — architecture-rpcn-debugger/.memlog.md, the prefix-replay entries. The spike source no longer exists; re-derive it from the recorded findings

## Notes

- Waits on epic-breakpoints-and-evaluation because: it reuses the evaluator and extends the breakpoint resolver with statement lines.
