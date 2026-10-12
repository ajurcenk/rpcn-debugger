---
type: epic
title: "Concurrent branches as threads"
parent: initiative-rpcn-debugger
covers: [CAP-9, CAP-3, CAP-12]
after: []
assignee: ""
risk: high
---

# Concurrent branches as threads

## Description

Adds the concurrent parents (`workflow`, `parallel`, `retry` with `parallel: true`) with a thread per concurrent entry and per-thread step state, while keeping results identical.

## Outcome

A developer debugging a `workflow` sees each branch as its own thread and steps one without stopping the other (CAP-9).

## Done when

1. `workflow`, `parallel` and `retry parallel: true` step correctly. The rewrite marks them `concurrent: true`, and each entry forks a thread with a stable `threadId` (AD-16, AD-17).
2. Step state is kept per thread. Next / StepIn / StepOut stop only the thread they were issued on (AD-9).
3. Equivalence rows and CTX_LOST cases for these parents are green, both with no pauses and with Step Over at every stop.
4. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

The concurrent AD-7 rows only. The 'except at breakpoints' half of CAP-9 is proven in epic-breakpoints-and-evaluation.

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-9
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-7, AD-9, AD-16, AD-17

## Notes

- Waits on epic-tracer because: it needs Session.Arrive, frames in ctx and the gate.
- Waits on epic-safe-against-real-systems because: both change the rewrite and the stepper, so they must not run in parallel.
