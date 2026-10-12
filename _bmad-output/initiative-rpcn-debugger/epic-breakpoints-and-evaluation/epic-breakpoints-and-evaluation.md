---
type: epic
title: "Stop where it matters and ask Bloblang questions"
parent: initiative-rpcn-debugger
covers: [CAP-5, CAP-6, CAP-9]
after: []
assignee: ""
risk: medium
---

# Stop where it matters and ask Bloblang questions

## Description

Line, conditional and on-error breakpoints held as Session state, plus Watch and debug-console evaluation with the pipeline's own Bloblang environment and batch context.

## Outcome

A developer stops only where a condition holds and checks Bloblang expressions against the paused batch (CAP-5, CAP-6).

## Done when

1. Line breakpoints resolve on processor spans, verified at the processor's first line and unverified outside any processor. Pending breakpoints resolve at the end of Launch, and stepping waits for ConfigurationDone. A `processor_resources` breakpoint hits at every call site (AD-18).
2. A conditional breakpoint stops when any message matches and focuses the first match. On-error stops right after a processor introduces an error.
3. A breakpoint on one concurrent thread stops only that thread (the CAP-9 'except at breakpoints' clause).
4. Watch and console evaluate via `MessageBatch.BloblangExecutor` with the pipeline's Bloblang environment. `batch_index()` and `batch_size()` are correct (AD-13 evaluator).
5. The equivalence corpus is still green.
6. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

Not statement breakpoints or the tracer (epic-bloblang-statement-stepping). No logpoints or hit counts (spine Deferred).

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-5, CAP-6, CAP-9
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-10, AD-13, AD-18

## Notes

- Waits on epic-concurrency because: it needs per-thread step state to prove that a breakpoint on one thread stops only that thread.
