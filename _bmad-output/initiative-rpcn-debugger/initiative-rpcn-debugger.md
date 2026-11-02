---
type: initiative
title: "Redpanda Connect VSCode visual debugger"
parent: none
covers: [CAP-1, CAP-2, CAP-3, CAP-4, CAP-5, CAP-6, CAP-7, CAP-8, CAP-9, CAP-10, CAP-11, CAP-12, CAP-13]
after: []
assignee: ""
status: in-progress
risk: high
---

# Redpanda Connect VSCode visual debugger

## Description

A VS Code debugger that captures one batch from a Redpanda Connect pipeline's input and steps it through processors and Bloblang statements, showing message state at every stop, without touching production data or deployments. The spec owns the capabilities, constraints and non-goals. This is a personal project, and the MVP is distributed locally as a `.vsix`.

## Outcome

Pipeline developers find the processor or Bloblang statement that causes a wrong result where it happens, instead of steps later; the spec's success signal is the measure.

## Done when

1. The spec's success signal is demonstrated end to end on a real Redpanda topic: F5 → stop at the first processor → Step In through a branch and a mapping statement → fix → Restart on the same batch → production group offsets unchanged.
2. Every CAP-1 to CAP-13 passes its spec success criterion in the locally installed `.vsix`.
3. `task ci` is green on the pinned Connect version (v4.115.0 since 2026-10-22; v4.112.0 before), including the AD-8 equivalence corpus and the never-ack integration test.
4. A fresh machine can install the `.vsix` and debug a pipeline following the README alone.

## Boundaries

Split by outcome, one owner per epic (solo developer with agent lanes); units are not epic boundaries. Out of scope: the spec's Non-goals, plus Marketplace publication for the MVP. Tracer path: epic-tracer's first story, F5 on a `generate` input with one `mapping`, stopping before the mapping and showing the message.

- Touch point: Redpanda Connect engine v4.115.0 / benthos v4.84.0 (pinned in go.mod), consumed through the public API only; owner: epic-tracer
- Touch point: Redpanda broker container for tests, consumed; owner: epic-platform-baseline (the job), epic-safe-against-real-systems (the never-ack test)
- Touch point: VS Code extension host API, consumed; owner: epic-tracer

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, the binding ADs; cross-epic contracts (AD-3, AD-11, AD-12, AD-16, AD-17) live there
- constraint — the same spec, section Constraints

## Notes

- Decision: personal project; the extension is "Pipeline Debugger for Redpanda Connect" under the author's publisher (user, 2026-10-09).
- Decision: MVP distribution is local (`.vsix`); the VS Code Marketplace comes later (user, 2026-10-09).
- Decision: CI is local only (`task ci`) (user, 2026-10-09).
- Decision: E2 is split by outcome into tracer / safe against real systems / concurrency (user, 2026-10-09).
- Decision: the 8-epic set and its order were approved (user, 2026-10-12).
- Decision: plans have no word-count target. Split an entry instead when its plan would change frozen intent or name more than about 8 test cases, and plans probe engine behaviour for the cases they name (E2 retro A9, E3 retro B8; user, 2026-11-02).
- Decision: `deferred-work.md` is append-only, enforced by `task lint:deferred` in `task ci` (`scripts/check-append-only.sh`, which checks every commit that touched the file and the working tree). "Stage explicit paths, never `git commit -a`" and the append-only rule go into CLAUDE.md at the next refactor sweep (E2 retro A10, E3 retro B8; user, 2026-11-02).
- Decision: epic-edit-restart-replay is built right after epic-safe-against-real-systems; the build order is platform-baseline, tracer, safe-against-real-systems, edit-restart-replay, concurrency, breakpoints-and-evaluation, bloblang-statement-stepping, local-release. Re-resolving breakpoints and statement lines on Restart moves to epics breakpoints-and-evaluation and bloblang-statement-stepping. This supersedes the order approved on 2026-10-12 (user, 2026-11-02, at epic-edit-restart-replay's inception).
- Parked: stable message identity across `group_by` reordering; it's needed only by the deferred message-diff feature (spine Open Questions).
- Assumption: the default `maxMessages` is 10 (architect's choice).
