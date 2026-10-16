---
type: epic
title: "Debug against real systems without harming them"
parent: initiative-rpcn-debugger
covers: [CAP-2, CAP-11]
after: []
assignee: ""
risk: high
---

# Debug against real systems without harming them

## Description

Makes sessions against live input systems safe by default: input classes, the consumer-group override, dropped observability, stdio-conflict rules, and the opt-in `real` and `none` outputs. The never-ack integration test proves it against Redpanda.

## Outcome

A developer can point the debugger at a production topic and leave the production consumer group's offsets unchanged; this is the last clause of the spec's success signal.

## Done when

1. Input classes apply (AD-5): side-effecting inputs are refused without `allowSideEffects`, with a warning naming the effect; rejected inputs are refused with a replay hint; unknown inputs are treated as side-effecting.
2. Consumer-group fields are overridden to `rpcn-debug-<os-user>`, recursing into `broker`/`sequence`, unless `keepConsumerGroup` is set.
3. `output: real` forwards each batch to the user's output, and `output: none` discards them (AD-21).
4. The never-ack integration test against a Redpanda container shows no committed offsets for the production or the debug group, and a rerun captures the same messages.
5. The equivalence corpus is still green.
6. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

Not replay itself (epic-edit-restart-replay). The rejected-input hint names the `replay` key, which arrives there.

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-2, CAP-11
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-4, AD-5, AD-15, AD-21

## Notes

- Waits on epic-tracer because: it extends the rewrite's capture config, the capture and the sink.
- Handoff: epic-edit-restart-replay must let a replay session bypass the input-class rejection.
- Waits on epic-platform-baseline because: the never-ack test goes into its Redpanda job (package test/integration, build tag `integration`, entry 1.3).
- Decision: dropping `metrics`/`tracer`/`http`/`logger` and the stdio rules (AD-5, AD-15) moved to epic-tracer entry 5; this epic keeps the consumer-group override (user, 2026-10-16).
