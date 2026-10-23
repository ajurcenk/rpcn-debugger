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
2. Consumer-group fields are overridden to `rpcn-debug-<os-user>-<k>` (k per input in walker order), recursing into `broker`/`sequence`/`read_until`/`resource:`, unless `keepConsumerGroup` is set; a kept group makes the input side-effecting.
3. `output: real` forwards each batch to the user's output, and `output: none` discards them (AD-21).
4. The never-ack integration test against a Redpanda container shows no committed offsets for the production or the debug group, the production group's membership generation is unchanged, and a rerun captures the same messages.
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
- Decision: (D1, adversary H1) one input table (component, class predicate over its fields, group fields, start-offset fields, effect text) and one input-tree walker (`broker.inputs`, `sequence.inputs`, `read_until.input`, `resource:` → `input_resources`) in `debugger/rewrite`, used for the class, the group override and the stdin rule. A composite takes its worst child's class; classes are field-aware (`aws_s3` with `sqs` is side-effecting). The rewrite only reports findings; the Session alone refuses, against `allowSideEffects` (and later `replay`). `stdin` becomes a rejected-class row. New codes `INPUT_REJECTED` 1011 (replay hint) and `SIDE_EFFECTS_NOT_ALLOWED` 1012 (user, 2026-10-23).
- Decision: (D2, H2) under `output: real` the rewrite emits the user's `output` block without its top-level `processors` (they already ran in the stepper), with `stdout`/`stderr` replaced by `drop: {}` (warned). One `rpcn_debug_sink` with a `mode` and an output field writes synchronously (`MaxInFlight = 1`); the engine never parses an output string (user, 2026-10-23).
- Decision: (D3, H3) the sink calls `Deliver` once per batch, after the write under `real`, with an outcome (recorded, written, discarded, failed) and the write error; `Delivered` is emitted in all three modes. A failed write is `OUTPUT_FAILED` 1013. The Session owns the batch number and the mode-aware console texts (the DAP counter goes). `STRICT_REJECTED` is decided from which messages carry an error, never from error text (closes the mixed-batch strict risk) (user, 2026-10-23).
- Decision: (D4, H4) the class is computed after the group override: a group-consuming input whose group is kept (`keepConsumerGroup`, or a field the table can't override) is side-effecting ("joins consumer group G; its members rebalance"). The never-ack test also asserts the production group's membership generation is unchanged (user, 2026-10-23).
- Decision: (D5, H5) the Session supplies the debug-group base `rpcn-debug-<os-user>` from the process user as a rewrite option; the rewrite stays pure. Each overridden field gets `<base>-<k>` (k = the input's index in walker order), so distinct groups stay distinct. No per-session suffix in v1. Start-offset fields stay as configured, with a launch warning when one resolves to latest. After 5 s with nothing captured, one console note ("waiting for the input …") (user, 2026-10-23).
- Decision: (D6, H6) a Restart or `replay` session with `output: real` writes only with `allowRepeatWrites: true`, else it runs as dry-run with a note. Built by epic-edit-restart-replay; recorded in its Notes as a handoff (user, 2026-10-23).
- Decision: (D7, H8) a processor effect table (component plus field predicate) gives each side-effecting processor (`sql_insert`, `sql_raw`, `redis` writes, remote `cache` set/delete, `http` with a write method, `aws_lambda`, `command`, …) a launch warning naming the system it writes to, marked "runs during capture, not stepped" in the input tree. Warn only; no opt-in required (user, 2026-10-23).
- Decision: (D8, H9) the rewrite carries an explicit allowlist of top-level sections: `input`, the processor lists, `output` (D2), `*_resources`, `error_handling`, and `redpanda` with only its connection fields (`logs_topic`, `status_topic`, `pipeline_id` removed). If `StreamBuilder` doesn't wire the shared `redpanda` client, `redpanda_common` is a rejected-class row with that reason (user, 2026-10-23).
- Decision: (D9, H7) a `cache_resources`/`rate_limit_resources` entry reachable from both the input tree and the step tree gets a launch warning that its state isn't shared between capture and stepping, plus a corpus case (user, 2026-10-23).
- Decision: retro A5 (one rewrite per launch: Start runs Check's configs) joins this epic as its opening enabler, because the Session now passes rewrite options. A11, A12 and A13 stay out of this epic (user, 2026-10-23).
- Tracer bullet: entry 2 (a `kafka_franz` capture under the debug group; the never-ack test).
- Decision: entry 1 (retro A5, one rewrite per launch plus rewrite options) precedes the tracer, because the tracer's group base is a Session-supplied rewrite option (D5) (inception, 2026-10-23).
- Decision: one lane in build order. The set check chained the entries because nearly all of them change the rewrite or the launch config (inception, 2026-10-23).
- Decision: D9's corpus case is a warning golden for the shared cache, plus a corpus case with a cache used only in the step tree, which must stay equal (user, 2026-10-23).
- Decision: the breakdown was approved with 11 entries; the spine is updated for D1-D9 right after the inception (user, 2026-10-23).
- Decision: keep D5. The capture joins a real consumer group `rpcn-debug-<os-user>-<k>` over the real Kafka connection; reading with no group was considered and declined (user, 2026-10-23, after the 3.2 VS Code check).
