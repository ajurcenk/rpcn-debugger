---
title: 'Consumer-group override for every group field'
type: 'feature'
ticket: '5'
created: '2026-10-23'
status: done
baseline_revision: 'cbcf1e529339f8ca611d7f5224fdb9fc3018474d'
route: 'full'
route_source: 'auto'
risk: 'high'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** This is E3 entry 5 (CAP-2, Done when 2; D4 and D5). Only the top-level `redpanda`/`kafka_franz` input gets the debug group today (3.2).
- A Kafka input inside a `broker`, `sequence` or `input_resources` keeps the pipeline's group, so it counts as side-effecting and is refused.
- The legacy `kafka` and `redpanda_common` inputs aren't overridden at all.
- Nobody can deliberately keep the production group.
- A start offset of `latest` makes capture wait for new traffic with no explanation.

**Approach:**
- **Which fields are overridden** (user decision, 2026-10-23): the Kafka family's `consumer_group` (`kafka`, `kafka_franz`, `redpanda`, `redpanda_common`) and the `nats` input's `queue`. Every other group-like field in the table stays as configured, and its input keeps its class: Postgres `slot_name`, Pulsar `subscription_name`, Pub/Sub `subscription`, the checkpoint and metadata tables, `nats_jetstream`/`nats_stream` durables. Overriding those would create durable objects on the production system, or break the input.
- **Where:** the override applies through the 3.3 walker to every such input in the capture tree, `broker`/`sequence`/`read_until`/`batched` children and `input_resources` included.
- **Naming:** each overridden field gets `<DebugGroup>-<k>`. Here `k` counts the overridden inputs in walker order, from 0, so the 3.2 tracer stays `-0` and distinct groups stay distinct (D5). Each override keeps its one launch warning naming both groups.
- **`keepConsumerGroup`** (a new launch key, bool, default false) turns the override off for the session. Then any set group field, and any one the rewrite can't override (an interpolated value), makes its input side-effecting, with the effect "joins consumer group G; its members rebalance" (D4). It is refused without `allowSideEffects` (1012).
- **Latest warning:** a start offset that resolves to latest (`start_offset: latest`, or `start_from_oldest: false`) gets a launch warning: the capture waits for new records, and the 5 s wait note may follow.

## Boundaries & Constraints

**Always:**
- The rewrite stays pure: `DebugGroup` and the keep flag arrive as rewrite options from the Session.
- The class is computed after the override (D4). A replaced group counts as overridden, so it isn't "kept".
- The 3.2 tracer behaviour and `TestNeverAck` are unchanged. Existing goldens change only where a nested or `kafka`/`redpanda_common` group is now overridden, or where a new latest warning appears.
- `keepConsumerGroup` joins `dap.LaunchConfig`, the `Launch` command and the generated schema, like `allowSideEffects` (3.4).

**Never:**
- No override of the fields listed as left alone, and no new classes beyond the D4 rule.
- No change to start offsets themselves: the warning only.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Nested | `broker.inputs: [{redpanda: {consumer_group: a}}, {redpanda: {consumer_group: b}}]` | `-0` and `-1`; two warnings; replay-safe | none |
| Sequence | the same under `sequence.inputs` | `-0`, `-1` | none |
| Resource | `input: {resource: r}`, `r` a `kafka_franz` with a group | overridden; replay-safe | none |
| Legacy kafka | `kafka.consumer_group: prod` | `<base>-0`; replay-safe | none |
| NATS queue | `nats.queue: workers` | `<base>-0`; replay-safe | none |
| Interpolated | `consumer_group: ${GROUP}` | left as is; side-effecting "joins consumer group …" | 1012 without `allowSideEffects` |
| Keep | `keepConsumerGroup: true`, `consumer_group: prod` | not overridden; side-effecting | 1012 without `allowSideEffects`; launches with it, warning |
| Left alone | `postgres_cdc.slot_name: s` | unchanged; stays side-effecting | 1012 as today |
| Latest | `redpanda.start_offset: latest` | one latest warning | none |

</frozen-after-approval>

## Code Map

- `debugger/rewrite/rewrite.go`:
  - `overrideGroup` (~259): top-level only and `overridable` rows; it returns the replaced nodes, which the walker skips.
  - `Options` gains the keep flag. `DebugGroup` came in 3.1; `InputClasses` in 3.4.
- `debugger/rewrite/inputs.go`:
  - the rows and their `groups`, `starts` and `overridable` (~265–308);
  - `kafkaGroup` and `franzStart`;
  - the group rule that makes a set, unoverridden group side-effecting.
  - Mark `kafka`, `redpanda_common` and `nats` overridable on their group field. `nats` (~306) has `seIf(... set("queue"))`: an overridden queue must make it replay-safe.
- `debugger/rewrite/inputwalk.go`: the walker, `resourceClasses`, and where children and resources are visited. The override belongs in the walk, in walker order, so `k` follows the findings order. A resource referenced twice is overridden once.
- `debugger/core` (port.go `StartOptions`, protocol.go `Launch`), `debugger/core/session.go` (passes the options), `debugger/engine/engine.go` `LoadOptions`, `debugger/serve.go`: the 3.4 pattern for passing a launch option into the rewrite.
- `debugger/dap/launch.go` and `vscode/package.json`: the new bool key; run `task gen:schema`.
- Tests: rewrite goldens or table tests for the matrix, and a `debugger.Serve` test (`serve_inputs_test.go`) for Keep (1012, then a launch with `allowSideEffects`). `test/integration/neverack_test.go` must still pass unchanged.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/` -- the walker-wide override for the decided fields, `-<k>` numbering, the keep and interpolated rule, the latest warning -- D4 and D5.
- [x] `debugger/core/`, `engine`, `serve.go`, `dap/launch.go` plus the schema -- `keepConsumerGroup` -- the launch key.
- [x] Tests for every matrix row, through `Serve` for Keep.

**Acceptance Criteria:**
- Given each matrix scenario, when rewritten and launched, then the groups, classes, warnings and codes match the matrix.
- Given `task ci`, then it exits 0, with `TestNeverAck` and the equivalence corpus green.

## Implementation Notes

- The override moved from `rewrite.go` (`overrideGroup`, top level only) into the walker (`inputwalk.go` `overrideGroups`), called in `block` next to `forceReplayNacks` in the main walk only, so `k` follows the findings order and a resource named twice is overridden once (`emitted`). It replaces the value node in place in the resolved tree, so the capture config and `Plan.Resources` (`input_resources`) both carry the debug group, and lint positions stay at the user's line.
- The `overridden` node map is gone. `overridable(row, n)` is a pure predicate (overridable row, non-empty base, no keep, a literal non-empty scalar). `keptGroup` uses it, so the local walk in `resourceClasses`, which runs before the main walk overrides anything, classes a resource the same way the main walk will. A replaced value is still a literal, so it still counts as overridden.
- `kafka`, `redpanda_common` and `nats` are now `overridable`. `nats` lost its `seIf(set("queue"))` class: the group rule alone makes a kept queue side-effecting. A `queue` field is called a "queue group" in the warning and the effect.
- The latest warning (`warnLatest`) applies to any row's `starts` field whose last segment is `start_offset` with the literal `latest`, or `start_from_oldest` with the literal bool `false`; one per input (the first match), and none for an interpolated value. So `aws_kinesis`, `nats_stream` and `redis_streams` with `start_from_oldest: false` get it too.
- `keepConsumerGroup` goes `dap.LaunchConfig` → `core.Launch` → `StartOptions.KeepConsumerGroup` (kept on the Session for Start) → `engine.LoadOptions` → `rewrite.Options`. No new Session warning: under `allowSideEffects` the existing side-effect warning names the kept group.
- Goldens are unchanged: `TestGolden` runs without a `DebugGroup`, and no golden has a start offset at latest.
- `debugger/serve_inputs_test.go` now imports connect `components/kafka`, for `kafka_franz`.
- README's input section and the spine's built launch keys row now cover `keepConsumerGroup`, the wider override and the latest warning.

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 5, false 0, maybe-false 0. 4 patched by the implementer, 1 by the orchestrator. No safety finding.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `warnLatest` falls through to `start_from_oldest` for `kafka_franz`/`redpanda`, where only `start_offset` decides, so `start_offset: committed` + `start_from_oldest: false` warns falsely | low | patch | Rows with a `start_offset` field ignore `start_from_oldest`; a new no-warning test "committed, oldest false" |
| 2 | A kept NATS queue gets "its members rebalance", but core NATS queue members don't rebalance; the debugger takes production's share | low | patch | A `queueGroup` row flag on `nats`: "joins queue group … and takes its share of the messages, which aren't redelivered after the nack"; tests "nats keep" and "nats interpolated" |
| 3 | Under a kept group, `latest` only applies without committed offsets, yet the warning says the capture waits | low | patch | Kept-group wording "…unless the kept group has committed offsets…"; test "latest kept" |
| 4 | Most matrix rows are tested only at rewrite level, while the AC says "rewritten and launched" | low | patch | `TestServeGroupOverride` launches Nested, Sequence, Resource, Legacy kafka, NATS queue, Left alone (1012) and Latest through `debugger.Serve` |
| 5 | Spine AD-5 still says k is "the input's index in walker order" | low | patch (orchestrator) | AD-5 now says k counts overridden inputs from 0, and lists the overridden fields and the ones left alone; memlog entry; lint clean |

The implementer also updated the README's input section and the spine's launch-keys row (not named in the plan); the orchestrator checked the README text against the code.

## Verification

**Commands:**
- `go test -count=1 ./debugger/... ./test/...` -- expected: PASS.
- `task ci` -- expected: exit 0 (Docker running).

Run 2026-10-23 by the implementer: `go test -count=1 ./debugger/... ./test/...` PASS; `task ci` exit 0, with the integration tests (`TestNeverAck`) and the equivalence corpus green.

Run 2026-10-23 by the orchestrator after the review patches: `task ci` exit 0 (`TestNeverAck` included).
