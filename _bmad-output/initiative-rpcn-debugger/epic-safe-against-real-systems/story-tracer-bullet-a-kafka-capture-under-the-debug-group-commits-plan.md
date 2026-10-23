---
title: 'Tracer bullet: a Kafka capture under the debug group commits nothing'
type: 'feature'
ticket: '2'
created: '2026-10-23'
status: done
baseline_revision: '98c3c479d36f7094c2a9ea050c55fef5b61dfd75'
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

**Problem:** This is E3 entry 2, the tracer bullet, covering CAP-2 and Done when 4. Nothing proves yet that a debug session against a real Kafka topic leaves the production consumer group alone. Today the capture would join the pipeline's own `consumer_group`. Entry 1 gave the rewrite a `DebugGroup` option (`rpcn-debug-<os-user>`), but nothing uses it.

**Approach:** the thinnest path through launch, Session, rewrite, capture and a real Redpanda.
- **Override:** when `Options.DebugGroup` is set, the rewrite sets the top-level input's `consumer_group` to `<DebugGroup>-0` (D5) in the capture config. That covers the `redpanda` input and the deprecated `kafka_franz`, which share the field. The launch shows one warning naming the replaced group and the debug group.
  - Only a literal, non-empty `consumer_group` on the top-level input is touched. Nested inputs, other group fields and interpolated values wait for entries 3 and 5.
- **Never-ack test:** a new integration test runs a session against a Redpanda container through `test/dapdrive`, using a `redpanda` input with `consumer_group: prod`. It proves three things:
  - the session delivers the produced messages;
  - neither the production group nor the debug group has a committed offset afterwards;
  - a second session captures the same messages.
- **F5 sample:** `testdata/tracer/kafka.yaml`, for the acceptance run (entry 11).

## Boundaries & Constraints

**Always:**
- With an empty `DebugGroup` (every existing test and golden), the rewrite output is byte-identical to today's.
- The override is applied in the capture config only (AD-4(a)); the step config has no input.
- The integration test:
  - uses the existing container helper (`startRedpanda`) and the build tag `integration`;
  - reads committed offsets with `github.com/twmb/franz-go/pkg/kadm`, already an indirect dependency, so promote it;
  - uses a single-partition topic with the records produced before the session starts.
  "The same messages" means the same record values, in order, in the delivered batch of both sessions.
- The sample reads its broker from `${REDPANDA_BROKERS:localhost:9092}` and contains no credentials.

**Never:**
- No input classes, no `keepConsumerGroup`, and no recursion into `broker` or `sequence` (entries 3–5).
- No change to how capture stops or nacks: never-ack holds by construction.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Override | `redpanda.consumer_group: prod`, `DebugGroup` = `rpcn-debug-jo` | The capture config has `consumer_group: rpcn-debug-jo-0`; one launch warning names `prod` and `rpcn-debug-jo-0` | none |
| Deprecated input | `kafka_franz.consumer_group: prod` | Same override and warning | none |
| No group | `redpanda` with no `consumer_group` | Unchanged; no warning | none |
| Interpolated group | `consumer_group: ${GROUP}` | Unchanged in this entry (entry 5 classes it) | none |
| No option | `DebugGroup` empty | Unchanged | none |
| Real broker | 3 records in a single-partition topic, two sessions in a row | Both deliver the same 3 values; `prod` and `<DebugGroup>-0` have no committed offsets | none |

</frozen-after-approval>

## Code Map

- `debugger/rewrite/rewrite.go` `Rewrite`: `capture := without(input, "processors")` (~110) is the capture config node, encoded into `plan.Input`. Apply the override on that node, and use `w.warn` for the warning. `Plan.Options` (entry 1) holds `DebugGroup`. Helpers: `field`, `value`, `isInterpolated`, `scalar`.
- Goldens: `debugger/rewrite/testdata/<name>.{yaml,golden}`. The golden harness (`rewrite_test.go`, `TestGolden`) calls `Rewrite` with empty options. For the override cases, add a table test with a `DebugGroup`, or extend the harness to read an option for specific cases, keeping all existing goldens byte-identical.
- `test/integration/redpanda_test.go`: `startRedpanda(t)` and the franz-go producer pattern in `TestRedpandaSmoke`. The new test belongs in a new file (e.g. `test/integration/neverack_test.go`) in the same package.
- `test/dapdrive/dapdrive.go`: `Run(ctx, Options{Pipeline, Launch, Script})` returns `Result{Stops, Output, Delivered}`. Use `Always(Continue)`. Through `Serve`, the Session supplies `DebugGroup` from the process user (`core/debuggroup.go`, unexported); the test can learn the group name from the launch warning text, or compute it the same way.
- `testdata/tracer/`: the manual F5 samples; add `kafka.yaml` beside `pipeline.yaml`.
- `go.mod`: promote `github.com/twmb/franz-go/pkg/kadm` to a direct requirement (`go mod tidy`).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/rewrite.go` -- override the top-level input's literal `consumer_group` with `<DebugGroup>-0`, plus the warning -- the tracer's rewrite step.
- [x] `debugger/rewrite/` tests -- the matrix rows Override, Deprecated input, No group, Interpolated group, No option -- the rewrite cases.
- [x] `test/integration/neverack_test.go` -- the Real broker row: two `dapdrive` sessions, then `kadm` offset checks for both groups -- Done when 4 (offsets and rerun).
- [x] `testdata/tracer/kafka.yaml` -- the F5 sample -- for the acceptance run.
- [x] `go.mod`/`go.sum` -- `kadm` as a direct dependency.

**Acceptance Criteria:**
- Given a Redpanda topic with 3 records and a pipeline whose `redpanda` input has `consumer_group: prod`, when two debug sessions run to the end, then each delivers the same 3 values, and `kadm` shows no committed offsets for `prod` or the debug group.
- Given `task ci`, then it exits 0 with every existing golden byte-identical.

## Implementation Notes

- `walker.overrideGroup` (`rewrite.go`) runs on the capture node right after `without(input, "processors")`. It copies the component mapping and the group scalar it replaces (keeping the scalar's line, so `FilePosition` still maps a lint error there to the user's line), so the parsed input stays untouched. The warning is `input.<component>.consumer_group (line N): consumer group "prod" is replaced by the debug group "rpcn-debug-<user>-0" for the capture`.
- Skipped (unchanged, no warning): no option, a missing/null/empty group, an interpolated group, a non-scalar group. go.sum needed no change.
- `TestDebugGroupOverride` covers the five matrix rows plus an empty group; the non-override rows compare the whole rendered Plan with the no-option Plan. The goldens still run with empty options and are unchanged.
- `TestNeverAck` learns the debug group from the launch warning; it accepts `GROUP_ID_NOT_FOUND` from `FetchOffsets` as "no commits".
- Observed while checking the sample against a container: when capture stops the input, a franz-go `Kafka broker read failed ... context canceled` WARN reaches the Debug Console. It is cosmetic and left as is.

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 2, false 0, maybe-false 0. 3 patched, 1 deferred. The orchestrator added a fifth row, from a mutation check run before the review.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | The test can't tell whether the capture joined `prod`: with never-ack nothing is committed for any group, so the offset checks pass even with the original config | medium | patch | A live `prod` member (`startProdMember`, DisableAutoCommit) stays Stable through both sessions; afterwards `prod` must have that one member and the same generation (taken from the member's `GroupMetadata()`, since kadm v1.18.0's DescribeGroups doesn't report it). Implementer's mutation (override keeps `prod`): the test fails after 35 s with "a capture that waits for ever may have joined "prod"" |
| 2 | A never-created debug group passes the offset check trivially (GROUP_ID_NOT_FOUND is skipped) | medium | patch | Same root as #1 for the production side. A live member proves `prod` wasn't joined; the debug group's commits are checked after each session (#5) |
| 3 | The override warning uses the value's line, while every other rewrite warning uses the key's | low | patch | Now `gk.Line`; new row "value on its own line": the warning names line 5 and `FilePosition` still maps the value to line 6 |
| 4 | CLAUDE.md's Layout doesn't list `testdata/tracer/kafka.yaml` | low | defer | deferred-work.md, for E3's refactor sweep |
| 5 | (orchestrator) A committing session surfaced only as a 2-minute timeout in session 2 (mutation: the capture consumer acks) | medium | patch | `checkNoCommits` now runs after each session and each `dapdrive.Run` has a 30 s deadline. Re-run of the ack mutation: it fails in 11 s with "group rpcn-debug-aleksejjurcenko-0 has a committed offset 3 on neverack/0" |

## Verification

**Commands:**
- `go test -count=1 ./debugger/... ./test/dapdrive/ ./test/equivalence/` -- expected: PASS, with no existing golden changed.
- `task test:integration` -- expected: PASS (Docker running), including the new never-ack test.
- `task ci` -- expected: exit 0.

Run 2026-10-23 by the orchestrator after the review patches: `task ci` exit 0; no integration container left behind.
