---
title: 'Input table and input-tree walker'
type: 'feature'
ticket: '3'
created: '2026-10-23'
status: 'built'
baseline_revision: 'f4900e68232a14e4c6a13962889a7a95b65c4f8d'
route: 'full'
route_source: 'auto'
risk: 'high'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-input-table-and-input-tree-walker-input-classes.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** This is E3 entry 3 (CAP-2, D1). The debugger can't yet tell which inputs are safe to capture from. The research for this entry also found a hole in what has already shipped: with `auto_replay_nacks: false`, about 50 inputs treat the capture's nack as an ack. Kafka offsets get committed, the Postgres slot's LSN is confirmed, and CDC checkpoints advance.

**Approach:** All of this is in `debugger/rewrite` and touches nothing else.
1. **Input table:** one row per input component of the stock binary, taken from `story-input-table-and-input-tree-walker-input-classes.md`. Each row has a class predicate over the input's fields, its consumer-group fields, its start-offset fields, and an effect text. The user's decisions of 2026-10-23 override the research:
   - every row the research marks `?` is side-effecting, except `redpanda_common`, which stays replay-safe until entry 6's probe;
   - `http_client` with a method other than GET/HEAD, or a set `payload`, is side-effecting;
   - `socket` (a client) is side-effecting;
   - `dynamic` is rejected;
   - `subprocess`, `websocket` with `open_message`, and `sql_raw` are side-effecting;
   - the research's field predicates hold, e.g. `aws_s3` with `sqs`, `csv` with `delete_on_finish`, `sftp` with `watcher.enabled`, `kafka_franz`/`redpanda` with `start_offset: committed`;
   - an input with no row is unknown, which means side-effecting.
2. **Input-tree walker:** it follows `broker.inputs[*]`, `sequence.inputs[*]`, `read_until.input`, `batched.child`, `dynamic.inputs.<key>`, `azure_blob_storage.targets_input`, and `resource:` into `input_resources[label]`. Cycles are guarded, and a resource is classified once per Rewrite.
   - A composite takes its worst child's class, and so does a component that also holds a child input.
   - `Plan.Inputs` reports one finding per input: user path, line, component, class and effect, plus the composite's own row.
   - Group-consuming rows other than `kafka_franz` and `redpanda` are side-effecting until entry 5.
3. **The 3.2 group override** reads its group fields from the table. It stays limited to the top-level input in this entry.
4. **Never-ack for every input:** in the capture config, any input in the tree whose `auto_replay_nacks` isn't a literal `true` (`false`, or an interpolation) is set to `true`. The launch warns, naming the input.
5. **No refusals yet:** the rewrite refuses nothing on class (entry 4 adds that). `rejectStdin` stays as it is.

## Boundaries & Constraints

**Always:**
- The table is data, built once in one Go file, with one row per input. Each row cites its research evidence in a short comment where the class isn't obvious.
- Predicates read literal field values only. An interpolated value that would decide the class counts as the worse case.
- The rewrite stays pure: no env and no network.
- Existing goldens change only by the new `# inputs` section; their capture and step configs stay byte-identical. The exception is a golden that sets `auto_replay_nacks` to something other than `true`.
- An absent `auto_replay_nacks` is left absent, because its default is already `true`.

**Never:**
- No class-based refusal and no launch key; that's entry 4, including `inputClasses`.
- No group override beyond the top-level `redpanda`/`kafka_franz` input; that's entry 5.
- No change to the step config, except that the forced `auto_replay_nacks: true` in `input_resources` also reaches the step stream, which carries the same `*_resources` and never builds an input from them, so it is inert there (user decision, 2026-10-23, from the review).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Plain safe | `generate` | One finding: replay-safe | none |
| Field-dependent | `aws_s3` without `sqs` / with `sqs.url` | replay-safe / side-effecting, with the effect | none |
| Composite | `broker.inputs: [generate, aws_sqs]` | Findings for both children and the broker; the broker is side-effecting | none |
| Resource ref | `input: {resource: q}` with `input_resources: [{label: q, aws_sqs: …}]` | The finding for `q` is side-effecting and the input takes that class | none |
| Resource cycle | resource `a` → `broker` → `resource: b`, `b` → `resource: a` | Terminates; each is classified once | none |
| Unknown | an input with no table row | unknown, meaning side-effecting | none |
| Rejected | `http_server`, `dynamic` | rejected (refusal comes in entry 4) | none |
| Nack toggle | `kafka_franz.auto_replay_nacks: false` | The capture config has `true`; one warning names the input | none |
| Nested toggle | a `broker` child with `auto_replay_nacks: ${X}` | Forced to `true` with a warning | none |

</frozen-after-approval>

## Code Map

- `debugger/rewrite/rewrite.go` `Rewrite`:
  - `rejectStdin` calls (~104–110) stay;
  - `capture := without(input, "processors")` (~130) and `w.overrideGroup(capture, opts.DebugGroup)` (~131);
  - the `Plan` struct (add `Inputs []InputFinding` with exported fields);
  - `w.warn`.
  - `overrideGroup` (~251) currently hard-codes `redpanda`/`kafka_franz` with `consumer_group`; make it read the table's group fields.
- `debugger/rewrite/stdio.go`: `walkKeys`/`walkPairs` (a generic key search, not an input walk) and `find`. The new walker is a separate, structural walk; don't build it on `walkKeys`.
- `debugger/rewrite/state.go`: `resourceBodies`. The label → body map pattern is how `input_resources` lookup should work too (resources by label, `resourcePath` for user paths).
- New files: `debugger/rewrite/inputs.go` (the table and classes) and `debugger/rewrite/inputwalk.go` (the walker), or one file if it reads better.
- Research: `story-input-table-and-input-tree-walker-input-classes.md` (beside this plan) has every row, predicate, group field and start-offset field, with evidence paths into the v4.115/v4.84 sources, and the list of inputs with the `auto_replay_nacks` toggle.
- Goldens: `debugger/rewrite/testdata/*.{yaml,golden}`, regenerated with `go test ./debugger/rewrite/ -update`. The golden writer is in `rewrite_test.go`; add an `# inputs` section, one line per finding.
- A table-driven test should cover every table row, so every row's class and predicate is exercised. The goldens cover the walker's shapes.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/inputs.go` -- the input table, with the classes per the Intent -- D1 table.
- [x] `debugger/rewrite/inputwalk.go` -- the walker, worst-child composition, resource cycle guard, `Plan.Inputs` -- D1 walker and findings.
- [x] `debugger/rewrite/rewrite.go` -- the findings in Plan; `overrideGroup` reads group fields from the table; force `auto_replay_nacks: true` in the capture tree, with the warning -- wiring and never-ack.
- [x] `debugger/rewrite/rewrite_test.go` -- the `# inputs` golden section and a table-driven row test -- coverage.
- [x] `debugger/rewrite/testdata/` -- goldens for each matrix row -- the walker shapes.

**Acceptance Criteria:**
- Given any stock-binary input, when the pipeline is rewritten, then `Plan.Inputs` gives its class per the table, and an unknown component is side-effecting.
- Given a capture tree with `auto_replay_nacks: false` anywhere, when rewritten, then the capture config has `true` there and the launch warns about each one.
- Given `task ci`, then it exits 0, and existing goldens differ only by the `# inputs` section.

## Implementation Notes

- `inputs.go` holds `InputClass` (`replay-safe` < `side-effecting` < `unknown` < `rejected`, the worst-child order), `InputFinding` and `inputTable` (82 rows, matching `WalkInputs` of the stock env; `tigerbeetle_cdc`/`zmq4` are cgo-only and need no row). Predicate helpers (`isSet`, `boolIs`, `strIs`, `strNotIn`) treat an interpolation as the deciding (worse) case.
- `websocket` is a research `?` row, so it is side-effecting whether or not `open_message` is set (user decision: the blanket rule wins); `open_message` only changes the effect text. `http_client` follows its specific decision (side-effecting with a verb other than GET/HEAD or a `payload`), so a plain GET stays replay-safe.
- Group rule: a row with `groups` whose group field is set (literal non-empty or interpolated) and wasn't replaced by the override is side-effecting ("joins consumer group …"). Only `overridable` rows (`redpanda`, `kafka_franz`) at the top level are replaced, so a nested one, `kafka` and `redpanda_common` with a group are side-effecting until entry 5. `start_offset: committed` (or interpolated) is side-effecting on the franz family.
- `inputwalk.go`: pre-order findings (a composite's row before its children; a resource's finding where it is first named; each resource's class is computed once per Rewrite by `resourceClasses`: a local walk of its subtree with `resource:` children as references, then the worst class propagated to referrers to a fixed point, so every member of a cycle gets the worst member's class). A composite's effect is `through <child path>: <child effect>`. An undefined or interpolated `resource:` is unknown. `auto_replay_nacks` other than a literal `!!bool true` (false, `"false"`, `${X}`) is replaced with `true` in place, with a warning; absent or null is left alone. For `input_resources` this lands in `Plan.Resources`, which the step stream also carries but never builds an input from (benthos builds input resources lazily).
- `overrideGroup` reads the row's group fields and returns the replaced nodes, which the walker skips.
- `inputs_test.go`: `TestInputTableIsStock` (drift against the stock env, links `components/all`, adds ~15 s to the first build of the rewrite tests), `TestInputTableRows` (a case for every row, both sides of every field-dependent row), `TestNestedGroupKept`. New goldens `inputs-*.yaml` cover the matrix.

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 0, false 0, maybe-false 0, plus 2 deviations from the frozen intent that went to the user. 1 patched; both deviations were decided by the user.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | A resource on a reference cycle is cached with a partial class: `visiting` returns replay-safe, so the resource that closes the cycle keeps it (repro: `resource: b` reported replay-safe while `b` → `a` holds `aws_sqs`) | medium | patch | `resourceClasses()` classifies each `input_resources` entry once and propagates the worst class along references to a fixed point; new golden `inputs-resource-cycle-side-effecting`: all 7 findings side-effecting |
| 2 | Plain `websocket` is replay-safe, but the frozen intent makes every `?` row except `redpanda_common` side-effecting | intent | user decided | User 2026-10-23: side-effecting regardless of `open_message` (the blanket rule wins); the test case was updated |
| 3 | The forced `auto_replay_nacks: true` in `input_resources` reaches the step config, against the frozen "no change to the step config" | intent | user decided | User 2026-10-23: accepted (inert in stepping, which never builds an input resource); the frozen Never line was amended to say so |

## Verification

**Commands:**
- `go test -count=1 ./debugger/rewrite/` -- expected: PASS.
- `git diff --stat debugger/rewrite/testdata` -- expected: only `# inputs` sections added to existing goldens, plus new golden files.
- `task ci` -- expected: exit 0.

Run 2026-10-23 by the orchestrator after the review patches: `task ci` exit 0; existing goldens only gained `# inputs` lines.
