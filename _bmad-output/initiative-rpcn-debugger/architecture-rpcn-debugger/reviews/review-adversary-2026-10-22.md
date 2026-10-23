---
review-of: architecture-rpcn-debugger.md (status: final, updated 2026-10-22)
lens: 'Attack the spine: two units one level down, each compliant with every AD, that still build incompatibly'
reviewer: adversarial independent reviewer
date: 2026-10-22
focus: holes epic-safe-against-real-systems (E3) would hit, and holes the 2026-10-22 amendments introduced
evidence: spine + .memlog.md (uncommitted 2026-10-22 diff), epic-safe-against-real-systems.md, deferred-work.md, epic-tracer-retrospective.md, code at 5206563 (debugger/rewrite/rewrite.go, stdio.go; debugger/engine/engine.go, capture.go, sink.go; debugger/core/port.go, session.go, protocol.go; debugger/dap/launch.go), benthos v4.81.0 internal/manager/type.go, connect v4.112.0 internal/impl/kafka
---

# Adversarial review: rpcn-debugger spine, 2026-10-22 reconciliation

## Verdict

The E2 reconciliation is accurate about what was built. But it leaves the E3 surface (input safety, consumer groups, `output: real`/`none`) as one-line rules with no owner, no data shape and no failure semantics. Two E3 units can each follow AD-4/5/15/21 to the letter and still disagree on who refuses an input, what the real output is built from, and what `Delivered` means once a write can fail. The S1 amendment (a second env clone for capture) also adds a second resource manager and a second registration target, and AD-1 doesn't account for either.

I found 11 compliant-but-incompatible pairs: 2 Critical, 4 High, 4 Medium and 1 Low. Seven of them are on E3's path. H1–H6 should be closed before E3 is sliced.

Severity: **Critical** means the units can't be integrated without rewriting one of them. **High** means they integrate but harm a real system or give wrong results in common pipelines. **Medium** is a corner case or a UX mismatch. **Low** is wording.

| # | Hole | ADs | Sev | E3? | From 10-22? |
|---|---|---|---|---|---|
| H1 | Input safety has two deciders and no shared input-tree walk | AD-4, AD-5, AD-6, AD-15 | Critical | yes | partly (stdin rule) |
| H2 | `output: real` has no config shape or owner | AD-4, AD-6, AD-15, AD-21 | Critical | yes | yes (AD-21 composite clause) |
| H3 | `Delivered` semantics once a write can fail, or with `none` | AD-3, AD-8, AD-12, AD-21 | High | yes | yes (AD-3 assumption, STRICT_REJECTED) |
| H4 | `keepConsumerGroup` keeps Kafka "replay-safe" while it joins the production group | AD-5 | High | yes | no |
| H5 | The group override collapses distinct groups; start offset, capture wait and os-user have no owner | AD-4, AD-5, AD-22 | High | yes | no |
| H6 | Restart/replay under `output: real` writes the batch to production again | AD-5, AD-20, AD-21 | High | yes (contract) | no |
| H7 | Two env clones and two resource managers per run | AD-1, AD-3, AD-4, AD-8, AD-17, AD-22 | Medium | yes | **yes (S1)** |
| H8 | Side-effecting processors run in dry-run and during capture | AD-4, AD-5, AD-21 | Medium | yes | worsened by S1 |
| H9 | Dropped sections: the spine has a denylist, the code an allowlist; the top-level `redpanda` block is unspecified | AD-4, AD-5 | Medium | yes | no |
| H10 | The error-code registry is pinned but not closed; E3 needs new codes | AD-3, AD-5, AD-12 | Medium | yes | **yes** |
| H11 | Text contradictions the amendments left behind | AD-3, AD-6, AD-7 | Low | no | **yes** |

---

## H1. Input safety has two deciders and no shared input-tree walk [Critical, E3]

- **Unit A (U-REWRITE, E3 story "input classes") builds:** the AD-5 class table in `debugger/rewrite`, keyed by component name. `Rewrite` refuses side-effecting and rejected inputs itself, because the table lives there and the rewrite already refuses `stdin` (`rewrite/stdio.go rejectStdin`, unconditionally, with `PIPELINE_INVALID` and no replay hint).
- **Unit B (U-CORE/U-DAP, E3 story "launch options"; later E5 replay) builds:** `allowSideEffects` and `replay` as `Launch` fields. The Session refuses after `Check`, because only it sees the launch options, and E5's handoff says "a replay session must bypass the input-class rejection". `Rewrite(src []byte)` and `engine.Loader func(pipeline string)` take no options today, so the rewrite can't make that call.
- **Why both comply:** AD-5 puts the *table* in `debugger/rewrite` but doesn't say who *decides*. AD-15 says stdin is rejected "in stdio mode". AD-5 says it is rejected always, as "replay only". AD-4 says U-STEP never parses configs, which is silent on U-CORE deciding.
- **How they clash:**
  1. The refusal happens twice with different codes and texts, or the replay bypass has nowhere to go. A replay session with a `stdin` input is still refused by `rejectStdin`.
  2. **Recursion is not shared.** `rejectStdin` uses `walkKeys`, which matches any key named `stdin` anywhere in the input tree, including user map keys, and walks `input_resources` wholesale. AD-5's group-field table "recurses into `broker` and `sequence`" only. A third walk for classes will pick its own set. So `input: {resource: q}` with `input_resources: [{label: q, aws_sqs: …}]`, or `read_until.input`, is classified by one walk and missed by another.
  3. **Class is field-dependent, not name-dependent.** `aws_s3` is listed replay-safe, but `aws_s3` with `sqs.url` set receives SQS messages, and a receive (not an ack) bumps the receive count and can redrive the message to a DLQ. A name-keyed table gets this wrong. Never-ack neutralises only ack-gated effects (`delete_objects`, `delete_on_finish`), not receive-gated ones.
- **Tightening (AD-5, new "Input safety" bullet):**
  > `debugger/rewrite` owns one **input table** (rows: component, class predicate over its fields, consumer-group fields, start-offset fields, effect text) and one **input-tree walker** (`broker.inputs`, `sequence.inputs`, `read_until.input`, `resource:` → `input_resources`). Class, group override and the stdin rule all use them. A composite's class is its worst child's. The rewrite never refuses on class: it returns `Plan.Inputs []InputFinding{userPath, line, component, class, effect}`. The Session alone refuses, against `Launch.allowSideEffects` and `Launch.replay` (a replay launch skips the check). AD-15's stdin rule becomes a rejected-class row.

## H2. `output: real` has no config shape or owner [Critical, E3]

- **Unit A (U-REWRITE) builds:** the step config's output as `broker: {pattern: fan_out, outputs: [rpcn_debug_sink, <original output block>]}`. That is a rewrite of the config, which only the rewrite may do (AD-6).
- **Unit B (U-STEP) builds:** a `rpcn_debug_sink` spec with `service.NewOutputField("output")`, so the sink records the batch and then forwards it to an `OwnedOutput`. AD-21 says "In `real` it also forwards each batch to the user's output, built from the original block". Unit B has the rewrite pass the original block in a new `Plan.Output` string.
- **Why both comply:** AD-21 says the sink forwards but not who builds the forwarded output or from what. AD-4's ParseYAML ban covers processors only. AD-6 says only the rewrite parses YAML, but `ParseYAML` on an output string is the engine parsing, so it isn't obviously banned.
- **How they clash:**
  1. **`output.processors` run twice.** The "original block" holds the top-level `processors`, which already ran in the output stepper. Unit B forwards the original block verbatim, and production receives doubly processed messages. Unit A's fan_out gives the same result.
  2. **Ordering and ack differ.** fan_out writes the sink and the real output in parallel and acks when both succeed, so `Delivered` comes before the real write. Unit B's forward makes `Delivered` follow the write (H3). `MaxInFlight = 1` holds only in Unit B.
  3. **The AD-15 stdout redirect under `real`.** For `broker: {outputs: [kafka, stdout]}`, the rule says "redirect to the dry-run sink". Unit A replaces the child with a second `rpcn_debug_sink`, so every batch emits `Delivered` twice and the numbering doubles. Unit B replaces it with `drop: {}`.
  4. Today `Plan` has no output at all (`rewrite.go` only positions `Delivery` and warns), so E3 has to invent the shape.
- **Tightening (AD-21):**
  > Under `real`, `debugger/rewrite` emits `Plan.Output`: the user's `output` block **without its top-level `processors`**, with every `stdout`/`stderr` output in it replaced by `drop: {}` (warned). The step config's output stays exactly one `rpcn_debug_sink`, whose spec has `mode` and an `output` field (`NewOutputField`); the engine never parses an output string. The sink writes to the user's output synchronously inside `WriteBatch` (`MaxInFlight = 1` in every mode). Under `dry-run` and `none`, `Plan.Output` is empty and `output_resources` are still carried (they're lazy, so nothing opens).

## H3. `Delivered` semantics once a write can fail, or with `none` [High, E3]

- **Unit A (U-STEP sink, E3) builds:** `Deliver` called when the batch arrives (as today, `sink.go` calls `arr.Deliver` before returning `nil`), then the forward. A failed write returns its error, so benthos nacks back to the producer. If the inner output returns `ErrNotConnected`, the async writer reconnects and calls `WriteBatch` again, and Unit A records and delivers the batch a second time.
- **Unit B (U-CORE, the E3 refactor sweep for the AD-3 assumption) builds:** `Delivered.Batch` numbered on each `Deliver`, with "Nothing was delivered to dry-run output" when the count is 0, and the strict note "in production the input would see them nacked" (`session.go:375-385`). Both texts hard-code dry-run. For `none` it reasons that "discards them" (AD-21) means no `Delivered`, so it prints "Nothing was delivered".
- **Why both comply:** AD-21 defines what the sink *receives*, not when `Delivered` is emitted or whether it is emitted under `none`. AD-3 adopts Session numbering as an `[ASSUMPTION]` while the code has two counters. `STRICT_REJECTED` is "a run outcome" with no rule for how it is detected.
- **How they clash:**
  1. One batch can show as delivered twice, or as delivered although the real write failed.
  2. Under `none`, the frontend may show nothing or may show the batches. Either way the console text says "dry-run".
  3. **Mixed-batch strict** (deferred-work, 2.14): `engine.go` picks `STRICT_REJECTED` by substring match on the producer error. Under `real`, a write failure of the passing messages folds into the same `batch.Error`, so a real production write failure is reported as the benign strict note.
- **Tightening (AD-21 + AD-3):**
  > The sink calls `Deliver` exactly once per batch it receives, **after** the forward in `real`, with `Delivered.Outcome ∈ {recorded, written, discarded, failed}` and the write error. It never returns `ErrNotConnected` upward without the outcome already recorded. `Delivered` is emitted in all three modes, and `none` only skips the forward. The Session owns `Batch` numbering and the mode-aware texts, and the DAP server's counter is removed (fix in the E3 sweep, as the assumption says, and make it a rule, not an assumption). `STRICT_REJECTED` is decided from the batch the last stepper hands on (the messages that carry an error), never from error text. A write failure is `RUN_FAILED` or a new `OUTPUT_FAILED` (see H10).

## H4. `keepConsumerGroup` keeps Kafka "replay-safe" while it joins the production group [High, E3]

- **Unit A (rewrite, class table) builds:** Kafka family → replay-safe, by the row "Kafka family with a group override".
- **Unit B (rewrite, group override) builds:** with `keepConsumerGroup: true`, no override. The two tables are separate (AD-5 describes them separately), so the class doesn't change.
- **Why both comply:** the row's "with a group override" is a qualifier nobody owns. Nothing says what the class is *without* the override.
- **How they clash:** `keepConsumerGroup: true` with no `allowSideEffects` passes. The capture joins the production group, which triggers a rebalance: production consumers lose their partitions while the debug session holds them, and lag builds up. This is exactly AD-5's "Prevents: rebalancing production data", and it happens even though nothing is acked. The never-ack test in E3's Done when 4 passes anyway, because it checks only offsets.
- **Tightening (AD-5):**
  > The class is computed **after** the group override. A group-consuming input whose group is kept (`keepConsumerGroup`, or a group field the table can't override, such as an interpolated one) is side-effecting, with the effect "joins consumer group G; its members rebalance". The never-ack test also asserts that the production group's membership generation didn't change.

## H5. The group override collapses distinct groups; start offset, capture wait and os-user have no owner [High, E3]

- **Unit A (rewrite) builds:** every known group field set to `rpcn-debug-<os-user>`. It reads `os/user.Current()` itself.
- **Unit B (Session/engine) builds:** the debug group derived from the Session's env lookup (`USER` from `env`/`envFile`, AD-22), passed to the rewrite as an option. The code comment in `rewrite.go` already says "the rewrite has no env (AD-22)".
- **Why both comply:** AD-5 fixes the name pattern but not who computes `<os-user>`, nor what happens with several group fields.
- **How they clash:**
  1. There are two sources for the user name, so Check and Start can disagree, and they already re-read the file separately (S6).
  2. **Distinct groups merge.** `broker.inputs: [{kafka_franz: {topics: [t], consumer_group: a}}, {kafka_franz: {topics: [t], consumer_group: b}}]` reads every message twice in production. With both groups set to the same debug group, the two children split the partitions, and the capture differs from production.
  3. Two VS Code windows of the same user share a group and rebalance each other. One capture can get no partitions and wait forever.
  4. **Start position.** A fresh debug group follows the input's `start_offset`/`start_from_oldest`. With `latest`, capture waits for new traffic, and E3's "a rerun captures the same messages" fails. With `earliest`, it captures the oldest retained data, not what production would read next. AD-4 has no capture wait rule: `NOTHING_CAPTURED` only fires when the input *closes*, and a Kafka input never closes.
- **Tightening (AD-5 + AD-4):**
  > The Session supplies the debug-group base (`rpcn-debug-<os-user>`, from the process user, not the pipeline env) as a rewrite option. The rewrite is pure. Each overridden field gets `<base>-<k>` (k = the input's index in walker order) so distinct groups stay distinct. Start-offset fields are left as configured, and the launch warns when one resolves to latest. While capture has received nothing after 5 s, the Session emits one console note ("waiting for the input …"). Capture ends only with a batch, an input close (`NOTHING_CAPTURED`), Pause or Terminate. Decide whether a per-session suffix is wanted to keep concurrent sessions apart.

## H6. Restart/replay under `output: real` writes the batch to production again [High, E3 contract, built by E5]

- **Unit A (E3) builds:** `real` forwarding per AD-21. Each batch the sink receives goes to the user's output.
- **Unit B (E5) builds:** Restart per AD-20, which re-steps the already captured batch through the same step config, with no mention of the output mode.
- **Why both comply:** AD-20 and AD-21 each follow their own rule. AD-5 only guards inputs.
- **How they clash:** every Restart writes the same batch to the production output again: duplicate orders, duplicate events. A `replay` launch with `output: real` does the same with stale data. Each unit behaves exactly as its AD says.
- **Tightening (AD-20 + AD-21), decided now because E3 fixes the `real` contract:**
  > A Restart or `replay` session with `output: real` writes only when the launch also sets `allowRepeatWrites: true`. Otherwise it runs as `dry-run` with a console note. Name this in E5's Done when.

## H7. Two env clones and two resource managers per run [Medium, from the S1 amendment]

- **Unit A (E7 local-release, RPC plugins; or any registration) builds:** per AD-1, "on that clone it registers `rpcn_debug_stepper`, `rpcn_debug_sink`, and the launch's RPC plugins". So it registers on `run.env` only.
- **Unit B (as built, S1) builds:** `run.capEnv`, a second clone with a no-stop stepper (`engine.go newRun`).
- **Why both comply:** AD-1 talks about "the clone" (singular). The AD-4 amendment adds a second clone without changing AD-1. AD-22 says the license is applied "on every stream", which covers that part but not registration.
- **How they clash:**
  1. An RPC-plugin input exists only on `env`, so the capture stream fails with "unknown input". Restart "discards the cloned env", so E5 may discard only one of them.
  2. **Two resource managers.** Both streams get the whole `Resources` blob. benthos v4.81 builds resources lazily (`internal/manager/type.go` `LazyAdd` for inputs, caches, processors and outputs), so nothing opens twice. But a `memory` cache or a local `rate_limit` used by an input-tree processor (which runs in the capture stream) and by a pipeline processor (step stream) is now two separate states. A `dedupe` or `cache` write during capture is invisible to the pipeline, so the output differs from production (AD-8).
  3. The no-stop arriver lives in `engine` and calls `core.PushFrame` itself, while AD-3/AD-17 say only the Session changes frames.
- **Tightening (AD-1 + AD-4):**
  > A run has one clone **per stream** (capture, step). Every registration except the sink (the stepper, RPC plugins), and the license, apply to each clone. Restart discards both. The rewrite warns when a `cache_resources`/`rate_limit_resources` entry is reachable from both the input tree and the step tree ("state isn't shared between capture and stepping"), and the AD-8 corpus gets one case for it. The no-stop arriver is a `core` type, so frames stay core-owned.

## H8. Side-effecting processors run in dry-run and during capture [Medium, E3]

- **Unit A (rewrite/E3) builds:** AD-5 safety for inputs and the output only, as written.
- **Unit B (extension, docs, CAP-11 wording) promises:** "results go to VSCode only" for `dry-run`.
- **Why both comply:** AD-5's Prevents names "writing to production outputs", and processors aren't outputs.
- **How they clash:** `sql_insert`, `sql_raw` with an `INSERT`, `redis` with a write command, `cache` set/delete on a remote cache, `http` with POST, `aws_lambda` and `command` all write to production systems in dry-run. Input-tree processors (`broker.inputs[].processors`, the resources they call) do so during capture, unstepped, so the user can't even pause before them (S1). The first E3 user who points at production and trusts "dry-run" hits this.
- **Tightening (AD-5):**
  > A processor effect table in `debugger/rewrite` (component plus field predicate). Every matching processor gets a launch warning naming the system it writes to, and the warning says "runs during capture, not stepped" when it is in the input tree. Whether it also requires `allowSideEffects` is a user decision.

## H9. Dropped sections: the spine has a denylist, the code an allowlist [Medium, E3]

- **Unit A (as built) builds:** only `input`, the steppers and `*_resources` reach the engine (`rewrite.go`: "only the input, the steppers and the *_resources sections reach the engine").
- **Unit B (an E3 developer reading AD-5) builds:** drop `metrics`, `tracer`, `http` and `logger`, and carry everything else, so `redpanda_common` can connect.
- **Why both comply:** AD-5 lists four dropped sections, and AD-4 lists two carried ones (`*_resources`, `error_handling`).
- **How they clash:** `redpanda_common` (Kafka family, so "replay-safe") connects through the shared client that the top-level `redpanda` block configures (`connect v4.112 internal/impl/kafka/enterprise/redpanda_common_input.go`, `FranzSharedClientUse(SharedGlobalRedpandaClientKey, …)`). Under Unit A it can't connect. Under Unit B the same block's `logs_topic`/`status_topic` (`global_redpanda_writer.go`) ship debug-session logs and status to the production cluster. That is telemetry AD-5 forbids.
- **Tightening (AD-5):**
  > The rewrite carries an **allowlist**: `input`, the processor lists, `output` (H2), `*_resources`, `error_handling`, and `redpanda` with only its connection fields (`logs_topic`, `status_topic`, `pipeline_id` removed). Everything else is dropped. Verify whether `StreamBuilder` wires the global `redpanda` client at all. If it doesn't, `redpanda_common` is a rejected-class row with that reason.

## H10. The error-code registry is pinned but not closed [Medium, E3; from the 2026-10-22 AD-3 amendment]

- **Unit A (rewrite) builds:** a rejected input as `PIPELINE_INVALID` at the input path (the `rejectStdin` precedent).
- **Unit B (Session, H1) builds:** a missing `allowSideEffects` as `LAUNCH_INVALID` (it's about the launch), or adds `SIDE_EFFECTS_REQUIRED` = 1011 to `errors.go`.
- **Why both comply:** AD-3 now pins numbers 1001–1010 and says "numbers are never reused". It makes adding a protocol *member* a spine change, but says nothing about adding an error *code*.
- **How they clash:** the same refusal gets different codes depending on which unit raises it. A future quick fix in the extension ("set allowSideEffects") can't key on either. Two E3 stories can also both take 1011.
- **Tightening (AD-3):**
  > The code registry is closed like the protocol: adding a code is a spine change. E3 adds `INPUT_REJECTED` 1011 (with a replay hint), `SIDE_EFFECTS_NOT_ALLOWED` 1012 (`UserPath`/`Line` at the input, the effect in `Message`), and, if H3 takes it, `OUTPUT_FAILED` 1013.

## H11. Text contradictions the amendments left behind [Low]

- AD-6 says "if `error_handling.strict` is true, **every** stepper is written with `strict: true` (AD-7)", while AD-7's table has `catch` "written `strict: false`". A new rewrite contributor follows AD-6. Fix: "every stepper except `catch`".
- AD-3 is `[ADOPTED]` but carries an `[ASSUMPTION]` that the code contradicts (two batch counters). Promote it to a rule with its owner (H3), or mark that bullet as pending.
- AD-4's amendment says the no-stop arriver "pushes a frame", which conflicts with AD-3 ("only `core.Session` changes … frames") until H7 moves it into core.

## Checked and not a hole

- Lazy resource construction (benthos `LazyAdd`) means carrying `input_resources`/`output_resources` in both streams doesn't open them twice. Only the state split in H7 remains.
- Never-ack holds by construction across the two streams: the step stream's ack never reaches the capture input, and the capture consumer always returns `errNotAcked`.
- The interpolated-strict resolution (S2) uses one lookup for the engine and the steppers, so they can't disagree.
