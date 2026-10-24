---
title: 'Explicit top-level allowlist and the redpanda block'
type: 'feature'
ticket: '6'
created: '2026-10-23'
status: 'built'
baseline_revision: 'd6423c49535e4b173cf0b6c700ace3f70e83e285'
route: 'oneshot'
route_source: 'auto'
risk: 'medium'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** This is E3 entry 6 (CAP-2; D8). Two problems:
- The rewrite passes only `input`, the steppers and `*_resources` to the engine, but that list is implicit (a comment in `rewrite.go`).
- The top-level `redpanda` block is dropped silently.

The entry's unknown is now settled. The shared Redpanda client that `redpanda_common`, and a `redpanda` input with no `seed_brokers`, read from `__redpanda_global` is created only by the `rpk connect` CLI (`connect internal/cli/enterprise.go`, `GlobalRedpandaManager.InitFromParsedConfig`). `StreamBuilder` never creates it, and the public `schema` package only adds the `redpanda` field. So in a debug session those inputs can't connect, and nothing would use the block's `logs_topic`/`status_topic`.

A probe through `bin/rpcn-debug` against a local Redpanda (2026-10-23) stopped even earlier: "this feature requires a valid Redpanda Enterprise Edition license", because the debugger doesn't apply licenses yet (AD-22). No logs topic was created.

**Approach** (D8's fallback branch):
- **Explicit allowlist:** one named list in `debugger/rewrite` of the top-level sections and what happens to each:
  - `input` goes to capture;
  - `pipeline` goes to the steppers;
  - `output` positions delivery;
  - `*_resources` carry over;
  - `error_handling` is applied as strict;
  - `buffer` is dropped with its warning (from 2.x);
  - `metrics`, `tracer`, `http`, `logger`, `shutdown_delay`, `shutdown_timeout` and `tests` are dropped silently;
  - `redpanda` is dropped with a warning: "the top-level redpanda block isn't used in a debug session: its shared client, logs_topic and status_topic are only set up by rpk connect".
  - Any other top-level key is left to the engine's lint, as today.
- **Rejected rows:** in the input table, `redpanda_common` becomes rejected, with an effect naming the shared client that only `rpk connect` sets up. A `redpanda` input without `seed_brokers` becomes rejected for the same reason. `redpanda` with `seed_brokers` stays as it is.
- The README's input section and AD-5's allowlist sentence follow.

</frozen-after-approval>

## Implementation Notes

Oneshot: a named allowlist in `rewrite.go`, a warning, two class predicates in `inputs.go`, plus goldens and a Serve test (1011 for `redpanda_common`). The rejected rows need no Redpanda, because the launch is refused before anything connects.

- `rewrite.go`: a `section` type and the `topLevel` map (used: input, pipeline, output, error_handling; dropped: buffer, redpanda, metrics, tracer, http, logger, shutdown_delay, shutdown_timeout, tests). `*_resources` are carried by their suffix, as before. A non-null top-level `redpanda` gets one warning at its line.
- `inputs.go`: `noSharedClient`; `redpanda_common` is `always(Rejected, …)` (no longer overridable); `redpanda` uses `redpandaInput`, which is rejected without `seed_brokers`, else the franz `committed` rule. `core.FixedInputs` gains `redpanda_common`, so `inputClasses` can't loosen it (`TestFixedInputs` keeps the two texts equal).
- Tests:
  - `TestTopLevelAllowlist` checks `topLevel` against the stock schema's top-level fields (`schema.Standard(...).TemplateData()`). A mutation that removes `tests` from the map makes it fail.
  - `TestRedpandaBlockDropped` checks that `__logs` reaches no engine config, and that the warning is given.
  - The `redpanda_common`/`redpanda` rows were updated (`redpanda_common` is no longer field-dependent), and the redpanda_common row was removed from the entry-5 override matrix.
  - `TestServeInputClasses` gains "rejected redpanda_common" and "rejected redpanda without seed_brokers" (1011 at the input). This needs connect `components/kafka/enterprise`, and a `consumer_group` in the sample, because the lint runs before the refusal.
- README (the rejected row and the section allowlist), AD-5's top-level sentence, and a memlog entry.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 3, false 0, maybe-false 0. All 5 patched by the orchestrator (oneshot).

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | README, AD-5 and the `keepConsumerGroup` schema description still say `redpanda_common`'s group is overridden | low | patch | All three now name `redpanda` with `seed_brokers` (AD-5 says `redpanda_common` is rejected); `task gen:schema` regenerated `vscode/package.json` |
| 2 | `inputClasses: {redpanda: replay-safe}` loosens a seedless `redpanda`, which then can't connect | medium | patch | `hardRejected` (FixedInputs plus `redpanda` without `seed_brokers`): no `inputClasses` override applies. `TestSeedlessRedpandaStaysRejected` |
| 3 | A seedless `redpanda` still gets its group replaced (a `-k` index and a warning) before it is rejected, shifting later inputs' `k` | medium | patch | `overrideGroups` is skipped for hard-rejected inputs; the same test checks the next `kafka_franz` gets `dbg-0` and the seedless group stays `a` |
| 4 | `topLevel` doesn't drive the rewrite: nothing reads it, and `used`/`dropped` change nothing | low | patch | Entries carry their drop warning (`buffer`, `redpanda`), and `Rewrite` loops over the sections in file order. `TestTopLevelAllowlist` also checks that no dropped section's content reaches an engine config. Goldens unchanged |
| 5 | The `topLevel` doc comment is unclear ("both streams") and overlong | low | patch | Rewritten |

## Verification

**Commands:**
- `go test -count=1 ./debugger/...` -- expected: PASS.
- `task ci` -- expected: exit 0.

Run 2026-10-23 after the review patches: `go test ./debugger/...` PASS; `task ci` exit 0.
