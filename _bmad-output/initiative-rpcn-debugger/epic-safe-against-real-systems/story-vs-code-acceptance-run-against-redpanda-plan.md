---
title: 'VS Code acceptance run against Redpanda'
type: 'chore'
ticket: '11'
created: '2026-10-30'
status: 'built'
baseline_revision: '0cad9807a14db48427be072a9cfc686c9ff86541'
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

**Problem:** E3's Done when items 1, 2, 3 and 6 are user-facing. They hold only once a person sees them in VS Code, from the installed `.vsix` and the stock `rpcn-debug`, against a local Redpanda.

**Approach:** Manual F5 samples in `testdata/tracer/`, plus a checklist the user ticks in VS Code:
- `kafka.yaml` (from 3.2) covers the debug group and `keepConsumerGroup`.
- `kafka-real.yaml` covers `output: real`, `none` and dry-run to the `out` topic.
- `side-effecting.yaml` covers refusal 1012, `allowSideEffects` and `inputClasses`.
- `rejected.yaml` covers refusal 1011.
- `warnings.yaml` covers the processor-effect, state-split, capture-time and `redpanda`-block warnings.
- `output-failed.yaml` covers `OUTPUT_FAILED` 1013.

</frozen-after-approval>

## Checklist

Setup: the test Redpanda is on `localhost:19092` with topics `orders` (holding records) and `out`. Each launch sets `"env": {"REDPANDA_BROKERS": "localhost:19092"}`. Pass `-X brokers=localhost:19092` to every `rpk` command (the `local` profile points elsewhere), e.g. `rpk group describe <group> -X brokers=localhost:19092`.

Done when 2 (consumer groups):
- [ ] F5 on `kafka.yaml`: a launch warning says `"prod"` is replaced by `rpcn-debug-<os-user>-0`; the batch stops at the mapping; Continue delivers it. `rpk group describe prod` shows no new members or offsets; the debug group is Empty with no offsets. A second F5 captures the same records.
- [ ] `kafka-broker.yaml`: two warnings: `"prod-a"` (the broker's first child) becomes `rpcn-debug-<os-user>-0` and `"prod-b"` (the input resource `orders_b`) becomes `-1`; a batch is captured and delivered.
- [ ] `kafka.yaml` with `"keepConsumerGroup": true`: the launch is refused (1012), naming the `redpanda` input joining group `"prod"`. With `"allowSideEffects": true` added, it captures under `"prod"` with a side-effect warning and commits no offset.

Done when 1 (input classes):
- [ ] `side-effecting.yaml`: refused (1012), naming subprocess's effect. With `"allowSideEffects": true` it captures (warning naming the effect); with `"inputClasses": {"subprocess": "replay-safe"}` instead, it captures with the "can't prove it" warning.
- [ ] `unknown.yaml`: refused (1012), "the input resource is only known at runtime, so it is treated as side-effecting". With `"allowSideEffects": true` it captures `HELLO`.
- [ ] `rejected.yaml` (also with `"allowSideEffects": true`): refused (1011), "it is a server and waits for clients", with the replay hint.

Done when 3 (output):
- [ ] `kafka-real.yaml` with `"output": "real"`: warnings for the real output and the stripped batching; the Debug Console says "written to the output"; `rpk topic consume out -o -3 -X brokers=localhost:19092` shows the new records with `"seen": true`.
- [ ] `kafka-real.yaml` with `"output": "none"`: "not written (output: none)"; nothing new in `out`. With no `output` key (dry-run), "to dry-run output", nothing new in `out`.
- [ ] `output-failed.yaml` with `"output": "real"`: "failed to write: mkdir …", then the run ends with the output failure at `output, line 16`, no hang.

Launch warnings (CAP-11):
- [ ] `warnings.yaml`: four warnings: the child input's processor list runs during capture; the `redpanda` block isn't used; the `http` processor sends POST requests; the memory cache `seen` state is split. Stepping the pipeline's `cache` get shows the message erroring (the cache is empty while stepping).

Done when 6:
- [ ] `task ci` is green on the committed tree, and all of the above works from the installed `.vsix`.

## Implementation Notes

The oneshot route was chosen because the entry is samples and this checklist; there's no code.

- Every sample was run through `test/dapdrive` (a throwaway probe, not committed) with the stock environment against `localhost:19092`. Each gave the result the checklist states, and after the runs `prod` and the debug group had no committed offsets.
- `input.processors` at the top level are stepped, not run during capture. So the state-split and capture-time warnings need a processor in a nested input: `warnings.yaml` uses a `broker` child.
- The review asked for Done when 1's unknown class and Done when 2's per-input `k` and recursion, so two samples were added beside the Intent's list: `kafka-broker.yaml` (a broker child and an input resource, `-0` and `-1`) and `unknown.yaml` (an input resource named by an env var, so Unknown; it captures with `allowSideEffects`). Both were probed like the others.
- `subprocess` emits one message per batch, so `side-effecting.yaml` captures only its first line.
- Seen during the probe, cosmetic:
  - Every Kafka capture logs `level=WARN msg="Kafka broker read failed" … err="context canceled"` when the capture stops.
  - Under `OUTPUT_FAILED`, benthos's `Failed to send message to rpcn_debug_sink: …` error names the internal sink.
  - "Delivered batch 1 (1 messages)" should say "message".

## Review Triage Log

Pass 1 (quick): high 0, medium 3, low 3, false 1, maybe-false 1.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `rejected.yaml` claims `inputClasses` can't loosen `http_server`; it can (only `FixedInputs` are pinned), and `dap/launch.go`'s `allowSideEffects` text says the same for http_server and sockets | medium | patch + defer | Sample comment fixed; that server inputs can be loosened by `inputClasses` (and the schema text) predates 3.11, deferred |
| 2 | No item covers Done when 1's unknown inputs | medium | patch | `unknown.yaml` and a checklist item, probed |
| 3 | No item covers Done when 2's per-input `k` and recursion | medium | patch | `kafka-broker.yaml` and a checklist item, probed (`-0`, `-1`) |
| 4 | The entry's `verify` (items ticked, committed tree) isn't met | false | reject | That is the user's acceptance run, after this commit |
| 5 | The probe's cosmetic findings aren't in deferred-work.md | low | patch | Three entries appended |
| 6 | `rpk topic consume out` lacks `-X brokers` | low | patch | Added; Setup says to pass it to every rpk command |
| 7 | `warnings.yaml`'s http processor retries, so stepping it stalls | maybe-false | patch | `retries: 0` added (cheap; the warning is unchanged) |
| 8 | CLAUDE.md's `testdata/tracer/` list lacks the new samples | low | defer | Agent-context edits go to a sweep |
