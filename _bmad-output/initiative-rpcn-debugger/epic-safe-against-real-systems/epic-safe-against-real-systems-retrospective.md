---
epic: epic-safe-against-real-systems
date: 2026-10-30
verdict: accepted-with-open-items
criteria: declared
headless: false
---

# Retrospective: epic-safe-against-real-systems (E3)

## Epic summary

All 11 entries are `done` (3.1–3.11); none at `built`, `pending_tickets` empty. Done when: declared in `epic-safe-against-real-systems.md` (items 1–6).

Plan ranges (ordered by baseline; one linear history, no merges):

| Entry | Range | Commits |
|---|---|---|
| 3.1 | f31c2e7..98c3c47 | 4acb23a, 98c3c47 |
| 3.2 | 98c3c47..f4900e6 | bb52452, 0976584, 369f3d5, f4900e6 |
| 3.3 | f4900e6..54142f4 | 2cefe51, 54142f4 |
| 3.4 | 54142f4..cbcf1e5 | 1824c54, 8f18539, cbcf1e5 |
| 3.5 | cbcf1e5..d6423c4 | 5837337, 69b121f, d6423c4 |
| 3.6 | d6423c4..1016836 | efd87dc, 1016836 |
| 3.7 | 1016836..cfc0e78 | d1e72c1, 6761bc7, cfc0e78 |
| 3.8 | cfc0e78..af03af3 | 46ee1df, 6b0d925, af03af3 |
| 3.9 | af03af3..73cfe8b | d4a167a, 73cfe8b |
| 3.10 | 73cfe8b..0cad980 | 393ba28, 0cad980 |
| 3.11 | 0cad980..6e26cd8 (cut, inferred end) | 423907c, 68bded7, 6e26cd8; `e4a8831` excluded (epic-edit-restart-replay planning) |

Range boundaries fall on the next entry's baseline, so a decision commit for the next entry (369f3d5/f4900e6 in 3.2, af03af3 in 3.8) sits in the previous range; attribution follows the commit subject.

Evidence inventory: epic file, initiative tickets.toml, 11 plans (all with Review Triage Logs except 3.11's oneshot run), two research files (3.3 input classes, 3.9 processor effects), `deferred-work.md`, the spine and `.memlog.md`, the E2 retrospective. Session logs: this session's transcript only (3.8–3.11 and the post-summary record); earlier sessions' logs not read, so process lessons for 3.1–3.7 rest on the plans alone.

## Findings

Sources: three Phase 2 analyses (aggregate views; `bmad-review` adversarial, edge-case and verification-gap lenses over `git diff f31c2e7..6e26cd8 -- debugger cmd test vscode/src`, 114 files; the E2 follow-through), consolidated here. R1 and R3 were re-checked by the orchestrator with a throwaway `rewrite.Rewrite` probe at HEAD; R2 and R4–R6 by the reviewer's probe or a code read.

### Cross-entry review (diff scope)

| # | Finding | Source | Boundary | Instance | Prevention |
|---|---|---|---|---|---|
| R1 | An input reached through an interpolated `resource:` (`input: {resource: ${IN:a}}`) is classed Unknown, but the walker overrides groups and forces `auto_replay_nacks` only on resources reached by a literal reference. `Plan.Resources` keeps `consumer_group: prod` and `auto_replay_nacks: false`. With `allowSideEffects: true` (which the refusal tells the user to set), the capture joins the production group and its nack counts as an ack, so offsets are committed. That breaks never-ack, which the epic Notes call non-configurable | `debugger/rewrite/inputwalk.go:250-252`, `rewrite.go:147-149`; orchestrator probe 2026-10-30 | 3.3/3.5 vs 3.4 | **fix now** (high): apply the group override and `forceReplayNacks` to every `input_resources` entry; inputs are built lazily (benthos `manager/type.go:276`), so unreferenced entries cost nothing. Add a golden and a Serve test | A guarantee called "non-configurable" needs a test over every way an input is reached (literal, interpolated, nested), not only the walker's paths |
| R2 | Under `output: real`, a shared-client output (`redpanda_common`) inside `output_resources` isn't refused; only the top-level block is. The launch goes on and ends after 3.8's 30 s deadline with a generic `OUTPUT_FAILED` | `rewrite/rewrite.go:250` (`realOutput(outRes, …, false)`), `output.go:54-64`; tests cover only the top level (`output_test.go:233-252`) | 3.7 vs 3.6/3.8 | fix now (medium): apply the shared check to resources reachable from the output tree | — |
| R3 | A top-level key outside the allowlist (a typo such as `pipline:`) is dropped silently: no error, no warning, and the engine never lints it, although the `topLevel` comment says such keys are "left to the engine's lint" | `rewrite/rewrite.go:331-336`; orchestrator probe: `err=nil warnings=[]` | 3.6 | fix now (medium): warn or `PIPELINE_INVALID` for keys outside `topLevel` that don't end in `_resources` | A comment that delegates to another layer needs a test that the other layer runs |
| R4 | `keepConsumerGroup` plus `inputClasses: {kafka_franz: replay-safe}` joins the production group with only "can't prove it"; the kept-group effect text is wiped by the override | `rewrite/inputwalk.go:166-188`, `core/inputs.go:95-97` | 3.4/3.5 | fix now (low): keep the overridden effect text in the loosening warning | — |
| R5 | An unreferenced `input_resources` entry is scanned as capture-time by 3.9, giving false "runs during capture" and state-split warnings, although it is never built | `rewrite/effects.go:481-484`, `519-523` | 3.3 vs 3.9 | defer (low): seed the capture side with the resources the input walk reached | — |
| R6 | Under `output: real` the state-split check ignores the output tree (`outRefs` is computed but unused), and `refKeys` misses the `cache` output's `target` | `rewrite/effects.go:488-497`, `532-545`, `396` | 3.7 vs 3.9 | defer (low): include `outRefs` under real | — |

### Aggregate views

| # | View | Finding | Source | Instance |
|---|---|---|---|---|
| V1 | Architecture delta | Clean: rewrite/engine/dap import only `core`; core stdlib only (new: `os/user`); go-dap only in dap; no stdout in `debugger/`; only go.mod change is `kadm` direct for integration tests | `go list` imports at HEAD | accept |
| V2 | Spec reconciliation | Done when 1–6 met (tests per item, below) | see Acceptance verdict | accept |
| V3 | Spec reconciliation | `inputClasses` can loosen every rejected row outside `core.FixedInputs` (http_server, socket_server, otlp_*, gateway, inproc, nanomsg bind). This matches AD-5 ("loosening warns") but two texts drift: the `allowSideEffects` schema text says http_server and sockets "can't be debugged either way", and AD-5's not-loosenable list omits `redpanda_common` | `dap/launch.go:45`; spine AD-5 :133; `rewrite/inputs_test.go:513` | fix now (doc): the two texts; the pin-or-not question stays in its deferred entry (3.11) |
| V4 | Spec reconciliation | 3.8 changed frozen intent (per-stepper overwrite → one add/subtract counter per run), user-confirmed 2026-10-29, tested (`engine/output_test.go:268`), consistent with AD-3 | 3.8 plan :31, :123 | accept (recorded so later retros don't re-flag it) |
| V5 | Spec reconciliation | D8 named a kept `redpanda` block; 3.6 took D8's fallback (drop with warning), recorded in the plan and AD-5 :138 | `rewrite.go:336-351`, `TestRedpandaBlockDropped` | accept |
| V6 | Pattern divergence | 3.10 reintroduced a text match named `strictRejection` (log routing, `logging.go:106,118`), so 3.8's verification `grep strictRejection → none` no longer holds; the two deferred log-noise items would add more text filters | `logging.go`, AD-15 :280 | defer: rename and keep log-text filters in one table, next sweep |
| V7 | Duplication | Four hand-written processor-tree walks remain (`walk.go:317,439`, `effects.go:344,375`, `refs.go:115,148`); `labelRefs`/`refs` are near-twins. User decision in 3.10: no shared list finder. R5/R6 are what diverging walks cost | 3.10 plan Intent | accept for E3; revisit if E4 adds a walk |
| V8 | God-class growth | `debugger/rewrite` non-test lines 1,777 → 3,489; `effects.go` 601 (≈205 table), `inputs.go` 443 (table), `inputwalk.go` 400. Cohesive tables plus walkers | `git_evidence.py` | accept |
| V9 | God-class growth | `core/session.go` 887 → 979; `launch()` ≈100 lines (:251-349). A13 was left out of E3 by the user; E4 (Restart, replay, `inputOverride`, `allowRepeatWrites`) lands in `launch`/`run` | `core/session.go` | defer: A13 as an opening enabler of epic-edit-restart-replay |

Top net growth: `rewrite/inputs_test.go` +683, `rewrite/effects.go` +601, `rewrite/inputs.go` +443, `serve_inputs_test.go` +407, `rewrite/inputwalk.go` +400, `serve_output_test.go` +349, `engine/output_test.go` +344, `rewrite/effects_test.go` +299, `rewrite/output_test.go` +282, `test/integration/neverack_test.go` +207. Non-test: `engine/sink.go` +186, `rewrite/output.go` +120, `rewrite/rewrite.go` +118, `engine/engine.go` +112, `core/session.go` +92.

Views narrowed: no cross-package naming audit; no per-entry churn comparison.

### Process lessons

- Plan size: E3 plans ran 833–2,039 words (3.8: 2,039; 3.7: 1,898); A9 (decide plan sizing) has no recorded decision. The two largest plans carried the frozen-intent change (3.8) and the most review findings.
- Every entry's quick review found real defects (Review Triage Logs), but none crossed entries: R1–R6 all sit on entry boundaries, which only this epic-level review saw. Session logs for 3.1–3.7 were not read.

## Behavior verification

- The user's 3.11 acceptance run (2026-10-30) exercised every changed flow end to end from the installed `.vsix` against a local Redpanda: the debug group and per-input `k`, `keepConsumerGroup` with and without `allowSideEffects`, refusals 1011 and 1012 (side-effecting, unknown, rejected), `inputClasses`, `output: real`/`none`/dry-run with records checked in the `out` topic, `OUTPUT_FAILED` 1013, and the four launch warnings. No issues (3.11 plan, acceptance note).
- `rpk group describe` after the probe runs showed no committed offsets for `prod` or the debug group.
- `task ci` exit 0 on `423907c` (unit, equivalence, integration including `TestNeverAck` and `TestOutputModes`).
- Not exercised end to end: R1's interpolated-resource path (shown at the rewrite level only).

## Previous-retro follow-through

From `epic-tracer/epic-tracer-retrospective.md`, Action items:

| Item | Owner | Landed? | Evidence |
|---|---|---|---|
| A1 interpolated strict | dev loop, before E3 | landed | 8d6a071, `plan-s2-interpolated-strict.md`, `testdata/equivalence/strict-interpolated/` |
| A2 input-tree resources unstepped | dev loop, before E3 | landed | a58f484, `testdata/equivalence/input-resource/` |
| A3 lint position clamp | dev loop | not landed, never filed | `rewrite/positions.go` unchanged since eb66937; no ticket or deferred entry |
| A4 Variables overflow hint | dev loop | not landed, never filed | `dap/variables.go` unchanged since 00fc71c; no ticket or deferred entry |
| A5 Check hands Configs to Start | dev loop | landed (E3 3.1) | 4acb23a, `core/port.go:82-86` |
| A6 spine reconciliation | user + bmad-architecture | landed | 305cb7a, memlog "spine finalized (A6)" |
| A7 composite outputs; AD-8 narrowed | A6 | landed | memlog A7a/A7b, 5206563, deferred-work entries |
| A8 owners for orphaned deferred items | user + bmad-ticket | partly landed | mixed-batch strict (3.8) and strict Warn (3.10) done; retry/while labels unowned; `allThreadsContinued` not in epic-concurrency's Done when; Restart counter reset not in epic-edit-restart-replay's Done when |
| A9 plan sizing for E3 | user | no evidence found | no decision recorded; plans 833–2,039 words |
| A10 durable commit/append-only rules | user | not landed | only in user memory and the 3.10 plan's Boundaries |
| A11 Docker preflight | dev loop | deferred by user decision | E3 epic Notes (2026-10-23): A11–A13 out of E3; not filed |
| A12 depguard layering | dev loop | deferred by user decision | same; `.golangci.yml` has only `public-api-only` |
| A13 split `core/session.go` | E3 sweep | deferred by user decision | same; 3.10 plan "Never" |

## Action items

Proposed, not applied. "Entry" means story-shaped work for `bmad-build` (a backlog bug, or a pre-E4 entry).

| # | Action | Kind | Owner | From |
|---|---|---|---|---|
| B1 | Never-ack for every input resource: run the group override and `forceReplayNacks` on every `input_resources` entry, with a golden and a Serve test of `input: {resource: ${IN:a}}` | remediation, bug entry, **before the next epic** | dev loop | R1 |
| B2 | Refuse shared-client outputs reached through `output_resources` under `output: real` | remediation, small bug entry | dev loop | R2 |
| B3 | Warn (or `PIPELINE_INVALID`) for a top-level key outside the allowlist that isn't `*_resources` | remediation, small bug entry; needs a user choice: warning or error | dev loop | R3 |
| B4 | Keep the overridden effect text in the `inputClasses` loosening warning (kept group named) | remediation, tiny | dev loop | R4 |
| B5 | Doc fixes: the `allowSideEffects` schema text (`dap/launch.go:45`, then `task gen:schema`) and AD-5's not-loosenable list (add `redpanda_common`) | spec reconciliation | user + bmad-architecture (AD-5); dev loop (schema) | V3 |
| B6 | File the effect-scan drifts R5/R6 and the log-text filter table V6 in `deferred-work.md` for the next sweep | backlog hygiene | orchestrator, on approval | R5, R6, V6 |
| B7 | Make A13 (split `core/session.go`) an opening enabler entry of epic-edit-restart-replay at its inception | tech debt | user + bmad-ticket | V9 |
| B8 | Carry the unlanded E2 items: file A3 and A4 as backlog bugs; finish A8 (name `allThreadsContinued` in epic-concurrency's Done when, the `Session.delivered` reset in epic-edit-restart-replay's); decide A9 and A10 | backlog hygiene, process | user + bmad-ticket | follow-through |
| B9 | Process: each guarantee the epic Notes call non-configurable gets a test over every way the guarded thing is reached (here: literal, interpolated and nested input references) | process | user | R1 |

## Acceptance verdict

**Accepted-with-open-items** (criteria declared). User decision 2026-10-30: the proposed verdict and action items approved; R1 is an open item, not blocking, and B1 lands before the next epic starts.

- All 11 tickets are done; `pending_tickets` empty.
- Done when 1 (input classes): `serve_inputs_test.go` `TestServeInputClasses` (rejected http_server, rejected beats opt-in), `core/inputs_test.go`.
- Done when 2 (group override, recursion, `keepConsumerGroup`): `rewrite/inputs_test.go` `TestGroupOverrideMatrix`, `serve_inputs_test.go` `TestServeGroupOverride`; 3.11 `kafka-broker.yaml`.
- Done when 3 (`output: real`/`none`): `TestServeOutputRealWritesOnce`, `TestSinkDryRunAndNoneWriteNothing`, integration `TestOutputModes`.
- Done when 4 (never-ack integration test): `test/integration/neverack_test.go` `TestNeverAck` (generation check, no commits for either group).
- Done when 5 (corpus green): `testdata/equivalence` unchanged across the epic; `task ci` green.
- Done when 6 (merged, ci, `.vsix`): 3.11 acceptance by the user, 2026-10-30.
- Open: R1 breaks the never-ack guarantee for an input reached through an interpolated `resource:` once `allowSideEffects` is set. It's outside Done when 4's test and needs consent, so it isn't judged blocking, but B1 should land before the next epic starts. If the user judges R1 blocking, the verdict is **rejected** until B1 lands.

## Open questions

1. ~~Is R1 blocking?~~ Settled 2026-10-30: an open item; B1 lands before the next epic.
2. B3 (open, for its bug entry): should an unknown top-level key be a warning or a launch error?
3. (Open, in the 3.11 deferred entry) Should the rejected server inputs be pinned against `inputClasses` (the 3.11 deferred entry), or stay loosenable with a warning as AD-5 says, now that `inputOverride` is planned for epic-edit-restart-replay?
