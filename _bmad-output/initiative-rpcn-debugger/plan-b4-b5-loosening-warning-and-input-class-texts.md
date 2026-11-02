---
title: 'The loosening warning names the effect; input-class texts match the code'
type: 'bugfix'
ticket: ''
created: '2026-10-30'
status: 'done'
baseline_revision: '0a22a78268cd065c0da5ab469b8f2a6ce3463869'
route: 'oneshot'
route_source: 'auto'
risk: 'low'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** E3 retro findings R4 and V3 (actions B4 and B5).
- B4: when `inputClasses` marks an input replay-safe, its effect is wiped, so the launch's only text is "inputClasses marks kafka_franz at input replay-safe; the debugger can't prove it". With `keepConsumerGroup` the capture then joins the production group with no word that it does.
- B5: the `allowSideEffects` schema text says a rejected input (stdin, http_server, sockets) "can't be debugged either way", but `inputClasses` can loosen every rejected input outside `core.FixedInputs`. And AD-5's (and the README's) list of inputs that can't be loosened omits `redpanda_common`, which `FixedInputs` holds since 3.6.

**Approach:** B4: a loosened finding keeps the effect the override set aside, and the Session's loosening warning appends it ("…; the debugger can't prove it: joins consumer group \"prod\" (consumer_group)"). The class and refusals don't change. B5: the schema text says `allowSideEffects` doesn't lift a rejected input and only `inputClasses` can, except the four `FixedInputs`; AD-5 and the README list all four. Whether server inputs should be pinned stays the open deferred decision.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because both are a few lines.

- `rewrite/inputwalk.go`: the `o == ReplaySafe` override case no longer clears `effect`. `core/inputs.go`: `InputFinding.Effect`'s doc says a loosened finding keeps it; `inputDecision` appends `": " + Effect` to the loosening warning when the override is replay-safe (a loosening to side-effecting already carries the effect and its own warning).
- Tests: `TestInputClassesOverride` expects the kept effects, plus a row "kept group loosened keeps its effect"; `TestLaunchInputDecision` row "loosen keeps the effect" checks the full warning.
- B5: `dap/launch.go` `allowSideEffects` desc, `task gen:schema` (`vscode/package.json`); AD-5 per-session overrides and README list `redpanda_common`; memlog entry 85; spine lint clean.
- Review patches: the warning reads "…; the debugger can't prove it. Without the override: <effect>", since some effects end "so it is treated as side-effecting", which the loosened class contradicts (the Intent's example used ": "). The not-loosenable list names `redpanda` without `seed_brokers` too (`hardRejected`), in the schema text, AD-5 and the README; the README's rejected row now says `allowSideEffects` doesn't lift it and `inputClasses` can, except those five. Tests use the real kept-group effect text, and `TestServeInputClasses` gains "loosen a kept group" (`keepConsumerGroup` + `inputClasses: {kafka_franz: replay-safe}` through `debugger.Serve`).
- 2026-11-02 acceptance (user): tested in VS Code from the installed .vsix against the local Redpanda, no issues: the `allowSideEffects` description, the kept-group refusal without the override, the one loosening warning naming the set-aside effect, and no offsets committed for `prod`.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 3, false 0, maybe-false 0. All patched.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | Appending the effect as-is contradicts the class for effects that say "so it is treated as side-effecting" (unknown, `unsure` rows) | low | patch | "Without the override: <effect>" |
| 2 | The core test's kept-group effect isn't the real text, and no Serve test runs `keepConsumerGroup` + `inputClasses` | medium | patch | Real text; Serve row "loosen a kept group" |
| 3 | `redpanda` without `seed_brokers` can't be loosened either (`hardRejected`), but the texts list only the four `FixedInputs` | low | patch | Schema text, AD-5, README name it |
| 4 | The README's rejected row still says "Always fails" and "can't be debugged" | low | patch | Row reworded |

## Verification

**Commands:**
- `go test -count=1 ./debugger/...` -- expected: PASS.
- `task ci` -- expected: exit 0.

Run 2026-10-30 after the review patches: `task ci` exit 0.
