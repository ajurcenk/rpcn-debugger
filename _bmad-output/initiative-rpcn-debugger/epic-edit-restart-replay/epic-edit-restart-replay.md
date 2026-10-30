---
type: epic
title: "Change the message, change the config, try again"
parent: initiative-rpcn-debugger
covers: [CAP-8, CAP-10]
after: []
assignee: ""
risk: medium
---

# Change the message, change the config, try again

## Description

Editing paused messages, restarting on the same captured batch with a re-read config, and saving captures that replay without the input system.

## Outcome

A developer fixes the YAML and sees the result on the same batch at once; this is the 'Restart' step of the spec's success signal (CAP-8, CAP-10).

## Done when

1. `SetMessage` edits apply downstream of the stop only (AD-19).
2. Restart re-reads the YAML, re-resolves processor and statement breakpoints, and re-steps the identical batch without reading the input (AD-20).
3. `SaveCapture` writes a `0600`, gitignored, lossless file. The `replay` launch key replays it byte-identically with no broker, including for inputs that epic-safe-against-real-systems rejects. The capture round-trip test is green, and the schema is regenerated.
4. The `inputOverride` launch key replaces the pipeline's `input` for the capture with an input block, given inline or as a path to a YAML file holding one. This lets a pipeline whose input can't be captured (`http_server` and the other rejected server inputs) be debugged from sample data. A launch warning names the replaced input and its replacement. The replacement goes through the same input classes, group override, never-ack and `allowSideEffects` rules; only the original input's class is bypassed. Top-level `input.processors` stay and are stepped. The schema is regenerated.
5. The equivalence corpus is still green.
6. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

Not a fresh capture on Restart (that's a new launch).

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-8, CAP-10
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-5, AD-11, AD-19, AD-20, AD-22

## Notes

- Waits on epic-safe-against-real-systems because: replay has to bypass its input-class rejection.
- Waits on epic-breakpoints-and-evaluation because: Restart re-resolves breakpoints with its resolver.
- Waits on epic-bloblang-statement-stepping because: Restart re-resolves statement lines.
- Handoff from epic-safe-against-real-systems (decision D6, 2026-10-23): a Restart or `replay` session with `output: real` writes only when the launch also sets `allowRepeatWrites: true`; otherwise it runs as dry-run with a console note. Name it in this epic's Done when at its inception.
- Decision (2026-10-30, user): `inputOverride` belongs to this epic, beside `replay`. Replay needs a capture saved by an earlier session, which an `http_server` pipeline can never make, so the override is how such pipelines are debugged. The launch key itself is the consent (no VS Code dialog), as with `allowSideEffects`. The override may carry its own `processors` to recreate the metadata the original input would set. At inception, add `inputOverride` beside `replay` in the spine's AD-5 rejected-input row and in the launch-key table.
