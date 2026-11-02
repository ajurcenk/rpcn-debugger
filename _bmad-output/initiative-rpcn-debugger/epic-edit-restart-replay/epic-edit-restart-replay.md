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
2. Restart re-reads the YAML and re-steps the identical batch without reading the input (AD-20). The session stays alive after the run ends until Stop, so Restart works from a stop and after the run.
3. Restart numbers delivered batches from 1 again: the Session's delivered-batch counter (`Session.delivered`) is reset per run, so the console's "Delivered batch N" and the nothing-delivered note describe the restarted run only (deferred from epic-tracer 2.8, retro A8).
4. `SaveCapture` (the `saveCapture` launch key) writes a `0600`, gitignored, lossless file. The `replay` launch key replays it byte-identically with no broker, including for inputs that epic-safe-against-real-systems rejects. The capture round-trip test is green, and the schema is regenerated.
5. A Restart or `replay` session with `output: real` writes to the user's output only with `allowRepeatWrites: true`; otherwise it runs as dry-run with a console note (AD-20).
6. The `inputOverride` launch key replaces the pipeline's `input` for the capture with an input block, given inline or as a path to a YAML file holding one. This lets a pipeline whose input can't be captured (`http_server` and the other rejected server inputs) be debugged from sample data. A launch warning names the replaced input and its replacement. The replacement goes through the same input classes, group override, never-ack and `allowSideEffects` rules; only the original input's class is bypassed. Top-level `input.processors` stay and are stepped. The schema is regenerated.
7. The equivalence corpus is still green.
8. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

- Not a fresh capture on Restart (that's a new launch).
- Re-resolving breakpoints on Restart is epic-breakpoints-and-evaluation's, and statement lines epic-bloblang-statement-stepping's (D1). This epic keeps the breakpoints set so far across a Restart.
- SetMessage at a statement-stepping stop is epic-bloblang-statement-stepping's.

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-8, CAP-10
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-3, AD-4, AD-5, AD-11, AD-12, AD-19, AD-20, AD-21, AD-22

## Notes

- Waits on epic-safe-against-real-systems because: replay has to bypass its input-class rejection.
- Handoff from epic-safe-against-real-systems (decision D6, 2026-10-23): a Restart or `replay` session with `output: real` writes only when the launch also sets `allowRepeatWrites: true`; otherwise it runs as dry-run with a console note. Named in Done when 5 at inception (2026-11-02).
- Decision (2026-10-30, user): `inputOverride` belongs to this epic, beside `replay`. Replay needs a capture saved by an earlier session, which an `http_server` pipeline can never make, so the override is how such pipelines are debugged. The launch key itself is the consent (no VS Code dialog), as with `allowSideEffects`. The override may carry its own `processors` to recreate the metadata the original input would set. At inception, add `inputOverride` beside `replay` in the spine's AD-5 rejected-input row and in the launch-key table.
- Decision (D1, user, 2026-11-02): this epic is built right after epic-safe-against-real-systems, ahead of epics concurrency, breakpoints-and-evaluation and bloblang-statement-stepping. Done when 2 drops breakpoint and statement re-resolution, which move to those epics' Done when.
- Decision (D2, user, 2026-11-02): SaveCapture is triggered by the launch key `saveCapture`, which saves right after the capture; `replay` replays the file later. No extension change and no AD-12 change.
- Decision (D3, user, 2026-11-02): SetMessage edits a message's whole content and each metadata value (DAP setVariable). Field-level JSON edits are out of scope.
- Decision (D4, user, 2026-11-02): Restart re-reads only the pipeline YAML; the launch keys stay as launched, with a console note that launch.json changes need a new launch.
- Decision (D5, user, 2026-11-02): the session stays alive after the run ends: the root thread exits, a console note says Restart re-steps the batch and Stop ends the session, and Terminated comes only on Stop. A launch that captures nothing or fails before capture still ends the session.
- Decision (D6, user, 2026-11-02): `saveCapture` takes a file name and the file always goes in `<cwd>/.rpcn-debug/`; a name that escapes that folder is LAUNCH_INVALID. `replay` takes any path.
- Decision (user, 2026-11-02, approving the breakdown): a restarted run stops where F5 stops; Restart ignores edits to the input block and never re-reads `inputOverride`; `replay` with `saveCapture` or `inputOverride` is LAUNCH_INVALID; SetMessage keeps a metadata value's type, refuses text that doesn't parse, and changes values only.
- Tracer bullet: entry 2, Restart from a stop, through DAP, Session, rewrite and engine. Sequencing: entry 5 (the run lifecycle) comes before every later entry because it changes how dapdrive ends a run; entry 3 (the capture file) is the least certain; entries 7 and 8 wait on their predecessors because they edit the same `LaunchConfig`, schema and Session input check. The chain is linear wherever entries share code.
