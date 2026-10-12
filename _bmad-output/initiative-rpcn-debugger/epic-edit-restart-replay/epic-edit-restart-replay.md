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
4. The equivalence corpus is still green.
5. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

Not a fresh capture on Restart (that's a new launch).

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-8, CAP-10
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-11, AD-19, AD-20, AD-22

## Notes

- Waits on epic-safe-against-real-systems because: replay has to bypass its input-class rejection.
- Waits on epic-breakpoints-and-evaluation because: Restart re-resolves breakpoints with its resolver.
- Waits on epic-bloblang-statement-stepping because: Restart re-resolves statement lines.
