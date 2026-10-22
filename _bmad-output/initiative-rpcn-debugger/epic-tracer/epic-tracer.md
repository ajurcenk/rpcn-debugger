---
type: epic
title: "Step a captured batch through a pipeline in VSCode"
parent: initiative-rpcn-debugger
covers: [CAP-1, CAP-3, CAP-4, CAP-11, CAP-12]
after: []
assignee: ""
status: in-progress
risk: high
---

# Step a captured batch through a pipeline in VSCode

## Description

The tracer through every layer: F5 on a pipeline captures one batch without acking it and steps it processor by processor through every non-concurrent control-flow parent, showing message state in VS Code and delivering to dry-run. It also lays the shared contracts (the closed protocol, Snapshot, the launch schema, logging, env lookup) that later epics only extend.

## Outcome

A pipeline developer can see the state of a message before every processor; this is the first half of the spec's success signal.

## Done when

1. F5 on a pipeline, with stock `rpcn-debug` via `rpcnDebug.binaryPath`, captures one batch (`maxMessages`, nack, input stopped) and stops before each processor of every non-concurrent AD-7 parent and strict mode. It shows line highlight, repeat labels, and Batches › Message › content / metadata / error, with Step Over / In / Out and Pause.
2. Delivered batches reach `rpcn_debug_sink` in dry-run and are printed to the Debug Console as Output events (content, metadata, error), with the output-batching warning (AD-21).
3. The full closed AD-3 protocol, error registry and AD-11 Snapshot (including the optional `bloblang` and `skipped` fields) are defined. The launch-schema generator and check run in `task ci`. `SetBreakpoints` answers everything unverified until the breakpoints epic.
4. One slog handler goes to stderr and DAP Output, translating paths (AD-6, AD-15). `${VAR}` resolves through process env, then `envFile`, then `env` (AD-22). `processor_resources` lists are wrapped. Unknown and concurrent parents (`workflow`, `parallel`, `retry` with `parallel: true`) run as one opaque step with a warning, and nothing below them is wrapped. `metrics`/`tracer`/`http`/`logger` are dropped, `stdin` is rejected and `stdout`/`stderr` outputs are redirected (AD-5, AD-15). The AD-8 time-budget warning fires.
5. The equivalence corpus (case comparison plus the scripted Step Over driver) is green for every row in scope, including the CTX_LOST cases.
6. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

Not `output: real`/`none`, input classes or the group override (epic-safe-against-real-systems); not concurrency (epic-concurrency); not breakpoints or evaluate (epic-breakpoints-and-evaluation); not statement stepping (epic-bloblang-statement-stepping); not SetMessage, Restart or replay (epic-edit-restart-replay); not plugins, license or bundling (epic-local-release).

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-1, CAP-3, CAP-4, CAP-11, CAP-12
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-1 to AD-4, AD-6 to AD-12, AD-15 to AD-17, AD-19, AD-21, AD-22
- spike evidence — architecture-rpcn-debugger/.memlog.md, the 2026-10-08 capture and stepper entries. The spike source was in a session temp folder and no longer exists, so re-derive the code from the recorded findings

## Notes

- Waits on epic-platform-baseline because: it needs the scaffold, `task ci`, the equivalence harness and the DAP test client.
- Decision: E2 was split by outcome into tracer / safe against real systems / concurrency (user, 2026-10-09).
- Decision: this epic owns the pause gate, the single root thread and ForkThread/ExitThread; epic-concurrency adds per-thread step state and the `concurrent: true` fork.
- Handoff from 1.1: vscode/package.json has a hand-written `initialConfigurations` (type/request/name only). The launch-schema generator story decides whether Go emits it, so the AD-12 check doesn't flag it as drift.
- Handoff from 1.4 (moved here by the 1.7 sweep): `debugger.Serve` builds `slog.NewTextHandler(os.Stderr, nil)` itself, so `cmd/rpcn-debug`'s injected stderr and library callers can't redirect its logs (deferred-work.md). The AD-15 slog handler (stderr plus DAP `Output`) built in this epic replaces it. Decide here whether `Serve` takes an Option for the stderr side (user, 2026-10-16: moved here from the refactor sweep).
- Decision: tracer bullet = entry 1. F5 on a `generate` + one `mapping` pipeline captures a batch (nack, input stopped), stops before the mapping with the line highlighted and content shown, then Step Over delivers and the session ends (2026-10-16).
- Decision: sequencing. Entries 3 and 5 run in parallel after the contracts entry (2). Everything else is one lane: 6 after 3 and 5 (Arrive contract vs stepper config spec), 4 after 3, 5 and 6 (it needs the LaunchConfig env keys, Translate's implementation and the Session-built lookup), then 7 → 8 → 9 → 10 → 11 (2026-10-16).
- Decision: concurrent parents (`workflow` branches, `parallel`, `retry` with `parallel: true`) run as one opaque step with a warning in this epic, and nothing below them is wrapped. epic-concurrency wraps them. This is an interim departure from AD-6's wrap list (user, 2026-10-16).
- Decision: delivered batches are shown as Debug Console Output events (content, metadata, error), not as a final stop. A webview or a custom DAP request can come later under AD-12 (user, 2026-10-16).
- Decision: this epic (entry 5) owns dropping `metrics`/`tracer`/`http`/`logger` and the stdio rules (`stdin` rejected, `stdout`/`stderr` outputs redirected). A `stdin` input would read the DAP stream in every tracer session, and entry 4 needs the logger replaced. Moved out of epic-safe-against-real-systems' Done when 2 (user, 2026-10-16).
- Decision: a `processor_resources` entry referenced from inside an opaque concurrent parent is left unwrapped by the rewrite, with a warning. The rewrite sees `resource:` references statically. epic-concurrency lifts this (user, 2026-10-16).
- Decision: the `resources[]` launch key belongs to epic-local-release (user, 2026-10-16).
- Decision: two entries added from the 2.4 review (user, 2026-10-20): 12 "Launch lint errors at file lines" (after 4 and 5, built after 8) and 13 "Rewrite rejects undefined resource references" (after 5, built before 7). The refactor sweep (10) also waits on both.
- Decision: entry 14 "Strict-mode output rejection is not a run failure" added from the 2.9 findings, as a new entry before the sweep, not sweep scope (user, 2026-10-22). The sweep (10) also waits on it.
