---
type: epic
title: "Install and use it on any machine"
parent: initiative-rpcn-debugger
covers: [CAP-1, CAP-13]
after: []
assignee: ""
risk: medium
---

# Install and use it on any machine

## Description

Turns the debugger into a locally installable product: a `.vsix` with a bundled binary, RPC plugins, a user debug `main` example, license handling, version checks and a README, closed by the spec's full success-signal demo.

## Outcome

On a fresh machine a developer installs the `.vsix` and runs the spec's success signal end to end (CAP-1, CAP-13).

## Done when

1. The `.vsix` bundles `rpcn-debug` for the developer's OS/arch, used when `rpcnDebug.binaryPath` is unset (AD-23 MVP).
2. The `rpcPlugins` launch key loads RPC plugin manifests, and `resources[]` adds resource files to the session. A tested example user debug `main` calls `Serve` with a compiled-in plugin.
3. The `license` key accepts only a file or env reference, applied through `StoreCustomLicenseBytes`. `initialize` reports `rpcnDebugVersion`, `rpcnConnectVersion` and `launchSchemaVersion`, and the extension warns or refuses on mismatches (CAP-13). The schema is regenerated.
4. The README covers install, usage, 'Evaluate is unrestricted' and no telemetry. The spec's success-signal demo passes on a real Redpanda topic from the installed `.vsix`.
5. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

Not the Marketplace, download or signing (post-MVP; spine Deferred).

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, CAP-1, CAP-13, Success signal
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, AD-1, AD-14, AD-22, AD-23

## Notes

- Waits on epic-platform-baseline because: it uses the link spike's results.
- Waits on epic-safe-against-real-systems, epic-bloblang-statement-stepping and epic-edit-restart-replay because: the success-signal demo exercises all of them.
- Decision: the MVP is distributed locally; the Marketplace comes later (user, 2026-10-09).
- Decision: this epic owns the `resources[]` launch key (user, 2026-10-16, at epic-tracer inception).
