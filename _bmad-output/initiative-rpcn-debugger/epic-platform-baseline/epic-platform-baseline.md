---
type: epic
title: "Repo, task runner and test harness ready"
parent: initiative-rpcn-debugger
covers: []
after: []
assignee: ""
status: done
risk: medium
---

# Repo, task runner and test harness ready

## Description

Stands up the repo every later epic builds on: the Go module and `vscode/` skeleton, the local `task ci` pipeline, the shared test tooling, and an early `components/all` link spike that answers the spine's binary-size, CGO and license-timing questions before the engine work depends on them.

## Outcome

The developer can run `task ci` on a fresh clone and install a skeleton `.vsix`; this is a platform baseline, so no spec signal applies.

## Requirements

- B1: Repo scaffold per the spine's Structural Seed and Stack: Go module, the debugger/ package layout, cmd/rpcn-debug, the vscode/ extension skeleton, the Taskfile. (spine, Structural Seed, Stack)
- B2: Lint gates: forbidigo (fmt.Print*, os.Stdout in debugger/...) and a depguard public-only allow-list. (spine AD-2, AD-15)
- B3: A Redpanda container integration-test job in `task ci`. (spine AD-8)
- B4: A DAP test client (@vscode/debugadapter-testsupport) driving the binary over stdio, through debugger.Serve. (spine Stack, AD-1, AD-12)
- B5: The equivalence harness discovers cases and runs each reference (no-debugger) run. (spine AD-8)
- B6: A components/all link spike answering binary size, CGO and license timing. (spine Open Questions)
- B7: A skeleton .vsix installs in VS Code ^1.120.0 and registers the redpanda-connect debug type. (spine AD-12, AD-23 MVP; spec Constraints)

## Done when

1. `task ci` runs golangci-lint (forbidigo and depguard: the AD-2 and AD-15 lint part only), unit tests, a Redpanda testcontainers job, the DAP test client (@vscode/debugadapter-testsupport) and `vsce package`, all green on an empty skeleton.
2. The skeleton `.vsix` (`pipeline-debugger-for-redpanda-connect`) installs in VS Code ^1.120.0 and registers the `redpanda-connect` debug type.
3. The equivalence harness discovers and runs cases under `testdata/equivalence/`. Case comparison belongs to the tracer epic, which owns the Snapshot type.
4. A spike binary linking `components/all` builds; its size, CGO needs and license-at-Build() behaviour are recorded in the spine's Open Questions.
5. Merged, `task ci` green, and it works from the locally installed `.vsix`.

## Boundaries

Platform baseline: scaffold, task runner, test tooling. No debugger behaviour.

## References

- spec — _bmad-output/initiative-rpcn-debugger/spec-rpcn-debugger/spec-rpcn-debugger.md, sections Constraints
- architecture — _bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md, sections Stack, Structural Seed, AD-2, AD-8, AD-15, Open Questions

## Notes

- Decision: CI is local only, via a Taskfile `task ci`; there is no hosted CI (user, 2026-10-09).
- Decision: GitHub personal repo `github.com/ajurcenk/rpcn-debugger` (remote git@github.com:ajurcenk/rpcn-debugger.git), publisher `ajurcenk` (user, 2026-10-12).
- Decision: no telemetry from the binary or the extension (AD-22).
- Decision: operations are n/a. It's a local developer tool; there's no deployed service.
- Assumption: Go 1.26.x and Node/npm are installed locally.
- Decision: tracer bullet = entry 1. Fresh clone → `task ci` → install the .vsix → the Redpanda Connect debugger type is visible (2026-10-12).
- Decision: entries 2, 3 and 4 are chained because they share the Taskfile and go.mod; 5 and 6 can run alongside them (2026-10-12).
- Decision: MIT licence (user, 2026-10-12).
- Decision: the breakdown was approved (user, 2026-10-12).
- Decision: a Redpanda enterprise license is available locally at `/etc/redpanda/redpanda.license` for spike 6. It is read from that path and never committed (user, 2026-10-13).
- Closed 2026-10-16 (user): all 7 entries done; Done-when 1–5 met. `task ci` is green, the .vsix F5 check passed in VS Code 1.141.0, spike answers are in the spine, and `main` is pushed to github.com/ajurcenk/rpcn-debugger.
