---
title: 'Refactor sweep'
type: 'chore'
ticket: '7'
created: '2026-10-16'
status: 'built'
baseline_revision: '44b70e8811d5d26812d95d9e411fc9291aba1c99'
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

**Problem:** Entries 1–6 left two deferred items. CLAUDE.md still says the repo has no code, build, tests or git history, so agents start from a false picture. And `debugger.Serve` hard-wires its logs to `os.Stderr`.

**Approach:**
- Rewrite CLAUDE.md's Project state section to describe the code and tooling that now exist (user, 2026-10-16: yes).
- Move the `Serve` log-destination item to epic-tracer, which owns the AD-15 slog handler, as a handoff note. Don't add public API in this cleanup entry (user, 2026-10-16: option A).
- No code behaviour changes.

</frozen-after-approval>

## Implementation Notes

The oneshot route was chosen because the change is about 30 lines of docs: CLAUDE.md and one epic-tracer note. The code sweep found no stale comments, TODOs or dead code in `debugger/`, `cmd/`, `test/` or `vscode/src`, and the README status matches the seed.

- Files: `CLAUDE.md` (Project state rewritten; BMAD skills section unchanged) and `epic-tracer/epic-tracer.md` (handoff note). No code changed.
- Verified: every path and task named in CLAUDE.md exists. `task ci` exited 0 (lint 0 issues; Go, equivalence, integration and extension tests pass; `.vsix` packaged).

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 4, false 1, plus one rejected.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | CLAUDE.md described capture, stepping and the `core`/`engine`/`rewrite`/`blobl` packages as if they existed, though only the DAP seed is built | medium | patch | Confirmed: `serve.go` discards `env`, and those four packages hold only `doc.go`. The text now says what exists today, and that capture and stepping arrive with epic-tracer |
| 2 | "Only `--version` writes to stdout" ignored the DAP stream | low | patch | Reworded: stdout carries only the DAP stream or the `--version` line |
| 3 | `test:ext` prerequisite omitted `build:ext` (`npm ci`) | low | patch | Confirmed by the Taskfile's `test:ext` desc. Now says it needs `build:go` and `build:ext` first |
| 4 | The license line cited AD-22 for "dev license" and "never commit" | low | patch | Now cites the epic-platform-baseline decision (2026-10-13). AD-22 is named for the lookup order and timing only |
| 5 | "`tickets.toml` per epic" didn't match the tree | low | patch | Now says: initiative-level `tickets.toml`, plus one per epic once it is sliced |
| 6 | The `Serve` log item became a prose handoff note, not a new entry, against the ticket's "becomes a new entry" | low | reject | The user chose this (option A, 2026-10-16), and epic-tracer has no `tickets.toml` yet. Its inception will turn the note into an entry. The note's label was corrected to "Handoff from 1.4 (moved here by the 1.7 sweep)" |
| 7 | No `task ci` run recorded | false | reject | `task ci` ran and exited 0 before the review returned; that's now recorded above |
