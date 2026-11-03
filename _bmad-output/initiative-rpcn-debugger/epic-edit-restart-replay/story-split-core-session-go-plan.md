---
title: 'Split core/session.go'
type: 'refactor'
ticket: '1'
created: '2026-11-02'
status: 'done'
baseline_revision: '00fe346870794cdc26093cc66bf0b1e8b2579a08'
route: 'full'
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

**Problem:** `debugger/core/session.go` is 979 lines holding launch, the run, threads, the stop decision, resume and delivery. Restart, SetMessage and the capture file (entries 2–6) all change it, and a file this size makes each of those diffs hard to read (E3 retro B7, E2 retro A13).

**Approach:** Move the declarations into files by concern inside package `core`, unchanged: a pure move, so the tests and behaviour stay exactly as they are.

## Boundaries & Constraints

**Always:** Every declaration and its doc comment move verbatim (only import blocks change). Package `core` stays stdlib only. Each new file starts with the package clause and its own imports.

**Never:** No rename, no signature change, no new or removed declaration, no test file change, no behaviour change. No edit to CLAUDE.md (its Layout line for `session.go` goes to the refactor sweep, entry 9).

</frozen-after-approval>

## Code Map

Current `debugger/core/session.go` (line numbers at baseline). Target file per group:

- **`session.go`** (launch and run): consts `captureWait`, `unverifiedMessage`; `type Session` (137), `NewSession` (188), `Do` (205), `notSupported` (244), `launch` (251), `configurationDone` (349), `setBreakpoints` (379), `run` (391), `Translator` (545), `Log` (555), `resolve` (570), `terminate` (963).
- **`threads.go`** (new): consts `ThreadID` (19), `rootThreadName` (22); `type Thread` (68), `type thread` (93); `ForkThread` (583), `forkThreadLocked` (595), `ExitThread` (607), `Threads` (624), `threadLocked` (638).
- **`stop.go`** (new, the stop decision and resume): `type Frame` (34), `type Stop` (52), `stepMode` and its consts (74–81), `stepState` (86), `liveFrame` (102), `siteKey` (115), `siteCount` (123); `Enter` (658), `Skip` (732), `frameName` (759), `Arrive` (773), `shouldStop` (868), `snapshot` (893), `lfFrame` (905), `Stopped` (911), `resumeRun` (922), `pause` (947).
- **`delivery.go`** (new, delivery and console notes): `nothingDelivered` (468), `Captured` (481), `SetCaptureWait` (495), `waitingNote` (503), `Deliver` (519).
- Leave alone: `frames.go` (ctx frame stack, AD-17), `gate.go`, every `*_test.go`.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/threads.go`, `debugger/core/stop.go`, `debugger/core/delivery.go` -- create, moving the declarations above verbatim with their doc comments -- the split by concern.
- [x] `debugger/core/session.go` -- remove the moved declarations; trim imports to what remains -- launch and run only.
- [x] Fix each file's imports (`go build`, `go vet`, then gofmt) -- a pure move must compile with no other edit.

**Acceptance Criteria:**
- Given the baseline, when the split is done, then `git diff --stat` shows changes only in `debugger/core/` non-test `.go` files, and the set of top-level declarations in package `core` (names, signatures and bodies) is identical before and after.
- Given the split, when `task ci` runs, then it is green with no test file changed.
- Given the split, when `session.go` is read, then it holds none of the thread, stop-decision or delivery declarations listed above.

## Implementation Notes

- The move was done by a scratchpad Go tool that cut each declaration with its doc comment from the file's text and wrote each file's imports from what it uses. `session.go` 979 → 415 lines; `threads.go` 101, `stop.go` 399, `delivery.go` 83.
- Checks: a hash of all 184 top-level declarations in package `core` (doc comment through body) matches before and after; a sorted-line comparison of the old file against the four new ones differs only in package clauses and import blocks; no test file changed; `task ci` exit 0.
- CLAUDE.md's Layout line for `session.go` is now stale; deferred to the refactor sweep (deferred-work.md).

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 0, false 0, maybe-false 0. The reviewer found no unmet criterion, broken rule or bug; it re-hashed the 184 declarations (all match) and ran build, vet, gofmt, the `core` tests and golangci-lint. Its note on CLAUDE.md's stale Layout line was not a finding (the plan defers it); an entry was appended to deferred-work.md for entry 9.

## Design Notes

Do the move with a tool, not by hand: a short Go program in the scratchpad using `go/parser` and `go/printer` (or `go/ast` positions incl. doc comments) to cut each named declaration's source range, including its doc comment, into the target file. Verify the declaration set with a second run that prints `name + body hash` per declaration for baseline and result and compares them.

## Verification

**Commands:**
- `go build ./... && go vet ./debugger/core/` -- expected: clean.
- `git diff --stat 00fe346870794cdc26093cc66bf0b1e8b2579a08 -- '*_test.go'` -- expected: empty.
- `task ci` -- expected: exit 0.
- `wc -l debugger/core/session.go` -- expected: well under 979, holding only launch and run.
