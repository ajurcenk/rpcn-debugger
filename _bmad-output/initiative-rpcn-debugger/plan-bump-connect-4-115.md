---
title: 'Bump to Connect v4.115.0, benthos v4.84.0 and Go 1.27.3'
type: 'chore'
ticket: ''
created: '2026-10-22'
status: 'built'
baseline_revision: '305cb7a3582312a5bc4e736f65368fb290c6c102'
route: 'oneshot'
route_source: 'auto'
risk: 'medium'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** This is a pre-E3 entry from the spine update (A6 gate, user decision 2026-10-22). The project pins Connect v4.112.0, benthos v4.81.0 and Go 1.26.6. The live registries have Connect v4.115.0 and benthos v4.84.0, and both require Go 1.27.3. E3 should start on current versions.

**Approach:**
- In `go.mod`, set `connect/v4` to v4.115.0 and `benthos/v4` to v4.84.0, change the `go` directive to 1.27.3, and run `go mod tidy`. Change `tools/go.mod`'s `go` directive to match; bump golangci-lint only if v2.14.0 can't lint Go 1.27 code.
- Fix whatever the bump breaks, through the public API only (AD-2). Re-check the engine-text dependencies the code matches:
  - `strictRejection` in `debugger/engine/engine.go`;
  - the YAML/lint error regexes in rewrite and engine.
- AD-14: `task ci` must pass, including the AD-8 equivalence corpus and the integration test. Goldens may change only if an engine message changed; record any change.
- Update the Go version in CLAUDE.md, the Stack table in the spine (and a `version` entry in its memlog), and add a "Delivered" entry to `deferred-work.md`.

</frozen-after-approval>

## Implementation Notes

Oneshot: version pins, `go mod tidy`, docs, and only the fixes the build or tests demand.

- `go mod edit -go=1.27.3` with connect v4.115.0 and benthos v4.84.0, then `go mod tidy`. `GOTOOLCHAIN=auto` fetched go1.27.3. `tools/go.mod` gets `go 1.27.3`, and `go mod tidy` there changed only go.sum. golangci-lint v2.14.0 lints Go 1.27 with 0 issues, so it wasn't bumped.
- The build and every test passed unchanged, except `cmd/rpcn-debug/version_test.go`, whose pinned versions were updated. No rewrite golden changed.
- Engine-text dependencies re-checked against the v4.84.0 source: `internal/component/processor/execute.go` is byte-identical to v4.81.0 (the `stepper_test.go` ports of ExecuteTryAll stay valid), and `async_writer.go` still wraps "rejected due to failed processing" (`strictRejection`). `TestStrictInterpolated` passes, so the ConfigQuerier behaviour holds.
- Error-format regexes: `rewrite.go` `yamlLine` and `engine/positions.go` `yamlPos` read yaml.v3's `yaml: line N:` text, and yaml.v3 stays at v3.0.1, so that format can't have changed. `logging.go` `lineRef` and the `lint errors: (L,C)` mapping read benthos texts, and the position tests (`engine/positions_test.go` `TestCheckErrorPositions`, `TestCheckErrorYAMLLine`, …) and `logging_test.go` `TestLogTranslation` pass unchanged, so those formats hold.
- Version mentions updated: `stepper_test.go` and `engine.go` comments (v4.84), the Taskfile CGO note, README (Go 1.27, the `--version` sample), CLAUDE.md (Go 1.27.3), the spine Stack table, plus a memlog `version` entry and a "Delivered" entry in deferred-work.md.

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 3, false 0. All 3 patched.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `initiative-rpcn-debugger.md` (success criterion 3, touch point) still names v4.112.0 / v4.81.0 | low | patch | Updated to v4.115.0 / v4.84.0, keeping the earlier pin in the criterion's history |
| 2 | The plan's regex re-check isn't recorded | low | patch | Implementation Notes now state what each regex reads and why it still holds |
| 3 | The new "Delivered" entry doesn't name the item it closes, and its evidence lists only files | low | patch | Reworded in the file's format; it cites the tests and the `task ci` run. It's this change's own uncommitted entry, so no earlier entry was edited |

## Verification

**Commands:**
- `go build ./... && go test -count=1 ./...` -- expected: PASS.
- `task ci` -- expected: exit 0 (Docker running).

Run 2026-10-22: `go test -count=1 ./...` PASS; `task ci` exit 0 (lint 0 issues, AD-8 corpus, integration, extension).
