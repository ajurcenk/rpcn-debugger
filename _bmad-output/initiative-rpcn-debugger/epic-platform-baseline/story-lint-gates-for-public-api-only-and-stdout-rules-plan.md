---
title: 'Lint gates for public-API-only and stdout rules'
type: 'chore'
ticket: '2'
created: '2026-10-12'
status: 'built'
baseline_revision: 'b0c12c09eaf404edca22347295c5eb08e61ee7ab'
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

**Problem:** AD-2 (public engine API only, no patching) and AD-15 (stdout belongs to DAP) aren't enforced, so any later story can break them silently.

**Approach:** Add golangci-lint v2.14.0 to `task ci`, pinned through a `tool` directive in a separate `tools/go.mod`. Enable forbidigo for `fmt.Print*`/`os.Stdout`, scoped to `debugger/...`. Add a depguard allow-list that permits only `public/...` under `github.com/redpanda-data`. Add a `task` check that fails when go.mod has a `replace` for benthos or connect.

</frozen-after-approval>

## Code Map

- `Taskfile.yml`: has `build:go`, `test:go`, `build:ext`, `package:ext` and `ci`; `env.CGO_ENABLED='0'`. Add `lint:go` and `lint:gomod` to `ci`, after `build:go`.
- `debugger/**` holds only `doc.go` files today (package comments), so the rules start green.
- `cmd/rpcn-debug/main.go` writes to stdout for `--version` on purpose (AD-15 allows it), so forbidigo must not apply outside `debugger/`.
- The tools go in a separate module because golangci-lint's dependencies would otherwise mix into the product's go.mod and the Connect dependency graph, which already needed a containerd fix in 1.1.

## Implementation Notes

The oneshot route was chosen because there are about 60 hand-written lines (`.golangci.yml`, Taskfile tasks, README) plus a generated `tools/go.mod`/`go.sum`.

- `tools/go.mod`: `go get -tool -modfile=tools/go.mod github.com/golangci/golangci-lint/v2/cmd/golangci-lint@v2.14.0`, run as `go tool -modfile=tools/go.mod golangci-lint run ./...`.
- `.golangci.yml` (v2 format), run with the `default: none` linter set and only these enabled:
  - forbidigo with `analyze-types: true`, forbidding `^fmt\.Print.*$` and `^os\.Stdout$` with an AD-15 message, plus an exclusion rule using `path-except: ^debugger/` for forbidigo;
  - depguard rule `public-api-only` (`list-mode: lax`, `files: [$all]`): deny `github.com/redpanda-data`, allow `github.com/redpanda-data/benthos/v4/public` and `github.com/redpanda-data/connect/v4/public`. The longest match wins, so this is an allow-list.
- `lint:gomod`: `go mod edit -json | jq` fails with "AD-2: replace directive for <path> is forbidden" when any `Replace[].Old.Path` matches `github.com/redpanda-data/(benthos|connect)/v4`.

Implementation (2026-10-12):
- Files: `tools/go.mod` + `tools/go.sum` (golangci-lint v2.14.0 as a `tool`), `.golangci.yml`, `Taskfile.yml` (`lint:go`, `lint:gomod`, both in `ci` after `build:go`), `README.md` (lint section, jq prerequisite).
- Verified by planting each violation, then removing it:
  - `fmt.Println` in debugger/core → forbidigo, with the AD-15 message;
  - `os.Stdout` (on its own line, because golangci-lint keeps one issue per line) → forbidigo;
  - `common-go/license` import from cmd/rpcn-debug → depguard `public-api-only`, with the AD-2 message;
  - `benthos/v4/internal/message` import → a Go typecheck "use of internal package" error (accepted by the ticket);
  - `replace connect/v4` → `lint:gomod` prints "AD-2: replace directive for github.com/redpanda-data/connect/v4 is forbidden" and exits non-zero.

  After removal: 0 issues, and `task ci` exits 0.
- Surprise: the depguard allow-list also denies `github.com/redpanda-data/connect/public/bundle/free/v4`, a public module whose path isn't under `connect/v4/public`. Nothing imports it today. If it's ever needed, add it to `allow` deliberately.
- Surprise: `jq` became a build prerequisite (`lint:gomod`). It's listed in the README.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 1, false 0, maybe-false 0. All three were patched.
- medium, patch: with no pipefail, `lint:gomod` passed when `go mod edit -json` failed (reviewer reproduced it with a broken go.mod). Fix: `go mod edit -json` runs on its own and fails the task ("lint:gomod: cannot read go.mod"). Verified with an appended `garbage(`.
- medium, patch: the replace regex only matched the exact `benthos/v4` and `connect/v4` paths, missing `connect/public/bundle/free/v4` and future majors. Fix: `^github\.com/redpanda-data/(benthos|connect)(/|$)`, dots escaped. Verified: bundle and benthos replaces are caught, and an unrelated `common-go/license` replace is allowed.
- low, patch: the README said "lints it" (the binary). Changed to "lints the Go code".
