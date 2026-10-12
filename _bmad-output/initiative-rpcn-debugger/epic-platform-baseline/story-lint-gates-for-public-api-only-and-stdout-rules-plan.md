---
title: 'Lint gates for public-API-only and stdout rules'
type: 'chore'
ticket: '2'
created: '2026-10-12'
status: 'in-progress'
baseline_revision: ''
route: 'oneshot'
route_source: 'auto'
risk: 'low'
review: ''
review_source: ''
lenses_ran: []
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
