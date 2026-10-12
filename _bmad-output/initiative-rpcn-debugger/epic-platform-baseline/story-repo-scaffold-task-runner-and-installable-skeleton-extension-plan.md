---
title: 'Repo scaffold, task runner and installable skeleton extension'
type: 'chore'
ticket: '1'
created: '2026-10-12'
status: 'ready-for-dev'
baseline_revision: ''
route: 'full'
route_source: 'auto'
risk: 'medium'
review: ''
review_source: ''
lenses_ran: []
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** The project has planning artifacts but no code, repo or build. Every later epic needs a Go module, the package layout, a stock binary and an installable extension to build on.

**Approach:**
- Turn the project folder into the git repo for `git@github.com:ajurcenk/rpcn-debugger.git`.
- Scaffold the Go module, the `debugger/` layout and `cmd/rpcn-debug` (stock binary: `schema.Standard` + `components/all`).
- Scaffold the `vscode/` skeleton extension.
- Add a Taskfile whose `task ci` builds, tests and packages a `.vsix` non-interactively.

## Boundaries & Constraints

**Always:**
- Go module `github.com/ajurcenk/rpcn-debugger`, `go 1.26.6`. Pin `github.com/redpanda-data/connect/v4 v4.112.0` and `benthos/v4 v4.81.0`.
- Import only the `public/...` packages of benthos/connect (AD-2).
- Extension `name` is `pipeline-debugger-for-redpanda-connect`, `displayName` is "Pipeline Debugger for Redpanda Connect", `publisher` is `ajurcenk`. It needs `engines.vscode ^1.120.0`, TypeScript 6.0.3, @types/vscode 1.120.0 and @vscode/vsce 4.0.0, and contributes debug type `redpanda-connect` with label "Redpanda Connect".
- MIT `LICENSE` at the repo root and in `vscode/`. A `repository` field, so that `vsce package` never prompts.
- `rpcn-debug` writes nothing to stdout except the `--version` output, and `--version` exits before any DAP mode (AD-15).
- Keep `_bmad/`, `_bmad-output/`, `.agents/`, `.claude/` and `skills-lock.json` unchanged and committed.

**Never:**
- Push to the remote without asking the user.
- No DAP server, `debugger.Serve`, lint gates, Redpanda tests or equivalence harness: those are entries 1.2 to 1.5.
- No `launch.json` schema (`configurationAttributes`): it's generated from Go in epic-tracer (AD-12).
- No `replace` directives. No vendoring.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Version | `rpcn-debug --version` | stdout: `rpcn-debug <version> (connect v4.112.0, benthos v4.81.0)`, exit 0 | (none) |
| No args (pre-1.4) | `rpcn-debug` | Nothing on stdout. stderr says the DAP server isn't implemented yet; exit 1 | Replaced by `Serve` in 1.4 |
| Unknown flag | `rpcn-debug --nope` | Usage on stderr, exit 2 | stdlib `flag` behaviour |
| Fresh clone | `task ci` | Go builds and tests pass; `vscode/dist/*.vsix` produced | Fails fast on the first broken step |

</frozen-after-approval>

## Code Map

The project is greenfield: there is no source code.
- `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md`: Structural Seed (tree), Stack (versions), AD-1, AD-2, AD-12, AD-15, AD-23.
- Connect v4.112.0 local reference: `/home/aleksejjurcenko/projects/redpanda-replicator/v4.112.0/connect`.
  - `public/schema/schema.go:30`: `Standard(version, dateBuilt) *service.ConfigSchema`.
  - `cmd/redpanda-connect/main.go`: the import pattern for `public/components/all`.
- benthos v4.81.0, `public/service/stream_schema.go:71`: `(*ConfigSchema).Environment()`.
- CGO: in v4.112.0 only `zeromq` and `ffi` use CGO, and both are behind the `x_benthos_extra` build tag (`public/components/{zeromq,ffi}/x_benthos_extra.go`). Never set that tag, so the stock binary stays pure Go and no C toolchain belongs in the README. Spike 1.6 confirms this with `CGO_ENABLED=0`.
- Tools present: Go 1.26.0 (the toolchain auto-fetches 1.26.6), Node 22.23 (vsce 4 needs ≥ 22), `task`, `code`, Docker.

## Tasks & Acceptance

**Execution:**
- [ ] repo root: `git init -b main`; `git remote add origin git@github.com:ajurcenk/rpcn-debugger.git`; commit the existing planning and BMAD files as the baseline commit, and record its full hash in `baseline_revision`. This gives a revision to diff against.
- [ ] `.gitignore`: ignore `bin/`, `vscode/node_modules/`, `vscode/out/`, `vscode/dist/`, `*.vsix` and `_bmad/render/`. Build outputs and render cache stay out of git.
- [ ] `go.mod` / `go.sum`: the module, go 1.26.6, the connect/benthos pins via `go get` and `go mod tidy`. AD-2 pins.
- [ ] `debugger/doc.go` and `debugger/{core,engine,rewrite,blobl,dap}/doc.go`: a package comment each, naming its unit from the spine. This is the Structural Seed layout.
- [ ] `cmd/rpcn-debug/main.go`: flags with `--version`. Builds the env via `schema.Standard(version, date).Environment()` and blank-imports `components/all`. The no-args stub goes to stderr, exit 1. This is the AD-1 stock binary.
- [ ] `cmd/rpcn-debug/version.go`, `version_test.go`: read the connect/benthos versions from `runtime/debug.ReadBuildInfo`, with `version`/`date` set by ldflags. The test asserts that the connect version reported is v4.112.0.
- [ ] `vscode/package.json`, `tsconfig.json`, `src/extension.ts`, `LICENSE`, `README.md`, `.vscodeignore`: the skeleton extension. `contributes.debuggers` has type/label/`languages: ["yaml"]`; `activate` is a no-op. B7.
- [ ] `Taskfile.yml`: the tasks `build:go` (to `bin/rpcn-debug`, with ldflags), `test:go`, `build:ext` (`npm ci`, `tsc`), `package:ext` (`vsce package --no-dependencies -o dist/`), and `ci` (all of these in order). This is the `task ci` contract for 1.2 to 1.5.
- [ ] `LICENSE` (MIT, ajurcenk, 2026) and `README.md`: prerequisites (Go 1.26, Node ≥ 22, Task, Docker, plus any CGO or system libraries that `components/all` turns out to need), `task ci`, and how to install the `.vsix`.

**Acceptance Criteria:**
- Given a fresh clone, when `task ci` runs, then it exits 0 and a single `vscode/dist/pipeline-debugger-for-redpanda-connect-*.vsix` exists.
- Given the `.vsix` is installed with `code --install-extension`, when the user opens Run and Debug and picks "create a launch.json", then "Redpanda Connect" is offered as a debugger.
- Given the built binary, when `bin/rpcn-debug --version` runs, then stdout names connect v4.112.0 and benthos v4.81.0, and nothing else is written to stdout.
- Given the repo, when `git remote -v` runs, then `origin` is the user's GitHub remote, and nothing has been pushed.

## Implementation Notes

## Plan Change Log

## Review Triage Log

## Design Notes

- The no-args stub is not a placeholder for scope this entry drops. `debugger.Serve` belongs to entry 1.4, which replaces the stub.
- If `components/all` makes `go build` slow, that's expected. Measuring it is spike 1.6, and nothing here optimises it.
- The skeleton has no adapter yet. Picking the debugger type and launching it will fail until 1.4 adds the `DebugAdapterExecutable`, which is acceptable here.

## Verification

**Commands:**
- `task ci`: expected exit 0, with the `.vsix` in `vscode/dist/`.
- `bin/rpcn-debug --version`: expected one line with the connect and benthos versions; exit 0.
- `bin/rpcn-debug >/dev/null; echo $?`: expected stderr message, and `1`.
- `git log --oneline | wc -l` ≥ 2 and `git status --short` empty after commit.

**Manual checks:**
- Install the `.vsix` in VS Code; "Redpanda Connect" appears in the debugger picker.
