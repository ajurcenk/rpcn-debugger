---
title: 'Repo scaffold, task runner and installable skeleton extension'
type: 'chore'
ticket: '1'
created: '2026-10-12'
status: 'built'
baseline_revision: 'd6791c6a493eb35ebb123c361406fc13fdd7c34d'
route: 'full'
route_source: 'auto'
risk: 'medium'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
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
- [x] repo root: `git init -b main`; `git remote add origin git@github.com:ajurcenk/rpcn-debugger.git`; commit the existing planning and BMAD files as the baseline commit, and record its full hash in `baseline_revision`. This gives a revision to diff against.
- [x] `.gitignore`: ignore `bin/`, `vscode/node_modules/`, `vscode/out/`, `vscode/dist/`, `*.vsix` and `_bmad/render/`. Build outputs and render cache stay out of git.
- [x] `go.mod` / `go.sum`: the module, go 1.26.6, the connect/benthos pins via `go get` and `go mod tidy`. AD-2 pins.
- [x] `debugger/doc.go` and `debugger/{core,engine,rewrite,blobl,dap}/doc.go`: a package comment each, naming its unit from the spine. This is the Structural Seed layout.
- [x] `cmd/rpcn-debug/main.go`: flags with `--version`. Builds the env via `schema.Standard(version, date).Environment()` and blank-imports `components/all`. The no-args stub goes to stderr, exit 1. This is the AD-1 stock binary.
- [x] `cmd/rpcn-debug/version.go`, `version_test.go`: read the connect/benthos versions from `runtime/debug.ReadBuildInfo`, with `version`/`date` set by ldflags. The test asserts that the connect version reported is v4.112.0.
- [x] `vscode/package.json`, `tsconfig.json`, `src/extension.ts`, `LICENSE`, `README.md`, `.vscodeignore`: the skeleton extension. `contributes.debuggers` has type/label/`languages: ["yaml"]`; `activate` is a no-op. B7.
- [x] `Taskfile.yml`: the tasks `build:go` (to `bin/rpcn-debug`, with ldflags), `test:go`, `build:ext` (`npm ci`, `tsc`), `package:ext` (`vsce package --no-dependencies -o dist/`), and `ci` (all of these in order). This is the `task ci` contract for 1.2 to 1.5.
- [x] `LICENSE` (MIT, ajurcenk, 2026) and `README.md`: prerequisites (Go 1.26, Node ≥ 22, Task, Docker, plus any CGO or system libraries that `components/all` turns out to need), `task ci`, and how to install the `.vsix`.

**Acceptance Criteria:**
- Given a fresh clone, when `task ci` runs, then it exits 0 and a single `vscode/dist/pipeline-debugger-for-redpanda-connect-*.vsix` exists.
- Given the `.vsix` is installed with `code --install-extension`, when the user opens Run and Debug and picks "create a launch.json", then "Redpanda Connect" is offered as a debugger.
- Given the built binary, when `bin/rpcn-debug --version` runs, then stdout names connect v4.112.0 and benthos v4.81.0, and nothing else is written to stdout.
- Given the repo, when `git remote -v` runs, then `origin` is the user's GitHub remote, and nothing has been pushed.

## Implementation Notes

- Git init, remote and baseline commit already existed at `d6791c6` when implementation started; nothing was redone.
- `go mod tidy` initially failed with an ambiguous import of `github.com/containerd/containerd/api/services/content/v1` (containerd v1.7.12 from connect vs `containerd/api` v1.11.1 from iceberg-go's tests). `go get github.com/containerd/containerd@v1.7.36` (which splits `api/` out) resolves it; tidy then drops containerd from `go.mod` entirely, and plain `go mod tidy` is clean. containerd isn't linked into the binary. No `replace`, no vendoring.
- Connect's own `replace github.com/99designs/keyring => github.com/Jeffail/keyring` is not inherited by consumers; keyring isn't linked into `rpcn-debug`, so it doesn't matter today.
- `CGO_ENABLED=0` is set for all Taskfile tasks; the full build succeeds pure Go (binary: ~467 MB unstripped, ~338 MB with the Taskfile's `-s -w`; a cold build took ~2m20s here).
- `vsce` requires `activationEvents` when `main` is set, so the manifest declares `onDebugResolve:redpanda-connect`.
- The debugger contribution adds a minimal `initialConfigurations` entry (`type`, `request`, `name` only, no launch keys). Without it, VS Code lists a debugger under "create a launch.json" only when a YAML editor is active. It carries no schema or defaults, so AD-12 still holds; epic-tracer's generator may take it over.
- The local VS Code is 1.103.2, below `engines.vscode ^1.120.0`, so `code --install-extension` refuses the `.vsix` here ("not compatible with VS Code '1.103.2'"). The manual picker check needs VS Code 1.120+.

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 1, medium 1, low 0, false 0, maybe-false 1.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | CLAUDE.md still says there is no code, build, tests or git history | medium | defer | Confirmed: CLAUDE.md:7 says this, while the diff adds a Go module, a Taskfile, tests, an extension and git history. Agents reading it get a wrong picture. The fix edits an agent-context file, so it is deferred and offered to the user |
| 2 | `initialConfigurations` in vscode/package.json conflicts with AD-12 (Go owns the launch schema and defaults) | maybe-false | reject (low if true) | The block holds only VS Code's standard `type`/`request`/`name`, none of the LaunchConfig keys, defaults or enums that AD-12 binds. Drift only happens if epic-tracer's schema check compares this block, which isn't designed yet. To settle: the epic-tracer generator story decides whether it emits `initialConfigurations`. A handoff note was added to epic-tracer's Notes |
| 3 | AC 2 (debugger offered in "create a launch.json") was never verified: local VS Code is 1.103.2, and `engines.vscode` is `^1.120.0` | high | intent_gap | Confirmed by the Implementation Notes: the install is refused locally. The root cause is the frozen `engines.vscode ^1.120.0` against the user's installed VS Code. Settling it needs a human decision (update VS Code, or lower the minimum). The code itself is correct. **Resolved 2026-10-12:** the user will use VS Code 1.141.0, so the minimum `^1.120.0` stands and no code changes. Verified: the .vsix installs into a portable VS Code 1.141.0 (isolated user-data and extensions dirs) as ajurcenk.pipeline-debugger-for-redpanda-connect@0.0.1, with debugger redpanda-connect / "Redpanda Connect". The picker check in the GUI is left to the user |

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
