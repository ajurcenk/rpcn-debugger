# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

This repository is building a VS Code visual debugger for Redpanda Connect pipelines. The goal is to capture one batch from a pipeline's input and step it through the processors over the Debug Adapter Protocol. Today only the platform baseline exists: the scaffold, `task ci`, a DAP seed that answers `initialize`/`launch`/`disconnect`, and the test harnesses. Capture and stepping arrive with epic-tracer. Its binding architecture (ADs) is `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md`, with the decision log in `.memlog.md` next to it. Follow its ADs when writing code. Work is tracked as a BMAD ticket tree under `_bmad-output/initiative-rpcn-debugger/` (an initiative-level `tickets.toml` listing the epics, one `tickets.toml` per epic once it is sliced, and one plan file per built entry).

Layout:

- `debugger/`: the Go library, with entry point `debugger.Serve(env, in, out, opts...)` (AD-1). Its packages are `core`, `engine`, `rewrite`, `blobl` and `dap`. Only `dap` has code so far (the DAP seed); the other four hold just a package doc until epic-tracer. Only `debugger/dap` may import go-dap (AD-3).
- `cmd/rpcn-debug/`: the thin stock binary. It builds `schema.Standard(...).Environment()` with `components/all` and serves DAP on stdio. stdout carries only the DAP stream or the `--version` line.
- `test/integration/` (build tag `integration`): Redpanda container tests via testcontainers-go.
- `test/equivalence/` + `testdata/equivalence/<name>/pipeline.yaml`: the AD-8 harness (`Cases`, `Case.RunReference`).
- `vscode/`: the thin TypeScript extension. It starts the binary from the machine-scoped `rpcnDebug.binaryPath` setting.

Commands (Task):

- `task ci`: runs build, lint, unit, equivalence and integration tests, then the extension build, its test and the `.vsix` package. CI is local only, with no hosted CI.
- Single steps: `task build:go`, `lint:go`, `lint:gomod`, `test:go`, `test:equivalence`, `test:integration` (needs Docker), `build:ext`, `test:ext` (needs `build:go` and `build:ext` first), `package:ext`.
- Builds are pure Go (`CGO_ENABLED=0`). Never set the `x_benthos_extra` tag. Go is 1.26.6, and Node must be 22 or later.

Rules the gates enforce:

- Import only `public/...` of benthos and connect, and never add a `replace` for them (AD-2: depguard and `lint:gomod`).
- No `fmt.Print*` or `os.Stdout` in `debugger/...`, because stdout belongs to DAP (AD-15: forbidigo).

golangci-lint is pinned in `tools/go.mod` and runs via `go tool -modfile=tools/go.mod`. The dev license is read at run time from `/etc/redpanda/redpanda.license` and is never committed (epic-platform-baseline decision, 2026-10-13); AD-22 fixes the license lookup order and when it is applied.

## BMAD skills

- The skills live in `.agents/skills/`. Each one is symlinked into `.claude/skills/` (`../../.agents/skills/<name>`) so Claude Code can find it. When a skill is added or removed in `.agents/skills/`, add or remove its symlink too.
- Treat `.agents/skills/` and `skills-lock.json` as vendored. Do not edit them by hand. Customization goes through each skill's `customize.toml` / the `bmad-customize` skill.
- The `bmad` skill is the entry point (help, next-step recommendations, setup). Its scripts run via `uv run`, and it expects a `_bmad/` folder at the project root (set up; the active initiative is `initiative-rpcn-debugger`, and outputs go to `_bmad-output/`).
