# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project state

This repository is building a VS Code visual debugger for Redpanda Connect pipelines. The goal is to capture one batch from a pipeline's input and step it through the processors over the Debug Adapter Protocol. Beyond the platform baseline (the scaffold, `task ci`, the test harnesses), epic-tracer delivers the dry-run debugger:
- F5 captures one batch (truncated to `maxMessages`, default 10) without acking it.
- The batch is stepped through `input.processors`, `pipeline.processors` and `output.processors`, and through the child lists of every non-concurrent AD-7 parent (`branch`, `while`, `retry`, `try`, `catch`, `for_each`, `switch` cases, `group_by` groups) and of `processor_resources`. Next/StepIn/StepOut/Continue/Pause stop at the YAML lines, and frames carry the `[msg i/n]`, `[attempt i/max]`, `[loop i]` and `[group i]` labels. Concurrent (`workflow`, `parallel`, parallel `retry`) and unknown parents run as one opaque step, with a warning.
- `rpcn_debug_sink` delivers each output batch to the Debug Console. Launch and lint errors name the user's file line and path, and strict-mode output rejection is a note, not a run failure.
- The AD-8 equivalence corpus proves the debugger doesn't change the output.

Bloblang statement stepping (`debugger/blobl`), `output: real`, Restart/replay and concurrency are later epics. The binding architecture (ADs) is `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md`, with the decision log in `.memlog.md` next to it. Follow its ADs when writing code. Work is tracked as a BMAD ticket tree under `_bmad-output/initiative-rpcn-debugger/` (an initiative-level `tickets.toml` listing the epics, one `tickets.toml` per epic once it is sliced, one plan file per built entry, and `deferred-work.md` for findings left for later entries).

Layout:

- `debugger/`: the Go library, with entry point `debugger.Serve(env, in, out, opts...)` (AD-1; `WithLogOutput` sets the log destination). `serve.go` wires the packages, and `logging.go` is the one AD-15 slog handler (stderr plus DAP Output, with engine paths translated to user paths). Packages:
  - `core` (stdlib only): the closed AD-3 protocol in `protocol.go` behind `Session.Do(ctx, Command)` and `Frontend.Emit(Event)`; the `core.Error` code registry in `errors.go`; the AD-11 Snapshot in `snapshot.go`; the Session (`session.go`: stop predicate, step state, one Snapshot per frame), the pause gate and the threads (`session.go`), the ctx frame stack and per-call entry marks (`frames.go`, AD-17); the env lookup with `envFile` parsing (`dotenv.go`, AD-22); and the `Engine`/`Frontend`/`LineMap`/`Translate` ports.
  - `rewrite`: the yaml.v3 rewrite into the capture config and the `rpcn_debug_stepper` configs (`rewrite.go`, `walk.go` with the one AD-7 parent table, `refs.go` rejecting undefined resource references, `stdio.go` for the AD-15 stdio rules), the LineMap, the engine-path translation (`translate.go`) and the position maps that put lint errors at file lines (`positions.go`). Golden tests live in `rewrite/testdata/` (`go test ./debugger/rewrite/ -update` rewrites them).
  - `engine`: capture, the stepper (`all`/`try` modes, strict skip, frames and repeat labels), the dry-run sink, Snapshot building, lint-error mapping, and the `core.Engine` adapter on `env.Clone()`.
  - `dap`: the DAP server, `LaunchConfig` (the launch schema source, AD-12) and the Variables tree. It imports only `core`, and only it may import go-dap (AD-3).
  - `blobl`: still just a package doc.
- `cmd/rpcn-debug/`: the thin stock binary. It builds `schema.Standard(...).Environment()` with `components/all` and serves DAP on stdio. stdout carries only the DAP stream or the `--version` line.
- `cmd/gen-launch-schema/`: writes the launch.json schema and `initialConfigurations` into `vscode/package.json` from `dap.LaunchConfig` (`-check` fails on drift).
- `test/dapdrive/`: a scripted DAP client (plain package, raw JSON, no go-dap). `Run` drives `debugger.Serve` with a `Script` per stop and returns the stops, Debug Console output and delivered batches.
- `test/equivalence/` + `testdata/equivalence/<name>/`: the AD-8 harness and corpus. Each case has a `pipeline.yaml` (plus any input files) and an `expect.yaml` (reference batches and messages, Step In stops, forbidden paths, expected output). Its input must yield a single batch of at most 10 messages (the same-batch rule). `TestEquivalence` runs each case with Continue, Next and Step In and compares the delivered batches with `Case.Reference` (which also counts the input batches for the same-batch rule; `RunReference` wraps it).
- `test/integration/` (build tag `integration`): Redpanda container tests via testcontainers-go.
- `testdata/tracer/`: manual F5 samples: `pipeline.yaml` (the sample), `nested.yaml`, `batch12.yaml` (truncation) and `parents/` (one per AD-7 parent).
- `vscode/`: the thin TypeScript extension. It starts the binary from the machine-scoped `rpcnDebug.binaryPath` setting and fills a missing `pipeline` and `cwd`.

Commands (Task):

- `task ci`: runs build, lint (including the launch-schema check), unit, equivalence and integration tests, then the extension build, its test and the `.vsix` package. CI is local only, with no hosted CI.
- Single steps: `task build:go`, `lint:go`, `lint:gomod`, `gen:schema`, `lint:schema`, `test:go`, `test:equivalence`, `test:integration` (needs Docker), `build:ext`, `test:ext` (needs `build:go` and `build:ext` first), `package:ext`.
- Builds are pure Go (`CGO_ENABLED=0`). Never set the `x_benthos_extra` tag. Go is 1.26.6, and Node must be 22 or later.

Rules the gates enforce:

- Import only `public/...` of benthos and connect, and never add a `replace` for them (AD-2: depguard and `lint:gomod`).
- No `fmt.Print*` or `os.Stdout` in `debugger/...`, because stdout belongs to DAP (AD-15: forbidigo).

golangci-lint is pinned in `tools/go.mod` and runs via `go tool -modfile=tools/go.mod`. The dev license is read at run time from `/etc/redpanda/redpanda.license` and is never committed (epic-platform-baseline decision, 2026-10-13); AD-22 fixes the license lookup order and when it is applied.

## BMAD skills

- The skills live in `.agents/skills/`. Each one is symlinked into `.claude/skills/` (`../../.agents/skills/<name>`) so Claude Code can find it. When a skill is added or removed in `.agents/skills/`, add or remove its symlink too.
- Treat `.agents/skills/` and `skills-lock.json` as vendored. Do not edit them by hand. Customization goes through each skill's `customize.toml` / the `bmad-customize` skill.
- The `bmad` skill is the entry point (help, next-step recommendations, setup). Its scripts run via `uv run`, and it expects a `_bmad/` folder at the project root (set up; the active initiative is `initiative-rpcn-debugger`, and outputs go to `_bmad-output/`).
