# rpcn-debugger

A visual debugger for [Redpanda Connect](https://docs.redpanda.com/redpanda-connect/) pipelines in VS Code. It captures one batch from a pipeline's input and steps it through the processors over the Debug Adapter Protocol.

> **Status: tracer bullet.** A launch captures one batch from the pipeline's input without acknowledging it, stops before the first processor (input, then pipeline, then output processors), shows the stop's line and each message's content, and steps the batch through the input, pipeline and output processors, nested ones included. Breakpoints come later.

## Layout

| Path | Contents |
| --- | --- |
| `debugger/` | The Go library: the public entry point, and the `core`, `engine`, `rewrite`, `blobl` and `dap` packages |
| `cmd/rpcn-debug/` | The stock binary: every Connect component, built on the library |
| `vscode/` | The VS Code extension |
| `test/equivalence/` | The equivalence harness: discovers the corpus cases and runs each pipeline without the debugger |
| `testdata/equivalence/` | The equivalence corpus: one directory per case, `<name>/pipeline.yaml` |
| `testdata/tracer/` | The sample pipeline to try the debugger on |
| `Taskfile.yml` | Build, test and package tasks |

## Prerequisites

- Go 1.26 (the `go 1.26.6` toolchain is fetched automatically if needed)
- Node.js 22 or later (required by `@vscode/vsce` 4)
- [Task](https://taskfile.dev)
- [jq](https://jqlang.org), used by `task lint:gomod`
- Docker, used by the integration tests (`task test:integration`), which start a Redpanda container. The task keeps testcontainers' Ryuk reaper on and fails if a test container is left behind
- VS Code 1.120 or later, to install the extension

The binary is pure Go (`CGO_ENABLED=0`), so no C toolchain or system libraries are needed.

## Build

```sh
task ci
```

This builds `bin/rpcn-debug`, lints the Go code, runs the Go unit tests, the equivalence cases and the integration tests, compiles the extension, runs the extension's DAP test against the built binary, and packages the extension as `vscode/dist/pipeline-debugger-for-redpanda-connect-<version>.vsix`. The individual steps are `task build:go`, `task lint:go`, `task lint:gomod`, `task lint:schema`, `task test:go`, `task test:equivalence`, `task test:integration`, `task build:ext`, `task test:ext` and `task package:ext`.

`task test:equivalence` runs every case in `testdata/equivalence/` through its own input and processors, with no debugger, and lists each case as a subtest. To add a case, create `testdata/equivalence/<name>/pipeline.yaml`; its `output` is replaced by a collector, so any output will do.

`task test:ext` starts the binary named by `RPCN_DEBUG_BIN` (default `../bin/rpcn-debug`, relative to `vscode/`), so run `task build:go` and `task build:ext` first.

The lint gates enforce two architecture rules:

- `debugger/...` never writes to stdout, which belongs to DAP (forbidigo).
- Only the `public/...` packages of benthos and connect are imported, and go.mod has no `replace` for them (depguard and `lint:gomod`).

golangci-lint is pinned in `tools/go.mod` and runs as `go tool -modfile=tools/go.mod golangci-lint`, so you don't need to install it.

The first build compiles every Connect component and takes a few minutes.

```sh
bin/rpcn-debug --version
# rpcn-debug <version> (connect v4.112.0, benthos v4.81.0)
```

## Install the extension

```sh
code --install-extension vscode/dist/pipeline-debugger-for-redpanda-connect-*.vsix
```

Or, in VS Code, run **Extensions: Install from VSIX...** and pick the file. "Redpanda Connect" then appears in the debugger list under **Run and Debug > create a launch.json file**.

The extension doesn't bundle the binary yet. Point the `rpcnDebug.binaryPath` setting at the one you built, as an absolute path to `bin/rpcn-debug` in this repo. Set it in your user settings (**Preferences: Open User Settings (JSON)**):

```json
"rpcnDebug.binaryPath": "/path/to/rpcn-debugger/bin/rpcn-debug"
```

The setting is machine-scoped, so a workspace's `.vscode/settings.json` can't change which binary VS Code runs. If it is unset, starting a session shows an error asking you to set it.

## Try it

Open `testdata/tracer/pipeline.yaml` and press F5 (with no `launch.json`, or a `Debug pipeline` entry without `pipeline`): VS Code stops before the mapping with the captured message under **Variables** (Batches › batch 0 › message 0: content, metadata, and an `internal` node for `__` keys); Step Over moves to the end stop on the `processors:` line, and the next Step Over ends the session. Breakpoints show as unverified for now.

Stepping: the debugger stops before each processor and once after the last one of each list (on the list's key line). Step Over (F10) runs a processor, nested children included; Step In (F11) enters a parent's processors (`branch`, `switch` cases, …), adding a frame to the call stack, each frame with its own batch under Variables; Step Out (Shift+F11) runs to the end of the current list and then back to its parent; Pause stops at the next processor.

A `redpanda-connect` launch.json entry takes `pipeline` (default: the active file), `cwd` (default: the workspace folder; relative paths resolve against it), `maxMessages` (default 10: the captured batch is cut to that many, and the Debug Console says so; try `testdata/tracer/batch12.yaml`), `env` and `envFile` (validated, applied in a later version; use `${env:NAME}` rather than literal secrets). Any other key fails the launch. The schema VS Code validates against is generated from Go: after changing `dap.LaunchConfig`, run `task gen:schema` (`task lint:schema`, part of `task ci`, fails on a stale `vscode/package.json`).

## License

[MIT](LICENSE)
