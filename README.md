# rpcn-debugger

A visual debugger for [Redpanda Connect](https://docs.redpanda.com/redpanda-connect/) pipelines in VS Code. It captures one batch from a pipeline's input and steps it through the processors over the Debug Adapter Protocol.

> **Status: skeleton.** The repo builds a stock `rpcn-debug` binary and an installable extension that registers the `Redpanda Connect` debugger. The DAP server is a seed: it answers `initialize` and `disconnect`, and a launch ends at once, so a debug session starts and ends without stepping anything yet.

## Layout

| Path | Contents |
| --- | --- |
| `debugger/` | The Go library: the public entry point, and the `core`, `engine`, `rewrite`, `blobl` and `dap` packages |
| `cmd/rpcn-debug/` | The stock binary: every Connect component, built on the library |
| `vscode/` | The VS Code extension |
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

This builds `bin/rpcn-debug`, lints the Go code, runs the Go unit and integration tests, compiles the extension, runs the extension's DAP test against the built binary, and packages the extension as `vscode/dist/pipeline-debugger-for-redpanda-connect-<version>.vsix`. The individual steps are `task build:go`, `task lint:go`, `task lint:gomod`, `task test:go`, `task test:integration`, `task build:ext`, `task test:ext` and `task package:ext`.

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

The setting is machine-scoped, so a workspace's `.vscode/settings.json` can't change which binary VS Code runs. If it is unset, starting a session shows an error asking you to set it. With it set, pressing F5 on the `Debug pipeline` configuration starts a session that ends at once.

## License

[MIT](LICENSE)
