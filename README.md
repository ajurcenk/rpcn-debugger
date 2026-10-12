# rpcn-debugger

A visual debugger for [Redpanda Connect](https://docs.redpanda.com/redpanda-connect/) pipelines in VS Code. It captures one batch from a pipeline's input and steps it through the processors over the Debug Adapter Protocol.

> **Status: skeleton.** The repo builds a stock `rpcn-debug` binary and an installable extension that registers the `Redpanda Connect` debugger. The DAP server isn't implemented yet, so a debug session can't start.

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
- Docker (used by integration tests in later milestones)
- VS Code 1.120 or later, to install the extension

The binary is pure Go (`CGO_ENABLED=0`), so no C toolchain or system libraries are needed.

## Build

```sh
task ci
```

This builds `bin/rpcn-debug`, lints the Go code, runs the Go tests, compiles the extension and packages it as `vscode/dist/pipeline-debugger-for-redpanda-connect-<version>.vsix`. The individual steps are `task build:go`, `task lint:go`, `task lint:gomod`, `task test:go`, `task build:ext` and `task package:ext`.

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

## License

[MIT](LICENSE)
