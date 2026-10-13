---
title: 'DAP adapter seed, debugger.Serve and extension wiring'
type: 'feature'
ticket: '4'
created: '2026-10-13'
status: 'built'
baseline_revision: '9fdb975f532b7b0417550b1ecface50ded071814'
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

**Problem:** `rpcn-debug` with no arguments exits with "not implemented", and the extension has no adapter, so picking the Redpanda Connect debugger can't start a session. Every later epic needs a working DAP channel from VS Code to `debugger.Serve`.

**Approach:** Add `debugger.Serve(env, in, out, opts...)` (AD-1), backed by a minimal go-dap server in `debugger/dap` that answers `initialize` with empty capabilities and handles `disconnect`. Have `cmd/rpcn-debug` call it over stdio. Wire the extension: a `rpcnDebug.binaryPath` setting and a `DebugAdapterExecutable` that starts the binary. Add a TS test with `@vscode/debugadapter-testsupport` that `task ci` runs against the built binary.

## Boundaries & Constraints

**Always:**
- Pin `github.com/google/go-dap v0.12.1-0.20260926015354-7505f696918e` and `@vscode/debugadapter-testsupport 1.68.0` (Stack).
- No go-dap type appears outside `debugger/dap` (AD-3). `debugger` exposes only `Serve`, `Option` and nothing that names go-dap.
- Only the DAP encoder writes to `out` (AD-15). Diagnostics go to stderr through `log/slog`. The forbidigo lint must stay green.
- `Serve` never modifies the caller's `env` (AD-1). It is unused for now except to be held for later epics.
- `initialize` returns empty capabilities. The AD-14 version fields belong to epic-local-release.
- `rpcnDebug.binaryPath` has `"scope": "machine"`, so a workspace's `.vscode/settings.json` can't make VS Code run an arbitrary binary.
- `--version` keeps exiting before any DAP mode.
- Decision (user, 2026-10-13): `launch` is also answered with success and followed by a `terminated` event, so F5 starts and ends a session without a dialog. epic-tracer replaces this with the real launch.

**Never:**
- No `core.Session`, protocol.go, `LaunchConfig`, launch schema or `configurationAttributes`: those belong to epic-tracer (AD-3, AD-12).
- No bundled binary or download. Bundling is epic-local-release (AD-23 MVP).
- No `initialized` event and no breakpoint, thread or stack handling.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Handshake | `initialize` then `disconnect` on stdin | Success responses for both, with `seq`/`request_seq` correct; `Serve` returns nil, exit 0 | (none) |
| Client vanishes | stdin hits EOF before `disconnect` | `Serve` returns nil, exit 0 | Logged on stderr |
| F5 launch | `initialize`, then `launch` | Success for both, then a `terminated` event. VS Code sends `disconnect`, and the process exits 0 | (none) |
| Unknown request | e.g. `threads` | Error response `success:false`, message names the command; the server keeps running | (none) |
| Malformed frame | Bad Content-Length or bad JSON | `Serve` returns an error, exit 1 | Error on stderr; nothing extra written to stdout |
| binaryPath unset | F5 in VS Code | No adapter is started | An error message tells the user to set `rpcnDebug.binaryPath` |

</frozen-after-approval>

## Code Map

- `cmd/rpcn-debug/main.go`: `run(args, stdout, stderr)` currently prints "not implemented" and exits 1 on no args. Change it to `run(args, stdin, stdout, stderr)` and call `debugger.Serve(env, stdin, stdout)`. Keep `standardEnvironment()` and the `--version` path.
- `cmd/rpcn-debug/version_test.go`: `TestRunNoArgs` asserts the old stub. Rewrite it to feed initialize and disconnect frames on stdin, then assert both responses on stdout and exit 0. `TestRunVersion` and `TestRunUnknownFlag` keep passing with the new signature.
- `debugger/doc.go`: rewrite the package comment. Add `debugger/serve.go`, which holds `Serve` and `Option` (a functional option with no options yet).
- `debugger/dap/doc.go` says "imports only debugger/core". Seed scope doesn't import core yet, which is still within that rule. Add `debugger/dap/server.go`.
- go-dap v0.12.1 API: `dap.ReadProtocolMessage(*bufio.Reader) (Message, error)`, `dap.WriteProtocolMessage(io.Writer, Message) error`, and the request/response structs in `schematypes.go` (`InitializeRequest`, `InitializeResponse`, `DisconnectRequest`, `ErrorResponse`). `ReadProtocolMessage` returns `io.EOF` at clean end.
- `vscode/src/extension.ts` is a no-op `activate`. Add `registerDebugAdapterDescriptorFactory('redpanda-connect', …)`, which returns `new vscode.DebugAdapterExecutable(binaryPath)`.
- `vscode/package.json`:
  - add `contributes.configuration` with `rpcnDebug.binaryPath`;
  - `devDependencies` already has `@vscode/debugadapter-testsupport 1.68.0`, added during investigation. Add `@types/node` 22.x;
  - add a `test` script.
- `vscode/tsconfig.json`: `types: ["vscode"]` needs `"node"` added for the test.
- `vscode/.vscodeignore`: exclude `out/test/**`.
- `@vscode/debugadapter-testsupport` `DebugClient.start()` spawns `runtime [executable]`, which would pass a stray positional argument to `rpcn-debug`. The test spawns the binary itself and calls `dc.connect(proc.stdout, proc.stdin)` (inherited from `ProtocolClient`). It asserts the exit code on its own.
- `Taskfile.yml`: add `test:ext` (`npm test`, needs `bin/rpcn-debug`) to `ci`, after `build:ext` and before `package:ext`. The binary path is passed as `RPCN_DEBUG_BIN` and defaults to `../bin/rpcn-debug`.
- Node 22.23 has `node --test`, so no mocha is needed.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/dap/server.go`: `Serve(in io.Reader, out io.Writer, log *slog.Logger) error` with a single-goroutine read loop. It answers `initialize` (empty `Capabilities`), `launch` (success, then a `terminated` event) and `disconnect`, sends an error response for anything else, returns nil on EOF or after disconnect, and returns an error on a malformed frame. This is the U-DAP seed.
- [x] `debugger/dap/server_test.go`: table tests over the I/O matrix rows (handshake, F5 launch, EOF, unknown request, malformed frame), using in-memory pipes. They prove the protocol without a binary.
- [x] `debugger/serve.go` and `debugger/doc.go`: `Serve(env *service.Environment, in io.Reader, out io.Writer, opts ...Option) error` delegates to `dap.Serve` with a stderr slog logger, and `type Option func(*config)`. This is the AD-1 entry point.
- [x] `cmd/rpcn-debug/main.go` and `version_test.go`: call `Serve` over stdio and update `TestRunNoArgs`, so the stub is gone.
- [x] `vscode/src/extension.ts`, `vscode/package.json`, `tsconfig.json` and `.vscodeignore`: the factory, the setting and the test script. When the setting is unset, the factory throws an error that names the setting. This is the AD-12 / AD-23 MVP path.
- [x] `vscode/src/test/adapter.test.ts`: spawns `RPCN_DEBUG_BIN`, sends initialize through `DebugClient`, asserts a successful response with no capabilities set, disconnects, and asserts exit code 0.
- [x] `Taskfile.yml` and `README.md`: add `test:ext` to `ci`, and document setting `rpcnDebug.binaryPath` to `bin/rpcn-debug`.

**Acceptance Criteria:**
- Given a fresh clone, when `task ci` runs, then the TS DAP test starts `rpcn-debug`, gets a successful `initialize` response, disconnects, sees the process exit 0, and `task ci` exits 0.
- Given the installed `.vsix` with `rpcnDebug.binaryPath` set to the built binary, when the user presses F5 on the `Debug pipeline` configuration, then a session starts and ends with no error dialog.
- Given `debugger/` and `cmd/`, when `task lint:go` runs, then it reports 0 issues, and no go-dap import exists outside `debugger/dap`.

## Plan Change Log

## Implementation Notes

- `tsconfig.json` gained `"skipLibCheck": true`. TypeScript 6.0.3 rejects `declare module DebugProtocol` in `@vscode/debugprotocol` 1.68.0 (TS1540), which the test pulls in through `@vscode/debugadapter-testsupport`.
- `@types/node` is pinned exactly at `22.20.5`, matching the other exact devDependency pins.
- The `test` script runs `tsc -p ./` before `node --test "out/test/**/*.test.js"`, so `npm test` on its own never runs stale output.
- `ProtocolClient.connect` is protected, so the test subclasses `DebugClient` to call it.
- go-dap decodes a request with an unregistered command as `DecodeProtocolMessageFieldError`. The server answers it with an error response and keeps going, like any other unknown request, rather than treating it as a malformed frame. A truncated body (`io.ErrUnexpectedEOF`) is a malformed frame.
- `cmd/rpcn-debug/version_test.go` parses the DAP frames by hand, so no go-dap import exists outside `debugger/dap`.

- Matrix audit (orchestrator): the "binaryPath unset" row had no test. The setting check moved into `vscode/src/binaryPath.ts` (no `vscode` import) and is tested by `src/test/binaryPath.test.ts` under `node --test`; `extension.ts` calls it.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 2, false 1, maybe-false 0, plus one low that was deferred.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | A partial or headerless frame (`"hello world"`, `"Content-Length: 10"`, `"…\r"`, a valid frame followed by `"Content-Len"`) counted as a clean EOF, so `Serve` returned nil and the process exited 0 | medium | patch | Confirmed: go-dap's `readContentLengthHeader` returns a bare `io.EOF`, which contradicts the matrix's Malformed frame row. Fix: `Peek(1)` before each read, so EOF is clean only on a frame boundary, and an EOF in the middle of a frame becomes `io.ErrUnexpectedEOF`. Four malformed cases were added. Verified: `printf 'Content-Length: 10\r' \| bin/rpcn-debug` exits 1 |
| 2 | A client event, or a response with an unregistered name, kills the session (`DecodeProtocolMessageFieldError` with SubType Event/Response) | low | reject | DAP clients don't send events, and they send responses only to reverse requests (`runInTerminal`, `startDebugging`), which this server never issues. That leaves it unreachable in normal use, and the fix adds branches |
| 3 | A known command with JSON of the wrong type (`initialize` with `"linesStartAt1":"yes"`) exits 1 instead of getting an error response | low | reject | VS Code sends well-typed `initialize`/`disconnect` arguments, and go-dap keeps `launch` arguments as raw JSON, so no type error can arise there. It falls within the matrix's "bad JSON" fatal row, and handling it would add an error path |
| 4 | The slog handler in `debugger.Serve` is hard-wired to `os.Stderr`. It ignores `run`'s injected `stderr`, and library callers can't redirect logs | low | defer | Real for developers: `TestRunNoArgs` can't show server diagnostics. The fix adds an exported `Option` (public surface), and the single AD-15 slog handler (stderr plus DAP `Output`) belongs to epic-tracer |
| 5 | The `node --test` glob needs Node 21 or later, and nothing pins the Node version | false | reject | README Prerequisites requires "Node.js 22 or later (required by `@vscode/vsce` 4)", and `package:ext` already fails below 22 |

## Design Notes

The server loop stays deliberately serial: one goroutine reads, handles and writes. Concurrency (events from the engine while a request is in flight) arrives with `core.Session` in epic-tracer, which owns the writer. Nothing here should anticipate it.

## Verification

**Commands:**
- `go test ./debugger/... ./cmd/...`: expected PASS.
- `task ci`: expected exit 0, with the `test:ext` output showing the adapter test passing.
- `grep -rn 'google/go-dap' --include=*.go . | grep -v '^./debugger/dap/'`: expected no output.

**Manual checks:**
- In VS Code 1.141.0 with the new `.vsix` installed and `rpcnDebug.binaryPath` set, press F5 on `Debug pipeline`. The session starts and ends, and no dialog appears.
