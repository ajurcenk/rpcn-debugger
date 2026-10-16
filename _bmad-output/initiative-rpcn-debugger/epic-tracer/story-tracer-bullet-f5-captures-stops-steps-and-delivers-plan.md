---
title: 'Tracer bullet: F5 captures, stops, steps and delivers'
type: 'feature'
ticket: '1'
created: '2026-10-16'
status: done
baseline_revision: '1926a78bb88c550e894a8c2cf0af11301090f964'
route: 'full'
route_source: 'auto'
risk: 'high'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** F5 starts a session that ends at once: nothing is captured or stepped. Every layer of the debugger (extension → DAP → Session → rewrite → engine → sink) exists only as an empty package, so no later entry can build on a proven path.

**Approach:** Build the thinnest path through every layer for a `generate` + one `mapping` pipeline:
- capture one batch without acking it;
- step it through `pipeline.processors` with a minimal `rpcn_debug_stepper` that stops before each child through `core.Session.Arrive` under the pause gate;
- show the stop in VS Code (line and message content);
- deliver to a minimal `rpcn_debug_sink` on Step Over or Continue, then terminate.

## Boundaries & Constraints

**Always:**
- Hexagonal layering (spine Design Paradigm):
  - `debugger/core` imports only the stdlib. `dap` imports only `core`.
  - The `debugger` package wires `rewrite`, `engine` and `dap` together behind `Serve`.
  - Session reaches the engine only through a port interface defined in `core`.
- AD-4:
  - The capture config is the user's input block only. It runs with `AddBatchConsumerFunc`; the consumer returns an error (never acks), takes the first batch, and the capture stream is stopped before stepping starts.
  - The step config feeds that batch through `AddBatchProducerFunc`, the stepper and `rpcn_debug_sink`.
  - Processors are built only through the stepper's `NewProcessorListField`.
- Only `debugger/rewrite` parses YAML, with yaml.v3, and it never writes the user's file (AD-6). Each child's line comes from its yaml.v3 node.
- `Arrive` holds the process-wide gate while it decides and while it is paused. The gate is never held while a processor runs (AD-9). The stepper passes a copy of the batch (AD-19).
- AD-15:
  - Only the DAP encoder writes to `out`.
  - Every StreamBuilder gets an explicit stderr slog logger and a private HTTP mux. The default logger writes to stdout, `SetPrintLogger(nil)` panics, and the default `http` binds :4195.
- Each session works on `env.Clone()`, where it registers `rpcn_debug_stepper` and `rpcn_debug_sink` (AD-1).
- The extension adds only a missing `pipeline` (the active editor's file) to the launch config (AD-12).

**Never:**
- None of the full closed protocol, error registry or Snapshot model (2.2); no `LaunchConfig` schema or `LAUNCH_INVALID` (2.3); the launch reads `pipeline`/`cwd` leniently.
- No wrapping beyond `pipeline.processors` (2.5); no Step In/Out, Pause, frames in ctx or the end-of-entry stop (2.6); no metadata in Variables (2.2); no dry-run delivery display (2.8).
- No `internal/` imports. No goroutine IDs.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| F5 happy path | Sample pipeline; initialize → launch → configurationDone | `initialized` event; a `stopped` event (reason `entry`) before the mapping; stackTrace has one frame at the mapping's line; variables show the message content | (none) |
| Step Over | `next` at the stop | The mapping runs, the batch reaches the sink, then `terminated`; the process exits 0 after disconnect | (none) |
| Continue | `continue` at the stop | Same as Step Over | (none) |
| No pipeline | launch without `pipeline` | No capture | The launch response fails with a message naming `pipeline` |
| Unreadable or invalid YAML | Bad path or bad YAML | No capture | The launch response fails with the file path and the parser's line |
| Input yields nothing | Input closes before a batch | No stop | An Output event says no batch was captured, then `terminated` |
| Disconnect while stopped | `disconnect` at the stop | Gate released, streams stopped, nothing delivered | `Serve` returns nil |

</frozen-after-approval>

## Code Map

- `debugger/serve.go`: `Serve` currently calls `dap.Serve(in, out, log)` and discards `env`. It becomes the wiring point: build the Session with an engine adapter (rewrite + engine) and pass it to the DAP server. The `slog.NewTextHandler(os.Stderr, nil)` stays; replacing it is 2.4.
- `debugger/dap/server.go`: a serial read/handle/write loop with `send` stamping `seq`. It currently handles initialize, launch→terminated and disconnect, and any other request gets an error response. Changes:
  - writes from Session goroutines (stopped, output, terminated) need a mutex around `send`;
  - `launch` stops terminating at once; it returns success and sends `initialized`;
  - add configurationDone, threads, stackTrace, scopes, variables, next and continue;
  - initialize advertises `supportsConfigurationDoneRequest: true`.
- go-dap v0.12.1 types: `ConfigurationDoneRequest`, `ThreadsRequest`, `StackTraceRequest`, `ScopesRequest`, `VariablesRequest`, `NextRequest`, `ContinueRequest`, `InitializedEvent`, `StoppedEvent`, `OutputEvent`, `TerminatedEvent`. Launch arguments are `json.RawMessage`; decode `pipeline` and `cwd` from them.
- benthos v4.81.0 `public/service`:
  - `StreamBuilder.AddBatchConsumerFunc` (stream_builder.go:414): its handler's returned error nacks the transaction (stream_builder.go:811-835).
  - `AddBatchProducerFunc` (:277) returns a func that blocks until the batch is delivered or ctx ends.
  - `SetYAML` (:545) can't be combined with consumer/producer funcs. So both streams are built from parts: the capture from `AddInputYAML` plus the consumer func, and the step stream from `AddBatchProducerFunc`, `AddProcessorYAML` (the stepper), `AddOutputYAML` (`rpcn_debug_sink: {}`) and `AddResourcesYAML` (`*_resources`).
  - `Stream.StopWithin`, `Stream.Run`.
  - `NewProcessorListField` / `FieldProcessorList` (config_processor.go:47,78).
  - `OwnedProcessor.ProcessBatch` to run one child, and `MessageBatch.Copy`.
  - `RegisterBatchProcessor`, `RegisterBatchOutput` (MaxInFlight 1).
- `test/equivalence/equivalence.go`: `standardEnv()`, the `rpcn_reference_sink` pattern (RegisterBatchOutput on a Clone, a collector with a mutex, `batch.Copy()`), `SetHTTPMux` and `StopWithin` on ctx end. Reuse the shape, not the code.
- `vscode/src/extension.ts`: has the `AdapterFactory` only. Add a `DebugConfigurationProvider.resolveDebugConfiguration` that sets `pipeline` from `vscode.window.activeTextEditor` when it's missing.
- `vscode/src/test/adapter.test.ts`: `StdioDebugClient` attaches to a spawned binary. Extend it with the F5 path test.
- `cmd/rpcn-debug/main.go` and `version_test.go`: unchanged, except that `TestRunNoArgs` must still pass (initialize + disconnect).
- New sample: `testdata/tracer/pipeline.yaml` (generate `count: 1`, `interval: ""`, one `mapping`, `output: drop: {}`). It's kept separate from the equivalence corpus.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/session.go`, `gate.go`, `port.go` (new):
  - `Session` with `Launch(pipeline, cwd)`, `ConfigurationDone()`, `Next()`, `Continue()`, `Disconnect()`;
  - `Arrive(ctx, stepperID, childIndex int, batch BatchCopy) error`, which takes the process-wide gate, decides to stop (in this entry: every Arrive stops after configurationDone and step mode), emits `Stopped`, and blocks until resumed;
  - a `Frontend` interface for events (Stopped, Output, Terminated);
  - an `Engine` port (`Start(ctx, pipeline string, cwd string, s Arriver) error`, ending when delivered or stopped);
  - a minimal frame record (stepperID, childIndex, line, messages' content) for stackTrace and variables.

  Unit tests use a fake engine.
- [x] `debugger/rewrite/rewrite.go` (new): `Rewrite(src []byte) (Plan, error)` returns the capture input YAML, the resources YAML, and the stepper YAML that wraps `pipeline.processors` as `rpcn_debug_stepper: {id: 0, lines: [...], processors: [...]}`. Lines are 1-based, one per child, from yaml.v3. Golden tests cover the sample, a missing `pipeline.processors`, and bad YAML with its line.
- [x] `debugger/engine/stepper.go`, `sink.go`, `capture.go`, `engine.go` (new):
  - `rpcn_debug_stepper` (fields `id`, `lines`, `processors`) runs children one by one with `ProcessBatch` and calls `Arrive` before each with a copied batch;
  - `rpcn_debug_sink` collects with MaxInFlight 1;
  - capture runs the input, nacks, keeps the first batch and stops the stream;
  - the engine adapter implements `core.Engine` on `env.Clone()`, with stderr logger and private mux on both builders.

  Tests run the sample against an in-memory Arriver.
- [x] `debugger/dap/server.go`: add the requests and events listed in the Code Map, a send mutex, one thread (id 1), one frame, a "Batch" scope with one variable per message (its content as text), and launch errors as error responses. Tests: an in-memory DAP session over pipes covering the I/O matrix.
- [x] `debugger/serve.go`: wire Session, engine adapter and DAP server. Keep the `Serve` signature and `env` untouched (clone per session).
- [x] `vscode/src/extension.ts`: a `DebugConfigurationProvider` that fills a missing `pipeline` from the active editor. `vscode/src/test/adapter.test.ts`: the F5 path (launch with `testdata/tracer/pipeline.yaml`, configurationDone, stopped at the mapping line, variables content, next → terminated, exit 0).
- [x] `testdata/tracer/pipeline.yaml` and `README.md`: the sample, and a one-line "try it" section.

**Acceptance Criteria:**
- Given the installed `.vsix` with `rpcnDebug.binaryPath` set, when the user opens `testdata/tracer/pipeline.yaml` and presses F5 (with an empty `launch.json` entry or none), then VS Code stops before the mapping with that line highlighted and the message content in Variables. Step Over then ends the session with no error dialog.
- Given `task ci`, when it runs, then the Go tests and the TS F5-path test pass, `task lint:go` reports 0 issues, and nothing is written to stdout except DAP frames.
- Given the sample pipeline's `generate` input, when the session ends, then the batch was nacked: the consumer returned an error and no ack reached the input.

## Plan Change Log

## Implementation Notes

- **Engine port has two methods.** `core.Engine` is `Check(pipeline, cwd) (LineMap, error)` plus `Start(ctx, pipeline, cwd, Arriver) error`. `Check` runs at `Launch`, so YAML and config errors fail the launch response and the Session gets the LineMap for frame lines; `Start` runs at `configurationDone` and re-reads the file. `Check` also builds (never runs) both streams, so an unknown component fails the launch too.
- **The engine never parses YAML.** `engine.New(env, log, load)` takes a `Loader`; `debugger/serve.go` supplies one that reads the file and calls `rewrite.Rewrite` (AD-4: U-STEP never parses configs; AD-6).
- **Capture input kept verbatim.** The capture config is the user's input block as written, `input.processors` included. AD-4(a) removes them, but until 2.5 wraps `input.processors` in a stepper, removing them would drop those processors from the run.
- **F5 with no launch.json.** VS Code then resolves an empty config, so `fillLaunchConfig` also sets `type`/`request`/`name` when all three are missing; a launch.json entry only ever gets a missing `pipeline` (AD-12).
- **Ending while stopped.** `Arrive` returns `ErrSessionEnded`; the stepper then returns no batches, so nothing reaches the sink. The step stream's producer call uses its own context: abandoning the transaction on ctx end stalled `StopWithin` for ~4 s (caught by `TestServeTracerSample/disconnect`).
- **Gate scope.** The gate is owned by the Session and shared by every stepper of its debuggee (one session per `Serve`), not a package global.
- `cwd` is used only to resolve a relative `pipeline`.

- 2026-10-16 acceptance (user): in VS Code 1.141.0 with the installed .vsix, F5 on `testdata/tracer/pipeline.yaml` stopped at the mapping, Variables › Batch showed the message, and Step Over ended the session. AC 1 is met.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 4, false 1, maybe-false 0. Four were patched, one deferred, and one found false (its change was kept anyway). The orchestrator also found and fixed a flaky shutdown under load.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | An explicit `input: stdin` passes the rewrite, so the capture reads the DAP stream (AD-15, AD-5) | medium | patch | `Rewrite` now rejects a `stdin` input, naming its line (`stdin.golden`). The full stdio/output rules stay in 2.5 |
| 2 | `Check` builds both streams and never stops them, opening `*_resources` at launch | false | patch kept | The implementer verified that benthos v4.81.0's `manager.New` registers resources with `LazyAdd` and doesn't open them in `Build()`, which matches the spike 1.6 finding. `Check` now stops after the `Add*` lint/parse anyway (`configureCapture`/`configureStep`), at no cost |
| 3 | YAML aliases to anchors outside the re-marshalled subtree become unknown anchors | low | defer | Reproduced by the reviewer. Resolving aliases means a node-copy pass. Deferred to 2.5, which owns the rewrite (deferred-work.md) |
| 4 | A null `pipeline:`/`processors:` is rejected though Connect accepts it | low | patch | Null is now treated like a missing key (`null-pipeline`, `null-processors` goldens) |
| 5 | Variables ignore `start`/`count`, so batches over 100 repeat per page | low | patch | Paging is honoured and `filter: named` returns nothing (`TestVariablesPaging`) |
| 6 | A relative `cwd` gives a relative `Source.Path`, so VS Code can't highlight the line | low | patch | `filepath.Abs` after the join (`TestRelativeCwdGivesAbsolutePipeline`) |

Orchestrator verification:
- `task ci` failed once on `TestServeTracerSample` (5.01s). It was reproduced with 7 of 200 runs failing under 8 busy-loop CPU hogs: `stop stream: context deadline exceeded` came before the first stop.
- Root cause: the capture input keeps re-sending the batch it was nacked for, so the graceful stop of the capture stream waits for that transaction until `StopWithin` times out.
- Fix: `stopCapture` gives the graceful stop 1s and treats a timeout as harmless. Benthos then stops forcefully, and since nothing is ever acked, nothing is committed (AD-4, AD-5).
- Verified 200 of 200 under the same load. `gofmt` was applied to two test files.

## Design Notes

The stop predicate is deliberately trivial in this entry: after `configurationDone`, the first Arrive stops with reason `entry`, and `next` or `continue` both run to the end. Real step state (Step Over versus Continue, nested entries, the end-of-entry stop) is 2.6. The `Engine` port is the seam 2.5 (rewrite) and 2.7 (stepper modes) grow behind, so its shape should carry the pipeline path and the Arriver and nothing engine-specific.

## Verification

**Commands:**
- `go test ./debugger/...`: expected PASS.
- `task ci`: expected exit 0, with the TS F5-path test passing.
- `bin/rpcn-debug < /dev/null`: expected exit 0, with nothing on stdout.

**Manual checks:**
- In VS Code 1.141 with the new `.vsix` installed: F5 on `testdata/tracer/pipeline.yaml` stops at the mapping line, and Variables shows the content. Step Over ends the session with no dialog.
