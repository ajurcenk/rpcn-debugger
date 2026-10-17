---
title: 'Contracts: closed protocol, error registry, Snapshot and Variables tree'
type: 'feature'
ticket: '2'
created: '2026-10-16'
status: 'built'
baseline_revision: '1399b01ad2464b58c86b9058fb874128fd0a2f80'
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

**Problem:** 2.1's Session talks to DAP through ad-hoc methods (`Launch`, `Next`, `Stopped`…), carries messages as content bytes only, and returns plain Go errors. Every later entry needs the binding shapes instead: AD-3's closed protocol and error registry, AD-11's Snapshot, and the `LineMap`/`Translate` interfaces (AD-6, AD-16). The user can't see metadata or errors in VS Code yet.

**Approach:**
- `debugger/core` gets:
  - `protocol.go`, with the full closed command and event set as types;
  - `errors.go`, with `core.Error` and a code registry with stable DAP numbers;
  - `snapshot.go`, with the full AD-11 Snapshot;
  - the `Translate` interface beside `LineMap`.
- The Session takes commands and emits events through them. The stepper builds Snapshot batches with lossless metadata.
- The DAP server renders Batches › Message › content / metadata / error, with an internal scope. It maps errors to the registry and answers `SetBreakpoints` as unverified.

## Boundaries & Constraints

**Always:**
- The protocol is the closed AD-3 set, exactly:
  - commands: `Launch, ConfigurationDone, SetBreakpoints, SetExceptionBreakpoints, Next, StepIn, StepOut, Continue, Pause, Evaluate, SetMessage, SaveCapture, Restart, Terminate`;
  - events: `ThreadStarted, ThreadExited, StepStarted, StepFinished, Paused, Continued, MessagesSkipped, AttemptStarted, StatementStepped, BreakpointResolved, Delivered, Output, Terminated`.

  Adding a member is a spine change. Commands that later entries implement return a registry error (`NOT_SUPPORTED`), never a panic and never silence.
- Session failures are `core.Error{Code, Message, UserPath?, Line?}`, with `Code` an `UPPER_SNAKE` constant from `errors.go`. Each code has a stable number, which becomes the DAP error id.
- Snapshot follows AD-11 exactly, with `schemaVersion` set:
  - metadata `type` is one of `string | bytes | int | float | bool | timestamp | object | array | null`;
  - `value` is lossless: bytes as base64, ints as decimal strings, timestamps as RFC 3339 with nanoseconds;
  - `text` is for display only;
  - `internal: true` for keys starting with `__`, and core never filters them.
- A Snapshot never references engine objects. The stepper builds it from its deep copy (AD-19). `debugger/core` still imports only the stdlib, and go-dap stays inside `debugger/dap`.
- 2.1's behaviour and tests keep passing: F5 stops, Step Over/Continue deliver, disconnect while stopped.

**Never:**
- No real Step In/Out, Pause, frames in ctx, threads beyond the root (2.6); no LaunchConfig schema (2.3); no `Translate` implementation (2.5); no logging handler (2.4).
- No breakpoint resolution (epic-breakpoints-and-evaluation): every breakpoint is answered unverified.
- No capture files or `SaveCapture` behaviour (epic-edit-restart-replay).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Metadata round-trip | A message with string, `[]byte`, int64, float64, bool, `time.Time` (ns), map, slice and nil metadata | Snapshot types `string, bytes, int, float, bool, timestamp, object, array, null`; values decode back to the originals exactly | (none) |
| Internal keys | `__rpcn_x` metadata plus a normal key | Both in the Snapshot, with `internal: true` on the `__` key; in DAP the `__` key sits under a collapsed "internal" node | (none) |
| Byte metadata display | `kafka_key` as `[]byte("abc")` | DAP shows `abc` (from `text`); the value stays base64 | Non-UTF-8 bytes display as base64 |
| Content views | JSON content, then non-JSON bytes | `text` and `json` set for the first; only `raw` (and `text` when valid UTF-8) for the second | (none) |
| Message error | A processor sets an error | `error` holds the message; DAP shows an `error` variable | (none) |
| Variables tree | Stopped with 2 messages | Scope "Batches" › `batch 0` › `message 0`/`message 1` › `content`, `metadata`, `error` | (none) |
| SetBreakpoints | 3 lines in the pipeline | 3 breakpoints, `verified: false`, each with a message | (none) |
| Unimplemented command | DAP `stepIn` (or `pause`) | Error response, with id = the `NOT_SUPPORTED` number | Session state unchanged |
| Launch error | Missing `pipeline` / bad YAML | Error response, with id = the registry number of `LAUNCH_INVALID` / `PIPELINE_INVALID`, plus `UserPath`/`Line` in the message when known | (none) |

</frozen-after-approval>

## Code Map

- `debugger/core/port.go` holds:
  - `MessageCopy{Content}` / `BatchCopy`, which are replaced by Snapshot batches;
  - `Arriver.Arrive(ctx, stepperID, childIndex, BatchCopy)`, whose batch becomes `[]Batch` (several batches, AD-7);
  - `LineMap.Position`, which is kept;
  - the `Frontend{Stopped, Output, Terminated}` methods, which become one `Emit(Event)`;
  - `Engine{Check, Start}`, which stays.

  The `ErrNothingCaptured`/`ErrSessionEnded` sentinels move into the registry as codes (`NOTHING_CAPTURED`, `SESSION_ENDED`). Callers use `errors.As` with `core.Error`.
- `debugger/core/session.go`: today it has `Frame{StepperID, ChildIndex, Path, File, Line, Messages [][]byte}` and `Launch/ConfigurationDone/Next/Continue/Disconnect` methods.
  - The frame gets a `Snapshot`.
  - The Session gets one entry point, `Do(ctx, Command) (Result, error)`. The DAP side maps each request to a protocol command. `Terminate` maps to the current disconnect path.
- `debugger/engine/stepper.go`: `copyBatch` (line ~84) builds `core.BatchCopy`. It now builds `[]core.Batch` from `service.MessageBatch`:
  - content via `AsBytes`;
  - metadata via `MetaWalkMut`, with any-typed values mapped to the closed type set;
  - errors via `GetError`.

  Put the conversion in a small `snapshot.go` in engine, with its own tests.
- `debugger/dap/server.go`: request switch at lines ~109–215; `ScopesRequest`/`VariablesRequest` (lines ~162–195) currently show one flat "Batch" scope with paging (keep honouring `start`/`count` on indexed children). `sendFailure`/`failure` (lines ~254–265) take plain errors; map `core.Error` to `ErrorResponse.Body.Error{Id: code number, Format: message}`. Add `SetBreakpointsRequest` and `SetExceptionBreakpointsRequest` (empty filters, success). Variables references: a stable encoding of (batch, message, field) valid until resume (AD-17).
- `debugger/serve.go`: wires the Frontend. Its interface change follows here.
- Tests to update: `debugger/core/session_test.go`, `debugger/dap/session_test.go`, `debugger/serve_test.go`, `debugger/engine/engine_test.go` and the TS `vscode/src/test/adapter.test.ts` (Variables now nest).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/protocol.go`:
  - one type per AD-3 command and event, sealed by an unexported method;
  - `Result` for commands that answer (`Launch` → nothing, `SetBreakpoints` → `[]BreakpointResult`);
  - Go doc naming the entry that implements each not-yet-supported command.
- [x] `debugger/core/errors.go`:
  - `Error{Code, Message, UserPath, Line}` implementing `error`;
  - the code constants with a `Number(code)` lookup.

  Codes: `LAUNCH_INVALID`, `PIPELINE_INVALID`, `NOTHING_CAPTURED`, `SESSION_ENDED`, `NOT_SUPPORTED`, `NOT_STOPPED`, `RUN_FAILED`, `CTX_LOST`, `LICENSE_REQUIRED`. Numbers are fixed and never reused, and a test pins them.
- [x] `debugger/core/snapshot.go`: the AD-11 types (`Snapshot`, `Position`, `Batch`, `Message`, `Content`, `Meta`, `Skipped`, `Bloblang`), `SchemaVersion`, the closed `MetaType` set, and JSON tags matching AD-11's field names. A JSON round-trip test.
- [x] `debugger/core/port.go` and `session.go`:
  - `Arrive` takes `[]Batch` and keeps one Snapshot per live frame;
  - `Frontend.Emit(Event)`;
  - `Session.Do(ctx, Command)`, with unimplemented commands returning `NOT_SUPPORTED`;
  - the `Translate` interface (engine path → user path) beside `LineMap`.

  Update the core tests.
- [x] `debugger/engine/snapshot.go` (+ test): `service.MessageBatch` → `[]core.Batch`, covering every matrix metadata type, the content views and errors. `stepper.go` calls it.
- [x] `debugger/dap/server.go` (+ tests):
  - requests map to commands and Session events map to DAP events;
  - the Variables tree and internal node from the matrix;
  - registry-backed error responses;
  - `setBreakpoints` returns unverified results, and `setExceptionBreakpoints` succeeds.
- [x] `debugger/serve.go`, `debugger/serve_test.go` and `vscode/src/test/adapter.test.ts`: follow the new shapes. The F5 path asserts the nested Variables.
- [x] `testdata/tracer/pipeline.yaml`: the generator's mapping also sets `meta source = "tracer"`, a bytes key (`meta kafka_key = "k1".bytes()`) and `meta __rpcn_internal = "x"`. This lets F5 show metadata, bytes-as-text and the internal node.

**Acceptance Criteria:**
- Given the tracer sample with metadata added (a string key, a bytes key and a `__` key), when the user presses F5 in VS Code, then Variables shows Batches › batch 0 › message 0 with content, metadata (the bytes key reads as text) and a collapsed internal node holding the `__` key.
- Given `task ci`, when it runs, then all Go and TS tests pass, `task lint:go` reports 0 issues, and `debugger/core` imports only the stdlib.
- Given a breakpoint set in the YAML, when the session starts, then VS Code shows it as unverified (hollow) and no error dialog appears.

## Plan Change Log

## Implementation Notes

- Registry numbers are 1001–1009 in the order listed in the plan (`LAUNCH_INVALID` = 1001 … `LICENSE_REQUIRED` = 1009), pinned by `TestRegistryNumbersArePinned`. Go identifiers are `core.Code<Name>`; their string values are the UPPER_SNAKE codes. `core.Error` is a value type with an `Is` method, so `errors.As(err, &core.Error{})` and `core.HasCode(err, code)` both work.
- `Error()` renders `Message (at <UserPath>, line N)` when either is known. Rewrite errors still carry their line in the message text ("yaml: line 4: …"); they become `PIPELINE_INVALID` via `core.AsError` in the Session. A DAP request with no protocol command (e.g. `setVariable`) also answers `NOT_SUPPORTED`; any other non-`core.Error` maps to `RUN_FAILED`.
- `LineMap.Position` now returns the AD-11 `core.Position` (only `UserPath`, `Line` filled); the old `{Path, Line}` type is gone.
- Metadata `value` is always a string (floats in Go's shortest round-trip form, so NaN/Inf survive); object/array values are JSON text. Metadata is sorted by key for a deterministic Snapshot.
- The stepper now arrives once per child with all current batches, instead of once per batch.
- Variables references: `8 + (((stop << 8 | batch) << 12 | message) << 3) + kind`, with stop = frame id mod 128, below 2^31 for batch < 256 and message < 4096 (beyond that the node is a leaf). The "Batches" scope is encoded the same way. A reference whose stop doesn't match the current frame resolves to nothing (AD-17); only one from exactly 128 stops earlier could collide. Frame ids count up per stop.
- `core.AsError` on an error that wraps a `core.Error` keeps the wrapped Code/UserPath/Line and uses the full wrapped text as Message.
- Events with no DAP rendering yet (StepStarted, StepFinished, MessagesSkipped, AttemptStarted, StatementStepped, BreakpointResolved, Delivered) are logged at debug level and dropped by the DAP frontend. The Session emits only Paused, Output and Terminated in this entry.
- The tracer sample's mapping moved from line 13 to 16 (three `meta` lines added); goldens and TS/Go tests follow.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 4, false 0, maybe-false 0. Three were patched, two deferred, one rejected.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `variablesReference` encodes only (kind, batch, message), so an old reference resolves against a new frame (AD-17) | medium | patch | The reference now carries the stop (frame id mod 128), and `variables` returns nothing on a mismatch (`TestStaleReferences`). Limits are now 256 batches and 4096 messages |
| 2 | `AsError` drops the outer wrap text of a wrapped `core.Error` | low | patch | The inner Code/UserPath/Line are kept and the Message is the full `err.Error()` (`TestAsErrorKeepsWrapText`) |
| 3 | `core.Error.Line`/`UserPath` are never set; the line is only in the message text | low | defer | The matrix asks for the line "in the message when known", which holds. The structured fields need the rewrite to return `core.Error`, so this goes to 2.5 (deferred-work.md) |
| 4 | Object/array metadata isn't lossless (nested bytes, timestamps, NaN), unknown Go types become strings, and int64 overflow is labelled float | medium | defer | Real against AD-11. Scalars are lossless. A typed nested encoding is a format decision, deferred to epic-edit-restart-replay, whose capture round-trip test must be byte-identical (deferred-work.md) |
| 5 | Pointer commands (`&core.Next{}`) fall to `NOT_SUPPORTED` | low | reject | Every caller passes values. Handling both forms adds code for a state no caller reaches |
| 6 | CLAUDE.md's layout doesn't describe the new contracts | low | patch | Orchestrator updated the `core` entry (protocol, `Session.Do`/`Frontend.Emit`, registry, Snapshot, `Translate`). CLAUDE.md itself asks to be kept current as code lands |

## Design Notes

`Session.Do(ctx, Command)` is the single seam AD-3 asks for. The DAP server becomes a translator (request → command, event → DAP event), and later frontends reuse the same seam. Keeping each command a distinct type (not a string plus args) lets the compiler find every place a later entry has to touch when it implements one.

## Verification

**Commands:**
- `go test -race ./debugger/...`: expected PASS.
- `go list -deps ./debugger/core | grep -v '^[a-z]*\(/[a-z]*\)*$'`: expected no non-stdlib packages.
- `task ci`: expected exit 0.

**Manual checks:**
- In VS Code with the new `.vsix`, F5 on the tracer sample (with metadata added to the generator) shows the nested Variables tree and the internal node, and a YAML breakpoint shows as unverified.
