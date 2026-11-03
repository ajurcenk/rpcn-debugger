---
title: 'Capture file and the saveCapture launch key'
type: 'feature'
ticket: '3'
created: '2026-11-03'
status: 'built'
baseline_revision: '497b46791bef0e20df89fdf7926d92d538de39d6'
route: 'full'
route_source: 'auto'
risk: 'high'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** A captured batch lives only in the session's memory. It can't be kept for a later session, or debugged again once the broker or the data is gone. Entry 4 (`replay`) needs a file to feed (AD-11, AD-22, CAP-10).

**Approach:** Add the AD-11 capture file. It is a versioned Snapshot of the truncated batch plus `skipped`, with lossless metadata, and the engine can write it and read it back. Add the launch key `saveCapture: <file name>`: the Session saves the kept batch to `<cwd>/.rpcn-debug/<name>` right after the capture.

## Boundaries & Constraints

**Always:**
- **File format:** a `core.Snapshot` as JSON with `schemaVersion` "1.0" and one batch holding the kept messages. `skipped` lists the dropped indices `kept..total-1` with reason `maxMessages`. `position` is zero apart from `file`, the pipeline path.
- **Lossless round trip:** content bytes, every metadata key, type and value, and a message's error read back so that their Snapshot is byte-identical. That covers string, bytes, int, float, bool, timestamp, object, array and null. Ints read back as int64, floats as float64, and objects and arrays as Bloblang values (integral numbers int64, others float64).
- **Version check:** the reader rejects a newer major `schemaVersion` (e.g. "2.0") and a missing or unparseable one, with a clear error. An older or equal major is read.
- **File location:** `saveCapture` is a local relative path, checked with `filepath.IsLocal`. It is written to `<cwd>/.rpcn-debug/<name>` with mode `0600`, through a temp file and a rename, so an existing file is replaced, also as `0600`. `.rpcn-debug/` is created `0700`. A missing `.rpcn-debug/.gitignore` is created holding `*`; an existing one is never changed.
- **Save timing:** the Session saves once per launch, right after a successful capture and before stepping. Then it emits the console note "Saved the capture to <absolute path>.". Restart never saves again.
- **Save failure:** a failed save is a stderr note naming the path and the error. The run still steps the batch.
- **Protocol:** `Do(SaveCapture{Path})` saves the kept batch to that path with the same rules. DAP maps no request to it. The launch schema is regenerated.

**Never:**
- No `replay` launch key and no reading from a launch (entry 4).
- No messages skipped by `try` in the file (those are `MessagesSkipped`).
- No extension code change beyond the generated schema.
- No new error code.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Save | `saveCapture: "orders.json"`, 12 messages, maxMessages 10 | `<cwd>/.rpcn-debug/orders.json` mode 0600 with 10 messages and skipped 10, 11; `.gitignore` `*`; the note | none |
| Escaping name | `"../x.json"`, `"/tmp/x.json"`, `""` when set to empty | launch fails | LAUNCH_INVALID naming saveCapture |
| Existing gitignore | `.rpcn-debug/.gitignore` holds `foo` | left as `foo` | none |
| Overwrite | file exists with mode 0644 | replaced, mode 0600 | none |
| Unwritable | `.rpcn-debug` is a file | stderr note; the run still steps and delivers | note |
| Round trip | every metadata type, binary content, an error | the read-back Snapshot is byte-identical | none |
| Newer major | `schemaVersion` "2.0" | read refused | error naming both versions |
| Restart | after a save | no second save, no note | none |

</frozen-after-approval>

## Code Map

- **`debugger/core/protocol.go`:** `Launch.SaveCapture string`. `SaveCapture` loses its "not supported" comment.
- **`debugger/core/port.go`:** `Engine.SaveCapture(b CapturedBatch, pipeline, path string) error` writes `b` as a capture file at `path`. The engine owns the encoding; the Session owns the path rules and the folder.
- **`debugger/core/snapshot.go`:** `CheckSchemaVersion(v string) error`, the major-version gate, for the engine's reader.
- **`debugger/core/session.go`:**
  - `launch` validates the name, `""` meaning unset.
  - `run` saves after `capture` succeeds, when `kept == nil` at the start.
  - `Do` routes `SaveCapture`. A new `capture.go` (or `delivery.go`) holds `saveCapture(path)`: the folder, the `.gitignore`, the call to `engine.SaveCapture`, and the notes.
- **`debugger/engine/capturefile.go` (new):**
  - `writeCapture(path, pipeline string, batch service.MessageBatch, total int) error` encodes with `snapshotBatches`, then writes a temp file in the same directory (0600) and renames it.
  - `ReadCapture(path string) (service.MessageBatch, int, error)` (the int is the total) decodes, checks the version, and rebuilds the messages: `service.NewMessage(raw)`, then `MetaSetMut` with the typed value per `core.Meta.Type`, then `SetError(errors.New(e))`.
  - `Adapter.SaveCapture` type-asserts the batch, as `Step` does.
  - Reuse `metaEntry`, `contentViews` and `snapshotBatches` from `engine/snapshot.go`. Don't change them.
- **`debugger/dap/launch.go`:** a `SaveCapture string` field with `json:"saveCapture"` and a `desc` (a file name under `<cwd>/.rpcn-debug/`, gitignored, mode 0600; the folder is created). `server.go` passes it into `core.Launch`.
- **`vscode/package.json`:** regenerated by `task gen:schema`.
- **Fake engines** (core, dap, `debugger/logging_test.go`, `rewrite/translate_test.go`): add a `SaveCapture` stub.
- **Leave alone:** `rewrite/`, `vscode/src`, `CLAUDE.md`, `testdata/tracer/restart.yaml` (it has the user's uncommitted edit; never stage it).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/protocol.go`, `port.go`, `snapshot.go` -- `Launch.SaveCapture`, `Engine.SaveCapture`, `CheckSchemaVersion` -- the port.
- [x] `debugger/engine/capturefile.go` (+ `capturefile_test.go`) -- the writer, `ReadCapture`, `Adapter.SaveCapture`; the round-trip test of every metadata type, binary content and an error; the newer-major and missing-version refusals -- the least certain part.
- [x] `debugger/core/session.go` (+ a capture-file helper file) -- name validation, the save after capture, `Do(SaveCapture)`, the notes -- the Session side.
- [x] `debugger/dap/launch.go`, `server.go`; `task gen:schema` -- the launch key and schema.
- [x] Tests -- core tests for each matrix row on the Session side (with a fake engine); a dapdrive test for the Save row (0600, the gitignore, the note, skipped via `ReadCapture`); fake-engine stubs.

**Acceptance Criteria:**
- Given a dapdrive launch with `saveCapture` and `maxMessages` below the batch size, when it runs, then the file is 0600, `.gitignore` holds `*`, the Debug Console shows the note, and `ReadCapture` returns the kept messages with the right total.
- Given `task ci`, when it runs, then it is green and `lint:schema` shows no drift.

## Implementation Notes

- **Port and version check:**
  - `Engine.SaveCapture(b, pipeline, path)` writes the file; `ReadCapture(path)` returns the messages and the total.
  - `core.CheckSchemaVersion` refuses a newer major and a missing or unparseable version.
- **Session (`core/capture.go`):**
  - **Names:** checked with `filepath.IsLocal`. A name that cleans to `.` or `.gitignore` is also LAUNCH_INVALID: it would point at the folder itself, or overwrite the `.gitignore` the plan says is never changed.
  - **Folders:** a subfolder in the name is created `0700`.
  - **`Do(SaveCapture)`:** a bad name is LAUNCH_INVALID, before the capture it is NOT_SUPPORTED, and a failed save is RUN_FAILED.
  - **Save timing:** the save runs right after the launch's capture, even when a Restart arrives in between; the review found that a Restart in that window cancelled it.
- **Engine (`engine/capturefile.go`):**
  - The file is written through a `0600` temp file and a rename.
  - Nested numbers in object and array metadata read back as int64, then uint64, then a float64 only when it re-marshals to the same text; otherwise the `json.Number` is kept. That way the Snapshot stays byte-identical.
- **DAP:** `LaunchConfig.SaveCapture` (with `min:"1"`, so an empty string fails the launch); `vscode/package.json` regenerated.
- **Tests:**
  - `engine/capturefile_test.go`: the byte-identical round trip of every type, plus NaN, ±Inf, float32, max uint64 at the top level and nested, `json.Number` values, binary and empty content, and an error; overwrite to `0600`; the version refusals.
  - `core/capture_test.go`: each Session-side matrix row.
  - dapdrive `TestSaveCapture`: 12 messages with maxMessages 10 → 10 of 12, mode `0600`, the `.gitignore` and the note.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 2, false 0, maybe-false 0.
- **medium, patch (fixed):** the nested numbers weren't lossless. A uint64 above MaxInt64, and `json.Number` values like `1.0` and `1e2`, came back as float64 and re-encoded differently. They now read back as described in the Implementation Notes, and the round-trip test covers them.
- **low, patch (fixed):** a Restart between `keepCaptured` and `autoSave` made the launch skip its save without a word. `autoSave` now skips only when the session has ended. No deterministic test: there is no seam between the two calls.
- **low, rejected:** an object or array value that JSON can't encode (a chan or func) is written by `jsonText`'s `fmt.Sprint` fallback, still typed object or array, so `ReadCapture` fails on that file. Bloblang and Connect components don't produce such metadata, and the fix would change the shared `metaEntry` encoder.

## Design Notes

Typed values on read are chosen so that `metaEntry` re-encodes them to the same `core.Meta`:

| Type | Read back as |
|---|---|
| int | `strconv.ParseInt` → int64, or `ParseUint` → uint64 when the value is too large for an int64 |
| float | `ParseFloat(…, 64)` |
| timestamp | `time.Parse(time.RFC3339Nano)` |
| bytes | base64-decoded |
| object or array | `json.Decoder` with `UseNumber`, then numbers converted: int64 when `Int64()` works, else float64 |
| null | `nil` |

The round-trip test compares `json.Marshal` of the original `snapshotBatches` with that of the read-back batch.

## Verification

**Commands:**
- `go test -race ./debugger/... ./test/dapdrive/...` -- expected: pass.
- `task ci` -- expected: exit 0.

**Manual checks:**
- In VS Code, add `"saveCapture": "restart.json"` to the launch and press F5 on `testdata/tracer/restart.yaml`. Check that `.rpcn-debug/restart.json` (mode 0600) and `.rpcn-debug/.gitignore` appear, and that the note shows in the Debug Console.
