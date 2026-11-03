---
title: 'SetMessage edits content and metadata at a stop'
type: 'feature'
ticket: '6'
created: '2026-11-03'
status: 'built'
baseline_revision: '8c868e57a6f6d925a3e328bb9cd2cd913ff376c4'
route: 'full'
route_source: 'auto'
risk: 'medium'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context: []
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** At a stop the developer can see a message but not change it. To try "what if this field were X" they have to edit the input data and relaunch (CAP-8). SetMessage is NOT_SUPPORTED today.

**Approach:** DAP `setVariable` on a message's `content`, or on one of its metadata values, in the top frame of the stopped thread becomes `core.SetMessage`. The Session validates it, records it on the stop's Snapshot, and returns the stop's edits from `Arrive`. The stepper applies them to its own batch before the next child runs (AD-19), so only downstream processors see them.

## Boundaries & Constraints

**Always:**
- **Where an edit applies:** only the top (innermost) frame of the current stop of the stopped thread, at any stop position, including an end-of-entry stop, where the edit goes to the batches the entry hands back.
- **Content:** the whole content is replaced. The new text is taken as UTF-8 bytes. When the content was shown as base64 (not UTF-8), the new value is base64-decoded, and text that doesn't decode is refused.
- **Metadata:** only an existing key's value changes, and its type is kept. Keys are never added or deleted. The value is parsed by type:

  | Type | Accepted value |
  |---|---|
  | string | as is |
  | bytes | text when the key was shown as text, otherwise base64 |
  | int | decimal |
  | float | Go float syntax |
  | bool | `true` or `false` |
  | timestamp | RFC 3339 |
  | object | a JSON object |
  | array | a JSON array |
  | null | only `null` |

  Text that doesn't parse is refused, and nothing changes.
- **Effect at the stop:** after a successful edit, the current stop's top-frame Snapshot shows it: the Variables view and the message summary refresh. Snapshots of earlier stops and of other frames never change. Several edits at one stop all apply, in order.
- **Effect downstream:** the edit reaches only processors that run after the stop. The delivered batch reflects it. Restart re-steps the original captured batch.
- **Refusals:** an edit on any other frame, on a stale stop, while not stopped, on an unknown batch, message or key, or on any other variable (error, batch, message summary) is refused with the new core.Error code `EDIT_INVALID` (1014) and a message saying why.
- **DAP:** advertises `supportsSetVariable`. The response carries the new display value and type.

**Never:**
- No field-level JSON edits (D3), no adding or deleting keys, no change to a message's error, and no edits at statement stops (epic 6).
- No new protocol command or event: `SetMessage` already exists. Only its fields change.
- No change to the equivalence corpus results.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| JSON content | stop before the 2nd processor; content set to `{"n":5}` | the later processor sees `n: 5`; delivered shows it; the 1st stop's Snapshot is unchanged | none |
| Int metadata | `count` (int) set to `42` | downstream `meta("count")` is int 42; the stop's view shows 42 | none |
| Bad int | `count` set to `4x` | refused; the value stays | EDIT_INVALID |
| Non-top frame | edit in a parent frame of a nested stop | refused | EDIT_INVALID |
| Unknown key / other variable | an edit of `error` or of a `batch 0` node | refused | EDIT_INVALID |
| Binary content | non-UTF-8 content; valid base64 / invalid | replaced / refused | EDIT_INVALID |
| End-of-entry stop | edit at the end stop of a `branch` | the parent's next child sees it | none |
| Restart | after an edit | the original batch, unedited | none |

</frozen-after-approval>

## Code Map

- **`debugger/core/protocol.go`:** `SetMessage{FrameID, Batch, Index, Content []byte, Metadata []Meta}`. Content is set when non-nil. Metadata now means the keys to set, each with its type and lossless `Value`. Fix the comments.
- **`debugger/core/errors.go`:** `CodeEditInvalid = "EDIT_INVALID"`, number 1014.
- **`debugger/core/port.go`:** `Arriver.Arrive` returns `([]SetMessage, error)`: the edits made at that stop, in order.
- **`debugger/core/stop.go`:**
  - `Arrive` collects the stop's edits and returns them once the stop is resumed.
  - New `setMessage(c)` checks the rules and parses the value into a lossless `core.Meta`, with the parsing in a small `core/edit.go`. It then rebuilds the current stop's top-frame Snapshot (a copy of it, so no earlier stop's Snapshot changes) and appends the edit.
  - `Do` routes `SetMessage`, replacing `notSupported`.
- **`debugger/engine/stepper.go`, the Arrive calls at about lines 243 and 271:** apply the returned edits.
  - Before a child, `Batch` b is `items[b].batch`, which matches `snap = batchesOf(items)`.
  - At the end stop, it is `out[b]`.
  - Content uses `m.SetBytes`. Metadata uses `m.MetaSetMut(key, typed)`, with the typed value from `capturefile.go`'s `metaValue` (reuse it; don't change it).
  - The edited message is replaced in its batch with `m.Copy()` before it is changed, so a message shared with another batch isn't edited twice.
- **`nopArriver`, the stepper-test arrivers, and every fake Arriver or Session caller:** move to the new signature.
- **`debugger/dap/server.go` and `variables.go`:**
  - `SupportsSetVariable` is advertised.
  - A `SetVariableRequest` is decoded into a frame, batch and message with `decodeRef`. `kindMessage` with the name `content` is a content edit. `kindMetadata` or `kindInternal` with a key name is a metadata edit, carrying the key's current type from the stop's Snapshot.
  - The DAP frame index maps to the core frame through the current `Stop`. Anything else gets EDIT_INVALID from the Session or the server.
  - The response comes through `respondLocked`.
- **`test/dapdrive/dapdrive.go`:** `Options.Edits func(stops []Stop) []Edit`, run at each stop before the Script.
  - `Edit{Frame int; Batch, Index int; Content *string; Meta map[string]string}`; `Frame` 0 is the top frame.
  - It sends `setVariable` through the stop's references, and a failure goes to `Result.EditErrors`.
- **Leave alone:** `rewrite/`, `vscode/src`, `CLAUDE.md`, the equivalence corpus, and `testdata/tracer/restart.yaml` (it has the user's uncommitted edit; never stage it).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/core/protocol.go`, `errors.go`, `port.go`, `stop.go`, `edit.go` -- SetMessage validation, the Snapshot update, and Arrive returning the edits -- the Session side.
- [x] `debugger/engine/stepper.go` (+ the arrivers) -- apply the edits before the gate releases -- AD-19.
- [x] `debugger/dap/server.go`, `variables.go` -- `supportsSetVariable` and the setVariable mapping -- DAP.
- [x] `test/dapdrive/dapdrive.go` -- `Edits` and `EditErrors` -- the driver.
- [x] Tests -- core parsing and refusal for every type; a stepper test for the before-child and end-stop edits; a dap setVariable test; dapdrive tests for every matrix row -- coverage.
- [x] `testdata/tracer/edit.yaml` -- an F5 sample: a `generate` input with an int metadata key and JSON content, then two mappings, the second using both -- the manual check.

**Acceptance Criteria:**
- Given a dapdrive run stopped before the second processor, when the JSON content and an int metadata value are edited and stepping continues, then the delivered batch shows both edits passed through the second processor, and the first stop's batches are unchanged.
- Given `task ci`, when it runs, then it is green and the equivalence corpus is unchanged.

## Implementation Notes

- **Core:**
  - `EDIT_INVALID` 1014.
  - `SetMessage` carries `StopID` and the values as typed. The Session parses them in `core/edit.go`, and the edits it hands back carry the parsed bytes and lossless metadata.
  - `Arriver.Arrive` returns `([]SetMessage, error)`.
  - `stop.go` `setMessage` validates everything before changing anything. It then replaces the current stop's top-frame Snapshot with an edited copy and records the edit for `Arrive`.
  - Object and array values are stored in canonical form (UseNumber, then `json.Marshal`), so the edit stop shows what later stops show.
- **Stepper:** `applyEdits` runs before the next child and at the end stop. It copies the batch slice and the message (`m.Copy()`) first, then uses `SetBytes`, `MetaSetMut` and the existing `metaValue`.
- **DAP:**
  - `supportsSetVariable` is advertised.
  - `setMessageOf` maps the request through the current stop. It fills `StopID`, requires a key's `Internal` to match its node, and refuses anything else with EDIT_INVALID.
  - When the client supports it, the server sends an `invalidated` event for the variables area (an addition), so the message summary refreshes.
- **Outside the Code Map's leave-alone list, test files only:**
  - `rewrite/translate_test.go`: the test arriver's new signature.
  - `vscode/src/test/adapter.test.ts`: the pinned capability set now includes `supportsSetVariable`.
- **dapdrive:** `Options.Edits`, `Edit`, `Result.EditErrors` and `Stop.Edited`.
- **Sample:** `testdata/tracer/edit.yaml`.
- **Tests:**
  - core: `edit_test.go`, including `TestSetMessageLeavesEarlierStops` and the stale-stop refusal.
  - dap: `setvariable_test.go`.
  - dapdrive: tests for the JSON plus int edit, the refusals, binary content, the `branch` end stop and Restart.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 5, false 0, maybe-false 0.
- **medium, patch (fixed):** nothing proved that an edit at a later stop of the same entry leaves an earlier stop's Snapshot alone. dapdrive read both stops before editing, and `lf.snap` persists across the stops of an entry. `TestSetMessageLeavesEarlierStops` now covers it.
- **low, patch (fixed):** the Session couldn't refuse a stale stop, since the frame id stays the same across an entry's stops. `SetMessage.StopID` was added; the Session refuses a mismatch, and DAP fills it.
- **low, patch (fixed):** a key was accepted under a node that doesn't list it (`__x` through `metadata`, a normal key through `internal`). That is now EDIT_INVALID, and both cases are in `setvariable_test.go`.
- **low, patch (fixed):** an edited object showed the entered key order at the edit stop but sorted keys afterwards. It is now stored in canonical form.
- **low, rejected:** `vscode/src/test/adapter.test.ts` changed although the Code Map said to leave `vscode/src` alone. The test pins the advertised capabilities, so `supportsSetVariable` requires the change. It is a test only, recorded above.
- **low, rejected:** the "error or batch node" matrix row has no dapdrive test, because `dapdrive.Edit` can't address those variables. The refusal is covered at the DAP server in `setvariable_test.go`.

## Verification

**Commands:**
- `go test -race ./debugger/... ./test/dapdrive/...` -- expected: pass.
- `task ci` -- expected: exit 0.

**Manual checks:**
- In VS Code, F5 `testdata/tracer/edit.yaml`, then Step Over once. In Variables, set `content` and a metadata value with "Set Value", then Continue. The Debug Console's delivered batch shows the edits.
