---
title: 'Launch lint errors at file lines'
type: 'feature'
ticket: '12'
created: '2026-10-21'
status: 'built'
baseline_revision: 'fce1700b3dc6ccbc109c7243916504040c6086a6'
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

**Problem:** When a processor or input config is invalid, the engine's lint or parse error from `Add*YAML` reports positions like `(13,21)` in the YAML the rewrite re-encoded (the stepper wrapper, the input block, the resources), not in the user's `pipeline.yaml`. The path text has been translated since 2.4, but the line and column point nowhere the user can find (against AD-6).

**Approach:**
- For every config part it encodes (the capture input, each top-level stepper, the resources), the rewrite builds a position map from the encoded text back to the original file. It re-parses its own encoded output and walks it in step with the source nodes.
- The engine uses that map to rewrite every `(L,C)` in a lint or parse error from that part to the file's line and column, and adds the user path of the innermost processor at that position.
- The launch error (`PIPELINE_INVALID`) then carries the structured `Line` and `UserPath` of the first such position.

## Boundaries & Constraints

**Always:**
- Only `debugger/rewrite` parses or maps YAML (AD-6). The engine never parses YAML: it gets a lookup function per config part through `engine.Configs`, and only rewrites error text with it.
- The map is exact for every node that came from the user's file: encoded line and column map to the source node's `Line`/`Column`. That includes nodes copied in by alias or merge resolution, which map to where they were written. A node the rewrite synthesized (the `rpcn_debug_stepper` wrapper keys) maps to the nearest enclosing source node.
- Rewritten error text: each `(L,C)` becomes `(line X, column Y)` in file terms. When the position falls inside a processor, its user path is prefixed once, e.g. `pipeline.processors[1]: (line 10, column 18) required: expected query`.
  - A position the map can't resolve is left as written, and the message says the positions refer to the rewritten config (the 1.5 wording).
- The launch failure stays `PIPELINE_INVALID`, with `Line` and `UserPath` from the first mapped position (2.2 deferred item: rewrite errors carry structured fields).
- This is consistent with 2.4: engine path translation is unchanged; this adds positions.
- Values in error text are never altered beyond the position, so secrets in config values stay as the engine printed them (no new echo).

**Never:**
- No change to which configs are valid, and no extra lint pass of our own. No position mapping for runtime errors (they have no config positions; 2.4 covers paths).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Bad Bloblang in a pipeline mapping | `- mapping: 'root = '` at line 10 | Launch fails: `pipeline.processors[1]` and `line 10`; `Line` = 10, `UserPath` = `pipeline.processors[1]` | `PIPELINE_INVALID` |
| Unknown field in a nested processor | `branch.processors[0]` with `mapping` plus `nope: 1` at line 14 | Names `pipeline.processors[0].branch.processors[0]`, line 14 | `PIPELINE_INVALID` |
| Bad input field | `generate: {count: x}` at line 3 | Names `input`, line 3 (capture config) | `PIPELINE_INVALID` |
| Bad resource | A `processor_resources` entry with a bad mapping at line 20 | Names `processor_resources.<label>`, line 20 | `PIPELINE_INVALID` |
| Output processor | A bad mapping under `output.processors` | Names `output.processors[0]` and its line | `PIPELINE_INVALID` |
| Alias | The bad value comes in through `*anchor` | The line is the anchor's definition | `PIPELINE_INVALID` |
| Several errors | Two lint errors in one stepper | Both positions rewritten; `Line` is the first | `PIPELINE_INVALID` |
| Unmappable | A synthetic test position outside the map | Left as written, with the rewritten-config note | `PIPELINE_INVALID` |

</frozen-after-approval>

## Code Map

- `debugger/rewrite/rewrite.go`:
  - `encode(n)` (line ~351) is `yaml.Marshal` on a subtree. It's called for `Plan.Input`, each `Plan.Steppers[k]` and `Plan.Resources`.
  - Add an encode variant that returns the text plus a position map, built by `yaml.Unmarshal` of the output into a `yaml.Node` and a parallel walk with the encoded source node.
  - The source node keeps `Line`/`Column` from the original parse. Aliases are resolved by deep copy, so copied nodes keep the anchor's position.
  - Synthesized nodes (built with `scalar()` etc.) have `Line == 0`; fall back to the parent.
  - Also record, per encoded line, the innermost processor user path. The walker already knows the user paths of every child (`stops`/LineMap); attach them to the source mapping nodes it visits.
- `debugger/rewrite/rewrite.go` `Plan`: expose a lookup, e.g. `Plan.Position(part string, line, col int) (fileLine, fileCol int, userPath string, ok bool)`, with parts `input`, `stepper/<k>` and `resources`.
- `debugger/engine/engine.go` `configureStep` (around line 173: `AddProcessorYAML(st)` wraps errors with `run.stepperName(i)`), and `capture.go:30` `AddInputYAML`, plus `AddResourcesYAML`:
  - pass each error through a rewrite of its `(L,C)` groups using `Configs.Positions` (a func field set in `debugger/serve.go` `loadPipeline` from the Plan);
  - build the `core.Error` with `Line`/`UserPath` from the first mapped one.
  - Check the real error formats first: benthos lint errors look like `lint errors: (13,21) required: …`, possibly several separated by newlines, and YAML parse errors look like `yaml: line N: …`. Probe both with the stock env.
- `debugger/serve.go` `loadPipeline` maps `Plan` into `engine.Configs`; add the positions func.
- `debugger/core/translate.go` `TranslateText` (2.4) handles paths; this entry doesn't change it.
- Tests: rewrite unit tests for the map (including alias and synthesized nodes); an engine-backed test per matrix row (real `Check` on temp pipelines); a serve test that the DAP launch error carries line and path.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite` (+ tests): encode with a position map per part, plus user paths per position, exposed on `Plan`.
- [x] `debugger/engine/engine.go`, `capture.go` (+ tests): rewrite `(L,C)` in Add*YAML errors, and set the structured Line/UserPath.
- [x] `debugger/serve.go` (+ `serve_test.go`): wire the positions func, and check a DAP launch error for a bad mapping names the file line and user path.
- [x] `README.md`: nothing, unless the README describes launch errors.

**Acceptance Criteria:**
- Given a pipeline whose second processor has an invalid mapping on line 10, when the user presses F5 in VS Code, then the error names `pipeline.processors[1]` and line 10, not a rewritten-config position.
- Given `task ci`, when it runs, then every matrix row has a passing engine-backed test.

## Plan Change Log

- 2026-10-21 (user): the matrix row "Bad input field" uses an unknown input field (`nope: 2` at line 3) instead of `generate: {count: x}`. Benthos lint accepts `count: x`; it fails only at Run, with a field-relative error that carries no config position (out of scope). The row's intent, input lint errors named at `input` and their file line, is unchanged.

## Implementation Notes

- `rewrite.Plan.FilePosition(part, line, col)` (parts `input`, `stepper/<k>` via `PartStepper`, `resources`) is backed by `debugger/rewrite/positions.go`: `walker.encodeMapped` re-parses each encoded part and walks it in step with the source tree. The walker tags processor, child-list and resource-entry nodes with their user paths (`procs`), and anchors synthesized stepper wrapper nodes to the list they wrap (`anchors`), so wrapper keys map to that list and its path. The `LineMap` method is already named `Position`, hence `FilePosition`.
- Probed engine formats (benthos v4.81): `lint errors: (L,C) msg`, with later errors on their own lines; `yaml: line N: msg` for a parse error, which only happens when a `${VAR}` value breaks the YAML. A Bloblang error is reported at the value node's position plus the error's line (block: +l, flow scalar: +l-1) and column (+c); the map runs that rule backwards against the source node. A block line past its last content line (an error at the trailing newline) is clamped to the last one. A source literal with trailing spaces is re-encoded double-quoted; the map handles the style change. yaml.v3 puts a node's `Column` at its anchor or tag, so the column skips `&a`/`!tag`. Field lint errors use column 1; these map to the first node on the line.
- Review fixes:
  - The engine rebuilds a lint error from `service.LintError` (its `Line`/`Column`/`What`), as `lint errors: ` plus one `<user path>: (line X, column Y) <what>` per line, so no item text is ever pattern-matched. Only a `yaml: line N: ` at the start of the message is rewritten, as `<path>: (line X) yaml: …`.
  - These errors are `core.Error{Inline: true}`: `Error()` omits the `(at …)` suffix, and `Line`/`UserPath` hold the first position. The coarse part label (`pipeline.processors:`) is kept only when some position stayed unmapped, together with the rewritten-config note.
  - A `|` block that the encoder re-quotes onto one line (because of trailing spaces) is recorded per part. Positions on the lines its value spans after that one are unmapped rather than guessed.
  - On a file line with `${`, the column is 0, rendered as `(line X)`.
  - The part names live once in `core` (`PartInput`, `PartResources`, `StepperPart(i)`), used by both rewrite and engine.
- Deviation, matrix row "Bad input field": `generate: {count: x}` passes lint (Check), so it never reaches this code. It fails only at Run, where the error is `field 'count': yaml: unmarshal errors: line 1: …`, relative to the field rather than the part, which is a runtime error and out of scope. The test uses an unknown field (`nope: 2` at line 3) instead.
- The YAML-line test uses `a: b: c` as the bad value. For a flow error like `[a: b`, yaml.v3 itself reports the line above, and the map doesn't correct yaml.v3's own marks.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 5, false 0, maybe-false 0, plus one matrix-row renegotiation. All were patched or settled.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | A `|` block with trailing spaces is re-encoded as a one-line quoted string, so errors on Bloblang line ≥2 land on the next node (wrong processor and line) | medium | patch | It can't be exact without changing the user's value. Positions inside such a value's shadow lines now come back unmapped, with the note (`TestFilePositionRequoted`, `TestCheckErrorRequotedBlock`) |
| 2 | The "Bad input field" row can't be met as written | — | user | Renegotiated (Plan Change Log): `nope: 2` instead of `count: x` |
| 3 | The single `(at …)` suffix after several lint errors reads as the last error's location | low | patch | `core.Error.Inline` drops the suffix and keeps Line/UserPath. Fully mapped errors render as `lint errors: <path>: (line X, column Y) <what>` per item |
| 4 | The launch text repeats path and line (stepper label, inline, suffix) | low | patch | Same fix as #3; the coarse label is kept only when a position is unmapped |
| 5 | The `(L,C)` regex matched continuation lines that echo source text; the yaml pattern was unanchored | low | patch | The engine now rebuilds the message from `service.LintError` items (no pattern matching), and `^yaml: line N: ` is anchored (`TestCheckErrorEchoedPosition`) |
| 6 | Columns drift on lines with `${VAR}` (the engine lints substituted text) | low | patch | Column 0 there, rendered as `(line X)` (`TestCheckErrorInterpolatedLine`) |
| 7 | Part names were duplicated as string literals across engine and rewrite | low | patch | `core.PartInput`, `core.PartResources`, `core.StepperPart(i)`, used on both sides |

## Design Notes

Re-parsing our own encoded output is what makes the map exact: yaml.v3 gives both trees the same shape, so a lockstep walk pairs each encoded node with its source node, with no heuristics about how Marshal re-indents. The only nodes without a source are the ones the rewrite synthesized, and those are exactly the wrapper keys, which belong to the enclosing source list.

## Verification

**Commands:**
- `go test -race ./debugger/...`: expected PASS.
- `task ci`: expected exit 0.

**Manual checks:**
- In VS Code, F5 on a pipeline with a broken mapping: the error dialog names the YAML path and the file line.
