---
title: 'Rewrite: step and capture configs for every non-concurrent parent'
type: 'feature'
ticket: '5'
created: '2026-10-16'
status: done
baseline_revision: 'b4e9c3374ccdaf2f194897c6ba11fa9ad0444b4b'
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

**Problem:** The rewrite wraps only `pipeline.processors` in one stepper. `input.processors`, `output.processors`, every nested parent and `processor_resources` run unseen. `input.processors` stays in the capture config, against AD-4(a). Unsafe stdio inputs and outputs are only partly handled, and errors carry no structured line.

**Approach:** `debugger/rewrite` produces the full AD-4 pair:
- the capture config: the input block with `input.processors` removed;
- the step config: an ordered stepper list for `input.processors` → `pipeline.processors` → `output.processors`, with every non-concurrent AD-6 child list wrapped recursively, including inside `processor_resources`.

Each stepper carries a dense `id`, `parent`, `mode` (AD-7 table), `strict` and `lines`. `LineMap` covers every (stepperId, childIndex) stop, including the end-of-entry stop. Concurrent and unknown parents stay opaque, with launch warnings. The stepper config spec in `debugger/engine` accepts the new fields, so the tracer path keeps running.

## Boundaries & Constraints

**Always:**
- Only `debugger/rewrite` parses or rewrites YAML (yaml.v3), and it never writes the user's file. `${VAR}` stays verbatim (AD-6).
- Wrapped child lists (AD-6, AD-7):
  - `input`/`pipeline`/`output` `processors`, `branch.processors`, each `switch` case's `processors`, `try`, `catch`, `for_each`, `while.processors`, each `group_by` group's `processors`, and `retry.processors` when `retry.parallel` isn't true;
  - the same lists inside every `processor_resources` entry.

  A wrapped list becomes `[{rpcn_debug_stepper: {id, parent, mode, strict, concurrent: false, lines, processors: <the original list>}}]`.
- Mode comes from the AD-7 table: `try` for `try`, and `all` for everything else. `strict: true` on every stepper when `error_handling.strict` is true. Ids are dense integers in rewrite order. `parent` is the id of the enclosing stepper, or -1 for a top-level one (AD-16).
- `LineMap` is keyed by (stepperId, childIndex). It gives each child's user path (e.g. `pipeline.processors[2].branch.processors[1]`, `processor_resources.<label>.…`) and 1-based line. It also gives childIndex = n (after the last child), reported at the line of the parent's key (AD-10).
- Opaque, with a warning, and nothing below them wrapped:
  - concurrent parents: `workflow` (and its `branches`), `parallel`, `retry` with `parallel: true`;
  - unknown parents: any processor whose config holds a `processors` list and isn't in the table.

  A `processor_resources` entry referenced (`resource: <label>`, or a workflow's `branch_resources`) from inside a concurrent parent stays unwrapped, with a warning (epic decisions, 2026-10-16).
- Stdio and dropped sections (AD-5, AD-15, epic decision):
  - `stdin` is rejected anywhere in the input tree, including inside `broker`/`sequence`;
  - `stdout`/`stderr` outputs get a "redirected to dry-run" warning;
  - `metrics`, `tracer`, `http` and `logger` are dropped.
- Errors are `core.Error` with code `PIPELINE_INVALID`, `Line` and `UserPath` set when known.
- YAML aliases are resolved before any subtree is re-encoded, so an alias to an anchor elsewhere in the file still works (deferred from 2.1).
- The tracer F5 path and all existing tests keep passing.

**Never:**
- No stepper behaviour beyond accepting the fields. `mode`, `strict`, `parent`, frames, labels and `CTX_LOST` are acted on in 2.7, so until then a `try` stepper runs like `all`; this is a known interim gap, checked by 2.9.
- No wrapping of `workflow`/`parallel`/parallel `retry` (epic-concurrency). No consumer-group overrides or input classes beyond `stdin` (epic-safe-against-real-systems).
- No Bloblang statement lines (epic-bloblang-statement-stepping).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Top-level lists | `input`, `pipeline` and `output` each with 1 processor | Capture input has no `processors`. The step config has 3 steppers in that order (ids 0, 1, 2; parent -1) | (none) |
| Nested parents | One of each wrapped parent | Each child list is wrapped, with the right `mode`, `parent` and `lines`. LineMap has every child plus childIndex = n at the parent key line | (none) |
| Strict | `error_handling.strict: true` | Every stepper has `strict: true` | (none) |
| processor_resources | A resource with a `branch` inside, used via `resource:` | The resource's inner list is wrapped; its paths start `processor_resources.<label>` | (none) |
| Concurrent parent | `parallel` with a nested `branch` | Neither is wrapped; one warning names `parallel` and its line | (none) |
| Resource under a concurrent parent | `workflow.branch_resources: [b]`, or `resource: r` inside `parallel` | That resource stays unwrapped, with a warning | (none) |
| Unknown parent | A plugin with a `processors` list | Unwrapped, with a warning naming it and its line | (none) |
| Stdio | `stdin` inside `broker.inputs`; `stdout` output | Launch fails | `PIPELINE_INVALID`, `Line` = the `stdin` key line. The `stdout` output gives a redirect warning |
| Dropped sections | `metrics`, `tracer`, `http`, `logger` | None in the step or capture config | (none) |
| Alias | `x: &m 'root = 2'` and `- mapping: *m` | The rewritten config holds the resolved value and builds | (none) |
| Bad structure | `pipeline.processors` is a mapping | No plan | `PIPELINE_INVALID`, with `Line` and `UserPath` `pipeline.processors` |

</frozen-after-approval>

## Code Map

- `debugger/rewrite/rewrite.go` (161 lines) is the starting point:
  - `Plan{Input, Resources, Stepper, Children}`, and `Position` with a single stepper id 0.
  - `Rewrite` already rejects a top-level `stdin`, handles null `pipeline`/`processors`, and collects `*_resources` into `Plan.Resources`.
  - The `value`, `isNull`, `scalar` and `encode` helpers stay.

  Changes:
  - Replace `Stepper` with an ordered `Steppers []string` (the input, pipeline and output stepper configs, omitting empty ones). Add `Warnings []string`, and replace `Children` with a map behind `Position`.
  - Rewrite `processor_resources` entries in place inside `Resources`.
- `debugger/rewrite/testdata/*.yaml|.golden` and `rewrite_test.go`: golden tests regenerated with `-update`. Add one golden pair per matrix row.
- `debugger/engine/engine.go`:
  - `Configs{Input, Resources, Stepper}` and `buildStep`/`configureStep` call `AddProcessorYAML(r.conf.Stepper)` once (around line 136). Change them to call `AddProcessorYAML` for each stepper in order.
  - `Configs` gains `Warnings`.
- `debugger/engine/stepper.go`: `stepperSpec()` has fields `id`, `lines` and `processors`. Add `parent` (int, default -1), `mode` (string enum `all`/`try`, default `all`), `strict` (bool, default false) and `concurrent` (bool, default false), all accepted and stored but not acted on. `ProcessBatch` is unchanged.
- `debugger/serve.go` `loadPipeline`: today it maps `plan.Stepper`; map `Steppers` and `Warnings` instead.
  - It wraps rewrite errors as `core.NewError(CodePipelineInvalid, "%s: %v", path, err)`. Pass a returned `core.Error` through so `Line`/`UserPath` survive (`core.AsError` keeps them since 2.2).
  - Launch warnings must reach the user as DAP Output events. The `core.Engine.Check` result carries them: extend its return with warnings, which the Session emits as `core.Output` events at launch.
- `debugger/core/port.go`: `Engine.Check(pipeline, cwd) (LineMap, error)` becomes a result with warnings. Update the fake engines in core, dap and serve tests.
- Connect v4.112.0 component config shapes, to check against docs/source:
  - `switch` is a list of `{check, processors}`;
  - `group_by` is a list of `{check, processors}`;
  - `try`, `catch` and `for_each` are lists;
  - `branch`, `while` and `retry` are mappings with `processors`;
  - `workflow` has `branches` and `branch_resources`;
  - `parallel` has `processors`.

  The source is at `/home/aleksejjurcenko/projects/redpanda-replicator/v4.112.0/connect` (benthos processors in the module cache, `internal/impl/pure/processor_*.go`).

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/rewrite.go` (split into `walk.go`, `linemap.go` and `stdio.go` if it grows past ~300 lines):
  - resolve aliases;
  - build the capture input without `processors`;
  - walk the input/pipeline/output lists and `processor_resources`, wrapping per the table;
  - give steppers ids and parents;
  - fill LineMap (children plus end-of-entry stops);
  - collect warnings;
  - apply the stdio rules;
  - return `core.Error`s.
- [x] `debugger/rewrite/rewrite_test.go` + `testdata/`: one golden pair per matrix row, and LineMap assertions for the nested case (paths and lines, including childIndex = n).
- [x] `debugger/engine/stepper.go`, `engine.go` (+ tests): accept the new stepper fields, add steppers in order, and carry `Warnings`. A test runs a pipeline with input, pipeline and output processors plus a nested `branch` through the engine and checks it delivers.
- [x] `debugger/core/port.go`, `session.go`, `debugger/serve.go` (+ tests): the Check result carries warnings, the Session emits them as Output at launch, and rewrite `core.Error`s pass through with `Line`/`UserPath`.
- [x] `testdata/tracer/nested.yaml`: a demo pipeline with `input.processors`, a `branch` and a `switch` inside `pipeline.processors`, and `output.processors`. It's used by the engine test and is F5-able.

**Acceptance Criteria:**
- Given `testdata/tracer/nested.yaml`, when the user presses F5, then the session stops before the first `input.processors` child (the line VS Code highlights) and Continue delivers. The Debug Console shows no warnings.
- Given a pipeline with a `parallel` processor, when the user presses F5, then the Debug Console shows a warning naming `parallel` and its line, and the session still runs.
- Given `task ci`, when it runs, then every golden and engine test passes and the tracer F5-path test still passes.

## Plan Change Log

## Implementation Notes

- `debugger/rewrite` is split into `rewrite.go` (Plan, Rewrite, helpers, alias resolution), `walk.go` (wrapping, ids, LineMap, opaque/taint warnings) and `stdio.go` (stdin rejection, stdout/stderr warnings). `Position` lives in `rewrite.go`; a separate `linemap.go` wasn't needed.
- Aliases: the whole document is deep-copied with every alias replaced by a copy of its target, and anchors cleared, before anything is wrapped or encoded.
- Empty and null child lists get no stepper (top-level and nested alike), so a pipeline with no processors anywhere has no steppers.
- A resource used inside a concurrent parent is found transitively (resource → resource) and always gets the warning, even when it has no child list. The `resource:` scan is key-based, so a `cache` processor's `resource:` field inside a concurrent parent also marks a same-named processor resource.
- Unknown parents: any component whose config is a mapping with a `processors` list (e.g. `cached`, `try_catch`, plugins), plus the `processors` processor (a bare list).
- stdin is also rejected inside `input_resources`. stdout/stderr also get the warning inside `output_resources`. The warnings skip `processors` subtrees.
- Errors: yaml.v3 parse errors get `Line` from their text. Structural errors use the line of the offending value node (e.g. `pipeline.processors` as a mapping reports the mapping's first line).
- `core.CheckResult{Lines, Warnings}` replaces the LineMap return of `Engine.Check`. The Session emits each warning as `Output{Category: "console", Text: "Warning: …"}` at the end of launch, so it reaches DAP before the launch response.
- Review fixes: merge keys (`<<`) are expanded during alias resolution (explicit keys win, then earlier sources). An interpolated `retry.parallel` counts as concurrent. An interpolated `error_handling.strict` is written verbatim into each stepper, with a warning. Otherwise strict reaches the step stream through `Plan.Strict` → `Configs.Strict` → `StreamBuilder.SetStrict`. Processor lists below the top level of the input or output tree get a warning. Unknown parents are found at any depth, and resources referenced from inside them stay unwrapped. YAML parse errors drop the duplicated "yaml: line N: " prefix.
- `TestNothingCaptured` (engine) now uses an input that closes at once. Its old setup (`input.processors` deleting the message) is now `TestInputProcessorsAreStepped`, because input.processors run in the step stream.

- 2026-10-19 F5 check (user): `testdata/tracer/nested.yaml` stopped at the first `input.processors` child with no warnings and Continue ended the session; a scratch pipeline with `parallel` showed the warning in the Debug Console and still ran. AC 1 and 2 are met. The scratch file was deleted.

## Review Triage Log

Pass 1 (quick): high 1, medium 4, low 2, false 0, maybe-false 0. All seven were patched.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | Merge keys (`<<: *base`) are inlined as a `<<` key, but lookups see only literal keys, so processors are silently dropped | high | patch | Reproduced by the reviewer (no steppers, no warning). `resolve` now expands merges with YAML semantics (explicit keys win, then earlier sources), covered by the `merge` golden for input, pipeline and a nested `branch` |
| 2 | `${VAR}` in `retry.parallel` or `error_handling.strict` decodes as false | medium | patch | An interpolated `retry.parallel` is opaque, with a "decided at runtime" warning, and counts as a taint root. An interpolated strict is written verbatim into each stepper's `strict` (benthos resolves env first). Covered by the `interpolated` golden |
| 3 | Processor lists in composite outputs and sub-inputs get no warning (AD-6), though output ones don't run in dry-run and input ones run unstepped at capture | medium | patch | `warnNestedLists` gives one warning per list with its path and line (`composite` golden). `input.broker.processors` isn't a Connect field, so it isn't in the fixture |
| 4 | Unknown-parent detection only checks a top-level `processors` key (misses `openai_chat_completion.tools[].processors`) | medium | patch | It now looks at any depth through a shared `classify()` (`unknown-nested` golden) |
| 5 | Resources referenced inside opaque unknown parents are still wrapped | medium | patch | Unknown parents are now taint roots, and the scan follows processor structure (`unknown-taint` golden) |
| 6 | `error_handling` doesn't carry into the step config (AD-4b), so strict pipelines run non-strict | medium | patch | `Plan.Strict` → `Configs.Strict` → `SetStrict` on the step builder. `TestStrictStepStream` fails without it. An interpolated strict warns "per stepper only" |
| 7 | Parse errors repeat their line | low | patch | The `yaml: line N:` prefix becomes `invalid YAML:` and the line goes in `Line` (`bad-yaml.golden`) |

## Design Notes

Each wrapped list becomes a one-element list holding the stepper, so the parent processor's own engine code (ExecuteAll, ExecuteTryAll, catch selection, group splitting) still drives when the list runs, and AD-7 equivalence holds by construction. The stepper only decides when to stop inside it. Two orderings matter. Ids are assigned in a depth-first pre-order walk (top-level input, then pipeline, then output, then `processor_resources` in file order), so the ids are stable for a given file. The end-of-entry stop (childIndex = n) uses the line of the key that owns the list (e.g. `processors:` under `branch`), which is where VS Code highlights "leaving this block".

## Verification

**Commands:**
- `go test -race ./debugger/...`: expected PASS, including the new goldens.
- `task ci`: expected exit 0.

**Manual checks:**
- F5 on `testdata/tracer/nested.yaml` stops at the first `input.processors` child. F5 on a copy with a `parallel` processor shows the warning in the Debug Console.
