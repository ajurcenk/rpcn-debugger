---
title: 'One logging handler and env lookup'
type: 'feature'
ticket: '4'
created: '2026-10-20'
status: done
baseline_revision: '1446496fdaa44d74ffd1a99e13d1707007be3e87'
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

**Problem:**
- Engine logs and processor errors go only to stderr, with rewritten engine paths (`root.pipeline.processors.0.rpcn_debug_stepper.processors.1`) and Bloblang line numbers relative to the mapping. So the user never sees them in VS Code, and can't map them to the YAML.
- `debugger.Serve` hard-wires `os.Stderr`, so `cmd/rpcn-debug`'s injected stderr and library callers can't redirect its logs (the 1.4 handoff).
- `${VAR}` in a pipeline resolves only from the process env; the launch's `env` and `envFile` are carried but not applied.

**Approach:**
- `Serve` builds the single AD-15 slog handler. It writes every record to the stderr side and to DAP Output events, after translating engine paths through `Translate` and Bloblang error lines through the mapping base lines (AD-6), both implemented by `debugger/rewrite`. It is installed on every StreamBuilder.
- `debugger.WithLogOutput(io.Writer)` sets the stderr side (default `os.Stderr`), and `cmd/rpcn-debug` passes its injected stderr.
- The Session builds one env lookup (process env, then `envFile`, then `env`; later wins) and the engine installs it with `SetEnvVarLookupFunc` on every builder, for both Check and Start (AD-22).

## Boundaries & Constraints

**Always:**
- One handler (AD-15). Every log record from the debugger and the engine goes through it. The handler never writes to `out`, and forbidigo stays green.
- Levels (decision, 2026-10-20):
  - stderr receives Info and above;
  - DAP Output receives Warn and above, with category `stderr` for errors and `console` for warnings;
  - the debugger's own launch warnings and notices keep their existing Output events.

  This keeps the engine's routine Info chatter ("Input type generate is now active") out of the Debug Console.
- Translation before anything reaches stderr or DAP (AD-6):
  - Every engine path in a record's `path` attribute and message is rewritten to the user path: `pipeline.processors[2].branch.processors[1]`, `input.processors[0]`, `processor_resources.<label>…`. A path that doesn't map is passed through unchanged.
  - A Bloblang error line inside a `mapping`/`mutation`/`bloblang` child is shifted to the file line using the AD-6 base-line rule: a literal `|` block's base is `Node.Line + 1`; a plain scalar on the key's line is `Node.Line`. Folded and quoted values aren't shifted.
- Env lookup (AD-22):
  - One function, built by the Session: process env, then `envFile`, then `env`, with a later source overriding an earlier one.
  - It is installed with `StreamBuilder.SetEnvVarLookupFunc` on the capture and step builders, and for Check, so lint and parse see the same values.
  - `envFile` is dotenv: `KEY=VALUE`, `#` comments, blank lines, an optional `export ` prefix, and single- or double-quoted values.
  - A malformed line is `LAUNCH_INVALID` naming `envFile` and its line number, never its value.
- Env values never appear in logs or errors.
- The `env`/`envFile` schema descriptions say they are applied, in that order. Regenerate with `task gen:schema`.

**Never:**
- No log-level launch key, and no `logger` section from the pipeline (it's dropped, AD-5).
- No `${env:NAME}` handling in Go; VS Code substitutes it.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Processor error | A `pipeline.processors[1]` mapping that throws | A DAP Output event (category `stderr`) naming `pipeline.processors[1]`, never `rpcn_debug_stepper`; the same text on stderr | (none) |
| Nested path | An error in a `branch` child | Output names `pipeline.processors[0].branch.processors[0]` | (none) |
| Bloblang line | A mapping at file line 16 (`\|`) whose 2nd statement fails | The reported line is 18 | (none) |
| Info chatter | A normal run | stderr shows the engine's Info lines; the Debug Console doesn't | (none) |
| WithLogOutput | `Serve(..., WithLogOutput(buf))` | Logs land in `buf`, not `os.Stderr`; `TestRunNoArgs` sees server logs in its injected stderr | (none) |
| Env precedence | Process `A=p`, envFile `A=f`, env `A=e` | `${A}` resolves to `e`; with no `env` entry, `f`; with neither, `p` | (none) |
| Default | `${B:fallback}` with B unset everywhere | `fallback` | (none) |
| Bad envFile | A line `=x` or `KEY` with no `=` | No launch | `LAUNCH_INVALID`, `envFile line 3`, value not shown |
| Check sees env | `${PROC}` names a processor field required at lint | Launch succeeds when `env` sets it | Without it: the normal lint error |

</frozen-after-approval>

## Code Map

- `debugger/serve.go`:
  - `Option func(*config)` with an empty `config`, and `Serve` builds `slog.New(slog.NewTextHandler(os.Stderr, nil))`. Replace this with the AD-15 handler.
  - Add `WithLogOutput(w io.Writer) Option`.
  - `loadPipeline` returns `rewrite.Plan` as the LineMap; the Plan must also provide `Translate`.
- `cmd/rpcn-debug/main.go` `run(args, stdin, stdout, stderr)` calls `debugger.Serve(env, stdin, stdout)`. Pass `debugger.WithLogOutput(stderr)`. `version_test.go` `TestRunNoArgs` can then assert server diagnostics in its stderr buffer.
- New `debugger/logging.go` (in package `debugger`, which wires the ports; `core` stays free of DAP):
  - an `slog.Handler` that formats once (text);
  - the stderr side with an Info threshold;
  - translation, with the Translate source swapped in per launch, e.g. an `atomic.Pointer` set from the Session's Check result;
  - Warn+ records forwarded as `core.Output` through a sink the DAP side provides.

  The Session already emits `Output` events via `Frontend.Emit`. Expose a way for the handler to emit through the same path, e.g. `Session.Log(level, text)`, or a `core.Frontend` the handler holds, set once `dap.Serve` builds the Session.
- `debugger/core/port.go`: `Translate` interface (`Translate(enginePath) (userPath, ok)`). Add the Bloblang line mapping, e.g. `TranslateLine(enginePath string, line int) (int, bool)`. `CheckResult` carries the translator.
- `debugger/rewrite`:
  - The rewrite knows each stepper's position. The step config adds steppers with `AddProcessorYAML` in input, pipeline, output order, so the engine path of top-level stepper k is `root.pipeline.processors.<k>` and of its child i is `root.pipeline.processors.<k>.rpcn_debug_stepper.processors.<i>`. Nested steppers sit under the parent child's engine path plus `.<parent kind path>.0.rpcn_debug_stepper.processors.<i>`.
  - Resources are added with `AddResourcesYAML`; record their engine paths too.
  - Build a prefix table (engine path prefix → user path) and replace the longest matching prefix. Convert the remaining `.N` segments to `[N]` in the user style.
  - **Verify the real engine path formats** by provoking errors in an engine test, before trusting this description.
  - The mapping base lines come from the child nodes the walker already visits (AD-6 rule).
- `debugger/core/session.go`:
  - `launch` stores `env` (map) and `envFile` (resolved path; it's checked to exist since 2.3).
  - Build the lookup there: parse envFile (new `core/dotenv.go`, stdlib), merge in order, and fall back to `os.LookupEnv`.
  - Pass it in `StartOptions` and to Check. `Engine.Check` takes the options it needs (pipeline, cwd, lookup).
- Check first which level Connect v4.112.0 / benthos v4.81.0 logs a failing `mapping` at. If it's below Warn, the AC ("the Debug Console shows the error") still holds: forward records that carry a processor `path` attribute and an error at that level too, and record which in Implementation Notes.
- `debugger/engine/engine.go` `(r *run) builder()` calls `SetLogger(r.log)` and `SetHTTPMux`; add `SetEnvVarLookupFunc(opts.Lookup)`. `Check` builds through `configure*` and needs the lookup too.
- `debugger/dap/launch.go`: the `env`/`envFile` `desc` tags say "Parsed and validated, but not applied yet". Update them, then run `task gen:schema`.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite` (+ tests): a Translate table and Bloblang base lines on `Plan`. An engine-backed test provokes real errors (pipeline, nested branch, input processor, resource) and asserts the translated paths and lines.
- [x] `debugger/logging.go` (+ test): the handler (stderr at Info, DAP at Warn+, translation, swappable translator) and `WithLogOutput`. `serve.go` wires it, and `cmd/rpcn-debug/main.go` passes stderr.
- [x] `debugger/core/dotenv.go` (+ test), `session.go`, `port.go`: dotenv parsing with line-numbered errors that never show values, the precedence lookup, and lookup and translator passed through Check/Start.
- [x] `debugger/engine/engine.go` (+ test): `SetEnvVarLookupFunc` on every builder, including Check. An engine test proves precedence through a real `${A}` in a mapping.
- [x] `debugger/dap/launch.go` descs + `task gen:schema`. `debugger/serve_test.go`: a processor error reaches a DAP Output event with the user path, and `${A}` from `env` reaches the message content.
- [x] `README.md`: one line on logs in the Debug Console and env precedence.

**Acceptance Criteria:**
- Given a pipeline whose second processor throws, when the user presses F5 and continues in VS Code, then the Debug Console shows the error naming `pipeline.processors[1]` and its file line, with no `rpcn_debug_stepper` text.
- Given a launch.json with `env: {GREETING: "hi"}` and a mapping `root.g = "${GREETING}"`, when the user presses F5, then Variables shows `g: hi`.
- Given `task ci`, when it runs, then all tests pass and `lint:schema` is clean after the regenerated descriptions.

## Plan Change Log

## Implementation Notes

- **Engine log levels (Connect v4.112.0 / benthos v4.81.0), checked with a probe:** a failing `mapping`/`mutation` logs twice: `Processor failed: …` at Debug (from benthos' auto-observed wrapper) and the bare error at **Error** (from the Bloblang processor itself). The Error record already reaches the Debug Console at the Warn threshold, so Debug records are not forwarded. Other processors that only report through the Debug `Processor failed` line (e.g. a failing `http`) don't reach the Debug Console; their error stays on the message (`Snapshot.error`).
- **Real engine path formats (verified, `rewrite/translate_test.go` runs a real engine):** the benthos slog adapter adds one `path` attribute per nested component (`root.pipeline`, `root.pipeline.processors.0`, `….rpcn_debug_stepper`, `….processors`, `….processors.1`, …), so the last `path` is the record's. Top-level stepper k is `root.pipeline.processors.<k>`. Resources log under `root.processor_resources` with the entry told apart only by the `label` attribute that follows it, and their children under `root.processor_resources.branch…` with no label. The handler qualifies these to `root.processor_resources.<label>…` (`rewrite.QualifyResource`) before translating.
- **Translate:** `rewrite.Plan` now implements `core.Translate` (`Translate` + the new `TranslateLine`). The walker records the engine path of every stepper, its config/child list and each child (`paths`), and the AD-6 base line of every `mapping`/`mutation`/`bloblang` child (`bases`). Lookup is the longest recorded prefix; remaining numeric segments become `[N]`. Unmapped paths (`root.input`, `root.output`, `root.pipeline`) pass through. Children of opaque parents (workflow, parallel, unknown plugins) translate by prefix but get no base lines; an unwrapped (tainted) resource records only its own mapping's base.
- **Handler (`debugger/logging.go`):** formats once as slog text without the time (`level=ERROR msg="failed assignment (line 18): …" path=pipeline.processors[1]`); stderr gets `time=… ` + that text, the Output event gets the text itself. Repeated attribute keys keep the last value and empty `label`s are dropped. Bloblang lines are shifted only in `(line N)` references; engine paths inside the message are translated too.
- **Who reaches the Debug Console:** only the engine's logger (Warn+). The DAP server's own logger uses the same handler and sink but stderr-only: it logs while holding its write lock (`failure` inside `respondLocked`, `sending DAP event failed`), so forwarding would deadlock or recurse, and its failures are already DAP error responses.
- **Wiring:** `dap.Serve` gained a variadic `dap.Option`; `dap.OnSession(fn)` hands the built Session to the handler's sink. The handler reads the translator per record via `Session.Translator()` (set from `CheckResult.Translator` on launch) and emits through `Session.Log(level, text)`, which sends nothing after Terminate. Records before a launch go out untranslated.
- **Env:** `core.EnvLookup(fileEnv, env)` (exported so the engine test can prove precedence) and `parseDotenv` in `core/dotenv.go`. `Engine.Check` now takes `core.StartOptions` (Pipeline, Cwd, Lookup; MaxMessages ignored). The lookup is set with `SetEnvVarLookupFunc` in `run.builder()`, before any `Add*YAML`, since benthos substitutes `${VAR}` in each `Add*YAML` call. Bad envFile error: `envFile line 3: expected NAME=value (in /abs/.env)`.
- `TestRunLogsToStderr` (new, next to `TestRunNoArgs`) asserts the server's EOF notice lands in the injected stderr.

- 2026-10-20 VS Code check (user): with `env: {GREETING: hi}`, Variables showed `g: hi`. The throwing processor's error appeared in the Debug Console at `pipeline.processors[1]` and its file line, with no rewritten paths and no engine Info chatter. All ACs met. The scratch pipeline and launch entry were removed.

## Review Triage Log

Pass 1 (quick): high 0, medium 2, low 4, false 0, maybe-false 0. Five were patched; the rest went to two new entries (user, 2026-10-20).

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | dotenv: `A= # nothing` gives the comment as the value | medium | patch | A value starting with `#` is now empty (`TestParseDotenv` H, I) |
| 2 | dotenv: `export<TAB>NAME=…` is rejected with a misleading error | low | patch | `export` plus any whitespace is stripped (J) |
| 3 | Launch and run errors reach the frontend untranslated (AD-6); lint shows rewritten-config `(L,C)` and stepper wording | medium | patch (paths) + new entry 2.12 (positions) | `core.TranslateText` covers the Check error and the run-failure output, a failed Check still returns the translator, and the stepper is named by its user list path (`TestRunFailureTranslated`, `TestLaunchErrorTranslated`, `TestCheckErrorNamesUserPath`). Remapping `(L,C)` to file lines became entry 2.12 (user) |
| 4 | Every `(line N)` is shifted, including user text | low | patch | Only Bloblang's leading `(line N):` is shifted (`TestLogTranslation`) |
| 5 | Warn+ engine logs are emitted after Terminated when a run ends on its own | low | patch | A `runDone` flag is set before Terminated, and Log drops records after it (`TestLogAfterRunTerminated`) |
| 6 | An undefined `resource:` reference hangs the session (pre-existing since 2.1) | medium | new entry 2.13 | The rewrite will reject it statically with PIPELINE_INVALID (user decision) |

## Design Notes

The handler lives in package `debugger`, not `core`: it bridges the engine's slog world and the frontend, which is wiring, and `core` stays stdlib-only and frontend-free. The translator is swapped per launch because the LineMap only exists after Check, while the handler must exist before the first log line. The Warn threshold for DAP is a usability call: Connect logs several Info lines per stream start, and the Debug Console is where the user reads processor errors and debugger warnings.

## Verification

**Commands:**
- `go test -race ./debugger/... ./cmd/...`: expected PASS.
- `task ci`: expected exit 0.

**Manual checks:**
- F5 on a pipeline with a throwing processor: the Debug Console shows the user path and line. F5 with `env` set: Variables shows the substituted value.
