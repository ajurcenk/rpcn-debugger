---
title: 'Interpolated error_handling.strict applies to the step stream'
type: 'bugfix'
ticket: ''
created: '2026-10-22'
status: done
baseline_revision: '6d633ec0a1b961d2656ea7426362bbe1af19fe61'
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

**Problem:** S2 from the epic-tracer retrospective (action A1, fixed before E3 by user decision 2026-10-22). `error_handling.strict: ${STRICT}` with `STRICT=true` delivers a message that failed a processing step, which production rejects. This breaks AD-8: the debugger changes the output. The cause is that rewrite reports `Plan.Strict=false` for an interpolation. So the engine calls `SetStrict(false)` on the step stream and skips the `STRICT_REJECTED` gate. Only the steppers get the verbatim `${STRICT}`.

**Approach:** The rewrite passes the verbatim interpolated scalar through to the engine. The engine resolves it with the session's env lookup (AD-22), the same way benthos resolves `${VAR}`. The resolved value drives both `SetStrict` and the `STRICT_REJECTED` gate. The launch warning that strict is applied per stepper only goes away.

## Boundaries & Constraints

**Always:**
- Resolve the value with benthos's own interpolation, through the public API (AD-2). Use `env.CoreConfigSchema("", "").NewConfigQuerier()` with `SetEnvVarLookupFunc` set to the run's lookup (nil means `os.LookupEnv`). Parse `error_handling:\n  strict: <verbatim>` with `ParseYAML`, then read the bool. That covers `${VAR}`, `${VAR:default}` and the `|func` forms exactly as production does.
- When `STRICT` is unset with no default, or isn't a bool, the run must behave the way the stock stream builder does on the same full config. Probe it first: build a tiny config in a test with `schema.Standard` and the builder, then match it. Record what you found in Implementation Notes. Check and Start must agree.
- Literal `strict: true/false` and a missing `error_handling` behave exactly as before. Every rewrite golden is unchanged except `interpolated.golden` (the warning is dropped).

**Never:**
- Don't re-implement the `${...}` regex or the bool parsing in this repo.
- Don't resolve inside `debugger/rewrite`. It has no env, and it stays env-free.
- Don't change how the steppers get strict. They keep the verbatim value and resolve it themselves.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Interpolated true | `strict: ${STRICT}`, launch env `STRICT=true`, a processor throws | Nothing is delivered. The run ends with `STRICT_REJECTED` (a note, not a failure), as with literal `true` | none |
| Interpolated false | `STRICT=false` | The failed message is delivered with its error, as with literal `false` | none |
| Default | `strict: ${EQ_STRICT:true}` with the var unset | Same as true | none |
| From envFile | `STRICT=true` comes from the launch `envFile` | Same as true (AD-22 lookup order) | none |
| Unset or invalid | `${STRICT}` unset, or `STRICT=maybe` | Matches the stock builder's behaviour, as probed | If production errors, the launch fails `PIPELINE_INVALID` naming `error_handling.strict` and its line |

</frozen-after-approval>

## Code Map

- `debugger/rewrite/rewrite.go`:
  - `Plan.Strict` (doc lines 34-36) and `strict()` (~219), which returns `(false, copy)` for an interpolation.
  - Lines 99-102 emit the "applied per stepper only" warning; remove it.
  - Add a Plan field holding the verbatim scalar (empty when not interpolated) and its line.
  - `isInterpolated` (~323) is the existing test for an interpolation.
- `debugger/engine/engine.go`:
  - `Configs` (~25-55): add the matching field(s).
  - `newRun` (~120) has the lookup. Resolve there or in a helper, store the effective strict on `run`, and have Check and Start fail the same way.
  - `configureStep` `b.SetStrict(r.conf.Strict)` (~169) and the gate in `step` (~234) use the effective value.
- `debugger/serve.go` `loadPipeline` (~83): copy the new Plan fields into Configs.
- `debugger/engine/engine_test.go`: its `load` helper (line 39) copies Plan into Configs, so copy the new fields there too. `TestStrictStepStream` (~435) and `TestStrictRejection` (~759) are the models for the interpolated variants.
- `debugger/rewrite/testdata/interpolated.{yaml,golden}`: regenerate with `-update`. Only the warning line may change.
- `testdata/equivalence/strict/` (`pipeline.yaml`, `input.json`, `expect.yaml`) is the template for a new case, `testdata/equivalence/strict-interpolated/`, which uses `strict: ${EQ_STRICT:true}`. The harness already passes the process env to both runs (`caseEnv`, `test/equivalence/equivalence_test.go:194`).
- `debugger/core/errors.go` mentions STRICT. Read it only for the `CodeStrictRejected` doc; don't change it.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/rewrite.go` -- expose the verbatim interpolated strict and its line in Plan, and drop the per-stepper warning -- the engine needs the text to resolve.
- [x] `debugger/engine/engine.go` -- add Configs fields and resolve them through the ConfigQuerier with the run's lookup. Use the result for `SetStrict` and the `STRICT_REJECTED` gate, and handle errors as probed -- the fix.
- [x] `debugger/serve.go` -- pass the fields through.
- [x] `debugger/engine/engine_test.go` -- add interpolated cases covering the matrix (true, false, default, envFile-style lookup, unset/invalid), and update the `load` helper.
- [x] `debugger/rewrite/testdata/interpolated.golden` -- regenerate.
- [x] `testdata/equivalence/strict-interpolated/` -- copy the strict case with `${EQ_STRICT:true}`, with expectations identical to `strict`.

**Acceptance Criteria:**
- Given the retro repro (`strict: ${STRICT}`, `STRICT=true`, a throwing processor), when it is run through `debugger.Serve` (`test/dapdrive`) to the end, then nothing is delivered and the Debug Console shows the strict-rejection note, the same as the literal form.
- Given `task ci`, then it exits 0, and `TestEquivalence/strict-interpolated` passes for Continue, Next and Step In.

## Implementation Notes

- **Probe (benthos v4.81, `StreamBuilder.SetYAML` on a full config with `error_handling.strict: ${STRICT}` and a throwing mapping):** unset with no default, or set to `""`, resolves to an empty scalar, so strict is **false** and the run delivers the failed message, with no error (the builder swallows `ErrMissingEnvVars` and uses its best attempt). `true` rejects; `false` delivers. `maybe` fails `SetYAML` with `cannot unmarshal !!str \`maybe\` into bool`. A quoted `"${STRICT}"` with `STRICT=true` also fails (a `!!str` is not a bool), and an unknown `|func` fails with `unknown env var decode function`. The ConfigQuerier gives the same results in every case.
- **Engine:** `run.resolveStrict` (in `newRun`, so Check and Start agree) parses `error_handling:\n  strict: <StrictExpr>` with the querier and the run's lookup (nil keeps the querier's `os.LookupEnv`), then reads `FieldAtPath("error_handling", "strict").FieldBool()`. The YAML is padded with newlines so a YAML error names the user's file line. A failure is `PIPELINE_INVALID` with `UserPath: error_handling.strict` and `Line` set. `r.strict` drives `SetStrict` and the `STRICT_REJECTED` gate. Check and Start now prefix `newRun` errors with the pipeline path, like their other config errors.
- **Rewrite:** `Plan.StrictExpr` is the scalar re-encoded with yaml.v3 (not `Value`), so a quoted scalar stays quoted, exactly as the steppers receive it; `Plan.StrictLine` is its line.
- **Tests:** `TestStrictInterpolated` (engine: env true/false, envFile, `:true` default, env beating the default, unset, empty, `maybe`, quoted, unknown `|func`, for Check and Start), `TestStrictExpr` (rewrite), `TestStrictInterpolatedRejects` (dapdrive retro repro, literal and interpolated, fails on the baseline), and the `strict-interpolated` equivalence case (fails on the baseline for all three scripts).

## Plan Change Log

- 2026-10-22: the querier uses `env.FullConfigSchema("", "")`, not `CoreConfigSchema`. The core schema holds only input/buffer/pipeline/output, so `FieldAtPath("error_handling", "strict")` fails with `field not recognised`. Both are public API (AD-2).

## Review Triage Log

Pass 1 (quick): high 0, medium 0, low 2, false 1, maybe-false 0. 2 patched, 1 rejected.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | A head comment on the strict scalar is encoded in front of the value, so `resolveStrict` builds `strict: # c\n${STRICT}`, which fails to parse. The launch fails `PIPELINE_INVALID` where production accepts it | low | patch | The reviewer reproduced it with the pinned yaml.v3. It's a rare form, but it breaks AD-8 parity and the fix is direct: encode a copy with its comments cleared, plus a test |
| 2 | The `Plan.StrictExpr` doc comment isn't reflowed | low | patch | One line runs past its neighbours' width; reflowed |
| 3 | The code uses `FullConfigSchema` where the frozen text names `CoreConfigSchema` | false | reject | No bad outcome. The core schema has no `error_handling` field, so the frozen wording can't work as written. The intent (benthos's public interpolation, AD-2) is met, the change is logged in the Plan Change Log, and it's flagged to the user at presentation |

## Verification

Run 2026-10-22: `go test -count=1 ./debugger/... ./test/dapdrive/` PASS (only `interpolated.golden` changed under `debugger/rewrite/testdata`, the warning line dropped); `task ci` exit 0 (lint 0 issues, equivalence incl. `strict-interpolated` continue/next/stepIn, integration, extension build/test/package).

**Commands:**
- `go test -count=1 ./debugger/... ./test/dapdrive/` -- expected: PASS, with only `interpolated.golden` changed under `debugger/rewrite/testdata`.
- `task ci` -- expected: exit 0 (Docker running).

After the review patches (2026-10-22): `task ci` exit 0.
