---
title: 'Resources used by the input run unstepped during capture'
type: 'bugfix'
ticket: ''
created: '2026-10-22'
status: 'built'
baseline_revision: '57e20a2d9a741fd6a9d429d15136f2d30e7e89a4'
route: 'oneshot'
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

**Problem:** This is S1 from the epic-tracer retrospective (action A2, to fix before E3 by user decision 2026-10-22). Take a `processor_resources` entry that is referenced from the input tree, for example `input.broker.inputs[].processors: [{resource: r}]`. It is stepped inside the capture stream: the first stop is `processor_resources.r.branch.processors[0]`, before `Captured` and before any pipeline stop. A message the input re-sends after its nack would also hit a stop in a stream the session is stopping. The launch warning says input-tree processors run "during capture, not stepped". The cause: `configureCapture` adds the wrapped `Resources`, and both streams build on one env clone, `r.env`. Its `rpcn_debug_stepper` is registered with the session's real arriver.

**Approach:**
- In `debugger/engine/engine.go` `newRun`, the capture stream gets its own `a.env.Clone()`, with `rpcn_debug_stepper` registered against `nopArriver{}`. The step stream keeps `r.env` and the real arriver.
- `configureCapture` (`debugger/engine/capture.go`) builds on that clone, so resource steppers run their children there but never stop, never fork threads and never emit Skip.
- `builder()` takes the env to build on, or an equivalent small change.
- The wrapped resources are still added to the capture stream. The rewrite, LineMap, positions and translation stay unchanged, so lint errors and logs in capture still map to file lines.
- The stepper with a no-stop arriver runs children the way the unwrapped config does; the AD-8 corpus proves that for every parent kind.

Tests:
- A `test/dapdrive` test with the retro repro: the first stop is `pipeline.processors[0]`, no stop lands under `processor_resources.r` before `Captured`, and the delivered message carries `c: 3` (the branch ran during capture).
- A new equivalence case, `testdata/equivalence/input-resource/`: a broker input whose `processors` reference a branch resource that the pipeline also uses. It proves the output matches production, and that the resource is still stepped when the pipeline uses it.
- No golden changes. The rewrite is untouched.

</frozen-after-approval>

## Implementation Notes

Oneshot: the change is a second env clone and the builder plumbing in `engine.go`/`capture.go` (~20 lines), plus two tests and one corpus case. There are no rewrite changes.

- `engine.go`: `run.capEnv` is a second `a.env.Clone()` with `rpcn_debug_stepper` registered against `nopArriver{}`. `builder` takes the env, so `configureStep` builds on `r.env` and `configureCapture` (`capture.go`) on `r.capEnv`. The sink is registered on `r.env` only, which the capture stream never uses.
- Surprise: `nopArriver.Arrive` returned an error ("not running"), which would have failed every input-tree stepper. Check never runs its streams, so nothing depended on that error; it now returns nil, and the doc says the capture stream uses it too. The unused `errors` import was dropped.
- `test/dapdrive/dapdrive_test.go` `TestInputResourceUnstepped`: the retro repro (input only: the first stop is `pipeline.processors[0]`, no stop inside `processor_resources.r`, and the delivered message is `{"a":1,"b":2,"c":3}`), plus the same resource also used in the pipeline (stepped once there). It fails on the baseline: the first stop is `processor_resources.r.branch.processors[0]`. While writing it I mistook `mapping: 'root.b = 2'` for a keep-the-document step; a `mapping` starts from an empty root, so the test uses `mutation`.
- `testdata/equivalence/input-resource/`: a branch resource used by a broker input and by the pipeline, each run adding 1 to `k`, so `k == 2` shows each ran exactly once. It passes on the baseline too, because stepping during capture never changed the output, only where the debugger stopped. The dapdrive test guards the stop rule, and this case guards output parity.
- No rewrite golden changed.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 1, false 0, maybe-false 0. 1 patched, 1 rejected.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `nopArriver.Enter` pushes no frame, so a nested parent inside an input-used resource (e.g. `branch` > `try`) logs a false `CTX_LOST` warning to the Debug Console during capture, once more for each re-send. The change introduced it | medium | patch | Reproduced: the warning appears 3 times with `Enter` returning ctx unchanged. `Enter` now pushes an empty `core.FrameRef` (only the Session reads frames, and capture ctx never reaches it). The dapdrive test's resource nests a `try` and asserts no `CTX_LOST` output; it fails without the fix |
| 2 | The `input-resource` corpus case's `stops` entry can't tell a pipeline stop from a capture stop, so it doesn't prove the resource is stepped only in the pipeline | low | reject | True, and noted in Implementation Notes: the case guards output parity. The stop rule is guarded by `TestInputResourceUnstepped` (exactly 2 stops under `processor_resources.r` when the pipeline uses it, 0 when only the input does; it fails on the baseline), so no extra harness feature is warranted |

## Verification

**Commands:**
- `go test -count=1 ./debugger/... ./test/dapdrive/ ./test/equivalence/` -- expected: PASS, with `git status debugger/rewrite/testdata` clean.
- `task ci` -- expected: exit 0.

Run 2026-10-22, after the review patch: `go test` PASS on all packages, `git status debugger/rewrite/testdata` clean, `task ci` exit 0.
