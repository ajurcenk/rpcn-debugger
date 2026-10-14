---
title: 'Equivalence harness: case discovery and reference runs'
type: 'chore'
ticket: '5'
created: '2026-10-14'
status: 'built'
baseline_revision: 'f0540587a484eaabaf6374f7c34173131693b46d'
route: 'full'
route_source: 'auto'
risk: 'low'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** AD-8 requires every case in `testdata/equivalence/` to give the same batches with and without the debugger. Nothing discovers those cases or produces the no-debugger reference output yet, so epic-tracer has nothing to compare against.

**Approach:** Add a Go harness package `equivalence` with `Cases(dir) ([]Case, error)`, which discovers `testdata/equivalence/<name>/pipeline.yaml`. Add `Case.RunReference(ctx) ([]service.MessageBatch, error)`, which runs the pipeline's own input and processors through a plain StreamBuilder, with no debugger, and collects the output batches. Ship one generate + mapping case, and run it from `task ci` so the discovered cases are listed.

## Boundaries & Constraints

**Always:**
- Only the `public/...` APIs of benthos and connect (AD-2). The reference run uses the same component set as the stock binary: `schema.Standard(...).Environment()` with `components/all`.
- The reference run never modifies the shared environment. It registers its collector on `env.Clone()`, mirroring AD-1's per-session clone.
- Batches are collected exactly as the output layer receives them: boundaries and order preserved, one batch in flight. This is the AD-8 notion of "what the sink received".
- Everything else in the case's config (`input`, `buffer`, `pipeline` with threads, resources, `error_handling.strict`) runs as written. Only the `output` section is replaced.
- Case layout follows the Consistency Conventions: `testdata/equivalence/<name>/pipeline.yaml`. Cases are returned in name order.

**Never:**
- No `input.snapshot.json`/`expected.snapshot.json`, no Snapshot type, no comparison and no debugger run: those belong to epic-tracer (AD-11).
- No stdout writes from the harness. The run's logs go to stderr at warn level.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Discover | A dir with `a/pipeline.yaml` and `b/pipeline.yaml` | Two Cases, `a` then `b`, each with Name and the path to its pipeline | (none) |
| Stray entries | Plain files next to the case dirs | Ignored | (none) |
| Case dir missing pipeline.yaml | `c/` holds only other files | Not returned | `Cases` returns an error naming `c` |
| Missing root | `dir` doesn't exist | No cases | Error |
| Reference run | Sample generate (count 3) + mapping | 3 messages, mapped, in input order | (none) |
| Bad pipeline | `pipeline.yaml` fails to lint or parse | No batches | `RunReference` returns the error |
| Never-ending input | Input never closes | Run stops when ctx ends | `RunReference` returns ctx's error |

</frozen-after-approval>

## Code Map

- New package `test/equivalence/`, next to `test/integration/`, with a non-test `equivalence.go`, so epic-tracer's tests can import it. It isn't behind a build tag, because generate needs no Docker.
- benthos v4.81.0 `public/service/stream_builder.go`:
  - `SetYAML` (line 545) sets `outputs = []output.Config{sconf.Output}`, and it errors if a consumer func was already added.
  - `AddBatchConsumerFunc` (line 414) appends a second output, which makes a fan_out with the config's own output.
  - So the run must **not** combine those two. Instead, rewrite the YAML's `output:` to a collector plugin, and call `SetYAML` on a builder from the cloned env.
- Collector: `env.RegisterBatchOutput("rpcn_reference_sink", spec, ctor)` returns `maxInFlight 1` and a zero `BatchPolicy`. It stores `batch.Copy()` under a mutex. The name matches `^[a-z0-9]+(_[a-z0-9]+)*$`.
- Environment: `schema.Standard(version, date).Environment()` plus a blank import of `connect/v4/public/components/all`, the same as `cmd/rpcn-debug/main.go` `standardEnvironment()`. Build it once with `sync.OnceValue`, and give each run its own `Clone()`.
- `gopkg.in/yaml.v3` (Stack) is used to replace or insert the top-level `output` mapping key. It's already in the module graph.
- `stream.Run(ctx)` returns nil when a finite input such as generate with `count` is exhausted and the pipeline drains.
- generate's `interval` defaults to `1s`. The sample sets `interval: ""` so the run is instant.
- `Taskfile.yml`: `test:go` runs `./...`, which compiles this package too. Add `test:equivalence` (`go test -count=1 -v -run TestCases ./test/equivalence/`) to `ci` after `test:go`, so the case names show in the output.

## Tasks & Acceptance

**Execution:**
- [x] `test/equivalence/equivalence.go`: `Case{Name, Dir, Pipeline string}`, `Cases(dir)` and `(Case) RunReference(ctx)`. The run reads the pipeline file, swaps `output` for `rpcn_reference_sink: {}`, uses a cloned env with the collector registered, builds through `SetYAML` with a warn-level stderr slog logger, runs to completion and returns the collected batches. This is the B5 harness.
- [x] `test/equivalence/equivalence_test.go`:
  - unit tests for `Cases` over the discovery rows of the I/O matrix, using `t.TempDir()`;
  - unit tests for `RunReference` on bad-pipeline and never-ending (ctx timeout) temp cases;
  - `TestCases` over `../../testdata/equivalence`: one subtest per case that runs `RunReference`, and the sample case asserts 3 messages with mapped content.

  Together these cover every matrix row and the verify line.
- [x] `testdata/equivalence/generate-mapping/pipeline.yaml`: generate `count: 3`, `interval: ""`, with a mapping; output `drop: {}`. This is the sample case.
- [x] `Taskfile.yml` and `README.md`: add `test:equivalence` to `ci`, and describe the corpus layout in a line or two.

**Acceptance Criteria:**
- Given a fresh clone, when `task ci` runs, then the `test:equivalence` output lists `generate-mapping` as a passing subtest, and `task ci` exits 0.
- Given the harness, when `task lint:go` runs, then it reports 0 issues (AD-2 depguard covers the new package).

## Plan Change Log

## Implementation Notes

- `output.label` and `output.processors` are kept when the output plugin is swapped for `rpcn_reference_sink: {}`. Only the plugin (and with it its `batching`) is replaced, so the collector receives batches after `output.processors`, the same point where `rpcn_debug_sink` receives them (AD-21). `TestWithReferenceSinkKeepsOutputProcessors` covers this.
- The builder gets `SetHTTPMux(http.NewServeMux())`. Without it, a config that leaves `http` at its defaults binds `0.0.0.0:4195` for every run, so parallel or concurrent runs collide. The HTTP server has no effect on batches.
- `Stream.Run` returns `ctx.Err()` at once when ctx ends and leaves the stream running, so `RunReference` calls `StopWithin(5s)` before it returns the ctx error.
- The sample case sets `pipeline.threads: 1`. With the default (one thread per CPU), the 3 messages reached the sink out of input order in the first run. Any case that asserts order needs one thread.
- `gopkg.in/yaml.v3` moved from indirect to direct in `go.mod` (`go mod tidy`). The version is unchanged (v3.0.1).
- `schema.Standard("dev", "")` builds the shared environment. The harness has no ldflags version.
- Verified: `go test -count=1 -v ./test/equivalence/` PASS (including `TestCases/generate-mapping`), stable over 5 runs and `-race -count=3`. `task ci` exits 0, with `lint:go` at 0 issues.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 5, false 0, maybe-false 0. Three were patched and three rejected.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | Output processors are dropped when `output` is a YAML alias or uses a `<<` merge key | low | reject | Confirmed by the reviewer's probe. Corpus cases are hand-written in `testdata/equivalence/`, and none use anchors. Handling it means resolving aliases and merges, which is more than a direct correction for a layout nobody writes |
| 2 | An anchor on the `output` node is lost when the node is replaced, so a later `*alias` breaks | low | reject | Same root as #1: a pattern with anchors in a test corpus we author ourselves. Out of everyday use, and the fix is nontrivial |
| 3 | Lint `line:col` positions point into the re-encoded YAML, not the case's `pipeline.yaml` | low | patch | Confirmed: `yaml.Marshal` re-indents the YAML and inserts the sink key. Fix: the `SetYAML` error now says the positions refer to the rewritten config, with no remapping |
| 4 | A case with no `input` runs benthos's default (stdin) input and passes `TestCases` with 0 batches | medium | patch | Confirmed: with no `input`, the probe returned `batches=0 err=nil`, which hides a broken case behind the passing list that the AC relies on. Fix: `withReferenceSink` rejects a config with no top-level `input`, naming the case. The new `no-input` and `empty` cases are in `TestRunReferenceBadPipeline` |
| 5 | After ctx ends, the `StopWithin` error is discarded, and a real `Run` error is masked by `ctx.Err()` | low | patch (part) | The stop error is now returned as `errors.Join(ctxErr, stop error)`. The masking half is rejected: it needs ctx to expire during a natural stop, and that path returns nil batches anyway |
| 6 | Symlinked case dirs are skipped, and hidden subdirs count as cases | low | reject | The corpus has no symlinks or hidden dirs. Not met in normal use |

Also: `test:equivalence` used `-run TestCases`, which also matched the `TestCases*` discovery tests. Narrowed to `^TestCases$`, as the implementer noted.

## Design Notes

Replacing `output` with a registered batch output, rather than stacking a consumer func on top, is the shape the debugger's own `rpcn_debug_sink` takes (AD-8 compares "what the sink received"). epic-tracer can then run the debugger side through the same kind of sink, and the two results are directly comparable.

## Verification

**Commands:**
- `go test -count=1 -v ./test/equivalence/`: expected PASS, with a `TestCases/generate-mapping` subtest.
- `task ci`: expected exit 0.
