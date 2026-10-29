---
title: 'Launch warnings: side-effecting processors and state split between capture and stepping'
type: 'feature'
ticket: '9'
created: '2026-10-29'
status: 'built'
baseline_revision: 'af03af36b15ef7d3b70bd2a096a6a29b51b59b9e'
route: 'full'
route_source: 'auto'
risk: 'low'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-launch-warnings-side-effecting-processors-and-state-split-be-processor-effects.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:** This is E3 entry 9 (CAP-11; D7, D9). Under `dry-run`, nothing is written to the pipeline's output, but processors still run. A `sql_insert`, a POST `http`, a `redis` SET or a remote `cache` write still changes production, and nothing tells the user. Processors in the input tree also run during capture, before the user can pause.

Separately, an in-process cache or rate limit used both in the input tree (capture) and in the step tree holds two separate states in a debug session, because each stream builds its own resources (AD-1). In production that state is shared, so the output can differ.

**Approach:** launch warnings only; nothing is refused and stepping is unchanged. All of it is in `debugger/rewrite`.

- **Processor effect table** (D7): one row per side-effecting processor from the research file `story-launch-warnings-side-effecting-processors-and-state-split-be-processor-effects.md`. Each row has a predicate over its fields and a warning text naming what it writes to (its target field: url, dsn, resource, topic, …). The user's rules of 2026-10-29 override the research:
  - `sql_raw` and `sql` always warn;
  - `http` warns unless `verb` is GET or HEAD (its default is POST);
  - `javascript` warns when `v0_fetch` appears in `code` or `file`;
  - `awk` warns when its program contains `system(`;
  - `ollama_*` warns, because it pulls a model at startup;
  - `rate_limit` warns when its resource is a `redis` rate limit;
  - `cache`, `cached` and `dedupe` warn only when their cache resource is remote or persistent. In-process caches (`memory`, `lru`, `ttlru`, `ristretto`, `noop`) don't warn; `file`, `multilevel` with any non-in-process level, and every other cache do.
- **Where it applies:** every processor list in the pipeline. That covers the stepped lists, opaque parents' lists, `processor_resources`, nested input processors, and the chat processors' `tools[].processors`. A processor in the input tree, or in a resource referenced only from it, says "runs during capture, not stepped". One warning per processor.
- **State split** (D9): a `cache_resources` entry of an in-process kind, or a `rate_limit_resources` entry of kind `local`, referenced both from the input tree (the capture config, `input_resources`, and resources reached from there) and from the step tree gets one warning: "its state isn't shared between capture and stepping, so the output may differ from production". A reference is a string field named `resource` or `cache` (or `rate_limit` on inputs and processors) whose value is the label.

## Boundaries & Constraints

**Always:**
- Warnings only: no refusal, no new launch key, and no change to classes, configs or stepping. Every existing golden's config sections stay byte-identical; a golden changes only where it now shows a new warning.
- The table is data, in one file, citing the research evidence where the rule isn't obvious.
- Literal values only. An interpolated field that decides the predicate counts as the warning case.

**Never:**
- No warning for pure transforms, reads, in-process caches or the `local` rate limit.
- No refusal of side-effecting processors (D7 is warn only).

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| SQL | `pipeline.processors: [sql_insert: {driver: postgres, dsn: …, table: t}]` | One warning naming the database and table | none |
| HTTP POST | `http: {url: http://x}` (default verb) | One warning: sends POST requests to `http://x` | none |
| HTTP GET | `http: {url: http://x, verb: GET}` | No warning | none |
| Remote cache | `cache: {resource: r, operator: set}`, `r` is `redis` | One warning naming cache `r` | none |
| Local cache | the same with `r` a `memory` cache | No warning | none |
| Input tree | `broker.inputs[].processors: [sql_insert…]` | The warning says "runs during capture, not stepped" | none |
| Resource from input | `input_resources`/`processor_resources` processor reached only from the input tree | The same "runs during capture" marking | none |
| javascript | `javascript: {code: 'benthos.v0_fetch(…)'}` | One warning | none |
| Tools | `openai_chat_completion.tools[].processors: [http POST]` | One warning for the tool's processor | none |
| State split | a `memory` cache used by a broker input's `cache` processor and by a `pipeline` `cache` processor | One state-split warning naming the cache | none |
| No split | the same cache used only in the pipeline | No state-split warning | none |

</frozen-after-approval>

## Code Map

- `debugger/rewrite/state.go`: the `stateful` table (AD-7, a different purpose; don't merge it) and `resourceBodies`/`statefulResources`. They are the pattern for label lookups and fixed-point propagation over `processor_resources`.
- `debugger/rewrite/stdio.go`: `walkKeys`/`walkPairs` (generic key walks) and `warnNestedLists(input, "input", "runs during capture, not stepped")`, which already finds nested input lists. Reuse its marking text.
- `debugger/rewrite/walk.go`: `childLists`/`parents` (AD-7 child lists). Opaque parents' and `tools` lists aren't in `parents`, so walk processor lists generically: any `processors` sequence, plus the processor-list fields of `parallel`/`workflow` branches and `tools[].processors`.
- `debugger/rewrite/inputwalk.go`: the input-tree walker and its resource handling (3.3). It is the source of "what is in the input tree", including `input_resources`.
- `debugger/rewrite/rewrite.go` `Rewrite`: where warnings are emitted (`w.warn`), in rewrite order; add the new pass after the existing warnings.
- Research: `story-launch-warnings-side-effecting-processors-and-state-split-be-processor-effects.md` beside this plan (rows, defaults, target fields, local and remote caches and rate limits).
- Goldens: `debugger/rewrite/testdata/` (`-update`), one per matrix row or grouped. The corpus already has `testdata/equivalence/try-stateful` (a `memory` cache used only in the step tree), which must stay green with no state-split warning.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/rewrite/effects.go` (new) -- the effect table, the cache and rate-limit kind table, and the warning pass with the input-tree marking -- D7.
- [x] Same file or `state.go` -- the state-split check over the input-tree and step-tree references -- D9.
- [x] `debugger/rewrite/rewrite.go` -- call the passes -- wiring.
- [x] Goldens and table tests for every matrix row, plus one test per effect-table row.

**Acceptance Criteria:**
- Given each matrix scenario, when launched, then the warnings are exactly those in the matrix, and nothing else in the rewrite changes.
- Given `task ci`, then it exits 0, the `try-stateful` corpus case stays green, and existing goldens change only by new warning lines.

## Implementation Notes

- `debugger/rewrite/effects.go`: the `effects` table (one `effectFunc` per research row, evidence cited inline), `inProcessCaches`/`localRateLimits`, the generic list search (any `processors` list, the own-list parents `try`/`catch`/`for_each`/`processors`, and `tools[]`, whose warnings start "when the model calls the tool …"), and `warnEffects`, which also does the D9 state-split check. Reachability (`reached`) follows `resource`/`cache`/`rate_limit` string fields and `branch_resources` through `processor_resources` to a fixed point.
- The pass runs in `Rewrite` before any list is wrapped (the wrap replaces lists in place), into a separate walker; its warnings are appended after all the others.
- Addresses and DSNs in warning texts are redacted (userinfo and `password=`/`pwd=` parameters dropped); `azure_cosmosdb` names only `endpoint`.
- `redis` clears only a known read command (an allowlist), so an unlisted command warns.
- `javascript`'s `file` field is matched as written: the rewrite has no base directory and reads no script files, so a `v0_fetch` inside a script file is not detected.
- Nested output lists and `output_resources` are searched only under `output: real` (in dry-run and none they don't run).

## Plan Change Log

## Review Triage Log

Pass 1 (quick): high 0, medium 3, low 2, false 0, maybe-false 0. All 5 patched.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | `redact` treats any `@` as the end of userinfo, so `http://api/users/${! @id }` is shown as `"http://id }"` (the host is lost) | medium | patch | Userinfo only inside the authority (after `://`, before the first `/`, `?` or `#`); TestRedact cases for `@` in the path, query and an interpolation |
| 2 | `passwordParam` misses `sslpassword=` and masks a quoted value only in part | medium | patch | Any key ending in `password`/`pwd`, case-insensitive; quoted values masked whole; tests |
| 3 | A `processor_resources` entry reached only from a list that doesn't run (a nested output list under dry-run) warns "runs for real", contradicting "not run in dry-run" | medium | patch | Warned only when reached from something that runs (the step lists, the input tree, the output tree under real); unreferenced entries aren't warned; tests in both modes |
| 4 | A state split through a `multilevel` cache with in-process levels isn't reported | low | patch | A reference to a multilevel cache counts for its levels (cycle-safe); the split is reported once per level; test |
| 5 | The `effects` local shadows the package-level `effects` table | low | patch | Renamed to `effectWarn` |

The orchestrator added a README paragraph on the processor and state-split warnings.

## Verification

**Commands:**
- `go test -count=1 ./debugger/rewrite/ ./test/equivalence/` -- expected: PASS.
- `git diff debugger/rewrite/testdata` -- expected: only added warning lines in existing goldens.
- `task ci` -- expected: exit 0.

Run 2026-10-29 by the orchestrator after the review patches: `task ci` exit 0; no existing golden changed.
