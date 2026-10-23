# Review: architecture-rpcn-debugger spine, E2 reconciliation (reconcile + rubric)

- Reviewed: `architecture-rpcn-debugger.md` (updated 2026-10-22, working tree on top of `5206563`) and the last 18 `.memlog.md` entries, against `epic-tracer/epic-tracer-retrospective.md` (R1, A6, A7) and the code in `debugger/`, `vscode/src/`, `test/equivalence/`, `testdata/equivalence/`.
- Stance: independent. The spine was not edited. The code is taken as the truth for as-built facts.
- Severity: high (units will likely diverge, or the output/safety contract is silently broken), medium (plausible divergence, a rule that contradicts the code, or an unenforceable rule), low (clarity, traceability).

## Verdict

The update landed nearly all of R1 accurately, and the S1/S2 fixes are described correctly. One R1 item is missing (the license is not built), a few landed with internal contradictions (AD-6 vs AD-7 on strict, AD-10's repeat-label ownership), and the update left one real seam unruled: nothing binds the LineMap/Translator the Session keeps to the configs the engine actually runs (retro S6). One stream-level section (`buffer`) is dropped silently, which breaks AD-8 with no warning and no rule. None of this needs a redesign; each fix is a sentence, a row or a Binds entry.

---

## Part 1: Reconciliation (R1 and A6/A7 → spine)

| R1 item | Spine status | Code check | Notes |
| --- | --- | --- | --- |
| AD-3 `core.Error.Inline` (2.12) | Landed (AD-3) | `core/errors.go:89-93` matches | OK |
| AD-3 `STRICT_REJECTED` is an outcome, not a failure (2.14) | Landed (AD-3) | `errors.go:15-18, 46-50`; `engine.go:262-268` | OK. Detection matches benthos internal text, see F12 |
| AD-3 `MessagesSkipped`/`AttemptStarted` gained fields (2.7) | Landed via "a field is not a spine change" | `protocol.go` MessagesSkipped(UserPath, File, Line, Of), AttemptStarted(UserPath) | OK |
| AD-3 batch numbering counted in both Session and DAP (2.8) | Landed as `[ASSUMPTION]`, rule says frontends don't count | `dap/server.go:69, 459` counts; `core.Delivered` has no `Batch` | **Rule contradicts code and the owner is untracked** (F6) |
| AD-6 interpolated `retry.parallel` opaque | Landed | `walk.go:290-293` | OK |
| AD-6 resources under unknown parents unwrapped | Landed ("concurrent or unknown parent") | `walk.go:370, 461-462` | OK |
| AD-6 undefined `resource:` rejected (2.13) | Landed | `refs.go`, `rewrite.go:149-151` | OK |
| AD-6 interpolated strict (2.5, fixed by S2) | Landed | `engine.go:159-192` (`ConfigQuerier`, session lookup, `PIPELINE_INVALID` at `error_handling.strict`); `SetStrict(r.strict)` and the STRICT_REJECTED gate use the resolved value | OK. But AD-6 still says "every stepper is written with `strict: true`", which contradicts AD-7's `catch` row (F4) |
| AD-6/AD-21 composite outputs not run in dry-run (A7a) | Landed (AD-6, AD-21, Deferred row) | `rewrite.go:180` `warnNestedLists(out, "output", "is not run in dry-run")` | OK in the spine. The A7a "append a deferred-work candidate" was not done: `deferred-work.md` has no `fan_out` entry (outside the spine, but A7a is not closed) |
| AD-7 `catch` written `strict: false` (2.7); "add a column" | Landed inside the Mode cell, not as a column | `walk.go:68, 222-225` | Acceptable. The repeat kind per parent is still not in the table (F3) |
| AD-8 time budget narrowed to `retry` (A7b) | Landed | `core/session.go:733-742`, `frames.go:18-20` | OK. The warning is a Session behaviour, but AD-8 doesn't bind U-CORE (F8) |
| AD-8 corpus layout and same-batch rule (2.9) | Landed (AD-8, Conventions) | `testdata/equivalence/*/{pipeline.yaml, expect.yaml[, input.json]}` | OK. "Covers every row of AD-7" is not true as built (F8) |
| AD-10 Step Over at an entry's end; top-level "parent's next stop" (2.6) | Landed | n/a | OK |
| AD-11/CAP-4 "internal" is a child node, not a scope | Landed | `dap/variables.go:27-33` (`kindMetadata` → `kindInternal`) | OK |
| AD-12 type/request/name fill; yaml breakpoints; `rpcnDelivered` event | Landed | `vscode/src/launchConfig.ts:26-30`; `package.json` `breakpoints: [{language: yaml}]`; `server.go:440` | OK |
| AD-15 two logger instances; DAP gets Warn+; Session Output bypasses slog | Mostly landed | `serve.go:55-56` (`dapLog` without frontend, `engineLog` with); `logging.go` | Two loggers on one handler type isn't stated, and neither is the consequence that Session warnings never reach stderr. Low |
| AD-17 Session's `Enter` forks/exits; root thread at ConfigurationDone | Landed | `session.go:566-573, 324-356` | OK. `core.Entry` has no `Concurrent` field yet, so "forks when the config has `concurrent: true`" is target-only (F3) |
| AD-22 dotenv grammar (2.4) | Landed | `core/dotenv.go` | Code is richer (double-quote escapes, trailing ` # comment` on unquoted values, BOM, later line wins). Not a divergence risk (one owner). OK |
| AD-22 license not implemented; `LICENSE_REQUIRED` never raised | **Missing** | no `StoreCustomLicenseBytes` anywhere; `CodeLicenseRequired` only in `errors.go` | F5 |
| Unspecified limits: `maxMessages` ≤ 1024; 32 frames / 64 batches / 64 stops | Landed (AD-4, Conventions) | `dap/launch.go:33`; `variables.go:37-44` | Who enforces the cap and what happens past a limit are not ruled (F9) |
| `updated:` date, memlog entries | Landed | frontmatter `updated: '2026-10-22'`; memlog has the run | OK |

Retro items outside R1 that the update could have taken but didn't: the S6 seam (F1) and the "translation happens in 4 places" duplication against AD-6's "single handler" wording (F4).

---

## Part 2: Rubric findings

### F1 (high): Nothing binds the launch's LineMap/Translator to the configs the engine runs. AD-4, AD-6, AD-16 (retro S6, A5)

- **As built:** `Engine.Check` loads and rewrites the file and returns `Lines`/`Translator` (`engine.go:80-99`); the Session keeps them (`session.go:301`). `Engine.Start` calls `a.load` again (`engine.go:104`) and builds the streams from a second rewrite. The log handler translates through the Session's (first) translator (`logging.go:92`). An edit between launch and `configurationDone`, which this user does routinely, gives stepper ids that don't match the LineMap, stops at wrong lines, and wrong user paths in logs.
- **Why it's a spine gap:** two units (U-CORE keeps the maps, U-STEP runs the streams) each read the file, and AD-16's "LineMap keyed by `(stepperId, childIndex)`, unique within one rewrite" silently assumes one rewrite. AD-20 even says Restart re-reads, so a builder can reasonably think re-reading is normal.
- **Fix:** add to AD-6 (or AD-4): "One rewrite per launch and per Restart. `Engine.Check` produces the Plan; `Start` runs that same Plan and never reads the pipeline file. The LineMap, the Translator and every stream config of one run come from one Plan." Bind U-CORE and U-STEP. A5 then implements it.

### F2 (medium-high): `buffer` (and any other stream section) is dropped silently. AD-4, AD-5, AD-8

- **As built:** only `input` (without processors), the steppers and the `*_resources` sections reach the engine (`rewrite.go:197-212`). `buffer` exists in benthos v4.81's stream config (`internal/stream/config.go:15`) and is dropped with no warning; `grep buffer debugger/` finds nothing outside tests. The AD-8 reference keeps "the rest of the config" (`test/equivalence/equivalence.go:113-119`), so a case with a `buffer` would fail, but no case has one.
- **Why it matters:** a `memory` buffer with a `batch_policy` or a `system_window` buffer changes batch boundaries before `pipeline.processors`. Dry-run then delivers different batches from production with no notice, which is what AD-8 and AD-21's batching warning exist to prevent.
- **Also:** AD-4(b) says "Shared top-level sections carry over (`*_resources`, `error_handling`)". `error_handling` doesn't carry over: only `strict` exists in v4.81 (`ErrorHandlingConfig{Strict}`), and it is applied by `SetStrict` and the stepper `strict` fields.
- **Fix:** in AD-5, replace the four-item drop list with a closed list of what reaches the engine (input, the steppers, `*_resources`, the sink; `error_handling.strict` via `SetStrict`). Decide `buffer`: warn like output `batching` (AD-21), or reject. Add a corpus case either way.

### F3 (medium): AD-10's repeat-label ownership contradicts the code. AD-10, AD-16, AD-17

- **Spine:** "`repeatLabel` … is computed by the stepper from the per-entry counter in its parent frame … The Session displays it and never computes it." The memlog AD-16 entry says "the stepper computes repeatLabel from repeat and its per-entry counters".
- **As built:** the stepper passes `Entry.Label func(Site) string` plus `Entry.Site`. The **Session** owns the per-call-site counters (`pf.sites`, `siteCount`, and the `MarkCall` message count) and calls `Label` once inside `Enter` (`frames.go:27-43`, `session.go:566-600`). That is the right design under AD-3 (the Session owns all debug state), but the spine describes the opposite split.
- **Also:** which parent gets which repeat kind (`for_each`/`try` → msg, `retry` → attempt, `while` → loop, `group_by` → group, `catch`/`branch`/`switch` → none) lives only in `walk.go:19-24, 63-71`. `catch` is entered once per caught message but has no label. A second walker or the E4 concurrent rows could pick differently.
- **Also:** AD-17's "forks when the config has `concurrent: true`" can't be followed through today's port: `core.Entry` has no `Concurrent` field, and the rewrite always writes `concurrent: false` (`walk.go:231`). That's fine as target state, but say so.
- **Fix:** reword AD-10: "The stepper supplies the label format for its repeat kind; the Session owns the per-call-site counters and computes the label once, in `Enter`. Frontends only display it." Add a Repeat column to the AD-7 table. In AD-17, note that `Entry` gains `Concurrent` with epic-concurrency (a field, not a spine change).

### F4 (medium): AD-6 contradicts AD-7, and the code, in three places. AD-6

1. "If `error_handling.strict` is true, **every** stepper is written with `strict: true`" vs AD-7 "`catch`: written `strict: false`" (`walk.go:222-225`). Fix: "every stepper except `catch`'s (AD-7)".
2. "What it wraps" lists `workflow.branches.*` and `parallel`, but they are opaque and unwrapped until epic-concurrency (`walk.go:300-301`). AD-7 says so, AD-6 doesn't. Fix: mark them "(target; opaque until epic-concurrency, AD-7)".
3. "The single slog handler … translates every engine path … before anything reaches a frontend or stderr." As built, translation also happens outside the handler: in the Session for launch-error text (`core/translate.go:19`, `session.go:296`), in the engine for stepper names and lint positions (`engine.go:212`, the `Positions` map). Those are errors, not log records, so the rule as written doesn't cover them, and a new error path can skip translation without breaking any rule. Fix: "Every engine path or Bloblang line shown to the user goes through the one `Translate` from the run's Plan: the slog handler for logs, the Session for `core.Error` text."

### F5 (medium): AD-22's license clause reads as built, and it names the wrong unit. AD-22 (R1 item missing)

- No code calls `StoreCustomLicenseBytes`, and `LICENSE_REQUIRED` is never raised (retro R1, verified). The spine doesn't record that the clause is unbuilt or that epic-local-release owns it.
- "Applied … on every stream **the Session** builds": streams are built by U-STEP (`engine.go` `buildCapture`, `buildStep`; Check builds none on purpose). The Session builds only the env lookup.
- Fix: "U-STEP applies it to both streams `Engine.Start` builds (capture and step), after `Build()` and before `Run()`. Not built yet: epic-local-release." Make sure AD-22's Binds include U-STEP (they do) and that the Unit map lists AD-22 under U-STEP (it doesn't, F7).

### F6 (medium): The AD-3 batch-number rule contradicts the code, and its owner exists only in the spine. AD-3, AD-21

- The rule says "frontends display it and don't count", but `dap/server.go` counts (`delivered atomic.Int64`), and the Session counts separately for "Nothing was delivered". The `[ASSUMPTION]` sends the fix to epic-safe-against-real-systems' refactor sweep, but neither `epic-safe-against-real-systems.md` nor `deferred-work.md` mentions it. A ruling that lives only in the spine won't be scheduled. Restart (E-edit-restart-replay) must also reset it, and that is untracked too (retro A8).
- Fix: append a `deferred-work.md` entry (owner: epic-safe refactor sweep; `core.Delivered` gains `Batch`, 1-based, reset on Restart; DAP renders it). Until it lands, mark the rule "target; as built DAP counts" so it ratifies the code rather than contradicting it.

### F7 (medium): The Unit → Architecture Map disagrees with the ADs' Binds

A builder who reads only their unit's row misses ADs that bind them. Derived from each AD's Binds, the map is missing:

| Unit | ADs that bind it but are missing from its row |
| --- | --- |
| U-CORE | AD-1, AD-2, AD-4, AD-13, AD-15, AD-16, AD-21, AD-22 (the dotenv parser and env lookup live in `core`) |
| U-REWRITE | AD-1, AD-2, AD-8, AD-13, AD-15, AD-18, AD-21, AD-22 |
| U-STEP | AD-1, AD-3, AD-5, AD-6, AD-10, AD-11, AD-15, AD-16, AD-17, AD-20, AD-22 |
| U-BLOBL | AD-1, AD-2, AD-3, AD-6, AD-10, AD-11, AD-18, AD-19 |
| U-DAP | AD-1, AD-2, AD-5, AD-6, AD-9, AD-10, AD-11, AD-13, AD-14, AD-17, AD-20, AD-22 |
| U-EXT | AD-20 |
| U-REL | AD-21 |

Fix: generate the map from Binds (or label it "primary ADs" and add "plus every AD whose Binds name the unit").

### F8 (medium): AD-8 doesn't bind the unit that implements part of it, defines its reference loosely, and the corpus doesn't meet its own rule

- The time-budget warning is emitted by the Session (`session.go:733-742`, `InRetry` in `frames.go`), but AD-8 binds U-STEP, U-REWRITE and U-REL only. Add U-CORE.
- "Equal to the same pipeline run without the rewrite" is ambiguous now that composite-output processors don't run. As built, the reference replaces only the output plugin with a collector, keeping its `label` and `processors` and running the rest as written (`equivalence.go:113-119, 239-258`). That is what makes AD-21's "dry-run delivers what reaches output" comparable. Write it into AD-8.
- "The corpus covers every row of AD-7": there is no `workflow` case and no `retry` with `parallel: true` (or interpolated) case. `opaque-parallel` covers only `parallel`. There is also no composite-output case to pin down the A7a decision. Add those cases, or say the opaque rows are covered by `opaque-parallel` alone.

### F9 (low): The `maxMessages` cap and DAP limits say what, not who or what happens past them. AD-4, Conventions

- The 1024 cap is enforced only by `dap.LaunchConfig`'s `max` tag. `core.Launch.MaxMessages` and `StartOptions.MaxMessages` say "0 means no limit". A second frontend (MCP/TUI, Deferred) could step unlimited batches while the Variables encoding assumes 1024. Say "the Session rejects `MaxMessages` outside 1..1024" or "each frontend's launch schema enforces it".
- Fan-out past 64 batches or 1024 messages per stop is silently unexpandable (retro S3, A4). Add the overflow behaviour to the Conventions row ("beyond a limit, U-DAP shows a 'N more not expandable' node").

### F10 (low): AD-23 omits the machine scope of `rpcnDebug.binaryPath`, and the as-built MVP has no bundled fallback

- `package.json` sets `"scope": "machine"`, so a cloned workspace can't point the extension at an arbitrary executable. That is a security property; it belongs in AD-23's Rule and Prevents.
- AD-23's MVP says "the setting, then the binary bundled in a locally built `.vsix`". As built, `binaryPath.ts` throws when the setting is empty, with no bundled fallback. Mark the bundled step as not built yet (epic-local-release).

### F11 (low-medium): Build-status markers are inconsistent, and one gap is a safety gap

Some unbuilt parts are marked (launch keys "Built so far", AD-9's `continued` event, AD-7's concurrent rows, AD-23 MVP). Others read as built but aren't:
- AD-5 consumer-group override and input classes (epic-safe); no `consumer_group` handling exists in `debugger/`;
- AD-14 version fields in `initialize` (`server.go` sets only `SupportsConfigurationDoneRequest`);
- AD-20 `supportsRestartRequest`;
- AD-1 RPC plugins;
- AD-22 license (F5);
- AD-16 `concurrent` (always `false`).

The AD-5 one matters: a reader can conclude that debugging a Kafka pipeline today uses `rpcn-debug-<user>`, but as built it joins the production consumer group, and the rebalance hits production consumers. Fix: add a one-line "Built: E2 / Target: <epic>" note to each AD with unbuilt parts, and have the extension or launch warn on Kafka-family inputs until E3 lands (or record that risk under Open Questions).

### F12 (low): `STRICT_REJECTED` depends on benthos internal error text. AD-2, AD-14

`engine.go:273` matches `"rejected due to failed processing"` from benthos `internal/component/output/async_writer.go`, because the error type is internal. It doesn't break AD-2 (no import), but it is an unstated coupling to engine internals. Add it to AD-14's Connect-bump gate: the `strict` corpus case must assert the STRICT_REJECTED note, so a reworded message fails CI instead of turning a note into "The debug run failed".

---

## Part 3: Checklist

| Criterion | Result |
| --- | --- |
| Fixes the real divergence points for the units and misses none | **Mostly.** It misses the one-Plan-per-launch seam (F1), the stream-section allow-list (F2) and the repeat-kind-per-parent table (F3) |
| Every AD's Rule is enforceable and prevents its stated divergence | **Mostly.** AD-6's translation rule doesn't cover error paths (F4). AD-10's ownership sentence can't be followed as written (F3). Layering ("`core` imports only stdlib, `dap` only `core`") still has no gate beyond AD-2's depguard rule (retro A12). The diagram's claim is unenforced, so name A12 in the CI-lint convention |
| Nothing under Deferred could let two units diverge | **Yes** for the Deferred table. The `[ASSUMPTION]` in AD-3 is effectively deferred work with no ticket (F6) |
| Ratifies rather than contradicts the existing codebase | **Partly.** It contradicts the code in AD-3 batch numbering (F6), AD-6 strict wording and wrapped-list list (F4), AD-10 label ownership (F3), AD-4(b) `error_handling` carry-over (F2), and AD-22 "the Session builds" (F5). The unbuilt-but-unmarked rules are in F11 |
| Every dimension is decided, deferred or an open question | **Gaps:** `buffer` and other stream-level sections (F2); behaviour past the DAP limits (F9); behaviour when the capture input never yields (no timeout; only Pause/Terminate end it; decide or defer); the interim safety of Kafka inputs before epic-safe (F11) |

## Spot-checks that confirmed the spine (no finding)

- AD-3 command and event sets match `core/protocol.go` exactly. The registry is 1001-1010 as listed.
- AD-4 S1 fix: a separate `capEnv` clone with `nopArriver`, which pushes `core.FrameRef{}` and never stops (`engine.go:139-150, 318-332`). `NOTHING_CAPTURED` is raised when the input closes first (`capture.go`).
- AD-9 Arriver port (`Captured, Enter, Skip, Arrive, Deliver`) matches `core/port.go`. Only the `continue` response carries `allThreadsContinued` (`server.go:199-212`).
- AD-11 Snapshot JSON fields match `core/snapshot.go`.
- AD-12 the extension adds only `pipeline`, `cwd`, and `type`/`request`/`name` when all three are absent (`launchConfig.ts`).
- AD-15 `forbidigo`/`depguard` rules are in `.golangci.yml`. The handler writes Info+ to stderr and engine Warn+ to the frontend.
- AD-16 the stepper fields `id, parent, mode, strict, concurrent, repeat, max_attempts, lines, processors` match `engine/stepper.go:35-45`. The spine omits `processors` (trivial).
- AD-17 `CTX_LOST` is logged by the stepper (`stepper.go:143`). The root thread is named `batch`.
