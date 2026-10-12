# Review: architecture-rpcn-debugger spine (reconcile + rubric)

- Reviewed: `architecture-rpcn-debugger.md` (draft, 2026-10-08) against `.memlog.md`
- Reviewer stance: independent, adversarial; the spine was not edited
- Severity scale: critical (units will certainly diverge or production harm is likely), high (likely divergence or a safety/security gap), medium (plausible divergence or an unenforceable rule), low (clarity, traceability)

## Verdict

The spine is strong on the engine-facing core (public API only, capture-then-step, stepper modes, pause gate, Snapshot, stdout discipline) and almost every memlog decision landed. It is not yet a complete consistency contract. One memlog constraint is contradicted (`catch` per message), the capture/step boundary leaves input- and output-level processors ambiguous between U-REWRITE and U-STEP, the production-safety rule only covers the Kafka family, and the security/secrets dimension of the operational envelope is silent. One Deferred item (binary distribution) can let U-EXT and U-REL diverge. None of this needs a redesign. Each fix is a rule or a table row.

---

## Part 1: Reconciliation (memlog -> spine)

| # | Memlog item | Spine status | Severity | Concerns | Suggested fix |
| --- | --- | --- | --- | --- | --- |
| R1 | "try/catch/for_each/parallel/retry run once per message" | **Contradicted.** The AD-7 table puts `catch` in the "once per batch" row, next to `pipeline`/`branch`. | high | AD-7, AD-10 (frame label `[msg i/n]`), AD-8 | Move `catch` to the per-message row (engine: `ExecuteCatchAll` per message). Add an AD-8 equivalence case where a 3-message batch has 1 errored message inside `catch`. Assert the frame labels in a stepping test, not only the output. |
| R2 | Stepper modes are exactly `all` (ExecuteAll) and `try` (ExecuteTryAll) | Weakened and muddled. The `catch` row cites `ExecuteCatchAll` with mode `all`, "gated by its parent". It's unclear whether a third mode exists. | medium | AD-7 | Say explicitly: "two modes only. `catch` children run in mode `all`, and the `catch` processor itself selects errored messages and clears errors." Or add a `catch` mode row if the spike actually needed one. |
| R3 | Hexagonal Sink port = real \| dry-run \| **none** | `none` dropped. AD-5 and the launch keys know only dry-run and `real`. The allowed values of the `output` key are never listed. | medium | AD-2/AD-3 ports, AD-5, Conventions (launch keys) | Enumerate `output: "dry-run" (default) \| "real" \| "none"` in AD-5. State what `none` does (discard, show nothing past the last processor). |
| R4 | AD-1 "Binds: all Go units" | Weakened. The spine's AD-1 binds U-CORE, U-STEP, U-DAP, U-REL and omits U-REWRITE and U-BLOBL. | low | AD-1 | Restore "all Go units". It matters for the semver rule on public API surface. |
| R5 | `let` vars read via `Message.BloblangQuery`, because `BloblangQueryValue` loses the message context | Partly landed. AD-13 names `BloblangQuery` for evaluations, but the prefix-replay probe rule ("append `root = $name`") doesn't say which call, and the trap isn't recorded. | low | AD-13 | Add: "All tracer probes use `Message.BloblangQuery`. `BloblangQueryValue` is forbidden (it drops the message context: `meta`, `@`, `content()`)." |
| R6 | Prefix-replay cost O(n^2), acceptable "for one batch" | Dropped. There's no bound, so a U-BLOBL implementer could apply it to a 500-line mapping x 10 messages x nested loops. | low | AD-13, AD-4 | Record the bound: tracing runs lazily, only on Step In and only for the paused message. Optionally add a statement-count cap with a "tracing disabled" notice. |
| R7 | `__`-prefixed metadata is "hidden or grouped" | Weakened to a data flag (`internal: true`). Which unit hides or groups it isn't decided. | low | AD-11, U-DAP | Add to U-DAP's rules: internal metadata goes in a collapsed `internal` Variables scope. The core never filters it. |
| R8 | Assumption: packaging C taken without the user's answer; "revisit if users are mostly stock-only and upstream is the goal" | Dropped. The revisit trigger isn't in the spine. | low | Open Questions | Add an Open Question: "User mix (stock / RPC / compiled-in) and upstream goal: revisit AD-1 if upstream `rpk connect debug` becomes the primary channel." |
| R9 | Assumption: hexagonal taken without stated roadmap priorities | Dropped (same pattern). | low | Open Questions | Add a revisit trigger, e.g. "if no second frontend or replay is planned within N releases, the port layer may be simplified". |
| R10 | Versions verified 2026-10-08: the memlog lists exactly what was checked | `@vscode/debugadapter-testsupport 1.68.0` appears in the Stack but wasn't in the verification entry. | low | Stack | Verify it, or mark it "unverified" the same way as the license item. |
| R11 | `@types/vscode 1.140.0` verified as the pinned version | **Self-contradiction.** The Stack says `@types/vscode (= engines.vscode minimum) 1.140.0`, which is a decision, but Open Questions lists "Minimum supported VSCode version" as undecided. | medium | Stack, Open Questions | Pick one. Either decide `engines.vscode: ^1.140.0` and remove the question, or change the Stack row to "@types/vscode tracks engines.vscode (TBD)". |
| R12 | RPC plugins via `DiscoverAndRegisterPlugins(fs, env, manifests)` | Landed, but the memlog's signature exposes a lifetime issue the spine doesn't resolve (see F7). | medium | AD-1 | See F7. |
| R13 | License "mirrors the stock CLI (--license / REDPANDA_LICENSE)", unverified | Landed as an Open Question. OK. | - | - | - |
| R14 | Capture/nack safety proven **with the redpanda input** | Generalised without evidence. AD-4/AD-5 state "never ack" as universally safe (see F3). | high | AD-4, AD-5 | See F3. |

All other memlog items landed faithfully: public API only, stepper via `NewProcessorListField`, `ParseYAML` ban, plugin-name regex, the wrap list (and extended), the pause gate, maxMessages truncation, list-of-batches, retry attempt counting, display rules, line offsets, `schema.Standard().Environment()`, in-process linking, BloblangTracer port + view semantics + `unassigned` root + non-deterministic labelling (extended with `timestamp_unix*`, `ksuid`), decisions 3-10, the Go 1.26.x pin, yaml.v3, and the go-dap maintenance risk.

---

## Part 2: Rubric findings

### F1 - Input- and output-level processors straddle the capture/step boundary [high]

- **Concerns:** AD-4 vs AD-6, U-REWRITE and U-STEP
- **Problem:** AD-6 wraps the processor lists of `input` and `output`. AD-4 builds the capture stream with "only the input" and the step stream from `AddBatchProducerFunc`, which has no input. So it's undecided whether `input.processors` (and `input.broker.inputs[].processors`, `batching.processors`) run in the capture stream. If they run wrapped there, the stepper pauses while the input is live, which breaks the "input stopped before stepping" guarantee and ack deadlines. If they run unwrapped there, they can't be stepped. If they're moved, the step stream has to prepend them to `pipeline.processors`. The output side is the same: with the dry-run sink, someone has to decide whether `output.processors` and the output batching policy still run before the sink. U-REWRITE and U-STEP will each pick a different answer.
- **Fix:** Add a rule to AD-4/AD-6: "The rewrite emits two configs: capture config = input with *all* processor lists stripped (incl. broker children and batching processors). Step config = `[input-level processors] -> pipeline -> [output-level processors]`, each list wrapped, with user paths preserved. The output batching policy is applied by the Sink adapter." Add AD-8 equivalence cases for input-level and output-level processors.

### F2 - stdio transport collides with `stdin` inputs and `stdout` outputs [high]

- **Concerns:** AD-15, AD-5, U-STEP, U-DAP
- **Problem:** AD-15 bans `fmt.Print*` in `debugger/...` but says nothing about pipeline components. A pipeline with `input: stdin` would read the DAP stream. With `output: real` and `output: stdout` (or `drop_on`/`broker` wrapping stdout), the pipeline writes into it. Lint can't catch these, because they're configuration.
- **Fix:** Extend AD-15: "When `Serve` runs over stdio, the rewrite rejects `stdin` inputs (session error with a hint to use `replay` or a file) and redirects `stdout`/`stderr` outputs to the dry-run sink even with `output: real`, with a warning." Add a test for each.

### F3 - The "production safe" rule only covers Kafka-family inputs [high]

- **Concerns:** AD-4, AD-5 (Prevents claims more than the Rule delivers)
- **Problem:** Not acking was proven only for `redpanda`. For destructive-read or competing-consumer inputs, never acking isn't harmless: `http_server`, `socket_server`, `stdin`, `nats` (core), `mqtt` QoS 0 and `redis_list`/`redis_streams` pop or claim semantics can lose or steal messages. `amqp_0_9`/`aws_sqs`/`gcp_pubsub`/`nats_jetstream` durable consumers compete with production and change redelivery counts or visibility, and can trip dead-letter thresholds. The `consumer_group` override only exists for `kafka`, `kafka_franz`, `redpanda` and `redpanda_common`. Units will diverge on what to do with, say, an `aws_sqs` input.
- **Fix:** Add a per-input safety table to AD-5 with three classes: (a) **replay-safe** (Kafka family with the group override, `file`, `generate`, `aws_s3` read): allowed. (b) **side-effecting** (queues, durable consumers): allowed only with `"allowSideEffects": true` and a warning naming the effect. (c) **destructive / transport-conflicting** (`stdin`, `http_server`, sockets): rejected or replay-only. Unknown inputs default to (b). Add an integration test (testcontainers Redpanda) asserting that no offset is committed for the debug group. CI runs it (see F12).

### F4 - Security and secrets handling is silent [high]

- **Concerns:** the operational envelope (rubric dimension), Conventions (launch keys `env`, `license`), AD-11 replay files, AD-13 evaluate
- **Problems (none decided, deferred or asked):**
  1. Launch keys `env` and `license` invite credentials into `launch.json`, which is usually committed.
  2. Replay files `.rpcn-debug/captures/*.json` hold **production message content and metadata** (PII, tokens in headers) inside the workspace. There's no gitignore, no file permissions, no retention rule and no opt-in.
  3. `evaluate`/Watch can run `env("SECRET")` or `file(...)`, and engine logs forwarded to DAP `output` can echo config values containing secrets. There's no redaction stance.
  4. It's undecided how `${VAR}` interpolation in the pipeline YAML resolves: process env inherited from VSCode, launch `env` merged, or `StreamBuilder.SetEnvVarLookupFunc`. U-DAP (LaunchConfig) and U-STEP (stream builder) will diverge, and the rewrite (yaml.v3 on raw text) must not resolve them.
  5. The debug consumer group needs ACLs on the production cluster (group create/read). Nothing says the debugger uses the pipeline's own credentials unchanged.
- **Fix:** Add an AD "Secrets and captured data", with these points:
  - `env` values support `${env:NAME}` and `envFile`. `license` accepts only a file path or env reference, never a literal. The schema description says so.
  - Captures are written only on explicit "save capture", with mode 0600. The debugger writes `.rpcn-debug/.gitignore` containing `*` on first use.
  - `${VAR}` is resolved only by the engine through one `EnvLookup` built by `Session` from (process env, then `envFile`, then `env`), and the rewrite preserves interpolations verbatim.
  - Evaluate is unrestricted (it's a local dev tool), and this is documented.
  - Credentials and TLS come only from the pipeline config. The debugger adds none.

  At minimum, list these as Open Questions so that the dimension isn't silent.

### F5 - Deferred "binary distribution channel" can split U-EXT and U-REL [high]

- **Concerns:** the Deferred row "Debug-binary distribution channel", AD-12(c), AD-14
- **Problem:** The deferral says the channel "doesn't affect other units", but AD-12(c) commits U-EXT to the lookup order `binaryPath -> bundled -> download`. U-EXT needs the download URL scheme, artifact naming per OS/arch, checksum/signature verification, and which binary version matches which extension version. Downloading and executing an unsigned binary is also a supply-chain security decision. Separately, the extension-to-binary protocol compatibility (LaunchConfig schema evolution, custom fields) has no versioning rule. AD-14 covers only the Connect version.
- **Fix:** Pull the contract up into an AD and keep only the hosting choice deferred. The contract: (a) extension version X.Y.* downloads binary X.Y.* from `<releases>/vX.Y.Z/rpcn-debug_<os>_<arch>[.exe]`; (b) verify SHA-256 from a signed checksums file (cosign/minisign) before exec; (c) the binary reports `rpcnDebugVersion` + `launchSchemaVersion` in `initialize`, and the extension refuses a major mismatch. Pick the Marketplace and Open VSX publication decision, or defer it explicitly.

### F6 - Breakpoints and conditional breakpoints are undefined for batch-level stops [high]

- **Concerns:** AD-10, AD-13, U-CORE / U-DAP / U-BLOBL
- **Problem:** Stops are "before child processor i of a stepper", which is batch-level. Nothing says how a source line maps to a stop: which YAML lines are valid, how a line inside a processor's config block (not its first line) maps, and what DAP `verified`/adjusted-line semantics apply. Conditional breakpoints run "against the paused message" (AD-13), but a batch-level stop has N messages: is the condition any, all, or per message, and does it create a `[msg i/n]` frame? Hit counts and logpoints also need defining. U-DAP and U-CORE will each invent semantics.
- **Fix:** Add to AD-10:
  - A breakpoint on any line inside processor P's YAML span binds to "before P". It's reported `verified` with the line set to P's first line. Lines outside any processor are unverified.
  - In Bloblang, a breakpoint binds to the statement whose span contains the line.
  - A condition is evaluated per message and stops if **any** message matches, focusing that message in the Snapshot.
  - Logpoints are supported or not, decide which.

### F7 - Environment lifetime: stepper and RPC plugins registered into a caller-owned env [medium]

- **Concerns:** AD-1, AD-4, Restart command (AD-3)
- **Problem:** `Serve(env, ...)` receives a built environment, but `rpcPlugins` arrive later in the DAP `launch` request, and `rpcn_debug_stepper` must also be registered. Registering into the caller's env mutates it, and on `Restart` it registers twice (an error), or leaks plugins across sessions. U-STEP and U-DAP will handle this differently.
- **Fix:** Rule: "Per session, `Session` uses `env.Clone()`, registers `rpcn_debug_stepper` and the launch's RPC plugins on the clone, and discards the clone on terminate/restart. The caller's env is never mutated."

### F8 - The stepper-to-Session interaction and message ownership are underspecified [medium]

- **Concerns:** AD-3, AD-9, the commands `SetMessage` / `Restart`
- **Problem:** AD-3 says steppers "emit events and block". But whether a given stop position should pause depends on the step mode and breakpoints, which Session owns, so the stepper has to *ask*. The shape of that call (sync API vs event + gate) is the main U-CORE/U-STEP interface and isn't fixed. `SetMessage` changes a batch that the stepper holds in flight, and AD-3 says only Session changes debug state, with no rule for how the edited batch reaches the stepper. `Restart` could mean re-capture or re-step the Session's copy.
- **Fix:** Fix the port signature in AD-3, e.g. `Session.At(ctx, pos, batches) (batches, error)`. It blocks while paused and returns the (possibly `SetMessage`-edited) batches the stepper must continue with. Steppers call it at every stop. Define `Restart` as "re-step the captured batch from Session's copy, with no new capture" and add a separate `Recapture` command if needed.

### F9 - Pausing changes wall-clock-dependent behaviour, and AD-8 can't detect it [medium]

- **Concerns:** AD-8, AD-9, AD-7 (`retry`)
- **Problem:** AD-8 proves equivalence for an unpaused run only. Processors that depend on elapsed time behave differently when a human pauses inside them: `retry` with `max_elapsed_time`/backoff, `workflow`/`branch` timeouts, `rate_limit`, `cached` TTLs, and context deadlines in `http`. A paused `retry` can exhaust its budget and produce output the real run wouldn't. This is a silent divergence from the user's point of view.
- **Fix:** Add to AD-8/AD-9:
  - Equivalence is guaranteed only for zero-pause runs.
  - When a stop happens inside a time-budgeted parent (start with `retry`), the debugger emits a warning output event.
  - The equivalence harness also runs each case with scripted Step Over at every stop, and asserts the same output when the corpus has no time budgets.

### F10 - Launch contract misses multi-file configs and environment keys [medium]

- **Concerns:** Conventions (launch keys), AD-6 (`LineMap` has `file`), AD-12
- **Problem:** `LineMap` carries `file`, which implies multiple files. But the launch keys have only `pipeline`. Stock CLI users rely on `-r` resource files, `-t` templates, `--set` overrides and `--env-file`. Templates create processors whose child lists are an "unknown parent" until decided. Without keys, U-EXT's schema and U-DAP's `LaunchConfig` will add these ad hoc.
- **Fix:** Add `resources: string[]`, `templates: string[]`, `envFile`, and optionally `set: {path: value}` to the launch keys. State that the rewrite covers `processor_resources` in resource files with `file` set accordingly, and that template-expanded processors are opaque steps in v1. Add this to Deferred if postponed.

### F11 - The Unit -> Architecture Map disagrees with the Binds lines [medium]

- **Concerns:** the Unit -> Architecture Map
- **Problem:** A unit implementer reading their own row misses binding ADs:
  - U-DAP's row omits AD-9, AD-10, AD-11, AD-13 and AD-14, which all bind U-DAP.
  - U-STEP's row omits AD-3, AD-6, AD-9, AD-11 and AD-15.
  - U-CORE's row omits AD-13 and AD-15.
  - U-BLOBL's row omits AD-3, AD-10 and AD-11.
  - U-REWRITE's row omits AD-8.
  - U-REL's row omits AD-15.
  - U-EXT's row lists AD-5, but nothing in AD-5 says what U-EXT must do (presumably the schema defaults and warnings).
- **Fix:** Generate the map from the Binds lines (or the reverse) and make them identical. In AD-5, say what U-EXT does (schema defaults `output: "dry-run"`, `keepConsumerGroup: false`, with descriptions warning of the effects).

### F12 - CI and release envelope only partly decided [medium]

- **Concerns:** U-REL, AD-8, AD-14, the operational envelope
- **Problem:** CI is named only for equivalence (AD-8), schema check (AD-12) and lint (AD-15). The following are undecided: CI platform, the OS/arch build matrix (the CGO open question covers only part of it), whether the never-ack integration test (a real broker) runs in CI, how a Connect bump is triggered (Renovate/manual), the extension test job, and the release cadence relative to Connect releases. The lint tool for AD-15 isn't named, so "lint enforces it" isn't yet enforceable.
- **Fix:** Add a U-REL row or an AD:
  - GitHub Actions; matrix linux/darwin/windows x amd64/arm64.
  - `golangci-lint` with `forbidigo` for `fmt.Print*`/`os.Stdout`/`log.Print*`, and `depguard` for `internal/` imports (which makes AD-2 enforceable as well).
  - A testcontainers Redpanda job for AD-4/F3.
  - Renovate opens Connect bumps, and a bump merges only with AD-8 green.
  - Release on every Connect minor.

  Anything not decided goes to Deferred or Open Questions.

### F13 - Pipeline observability sections would run against production [medium]

- **Concerns:** AD-5, the operational envelope (observability)
- **Problem:** The user's config may contain `metrics` (e.g. Prometheus push gateway), `tracer` (OTLP to production collectors), `http` (port 4195), and a `logger` section. Nothing says whether the debug stream builder honours or strips them. A debug session could push metrics and traces into production dashboards or fail on port conflicts. The debugger's own observability (no telemetry from the extension?) is also unstated.
- **Fix:** Add to AD-5: "The rewrite drops `metrics`, `tracer`, `http` and `logger` from the user config. The engine gets a no-op metrics/tracer and the AD-15 logger. `"keepObservability": true` opts back in." Add a line in Conventions: "No telemetry is collected by the extension or the binary" (or decide otherwise).

### F14 - Snapshot is ambiguous under concurrency and lacks stable identity [medium]

- **Concerns:** AD-11, AD-9
- **Problem:** `Snapshot` has one `position` and one `batches`, but several threads can be inside steppers (AD-9). It's unclear whether a Snapshot is per frame/thread or global. Message `index` is a batch position that changes across `group_by` reordering, so diffs (webview, tests, `expected.json`) can't track a message. `skipped[]` (truncated? try-skipped?) and the allowed metadata `type` values are undefined. The memlog decision that tests share the Snapshot shape isn't reflected in the `testdata/equivalence/*/input.json, expected.json` convention.
- **Fix:**
  - "A Snapshot is per frame (`threadId`, `frameId`)."
  - Add a stable `id` assigned at capture and preserved through processors where the engine keeps the message, with new ids for messages that a processor creates.
  - Define `skipped[] = {id, reason: truncated|try_skipped|filtered}`.
  - Enumerate metadata `type` as `string|bytes|int|float|bool|structured`.
  - State that `input.json` is a capture Snapshot and `expected.json` a list of Snapshot batches.

### F15 - `mutation` has no "unassigned" root [medium]

- **Concerns:** AD-13
- **Problem:** "`root` is shown as unassigned until the first `root` assignment" is correct for `mapping`/`bloblang`. For `mutation`, root starts as the input message and is changed in place. U-BLOBL and U-DAP may apply the unassigned rule uniformly and show a misleading state.
- **Fix:** Qualify the rule: "For `mutation`, root starts as the input document. For `mapping`/`bloblang`/branch `request_map`, it starts unassigned. Branch `result_map` starts from the original message."

### F16 - Mapping line arithmetic breaks for non-literal scalars; engine lint lines are not mapped [low]

- **Concerns:** AD-6, Conventions (Lines)
- **Problem:** "Value start line + statement offset" holds only for literal block scalars (`|`, where content starts on the next line). Folded (`>`), quoted, or single-line flow scalars don't preserve line offsets. AD-6 translates *paths* in engine errors, but lint/parse errors on the rewritten YAML also carry *lines* in the rewritten document.
- **Fix:** Specify that statement lines are exact for `|`/`|-` block scalars (start = header line + 1). For other scalar styles, all statements map to the value's line, and statement stepping is still offered. Extend the AD-6 translation to "paths and lines".

### F17 - Multi-thread stepping semantics [low]

- **Concerns:** AD-9, AD-10
- **Problem:** "Only one thread is stopped at a time" doesn't say what happens when thread A is stepping (Next) and thread B reaches a breakpoint first. It also doesn't say what DAP `allThreadsStopped` reports, or whether Continue releases one thread or all.
- **Fix:** Use `allThreadsStopped: false`. Step commands apply to the requesting thread, and a breakpoint hit in another thread takes priority and is reported as a new `stopped` event. `Continue` resumes all threads.

### F18 - `rpcnConnectVersion` source for user-built mains [low]

- **Concerns:** AD-14
- **Problem:** Users who link the library into their own main may use a different Connect version than the release pin. Nothing says where the reported version comes from.
- **Fix:** "Reported from `runtime/debug.ReadBuildInfo()` for module `connect/v4`, never from a constant."

### F19 - `core.Error.Code` values and custom DAP request naming are open [low]

- **Concerns:** Conventions (Errors), AD-12 (custom DAP requests)
- **Problem:** Error codes aren't enumerated or namespaced, so U-EXT can't match on them. Custom DAP requests and fields (`rpcnConnectVersion`, future webview requests) have no naming rule.
- **Fix:** Use a code registry in `debugger/core/errors.go` with string codes (`capture_timeout`, `unsupported_input`, ...). Prefix custom DAP requests and fields with `rpcn/` / `rpcn`.

### F20 - Capture lifecycle edge cases [low]

- **Concerns:** AD-4
- **Problem:** No rule covers an input that yields no batch (wait forever?), a capture timeout, a disconnect while capturing, or how the input is stopped (timeout on `Stop`).
- **Fix:** Add a `captureTimeout` launch key (default e.g. 60 s, then error `capture_timeout`). On terminate, Session stops both streams with a bounded `Stop(ctx)`, and the nack is guaranteed before exit.

---

## Rubric scorecard

| Criterion | Result |
| --- | --- |
| Fixes the real divergence points for U-CORE..U-REL | Mostly. Gaps: F1, F6, F7, F8, F10, F14 |
| Every Rule is enforceable and prevents its divergence | Mostly. AD-5 (F3), AD-7 (R1), AD-8 (F9), AD-15 lint unnamed (F12) |
| Nothing Deferred lets units diverge | Fails on "binary distribution channel" (F5). Others OK |
| Operational envelope: deployment/environments | Decided (dev machine only). OS/arch matrix partial (F12) |
| ... release | Partial (AD-14). Versioning between extension and binary, channels, cadence missing (F5, F12) |
| ... CI | Partial (F12) |
| ... observability | Logging decided. Pipeline `metrics`/`tracer`/`http` and telemetry silent (F13) |
| ... security/secrets (env vars, credentials) | **Silent** (F4) |
