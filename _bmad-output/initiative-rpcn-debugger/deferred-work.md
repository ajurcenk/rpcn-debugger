- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-platform-baseline/story-repo-scaffold-task-runner-and-installable-skeleton-extension-plan.md`
  summary: Update CLAUDE.md now that the repo has a Go module, a Taskfile (`task ci`, CGO_ENABLED=0), tests, the vscode/ extension and git history.
  evidence: CLAUDE.md:7 still says the repository has no code, build system, tests, README or git history; the 1.1 diff added all of them.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-platform-baseline/story-dap-adapter-seed-debugger-serve-and-extension-wiring-plan.md`
  summary: debugger.Serve hard-wires its slog handler to os.Stderr; give Serve an Option for the log destination (or the AD-15 handler) so cmd/rpcn-debug's injected stderr and library callers' main packages receive server diagnostics.
  evidence: debugger/serve.go builds slog.NewTextHandler(os.Stderr, nil) while cmd/rpcn-debug run() takes a stderr writer; TestRunNoArgs cannot show server logs. The AD-15 handler (stderr plus DAP Output) lands in epic-tracer.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-tracer-bullet-f5-captures-stops-steps-and-delivers-plan.md`
  summary: The rewrite re-marshals `input` and `pipeline.processors` subtrees separately, so a YAML alias (`*name`) to an anchor defined elsewhere in the file becomes an unknown anchor; resolve aliases (deep-copy the target node) before encoding — for entry 2.5, which owns the rewrite.
  evidence: Reviewer reproduced it: `x: &m 'root = 2'` with `processors: [{mapping: *m}]` yields stepper YAML `- mapping: *m` and `yaml: unknown anchor 'm' referenced`, while Connect accepts the pipeline.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-contracts-closed-protocol-error-registry-snapshot-and-variab-plan.md`
  summary: `core.Error.Line`/`UserPath` are never set: rewrite errors flatten their line into message text ("line N: …") and serve.go wraps them as PIPELINE_INVALID text; have debugger/rewrite return core.Error with Line/UserPath — for entry 2.5, which owns the rewrite.
  evidence: rewrite.go builds `fmt.Errorf("line %d: ...", node.Line)`; serve.go loadPipeline uses core.NewError(CodePipelineInvalid, "%s: %v", path, err); no non-test code sets Error.Line or UserPath.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-contracts-closed-protocol-error-registry-snapshot-and-variab-plan.md`
  summary: Object/array metadata values are JSON-encoded, so nested []byte, time.Time and NaN/±Inf don't round-trip, an unknown Go type becomes `string` via fmt.Sprint, and an int64-overflowing json.Number is labelled float; make nested values lossless (a typed encoding) — for epic-edit-restart-replay, whose capture round-trip test must be byte-identical (AD-11).
  evidence: debugger/engine/snapshot.go metaEntry/jsonText/sanitize use json.Marshal for object/array; TestMetadataRoundTrip covers only JSON-native nested values. Scalars are already lossless.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-session-stepping-semantics-frames-and-threads-plan.md`
  summary: next/stepIn/stepOut resume every thread but tell the client only about the stepped one; emit a DAP `continued` event with `allThreadsContinued: true` (the Session never emits `core.Continued`) — for epic-concurrency, which introduces more than one thread.
  evidence: AD-9 says every resume is process-wide with allThreadsContinued; only the `continue` response carries the flag. Invisible with this epic's single root thread.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-rewrite-rejects-undefined-resource-references-plan.md`
  summary: The AD-7 child-list table (branch/while/retry → processors; try/catch/for_each → list; switch/group_by → cases[].processors) is hand-copied in walker.processor, childLists, refChecker.processor and the taint scan (scanProcessor/refs); unify it into one table so a new AD-7 parent can't be added to one walk and missed by the others — for the 2.10 refactor sweep.
  evidence: debugger/rewrite/walk.go and refs.go; the 2.13 review found the copies already drifting (opaque vs refs reference search).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-stepper-modes-nested-parents-and-ctx-propagation-plan.md`
  summary: Retry `[attempt i/max]` is inferred (previous entry failed, attempts remain, identical input JSON), so when max_elapsed_time moves retry to the next message and that message is byte-identical (common with generate), its first attempt continues the previous count; and within one while call, a child that splits the batch makes one loop show as several `[loop i]` entries. Both need an engine-side marker; revisit with an upstream hook or a different detection — label-only, output is unaffected.
  evidence: benthos processor_retry.go:254-258 (backoff.Stop moves to the next message), processor_while.go:159 (ExecuteAll over all batches per loop); 2.7 review findings 3 and 4.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-stepper-modes-nested-parents-and-ctx-propagation-plan.md`
  summary: Strict-mode skip has no end-to-end equivalence check (only the unit test against a hand port of ExecuteTryAll); cover it in the 2.9 corpus's strict-mode row.
  evidence: no strict sample under testdata/tracer/parents; strict-parents.yaml is only built in TestRewrittenConfigsBuild.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-delivery-at-rpcn-debug-sink-dry-run-plan.md`
  summary: Reset the delivered-batch counters (core.Session.delivered and the DAP server's numbering) when Restart re-steps the batch, so numbering starts at 1 and "Nothing was delivered" is reported per run — for epic-edit-restart-replay (AD-20).
  evidence: debugger/core/session.go `delivered` and debugger/dap/server.go `delivered atomic.Int64` are never reset; latent while Restart is NOT_SUPPORTED.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-equivalence-corpus-and-scripted-step-over-driver-plan.md`
  summary: Delivered — the strict-mode end-to-end equivalence item (from 2.7) is covered by the 2.9 corpus case `testdata/equivalence/strict/`, run three ways (Continue, Next, Step In) against the reference in TestEquivalence.
  evidence: test/equivalence/equivalence_test.go TestEquivalence/strict; the case exercises both strict behaviours (skip of later processors, output rejection).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-equivalence-corpus-and-scripted-step-over-driver-plan.md`
  summary: Under error_handling.strict, the output rejecting a failed message (normal strict behaviour) surfaces as "The debug run failed: deliver the batch: rejected due to failed processing: …" although the delivered batches match the reference; report it as an expected strict rejection (e.g. a console note naming the rejected messages) instead of a run failure — needs a user decision on wording, then a small entry.
  evidence: seen in the 2.9 strict corpus case (testdata/equivalence/strict); core.Session run reports the producer's error as RUN_FAILED output.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-strict-mode-output-rejection-is-not-a-run-failure-plan.md`
  summary: Under strict, STRICT_REJECTED is chosen when the producer's error text contains "rejected due to failed processing"; in a mixed batch benthos folds a write failure of the passing messages into the same batch.Error, whose Error() is the first (rejection) message, so a real write failure would be reported as the expected strict note — for epic-safe-against-real-systems, when `output: real` lets the sink's write fail (distinguish via the rejected count vs the batch, or the sink's own error record).
  evidence: benthos v4.81 internal/component/output/async_writer.go ~251-283 (rejectErr.Failed(i, werr)) and internal/batch/error.go:115; latent today because rpcn_debug_sink.WriteBatch always returns nil.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-refactor-sweep-plan.md`
  summary: Delivered — the CLAUDE.md update (from 1.1) was delivered by the 1.7 refactor sweep, which rewrote Project state, Layout and Commands to describe the code that exists; this 2.10 sweep refreshes them again for epic-tracer.
  evidence: commit 259fcd5 "docs: refactor sweep: CLAUDE.md describes the code that exists (1.7)"; CLAUDE.md no longer says the repository has no code.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-refactor-sweep-plan.md`
  summary: Delivered — the Serve log destination (from 1.4) was delivered by 2.4: the `debugger.WithLogOutput(w)` Option sends the stderr side of the one AD-15 handler (stderr plus DAP Output) to w.
  evidence: debugger/serve.go WithLogOutput; debugger/logging.go; commit 01c223a (2.4).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-refactor-sweep-plan.md`
  summary: Delivered — YAML aliases (from 2.1) were delivered by 2.5: the rewrite deep-copies each alias target (`resolve`) before re-encoding a subtree, so `*name` to an anchor elsewhere in the file no longer becomes an unknown anchor.
  evidence: debugger/rewrite/rewrite.go resolve; golden case debugger/rewrite/testdata/alias.yaml; commit b965994 (2.5).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-refactor-sweep-plan.md`
  summary: Delivered — `core.Error` Line/UserPath (from 2.2) were delivered by 2.12: launch errors carry the user's file line and user path, both for rewrite errors (`invalid`, since 2.5) and for engine lint and parse errors mapped back through the rewrite's position maps.
  evidence: debugger/rewrite/rewrite.go invalid; debugger/rewrite/positions.go; debugger/engine/positions.go; golden files print `line:`/`path:`; commit eb66937 (2.12).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-refactor-sweep-plan.md`
  summary: Delivered — the strict reject message (from 2.9) was delivered by 2.14: under error_handling.strict the output's rejection of failed messages is `STRICT_REJECTED`, reported as a Debug Console note instead of "The debug run failed".
  evidence: debugger/core/errors.go CodeStrictRejected; debugger/engine/engine.go; debugger/core/session.go; testdata/equivalence/strict/expect.yaml output; commit ba03f8e (2.14).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-refactor-sweep-plan.md`
  summary: Delivered — the AD-7 child-list table (from 2.13) is now one `parents` table in debugger/rewrite/walk.go with a shared `childLists` iterator used by the walker, refChecker and the taint scan.
  evidence: entry 2.10 (refactor sweep); every rewrite golden byte-identical before and after.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-vs-code-acceptance-run-plan.md`
  summary: Benthos's own strict-mode Warn ("Rejecting N of M message(s) for output 'rpcn_debug_sink' … (example error: failed assignment (line 1): …)") reaches the Debug Console with the internal sink name and an unshifted mid-message Bloblang line; have the AD-15 handler render the sink as "dry-run output" and shift the embedded `(line N)` — cosmetic, for a later logging touch-up or epic-safe-against-real-systems (which changes the output).
  evidence: seen in the 2.11 acceptance probe on testdata/tracer/acceptance-strict.yaml; debugger/logging.go shifts only the leading Bloblang `(line N):`.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/epic-tracer-retrospective.md`
  summary: Spec reconciliation for A6 — narrow AD-8's time-budget clause to `retry` (backoff, max_elapsed_time), the only time-budgeted parent the debugger can pause inside. Branch and workflow have no timeout field, workflow runs as one opaque step, and pausing near a rate limit changes only throttling, not output. User decision 2026-10-22; no code change.
  evidence: benthos v4.81 internal/impl/pure/processor_branch.go and processor_workflow.go (time used only for latency metrics); processor_rate_limit.go (a leaf, with only a `resource` field); the warning lives in debugger/core/session.go (InRetry).

- source_plan: `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md`
  summary: The Session owns the delivered-batch number (AD-3): add `Batch` to `core.Delivered` (1-based, reset on Restart), and have debugger/dap display it instead of counting in its own `delivered` counter. For epic-safe-against-real-systems' refactor sweep.
  evidence: debugger/dap/server.go `delivered atomic.Int64` numbers batches for the Debug Console line and rpcnDelivered; debugger/core/session.go `delivered` counts them for "Nothing was delivered". Two owners; Restart would have to reset both. Spine update 2026-10-22 (A6).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/epic-tracer-retrospective.md`
  summary: Candidate, no owner: step `fan_out` broker and `switch` output cases in dry-run, delivering one sink batch per case. Their routing is deterministic, unlike round_robin, greedy and fallback, which keep the not-run warning.
  evidence: user decision A7(a), 2026-10-22; spine AD-21 and Deferred table.

- source_plan: `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md`
  summary: Entry before epic-safe-against-real-systems: a launch warning when a `try` or `catch` list holds a stateful child (cache, dedupe, rate_limit, …), because the engine runs them processor-major while the debugger steps message-major (AD-7). Add a corpus case pinning the stateless case.
  evidence: benthos v4.81 internal/component/processor/execute.go ExecuteTryAll/ExecuteCatchAll (for each processor, for each message) against debugger/engine/stepper.go (all children per message). User decision 2026-10-22 (A6 gate).

- source_plan: `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md`
  summary: Entry before epic-safe-against-real-systems: warn when the top-level `buffer` section is dropped (production batch boundaries may differ, like output batching; AD-5), and add a corpus case.
  evidence: the rewrite keeps only input, the steppers and *_resources, so `buffer` is dropped silently today. User decision 2026-10-22 (A6 gate).

- source_plan: `_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md`
  summary: Entry before epic-safe-against-real-systems: bump to Connect v4.115.0 / benthos v4.84.0 / Go 1.27.3, with the AD-8 job (`task ci`) passing (AD-14); update the spine Stack and CLAUDE.md's Go version.
  evidence: review-verification-2026-10-22.md (live registries 2026-10-22; v4.112 to v4.115 changed only component packages in connect/public, and the AD-7 parents only in doc strings). User decision 2026-10-22.

- source_plan: `_bmad-output/initiative-rpcn-debugger/plan-bump-connect-4-115.md`
  summary: Delivered — the pre-E3 bump (from the 2026-10-22 spine update, the entry "bump to Connect v4.115.0 / benthos v4.84.0 / Go 1.27.3" above) was delivered by plan-bump-connect-4-115, with `task ci` (AD-8 corpus and integration) green.
  evidence: go.mod and tools/go.mod at go 1.27.3; cmd/rpcn-debug/version_test.go TestDepVersions/TestRunVersion pin v4.115.0/v4.84.0; `task ci` exit 0 on 2026-10-22; no rewrite golden changed.

- source_plan: `_bmad-output/initiative-rpcn-debugger/plan-try-catch-stateful-and-buffer-warnings.md`
  summary: Delivered — the stateful `try`/`catch` launch warning (the entry "a launch warning when a `try` or `catch` list holds a stateful child" above): a `try` or `catch` list with two or more children, one of which holds a stateful processor (`cache`, `cached`, `dedupe`, `redis`, `redis_script`, `sql_raw`, `sql_insert`, `sql_select`, directly, through a `resource:`, or nested in a parent), gets one warning at the list's key line. `rate_limit` is left out (timing only, AD-8), and AD-7's example list no longer names it.
  evidence: debugger/rewrite/state.go (stateful, holdsStateful, warnStatefulOrder); goldens debugger/rewrite/testdata/stateful-*.yaml; corpus case testdata/equivalence/try-stateful (the stateless case is testdata/equivalence/try).

- source_plan: `_bmad-output/initiative-rpcn-debugger/plan-try-catch-stateful-and-buffer-warnings.md`
  summary: Delivered — the dropped `buffer` launch warning (the entry "warn when the top-level `buffer` section is dropped" above): a top-level `buffer` that is set (not null, empty or `none`) gets one warning at its key line, and a corpus case pins it.
  evidence: debugger/rewrite/state.go warnBuffer, called from Rewrite in debugger/rewrite/rewrite.go; goldens debugger/rewrite/testdata/buffer.yaml and buffer-none.yaml; corpus case testdata/equivalence/buffer (`memory: {}` keeps the batch as captured).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-one-rewrite-per-launch-with-session-supplied-rewrite-options-plan.md`
  summary: CLAUDE.md's Layout is stale after E3 entry 1: add `core/debuggroup.go` (the Session's debug-group name), `rewrite.Options`/`Plan.Options` (`Rewrite(src, opts)`), the `Prepared` hand-off from Check to Start, and `dapdrive.Options.AfterLaunch`. For E3's refactor sweep, which keeps CLAUDE.md current.
  evidence: quick review of E3 entry 1 (2026-10-23); fixes to agent-context files are deferred to a sweep.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-tracer-bullet-a-kafka-capture-under-the-debug-group-commits-plan.md`
  summary: CLAUDE.md's Layout should list `testdata/tracer/kafka.yaml` (the Kafka F5 sample for E3's acceptance run) and `test/integration/neverack_test.go` (the never-ack test). For E3's refactor sweep, with the entry-1 item above.
  evidence: quick review of E3 entry 2 (2026-10-23); fixes to agent-context files are deferred to a sweep.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-tracer-bullet-a-kafka-capture-under-the-debug-group-commits-plan.md`
  summary: Tell users what the debug group leaves on the cluster: an empty group `rpcn-debug-<os-user>-<k>` (no members, no offsets) that stays until the broker expires it or `rpk group delete` removes it, and that joining it needs Group READ on that name, so credentials limited to the production group can't capture. Belongs in the group-override launch warning or note (E3 entry 5) and in the user docs (epic-local-release).
  evidence: 3.2 VS Code check (2026-10-23): `rpk group list` shows `rpcn-debug-aleksejjurcenko-0` in state Empty after the session. The user kept the group-based capture (D5).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-consumer-group-override-for-every-group-field-plan.md`
  summary: A debug session killed abruptly (the binary gets no clean shutdown) leaves its member in the debug group until the broker's session timeout, so the next capture under the same `rpcn-debug-<os-user>-<k>` waits up to ~60 s in `CompletingRebalance` before it receives anything. Say so in the user docs and in the "Waiting for the input" note, or have the note name the group; production is unaffected.
  evidence: 3.5 VS Code prep (2026-10-23): after timeout-killed DAP runs, `rpk group describe rpcn-debug-aleksejjurcenko-0` showed CompletingRebalance with 1 member for 60 s; the next session then captured normally.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-output-real-and-none-plan.md`
  summary: When the step stream's Run fails (e.g. an output that fails to init under `output: real`), `engine.run.step` now returns at once, but the producer goroutine (`produce(context.Background(), batch)`) is left waiting and leaks for the life of the process. Bound or cancel it, for E3's refactor sweep or entry 8's write timeout.
  evidence: E3 entry 7 review fix (2026-10-23), noted by the implementer; harmless today (one goroutine per failed session).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-refactor-sweep-plan.md`
  summary: Delivered — the strict decision by error text (from 2.14, the entry "STRICT_REJECTED is chosen when the producer's error text contains …" above) was delivered by 3.8: `STRICT_REJECTED` is decided from the errored messages the top-level steppers count on their way to the output, never from benthos's error text, so a real write failure in a mixed batch is no longer reported as the strict note.
  evidence: debugger/engine/engine.go (`atOutput`, `erroredIn`, the outcome decision before `CodeStrictRejected`); commit 46ee1df (3.8).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-refactor-sweep-plan.md`
  summary: Delivered — the Session-owned delivered-batch number (from the 2026-10-22 spine update, the entry "The Session owns the delivered-batch number (AD-3)" above) was delivered by 3.8: `core.Delivered.Batch` (1-based) is numbered by `Session.Deliver`, and debugger/dap displays it instead of counting.
  evidence: debugger/core/protocol.go `Delivered.Batch`; debugger/core/session.go `Deliver`; debugger/dap/server.go; commit 46ee1df (3.8).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-refactor-sweep-plan.md`
  summary: Delivered — the producer leak (from 3.7, the entry "the producer goroutine (`produce(context.Background(), batch)`) is left waiting" above) was delivered by 3.8: the producer runs on a cancellable context, so a failed step stream no longer leaves it waiting.
  evidence: debugger/engine/engine.go `produce(prodCtx, batch)`; commit 46ee1df (3.8).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-refactor-sweep-plan.md`
  summary: Delivered — benthos's strict-mode Warn (from 2.11, the entry "Benthos's own strict-mode Warn ('Rejecting N of M message(s) for output 'rpcn_debug_sink' …') reaches the Debug Console" above) was delivered by 3.10, the E3 refactor sweep, by a different route (user decision 2026-10-30): the AD-15 handler keeps that record off the Debug Console and writes it to stderr only; the Session's strict note stays the one console line. Its embedded `(line N)` belongs to the failing processor, which the record doesn't name, so it isn't shifted.
  evidence: debugger/logging.go `strictRejection`; debugger/logging_test.go TestLogStrictRejection; test/equivalence/equivalence_test.go checkRun (no rejection record on the console, the record in the log when the strict note shows).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-refactor-sweep-plan.md`
  summary: Partly delivered — the stale debug-group member (from 3.5, the entry "A debug session killed abruptly … waits up to ~60 s") was delivered in the user docs by 3.10: the README's debug-group paragraph says so. The "Waiting for the input" note still doesn't name the group; that half stays open, for epic-local-release's user docs or a later note touch-up.
  evidence: README.md, "What a debug session does to your input", the debug-group bullet.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-refactor-sweep-plan.md`
  summary: Delivered — CLAUDE.md's stale Layout (from 3.1 and 3.2, the entries "CLAUDE.md's Layout is stale after E3 entry 1" and "CLAUDE.md's Layout should list `testdata/tracer/kafka.yaml` …" above) was delivered by 3.10: Project state describes E3 as of 3.9, and Layout lists `core/debuggroup.go`, `core/inputs.go`, `core/output.go`, `rewrite.Options`/`Plan.Options`, `CheckResult.Prepared`, the new rewrite files, `dapdrive.Options.AfterLaunch`, the integration tests and the new tracer samples.
  evidence: CLAUDE.md Project state and Layout.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-refactor-sweep-plan.md`
  summary: The remaining half of the stale debug-group member item (from 3.5): the "Waiting for the input" note should mention the possible ~60 s wait after a killed session, or name the debug group, so a user waiting on a stale member knows why. The README half was delivered by 3.10.
  evidence: debugger/core/session.go "Waiting for the input: nothing captured yet after %g s." names neither the group nor the stale-member wait.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-vs-code-acceptance-run-against-redpanda-plan.md`
  summary: `inputClasses` can loosen the server inputs (`http_server`, `socket_server`, `otlp_grpc`, `otlp_http`, `gateway`), which then capture from a server waiting for clients, and the `allowSideEffects` schema text in `debugger/dap/launch.go` says a rejected input ("stdin, http_server, sockets") can't be debugged either way; pin them (`core.FixedInputs`) or correct the text — needs a user decision.
  evidence: `core.FixedInputs` holds only stdin, dynamic, redpanda_migrator and redpanda_common; `checkInputClasses` and `hardRejected` don't pin http_server. Found by the 3.11 review.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-vs-code-acceptance-run-against-redpanda-plan.md`
  summary: Every Kafka capture logs `level=WARN msg="Kafka broker read failed" … err="context canceled"` to the Debug Console when the capture stops; filter it (it's the capture's own cancel) — cosmetic.
  evidence: Seen in every Kafka probe of 3.11 (kafka.yaml, kafka-real.yaml).

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-vs-code-acceptance-run-against-redpanda-plan.md`
  summary: Under `OUTPUT_FAILED`, benthos's `Failed to send message to rpcn_debug_sink: write to the output: …` error reaches the Debug Console naming the internal sink, after the Session's own failure line; like the strict Warn (3.10), keep it on stderr — cosmetic.
  evidence: 3.11 probe of output-failed.yaml with output: real.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-vs-code-acceptance-run-against-redpanda-plan.md`
  summary: "Delivered batch N (1 messages)" has no singular: `debugger/dap/server.go` formats `(%d messages)` — cosmetic.
  evidence: 3.11 probe of side-effecting.yaml and kafka-broker.yaml.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/story-vs-code-acceptance-run-against-redpanda-plan.md`
  summary: CLAUDE.md's `testdata/tracer/` entry should list the 3.11 samples (kafka-real, kafka-broker, side-effecting, unknown, rejected, warnings, output-failed). For the next refactor sweep.
  evidence: Agent-context edits are deferred to sweeps; found by the 3.11 review.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/epic-safe-against-real-systems-retrospective.md`
  summary: The effect scan (3.9) treats every `input_resources` entry as capture-time, so an unreferenced entry gets false "runs during capture" and state-split warnings although benthos never builds it; seed the capture side with the resources the input walk reached. For the next refactor sweep.
  evidence: E3 retro R5: `debugger/rewrite/effects.go:481-484`, `519-523`.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/epic-safe-against-real-systems-retrospective.md`
  summary: Under `output: real` the state-split check ignores the output tree (`outRefs` is computed but unused), and `refKeys` misses the `cache` output's `target`, so a memory cache shared by an input-tree processor and an output-tree processor gets no warning. For the next refactor sweep.
  evidence: E3 retro R6: `debugger/rewrite/effects.go:396`, `488-497`, `532-545`.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-safe-against-real-systems/epic-safe-against-real-systems-retrospective.md`
  summary: `debugger/logging.go` matches benthos log text (`strictRejection`, reusing the name 3.8 removed); the deferred Kafka-cancel and OUTPUT_FAILED log items would add more such filters. Rename it and keep the log-text filters in one table. For the next refactor sweep.
  evidence: E3 retro V6: `logging.go:106,118`; 3.8's verification `grep strictRejection` no longer finds nothing.

- source_plan: `_bmad-output/initiative-rpcn-debugger/plan-b1-never-ack-every-input-resource.md`
  summary: An input whose `resource:` is resolved only at run time (or names no literal label) is classed Unknown, so `allowSideEffects` lets it through even when the input resources it may reach include a rejected one (stdin, which would read the DAP stream; http_server); roll the worst Rejected class of the reachable resources into the referring input — needs a user decision, since it changes refusals.
  evidence: B1 review finding 2; `unreached()` walks those entries but drops their findings by design (B1's intent keeps refusals unchanged); `serve_inputs_test.go:225` shows a literal reference to a rejected resource is refused.
