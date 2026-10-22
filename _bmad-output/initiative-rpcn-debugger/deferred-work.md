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
