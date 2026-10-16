- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-platform-baseline/story-repo-scaffold-task-runner-and-installable-skeleton-extension-plan.md`
  summary: Update CLAUDE.md now that the repo has a Go module, a Taskfile (`task ci`, CGO_ENABLED=0), tests, the vscode/ extension and git history.
  evidence: CLAUDE.md:7 still says the repository has no code, build system, tests, README or git history; the 1.1 diff added all of them.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-platform-baseline/story-dap-adapter-seed-debugger-serve-and-extension-wiring-plan.md`
  summary: debugger.Serve hard-wires its slog handler to os.Stderr; give Serve an Option for the log destination (or the AD-15 handler) so cmd/rpcn-debug's injected stderr and library callers' main packages receive server diagnostics.
  evidence: debugger/serve.go builds slog.NewTextHandler(os.Stderr, nil) while cmd/rpcn-debug run() takes a stderr writer; TestRunNoArgs cannot show server logs. The AD-15 handler (stderr plus DAP Output) lands in epic-tracer.

- source_plan: `_bmad-output/initiative-rpcn-debugger/epic-tracer/story-tracer-bullet-f5-captures-stops-steps-and-delivers-plan.md`
  summary: The rewrite re-marshals `input` and `pipeline.processors` subtrees separately, so a YAML alias (`*name`) to an anchor defined elsewhere in the file becomes an unknown anchor; resolve aliases (deep-copy the target node) before encoding — for entry 2.5, which owns the rewrite.
  evidence: Reviewer reproduced it: `x: &m 'root = 2'` with `processors: [{mapping: *m}]` yields stepper YAML `- mapping: *m` and `yaml: unknown anchor 'm' referenced`, while Connect accepts the pipeline.
