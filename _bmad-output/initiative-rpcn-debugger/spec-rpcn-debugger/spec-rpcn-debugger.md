---
id: SPEC-rpcn-debugger
companions:
  - ../architecture-rpcn-debugger/architecture-rpcn-debugger.md
  - glossary.md
sources: []
---

> **Canonical contract.** This SPEC and the files in `companions:` are the complete, preservation-validated contract for what to build, test, and validate. Source documents listed in frontmatter are for traceability — consult them only if you need narrative rationale or prose color this contract intentionally omits.

# Redpanda Connect Visual Debugger

## Why

**Pain.** Redpanda Connect pipeline developers can't see message state between processors. Today they rely on logs, the `log` processor, and running the whole pipeline, so Bloblang and control-flow mistakes show up steps later as confusing errors.

The 2026-10-08 spikes caught three such mistakes by stepping:
- `root.x =` silently dropping fields
- `merge` turning a counter into an array
- a `null` modulo two processors after the real cause

A step-by-step debugger in the editor shows the cause at the processor (or Bloblang statement) where it happens. It does this without touching production pipelines or data.

## Capabilities

- **CAP-1**
  - **intent:** A developer can start a debug session from VSCode on a pipeline YAML. This works with the stock component set, with RPC plugins, and with their own build that has compiled-in plugins.
  - **success:** F5 on a `pipeline.yaml` stops before its first processor. This holds for the prebuilt binary, the same binary with an RPC plugin manifest, and a user debug `main` selected through `rpcnDebug.binaryPath`.
- **CAP-2**
  - **intent:** The session captures one batch (at most `maxMessages`) from the pipeline's real input, without consuming, deleting or rebalancing production data.
  - **success:**
    - After a session against a Redpanda topic, neither the production nor the debug consumer group has committed offsets, and a rerun captures the same messages.
    - A side-effecting input is refused unless `allowSideEffects` is true.
    - A rejected input (`stdin`, `http_server`) is refused, with a hint to use replay.
- **CAP-3**
  - **intent:** A developer can Step Over, Step In and Step Out through processors, including every control-flow parent. The current processor is highlighted in the YAML, and repeated entries are labelled in the call stack.
  - **success:** For each parent in the architecture's AD-7 table, Step In reaches every child at its correct YAML line. The labels `[msg i/n]`, `[attempt i/max]`, `[loop i]` and `[group i]` appear on the matching frames.
- **CAP-4**
  - **intent:** At every stop, a developer can inspect the content, metadata and error of each message, and how the messages are split into batches.
  - **success:**
    - Variables show Batches › Message › content / metadata / error.
    - Byte metadata such as `kafka_key` reads as text, and `__`-prefixed keys sit in a collapsed "internal" scope.
    - After `group_by`, the split into batches is visible.
- **CAP-5**
  - **intent:** A developer can set line breakpoints, conditional breakpoints written in Bloblang, and an on-error exception breakpoint.
  - **success:**
    - A breakpoint on any line inside a processor stops before that processor.
    - A condition stops only when at least one message matches, and focuses that message.
    - On-error stops right after a processor introduces an error.
    - Lines outside any processor are reported unverified.
- **CAP-6**
  - **intent:** A developer can evaluate Bloblang (Watch, debug console) against the paused batch, with the same functions and plugins as the pipeline.
  - **success:** A Watch expression gives the same value that a `mapping` using the same expression produces at that point. `batch_index()` and `batch_size()` reflect the real batch.
- **CAP-7**
  - **intent:** A developer can step statement by statement through a `mapping`, `mutation` or `bloblang` processor, or a branch `request_map`/`result_map`, and see `root`, `this`, metadata and `$variables` after each statement.
  - **success:**
    - The spike's 16-line mapping steps as 9 statements, with the correct root, metadata and variables after each.
    - `uuid_v4()`/`now()` values are labelled "recomputed".
    - The traced final state equals the real processor output, or the difference is flagged.
- **CAP-8**
  - **intent:** A developer can edit a paused message's content or metadata and continue with the edited message.
  - **success:** Changing a field at a stop changes the output of the processors downstream of that stop, and nothing upstream.
- **CAP-9**
  - **intent:** Branches that run at the same time (`workflow`, `parallel`, `retry` with `parallel: true`) appear as separate threads that a developer can step independently.
  - **success:** A `workflow` with two independent branches shows two threads. Stepping one doesn't stop the other except at breakpoints.
- **CAP-10**
  - **intent:** A developer can restart a session to apply an edited config to the same captured batch, and can save a capture and replay it later without the input system.
  - **success:**
    - After editing the YAML, Restart re-steps the identical batch under the new config and doesn't read from the input again.
    - A saved capture replays byte-identically, with no broker reachable.
- **CAP-11**
  - **intent:** By default, results are shown in VSCode and nothing is written to the pipeline's output. Writing to the real output requires an explicit opt-in.
  - **success:** A default session writes nothing to the output system and shows each delivered batch. With `output: real`, the output receives the same batches.
- **CAP-12**
  - **intent:** Debugging never changes what the pipeline produces.
  - **success:** The equivalence corpus (every control-flow parent, plus strict mode) produces identical output with and without the debugger, both with no pauses and with a scripted Step Over at every stop.
- **CAP-13**
  - **intent:** A developer is warned when the debugger's Connect version differs from their `rpk connect`, and is blocked when the extension and the binary disagree on the launch contract.
  - **success:** A minor or major Connect version difference shows a warning. A major `launchSchemaVersion` mismatch refuses to start the session.

## Constraints

- Uses only the public Redpanda Connect / benthos Go API, linked in-process: no engine fork, no `internal/` imports, no controlling a running `rpk connect` (AD-1, AD-2).
- Capture-then-step only: the input runs just long enough to take one batch, the batch is never acked, and pausing happens only after the input has stopped (AD-4).
- Production safety defaults: a debug consumer group, dry-run output, side-effecting inputs only with an opt-in, and observability sections dropped (AD-5).
- Primary users are laptop developers running stock `rpk connect`, so the prebuilt binary is the primary path; plugin builds are secondary (AD-1). The debugger runs only on a developer machine, next to the editor. Production deployments and images stay unchanged.
- Each release pins exactly one Connect version (v4.112.0 / benthos v4.81.0 at authoring) (AD-14).
- Minimum VS Code `^1.120.0`. The extension holds no pipeline logic (AD-12).
- Personal project, published as "Pipeline Debugger for Redpanda Connect" (`pipeline-debugger-for-redpanda-connect`) under the author's own publisher, never as an official Redpanda product. MVP distribution is local (a hand-installed `.vsix` with a bundled or locally built binary). The VS Code Marketplace comes later (AD-23).
- No credential literals in `launch.json`. Captures are saved only on explicit request, with `0600` permissions and auto-gitignored. No telemetry (AD-22).
- Results are guaranteed identical only when nothing time-budgeted is paused. Otherwise the user is warned (AD-8).

## Non-goals

- Attaching to or pausing a running production pipeline.
- A separate Bloblang interpreter, or statement stepping inside folded (`>`) or quoted mappings.
- DAP over TCP, or running the debugger in a container or Kubernetes in v1.
- MCP, TUI or webview frontends in v1.
- Logpoints and hit-count breakpoints in v1.
- Engine changes upstream as a prerequisite. Upstream hooks are optional upgrades only.
- Performance profiling or throughput measurement.
- Emulating output-level `batching` in dry-run.
- Marketplace or Open VSX publication and binary download in the MVP.

## Success signal

A developer presses F5 on a pipeline that reads from Redpanda. It stops at the first processor with the first batch visible. They Step In through a `branch` and through a mapping statement by statement, spot the faulty Bloblang line, fix it, press Restart, and see the corrected result on the same batch. Afterwards, the production consumer group's committed offsets are unchanged.

## Assumptions

- The default `maxMessages` is 10. The architect chose it, not the user.

## Open Questions

- License timing: `processor_resources` may be built before the license is stored (architecture open question).
- Stable message identity across `group_by` reordering, for diffs and tests (architecture open question).
- Binary size and CGO needs of `components/all` across OS/arch (architecture open question).
