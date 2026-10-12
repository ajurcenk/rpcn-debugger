---
review-of: architecture-rpcn-debugger.md (status: draft, 2026-10-08)
lens: 'Attack the spine: two units, each compliant with every AD, that still build incompatibly'
reviewer: adversarial independent reviewer
date: 2026-10-08
---

# Adversarial review: rpcn-debugger architecture spine

## Verdict

The spine fixes the big strategic choices well (in-process, public API, capture-then-step, a pause gate held only while paused, prefix replay as a view). It does not fix the **runtime identity contracts** that U-STEP, U-CORE, U-REWRITE and U-DAP all have to share: what a stepper's identity is, how a goroutine becomes a thread, where the frame stack lives, who owns a message while it is paused, and how a stop is decided. Every AD can be followed to the letter while those contracts are built two different ways. I found 16 compliant-but-incompatible pairs: 4 Critical, 6 High, 5 Medium, 1 Low. The Critical and High ones should be closed with new or tightened ADs before any unit starts.

Severity scale: **Critical** means the units cannot be integrated without one of them being rewritten. **High** means they integrate, but give wrong results or wrong behaviour in common pipelines. **Medium** means a mismatch shows up in a corner case or in the UX. **Low** means cosmetic or easy to fix later.

---

## H1. Stepper identity: what LineMap keys on versus what a stepper knows at runtime [Critical]

- **U-REWRITE builds:** A `LineMap` keyed by the *user path* string (`pipeline.processors[2].branch[1]`). The rewritten YAML puts that user path into each stepper's config as `path:`, plus the child lines. AD-6 only says LineMap maps "each stepper position" to user path + file + line.
- **U-STEP builds:** A stepper that identifies itself by the *engine path* it can see at runtime (the label or path from `service.Resources`/logger fields, such as `root.pipeline.processors.0.rpcn_debug_stepper.processors.2...`), and looks up LineMap with a key it derives by stripping the stepper segments.
- **Why both comply:** AD-6 requires a LineMap from stepper position to user location. It does not say what a "stepper position" key is, who assigns it, or whether it travels in the config. AD-7 only fixes `mode`.
- **How they clash:** The keys never match, so every lookup misses. Even if both agree on "user path", **one stepper config instance has many runtime entries**. A `processor_resources.foo` stepper is built once and entered from every `resource: foo` call site. A `for_each` child stepper is built once and entered N times. Neither unit can turn "user path" into a *frame* (call site + repeat index). So the two units also build different ideas of what a frame is (see H2, H3).
- **Proposed AD (new AD-16: Stepper identity):**
  > `debugger/rewrite` assigns every stepper it emits a dense integer `stepperId`, unique within one rewrite, and writes it into the stepper config as `id`, together with `parent` (the parent kind from AD-7's table), `mode`, and `concurrent: bool` (true when the parent runs its children's entries on several goroutines: `workflow`, `parallel`, `retry` with `parallel: true`, `branch` inside `workflow`). `LineMap` is keyed by `(stepperId, childIndex)`, where `childIndex` runs 0..n and n is "after last child", and returns `{userPath, file, line}`. A stepper never derives its identity from engine paths, labels or logger fields. `processor_resources.<label>` steppers get one `stepperId`. Their call site comes from the frame stack (AD-17), not from LineMap.

## H2. threadId binding: Go has no goroutine IDs [Critical]

- **U-CORE builds:** `Session.RegisterThread()`, which identifies the calling goroutine by parsing `runtime.Stack` for the goroutine number (or `petermattis/goid`). It allocates one `threadId` per goroutine, which is the literal wording of AD-9 ("every goroutine that enters a stepper is registered as one debug thread").
- **U-STEP builds:** A thread identity carried in `context.Context` and passed through `ProcessBatch(ctx, ...)`. The first stepper with no thread in ctx mints one. Nested steppers inherit it.
- **Why both comply:** AD-9 says threads are allocated by `Session` and are per goroutine. It does not say how a goroutine is recognised.
- **How they clash:**
  1. The goroutine-ID approach breaks when the engine runs one logical branch across goroutines. It also gives a goroutine pooled by the engine an old thread.
  2. The ctx approach collapses concurrent siblings into one thread. `parallel` and `workflow` start children with the *same* ctx, so every `parallel [msg i/n]` entry inherits the parent's thread. That gives two "stopped" positions on one threadId, which DAP cannot represent.
  3. Any engine processor that calls `context.Background()` internally breaks the ctx chain silently.
  4. The Conventions table says `threadId` "become[s] invalid on every resume". U-DAP following that would invent new threadIds after each step, while U-CORE following AD-9 keeps one thread per goroutine. In VSCode the call stack view loses its selection and expansion on every step, and the Threads view flickers.
- **Proposed AD (new AD-17: Frames and threads travel in ctx):**
  > The frame stack is an immutable linked list stored in `context.Context` under a key owned by `debugger/core`. Each stepper entry pushes one frame `{frameId, stepperId, repeatLabel, callSite}` and passes the new ctx to its children. A stepper mints a **new** `threadId` (via `Session.ForkThread(parentFrame)`) when its config has `concurrent: true` or when ctx has no frame. Otherwise it inherits the parent frame's thread. When the stepper entry that created a thread returns, it calls `Session.ExitThread`. The Session emits `ThreadStarted`/`ThreadExited`. Goroutine-ID introspection is forbidden. A stepper that finds no frame in ctx but has a non-root `parent` logs a `CTX_LOST` warning, and the equivalence/integration corpus contains one case per AD-7 parent to prove ctx propagates. Conventions change: `threadId` is stable for the life of the thread. Only `frameId` and `variablesReference` become invalid on resume.

## H3. Breakpoint resolution: who decides that a line is valid [High]

- **U-DAP builds:** It answers `setBreakpoints` itself. A line is verified if `LineMap` contains it. Because DAP sends `setBreakpoints` *before* `configurationDone` and the rewrite runs inside `launch`, U-DAP keeps pending breakpoints in its own map until LineMap exists.
- **U-CORE builds:** Breakpoints as Session state (AD-3: "Session is the only owner of debug state"). Only processor-start lines from LineMap are valid. A line inside a mapping is rejected, because statements are found by U-BLOBL's tracer, which runs only on Step In.
- **Why both comply:** AD-3 does not list breakpoints as a Command (the Commands are `Next, StepIn, StepOut, Continue, Evaluate, SetMessage, Restart`), so U-DAP holding them doesn't obviously violate AD-3. AD-6 says LineMap contains "the line of a mapping statement", which suggests U-REWRITE splits statements. AD-13 says the boundary rule (prefix parses + next line not indented) belongs to U-BLOBL.
- **How they clash:**
  1. There are two owners of breakpoint state, and pending breakpoints get lost or doubled when the Session re-validates.
  2. There are **two owners of the statement-boundary algorithm**: U-REWRITE needs it for LineMap statement lines, and U-BLOBL implements it for prefix replay. A boundary that one finds and the other doesn't gives a breakpoint that is verified but never hit, or the reverse.
  3. Nothing says whether Continue stops on a breakpoint inside a mapping. AD-10 lets you into statements only through Step In.
  4. A breakpoint on a line inside `processor_resources.foo` hits on every call site, and nothing says whether that is intended.
  5. Nothing says what happens to a breakpoint on a non-stop line (a YAML key, a comment). Does it move to the next valid line or stay unverified?
- **Proposed AD (new AD-18: Breakpoints):**
  > Breakpoints are Session state, set by `Command SetBreakpoints{file, lines[], conditions[]}`. The Session resolves them against a `LineMap` that includes **statement lines**, computed by calling `BloblangTracer.Statements(mappingSource) []StatementSpan` at rewrite time. U-BLOBL is the only owner of the boundary algorithm, and U-REWRITE calls it to fill LineMap. A requested line moves to the first valid stop line at or after it within the same processor, otherwise it is unverified. The Session emits `BreakpointResolved` for each one. Breakpoints received before the rewrite stay pending and are resolved at the end of `launch`. Stepping starts only after `configurationDone` (U-DAP holds the step stream's start until then). A breakpoint on a statement line makes Continue enter statement stepping of that processor automatically. A breakpoint inside a resource hits at every call site.

## H4. Mapping line offsets: block scalars and the "value start line" [Medium]

- **U-REWRITE builds:** It takes "the YAML line where the mapping value starts" as `yaml.Node.Line` of the scalar. For `mapping: |` that is the line of the `|` indicator.
- **U-BLOBL builds:** It converts Bloblang error line k to file line `start + k - 1`, taking the value's first *content* line as the start.
- **Why both comply:** AD-6's formula doesn't define "where the mapping value starts" for block, folded (`>`), quoted or flow scalars.
- **How they clash:** Every statement line and every error line is off by one between the two units for `|` scalars, which nearly every mapping uses. For `>` folded and double-quoted scalars, line offsets inside the Bloblang source don't correspond to file lines at all (newlines are folded and escapes collapse lines).
- **Proposed AD text (tighten AD-6):**
  > The mapping base line is the 1-based file line of the first content line of the scalar: `Node.Line + 1` for literal (`|`) block scalars, and `Node.Line` for plain scalars on the key's line. Statement line = base + (0-based line within the scalar's value). For folded (`>`), single-quoted or double-quoted mapping values, statement stepping and statement breakpoints are disabled (one step for the whole mapping) and the debugger warns. U-REWRITE computes the base line and is the only owner of this conversion. U-BLOBL receives `(source, baseLine)` and never computes file lines itself.

## H5. Message ownership while paused: Snapshot copies and SetMessage [Critical]

- **U-CORE builds:** A `Snapshot` that holds `*service.Message` references and serialises lazily when U-DAP asks for Variables (cheap, and AD-11 only fixes the *shape*). `SetMessage` is applied by the Session writing into those `*service.Message` values in place (`SetBytes`, `MetaSetMut`), since the Session "owns debug state".
- **U-STEP builds:** The stepper owns its in-flight `service.MessageBatch`. After the gate is released it passes that same batch to the child `ProcessBatch`, which may mutate it in place. It also reuses the slice for the next child.
- **Why both comply:** AD-11 fixes the shape and not the copy semantics. AD-3 says only the Session changes "debug state", but messages in flight are engine state. Nothing says which unit may mutate a `*service.Message`.
- **How they clash:**
  1. While thread A is paused, thread B (holding no gate, running a processor, which AD-9 allows) mutates messages that A's lazily serialised Snapshot shares. This happens with `parallel` and `workflow`, where branch copies can share parts. The Variables view shows values that never existed at A's stop position.
  2. `SetMessage` from the Session races the stepper's own write path and any concurrent processor. There are two mutation paths on one entity.
  3. A Snapshot captured "at stop i" shows the result after child i once the child mutates in place, so the debugger appears to break AD-8.
- **Proposed AD (new AD-19: Paused-message ownership):**
  > The stepper owns its batch for the whole entry. At each stop it hands the Session a **deep copy** (`msg.Copy()` plus materialised bytes and metadata), and the Session builds the immutable `Snapshot` from that copy while the gate is held. A Snapshot never references engine objects. `SetMessage{frameId, batch, index, content?, metadata?}` is valid only for the top frame of the stopped thread. The Session validates it and puts it into the resume reply. The **stepper** applies it to its own batch before releasing the gate. No other unit mutates a `*service.Message`. Evaluate (AD-13) runs against the stepper's copy owned by the paused thread, never against a batch another thread can reach.

## H6. Deciding a stop: the pause gate versus Step Over, Step Out and Continue on several threads [Critical]

- **U-STEP builds:** At every stop position the stepper takes the gate, emits `Paused`, and blocks. The Session decides at that moment whether this is a real stop and, if not, immediately replies `Continue`. That is the most literal reading of AD-9: "takes it only to report a pause".
- **U-CORE builds:** It expects the stepper to call `Session.ShouldStop(frame, childIndex)` without the gate, and to take the gate only on true. Step state (step-over target depth, step-out target frame) lives in the Session.
- **Why both comply:** AD-9 and AD-10 define stop positions and what Step Over/In/Out mean. They don't define the decision protocol, or which threads a step command applies to.
- **How they clash:**
  1. With U-STEP's protocol every stop position on every thread produces a `Paused` event, so U-DAP sends `stopped`/`continued` storms or has to filter. Filtering means U-DAP decides stops, which breaks AD-3.
  2. With U-CORE's protocol, `ShouldStop` reads step state without the gate while another thread's command changes it (a data race).
  3. Nothing says whether thread B, which reaches a stop while the user does Step Over on thread A, stops (DAP practice: only on breakpoints) or is held. Nothing says whether Step Out in a `for_each` child means "end of this message's entry" or "end of all entries". Nothing says whether AD-9's "all others wait on the gate" means `stopped.allThreadsStopped = true` (false: threads running processors are not stopped), or how DAP `pause` and `continue{threadId}` (`singleThread`) map onto one process-wide gate.
- **Proposed AD (tighten AD-9 and AD-10):**
  > At each stop position a stepper calls `Session.Arrive(ctx, stepperId, childIndex)`, which takes the gate, evaluates the stop predicate **under the gate**, and either returns immediately (no stop, no event) or emits `Paused` and blocks until a resume command. Step state is stored per thread in the Session. Step Over, Step In and Step Out stop only the thread they were issued on, and other threads stop only on breakpoints or exceptions. Step Out targets the end of the **current stepper entry** (one message, attempt or loop), not of all entries. Every resume is process-wide (`continued.allThreadsContinued = true`). `stopped.allThreadsStopped = false`. DAP `pause` sets a "stop at next Arrive" flag on all threads. The capability `supportsSingleThreadExecutionRequests` is false.

## H7. Restart semantics [High]

- **U-DAP builds:** `restart` (with `supportsRestartRequest: true`) maps to `Command Restart`, which U-CORE implements as "rewind the cursor and re-step the same captured batch, same config in memory".
- **U-EXT builds:** Nothing special, so VSCode's restart button falls back to terminate + relaunch. Or U-EXT sets `supportsRestartRequest` off because the generated schema doesn't mention it. That results in a **new capture**: a different batch, possibly from a different offset, because the consumer group never committed.
- **Why both comply:** AD-3 lists `Restart` as a command but gives it no meaning. AD-12 makes the extension thin and doesn't forbid relying on VSCode's default restart.
- **How they clash:** The same button gives different data depending on which path is taken. Users editing the YAML and pressing Restart expect the new config on the **same** batch. One build re-reads the file and the other doesn't. LineMap and breakpoints also need rebuilding when the file changed.
- **Proposed AD text (new AD-20: Restart):**
  > `Restart` re-reads and re-rewrites the pipeline YAML (new LineMap, breakpoints re-resolved), throws away all threads and frames, and re-steps the **already captured batch** through an in-memory replay Source. It never re-captures from the input. U-DAP advertises `supportsRestartRequest: true`. Getting a fresh capture is a separate launch. If the rewrite fails, the session stays alive and reports a `core.Error` with `UserPath`/`Line`.

## H8. Replay Source and capture Source: the Snapshot does not round-trip [High]

- **U-CORE builds:** It writes capture files by serialising the Snapshot. Metadata `type` is the Go type name (`int64`, `[]uint8`, `string`, `map[string]interface {}`). `value` is JSON, so int64 offsets above 2^53 lose precision. `[]byte` is written as UTF-8 text (AD-11).
- **U-STEP's replay Source builds:** A reader that expects Bloblang-style type names (`number`, `bytes`, `string`, `object`) and rebuilds messages with `MetaSetMut`. Text that was valid UTF-8 comes back as `string`, so `kafka_key` changes type from `[]byte` to `string`.
- **Why both comply:** AD-11 fixes the field names, `schemaVersion`, and the display rule for `[]byte`. It doesn't fix a type vocabulary, a lossless value encoding, or who writes capture files and when ("BIN reads/writes" is the only statement).
- **How they clash:**
  1. Replay is not equivalent to the capture. A replayed batch can produce a different output than the live run (`meta("kafka_key").type()` changes, and large offsets change), which breaks AD-8 without any rewrite fault.
  2. Two units can each write capture files (Session on capture, U-STEP's capture Source), giving duplicate or missing files.
  3. `skipped[]` is undefined: it could mean messages cut by `maxMessages`, or messages skipped by `try`.
- **Proposed AD text (tighten AD-11):**
  > Metadata `type` is one of the closed set `string | bytes | int | float | bool | timestamp | object | array | null`. `value` is lossless: bytes are base64, ints are decimal strings, timestamps are RFC 3339 with nanoseconds. The UTF-8 text rendering is a separate optional `text` field used only for display. Content keeps `raw` as the source of truth. Only the capture Source (U-STEP) writes capture files, once, right after truncation and before stepping. It writes the truncated batch plus `skipped[]` = the messages removed by `maxMessages`, as `{index, reason: "maxMessages"}`. Messages skipped by `try` are reported by the `MessagesSkipped` event, not in the capture file. A round-trip test (capture → file → replay Source → Snapshot) must be byte-identical and is part of the AD-8 CI job.

## H9. Input processors and consumer_group: two owners of the input config [High]

- **U-REWRITE builds:** It wraps `input.processors` in a stepper (AD-6 lists `input`) and leaves the input otherwise unchanged. The consumer-group override isn't its job, because AD-5 binds only U-STEP and U-EXT.
- **U-STEP builds:** Capture builds "a stream with only the input" (AD-4). To apply AD-5 it changes `consumer_group` in the config itself, with its own yaml.v3 walk or via `service.ParsedConfig`. It keeps `input.processors` in the capture stream because they are part of the input.
- **Why both comply:** AD-5 tells U-STEP to replace the group. AD-6 says only rewrite parses YAML, but AD-5 doesn't name U-REWRITE. AD-4 says "only the input" without saying whether input processors are included.
- **How they clash:**
  1. With input processors in the capture stream, the wrapped input steppers run, and pause, **during capture**, before the batch is taken and while the input is live. That is exactly the live pause AD-4 forbids. If they are stripped, nobody puts them at the front of the step stream, so they never run and the output differs.
  2. Two units parse the YAML, which breaks AD-6.
  3. The group field name differs per input (`consumer_group` on `redpanda`/`kafka_franz`, `consumer_group` under `kafka`, `queue` on `nats`, inputs nested in `broker`). Each unit can choose a different set.
- **Proposed AD text (tighten AD-4 and AD-5):**
  > U-REWRITE produces **two** configs: (a) a capture config that contains the input with `input.processors` removed and every known consumer-group field (a table in `debugger/rewrite`, recursing into `broker`/`sequence` children) set to `rpcn-debug-<os-user>`, unless `keepConsumerGroup` is set, and (b) a step config whose first stepper is `input.processors` (user path `input.processors[i]`), followed by `pipeline.processors`, then `output.processors`. U-STEP never edits configs. An input with no known group field and a group-like option gets a warning.

## H10. Output: dry-run sink versus output processors, output batching and group_by [High]

- **U-REWRITE builds:** It wraps `output.processors` (AD-6) inside the step config's `output` block, which is still the user's real output.
- **U-STEP's delivery builds:** A dry-run Sink that **replaces** the whole `output` block with an in-memory consumer. That silently drops the wrapped `output.processors`. It also ignores outputs that carry processors per child (`switch` output `cases[].output.processors`, `broker` outputs) and the output `batching` policy.
- **Why both comply:** AD-5 demands a dry-run sink. AD-6 demands wrapping output processors. Neither says where the boundary between "output processors" and "sink" is.
- **How they clash:**
  1. Output processors are stepped in one build and absent in the other.
  2. With `group_by`, several batches reach the sink. An output `batching` policy merges them again, so batch boundaries differ between dry-run and real. AD-8 compares boundaries, and the equivalence harness from U-REL won't match U-STEP's sink.
  3. The dry-run results "appear in VSCode", but no Snapshot, event or command for them is defined.
- **Proposed AD text (new AD-21: Delivery boundary):**
  > The step stream's `output` is always `rpcn_debug_sink`, a plugin output owned by U-STEP that receives batches *after* `output.processors` (moved into the step config by U-REWRITE, see H9). In `dry-run` mode it records each batch exactly as received, with `MaxInFlight = 1` to keep order. Output-level `batching` is not applied and the debugger warns that boundaries may differ in production. In `real` mode it forwards to the user's output, built from the original block. For composite outputs (`switch`, `broker`, `fan_out`), per-case processors run unwrapped (one opaque step, with a warning), the same as an unknown parent. Delivered batches are emitted as `Event Delivered{Snapshot}`. AD-8 compares what `rpcn_debug_sink` received.

## H11. Commands, events and error codes are open-ended [High]

- **U-DAP builds:** It handles `pause`, `setBreakpoints`, `threads`, `disconnect`/`terminate` and the `exited`/`thread` events with local state, because AD-3 lists no such Command or Event. It also maps `core.Error.Code` to the DAP error `id` with `strconv.Atoi`.
- **U-CORE builds:** It treats `Code` as a string (`"REWRITE_FAILED"`) and adds `Pause` and `Terminate` commands under names U-DAP doesn't know.
- **Why both comply:** AD-3 gives example lists without saying they are closed. The Conventions table fixes the shape of `core.Error` but not the code type or registry.
- **How they clash:** Debug state ends up split between Session and DAP (which breaks the *intent* of AD-3 but not its letter). Error ids don't parse. Two units choose different names for the same event (`Stopped` vs `Paused`, `ThreadStarted` vs `ThreadCreated`).
- **Proposed AD text (tighten AD-3):**
  > The Command and Event sets are **closed** and defined in `debugger/core/protocol.go`. Commands: `Launch, ConfigurationDone, SetBreakpoints, Next, StepIn, StepOut, Continue, Pause, Evaluate, SetMessage, Restart, Terminate`, each carrying `threadId`/`frameId` where relevant. Events: `ThreadStarted, ThreadExited, StepStarted, StepFinished, Paused, Continued, MessagesSkipped, AttemptStarted, StatementStepped, BreakpointResolved, Delivered, Output, Terminated`. Adding one is a spine change. `core.Error.Code` is a string constant from a registry in `debugger/core/errors.go` (`UPPER_SNAKE`). U-DAP sends it as `body.error.format` with a stable numeric `id` from the same registry.

## H12. Frame Snapshot versus thread Snapshot [Medium]

- **U-CORE builds:** One Snapshot per `Paused` event, for the top of the stopped thread's stack only. AD-11 `position` has no `threadId`, `frameId` or `childIndex`.
- **U-DAP builds:** `scopes`/`variables` for **every** frame in `stackTrace`, so a parent frame (for example `workflow` entry `[msg 2/4]`) shows that frame's batch. It asks U-CORE for a Snapshot by `frameId`.
- **Why both comply:** AD-11 defines one shape. AD-10 defines frames. Neither says whether there is one Snapshot per frame or one per stop.
- **How they clash:** U-DAP can't populate parent frames, or it shows the top frame's messages under every frame, which is misleading for `group_by` and `branch`, where batches differ by level. The "after last child" position has no defined `line`.
- **Proposed AD text (tighten AD-11):**
  > `position` gains `{threadId, frameId, stepperId, childIndex}`. The Session keeps one Snapshot per live frame: the batch as of that frame's current stop, deep-copied according to AD-19. The "after last child" position uses the line of the stepper's parent key, and `childIndex = n`.

## H13. Evaluate on a paused thread while other threads run [Medium]

- **U-BLOBL builds:** `Evaluate` runs `msg.BloblangQuery` on the paused message using the pipeline's environment (AD-13), on the DAP goroutine.
- **U-STEP builds:** Messages owned by the paused thread, but the shared pipeline environment and resources (caches, `counter()` state, rate limits) used by other threads at the same time.
- **Why both comply:** AD-13 requires the pipeline environment. AD-9 allows other threads to run processors while one is paused.
- **How they clash:** A Watch expression that calls `counter()` or a cache lookup advances shared state, so the next real processor sees different values and the output differs from a run without the debugger (an AD-8 break caused by the user's Watch list). Watches that evaluate on every stop multiply this.
- **Proposed AD text (tighten AD-13):**
  > Evaluate, Watch and conditional breakpoints run against a **copy** of the paused message, with a Bloblang environment derived from the pipeline's environment in which stateful functions (`counter`, and the `recomputed` list from AD-13) are replaced by stubs that return the value marked `recomputed`. Resource access (caches, rate limits) is read-only or refused with `EVAL_SIDE_EFFECT`. Evaluations run while the gate is held.

## H14. Launch config defaults and path resolution [Medium]

- **U-EXT builds:** `resolveDebugConfiguration` fills `pipeline: ${file}`, `maxMessages: 10`, `output: "dry-run"`, and resolves `replay` and `rpcPlugins` relative to the workspace folder.
- **U-DAP builds:** `dap.LaunchConfig` with Go defaults (`maxMessages` 10 today) and an `output` enum of `dryRun | real | none` (the Sink port has three variants). It resolves relative paths against the process cwd.
- **Why both comply:** AD-12 makes Go the source of truth for the *schema* and allows a generated `package.json`. Defaults, enum spellings and path bases are not covered. The `cwd` key is missing from the Conventions list.
- **How they clash:** `"dry-run"` is rejected by Go (or accepted by a lenient parser and treated as `real`, which hurts production safety). Defaults drift when Go changes them. Relative paths resolve differently when the binary is started from another directory.
- **Proposed AD text (tighten AD-12):**
  > Defaults live only in `dap.LaunchConfig` (struct tags), and the generated schema carries them as documentation. The extension never injects values except a missing `pipeline` (set to `${file}`) and `cwd` (set to `${workspaceFolder}`). `cwd` is added to the launch keys, and every relative path in the config resolves against it. Enum values are lowercase kebab-case (`output: dry-run | real | none`). An unknown key or enum value is a launch error (`LAUNCH_INVALID`), never ignored.

## H15. Path translation in logs: who applies LineMap [Medium]

- **U-STEP builds:** It sends engine logs straight to the slog handler that U-DAP owns, unchanged.
- **U-DAP builds:** It expects log lines that already use user paths. Per the dependency graph it imports only `core`, so it can't reach `rewrite.LineMap`.
- **Why both comply:** AD-6 says paths are translated "before any frontend sees it" but names no unit. AD-15 routes logs to DAP.
- **How they clash:** Users see rewritten paths (`...rpcn_debug_stepper.processors.2...`), or both units translate and one double-translates.
- **Proposed AD text (tighten AD-6):**
  > The `LineMap` type and `Translate(enginePath) userPath` interface live in `debugger/core`, implemented by `debugger/rewrite`. The single slog handler built by `Serve` for the engine translates paths and Bloblang error lines before they reach any Frontend or stderr. Frontends receive only user paths.

## H16. `[attempt i/max]` and `retry` re-entry counting [Low]

- **U-STEP builds:** Attempt counting in the stepper (a counter on the stepper instance per message index).
- **U-CORE builds:** Attempt counting from `AttemptStarted` events in the Session, keyed by user path.
- **Why both comply:** AD-10 says "the debugger counts attempts and loops itself" without naming a unit.
- **How they clash:** With concurrent `retry` (`parallel: true`), or the same resource used twice, a per-user-path counter in the Session mixes messages, so the two counts diverge in frame labels.
- **Proposed AD text (tighten AD-10):**
  > Repeat counters belong to the frame (AD-17). The stepper computes `repeatLabel` from the parent frame's per-entry counter in ctx and reports it in `StepStarted` and `AttemptStarted`. The Session displays it and never computes it.

---

## Suggested order to close

1. H1, H2, H5, H6 (new AD-16, AD-17, AD-19, tighter AD-9/AD-10). Nothing in U-STEP, U-CORE or U-DAP can be built consistently without them.
2. H3, H9, H10, H11, H7, H8 (breakpoints, the input/output boundary, closed protocol, restart, lossless capture).
3. H4, H12 to H16 as tightening edits.
