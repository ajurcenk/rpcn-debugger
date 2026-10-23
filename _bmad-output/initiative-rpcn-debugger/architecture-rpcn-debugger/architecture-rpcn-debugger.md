---
name: 'rpcn-debugger'
type: architecture-spine
purpose: build-substrate
altitude: feature
paradigm: 'hexagonal (ports & adapters) around an in-process Redpanda Connect engine'
scope: 'Redpanda Connect VSCode visual debugger: capture one batch from a pipeline input, step it through the processors (and Bloblang statements), and show the message state in VSCode over DAP'
status: final
created: '2026-10-08'
updated: '2026-10-22'
binds: [U-CORE, U-REWRITE, U-STEP, U-BLOBL, U-DAP, U-EXT, U-REL]
sources:
  - 'analyst handoff + spikes 2026-10-08 (scratchpad/spike: main.go, nested/, blobl/)'
  - 'spike 1.6, 2026-10-14 (epic-platform-baseline/spike-components-all-link-size-cgo-and-license-timing-plan.md)'
  - 'epic-tracer (E2) as built, 2026-10-22: the 14 plans, epic-tracer-retrospective.md (R1, A6, A7), fixes S1 and S2'
  - '.memlog.md'
  - 'reviews/review-rubric.md, reviews/review-verification.md, reviews/review-adversary.md'
companions: []
---

# Architecture Spine: rpcn-debugger

## Design Paradigm

**Hexagonal.** The core `Session` owns all debug state. The outside world reaches it only through ports. The Redpanda Connect engine is **linked in-process as a library**, using the public API only.

| Layer | Package | Contents |
| --- | --- | --- |
| Core (domain) | `debugger/core` | `Session`, the protocol (commands, events, errors), `Snapshot`, frames and threads, the pause gate, the `LineMap` interface |
| Engine adapters | `debugger/engine` | capture, `rpcn_debug_stepper`, `rpcn_debug_sink` |
| Config rewrite | `debugger/rewrite` | YAML → capture config + step config, the `LineMap` implementation |
| Bloblang tracer | `debugger/blobl` | statement spans, prefix replay, evaluator |
| Frontend adapters | `debugger/dap` | DAP server and `LaunchConfig` (v1) |
| Public entry | `debugger` | `Serve(env, in, out, opts...)` |
| Binary | `cmd/rpcn-debug` | thin `main` |
| Editor | `vscode/` | thin TypeScript extension |

```mermaid
flowchart TD
  CMD[cmd/rpcn-debug] --> PUB[debugger: Serve]
  PUB --> DAP[debugger/dap]
  PUB --> ENG[debugger/engine]
  PUB --> RW[debugger/rewrite]
  PUB --> BL[debugger/blobl]
  DAP --> CORE[debugger/core]
  ENG --> CORE
  RW --> CORE
  RW --> BL
  BL --> CORE
  ENG --> RPCN[(connect + benthos public API)]
  BL --> RPCN
  EXT[vscode extension] -. DAP over stdio .-> DAP
```

Arrows point from dependent to dependency. `core` imports only the Go stdlib. `dap` imports only `core`.

## Invariants & Rules

Unit IDs:
- **U-CORE**: session, protocol, snapshot, frames and threads, pause gate
- **U-REWRITE**: YAML rewrite, line map
- **U-STEP**: capture, stepper, sink
- **U-BLOBL**: Bloblang tracer and evaluator
- **U-DAP**: DAP adapter
- **U-EXT**: VSCode extension
- **U-REL**: build, release, CI

### AD-1: Library first, thin binary [ADOPTED]

- **Binds:** all Go units, U-REL
- **Prevents:** logic forked between the binary and the library; a debugger that can't run the components that users compiled in; one session's plugins leaking into another.
- **Rule:**
  - All behaviour lives in the Go package `debugger`, behind one entry point: `debugger.Serve(env *service.Environment, in io.Reader, out io.Writer, opts ...Option) error`.
  - `cmd/rpcn-debug` only builds `env` from `schema.Standard(version, date).Environment()` with `components/all` imported, then calls `Serve`. Users with compiled-in plugins call `Serve` from their own `main`.
  - Each stream a session builds works on its own `env.Clone()`: one for capture, one for stepping (AD-4). Every registration goes on each clone (`rpcn_debug_stepper`, the launch's RPC plugins via `public/plugin/go/rpcnloader.DiscoverAndRegisterPlugins`), except `rpcn_debug_sink`, which only the step clone has. All clones are discarded on terminate and on restart. The caller's `env` is never modified.
  - Each stream builds its own `*_resources`. A stateful resource (a `memory` cache, a local `rate_limit`) used both in the input tree and in the pipeline therefore holds separate state in capture and in stepping, unlike production, where it is shared.
  - The exported API of `debugger` follows semver from v1.

### AD-2: Public engine API only, linked in-process [ADOPTED]

- **Binds:** all Go units
- **Prevents:** a fork that breaks on every Connect release; dependence on a pause/step API that Connect doesn't have.
- **Rule:** Go code imports only `github.com/redpanda-data/benthos/v4/public/...` and `github.com/redpanda-data/connect/v4/public/...`. Importing any `internal/` package, vendoring or patching the engine, or controlling an external `rpk connect` process is forbidden. `depguard` enforces this in CI (AD-15). An engine change may only come as an optional upstream improvement behind an existing port.

### AD-3: Session owns all debug state through a closed protocol [ADOPTED]

- **Binds:** U-CORE, U-STEP, U-BLOBL, U-DAP
- **Prevents:** debug state split between Session and DAP; units inventing commands or events the others don't know; frontend types leaking into the engine.
- **Rule:**
  - Only `core.Session` changes debug state: breakpoints, step state, threads, frames, snapshots and the captured batch.
  - The protocol is a **closed set** defined in `debugger/core/protocol.go`, and adding a member is a spine change:
    - **Commands:** `Launch, ConfigurationDone, SetBreakpoints, SetExceptionBreakpoints, Next, StepIn, StepOut, Continue, Pause, Evaluate, SetMessage, SaveCapture, Restart, Terminate`.
    - **Events:** `ThreadStarted, ThreadExited, StepStarted, StepFinished, Paused, Continued, MessagesSkipped, AttemptStarted, StatementStepped, BreakpointResolved, Delivered, Output, Terminated`.
  - Adding a **field** to an existing command or event is not a spine change; adding a member is.
  - Errors are `core.Error{Code, Message, UserPath?, Line?, Inline}`. `Inline` means `Message` already names every location, and `UserPath`/`Line` hold the first one. `Code` is an `UPPER_SNAKE` string constant from the registry in `debugger/core/errors.go`, which also assigns each code a stable number for DAP. A new code takes the next number, which isn't a spine change; numbers are never reused: `LAUNCH_INVALID` 1001, `PIPELINE_INVALID` 1002, `NOTHING_CAPTURED` 1003, `SESSION_ENDED` 1004, `NOT_SUPPORTED` 1005, `NOT_STOPPED` 1006, `RUN_FAILED` 1007, `CTX_LOST` 1008, `LICENSE_REQUIRED` 1009, `STRICT_REJECTED` 1010.
  - `STRICT_REJECTED` is a run outcome, not a failure: the Session reports it as a Debug Console note.
  - The Session owns the delivered-batch number; frontends display it and don't count. `core.Delivered` carries it (1-based, reset on Restart). Target: today `debugger/dap` also counts (deferred-work, epic-safe-against-real-systems' refactor sweep).
  - No go-dap type appears outside `debugger/dap`.

### AD-4: Capture-then-step, with two configs from one rewrite [ADOPTED]

- **Binds:** U-REWRITE, U-STEP, U-CORE
- **Prevents:** pausing while the input is live; Kafka offsets committed by a debug session; two units editing the config.
- **Rule:** `debugger/rewrite` turns the pipeline into two configs:
  - **(a) Capture config:** only the user's input block, with `input.processors` removed and consumer groups overridden (AD-5).
  - **(b) Step config:** the steppers in order `input.processors` → `pipeline.processors` → `output.processors`, ending in `rpcn_debug_sink` (AD-21). Shared top-level `*_resources` sections carry over; `error_handling.strict` is applied with `StreamBuilder.SetStrict` (AD-6).

  U-STEP then:
  1. Runs (a) with `AddBatchConsumerFunc`, takes the first batch, and **never acks it**: the consumer returns an error, and the input is stopped before stepping starts. An input that closes first is `NOTHING_CAPTURED`.
  2. Truncates the batch to `maxMessages` (default 10, at most 1024).
  3. Feeds it into (b) through `AddBatchProducerFunc`.

  Further rules:
  - (a) carries the wrapped `*_resources`, but it is built on its own `env` clone whose `rpcn_debug_stepper` has a no-stop arriver: it pushes a frame and never stops, forks or reports a skip. Processors nested in the input tree, and the resources they call, run during capture without being stepped.
  - U-STEP never parses or edits configs.
  - Processors are built only through stepper fields (`NewProcessorListField`). `ConfigSpec.ParseYAML` is forbidden for processors, because it can't resolve `resource:` processors. The one exception is in AD-13.
  - A `replay` Source feeds a capture file (AD-11) instead of running (a).

### AD-5: Defaults that keep production safe [ADOPTED]

- **Binds:** U-REWRITE, U-STEP, U-EXT, U-DAP
- **Prevents:** a debug session stealing, deleting or rebalancing production data; writing to production outputs; sending telemetry to production systems.
- **Rule:**
  - **Input classes**, from a table in `debugger/rewrite`:

    | Class | Inputs | Behaviour |
    | --- | --- | --- |
    | replay-safe | Kafka family with a group override, `file`, `generate`, `aws_s3` | allowed |
    | side-effecting | queues and durable consumers: SQS, AMQP, NATS JetStream, `redis_list`, … | allowed only with `"allowSideEffects": true`, with a warning naming the effect |
    | rejected | `stdin`, `http_server`, sockets | rejected; replay only |
    | unknown | anything else | treated as side-effecting |

  - **Consumer groups:** every known consumer-group field is set to `rpcn-debug-<os-user>` unless `"keepConsumerGroup": true`. The table of fields lives in `debugger/rewrite` and recurses into `broker` and `sequence`.
  - **Output:** `output` is `dry-run` (the default; results go to VSCode only), `real`, or `none`. `real` is required to write to the user's output, and it logs a warning.
  - **Dropped sections:** the rewrite drops `metrics`, `tracer`, `http` and `logger`. The engine gets a no-op metrics and tracer and the AD-15 logger.
  - **`buffer`:** the top-level buffer is dropped with a launch warning that production batch boundaries may differ, like output `batching` (AD-21), and a corpus case pins it (target: an entry before epic-safe-against-real-systems; today it is dropped silently).
  - The captured batch is never acked, whatever the options.

### AD-6: The config rewrite is owned by one unit and never touches disk [ADOPTED]

- **Binds:** U-REWRITE, U-STEP, U-BLOBL, U-DAP
- **Prevents:** several places parsing the YAML differently; line numbers that disagree with engine errors; logs showing rewritten paths; user files being modified.
- **Rule:** Only `debugger/rewrite` parses or rewrites pipeline YAML. It uses `gopkg.in/yaml.v3`, the same library the engine uses, and never writes the user's file. `${VAR}` interpolations are kept verbatim; the engine resolves them (AD-22).
  - **What it wraps:** the child processor lists of `input`, `pipeline`, `output`, `branch`, `switch` cases, `try`, `catch`, `for_each`, `while`, `workflow.branches.*`, `parallel`, `group_by` groups and `retry`, and the same lists inside `processor_resources`. `workflow`, `parallel` and `retry` with `parallel` are wrapped from epic-concurrency on; until then they run opaque (AD-7).
  - **One rewrite per launch:** `Check` produces the configs, LineMap and Translator, and `Start` runs those same configs without reading the file again; `Restart` rewrites once more (AD-20). Target: today `Start` reads and rewrites the file a second time (retro A5).
  - **Unknown parents:** a parent not in this list (a third-party plugin) stays unwrapped. It runs as one opaque step, with a warning. A `retry` with an interpolated `parallel` counts as concurrent.
  - **Composite outputs:** the per-output processors of `broker`, `switch`, `fallback` and other composite outputs are not run in dry-run, with a warning (AD-21).
  - **Before wrapping**, YAML aliases and merge keys are expanded.
  - **Resources:** a `processor_resources` entry used under a concurrent or unknown parent stays unwrapped (tainted), with a warning. A `resource:` that names no defined label is `PIPELINE_INVALID` at the reference.
  - **Strict mode:** if `error_handling.strict` is true, every stepper except `catch`'s is written with `strict: true` (AD-7). An interpolated value is written verbatim into each stepper, and the engine resolves it through the session's env lookup (AD-22) for the step stream and the `STRICT_REJECTED` check. It uses the public `ConfigQuerier`, so it matches the stock builder; a value that isn't a bool is `PIPELINE_INVALID` at `error_handling.strict`.
  - **LineMap and Translate:** `LineMap` and `Translate(enginePath) userPath` are interfaces in `debugger/core`, implemented by `debugger/rewrite`. Every user-facing engine path and Bloblang error line, in logs and in error texts alike, goes through the one `Translate` before it reaches a frontend or stderr; the single slog handler that `Serve` builds does it for logs.
  - **Mapping base line:** for a literal (`|`) block, the base line is `Node.Line + 1`. For a plain scalar on the key's line, it is `Node.Line`. Statement line = base + the 0-based line inside the value. Folded (`>`) and quoted mapping values aren't statement-stepped; they run as one step, with a warning. U-REWRITE computes file lines, and U-BLOBL receives `(source, baseLine)`.

### AD-7: Each stepper mode copies its parent's engine behaviour [ADOPTED]

- **Binds:** U-STEP, U-REWRITE
- **Prevents:** debugger output that differs from the real run. Example: the `try` remainder running on a message that already failed.
- **Rule:** There are exactly two modes. `all` mirrors `ExecuteAll`, and when the stepper has `strict: true` it skips batches whose first message has an error, as the engine does in strict mode. `try` mirrors `ExecuteTryAll`. The mode comes from this table. Adding a parent requires a new row plus an equivalence case (AD-8).

| Parent | Engine function | Mode | Stepper entered | Repeat |
| --- | --- | --- | --- | --- |
| `input`/`pipeline`/`output`, `branch`, `switch` case, `workflow` branch | `ExecuteAll` | `all` | once per batch | none |
| `while` | `ExecuteAll` per loop | `all` | once per loop | `loop` |
| `group_by` group | `ExecuteAll` | `all` | once per group | `group` |
| `for_each`, `parallel` | `ExecuteAll` per message | `all` | once per message | `msg` |
| `retry` | `ExecuteAll` per message and attempt | `all` | once per attempt | `attempt` |
| `catch` | `ExecuteCatchAll`; the parent picks the failed messages and clears errors afterwards | `all`, written `strict: false` | once per caught message | none |
| `try` | `ExecuteTryAll` | `try` | once per message | `msg` |

Steppers accept and return several batches, because `group_by` splits and reorders them.

**Order inside `try` and `catch`:** the engine runs them processor-major (each child over every message, then the next child), and the debugger steps them message-major (one message through every child) for the `[msg i/n]` view. The output is the same unless children share state across messages. So the launch warns when a `try` or `catch` list holds a stateful child (`cache`, `dedupe`, `rate_limit`, …), and the AD-8 guarantee excludes that case (target: a rewrite entry before epic-safe-against-real-systems). `retry`'s `[attempt i/max]` takes max from `max_retries`. Until epic-concurrency, the concurrent rows (`workflow` branches, `parallel`, `retry` with `parallel`) run as one opaque step, with a warning.

### AD-8: The debugger must not change the output [ADOPTED]

- **Binds:** U-STEP, U-REWRITE, U-REL
- **Prevents:** silent changes in behaviour as processors or Connect versions are added.
- **Rule:**
  - For every case in `testdata/equivalence/`, the batches received by `rpcn_debug_sink` must equal those from the same pipeline run without the rewrite. "Equal" covers content, metadata, errors, batch boundaries and order. Each case runs three times: Continue, Next at every stop, and Step In at every stop.
  - **Same-batch rule:** a case's input yields exactly one batch of at most 10 messages, so the debugger captures the whole input.
  - The corpus covers every row of AD-7, plus strict mode.
  - The never-ack integration test (a Redpanda container, which must show no committed offsets for the debug group) and the capture round-trip test (AD-11) run in the same CI job.
  - Not covered, with a launch warning: stateful children of `try`/`catch` (AD-7), a top-level `buffer` (AD-5), and output `batching` (AD-21).
  - Equivalence is guaranteed only when nothing time-budgeted is paused. `retry` (backoff, `max_elapsed_time`) is the only time-budgeted parent the debugger can pause inside: the first stop of each stepper inside a `retry` emits a warning `Output` event. `branch` and `workflow` have no timeout, and pausing near a rate limit changes only throttling, not output.

### AD-9: How a stop is decided [ADOPTED]

- **Binds:** U-STEP, U-CORE, U-DAP
- **Prevents:** stop-event storms; data races on step state; nested-stepper deadlocks; steps on one thread stopping another.
- **Rule:**
  - Steppers reach the Session only through the `core.Arriver` port (`Captured`, `Enter`, `Skip`, `Arrive`, `Deliver`), which the Session implements.
  - At each stop position, a stepper calls `Arrive(ctx, stepperId, childIndex, batchCopies)`. `Arrive` takes the process-wide pause gate and evaluates the stop predicate **while holding it** (breakpoints, conditions, the step state of this thread, a pending `Pause`).
  - If the predicate is false, `Arrive` returns at once and emits no event. If it's true, it emits `Paused`, blocks until a resume, and returns any `SetMessage` edits (AD-19).
  - The gate is never held while a processor runs.
  - Step state is stored per thread. `Next`/`StepIn`/`StepOut` stop only the thread they were issued on. Other threads stop only on breakpoints or exceptions.
  - Every resume is process-wide (`allThreadsContinued: true`), and a stop is reported with `allThreadsStopped: false`. So far only the `continue` response carries the flag; the `continued` event for `next`/`stepIn`/`stepOut` arrives with epic-concurrency. `Pause` makes the next `Arrive` on any thread stop. Single-thread execution requests aren't supported.
  - Step order across concurrent threads is non-deterministic, while results stay deterministic (AD-8).

### AD-10: What a step is [ADOPTED]

- **Binds:** U-CORE, U-DAP, U-BLOBL, U-STEP
- **Prevents:** frontends or units inventing different stepping behaviour or repeat counts.
- **Rule:**
  - **Stop positions:** before child processor `i` of a stepper entry (`childIndex = i`), plus after its last child (`childIndex = n`, reported at the line of the parent's key).
  - **Step Over** goes to the next stop of the same entry, or the parent's next stop. At an entry's end stop it leaves the entry (for `for_each`, the message). At top level, the parent's next stop is the first stop of the next top-level list.
  - **Step In** enters the child stepper. On a `mapping`/`mutation`/`bloblang` processor or a `branch` `request_map`/`result_map`, it enters the Bloblang statements (AD-13).
  - **Step Out** runs to the end of the **current entry** (one message, attempt, loop or group), not of all entries.
  - A **frame** is one stepper entry. Its `repeatLabel` (`[msg i/n]`, `[attempt i/max]`, `[loop i]`, `[group i]`) has its format from the stepper (`Entry.Label`) and its counter from the Session, which keeps one counter per call site and calls `Label` once inside `Enter`. It is sent in `StepStarted`/`AttemptStarted`.
  - **Exception breakpoints:** `on-error` stops at the first stop after a processor sets a new error on any message.

### AD-11: One lossless, versioned Snapshot [ADOPTED]

- **Binds:** U-CORE, U-DAP, U-BLOBL, U-STEP
- **Prevents:** DAP Variables, replay files and tests each using a different message model; replay producing different output from the capture.
- **Rule:** `core.Snapshot` is the only representation of message state:
  `{schemaVersion, position{threadId, frameId, stepperId, childIndex, userPath, file, line}, batches[{messages[{index, content{raw(base64), text?, json?}, metadata[{key, type, value, text?, internal}], error?}]}], skipped[{index, reason}], bloblang?{root|unassigned, vars, meta, recomputed[]}}`.
  - **Metadata `type`** is one of the closed set `string | bytes | int | float | bool | timestamp | object | array | null`.
  - **`value` is lossless:** bytes are base64, ints are decimal strings, timestamps are RFC 3339 with nanoseconds. `text` is for display only.
  - Keys starting with `__` have `internal: true`. U-DAP shows them in a collapsed "internal" node under the message's metadata, and the core never filters them.
  - The Session keeps **one Snapshot per live frame**.
  - **Capture files:** only the capture Source writes them, and only on `SaveCapture`. A file holds the truncated batch plus `skipped` (reason `maxMessages`). Messages skipped by `try` are reported by `MessagesSkipped`, never in the file.
  - Readers reject a newer major `schemaVersion`. A round-trip test (capture → file → replay → Snapshot) must be byte-identical.

### AD-12: Thin extension; Go owns the launch contract [ADOPTED]

- **Binds:** U-EXT, U-DAP
- **Prevents:** pipeline logic duplicated in TypeScript; schema, defaults or enums drifting between TypeScript and Go.
- **Rule:**
  - The extension only:
    1. registers the debug type `redpanda-connect`
    2. contributes the generated `launch.json` schema
    3. gets the binary (AD-23)
    4. starts it over stdio (`DebugAdapterExecutable`)
    5. warns on a version mismatch (AD-14)
  - The Go struct `dap.LaunchConfig` is the only source of the schema, the defaults (struct tags) and the enum values. The schema in `package.json` is generated from it and checked in CI.
  - The extension adds nothing to the launch config except a missing `pipeline` (set to the active editor file) and `cwd` (set to the workspace folder). When the launch has none of `type`, `request` and `name` (F5 with no `launch.json`), it also fills them: `redpanda-connect`, `launch`, `Debug pipeline`.
  - It contributes breakpoints for the `yaml` language.
  - Relative paths resolve against `cwd`. Enum values are lowercase kebab-case. An unknown key or value is a `LAUNCH_INVALID` error, never ignored.
  - Custom DAP requests and events are allowed. The custom event `rpcnDelivered {batch, snapshot}` carries each `Delivered` Snapshot.
  - A future webview may only render Snapshots it gets through custom DAP requests or events.

### AD-13: Bloblang runs in Go; statement stepping is a view [ADOPTED]

- **Binds:** U-BLOBL, U-REWRITE, U-CORE, U-DAP
- **Prevents:** Watch or condition results that differ from the pipeline; two implementations of the statement-boundary algorithm; the debugger reporting a mapping result the engine didn't produce.
- **Rule:**
  - **Evaluation.** Every evaluation (Watch, `evaluate`, breakpoint conditions, `check` results, interpolation values):
    - runs in Go, against the paused thread's **copy** of the batch;
    - runs while the gate is held;
    - has batch context, through `MessageBatch.BloblangExecutor(exec).Query(i)`;
    - is parsed with the **pipeline's Bloblang environment**. That environment is reached by parsing the expression as a `NewBloblangField` through `ConfigSpec.ParseYAML(…, env)`, the only allowed use of `ParseYAML`.

    Every evaluation executor is newly parsed and never shared with pipeline processors. `BloblangQueryValue` is forbidden.
  - **Statement stepping** goes through the `BloblangTracer` port. U-BLOBL is the only owner of `Statements(source, baseLine) []StatementSpan`, and U-REWRITE calls it to put statement lines into `LineMap`. A boundary is where the prefix parses and the next line isn't indented.
    - v1 uses prefix replay: the state after statement k = running statements 1..k. `mutation` starts `root` from the input; `mapping` shows `root` as `unassigned` until its first assignment. Variables are read by appending `root = $name`.
    - Tracing runs lazily, only for the focused message and only on Step In or a statement breakpoint. Above 200 statements, tracing is disabled and the user is told.
  - **Recomputed values.** Values from `uuid_v4`, `uuid_v7`, `ulid`, `ksuid`, `nanoid`, `snowflake_id`, `now`, `timestamp_unix*`, `random_int`, `fake`, `counter`, `count` and `with_schema_registry_header` are labelled `recomputed`. Values from `env`, `file`, `file_rel` and `hostname` are labelled environment-dependent.
  - **Authority.** The trace is a view. The real processor's output is authoritative, and any difference is flagged.
  - A separate Bloblang interpreter is forbidden. A future upstream statement hook may replace prefix replay behind the same port.

### AD-14: Each release pins one Connect version and one launch schema [ADOPTED]

- **Binds:** U-REL, U-EXT, U-DAP
- **Prevents:** confusing "unknown field" errors when the user's Connect is newer; the extension and the binary disagreeing on the launch contract.
- **Rule:**
  - Each `rpcn-debug` release pins exactly one `connect/v4` (with its `benthos/v4`).
  - The binary reports `rpcnDebugVersion`, `rpcnConnectVersion` and `launchSchemaVersion` in the DAP `initialize` response.
  - The extension refuses a major mismatch of `launchSchemaVersion`. It warns when `rpk connect --version` differs by a minor or major version from `rpcnConnectVersion`.
  - A Connect bump requires the AD-8 job to pass.

### AD-15: stdout belongs to DAP [ADOPTED]

- **Binds:** U-DAP, U-STEP, U-CORE, U-REWRITE, U-REL
- **Prevents:** engine logs, stray prints or pipeline I/O corrupting the DAP stream.
- **Rule:**
  - When `Serve` runs over stdio, only the DAP encoder writes to `out`.
  - All logs go through one `log/slog` handler (after AD-6 translation). It writes Info and above to stderr, and sends engine records at Warn and above to DAP `Output` (console for warnings, stderr for errors). `Serve`'s `WithLogOutput` sets the stderr side.
  - The Session's own `Output` events (notes and warnings) go to the frontend directly, not through slog.
  - In stdio mode the rewrite rejects `stdin` inputs and redirects `stdout`/`stderr` outputs to the dry-run sink, with a warning, even under `output: real`.
  - `golangci-lint` enforces this together with AD-2: `forbidigo` bans `fmt.Print*` and `os.Stdout` in `debugger/...`, and `depguard` bans `internal/` imports.

### AD-16: Stepper identity comes from the rewrite [ADOPTED]

- **Binds:** U-REWRITE, U-STEP, U-CORE
- **Prevents:** LineMap keys that don't match what a stepper knows at runtime; frames that can't be told apart when one stepper config is entered many times.
- **Rule:**
  - The rewrite gives every stepper a dense integer `id`, unique within one rewrite, and writes it into the stepper config with `parent`, `mode`, `strict`, `concurrent`, `repeat` (`none`, `msg`, `attempt`, `loop` or `group`), `max_attempts` and `lines`.
  - `concurrent` is true when the parent runs entries on several goroutines: `workflow` branches, `parallel`, and `retry` with `parallel: true`.
  - `LineMap` is keyed by `(stepperId, childIndex)`.
  - A stepper never derives its identity from engine paths, labels or logger fields.
  - A `processor_resources` stepper has one `id`. Its call site comes from the frame stack (AD-17).

### AD-17: Frames and threads travel in ctx [ADOPTED]

- **Binds:** U-STEP, U-CORE, U-DAP
- **Prevents:** goroutine-ID hacks; concurrent siblings collapsing into one thread; thread IDs changing on every step.
- **Rule:**
  - The frame stack is an immutable list stored in `context.Context` under a key owned by `debugger/core`. Each stepper entry pushes one frame `{frameId, stepperId, repeatLabel, callSite}` and passes the new ctx to its children.
  - A stepper calls `Arriver.Enter` for each entry. The Session pushes the frame and forks a thread when the config has `concurrent: true` or ctx has no frame; otherwise the entry inherits the parent frame's thread. The entry's exit ends a thread it forked. The Session emits `ThreadStarted`/`ThreadExited`.
  - The root thread (named `batch`) starts at `ConfigurationDone`.
  - Using goroutine IDs is forbidden.
  - A stepper with a non-root `parent` that finds no frame in ctx logs `CTX_LOST`. The AD-8 corpus has one case per AD-7 parent that proves ctx reaches the child.
  - `threadId` is stable for the life of its thread. Only `frameId` and `variablesReference` become invalid on resume.

### AD-18: Breakpoints are Session state [ADOPTED]

- **Binds:** U-CORE, U-DAP, U-REWRITE, U-BLOBL
- **Prevents:** two owners of breakpoints; breakpoints that are verified but never hit; units choosing different condition semantics.
- **Rule:**
  - Breakpoints arrive as `SetBreakpoints{file, lines[], conditions[]}`. Those received before the rewrite stay pending and are resolved at the end of `Launch`. Stepping starts only after `ConfigurationDone`.
  - **Resolution:**
    - A line inside processor P's YAML span binds to "before P", and is reported verified at P's first line.
    - A line inside a statement-steppable mapping binds to the statement whose span contains it.
    - Any other line is unverified. Each result is sent as `BreakpointResolved`.
  - **Hitting:**
    - A statement breakpoint makes `Continue` enter statement stepping of that processor.
    - A breakpoint inside a `processor_resources` entry hits at every call site.
    - A condition is evaluated for each message (AD-13). It stops if **any** message matches, and the Snapshot focuses the first matching one.
  - Logpoints and hit counts aren't supported in v1.

### AD-19: The stepper owns the batch it is processing [ADOPTED]

- **Binds:** U-STEP, U-CORE, U-BLOBL
- **Prevents:** snapshots showing values that never existed; `SetMessage` racing a processor; two units modifying one message.
- **Rule:**
  - A stepper owns its batch for its whole entry. At each `Arrive` it passes a **deep copy** (content bytes, metadata and errors), and the Session builds the immutable Snapshot from that copy. A Snapshot never references engine objects.
  - `SetMessage{frameId, batch, index, content?, metadata?}` is valid only for the top frame of the stopped thread. The Session validates it and returns it from `Arrive`. The **stepper** applies it to its own batch before releasing the gate.
  - No other unit modifies a `*service.Message`.

### AD-20: Restart re-steps the same batch [ADOPTED]

- **Binds:** U-CORE, U-DAP, U-EXT, U-STEP
- **Prevents:** the Restart button producing a different batch depending on which code path handles it.
- **Rule:**
  - `Restart` re-reads and re-rewrites the pipeline YAML (a new LineMap, with breakpoints resolved again), discards all threads and frames and the cloned env, and re-steps the **already captured batch** through an in-memory replay Source.
  - It never re-captures. A fresh capture is a new launch.
  - U-DAP advertises `supportsRestartRequest: true`.
  - If the rewrite fails, the session stays alive and reports a `core.Error` with `UserPath`/`Line`.

### AD-21: Delivery happens at `rpcn_debug_sink` [ADOPTED]

- **Binds:** U-STEP, U-REWRITE, U-CORE, U-REL
- **Prevents:** dry-run silently dropping `output.processors`; the boundaries the sink compares drifting between dry-run and real; delivered results that have no shape.
- **Rule:**
  - The step config's output is always `rpcn_debug_sink`. It receives batches **after** `output.processors` and emits `Delivered{Snapshot}`.
  - In `dry-run` it records the batches exactly as received (`MaxInFlight = 1`). It doesn't apply output-level `batching`, and warns that production boundaries may differ.
  - Dry-run delivers what reaches `output`, before per-output processing: a composite output's per-output processors don't run, with a warning (AD-6). Their routing (`round_robin`, `greedy`, `fallback`) can't be reproduced without the real output; `real` runs them.
  - In `real` it also forwards each batch to the user's output, built from the original block. In `none` it discards them.
  - AD-8 compares what the sink received.

### AD-22: Secrets and captured data [ADOPTED]

- **Binds:** U-DAP, U-EXT, U-STEP, U-CORE, U-REWRITE
- **Prevents:** credentials in committed `launch.json`; production data left in the workspace; env vars resolved differently by different units; components built before the license is applied; a session hanging on an unlicensed resource.
- **Rule:**
  - **Credentials in `launch.json`:** `env` values accept `${env:NAME}` references, and `envFile` names a file. `license` accepts only a file path or an env reference, never a literal. The schema says so.
  - **Env resolution:** the engine resolves `${VAR}` in the pipeline through one lookup that the Session builds, in this order: process env, then `envFile`, then `env`. It is set with `StreamBuilder.SetEnvVarLookupFunc` (and used for an interpolated `error_handling.strict`, AD-6).
  - **`envFile`** holds `NAME=value` lines, with `#` comments, blank lines, an optional `export` prefix and quoted values. The file must exist.
  - **License** (not built yet; owned by epic-local-release, applied by U-STEP to every stream it builds): read from `license`, then `REDPANDA_LICENSE`, then `REDPANDA_LICENSE_FILEPATH`, then `/etc/redpanda/redpanda.license`. It is applied with `public/license.StoreCustomLicenseBytes(strm.Resources(), …)` after `Build()` and before `Run()`, on every stream the Session builds. A failure to obtain an enterprise `processor_resources` component is a session error `LICENSE_REQUIRED`, never a silent retry.
  - **Capture files:** written only on `SaveCapture`, with mode `0600`. The first save creates `.rpcn-debug/.gitignore` containing `*`.
  - **Credentials for the input system:** only from the pipeline config. The debugger adds none.
  - **Evaluate** is unrestricted (it's a local development tool), and the docs say so.
  - **Telemetry:** none is collected by the binary or the extension.

### AD-23: How the extension gets the binary [ADOPTED]

- **Binds:** U-EXT, U-REL
- **Prevents:** the extension and the release pipeline disagreeing on artifact names or versions; running an unverified downloaded binary.
- **Rule:**
  - Binary resolution order: the `rpcnDebug.binaryPath` setting (machine-scoped, so a workspace can't point it at another binary), then a bundled binary, then a download.
  - **MVP (local distribution):** only the first two apply: the setting, then the binary bundled in a locally built `.vsix`. The download step arrives with the Marketplace release.
  - Extension `X.Y.*` downloads binary `X.Y.*` from `<releases>/vX.Y.Z/rpcn-debug_<os>_<arch>[.exe]`.
  - Before running a downloaded binary, the extension checks its SHA-256 against the release's signed `checksums.txt`. If the check fails, it doesn't run it.

## Consistency Conventions

| Concern | Convention |
| --- | --- |
| Names | DAP type `redpanda-connect`; plugins `rpcn_debug_stepper` and `rpcn_debug_sink` (must match `^[a-z0-9]+(_[a-z0-9]+)*$`); binary `rpcn-debug` |
| User paths | `pipeline.processors[2].branch[1].switch.case[0]`, `workflow.branches.a`, `group_by.group[1]`, `processor_resources.<label>`, with indices 0-based |
| Lines | 1-based everywhere (yaml.v3, and DAP `linesStartAt1: true`) |
| DAP handles | `threadId` stable per thread; `frameId` and `variablesReference` become invalid on every resume (AD-17). Limits: 32 frames, 64 batches and 1024 messages per stop; stop ids wrap modulo 64 |
| Errors | Processor errors on messages are data (`Snapshot.error`). Session failures are `core.Error` (AD-3) |
| Logging | `log/slog`, one handler, stderr plus DAP `Output` (AD-15) |
| Launch keys | camelCase: `pipeline`, `cwd`, `resources[]`, `maxMessages`, `output` (`dry-run`, `real` or `none`), `keepConsumerGroup`, `allowSideEffects`, `rpcPlugins[]`, `replay`, `env`, `envFile`, `license`. Built so far (E2): `pipeline`, `cwd`, `maxMessages`, `env`, `envFile` |
| Equivalence tests | `testdata/equivalence/<name>/{pipeline.yaml, expect.yaml, input files}`. `expect.yaml` declares the reference `batches` and `messages`, Step In `stops` (path, frame count), `no_stops_under` paths and required `output` lines (AD-8) |
| CI lint | `golangci-lint` with `forbidigo` and `depguard` (AD-2, AD-15); a launch-schema check (AD-12) |

## Stack

| Name | Version |
| --- | --- |
| Go | 1.27.x (`go 1.27.3`, which Connect v4.115.0 requires) |
| github.com/redpanda-data/connect/v4 | v4.115.0 |
| github.com/redpanda-data/benthos/v4 | v4.84.0 |
| github.com/google/go-dap | v0.12.1-0.20260926015354-7505f696918e |
| gopkg.in/yaml.v3 | v3.0.1 (archived upstream; kept because benthos uses it directly) |
| TypeScript | 6.0.3 |
| VS Code minimum (`engines.vscode`) / @types/vscode | ^1.120.0 / 1.120.0 |
| @vscode/vsce | 4.0.0 |
| @vscode/debugadapter-testsupport | 1.68.0 |

## Structural Seed

```text
rpcn-debugger/
  go.mod                      # module github.com/ajurcenk/rpcn-debugger
  debugger/                   # public entry: Serve, Option
    core/                     # Session, protocol.go, errors.go, Snapshot, frames, gate, LineMap iface
    engine/                   # capture, rpcn_debug_stepper, rpcn_debug_sink
    rewrite/                  # capture + step configs, LineMap, input-class and group-field tables
    blobl/                    # Statements, prefix replay, evaluator
    dap/                      # DAP server, LaunchConfig (schema source)
  cmd/rpcn-debug/             # thin main (AD-1)
  cmd/gen-launch-schema/      # launch.json schema from dap.LaunchConfig (AD-12)
  test/dapdrive/              # scripted DAP client for tests
  testdata/equivalence/       # AD-8 corpus
  vscode/                     # TypeScript extension (AD-12, AD-23)
```

```mermaid
flowchart LR
  subgraph laptop[Developer machine]
    VS[VSCode + extension] -- DAP over stdio --> BIN[rpcn-debug or user debug main]
    BIN -. SaveCapture only .-> CAP[(.rpcn-debug/captures 0600)]
  end
  BIN -- capture config: 1 batch, never ack, debug group --> SRC[(Input system e.g. Redpanda)]
  BIN -. only with output: real .-> OUT[(Real output)]
  PROD[Production rpk connect / image] --- SRC
```

Environments: the debugger runs only on a developer machine, next to the editor, with network access to the input system. Production deployments are never changed. CI: an equivalence and integration job (AD-8), lint (AD-15), schema check (AD-12), and release builds per OS/arch (AD-23).

```mermaid
sequenceDiagram
  participant FE as Frontend (DAP)
  participant S as core.Session
  participant ST as rpcn_debug_stepper
  participant P as OwnedProcessor
  FE->>S: Launch, SetBreakpoints, ConfigurationDone
  S->>ST: capture config → 1 batch → step config
  ST->>S: Arrive(ctx, id, i, copy) [gate held]
  S-->>FE: Paused
  FE->>S: Next
  S-->>ST: return (edits?) [gate released]
  ST->>P: ProcessBatch (no gate)
  P-->>ST: batches
  ST->>S: Arrive(ctx, id, i+1, copy)
```

## Unit → Architecture Map

| Unit | Lives in | Governed by |
| --- | --- | --- |
| U-CORE | `debugger/core` | AD-1, AD-2, AD-3, AD-4, AD-9, AD-10, AD-11, AD-13, AD-15, AD-16, AD-17, AD-18, AD-19, AD-20, AD-21, AD-22 |
| U-REWRITE | `debugger/rewrite` | AD-1, AD-2, AD-4, AD-5, AD-6, AD-7, AD-8, AD-13, AD-15, AD-16, AD-18, AD-21, AD-22 |
| U-STEP | `debugger/engine` | AD-1, AD-2, AD-3, AD-4, AD-5, AD-6, AD-7, AD-8, AD-9, AD-10, AD-11, AD-15, AD-16, AD-17, AD-19, AD-20, AD-21, AD-22 |
| U-BLOBL | `debugger/blobl` | AD-1, AD-2, AD-3, AD-6, AD-10, AD-11, AD-13, AD-18, AD-19 |
| U-DAP | `debugger/dap` | AD-1, AD-2, AD-3, AD-5, AD-6, AD-9, AD-10, AD-11, AD-12, AD-13, AD-14, AD-15, AD-17, AD-18, AD-20, AD-22 |
| U-EXT | `vscode/` | AD-5, AD-12, AD-14, AD-20, AD-22, AD-23 |
| U-REL | `cmd/`, CI | AD-1, AD-8, AD-14, AD-15, AD-21, AD-23 |

## Deferred

| Item | Why it can wait |
| --- | --- |
| DAP over TCP; running the debugger in a container or Kubernetes pod | The v1 environment is a developer laptop. The `Frontend` port doesn't change |
| MCP and TUI frontends | The port exists (AD-3) |
| Upstream Bloblang statement hook; upstream `rpk connect debug` | Each sits behind an existing port or `Serve` (AD-1, AD-13) |
| Webview (pipeline graph, message diff) | Bound by AD-12 (render Snapshots only) |
| Logpoints and hit-count breakpoints | Excluded from v1 by AD-18 |
| Stepping `fan_out` broker and `switch` output cases in dry-run, one sink batch per case | Their routing is deterministic, but nobody has asked for it; dry-run warns and `output: real` runs them (AD-21) |
| Release hosting, signing tool, Marketplace / Open VSX publication, binary download | Post-MVP. The MVP is a local `.vsix` (personal project). The contract is fixed by AD-23 |

## Open Questions

| Question | Blocks | Revisit when |
| --- | --- | --- |
| **Resolved (spike 1.6):** license timing. `Build()` constructs nothing; inputs and processors are built in `Run()`, and `processor_resources` lazily. Applying `StoreCustomLicenseBytes(strm.Resources(), …)` between `Build()` and `Run()` licenses all of them. Without a license, `processor_resources` don't fail `Run()`: each message logs ERROR and retries | U-REL, enterprise components | Closed |
| Stable message identity across `group_by` reordering, for diffs and tests, without adding metadata that changes the output | U-CORE diffs, webview | Before the message-diff feature |
| **Resolved (spike 1.6):** binary size and CGO. Stripped `rpcn-debug` is 310–346 MB on linux/darwin amd64/arm64 and windows/amd64 (linux/amd64 is 92 MB gzipped). All targets build with `CGO_ENABLED=0`, and only the `tigerbeetle_cdc` input is lost without CGO | U-REL | Closed |
| Upstream goal (the user mix is confirmed: mostly stock `rpk connect` on laptops) | AD-1 emphasis | If upstream `rpk connect debug` becomes the main channel |
| Hexagonal ports were taken without stated roadmap priorities | AD-3 port surface | If no second frontend or replay use appears within 3 releases |
| **E3 design (adversary review 2026-10-22, H1–H6, H8, H9):** one input table and one input-tree walker for classes, group fields and stdin, with the Session alone refusing; the `output: real` config shape and a `Delivered` outcome (recorded, written, discarded, failed), plus a `STRICT_REJECTED` decision that isn't based on error text; the consumer-group base owner and per-field names, a kept group being side-effecting, and a "waiting for the input" note; Restart/replay under `real`; side-effecting processors in dry-run; the top-level `redpanda` block | epic-safe-against-real-systems | Before slicing E3 (its inception), per `reviews/review-adversary-2026-10-22.md` |
