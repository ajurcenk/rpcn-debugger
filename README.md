# rpcn-debugger

A visual debugger for [Redpanda Connect](https://docs.redpanda.com/redpanda-connect/) pipelines in VS Code. It captures one batch from a pipeline's input and steps it through the processors over the Debug Adapter Protocol.

> **Status: tracer bullet.** A launch captures one batch from the pipeline's input without acknowledging it, stops before the first processor (input, then pipeline, then output processors), shows the stop's line and each message's content, and steps the batch through the input, pipeline and output processors, nested ones included. Breakpoints come later.

## Layout

| Path | Contents |
| --- | --- |
| `debugger/` | The Go library: the public entry point, and the `core`, `engine`, `rewrite`, `blobl` and `dap` packages |
| `cmd/rpcn-debug/` | The stock binary: every Connect component, built on the library |
| `vscode/` | The VS Code extension |
| `test/equivalence/` | The equivalence harness: discovers the corpus cases, runs each pipeline without the debugger and through it, and compares the delivered batches |
| `testdata/equivalence/` | The equivalence corpus: one directory per case, `<name>/pipeline.yaml`, one input batch of at most 10 messages each |
| `testdata/tracer/` | The sample pipeline to try the debugger on |
| `Taskfile.yml` | Build, test and package tasks |

## Prerequisites

- Go 1.27 (the `go 1.27.3` toolchain is fetched automatically if needed)
- Node.js 22 or later (required by `@vscode/vsce` 4)
- [Task](https://taskfile.dev)
- [jq](https://jqlang.org), used by `task lint:gomod`
- Docker, used by the integration tests (`task test:integration`), which start a Redpanda container. The task keeps testcontainers' Ryuk reaper on and fails if a test container is left behind
- VS Code 1.120 or later, to install the extension

The binary is pure Go (`CGO_ENABLED=0`), so no C toolchain or system libraries are needed.

## Build

```sh
task ci
```

This builds `bin/rpcn-debug`, lints the Go code, runs the Go unit tests, the equivalence cases and the integration tests, compiles the extension, runs the extension's DAP test against the built binary, and packages the extension as `vscode/dist/pipeline-debugger-for-redpanda-connect-<version>.vsix`. The individual steps are `task build:go`, `task lint:go`, `task lint:gomod`, `task lint:schema`, `task test:go`, `task test:equivalence`, `task test:integration`, `task build:ext`, `task test:ext` and `task package:ext`.

`task test:equivalence` runs every case in `testdata/equivalence/` through its own input and processors, with no debugger, then through the debugger three times (Continue at the entry stop, Step Over at every stop, Step In at every stop), and fails when a run delivers different batches (content, metadata, errors, boundaries or order). It lists each case as a subtest. To add a case, create `testdata/equivalence/<name>/pipeline.yaml` (its `output` is replaced by a collector, so any output will do) and an `expect.yaml` beside it declaring the reference's batches and messages and the Step In stops, as the existing cases do. A case's input must yield exactly one batch of at most 10 messages (the `maxMessages` default), so the debugger captures the whole input; the harness fails a case that breaks this rule.

`task test:ext` starts the binary named by `RPCN_DEBUG_BIN` (default `../bin/rpcn-debug`, relative to `vscode/`), so run `task build:go` and `task build:ext` first.

The lint gates enforce two architecture rules:

- `debugger/...` never writes to stdout, which belongs to DAP (forbidigo).
- Only the `public/...` packages of benthos and connect are imported, and go.mod has no `replace` for them (depguard and `lint:gomod`).

golangci-lint is pinned in `tools/go.mod` and runs as `go tool -modfile=tools/go.mod golangci-lint`, so you don't need to install it.

The first build compiles every Connect component and takes a few minutes.

```sh
bin/rpcn-debug --version
# rpcn-debug <version> (connect v4.115.0, benthos v4.84.0)
```

## Install the extension

```sh
code --install-extension vscode/dist/pipeline-debugger-for-redpanda-connect-*.vsix
```

Or, in VS Code, run **Extensions: Install from VSIX...** and pick the file. "Redpanda Connect" then appears in the debugger list under **Run and Debug > create a launch.json file**.

The extension doesn't bundle the binary yet. Point the `rpcnDebug.binaryPath` setting at the one you built, as an absolute path to `bin/rpcn-debug` in this repo. Set it in your user settings (**Preferences: Open User Settings (JSON)**):

```json
"rpcnDebug.binaryPath": "/path/to/rpcn-debugger/bin/rpcn-debug"
```

The setting is machine-scoped, so a workspace's `.vscode/settings.json` can't change which binary VS Code runs. If it is unset, starting a session shows an error asking you to set it.

## Try it

Open `testdata/tracer/pipeline.yaml` and press F5 (with no `launch.json`, or a `Debug pipeline` entry without `pipeline`): VS Code stops before the mapping with the captured message under **Variables** (Batches › batch 0 › message 0: content, metadata, and an `internal` node for `__` keys); Step Over moves to the end stop on the `processors:` line, and the next Step Over ends the session. Breakpoints show as unverified for now.

Stepping: the debugger stops before each processor and once after the last one of each list (on the list's key line). Step Over (F10) runs a processor, nested children included; Step In (F11) enters a parent's processors (`branch`, `switch` cases, …), adding a frame to the call stack, each frame with its own batch under Variables; Step Out (Shift+F11) runs to the end of the current list and then back to its parent; Pause stops at the next processor.

Repeated entries carry a label in the call stack: `[msg i/n]` under `for_each` and `try`, `[attempt i/max]` under `retry` (`[attempt i]` when `max_retries` is 0), `[loop i]` under `while` and `[group i]` under `group_by`; the Debug Console reports each retry attempt, the messages a `try` (or `error_handling.strict`) skips, and a warning the first time you pause inside a `retry`, since that changes its backoff timing. Samples for each parent are in `testdata/tracer/parents/`.

At the end of the run the Debug Console shows each batch delivered to the dry-run output (after `output.processors`): `Delivered batch i (n messages) to dry-run output`, then each message's content (cut at 4 KiB), its metadata as `key = value` lines (internal `__` keys marked) and its error, or `Nothing was delivered` when every message was deleted. By default (`"output": "dry-run"`) nothing is written to your real output, and output `batching` isn't applied (a launch warning says so), so production batch boundaries may differ. `"output": "none"` also writes nothing. `"output": "real"` writes each delivered batch to the pipeline's output once, after `output.processors` (which run only once, in the stepper), with a launch warning naming the output; the output then runs its per-output processors (`broker`, `switch` cases, …) as in production, but its `batching` is removed (with a warning: the captured batch is written as one batch), every `stdout` or `stderr` output, or `file` output to `/dev/stdout`-like paths, in it or in `output_resources` is replaced by `drop`, since stdout carries the DAP stream, and a `redpanda_common` output (or `redpanda` without `seed_brokers`) fails the launch. A write the output refuses fails the run; an output that can't connect retries, as in production, until you stop the session. (The Debug Console text still says "dry-run output" in every mode for now.)

A `redpanda-connect` launch.json entry takes `pipeline` (default: the active file), `cwd` (default: the workspace folder; relative paths resolve against it), `maxMessages` (default 10: the captured batch is cut to that many, and the Debug Console says so; try `testdata/tracer/batch12.yaml`), `output` (`dry-run`, the default, `real` or `none`; see above), `env` and `envFile` (the pipeline's `${VAR}` resolve from the debugger's environment, then the dotenv `envFile`, then `env`, the later one winning; use `${env:NAME}` rather than literal secrets), and `allowSideEffects` and `inputClasses` (see [What a debug session does to your input](#what-a-debug-session-does-to-your-input)). Any other key fails the launch. Engine warnings and processor errors show in the Debug Console, naming your processor path (`pipeline.processors[1]`) and, for a Bloblang error in a `|` block or a one-line plain mapping, its file line; every log line, Info included, also goes to the debugger's stderr. The schema VS Code validates against is generated from Go: after changing `dap.LaunchConfig`, run `task gen:schema` (`task lint:schema`, part of `task ci`, fails on a stale `vscode/package.json`).

## What a debug session does to your input

A session reads **one batch** from the pipeline's input, keeps at most `maxMessages` of it, steps it, and stops the input. It **never acknowledges** anything, whatever the launch options:

- The capture returns an error (a nack) for every batch it receives, and the input is stopped right after the first one.
- `auto_replay_nacks` is forced to `true` on every input in the capture, with a launch warning when the pipeline sets anything else. With it off, many inputs treat a nack as an ack (committing Kafka offsets, confirming a Postgres slot's LSN, advancing CDC checkpoints).
- A Kafka-family input (`kafka`, `kafka_franz`, `redpanda` with `seed_brokers`) reads under the debug consumer group `rpcn-debug-<os-user>-<k>`, never under the pipeline's own group, and commits nothing; a `nats` input's `queue` gets the same treatment. This applies anywhere in the input tree, `broker`/`sequence` children and `input_resources` included, and `k` counts the overridden inputs in order from 0, so distinct groups stay distinct. Each replacement gets a launch warning naming both groups. The records stay in the topic, production's offsets and membership don't change, and the next session captures the same messages. The debug group stays on the cluster as an empty group (no members, no offsets) until the broker expires it or `rpk group delete` removes it, and joining it needs Group READ permission on that name.

Before anything connects, the launch checks every input in the input tree (`broker`, `sequence`, `read_until`, `batched`, `resource:` and the rest), using a built-in table of the stock binary's inputs:

| Class | Examples | What the launch does |
| --- | --- | --- |
| Replay-safe: reading changes nothing at the source | a Kafka-family input or `nats` with an overridden group, `file`, `aws_s3` without `sqs`, `generate`, `http_client` with GET | Launches |
| Side-effecting: receiving alone changes the source, or it isn't proven safe | a kept or interpolated consumer group, `aws_sqs`, `gcp_pubsub`, `amqp_0_9`, `amqp_1`, `redis_streams`, `postgres_cdc`, `azure_queue_storage`, `mysql_cdc` | Fails with `SIDE_EFFECTS_NOT_ALLOWED` (1012) at the input's line, naming each effect, unless `allowSideEffects` is `true` |
| Unknown: no row in the table | a plugin input | Same as side-effecting |
| Rejected: can't be debugged | `stdin` (it carries the DAP stream), `http_server`, `socket_server`, `dynamic`, `redpanda_common` and `redpanda` without `seed_brokers` (they read through the top-level `redpanda` block's shared client, which only `rpk connect` sets up) | Always fails with `INPUT_REJECTED` (1011), with a hint to replay a saved capture instead (coming with replay) |

The class can depend on fields: `aws_s3` with `sqs`, `csv` with `delete_on_finish` or `http_client` with a write method is side-effecting.

**`allowSideEffects: true`** lets a side-effecting or unknown input launch, with one warning per input naming its effect. It doesn't change the capture: nothing is acked, and it can't unlock a rejected input. What receiving does to such a source, even without an ack:

| Input | What happens to the received messages |
| --- | --- |
| `aws_sqs` | Hidden from other consumers until the visibility timeout ends, then back in the queue; the receive count goes up, and at the redrive policy's `maxReceiveCount` SQS moves the message to the DLQ |
| `gcp_pubsub` | Redelivered after the ack deadline |
| `amqp_0_9`, `amqp_1` | Requeued when the debugger's channel closes, marked redelivered |
| `nats_jetstream` (durable) | Pending on that consumer until `ack_wait`, then redelivered; the delivery count goes up |
| `redis_streams` | The group's cursor moves past the entries, so the next session doesn't read them again |
| CDC inputs (`postgres_cdc`, …) | The replication slot or stream is held open while capturing; no position is confirmed |
| `azure_queue_storage` | **Deleted**: the input deletes messages even on a nack |
| `spicedb_watch` | Writes its position token to the cache even on a nack |

Use a dev copy of the source when you can't afford those effects.

**`inputClasses`** overrides the table for the session, by component name: `"inputClasses": {"mysql_cdc": "replay-safe"}` marks one you know is safe (the launch warns that the debugger can't prove it), and `{"generate": "rejected"}` makes one stricter. A composite follows its overridden children. `stdin`, `dynamic` and `redpanda_migrator` can't be loosened; an unknown name or class fails the launch.

**`keepConsumerGroup: true`** keeps every consumer group as configured. The input then joins the production group, whose members rebalance, so it counts as side-effecting and needs `allowSideEffects`. A group the debugger can't override (an interpolated `${GROUP}`) is treated the same way. Other group-like fields (a Postgres `slot_name`, a Pulsar `subscription_name`, a Pub/Sub `subscription`, checkpoint tables, NATS JetStream durables) are never overridden, because a new name would create durable objects on the production system.

Only `input`, the processor lists, `output`, `error_handling` and the `*_resources` sections reach the engine. `buffer` and the top-level `redpanda` block are dropped with a warning; `metrics`, `tracer`, `http`, `logger`, `shutdown_*` and `tests` are dropped silently.

Start offsets stay as configured. One that resolves to latest (`start_offset: latest`, `start_from_oldest: false`) gets a launch warning, since the capture then waits for new records. When the input has sent nothing after 5 seconds, the Debug Console shows one "Waiting for the input" note.

## License

[MIT](LICENSE)
