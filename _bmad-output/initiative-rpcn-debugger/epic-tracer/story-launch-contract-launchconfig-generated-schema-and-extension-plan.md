---
title: 'Launch contract: LaunchConfig, generated schema and extension wiring'
type: 'feature'
ticket: '3'
created: '2026-10-19'
status: 'built'
baseline_revision: '61ffd0a6fa40fedc286fe77318a69ef1afdd9c62'
route: 'full'
route_source: 'auto'
risk: 'medium'
review: 'quick'
review_source: 'pinned'
lenses_ran: ['quick']
review_loop_iteration: 0
context:
  - '{project-root}/_bmad-output/initiative-rpcn-debugger/architecture-rpcn-debugger/architecture-rpcn-debugger.md'
---

<frozen-after-approval reason="human-owned intent — do not modify unless human renegotiates">

## Intent

**Problem:**
- The launch decodes `pipeline`/`cwd` leniently and ignores everything else. A typo such as `maxMesages` is silently dropped (against AD-12).
- `launch.json` has no schema in VS Code, so there's no IntelliSense or validation, and the hand-written `initialConfigurations` can drift from Go.
- The capture steps the whole first batch however large it is.

**Approach:**
- `dap.LaunchConfig` becomes the single Go source of the launch contract: keys, defaults in struct tags, descriptions and constraints. It covers `pipeline`, `cwd`, `maxMessages` (default 10), `env` and `envFile`.
- The launch decodes strictly into it. An unknown key or a bad value is `LAUNCH_INVALID`, and relative paths resolve against `cwd`.
- A Go generator writes the `launch` schema (`configurationAttributes`) and `initialConfigurations` into vscode/package.json, and `task ci` fails on drift.
- The capture truncates to `maxMessages` and tells the user how many messages it kept.
- The extension fills only a missing `pipeline` (the active editor) and `cwd` (the workspace folder).

## Boundaries & Constraints

**Always:**
- AD-12:
  - `dap.LaunchConfig` (struct tags) is the only source of the schema, the defaults and the constraints. package.json's `configurationAttributes.launch` and `initialConfigurations` are generated, never hand-edited.
  - Keys are camelCase. Enum values are lowercase kebab-case (none in this entry).
- Unknown keys are `LAUNCH_INVALID`, except the keys VS Code itself adds to every launch config: `type`, `request`, `name`, `presentation`, `preLaunchTask`, `postDebugTask`, `internalConsoleOptions`, `debugServer`, `noDebug`, `suppressMultipleSessionWarning`, and any key starting with `__`. This allow-list lives in Go beside `LaunchConfig`.
- Values are validated:
  - `pipeline` is a non-empty string;
  - `cwd` is a string;
  - `maxMessages` is an integer ≥ 1;
  - `env` is an object of string values;
  - `envFile` is a string naming an existing file after resolution.

  Each failure is `LAUNCH_INVALID`, and the message names the key.
- Relative `pipeline` and `envFile` resolve against `cwd`. A relative `cwd` resolves against the process working directory (AD-12).
- The schema descriptions say that `env` values accept `${env:NAME}`, which VS Code substitutes, and that literal secrets don't belong in `launch.json` (AD-22).
- The truncation keeps the first `maxMessages` messages of the captured batch. When it drops any, an Output event says "captured N of M messages (maxMessages)". Nothing is acked either way.
- `env` and `envFile` are parsed, validated and carried to the Session in `core.Launch`, but not applied. Resolving `${VAR}` through them is 2.4.

**Never:**
- No `env`/`envFile` lookup behaviour (2.4). No `output`, `resources[]`, `rpcPlugins`, `license`, `replay` or `keepConsumerGroup` keys (later epics add them to `LaunchConfig`, which regenerates the schema).
- No AD-14 `launchSchemaVersion` (epic-local-release).
- The extension doesn't validate or add any key beyond `pipeline` and `cwd`, except the existing type/request/name fill when F5 runs with no launch.json.

## I/O & Edge-Case Matrix

| Scenario | Input / State | Expected Output / Behavior | Error Handling |
|----------|--------------|---------------------------|----------------|
| Defaults | `{type, request, name, pipeline}` | Launch succeeds with `maxMessages` = 10 | (none) |
| VS Code keys | Config plus `__sessionId`, `__configurationTarget`, `presentation` | Accepted | (none) |
| Unknown key | `maxMesages: 5` | No launch | `LAUNCH_INVALID`, the message names `maxMesages` |
| Bad value | `maxMessages: 0`, `maxMessages: "5"`, `env: {A: 1}` | No launch | `LAUNCH_INVALID`, the message names the key |
| Relative paths | `cwd: /w`, `pipeline: p.yaml`, `envFile: .env` | Resolves to `/w/p.yaml` and `/w/.env` | A missing env file is `LAUNCH_INVALID` naming `envFile` |
| Truncation | Input batch of 12, default `maxMessages` | 10 messages at the first stop; Output "captured 10 of 12 messages (maxMessages)" | (none) |
| No truncation | Batch of 3, `maxMessages` 10 | 3 messages, no Output | (none) |
| Schema drift | `LaunchConfig` changed without regenerating | `task ci` fails, naming the stale file | (none) |
| Extension fill | launch.json entry without `pipeline`/`cwd`, editor on `a.yaml`, workspace `/w` | Config sent with `pipeline: <a.yaml>` and `cwd: /w`; set values are untouched | (none) |

</frozen-after-approval>

## Code Map

- `debugger/dap/server.go`, LaunchRequest case (~lines 109–122): decodes `launchArgs{Pipeline, Cwd}` with plain `json.Unmarshal` and calls `s.do(core.Launch{Pipeline, Cwd})`. Replace with a strict decode into `LaunchConfig`, then validation, then `core.Launch`. Keep the `initialized` event after success.
- New `debugger/dap/launch.go`:
  - `LaunchConfig` with `json:"…"` tags plus schema tags (e.g. `default:"10" min:"1" desc:"…"`);
  - `vsCodeKeys`;
  - `decodeLaunch(raw json.RawMessage) (LaunchConfig, error)`, which checks unknown keys by decoding into `map[string]json.RawMessage` first, then `DisallowUnknownFields` on the struct after removing the allowed VS Code keys;
  - `Schema() map[string]any` and `InitialConfigurations() []any`, built by reflecting over the struct tags.
- `debugger/core/protocol.go` `Launch{Pipeline, Cwd}`: add `MaxMessages int`, `Env map[string]string` and `EnvFile string`. `debugger/core/session.go` launch: resolve paths and carry `MaxMessages` to the engine.
- `debugger/core/port.go` `Engine.Start(ctx, pipeline, cwd, Arriver)`: pass the launch options (at least `MaxMessages`) as a struct, so 2.4 can add env without changing the signature again. Update fakes in core, dap and serve tests.
- `debugger/engine/capture.go`: `capture()` returns the first batch from `got`. Truncate to `MaxMessages` there, and report the total count so the Session can emit the Output event (e.g. through `core.Arriver` or a returned count). It stays nacked.
- New generator `cmd/gen-launch-schema/main.go`. It imports `debugger/dap`, reads vscode/package.json, replaces `contributes.debuggers[0].configurationAttributes` and `.initialConfigurations`, and writes the file back with stable two-space JSON. `-check` exits 1 if the file would change. It writes files, not stdout; forbidigo applies only to `debugger/`.
- `Taskfile.yml`:
  - `gen:schema` runs `go run ./cmd/gen-launch-schema`;
  - `lint:schema` runs `go run ./cmd/gen-launch-schema -check` and is added to `ci` after `lint:gomod`.
- `vscode/package.json`: the hand-written `initialConfigurations` becomes generated. Decision for the 1.1 handoff: Go emits it as `[{type: "redpanda-connect", request: "launch", name: "Debug pipeline", pipeline: "${file}"}]`, so a new launch.json debugs the active file.
- `vscode/src/launchConfig.ts` `fillLaunchConfig(config, debugType, activeFile)`: add a `workspaceFolder` argument and fill a missing `cwd`. `extension.ts` passes `folder?.uri.fsPath` from `resolveDebugConfiguration(folder, config)`. Unit tests in `src/test/launchConfig.test.ts`.
- `testdata/tracer/batch12.yaml`: generate with `count: 12` and `batch_size: 12`, so the TS or Go test sees truncation.

## Tasks & Acceptance

**Execution:**
- [x] `debugger/dap/launch.go` + `launch_test.go`: `LaunchConfig`, the VS Code allow-list, the strict decode, validation, `Schema()` and `InitialConfigurations()`. Tests cover every decode and validation row of the matrix.
- [x] `debugger/dap/server.go`: the launch uses `decodeLaunch`. Failures are `LAUNCH_INVALID` error responses naming the key.
- [x] `debugger/core/protocol.go`, `session.go`, `port.go` (+ fakes in tests): `Launch` carries `MaxMessages`/`Env`/`EnvFile`; paths resolve against `cwd`; the engine start options carry `MaxMessages`; the truncation Output event.
- [x] `debugger/engine/capture.go` (+ test): truncate to `MaxMessages`, still nacked. The test is the 12 → 10 batch.
- [x] `cmd/gen-launch-schema/main.go` + `Taskfile.yml`: the generator with `-check`, `gen:schema`, and `lint:schema` in `ci`. Regenerate vscode/package.json.
- [x] `vscode/src/launchConfig.ts`, `extension.ts` + tests: fill `cwd` from the workspace folder. `adapter.test.ts`: an unknown key gives a failed launch naming it.
- [x] `testdata/tracer/batch12.yaml` and `README.md`: the sample, and one line on the launch keys.

**Acceptance Criteria:**
- Given VS Code with the new `.vsix`, when the user edits a launch.json entry for `redpanda-connect`, then IntelliSense offers `pipeline`, `cwd`, `maxMessages`, `env` and `envFile` with descriptions, and flags `maxMessages: 0`.
- Given a launch.json entry with `maxMesages: 5`, when the user presses F5, then the session doesn't start and the error names `maxMesages`.
- Given `testdata/tracer/batch12.yaml`, when the user presses F5, then Variables shows 10 messages and the Debug Console says "captured 10 of 12 messages (maxMessages)".
- Given `task ci`, when `LaunchConfig` changes without `task gen:schema`, then `task ci` fails at `lint:schema`.

## Plan Change Log

## Implementation Notes

- Strict decode (`debugger/dap/launch.go`): the arguments go into `map[string]json.RawMessage`, every key outside `LaunchConfig` and the VS Code allow-list is reported (sorted, all of them), then each known key is decoded on its own into its field type. That is as strict as `DisallowUnknownFields` on the filtered struct and names the key in every type error. An explicit `null` for any key is `LAUNCH_INVALID`; no arguments or `null` arguments decode as `{}` (the Session then reports the missing `pipeline`).
- Tags: `json`, `desc`, `default`, `min` (integer minimum, or string minimum length: `pipeline` is `min:"1"`, which becomes `minLength: 1`). The schema has no `additionalProperties: false`: VS Code adds its own keys, and Go rejects unknown ones.
- `envFile` existence is checked in `Session.launch`, after it resolves `cwd` (relative cwd against the process's working directory) and the relative `pipeline`/`envFile` against it. `Env`/`EnvFile` are stored on the Session, not passed to the engine (2.4).
- `core.Launch.MaxMessages` 0 means no limit at the core level; the default 10 lives only in `LaunchConfig`. A negative value is `LAUNCH_INVALID`.
- Engine port: `Engine.Start(ctx, core.StartOptions{Pipeline, Cwd, MaxMessages}, Arriver)`. The truncation count reaches the Session through a new `Arriver.Captured(kept, total)`, called once before the first `Arrive`; the Session emits the console Output "captured N of M messages (maxMessages)" only when `kept < total`. The dropped messages are nacked with the rest.
- Generator (`cmd/gen-launch-schema`): parses package.json into an order-preserving tree, replaces `configurationAttributes` (appended after `initialConfigurations` the first time) and `initialConfigurations`, writes two-space JSON without HTML escaping. Generated maps have sorted keys; the initial entry is reordered to type, request, name first. `-check` exits 1 naming the stale file.
- `adapter.test.ts` gained the unknown-key failure and the 12 → 10 truncation (Output line plus 10 Variables entries) against the real binary.

## Review Triage Log

Pass 1 (quick): high 0, medium 1, low 1, false 0, maybe-false 0. Both were patched.

| # | Finding | Verdict | Route | Evidence |
|---|---|---|---|---|
| 1 | A launch type error echoes the raw JSON value, so a bad `env` leaks values VS Code substituted from `${env:NAME}` into the user-shown error and stderr (AD-22) | medium | patch | Errors now name the key, the expected type and the JSON kind received, never the value. `env` names the offending entry key (`"env.A" must be a string, not a number`). `TestDecodeLaunchErrorsHideValues` |
| 2 | `"envFile": ""` passes decode, and the Session treats it as unset | low | patch | `min:"1"` on `EnvFile` (schema `minLength: 1`, regenerated). The `empty envFile` case added |

## Design Notes

Unknown-key detection decodes the raw arguments into a key map first, so the error can name the key and VS Code's own keys can be dropped before the strict struct decode. `DisallowUnknownFields` alone would reject `__sessionId` and only report the first offender. The schema is derived from the struct by reflection over a few tags (`desc`, `default`, `min`), not a third-party jsonschema library: the contract is five keys, and keeping the generator in-repo keeps AD-12's single source simple and visible.

## Verification

**Commands:**
- `go test -race ./debugger/... ./cmd/...`: expected PASS.
- `task lint:schema`: expected exit 0 after `task gen:schema`, and exit 1 after editing a `desc` tag without regenerating.
- `task ci`: expected exit 0.

**Manual checks:**
- In VS Code: launch.json IntelliSense for the five keys; F5 with `maxMesages` fails, naming it; F5 on `testdata/tracer/batch12.yaml` shows 10 messages and the truncation line.
