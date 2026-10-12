# Review: verification of committed decisions (rpcn-debugger architecture spine)

- Lens: was each committed decision checked against the web, a registry, or real source, rather than asserted from training data?
- Reviewed: `architecture-rpcn-debugger.md` (spine) and `.memlog.md`, 2026-10-08
- Sources checked:
  - `B` = `~/.gvm/pkgsets/go1.26.0/global/pkg/mod/github.com/redpanda-data/benthos/v4@v4.81.0`
  - `C` = `/home/aleksejjurcenko/projects/redpanda-replicator/v4.112.0/connect`
  - `D` = `~/.gvm/pkgsets/go1.26.0/global/pkg/mod/github.com/google/go-dap@v0.12.0`
  - Go module proxy (`go list -m -versions -json`), npm registry (`npm view`), GitHub REST API, go.dev/dl JSON
  - A throwaway program run in the spike module that walked `bloblang.GlobalEnvironment()` with `connect/v4/public/components/all` imported. It has since been removed, and the spike's go.mod and go.sum were restored.

Legend: **confirmed** = matches source or registry. **wrong** = contradicts source or registry. **unverified** = not checkable here, or only partly checked.

## Verdict

The versions and most API names hold up against source and registries. Three things the spine states as fact are wrong or incomplete:

- The Bloblang evaluation API it names drops batch context and mutation semantics.
- The license mechanism it names has the wrong flag name.
- The list of non-deterministic Bloblang functions is incomplete.

The spine also misses a newer engine behaviour (strict error handling) that the stepper must copy. Two open questions can now be closed: go-dap is maintained, and a public license API exists.

## A. Engine APIs named in the spine

| # | Claim | Status | Evidence |
|---|---|---|---|
| A1 | `service.Environment` exists; `schema.Standard(version, date)` returns a `*service.ConfigSchema` whose `.Environment()` gives the stock environment | **confirmed** | `C/public/schema/schema.go:30-39` (`Standard` builds on `service.NewEnvironment()`); `B/public/service/stream_schema.go:71` (`func (s *ConfigSchema) Environment() *Environment`). The stock CLI uses the same call: `C/internal/cli/enterprise.go:100` `CLIOptSetEnvironment(schema.Environment())` |
| A2 | `public/plugin/go/rpcnloader.DiscoverAndRegisterPlugins` | **confirmed** | `C/public/plugin/go/rpcnloader/rpcnloader.go:32` `func DiscoverAndRegisterPlugins(fsys fs.FS, env *service.Environment, paths []string) error`. `paths` are glob patterns evaluated against `fsys` (doc comment, lines 26-31). The launch config `rpcPlugins[]` should therefore be documented as globs, and the debugger must pass an `fs.FS` (e.g. `os.DirFS`) |
| A3 | `StreamBuilder.AddBatchProducerFunc` / `AddBatchConsumerFunc` / `AddInputYAML` | **confirmed** | `B/public/service/stream_builder.go:277`, `:414`, `:319` |
| A4 | `NewProcessorListField` / `ParsedConfig.FieldProcessorList` -> `[]*OwnedProcessor` | **confirmed** | `B/public/service/config_processor.go:47`, `:78`. The child processors are built through `p.mgr.IntoPath(path...)`, so `resource:` processors resolve. This supports the AD-4 ban on standalone `ParseYAML` for processors |
| A5 | `service.Message.BloblangQuery` as the evaluation entry point (AD-13) | **wrong (as specified)** | `B/public/service/message.go:444-459` wraps the single message as `message.Batch{m.part}` and calls `MapPart(0, …)`. As a result, `batch_index()` is always 0, `batch_size()` is always 1, and `from`/`from_all` cannot see other messages. The real `mapping` processor runs `MapPart(i, b)` over the whole batch (`B/internal/impl/pure/processor_mapping.go:148`). **Fix:** evaluate with `MessageBatch.BloblangExecutor(exec).Query(i)` (`B/public/service/message_batch_blobl.go:27,50`) |
| A5b | AD-13 "`root` is shown as unassigned until the first `root` assignment" applies to `mutation` | **wrong for `mutation`** | `mutation` uses `MapOnto(msg, i, b)` (`B/internal/impl/pure/processor_mutation.go:152`), so `root` starts as the input message. Prefix replay of a mutation must use `MessageBatchBloblangExecutor.Mutate(i)` (`B/public/service/message_batch_blobl.go:103`). The "unassigned" label applies only to `mapping`/`bloblang` |
| A6 | The pipeline Bloblang environment can be reached from `service.Environment` | **wrong: no public getter** | `B/public/service/environment.go:103` has only the setter `UseBloblangEnvironment`. The getter `getBloblangParserEnv` is unexported (`:126`). **Public workaround (verified in source):** `service.NewConfigSpec().Field(service.NewBloblangField("m")).ParseYAML("m: <expr>", env)` then `.FieldBloblang("m")`. `ParseYAML` builds a manager with `env.getBloblangParserEnv()` (`B/public/service/config.go:331-335`), and `FieldBloblang` parses with that manager's environment (`B/public/service/config_bloblang.go:36`). This is a `ParseYAML` call outside the stepper, so AD-4's ban needs a stated exception for Bloblang-only specs. The expression must be YAML-escaped (marshal it with yaml.v3). `bloblang.GlobalEnvironment()` is **not** equivalent: `NewEnvironment()` clones it at clone time (`environment.go:58,76`), so plugins registered later on the env would be missed |
| A7 | Stepper modes `all` = `ExecuteAll`, `try` = `ExecuteTryAll`, `catch` = `ExecuteCatchAll` | **confirmed, incomplete** | `B/internal/component/processor/execute.go:34,70,107`. These are internal, so the stepper must reimplement them. The public `service.ExecuteProcessors` (`B/public/service/processor.go:205`) matches `ExecuteAll` only in non-strict mode |
| A8 | (Missing from the spine) strict error handling changes `ExecuteAll` | **new finding** | v4.81 `ExecuteAll` skips messages that already failed when `IsStrict(ctx)` (`execute.go:31-46`). Strict mode comes from `error_handling.strict` in the config, or from `StreamBuilder.SetStrict` (`B/public/service/stream_builder.go:117-123`), and is put on the context by the pipeline (`B/internal/pipeline/processor.go:53-55`). `IsStrict` is internal, so the stepper cannot read it from `ctx`. U-REWRITE must read `error_handling.strict` from the YAML and give the stepper a `strict` flag. AD-7 needs a row for this, and AD-8 needs an equivalence case for it |
| A9 | A public way to pass an enterprise license exists (`public/license`?) — Open Question 1 | **confirmed: can be closed** | `C/public/license/license.go:29` `LocateLicense(res *service.Resources, opts ...LocateLicenseOptFunc)` and `:40` `StoreCustomLicenseBytes(res, bytes) error`. No option funcs are exported (the file is 42 lines), so `LocateLicense` reads only the default `/etc/redpanda/redpanda.license` (`C/internal/license/service.go:23,233`). **It does not read the environment variables.** The debugger must read `REDPANDA_LICENSE` / `REDPANDA_LICENSE_FILEPATH` itself and call `StoreCustomLicenseBytes` |
| A9b | The license flag is "`--license`" (memlog) | **wrong** | The flag is `--redpanda-license` (`C/internal/cli/flags_redpanda.go:66-67`). The env vars are `REDPANDA_LICENSE` and `REDPANDA_LICENSE_FILEPATH` (`:73-74`) |
| A9c | License timing works with `StreamBuilder` | **partly confirmed** | Enterprise components check the license in their constructors (e.g. `C/internal/impl/gcp/enterprise/input_spanner_cdc.go:223`). Stream components are built in `Stream.Run` (`B/public/service/stream.go:67-80`), so calling `StoreCustomLicenseBytes(strm.Resources(), …)` between `Build()` and `Run()` works for them; Connect's own dry-run does exactly this (`C/internal/cli/dry_run.go:188-194`). However, `processor_resources` and other resources are built eagerly in `manager.New` during `Build()` (`B/internal/manager/type.go:224` ff., loop over `conf.ResourceProcessors`), **before** a license can be stored. An enterprise processor declared as a resource would therefore fail. **Unverified** whether any public hook exists for this |
| A10 | `consumer_group` is the field to override (AD-5) | **confirmed** for Kafka-family inputs | `C/internal/impl/kafka/franz_reader_ordered.go:35`, `franz_reader_unordered.go:37`, `input_sarama_kafka.go:36`. Other inputs (e.g. `redis_streams`, `C/internal/impl/redis/input_streams.go`) have their own group fields. AD-5 should name the inputs it covers |
| A11 | Plugin-name regex `^[a-z0-9]+(_[a-z0-9]+)*$` | **unverified here** | Recorded as spike-observed in the memlog; not re-checked |

## B. Stack table versions

| # | Item | Status | Evidence |
|---|---|---|---|
| B1 | connect/v4 v4.112.0 is the latest | **confirmed** | Go proxy: v4.112.0, 2026-10-02T08:44:56Z |
| B2 | benthos/v4 v4.81.0 is the latest, and is what Connect pins | **confirmed** | Go proxy: v4.81.0, 2026-09-18; `C/go.mod:146` |
| B3 | Go 1.26.x, matching `go 1.26.6` | **confirmed** | `C/go.mod:3` `go 1.26.6`; go.dev/dl JSON shows the latest stable is go1.27.2. 1.26 stays supported until 1.28 ships. Note that `go 1.26.6` is a *minimum*, so a 1.27 toolchain also builds it |
| B4 | go-dap v0.12.0 is the latest tag, last published 2024-02 (Open Question 2: is it maintained?) | **confirmed tag; the question can now be answered: yes, maintained** | Go proxy: v0.12.0, 2024-02-22. GitHub API: `archived: false`, `pushed_at: 2026-09-26`. The latest commit, "cmd/gentypes: sync with canonical DAP v1.71.0 schema (#99)", is 2026-09-26, with an earlier sync at DAP 1.68 on 2025-09-04. The pseudo-version `v0.12.1-0.20260926015354-7505f696918e` (`go list -m github.com/google/go-dap@main`) carries DAP 1.71 types that v0.12.0 lacks. **Recommendation:** pin that pseudo-version, or ask upstream for a tag; no fork or vendoring is needed. No maintained fork was found or needed (search was limited to GitHub API metadata) |
| B5 | gopkg.in/yaml.v3 v3.0.1, "the same library benthos uses" | **confirmed, with a status caveat** | `B/go.mod:50` requires `gopkg.in/yaml.v3 v3.0.1` directly. 42 non-test benthos files import it (including `public/service/stream_builder.go:15`), and none import `go.yaml.in/yaml/v3`. **However:** the upstream repo `go-yaml/yaml` is **archived** (GitHub API `archived: true`, last push 2025-04-01). The maintained continuation is `go.yaml.in/yaml/v3` (latest v3.0.5, 2026-07-26), and v4 is at v4.0.0-rc.6. The choice is right *because benthos uses it*, but the spine should record that it is archived and add "switch when benthos switches" to the AD-14 bump checklist, so that line numbers keep agreeing |
| B6 | TypeScript 7.0.2 | **confirmed exists; fit is doubtful** | npm: `latest` 7.0.2 (published 2026-07-08). **Toolchain gap:** `typescript-eslint` 8.71.1 (latest) peer-requires `typescript >=4.8.4 <6.1.0`, so it doesn't support TS 7. Whether TS 7 (the native Go port) offers the JS compiler API that bundlers and loaders such as ts-loader rely on was **unverified**. **Recommendation:** pin TS 6.0.x (latest 6.0.3, 2026-04-16) for the extension until typescript-eslint supports 7, or commit to type-checking with `tsc` only, building with esbuild (0.28.2) and linting without typescript-eslint |
| B7 | @types/vscode 1.140.0 as the `engines.vscode` minimum | **version confirmed; minimum questionable** | npm: 1.140.0, published 2026-10-01, a week before the review. Using it as `engines.vscode` excludes every user not on the newest VSCode, as well as forks such as Cursor/VSCodium, which lag behind. The spine table states it as a commitment while Open Questions lists it as open; this contradiction should be resolved. Pick the lowest version that has the DAP APIs used (`DebugAdapterExecutable` has been available since 1.30-era APIs) |
| B8 | @vscode/vsce 4.0.0 | **confirmed** | npm: `latest` 4.0.0, 2026-09-14; `engines.node >= 22`. The Node 22+ build requirement should be recorded for U-REL CI |
| B9 | @vscode/debugadapter-testsupport 1.68.0 | **confirmed latest; stale** | npm: `latest` 1.68.0, *published* 2024-10-03 (`time["1.68.0"]`). This is the same staleness as go-dap, but the spine doesn't flag it. It matches DAP 1.68, while go-dap main is at 1.71 |

## C. VSCode / DAP API claims

| # | Claim | Status | Evidence |
|---|---|---|---|
| C1 | `DebugAdapterExecutable` (AD-12d) | **confirmed** | `@types/vscode@1.140.0` `index.d.ts:16853` `export class DebugAdapterExecutable`, used through `debug.registerDebugAdapterDescriptorFactory` (`:17378`) |
| C2 | DAP `linesStartAt1: true` | **confirmed, wording off** | `@vscode/debugprotocol@1.68.0` `lib/debugProtocol.d.ts:513` `linesStartAt1?: boolean` ("If true all line numbers are 1-based (default)"). This is a **client** argument on `initialize`, not something the adapter declares. go-dap defaults it to true when absent (`D/schematypes.go:1796-1801`). The convention should read: "honour the client's `linesStartAt1`/`columnsStartAt1` (VSCode sends true)", converting if the client sends false |
| C3 | Custom field `rpcnConnectVersion` in the `initialize` response, read by the extension (AD-14) | **unverified mechanism** | The VSCode extension API doesn't expose the adapter's capabilities directly. The extension would need a `DebugAdapterTracker` (`onDidSendMessage`) or a custom request. The output format of `rpk connect --version` used for the comparison was not checked |

## D. Bloblang non-deterministic function list (AD-13)

Functions registered in the global environment with `connect/v4/public/components/all` (benthos v4.81 / connect v4.112), walked at runtime:

`batch_index batch_size bytes content count(deprecated) counter(experimental) deleted env error error_source_label error_source_name error_source_path errored fake file file_rel hostname json ksuid meta(deprecated) metadata nanoid nothing now pi random_int range root_meta(deprecated) snowflake_id throw timestamp_unix timestamp_unix_micro timestamp_unix_milli timestamp_unix_nano tracing_id tracing_span ulid uuid_v4 uuid_v7 var with_schema_registry_header`

| # | Claim | Status | Notes |
|---|---|---|---|
| D1 | `uuid_v4`, `now`, `timestamp_unix*`, `random_int`, `nanoid`, `ksuid`, `counter` exist | **confirmed** | All are present; `timestamp_unix*` covers four variants. `counter` is *experimental*. With `pure` only, `env`, `file`, `file_rel`, `hostname`, `fake`, `ulid`, `snowflake_id` and `with_schema_registry_header` are absent, so they come from non-pure Connect components |
| D2 | The list is complete | **wrong: missing entries** | Non-deterministic and missing: `uuid_v7`, `ulid`, `snowflake_id`, `fake`. Stateful and missing: `count` (deprecated predecessor of `counter`). Has side effects or does network I/O, and will be **re-executed on every prefix replay**: `with_schema_registry_header` (schema-registry lookup). Environment- or file-dependent (stable within a session but not tied to the capture, so they should be labelled): `env`, `file`, `file_rel`, `hostname`. Context-dependent: `tracing_id`/`tracing_span` (no span in replay), plus `batch_index`/`batch_size` (see A5). Plugin functions (from RPC or compiled-in plugins) can't be classified ahead of time. **Recommendation:** use a deny-by-default list derived from `WalkFunctions` (category `Environment`, `Fake Data Generation`, plus a named set), and treat unknown plugin functions as `recomputed`. Bloblang *methods* were not audited (the walk failed on the method view type), so they remain **unverified** |

## E. Other assertions that should be noted

- **E1, unverified:** the spike-proven claims in the memlog (nack leaves offsets uncommitted, reruns restart from offset 0, equivalence on all parents) were tested only with the `redpanda` input at these versions. They should be re-checked at each AD-14 version bump. That is already covered by AD-8 for outputs, but not for the capture's never-commit behaviour, which has no test listed.
- **E2, unverified:** whether a user config with a top-level `redpanda:` block (a Connect-only field, `C/public/schema/schema.go:25-37`) passes `StreamBuilder` YAML linting. Connect's dry-run calls `DisableLinting()` (`C/internal/cli/dry_run.go:182`), which suggests it may not. U-REWRITE should strip the block or disable linting.

## Recommended spine edits (for the author)

1. AD-13: replace `service.Message.BloblangQuery` with `MessageBatch.BloblangExecutor(…).Query(i)` (and `.Mutate(i)` for `mutation`). State how the env's Bloblang environment is obtained (via `ConfigSpec`+`NewBloblangField`+`ParseYAML(…, env)`), and add the AD-4 exception for it. Expand the `recomputed` list (D2), or make it derived.
2. AD-7/AD-8: add strict mode (`error_handling.strict`) as a stepper input, with an equivalence case.
3. Open Questions: close go-dap (maintained; pin the main pseudo-version for DAP 1.71) and the license question (`public/license.StoreCustomLicenseBytes` after `Build()`; the debugger reads the env vars itself; the flag is `--redpanda-license`). Open a new question about enterprise `processor_resources`.
4. Stack: change TS to 6.0.x, or record the typescript-eslint incompatibility. Lower the `engines.vscode` minimum (and remove the table/open-question contradiction). Note that yaml.v3 is archived and that vsce needs Node >= 22.
5. Conventions: change "DAP `linesStartAt1: true`" to "honour the client's `linesStartAt1`".
