# Side-effecting processors in the stock rpcn-debug binary

Source: `schema.Standard(...).Environment()` + `connect/v4/public/components/all`, Connect v4.115.0, benthos v4.84.0,
CGO_ENABLED=0. `WalkProcessors` yields 96 processors, `WalkCaches` 17, `WalkRateLimits` 2
(walker: scratchpad/walk/main.go, output scratchpad/walk/out.txt).

Path prefixes: `C/` = `connect/v4@v4.115.0/internal/impl/`, `B/` = `benthos/v4@v4.84.0/internal/impl/`,
`L/` = `connect/v4@v4.115.0/internal/llm/`.

"Remote cache" below means the cache resource's type is in the remote/persistent list at the end
(for `multilevel`, any level remote).

## A. Direct side effects (21 rows)

| # | processor | predicate (writes when) | warning text | target field(s) | evidence |
|---|---|---|---|---|---|
| 1 | `http` | `verb` not in {GET, HEAD, OPTIONS} (case-insensitive). Default verb is **POST**, so an unset verb writes. | "sends `<verb>` requests to `<url>`" | `url` (interpolated), `verb` | B/io/processor_http.go:61 `httpclient.ConfigField("POST", …)`; benthos internal/httpclient/config.go:40-42 (`url`, `verb`); send at processor_http.go:120 |
| 2 | `sql_insert` | always | "inserts rows into `<table>` on the `<driver>` database at `<dsn>`" | `driver`, `dsn`, `table` | C/sql/processor_sql_insert.go:101-120, Exec at :271/:278 |
| 3 | `sql_raw` | always, unless every query (`query` and each `queries[].query`) is a read (`SELECT`/`WITH … SELECT`/`SHOW`/`EXPLAIN`); `unsafe_dynamic_query: true` makes the text unknown, so treat as write | "runs SQL statements against the `<driver>` database at `<dsn>`" | `driver`, `dsn`, `query`, `queries[].query` | C/sql/processor_sql_raw.go:25-56 (`exec_only` doc: "useful when you are executing inserts, updates"), ExecContext at :324/:326 |
| 4 | `sql` (deprecated) | same as `sql_raw` (query text) | "runs SQL statements against the `<driver>` database at `<data_source_name>`" | `driver`, `data_source_name`, `query` | C/sql/processor_sql_deprecated.go:33-41 |
| 5 | `aws_dynamodb_partiql` | `query` starts with INSERT / UPDATE / DELETE (PartiQL write statements); `unsafe_dynamic_query: true` = unknown, treat as write | "runs PartiQL `<verb>` statements against DynamoDB in `<region>`" | `query` (table is inside the statement), `region` | C/aws/dynamodb/processor_partiql.go:34-47 |
| 6 | `mongodb` | `operation` in {insert-one, delete-one, delete-many, replace-one, update-one}. Default is **insert-one**. Reads: find-one, aggregate | "`<operation>`s documents in `<database>.<collection>` at `<url>`" | `url`, `database`, `collection` | C/mongodb/processor.go:46-48 (default `OperationInsertOne`), :175-196 write models; C/mongodb/common.go:353-365, :50-51 |
| 7 | `couchbase` | `operation` in {insert, remove, replace, upsert}. Default `get` (read) | "`<operation>`s documents in bucket `<bucket>` at `<url>`" | `url`, `bucket`, `collection` | C/couchbase/processor.go:49-55; C/couchbase/client/docs.go:25-29 |
| 8 | `azure_cosmosdb` | `operation` in {Create, Delete, Replace, Upsert, Patch}. Default **Create**. Read only for `Read` | "`<operation>`s items in container `<container>` of database `<database>` at `<endpoint/account>`" | `endpoint` / `account_key` / `connection_string`, `database`, `container` | C/azure/processor_cosmosdb.go:37 ("Creates or updates…"); C/azure/cosmosdb/docs.go:50,58,213,224 |
| 9 | `redis` | `command` (interpolated) is a write command (SET, SETEX, SETNX, GETSET, GETDEL, DEL, UNLINK, EXPIRE*, PERSIST, INCR*, DECR*, APPEND, HSET, HDEL, HINCRBY*, LPUSH, RPUSH, LPOP, RPOP, LREM, LSET, LTRIM, SADD, SREM, SPOP, SMOVE, ZADD, ZREM, ZINCRBY, XADD, XDEL, XTRIM, PUBLISH, RENAME, COPY, FLUSHDB, FLUSHALL, MSET, EVAL, FCALL, …) or contains an interpolation (`${!`), treat as write. Deprecated `operator`: `sadd`, `incrby` write; `keys`, `scard` read | "runs `<command>` against Redis at `<url>`" | `url`, `command`, `operator` | C/redis/processor.go:42-60 (`command`, `operator` enum incl. sadd/incrby), Do at :369; C/redis/client.go:37 (`url`) |
| 10 | `redis_script` | always (an arbitrary Lua script; could heuristically pass only if the script has no `redis.call` of a write command) | "runs a Lua script against Redis at `<url>`" | `url`, `script` | C/redis/script_processor.go:32,43, Run at :166 |
| 11 | `nats_kv` | `operation` in {create, put, update, delete, purge}. Reads: get, get_revision, history, keys | "`<operation>`s key `<key>` in NATS KV bucket `<bucket>` at `<urls>`" | `urls`, `bucket`, `key` | C/nats/processor_kv.go:54-62 (ops), :247/:257/:271/:282/:292 (Create/Put/Update/Delete/Purge); C/nats/docs.go:22 |
| 12 | `nats_request_reply` | always (publishes a request message; the responder may act on it) | "publishes requests to NATS subject `<subject>` at `<urls>`" | `urls`, `subject` | C/nats/processor_request_reply.go:32, RequestMsgWithContext at :185 |
| 13 | `aws_lambda` | always (invokes a function) | "invokes AWS Lambda function `<function>`" | `function`, `region` | C/aws/lambda/processor.go:36,93, Invoke at :302 |
| 14 | `sentry_capture` | always (submits events) | "sends events to Sentry at `<dsn>`" | `dsn` | C/sentry/processor_capture.go:37-39, CaptureMessage at :294 |
| 15 | `command` | always (runs a command per message) | "runs the command `<name>`" | `name`, `args_mapping` | B/io/processor_command.go:17,50-53, exec.CommandContext at :153 |
| 16 | `subprocess` | always (starts a long-lived subprocess) | "runs the subprocess `<name> <args>`" | `name`, `args` | B/io/processor_subprocess.go:31,73-76, exec.CommandContext at :344 |
| 17 | `cache` | `operator` in {set, add, delete} AND the `resource` cache is remote/persistent (see list). `get`/`exists` read | "`<operator>`s keys `<key>` in the `<cache type>` cache `<resource>`" | `resource`, `operator`, `key` | B/pure/processor_cache.go:22-26, :132 (enum), :284-292 |
| 18 | `cached` | the `cache` resource is remote/persistent (it always calls `cache.Set` on a miss unless `skip_on` matches). Also runs `processors` (transitive, see B) | "stores processor results in the `<cache type>` cache `<cache>`" | `cache`, `key` | B/pure/processor_cached.go:25,39, Set at :223 |
| 19 | `dedupe` | the `cache` resource is remote/persistent (it calls `cache.Add` per message) | "adds dedupe keys to the `<cache type>` cache `<cache>`" | `cache`, `key` | B/pure/processor_dedupe.go:22,63, Add at :144 |
| 20 | `schema_registry_encode` | `schema_metadata` is non-empty (default `""` only reads the latest schema) | "registers schemas under subject `<subject>` at the schema registry `<url>`" | `url`, `subject`, `schema_metadata` | C/confluent/processor_schema_registry_encode.go:41,95-96,107-110, CreateSchema at :688 |
| 21 | `rate_limit` | the `resource` rate limit is `redis` (each access runs INCR/PEXPIRE on a shared Redis key). Minor: a counter, but shared with production consumers, so the debugger eats production quota | "consumes quota from the redis rate limit `<resource>` at `<url>`" | `resource` | B/pure/processor_rate_limit.go:21,29,76; C/redis/rate_limit.go:100-104 |

## B. Program-dependent or transitive (count separately; 7 rows, three marked `?` in C)

| # | processor | predicate | warning text | target field | evidence |
|---|---|---|---|---|---|
| 22 | `javascript` | `code` (or the `file` contents) calls `benthos.v0_fetch(url, headers, method, body)`; the method is a runtime value, so any call is a possible write | "may send HTTP requests from JavaScript (`v0_fetch`)" | `code` / `file` | C/javascript/functions.go:102-140 (`http.NewRequest(method, …)` with `http.DefaultClient`) |
| 23 | `awk` | `program` uses `system(`, output redirection (`print … >`, `>>`) or a pipe (`print … \| "cmd"`, `"cmd" \| getline`). goawk runs without NoExec / NoFileWrites | "may run commands or write files from awk" | `program` | C/awk/processor.go:773-777 (`interp.Config{Output, Error, Funcs}` only, no `NoExec`/`NoFileWrites`) |
| 24 | `ollama_chat`, `ollama_embeddings`, `ollama_moderation` | at Connect (startup, not per message): always pulls `model` into the Ollama server's store; without `server_address` it also downloads the Ollama binary into `cache_directory` and starts it | "pulls model `<model>` into the Ollama server at `<server_address>` (or downloads and starts Ollama)" | `server_address`, `model`, `cache_directory`, `download_url` | C/ollama/base_processor.go:39-42, :347-351 (pullModel), :370; C/ollama/subprocess_unix.go:63-130 |
| 25 | `openai_chat_completion`, `cohere_chat`, `gcp_vertex_ai_chat`, `ollama_chat` | any `tools[].processors` list contains a side-effecting processor (run when the LLM calls the tool) | inherit the child's warning, prefixed "when the model calls tool `<name>`:" | `tools[].processors` | L/tools.go:30,54 (`NewProcessorListField("processors")`); C/openai/chat_processor.go:65, C/cohere/chat_processor.go:57, C/gcp/processor_vertex_ai_chat.go:56, C/ollama/chat_processor.go:54 |
| 26 | `cached` (children) | `processors` contains a side-effecting processor | inherit child's | `processors` | B/pure/processor_cached.go:39 |
| 27 | `resource` | the referenced `processor_resources` entry is side-effecting | inherit | `resource` name | (stepper already walks processor_resources) |
| 28 | every AD-7 parent incl. opaque ones (`workflow`, `parallel`, parallel `retry`, `branch`, `switch`, …) | any child is side-effecting. Matters most for the opaque/concurrent ones, whose children the debugger does not step but still runs | inherit | child lists | B/pure/processor_*.go with NewProcessorListField (parallel, while, catch, try, cached, branch, group_by, for_each, retry, processors, switch, try_catch) |

## C. Uncertain (`?`)

| processor | doubt | what settles it |
|---|---|---|
| `sql_raw` / `sql` with only SELECTs `?` | a SELECT can still write (volatile functions, `nextval()`, `SELECT … FOR UPDATE` locks, stored procedures via `SELECT fn()`, `INTO` in some dialects); `exec_only` doesn't separate reads from writes | a policy decision: warn on any `sql_raw` (safe), or accept a leading-keyword heuristic and document that it is one |
| `http` with GET/HEAD `?` | GET endpoints that mutate (webhooks, "trigger" URLs) can't be detected | policy: treat verb as the signal and say so in the docs |
| `javascript` `?` | v0_fetch method is computed at run time; a static scan for the literal `v0_fetch` can only say "may" | decide whether "contains v0_fetch" is enough to warn (recommended), since the method is not knowable statically |
| `awk` `?` | static detection of `system(`/`>`/`\|` in awk source is a text heuristic (`>` is also a comparison operator) | parse the program with goawk's parser (`parser.ParseProgram`) and look for redirect/pipe/system AST nodes, or just warn on `system(`, `print…>` and `\|` |
| `ollama_*` `?` | the write happens at Connect (startup), and model pulls into a local/self-hosted server may not count as "production" | product decision on whether startup pulls/downloads count; mechanically it always happens at Connect |
| `rate_limit` (redis) `?` | changes Redis counters, which is state, but only quota | product decision; mechanically it always INCRs |
| `redpanda_data_transform`, `wasm` | checked: **not** side-effecting. WASI is instantiated but the module config has no FS mount and no socket support; host modules only expose message get/set functions | C/wasm/processor_wazero.go:124-136 (no WithFS); C/redpanda/processor_data_transform.go:256-262 (NewModuleConfig: args/env only) |

Checked and read-only (not side-effecting): `gcp_bigquery_select` (SELECT only, C/gcp/processor_bigquery_select.go:97),
`jira` (queries only; no POST/PUT/DELETE in C/jira/), `slack_thread` (conversations.replies, C/slack/processor_thread.go:33,86),
`google_drive_download`/`google_drive_search`/`google_drive_list_labels` (read), `qdrant` (query, C/qdrant/processor.go:45),
`schema_registry_decode` (no CreateSchema), `protobuf`, the LLM/embedding/rerank/image/speech/transcription/translation
processors (remote API calls, no state change apart from the tool children in B), all pure transforms, `log`, `metric`,
`sleep`, `crash`, `sync_response`, `noop`, `benchmark`. Bloblang (`bloblang`, `mapping`, `mutation`, `branch` maps):
no write functions are registered (only `env`, `file`, `file_rel`, `hostname` read).

## D. Cache resources (17)

In-process (local, safe): `memory`, `lru`, `ttlru`, `ristretto`, `noop`
(B/pure/cache_memory.go:57, cache_lru.go:99, cache_ttlru.go:82, cache_noop.go:25; C/dgraph/cache_ristretto.go:52).

Local but persistent (writes files on the debugger host): `file` (writes `<directory>/<key>`, B/io/cache_file.go:29,68).
Treat as side-effecting.

Remote: `redis` (C/redis/cache.go:58), `memcached` (C/memcached/cache.go:54), `aws_dynamodb` (C/aws/dynamodb/cache.go:79),
`aws_s3` (C/aws/s3/cache.go:65), `gcp_cloud_storage` (C/gcp/cache_cloud_storage.go:45), `couchbase` (C/couchbase/cache.go:42),
`mongodb` (C/mongodb/cache.go:48), `nats_kv` (C/nats/cache_kv.go:41), `redpanda` (produces to a topic, C/kafka/cache_redpanda.go:59),
`sql` (C/sql/cache_sql.go:92; with `driver: sqlite` it is a local file, still persistent).

Composite: `multilevel` (config is a plain list of cache resource names, B/pure/cache_multilevel.go:17,54):
remote/persistent if any level is.

## E. Rate limit resources (2)

In-process: `local` (B/pure/rate_limit_local.go:29).
Remote: `redis` (C/redis/rate_limit.go:51; INCR + PEXPIRE Lua script at :100-104).
