# Input classes for the stock binary (Connect v4.115.0, benthos v4.84.0)

The set of inputs comes from `schema.Standard("x","x").Environment().WalkInputs` with `components/all` and CGO_ENABLED=0. There are 82 inputs. `tigerbeetle_cdc` and `zmq4` register only under cgo, so they are not in the stock binary. `twitter_search` is a template: it expands to `generate` plus input processors.

Paths: `C/` = `connect/v4@v4.115.0/internal/impl/`, `B/` = `benthos/v4@v4.84.0/internal/impl/`.

Class key: RS = replay-safe, SE = side-effecting, REJ = rejected, COMP = composite. A `?` marks a row I'm unsure of. "Class" is the class under default field values. The predicate column says what changes it.

## Cross-cutting finding: `auto_replay_nacks: false`

Many inputs are wrapped in `service.AutoRetryNacks*Toggled`, and their raw ack funcs ignore the error (`func(ctx, _ error)`). With the default `auto_replay_nacks: true`, a nack is retried in-process, so the raw ack never runs. With `auto_replay_nacks: false`, **a nack is treated as an ack**: Kafka offsets are committed (to the debug group, or to production's when the group is kept), the postgres LSN is confirmed, and CDC checkpoint caches and cursor caches advance. Examples: `C/cockroachdb/input_changefeed.go:381-397` (the comment says so explicitly), `C/kafka/franz_reader_unordered.go:692`, `C/kafka/franz_reader_ordered.go:697`, `C/mysql/input_mysql_stream.go:882`, `C/postgresql/input_pg_stream.go:1252`.
These inputs have the toggle (from the spec walk): aws_cloudwatch_logs, aws_dynamodb_cdc, aws_kinesis, azure_cosmosdb, cassandra, cockroachdb_changefeed, csv, discord, file, gcp_bigquery_select, gcp_spanner_cdc, generate, git, http_client, jira, kafka, kafka_franz, microsoft_sql_server_cdc, mongodb, mongodb_cdc, mqtt, mysql_cdc, nanomsg, nats, nats_kv, oracledb_cdc, parquet, pg_stream, postgres_cdc, redis_list, redis_pubsub, redis_scan, redis_streams, redpanda, redpanda_common, redpanda_migrator, salesforce, salesforce_cdc, salesforce_graphql, sap_hana, schema_registry, sftp, slack, slack_users, socket, socket_server, splunk, sql_raw, sql_select, stdin, websocket.
Recommendation: either the rewrite forces `auto_replay_nacks: true` in the capture config, or the predicate `auto_replay_nacks == false` makes every row that has the toggle side-effecting. The rows below write this as "+ARN".

Inputs whose ack runs on nack without any wrapper (no toggle): aws_sqs, amqp_0_9, amqp_1, azure_queue_storage, azure_table_storage, beanstalkd, gcp_pubsub, hdfs, nats_jetstream, nats_stream, nsq, pulsar, spicedb_watch, plus the servers. Two of them do damage on nack: **azure_queue_storage deletes** and **spicedb_watch advances its cursor**.

## Kafka family

| name | class | predicate | group field(s) | start field(s) | effect (SE) | depr | evidence |
|---|---|---|---|---|---|---|---|
| kafka | RS | SE if `consumer_group` is kept or interpolated; +ARN | `consumer_group` (default `""`, which means no stored offsets) | `start_from_oldest` | SE: "joins group G; its members rebalance; nacks commit offsets if auto_replay_nacks=false" | yes | C/kafka/input_sarama_kafka.go:142 (group, empty disables offsets), :201 (ARN wrap), :352 (ack ignores err), :324 (balanced topics need a group) |
| kafka_franz | RS | same as kafka; `start_offset: committed` with a fresh debug group blocks capture ("no prior commits") | `consumer_group` | `start_offset` (earliest\|latest\|committed), `start_from_oldest` (deprecated) | as kafka | yes | C/kafka/input_kafka_franz.go:52-65; C/kafka/franz_reader_unordered.go:50; C/kafka/franz_reader.go:198-215 |
| redpanda | RS | as kafka_franz. Without `seed_brokers` it uses the shared global `redpanda` client | `consumer_group` | `start_offset`, `start_from_oldest` | as kafka | no | C/kafka/input_redpanda.go:136-162, :204 (FranzSharedClientUse) |
| redpanda_common | RS ? | as kafka_franz; it always needs the shared top-level `redpanda` client | `consumer_group` | `start_offset` | as kafka | yes | C/kafka/enterprise/redpanda_common_input.go:51-70 |
| ockam_kafka | SE ? | the Kafka part is as kafka_franz | `kafka.consumer_group` | `kafka.start_offset`, `kafka.start_from_oldest` | "starts a local Ockam node, enrolls `enrollment_ticket` and may claim relay `relay`" | no | C/ockam/input_kafka.go:52-60, :97-128 (node/ticket/relay), :169 (kafka inlet) |
| redpanda_migrator | REJ | always: Connect fails without the paired `redpanda_migrator` output, and the capture config has no output | (`consumer_group`) | (`start_offset`) | n/a | no | C/redpanda/migrator/migrator.go:398-402 (Connect → onInputConnected), :677-683 ("output not initialized") |
| schema_registry | RS | none (HTTP GETs of subjects/schemas) | none | none | none | no | C/kafka/input_schema_registry.go:98, :311 |

## Cloud queues, brokers, streams

| name | class | predicate | group field(s) | start field(s) | effect (SE) | depr | evidence |
|---|---|---|---|---|---|---|---|
| aws_sqs | SE | always | (queue `url`) | none | "receives messages from the queue: ApproximateReceiveCount rises (redrive to the DLQ at maxReceiveCount), and they stay hidden for message_timeout" | no | C/aws/sqs/input.go:479-484 (ReceiveMessage with VisibilityTimeout), :619-640 (visibility changes) |
| azure_queue_storage | SE | always | none | none | "**deletes** the dequeued messages even on nack, and DequeueCount rises" | no | C/azure/input_queue_storage.go:144-148 (dequeue with visibility), :184-186 (ack ignores the error and calls DeleteMessage) |
| gcp_pubsub | SE | always (and creates the subscription if `create_subscription.enabled`) | `subscription` | none | "pulls from subscription S shared with production subscribers; nacks raise the delivery attempt (DLQ)" | no | C/gcp/input_pubsub.go:266-271, :331 (Nack), :165-222 (create_subscription) |
| amqp_0_9 | SE | always; worse with `auto_ack: true` (messages removed on delivery) or when the nack error matches `nack_reject_patterns` (dropped/DLX) | `queue` | none | "consumes from queue Q in competition with production; nacked messages are requeued as redelivered; queue_declare/bindings declare them" | no | C/impl/amqp09/input.go:371 (QueueDeclare), :556-566 (auto_ack, reject patterns, Nack requeue) |
| amqp_1 | SE | always | (`source_address`) | none | "receives with credit; nack = modified(delivery-failed): delivery-count rises (Service Bus moves the message to the DLQ at max delivery)" | no | C/amqp1/input.go:234, :301-307 |
| beanstalkd | SE | always | (tube) | none | "reserves a job, then releases it on nack with priority 2 (changes its priority, releases counter rises)" | no | C/beanstalkd/input.go:99, :110-112 |
| nsq | SE | always | `channel` | none | "subscribes channel C: creates it if new (it buffers forever), shares messages with its consumers, requeues raise attempts" | no | C/nsq/input.go:37, :197, :261, :285 (Requeue) |
| pulsar | SE | always | `subscription_name` (+`subscription_type`) | none | "subscribes as S (a durable cursor, created if new); shared types take messages, exclusive/failover block or take over; nacks raise redelivery count (DLQ)" | no | C/pulsar/input.go:79-84, :241 (Subscribe), :321 (Nack) |
| aws_kinesis | SE | always | `dynamodb.table` (lease/checkpoint table, not a group id) | `start_from_oldest` | "claims shard leases in DynamoDB table T and steals shards from production clients (rebalance); `dynamodb.create` creates T; `enhanced_fan_out` registers a stream consumer" | no | C/aws/kinesis/input_checkpointer.go:97 (ensureTableExists), :259-282 (Claim via UpdateItem); input.go:236 (EFO), :246 (steal) |
| mqtt | RS | SE if `clean_session: false` (persistent broker session), or if `client_id` is non-empty and `dynamic_client_id_suffix` is unset (the broker drops the production client with that id), or if `topics` use `$share/` | (`client_id`) | none | "takes over client id X / leaves a persistent session / joins shared subscription" | no | C/mqtt/input.go:58-60 (clean_session default true), :122, :247; C/mqtt/client.go:59-66 |
| nats | RS | SE if `queue` is set | `queue` | none | "joins queue group Q and takes its share of the messages" | no | C/nats/input.go:52, :184 (ChanQueueSubscribe) |
| nats_jetstream | RS ? | SE if `durable` is set (durable consumer: naks raise num_delivered toward max_deliver, ack floor state), if `bind`, if `queue` is set, or if `create_stream: true` | `durable`, `queue` | `deliver` | "uses durable consumer D / joins deliver group Q / creates stream S" | no | C/nats/input_jetstream.go:64-88, :302 (pull iff no deliver subject), :352-376, :490 (Nak) |
| nats_stream | RS ? | SE if `durable_name` or `queue` is set | `durable_name`, `queue` (+`client_id`, must be unique) | `start_from_oldest` | "uses durable subscription D / joins queue group Q" | no (NATS Streaming is EOL upstream) | C/nats/input_stream.go:284-310 |
| nats_kv | RS | none (KV watch) | none | (`include_history`) | none | no | C/nats/input_kv.go:199 (Watch), :254 |
| redis_list | SE | always | none | none | "pops (removes) items from list K with BLPOP/BRPOP; they are never pushed back" | no | C/redis/input_list.go:50, :110-113, :161 |
| redis_pubsub | RS | none (live tap) | none | none | none | no | C/redis/input_pubsub.go:129-131 |
| redis_streams | SE | always, even with an overridden group: XREADGROUP `>` advances the group's last-delivered-id, so the next F5 doesn't re-read the batch | `consumer_group` (+`client_id`) | `start_from_oldest` | "creates consumer group G on the streams (MKSTREAM if `create_streams`), advances its cursor, and leaves the entries pending in the PEL of client_id" | no | C/redis/input_streams.go:302-304 (XGroupCreate[MkStream]), :343 (XReadGroup), :269 (XAck) |
| nanomsg | REJ | REJ if `bind: true` (the default: it waits for peers to dial in); with `bind: false`, `socket_type: PULL` (the default) is SE ("takes a round-robin share of the PUSH peer's messages") and `SUB` is RS | none | none | see predicate | no | C/nanomsg/input.go:57-62 |

## Databases and CDC

| name | class | predicate | group field(s) | start field(s) | effect (SE) | depr | evidence |
|---|---|---|---|---|---|---|---|
| postgres_cdc | SE | always | `slot_name` (a slot, not a group) | `stream_snapshot` | "creates publication and replication slot S (persistent unless `temporary_slot`, and it retains WAL); an active slot is exclusive; heartbeat_interval writes WAL messages"; +ARN confirms the LSN | no | C/postgresql/pglogicalstream/logical_stream.go:221 (CreatePublication), :379-388 (CreateReplicationSlot); heartbeat.go:63 |
| pg_stream | SE | as postgres_cdc | `slot_name` | `stream_snapshot` | as postgres_cdc | yes | C/postgresql/input_pg_stream.go:642 |
| mysql_cdc | RS ? | +ARN (checkpoint_cache advances on nack) | none (`checkpoint_cache`/`checkpoint_key` are the cursor) | `stream_snapshot` | n/a | no | C/mysql/input_mysql_stream.go:335 (ARN wrap), :882 (ack ignores err) |
| microsoft_sql_server_cdc | SE ? | SE if `checkpoint_cache` is unset (the default SQL Server cache creates table `checkpoint_cache_table_name` and stored procedures); otherwise RS (+ARN) | none | `stream_snapshot` | "creates checkpoint table T in the source database" | no | C/mssqlserver/checkpoint_cache.go:189 (CREATE TABLE); input_mssqlserver_cdc.go:84 |
| oracledb_cdc | SE ? | as mssql (the default Oracle-table cache creates `checkpoint_cache_table_name`); also starts LogMiner sessions | none | `stream_snapshot`, `snapshot_mode` | "creates checkpoint table T in the source database" | no | C/oracledb/checkpoint_cache.go:227; input_oracledb_cdc.go:256-258 |
| mongodb_cdc | RS | +ARN | none (`checkpoint_cache`/`checkpoint_key`) | `stream_snapshot` | n/a | no | C/mongodb/cdc/input.go:344 (ARN), :1294 |
| cockroachdb_changefeed | RS | +ARN (the cursor is persisted on nack when `cursor_cache` is set) | none (`cursor_cache`) | none (a backfill when there is no cursor) | n/a | no | C/cockroachdb/input_changefeed.go:201, :381-397 |
| gcp_spanner_cdc | SE | always | `metadata_table` (partition-state table) | `start_timestamp` | "creates and writes partition metadata table T while reading (partitions CREATED→SCHEDULED→RUNNING); sharing production's T takes its partitions" | no | C/gcp/enterprise/changestreams/metadata/metadata.go:497-504, :537; input_spanner_cdc.go:110-113 |
| aws_dynamodb_cdc | SE ? | RS only if `checkpoint_table` exists, `checkpoint_namespace` is overridden and the snapshot writes nothing before ack | `checkpoint_namespace` (overridable like a group) | `start_from`, `snapshot_mode` | "creates checkpoint table T if missing; may write snapshot progress" | no | C/aws/dynamodb/checkpoint.go:150-176 (CreateTable), :584-599 (snapshot segment PutItem); input_cdc.go:229-233 |
| sap_hana | RS | +ARN | none (`checkpoint_cache`) | `incrementing_initial_value`, `timestamp_initial_value` | n/a | no | C/saphana/config.go:274 |
| salesforce_cdc | RS ? | +ARN | none (`checkpoint_cache`) | `replay_preset`, `stream_snapshot` | n/a | no | C/salesforce/input_salesforce_cdc.go:452 (ARN), :1028-1040 (saveStateLocked) |
| spicedb_watch | SE | always (when the cache is persistent or shared) | none (`cache`) | none | "writes the ZedToken to cache C even on nack (no ARN wrapper), so the next run starts after the captured batch" | no | C/spicedb/watch_input.go:35 (no wrapper), :259-262 |
| sql_select | RS | SE if `init_statement` or `init_files` is set (they run on connect); +ARN | none | none | "runs init_statement/init_files against the DB" | no | C/sql/conn_fields.go:95-113; input_sql_select.go:82, :277 |
| sql_raw | RS ? | SE if `query` is not read-only (e.g. `DELETE … RETURNING`, `UPDATE`) or `init_*` is set | none | none | "runs statement Q against the DB" | no | C/sql/input_sql_raw.go:60, :221 |
| cassandra | RS | none | none | none | none | no | C/cassandra/input.go:67, :127 |
| mongodb | RS | none (find/aggregate) | none | none | none | no | C/mongodb/input.go:86, :265 |
| redis_scan | RS | none (SCAN iterator) | none | none | none | no | C/redis/input_scan.go:113 |
| gcp_bigquery_select | RS | none (the query is billed) | none | none | none | no | C/gcp/input_bigquery_select.go:258, :240 |
| azure_cosmosdb | RS | none | none | none | none | no | C/azure/input_cosmosdb.go:82, :188 |
| azure_table_storage | RS | none | none | none | none | no | C/azure/input_table_storage.go:102, :178 |
| timeplus | RS | none (query; always ARN-wrapped) | none | none | none | no | C/timeplus/input.go:84, :186-187 |

## Files and object stores

| name | class | predicate | group | start | effect (SE) | depr | evidence |
|---|---|---|---|---|---|---|---|
| file | RS | none (`delete_on_finish` deletes only on ack) | none | none | n/a | no | B/io/input_file.go:181-183 |
| csv | RS | SE if `delete_on_finish: true`: the file is deleted at EOF, not on ack | none | none | "deletes file F once read to EOF" | no | B/io/input_csv.go:424-435 (closeCurrentFile → deleteFn), :526 |
| parquet | RS | none | none | none | n/a | no | C/parquet/input_parquet.go:58, :237 |
| aws_s3 | RS | SE if `sqs.url` is set (then it is the aws_sqs effect); `delete_objects` deletes only on ack (ARN) | none | none | "receives S3 event messages from SQS queue Q (receive count, visibility)" | no | C/aws/s3/input.go:300-301, :319, :578 |
| gcp_cloud_storage | RS | none (`delete_objects` only on ack) | none | none | n/a | no | C/gcp/input_cloud_storage.go:141, :205 |
| azure_blob_storage | RS | with `targets_input`: the worst of the child's class and its own; `delete_objects` only on ack | none | none | n/a | no | C/azure/input_blob_storage.go:139 (targets_input), :205, :328 |
| sftp | RS | SE if `watcher.enabled`: it writes a pending marker `!` to `watcher.cache` for each discovered path before ack, so a production watcher sharing the cache skips those paths; `delete_on_finish` only on ack | none | none | "marks discovered paths pending in cache C" | no | C/sftp/input.go:486-491, :606, :621-626 |
| hdfs | RS | none | none | none | n/a | no | C/hdfs/input.go:70, :146 |
| git | RS | none (clones to a local temp dir; `checkpoint_cache` is written on ack) | none | none | n/a | no | C/git/input.go:92, :130-139, :204 |

## Generators and pure

| name | class | predicate | group | start | effect | depr | evidence |
|---|---|---|---|---|---|---|---|
| generate | RS | none | none | none | n/a | no | B/pure/input_generate.go:90, :271 |
| inproc | REJ | always: it is fed only by an `inproc` output in another stream, and a debug session has none | none | none | n/a | no | B/pure/input_inproc.go:33 |
| twitter_search (template) | SE | always: its expanded input processors run during capture | none (`cache`, `cache_key`) | `backfill_period` | "writes the newest tweet id to cache C/K during capture; the next run starts after it" | no (experimental) | C/twitter/search_input.tmpl.yaml:135-137 (cache set), :83 (get) |

## Servers and sockets

| name | class | predicate | group | start | effect | depr | evidence |
|---|---|---|---|---|---|---|---|
| stdin | REJ | always | | | | no | B/io/input_stdin.go:115 |
| http_server | REJ | always (waits for clients) | | | | no | B/io/input_http_server.go:339 |
| socket_server | REJ | always | | | | no | B/io/input_socket_server.go:93 |
| otlp_grpc | REJ | always (gRPC server) | | | | no | C/otlp/input_grpc.go:245 |
| otlp_http | REJ | always (HTTP server) | | | | no | C/otlp/input_http.go:284 |
| gateway | REJ | always ("Receive messages delivered over HTTP"; it listens) | | | | no | C/gateway/input.go:112, :186, :191 |
| socket | REJ ? | it is a client, but data read from a live socket can't be re-read; `open_message_mapping` sends data | | | | no | B/io/input_socket.go:21-24, :49 |
| websocket | RS ? | SE if `open_message` is set (it sends to the server on connect) | none | none | "sends open_message to the server" | no | B/io/input_websocket.go:46, :54 |
| http_client | RS | SE ? if `verb` is not GET/HEAD or a `payload` is set (the request may mutate the server); `stream.enabled` is a live tap | none | none | "sends VERB requests to URL" | no | B/io/input_http_client.go:25, :69, :110-125 |
| subprocess | SE ? | always: it runs `name args`, and whatever the program does happens; `restart_on_exit` | none | none | "runs command X" | no (beta) | B/io/input_subprocess.go:19-23, :58 |

## Other services and APIs

| name | class | predicate | group | start | effect | depr | evidence |
|---|---|---|---|---|---|---|---|
| aws_cloudwatch_logs | RS | none (FilterLogEvents polling; the ack is a no-op) | none | `start_time` | n/a | no | C/aws/cloudwatch/input_logs.go:142, :423 |
| discord | RS | +ARN (the cache is written in the ack func, and the ack ignores the error) | none (`cache`) | none | n/a | no | C/discord/input.go:78 (ARN), :249-256 |
| slack | SE | always | none | none | "opens a Socket Mode connection for the app: Slack spreads events across the app's connections, so the debugger takes events from production; unacked envelopes are retried" | no | C/slack/input.go:36, :82, :125-129 (AckCtx) |
| slack_users | RS | none | none | none | n/a | no | C/slack/input_users.go:23, :135 |
| jira | RS | +ARN (the cursor is written after an acked page) | none (`cursor.cache`) | none | n/a | no | C/jira/input_jira.go:135, :680-694 |
| salesforce | RS | none | none | none | n/a | no | C/salesforce/input_salesforce.go:36, :332 |
| salesforce_graphql | RS | none | none | none | n/a | no | C/salesforce/input_salesforce_graphql.go:32, :285 |
| splunk | RS | none (search export; creates a transient search job) | none | none | n/a | no | C/splunk/input.go:47, :209 |

## Composites

| name | child-input field paths | notes | evidence |
|---|---|---|---|
| broker | `broker.inputs[*]` | (`copies` multiplies the children) | B/pure/input_broker.go:73, :96, :114 |
| sequence | `sequence.inputs[*]` | | B/pure/input_sequence.go:62, :456 |
| read_until | `read_until.input` | `restart_input` re-creates the child | B/pure/input_read_until.go:78, :129 |
| batched | `batched.child` | | B/pure/input_batched.go:16, :26 |
| dynamic | `dynamic.inputs.<key>` (a map) | inputs can also be added at run time through its HTTP API, so the walker can't see them all: consider REJ | B/io/input_dynamic.go:54, :96 |
| resource | `resource.resource` → `input_resources[label == value]` | | B/pure/input_resource.go:71 |
| (hybrid) azure_blob_storage | `azure_blob_storage.targets_input` | a real component that also holds a child input | C/azure/input_blob_storage.go:80, :139 |

## Counts (class under defaults)

- replay-safe: 44 (of which ? = 8: redpanda_common, nats_jetstream, nats_stream, mysql_cdc, salesforce_cdc, sql_raw, websocket, http_client)
- side-effecting: 22 (of which ? = 5: ockam_kafka, microsoft_sql_server_cdc, oracledb_cdc, aws_dynamodb_cdc, subprocess)
- rejected: 10 (of which ? = 1: socket)
- composite: 6 (plus the hybrid azure_blob_storage, which is counted under RS)
- total: 82
