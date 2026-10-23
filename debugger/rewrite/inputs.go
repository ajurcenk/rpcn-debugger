package rewrite

import (
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// InputClass is the AD-5 class of an input in the capture tree.
type InputClass = core.InputClass

// The input classes (AD-5), from best to worst. Unknown is treated as
// side-effecting.
const (
	ReplaySafe    = core.ClassReplaySafe
	SideEffecting = core.ClassSideEffecting
	Unknown       = core.ClassUnknown
	Rejected      = core.ClassRejected
)

// rank orders the classes: a composite takes its worst child's class.
func rank(c InputClass) int {
	switch c {
	case ReplaySafe:
		return 0
	case SideEffecting:
		return 1
	case Unknown:
		return 2
	default:
		return 3
	}
}

// InputFinding is the class of one input in the capture tree (AD-5): the
// rewrite reports it, and the Session decides what to refuse.
type InputFinding = core.InputFinding

// classFunc returns the class of an input from its component config conf
// (which may be nil or not a mapping), with the effect text.
type classFunc func(conf *yaml.Node) (InputClass, string)

// childShape is how a composite holds its child inputs.
type childShape int

const (
	childInputs childShape = iota // a list of input blocks
	childOne                      // one input block
	childMap                      // a map of input blocks
)

// childField is one field of a component config that holds child inputs.
type childField struct {
	name  string
	shape childShape
}

// inputRow is one row of the input table (AD-5).
type inputRow struct {
	// class is the class predicate; nil means replay-safe.
	class classFunc
	// groups are the consumer-group fields (dotted paths in the config).
	// Unless the capture overrides it, a set group field makes the input
	// side-effecting.
	groups []string
	// starts are the start-offset fields (dotted paths).
	starts []string
	// children are the fields that hold child inputs. The input takes
	// its worst child's class.
	children []childField
	// overridable marks the inputs whose literal groups the capture
	// replaces with the debug group (E3 entry 2: only redpanda and
	// kafka_franz, and only at the top level).
	overridable bool
}

// always returns a classFunc with a fixed class and effect.
func always(c InputClass, effect string) classFunc {
	return func(*yaml.Node) (InputClass, string) { return c, effect }
}

// seIf returns a classFunc that is side-effecting with effect when cond
// holds, and replay-safe otherwise.
func seIf(effect string, cond func(conf *yaml.Node) bool) classFunc {
	return func(conf *yaml.Node) (InputClass, string) {
		if cond(conf) {
			return SideEffecting, effect
		}
		return ReplaySafe, ""
	}
}

// fieldAt returns the node at the dotted path in the mapping conf, or nil.
func fieldAt(conf *yaml.Node, path string) *yaml.Node {
	n := conf
	for key := range strings.SplitSeq(path, ".") {
		n = value(n, key)
		if n == nil {
			return nil
		}
	}
	return n
}

// isSet reports whether the field at path is set: a non-empty literal, a
// non-empty list or mapping, or an interpolation (the worse case).
func isSet(conf *yaml.Node, path string) bool {
	n := fieldAt(conf, path)
	switch {
	case isNull(n):
		return false
	case n.Kind == yaml.ScalarNode:
		return n.Value != ""
	default:
		return len(n.Content) > 0
	}
}

// boolIs reports whether the bool field at path is want, with def when it
// is absent. An interpolation or a value that isn't a bool counts as want,
// the case that decides the class.
func boolIs(conf *yaml.Node, path string, want, def bool) bool {
	n := fieldAt(conf, path)
	if isNull(n) {
		return def == want
	}
	var b bool
	if n.Kind != yaml.ScalarNode || isInterpolated(n) || n.Decode(&b) != nil {
		return true
	}
	return b == want
}

// strIs reports whether the string field at path (def when absent) is one
// of vals, ignoring case. An interpolation or a non-scalar counts as a
// match, the case that decides the class.
func strIs(conf *yaml.Node, path, def string, vals ...string) bool {
	n := fieldAt(conf, path)
	v := def
	if !isNull(n) {
		if n.Kind != yaml.ScalarNode || isInterpolated(n) {
			return true
		}
		v = n.Value
	}
	for _, x := range vals {
		if strings.EqualFold(v, x) {
			return true
		}
	}
	return false
}

// strNotIn is strIs with the match inverted, but an interpolation or a
// non-scalar still counts as the deciding case.
func strNotIn(conf *yaml.Node, path, def string, vals ...string) bool {
	n := fieldAt(conf, path)
	if !isNull(n) && (n.Kind != yaml.ScalarNode || isInterpolated(n)) {
		return true
	}
	return !strIs(conf, path, def, vals...)
}

// anyOr reports whether any of conds holds.
func anyOr(conds ...func(*yaml.Node) bool) func(*yaml.Node) bool {
	return func(conf *yaml.Node) bool {
		for _, c := range conds {
			if c(conf) {
				return true
			}
		}
		return false
	}
}

func set(path string) func(*yaml.Node) bool {
	return func(conf *yaml.Node) bool { return isSet(conf, path) }
}

func enabled(path string, def bool) func(*yaml.Node) bool {
	return func(conf *yaml.Node) bool { return boolIs(conf, path, true, def) }
}

// committed is the kafka_franz family's start_offset predicate: with
// committed, a fresh debug group has no commits to start from and a kept
// group reads production's (research: C/kafka/franz_reader.go:198-215).
var committed = seIf("start_offset: committed starts from the consumer group's committed offsets", func(conf *yaml.Node) bool {
	return strIs(conf, "start_offset", "earliest", "committed")
})

// unsure is the effect of a row the research couldn't settle; the user's
// decision of 2026-10-23 makes those side-effecting.
func unsure(what string) classFunc {
	return always(SideEffecting, what+" (not proven safe, so treated as side-effecting)")
}

// mqttUnsafe: a persistent session, a fixed client id that takes over the
// production client, or a shared subscription (C/mqtt/input.go:58-60,
// :122, :247; C/mqtt/client.go:59-66).
func mqttUnsafe(conf *yaml.Node) bool {
	if boolIs(conf, "clean_session", false, true) {
		return true
	}
	if isSet(conf, "client_id") && !isSet(conf, "dynamic_client_id_suffix") {
		return true
	}
	topics := value(conf, "topics")
	if topics != nil && topics.Kind == yaml.SequenceNode {
		for _, t := range topics.Content {
			if t.Kind != yaml.ScalarNode || isInterpolated(t) || strings.HasPrefix(t.Value, "$share/") {
				return true
			}
		}
	}
	return false
}

// nanomsg: bind (the default) waits for peers to dial in; a PULL socket
// takes a share of the PUSH peer's messages; SUB is a tap
// (C/nanomsg/input.go:57-62).
func nanomsg(conf *yaml.Node) (InputClass, string) {
	if boolIs(conf, "bind", true, true) {
		return Rejected, "it binds and waits for peers to dial in"
	}
	if strNotIn(conf, "socket_type", "PULL", "SUB") {
		return SideEffecting, "takes a round-robin share of the PUSH peer's messages"
	}
	return ReplaySafe, ""
}

// websocket is a research "?" row, so it is side-effecting (the user's
// decision of 2026-10-23); open_message also sends data on connect
// (B/io/input_websocket.go:46, :54).
func websocket(conf *yaml.Node) (InputClass, string) {
	if isSet(conf, "open_message") {
		return SideEffecting, "sends open_message to the server on connect"
	}
	return unsure("reads from a live connection, so what it reads may not be readable again")(conf)
}

// httpClient: a verb other than GET or HEAD, or a payload, may change the
// server (B/io/input_http_client.go:69, :110-125).
func httpClient(conf *yaml.Node) (InputClass, string) {
	if strNotIn(conf, "verb", "GET", "GET", "HEAD") || isSet(conf, "payload") {
		return SideEffecting, "sends requests that may change the server (a verb other than GET or HEAD, or a payload)"
	}
	return ReplaySafe, ""
}

var (
	kafkaGroup = []string{"consumer_group"}
	franzStart = []string{"start_offset", "start_from_oldest"}
)

// inputTable is the AD-5 input table: one row per input component of the
// stock binary (Connect v4.115.0 with components/all, CGO_ENABLED=0), from
// story-input-table-and-input-tree-walker-input-classes.md, with the user's
// decisions of 2026-10-23. An input with no row is unknown. Every row with
// the auto_replay_nacks toggle is classed as if it were true, because the
// capture forces it (forceReplayNacks).
var inputTable = map[string]inputRow{
	// Kafka family. A kept group is side-effecting through groups.
	"kafka":       {groups: kafkaGroup, starts: []string{"start_from_oldest"}},
	"kafka_franz": {class: committed, groups: kafkaGroup, starts: franzStart, overridable: true},
	"redpanda":    {class: committed, groups: kafkaGroup, starts: franzStart, overridable: true},
	// Stays replay-safe until E3 entry 6 probes the shared client.
	"redpanda_common": {class: committed, groups: kafkaGroup, starts: []string{"start_offset"}},
	// C/ockam/input_kafka.go:97-128.
	"ockam_kafka": {
		class:  unsure("starts a local Ockam node, enrolls its enrollment_ticket and may claim its relay"),
		groups: []string{"kafka.consumer_group"}, starts: []string{"kafka.start_offset", "kafka.start_from_oldest"},
	},
	// C/redpanda/migrator/migrator.go:398-402, :677-683.
	"redpanda_migrator": {class: always(Rejected, "it needs its paired redpanda_migrator output, which the capture doesn't have")},
	"schema_registry":   {},

	// Cloud queues, brokers and streams.
	"aws_sqs":             {class: always(SideEffecting, "receives messages from the queue: their receive count rises (redrive to the dead-letter queue) and they stay hidden for message_timeout")},
	"azure_queue_storage": {class: always(SideEffecting, "deletes the dequeued messages even on nack, and their dequeue count rises")},
	"gcp_pubsub": {
		class:  always(SideEffecting, "pulls from the subscription shared with production subscribers, and nacks raise the delivery attempt (dead-letter topic); create_subscription creates it"),
		groups: []string{"subscription"},
	},
	"amqp_0_9": {
		class:  always(SideEffecting, "consumes from the queue in competition with production, and nacked messages are requeued as redelivered (auto_ack removes them)"),
		groups: []string{"queue"},
	},
	"amqp_1":     {class: always(SideEffecting, "receives with credit, and a nack raises the delivery count (Service Bus moves the message to the dead-letter queue)")},
	"beanstalkd": {class: always(SideEffecting, "reserves a job, then releases it with a changed priority")},
	"nsq": {
		class:  always(SideEffecting, "subscribes the channel (creating it if new), shares its messages with its consumers, and requeues raise their attempts"),
		groups: []string{"channel"},
	},
	"pulsar": {
		class:  always(SideEffecting, "subscribes as a durable subscription (created if new), takes or blocks messages of its other consumers, and nacks raise the redelivery count"),
		groups: []string{"subscription_name"},
	},
	"aws_kinesis": {
		class:  always(SideEffecting, "claims shard leases in the DynamoDB checkpoint table, stealing shards from production clients (dynamodb.create creates it)"),
		groups: []string{"dynamodb.table"}, starts: []string{"start_from_oldest"},
	},
	"mqtt": {class: seIf("takes over the client id, leaves a persistent session or joins a shared subscription", mqttUnsafe)},
	// C/nats/input.go:184 (ChanQueueSubscribe).
	"nats":           {class: seIf("joins the queue group and takes its share of the messages", set("queue")), groups: []string{"queue"}},
	"nats_jetstream": {class: unsure("may use a durable consumer, join a deliver group or create a stream"), groups: []string{"durable", "queue"}, starts: []string{"deliver"}},
	"nats_stream":    {class: unsure("may use a durable subscription or join a queue group"), groups: []string{"durable_name", "queue"}, starts: []string{"start_from_oldest"}},
	"nats_kv":        {},
	"redis_list":     {class: always(SideEffecting, "pops the items from the list, and they are never pushed back")},
	"redis_pubsub":   {},
	// XREADGROUP > advances the group even when it is overridden
	// (C/redis/input_streams.go:302-304, :343).
	"redis_streams": {
		class:  always(SideEffecting, "creates the consumer group on the streams, advances its cursor and leaves the entries pending"),
		groups: kafkaGroup, starts: []string{"start_from_oldest"},
	},
	"nanomsg": {class: nanomsg},

	// Databases and CDC.
	"postgres_cdc": {
		class:  always(SideEffecting, "creates the publication and the replication slot (which retains WAL unless temporary_slot), and an active slot is exclusive"),
		groups: []string{"slot_name"}, starts: []string{"stream_snapshot"},
	},
	"pg_stream": {
		class:  always(SideEffecting, "creates the publication and the replication slot (which retains WAL unless temporary_slot), and an active slot is exclusive"),
		groups: []string{"slot_name"}, starts: []string{"stream_snapshot"},
	},
	"mysql_cdc":                {class: unsure("may advance its checkpoint_cache"), starts: []string{"stream_snapshot"}},
	"microsoft_sql_server_cdc": {class: unsure("creates the checkpoint table and stored procedures in the source database unless checkpoint_cache is set"), starts: []string{"stream_snapshot"}},
	"oracledb_cdc":             {class: unsure("creates the checkpoint table in the source database unless checkpoint_cache is set, and starts LogMiner sessions"), starts: []string{"stream_snapshot", "snapshot_mode"}},
	"mongodb_cdc":              {starts: []string{"stream_snapshot"}},
	"cockroachdb_changefeed":   {},
	"gcp_spanner_cdc": {
		class:  always(SideEffecting, "creates and writes the partition metadata table, and sharing production's takes its partitions"),
		groups: []string{"metadata_table"}, starts: []string{"start_timestamp"},
	},
	"aws_dynamodb_cdc": {
		class:  unsure("creates the checkpoint table if missing and may write snapshot progress"),
		groups: []string{"checkpoint_namespace"}, starts: []string{"start_from", "snapshot_mode"},
	},
	"sap_hana":       {starts: []string{"incrementing_initial_value", "timestamp_initial_value"}},
	"salesforce_cdc": {class: unsure("may advance its checkpoint_cache"), starts: []string{"replay_preset", "stream_snapshot"}},
	// No auto_replay_nacks wrapper: the token is written even on nack
	// (C/spicedb/watch_input.go:259-262).
	"spicedb_watch": {class: always(SideEffecting, "writes the ZedToken to its cache even on nack, so the next run starts after the captured batch")},
	// C/sql/conn_fields.go:95-113.
	"sql_select":          {class: seIf("runs init_statement or init_files against the database", anyOr(set("init_statement"), set("init_files")))},
	"sql_raw":             {class: always(SideEffecting, "runs its query against the database, which may change it")},
	"cassandra":           {},
	"mongodb":             {},
	"redis_scan":          {},
	"gcp_bigquery_select": {},
	"azure_cosmosdb":      {},
	"azure_table_storage": {},
	"timeplus":            {},

	// Files and object stores. delete_on_finish and delete_objects act
	// only on ack, which the capture never gives.
	"file": {},
	// The file is deleted at EOF, not on ack (B/io/input_csv.go:424-435).
	"csv":               {class: seIf("deletes the file once it is read to the end", enabled("delete_on_finish", false))},
	"parquet":           {},
	"aws_s3":            {class: seIf("receives the S3 event messages from the SQS queue: their receive count rises and they stay hidden", set("sqs.url"))},
	"gcp_cloud_storage": {},
	"azure_blob_storage": {
		children: []childField{{"targets_input", childOne}},
	},
	// The pending marker is written before ack (C/sftp/input.go:486-491).
	"sftp": {class: seIf("marks the discovered paths pending in the watcher cache, so a production watcher sharing it skips them", enabled("watcher.enabled", false))},
	"hdfs": {},
	"git":  {},

	// Generators.
	"generate": {},
	"inproc":   {class: always(Rejected, "it is fed only by an inproc output of another stream, and a debug session has none")},
	// Its expanded input processors write the newest tweet id to the cache
	// during capture (C/twitter/search_input.tmpl.yaml:135-137).
	"twitter_search": {class: always(SideEffecting, "writes the newest tweet id to its cache during capture, so the next run starts after it")},

	// Servers and sockets.
	"stdin":         {class: always(Rejected, "stdin carries the debugger's DAP stream")},
	"http_server":   {class: always(Rejected, "it is a server and waits for clients")},
	"socket_server": {class: always(Rejected, "it is a server and waits for clients")},
	"otlp_grpc":     {class: always(Rejected, "it is a server and waits for clients")},
	"otlp_http":     {class: always(Rejected, "it is a server and waits for clients")},
	"gateway":       {class: always(Rejected, "it is a server and waits for clients")},
	"socket":        {class: always(SideEffecting, "reads from a live connection, so what it reads can't be read again")},
	"websocket":     {class: websocket},
	"http_client":   {class: httpClient},
	"subprocess":    {class: always(SideEffecting, "runs its command, and whatever the program does happens")},

	// Other services and APIs. Their cursors and caches are written on
	// ack only.
	"aws_cloudwatch_logs": {starts: []string{"start_time"}},
	"discord":             {},
	// Socket Mode spreads events across the app's connections
	// (C/slack/input.go:125-129).
	"slack":              {class: always(SideEffecting, "opens a Socket Mode connection for the app, which takes events from production's connections")},
	"slack_users":        {},
	"jira":               {},
	"salesforce":         {},
	"salesforce_graphql": {},
	"splunk":             {},

	// Composites: they take their worst child's class.
	"broker":     {children: []childField{{"inputs", childInputs}}},
	"sequence":   {children: []childField{{"inputs", childInputs}}},
	"read_until": {children: []childField{{"input", childOne}}},
	"batched":    {children: []childField{{"child", childOne}}},
	// Inputs can be added at run time through its HTTP API, so the walker
	// can't see them all.
	"dynamic": {
		class:    always(Rejected, "inputs can be added to it at run time through its HTTP API, so the debugger can't see them all"),
		children: []childField{{"inputs", childMap}},
	},
	// resource is followed into input_resources by the walker.
	"resource": {},
}
