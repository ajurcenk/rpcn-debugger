package rewrite

import (
	"fmt"
	"maps"
	"slices"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/schema"

	_ "github.com/redpanda-data/connect/v4/public/components/all"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// cgoOnly are the inputs that register only under cgo, so the stock binary
// (CGO_ENABLED=0) doesn't have them: they need no row.
var cgoOnly = []string{"tigerbeetle_cdc", "zmq4"}

// The input table has exactly one row per input of the stock binary.
func TestInputTableIsStock(t *testing.T) {
	var stock []string
	schema.Standard("x", "x").Environment().WalkInputs(func(name string, _ *service.ConfigView) {
		stock = append(stock, name)
	})
	if len(stock) == 0 {
		t.Fatal("the stock environment has no inputs")
	}
	for _, n := range stock {
		if _, ok := inputTable[n]; !ok && !slices.Contains(cgoOnly, n) {
			t.Errorf("stock input %s has no row in the input table", n)
		}
	}
	for n := range inputTable {
		if !slices.Contains(stock, n) {
			t.Errorf("input table row %s is not a stock input", n)
		}
	}
}

// inputCase is one input config and the class the walker gives it.
type inputCase struct {
	// conf is the component config as YAML flow (or a block indented
	// for the component key), "{}" when empty.
	conf string
	// group is the DebugGroup option, and keep KeepConsumerGroup.
	group string
	keep  bool
	want  InputClass
}

// inputCases covers every row of the input table. A field-dependent row
// (fieldDependent) has a case on each side of its predicate.
var inputCases = map[string][]inputCase{
	"kafka": {
		{conf: "{addresses: [k:9092], topics: [t]}", want: ReplaySafe},
		{conf: "{addresses: [k:9092], topics: [t], consumer_group: prod}", group: "dbg", want: ReplaySafe},
		{conf: "{addresses: [k:9092], topics: [t], consumer_group: prod}", group: "dbg", keep: true, want: SideEffecting},
		{conf: "{addresses: [k:9092], topics: [t], consumer_group: '${G}'}", group: "dbg", want: SideEffecting},
	},
	"kafka_franz": {
		{conf: "{seed_brokers: [k:9092], topics: [t]}", want: ReplaySafe},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: prod}", group: "dbg", want: ReplaySafe},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: prod}", want: SideEffecting},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: prod}", group: "dbg", keep: true, want: SideEffecting},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: '${G}'}", group: "dbg", want: SideEffecting},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: prod, start_offset: committed}", group: "dbg", want: SideEffecting},
		{conf: "{seed_brokers: [k:9092], topics: [t], start_offset: '${S}'}", want: SideEffecting},
		{conf: "{seed_brokers: [k:9092], topics: [t], start_offset: latest}", want: ReplaySafe},
	},
	"redpanda": {
		{conf: "{seed_brokers: [k:9092], topics: [t]}", want: ReplaySafe},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: prod}", group: "dbg", want: ReplaySafe},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: prod}", want: SideEffecting},
		{conf: "{seed_brokers: [k:9092], topics: [t], start_offset: committed}", want: SideEffecting},
		// Without seed_brokers it falls back to the shared client (E3 entry 6).
		{conf: "{topics: [t]}", want: Rejected},
		{conf: "{topics: [t], consumer_group: prod}", group: "dbg", want: Rejected},
	},
	// Always the shared client, which a debug session doesn't have (E3 entry 6).
	"redpanda_common": {
		{conf: "{topics: [t]}", want: Rejected},
		{conf: "{topics: [t], consumer_group: prod}", group: "dbg", want: Rejected},
		{conf: "{topics: [t], consumer_group: prod}", group: "dbg", keep: true, want: Rejected},
	},
	"ockam_kafka":       {{conf: "{kafka: {topics: [t]}}", want: SideEffecting}},
	"redpanda_migrator": {{conf: "{topics: [t]}", want: Rejected}},
	"schema_registry":   {{conf: "{url: http://r}", want: ReplaySafe}},

	"aws_sqs":             {{conf: "{url: http://q}", want: SideEffecting}},
	"azure_queue_storage": {{conf: "{queue_name: q}", want: SideEffecting}},
	"gcp_pubsub":          {{conf: "{project: p, subscription: s}", want: SideEffecting}},
	"amqp_0_9":            {{conf: "{urls: [amqp://a], queue: q}", want: SideEffecting}},
	"amqp_1":              {{conf: "{urls: [amqp://a], source_address: q}", want: SideEffecting}},
	"beanstalkd":          {{conf: "{address: b:11300}", want: SideEffecting}},
	"nsq":                 {{conf: "{nsqd_tcp_addresses: [n:4150], topic: t, channel: c}", want: SideEffecting}},
	"pulsar":              {{conf: "{url: pulsar://p, topics: [t], subscription_name: s}", want: SideEffecting}},
	"aws_kinesis":         {{conf: "{streams: [s]}", want: SideEffecting}},
	"mqtt": {
		{conf: "{urls: [tcp://m:1883], topics: [t]}", want: ReplaySafe},
		{conf: "{urls: [tcp://m:1883], topics: [t], clean_session: false}", want: SideEffecting},
		{conf: "{urls: [tcp://m:1883], topics: [t], clean_session: '${C}'}", want: SideEffecting},
		{conf: "{urls: [tcp://m:1883], topics: [t], client_id: prod}", want: SideEffecting},
		{conf: "{urls: [tcp://m:1883], topics: [t], client_id: prod, dynamic_client_id_suffix: nanoid}", want: ReplaySafe},
		{conf: "{urls: [tcp://m:1883], topics: [$share/g/t]}", want: SideEffecting},
	},
	"nats": {
		{conf: "{urls: [nats://n], subject: s}", want: ReplaySafe},
		{conf: "{urls: [nats://n], subject: s, queue: q}", want: SideEffecting},
		{conf: "{urls: [nats://n], subject: s, queue: q}", group: "dbg", want: ReplaySafe},
		{conf: "{urls: [nats://n], subject: s, queue: q}", group: "dbg", keep: true, want: SideEffecting},
	},
	"nats_jetstream": {{conf: "{urls: [nats://n], subject: s}", want: SideEffecting}},
	"nats_stream":    {{conf: "{urls: [nats://n], cluster_id: c, subject: s}", want: SideEffecting}},
	"nats_kv":        {{conf: "{urls: [nats://n], bucket: b}", want: ReplaySafe}},
	"redis_list":     {{conf: "{url: redis://r, key: k}", want: SideEffecting}},
	"redis_pubsub":   {{conf: "{url: redis://r, channels: [c]}", want: ReplaySafe}},
	"redis_streams":  {{conf: "{url: redis://r, streams: [s]}", want: SideEffecting}},
	"nanomsg": {
		{conf: "{urls: [tcp://n:5555]}", want: Rejected},
		{conf: "{urls: [tcp://n:5555], bind: '${B}'}", want: Rejected},
		{conf: "{urls: [tcp://n:5555], bind: false}", want: SideEffecting},
		{conf: "{urls: [tcp://n:5555], bind: false, socket_type: SUB}", want: ReplaySafe},
		{conf: "{urls: [tcp://n:5555], bind: false, socket_type: '${T}'}", want: SideEffecting},
	},

	"postgres_cdc":             {{conf: "{dsn: postgres://p, slot_name: s, tables: [t]}", want: SideEffecting}},
	"pg_stream":                {{conf: "{dsn: postgres://p, slot_name: s, tables: [t]}", want: SideEffecting}},
	"mysql_cdc":                {{conf: "{dsn: m, tables: [t], checkpoint_cache: c}", want: SideEffecting}},
	"microsoft_sql_server_cdc": {{conf: "{connection_string: s}", want: SideEffecting}},
	"oracledb_cdc":             {{conf: "{connection_string: s}", want: SideEffecting}},
	"mongodb_cdc":              {{conf: "{url: mongodb://m, database: d, collections: [c], checkpoint_cache: c}", want: ReplaySafe}},
	"cockroachdb_changefeed":   {{conf: "{dsn: postgres://c, tables: [t]}", want: ReplaySafe}},
	"gcp_spanner_cdc":          {{conf: "{project_id: p, instance_id: i, database_id: d, stream_id: s}", want: SideEffecting}},
	"aws_dynamodb_cdc":         {{conf: "{tables: [t]}", want: SideEffecting}},
	"sap_hana":                 {{conf: "{dsn: h}", want: ReplaySafe}},
	"salesforce_cdc":           {{conf: "{org_url: https://s}", want: SideEffecting}},
	"spicedb_watch":            {{conf: "{endpoint: s:50051, cache: c}", want: SideEffecting}},
	"sql_select": {
		{conf: "{driver: postgres, dsn: p, table: t, columns: ['*']}", want: ReplaySafe},
		{conf: "{driver: postgres, dsn: p, table: t, columns: ['*'], init_statement: 'CREATE TABLE x (a int)'}", want: SideEffecting},
		{conf: "{driver: postgres, dsn: p, table: t, columns: ['*'], init_files: [a.sql]}", want: SideEffecting},
		{conf: "{driver: postgres, dsn: p, table: t, columns: ['*'], init_files: []}", want: ReplaySafe},
	},
	"sql_raw":             {{conf: "{driver: postgres, dsn: p, query: 'SELECT 1'}", want: SideEffecting}},
	"cassandra":           {{conf: "{addresses: [c], query: 'SELECT 1'}", want: ReplaySafe}},
	"mongodb":             {{conf: "{url: mongodb://m, database: d, collection: c}", want: ReplaySafe}},
	"redis_scan":          {{conf: "{url: redis://r}", want: ReplaySafe}},
	"gcp_bigquery_select": {{conf: "{project: p, table: t, columns: [a]}", want: ReplaySafe}},
	"azure_cosmosdb":      {{conf: "{endpoint: e, database: d, container: c, query: 'SELECT 1'}", want: ReplaySafe}},
	"azure_table_storage": {{conf: "{table_name: t}", want: ReplaySafe}},
	"timeplus":            {{conf: "{query: 'SELECT 1'}", want: ReplaySafe}},

	"file": {{conf: "{paths: [a.txt], delete_on_finish: true}", want: ReplaySafe}},
	"csv": {
		{conf: "{paths: [a.csv]}", want: ReplaySafe},
		{conf: "{paths: [a.csv], delete_on_finish: false}", want: ReplaySafe},
		{conf: "{paths: [a.csv], delete_on_finish: true}", want: SideEffecting},
		{conf: "{paths: [a.csv], delete_on_finish: '${D}'}", want: SideEffecting},
	},
	"parquet": {{conf: "{paths: [a.parquet]}", want: ReplaySafe}},
	"aws_s3": {
		{conf: "{bucket: b}", want: ReplaySafe},
		{conf: "{bucket: b, sqs: {url: ''}}", want: ReplaySafe},
		{conf: "{bucket: b, sqs: {url: http://q}}", want: SideEffecting},
		{conf: "{bucket: b, sqs: {url: '${Q}'}}", want: SideEffecting},
	},
	"gcp_cloud_storage": {{conf: "{bucket: b}", want: ReplaySafe}},
	"azure_blob_storage": {
		{conf: "{container: c}", want: ReplaySafe},
		{conf: "{container: c, targets_input: {aws_sqs: {url: http://q}}}", want: SideEffecting},
	},
	"sftp": {
		{conf: "{address: s:22, paths: [a]}", want: ReplaySafe},
		{conf: "{address: s:22, paths: [a], watcher: {enabled: true, cache: c}}", want: SideEffecting},
	},
	"hdfs": {{conf: "{hosts: [h], directory: d}", want: ReplaySafe}},
	"git":  {{conf: "{repository_url: https://g}", want: ReplaySafe}},

	"generate":       {{conf: "{mapping: 'root = {}'}", want: ReplaySafe}},
	"inproc":         {{conf: "foo", want: Rejected}},
	"twitter_search": {{conf: "{query: q, api_key: k, api_secret: s}", want: SideEffecting}},

	"stdin":         {{conf: "{}", want: Rejected}},
	"http_server":   {{conf: "{path: /p}", want: Rejected}},
	"socket_server": {{conf: "{network: tcp, address: ':1'}", want: Rejected}},
	"otlp_grpc":     {{conf: "{}", want: Rejected}},
	"otlp_http":     {{conf: "{}", want: Rejected}},
	"gateway":       {{conf: "{}", want: Rejected}},
	"socket":        {{conf: "{network: tcp, address: 's:1'}", want: SideEffecting}},
	"websocket": {
		{conf: "{url: ws://w}", want: SideEffecting},
		{conf: "{url: ws://w, open_message: hello}", want: SideEffecting},
	},
	"http_client": {
		{conf: "{url: http://h}", want: ReplaySafe},
		{conf: "{url: http://h, verb: head}", want: ReplaySafe},
		{conf: "{url: http://h, verb: POST}", want: SideEffecting},
		{conf: "{url: http://h, verb: '${V}'}", want: SideEffecting},
		{conf: "{url: http://h, payload: '{}'}", want: SideEffecting},
	},
	"subprocess": {{conf: "{name: cat}", want: SideEffecting}},

	"aws_cloudwatch_logs": {{conf: "{log_group_name: g}", want: ReplaySafe}},
	"discord":             {{conf: "{channel_id: c, bot_token: t}", want: ReplaySafe}},
	"slack":               {{conf: "{app_token: a, bot_token: b}", want: SideEffecting}},
	"slack_users":         {{conf: "{bot_token: b}", want: ReplaySafe}},
	"jira":                {{conf: "{base_url: https://j}", want: ReplaySafe}},
	"salesforce":          {{conf: "{org_url: https://s}", want: ReplaySafe}},
	"salesforce_graphql":  {{conf: "{org_url: https://s}", want: ReplaySafe}},
	"splunk":              {{conf: "{url: https://s, query: q}", want: ReplaySafe}},

	"broker": {
		{conf: "{inputs: [{generate: {mapping: 'root = 1'}}]}", want: ReplaySafe},
		{conf: "{inputs: [{generate: {mapping: 'root = 1'}}, {aws_sqs: {url: http://q}}]}", want: SideEffecting},
		{conf: "{inputs: [{aws_sqs: {url: http://q}}, {http_server: {}}]}", want: Rejected},
	},
	"sequence": {
		{conf: "{inputs: [{generate: {mapping: 'root = 1'}}]}", want: ReplaySafe},
		{conf: "{inputs: [{redis_list: {url: redis://r, key: k}}]}", want: SideEffecting},
	},
	"read_until": {
		{conf: "{check: 'true', input: {generate: {mapping: 'root = 1'}}}", want: ReplaySafe},
		{conf: "{check: 'true', input: {no_such_input: {}}}", want: Unknown},
	},
	"batched": {
		{conf: "{child: {generate: {mapping: 'root = 1'}}, policy: {count: 1}}", want: ReplaySafe},
		{conf: "{child: {socket: {network: tcp, address: 's:1'}}, policy: {count: 1}}", want: SideEffecting},
	},
	"dynamic": {{conf: "{inputs: {a: {generate: {mapping: 'root = 1'}}}}", want: Rejected}},
	"resource": {
		{conf: "q", want: SideEffecting},
		{conf: "missing", want: Unknown},
		{conf: "'${R}'", want: Unknown},
	},
}

// fieldDependent are the rows whose class depends on their fields (or
// children): their cases must reach more than one class.
var fieldDependent = []string{
	"kafka", "kafka_franz", "redpanda", "mqtt", "nats", "nanomsg",
	"sql_select", "csv", "aws_s3", "azure_blob_storage", "sftp", "http_client",
	"broker", "sequence", "read_until", "batched", "resource",
}

func TestInputTableRows(t *testing.T) {
	for name := range inputTable {
		if len(inputCases[name]) == 0 {
			t.Errorf("input table row %s has no test case", name)
		}
	}
	for _, name := range fieldDependent {
		classes := map[InputClass]bool{}
		for _, c := range inputCases[name] {
			classes[c.want] = true
		}
		if len(classes) < 2 {
			t.Errorf("field-dependent row %s: cases reach only %v", name, classes)
		}
	}
	for name, cases := range inputCases {
		if _, ok := inputTable[name]; !ok {
			t.Errorf("test cases for %s, which has no row", name)
		}
		for _, c := range cases {
			src := "input:\n  " + name + ": " + c.conf + "\n" +
				"input_resources:\n  - label: q\n    aws_sqs: {url: http://q}\n"
			p, err := Rewrite([]byte(src), Options{DebugGroup: c.group, KeepConsumerGroup: c.keep})
			if err != nil {
				t.Errorf("%s %s: %v", name, c.conf, err)
				continue
			}
			if len(p.Inputs) == 0 {
				t.Errorf("%s %s: no findings", name, c.conf)
				continue
			}
			f := p.Inputs[0]
			if f.UserPath != "input" || f.Component != name || f.Line != 2 || f.Class != c.want {
				t.Errorf("%s %s (group %q, keep %v): finding %+v, want class %s", name, c.conf, c.group, c.keep, f, c.want)
			}
			if (f.Class == ReplaySafe) != (f.Effect == "") {
				t.Errorf("%s %s: class %s with effect %q", name, c.conf, f.Class, f.Effect)
			}
		}
	}
	// Start-offset fields are named on the Kafka family rows.
	for _, n := range []string{"kafka", "kafka_franz", "redpanda", "redpanda_common"} {
		if len(inputTable[n].starts) == 0 {
			t.Errorf("%s has no start-offset fields", n)
		}
	}
}

// The E3 entry 5 matrix: every overridable group in the capture tree gets
// <DebugGroup>-<k>, k in walker order, with one warning each; a kept,
// interpolated or left-alone group makes its input side-effecting; a start
// offset at latest gets one warning (AD-5, D4, D5).
func TestGroupOverrideMatrix(t *testing.T) {
	const rp = "{seed_brokers: [k:9092], topics: [t], consumer_group: %s}"
	broker := func(kind string) string {
		return "input:\n  " + kind + ":\n    inputs:\n" +
			"      - redpanda: " + fmt.Sprintf(rp, "a") + "\n" +
			"      - redpanda: " + fmt.Sprintf(rp, "b") + "\n"
	}
	franzRes := "input_resources:\n  - label: r\n    kafka_franz: {seed_brokers: [k:9092], topics: [t], consumer_group: prod}\n"
	replaced := func(path, noun, from, to string, line int) string {
		return fmt.Sprintf("%s (line %d): %s %q is replaced by the debug group %q for the capture", path, line, noun, from, to)
	}
	for name, c := range map[string]struct {
		src  string
		keep bool
		// capture and resources must be in the capture config and in
		// Plan.Resources; absent must be in neither.
		capture, resources, absent []string
		// classes are the findings' classes in walker order.
		classes []InputClass
		// effect must be in the first side-effecting finding's effect.
		effect   string
		warnings []string
	}{
		"nested": {
			src:     broker("broker"),
			capture: []string{"consumer_group: dbg-0", "consumer_group: dbg-1"}, absent: []string{"consumer_group: a", "consumer_group: b"},
			classes: []InputClass{ReplaySafe, ReplaySafe, ReplaySafe},
			warnings: []string{
				replaced("input.broker.inputs[0].redpanda.consumer_group", "consumer group", "a", "dbg-0", 4),
				replaced("input.broker.inputs[1].redpanda.consumer_group", "consumer group", "b", "dbg-1", 5),
			},
		},
		"sequence": {
			src:     broker("sequence"),
			capture: []string{"consumer_group: dbg-0", "consumer_group: dbg-1"}, absent: []string{"consumer_group: a"},
			classes: []InputClass{ReplaySafe, ReplaySafe, ReplaySafe},
			warnings: []string{
				replaced("input.sequence.inputs[0].redpanda.consumer_group", "consumer group", "a", "dbg-0", 4),
				replaced("input.sequence.inputs[1].redpanda.consumer_group", "consumer group", "b", "dbg-1", 5),
			},
		},
		"read_until and batched": {
			src: "input:\n  read_until:\n    check: 'true'\n    input:\n      batched:\n        policy: {count: 1}\n        child:\n" +
				"          redpanda: " + fmt.Sprintf(rp, "a") + "\n",
			capture: []string{"consumer_group: dbg-0"}, absent: []string{"consumer_group: a"},
			classes: []InputClass{ReplaySafe, ReplaySafe, ReplaySafe},
			warnings: []string{
				replaced("input.read_until.input.batched.child.redpanda.consumer_group", "consumer group", "a", "dbg-0", 8),
			},
		},
		"resource": {
			src:       "input:\n  resource: r\n" + franzRes,
			resources: []string{"consumer_group: dbg-0"}, absent: []string{"consumer_group: prod"},
			classes:  []InputClass{ReplaySafe, ReplaySafe},
			warnings: []string{replaced("input_resources.r.kafka_franz.consumer_group", "consumer group", "prod", "dbg-0", 5)},
		},
		"resource named twice": {
			src:       "input:\n  broker:\n    inputs:\n      - resource: r\n      - redpanda: " + fmt.Sprintf(rp, "a") + "\n      - resource: r\n" + franzRes,
			capture:   []string{"consumer_group: dbg-1"},
			resources: []string{"consumer_group: dbg-0"}, absent: []string{"consumer_group: prod", "dbg-2"},
			classes: []InputClass{ReplaySafe, ReplaySafe, ReplaySafe, ReplaySafe, ReplaySafe},
			warnings: []string{
				replaced("input_resources.r.kafka_franz.consumer_group", "consumer group", "prod", "dbg-0", 9),
				replaced("input.broker.inputs[1].redpanda.consumer_group", "consumer group", "a", "dbg-1", 5),
			},
		},
		"legacy kafka": {
			src:     "input:\n  kafka: {addresses: [k:9092], topics: [t], consumer_group: prod}\n",
			capture: []string{"consumer_group: dbg-0"}, absent: []string{"prod"},
			classes:  []InputClass{ReplaySafe},
			warnings: []string{replaced("input.kafka.consumer_group", "consumer group", "prod", "dbg-0", 2)},
		},
		"nats queue": {
			src:     "input:\n  nats: {urls: [nats://n], subject: s, queue: workers}\n",
			capture: []string{"queue: dbg-0"}, absent: []string{"workers"},
			classes:  []InputClass{ReplaySafe},
			warnings: []string{replaced("input.nats.queue", "queue group", "workers", "dbg-0", 2)},
		},
		"interpolated": {
			src:     "input:\n  redpanda: " + fmt.Sprintf(rp, "'${GROUP}'") + "\n",
			capture: []string{"${GROUP}"}, absent: []string{"dbg"},
			classes: []InputClass{SideEffecting}, effect: "joins the consumer group in consumer_group, which the debugger can't override, and its members rebalance",
		},
		"keep": {
			src: "input:\n  redpanda: " + fmt.Sprintf(rp, "prod") + "\n", keep: true,
			capture: []string{"consumer_group: prod"}, absent: []string{"dbg"},
			classes: []InputClass{SideEffecting}, effect: `joins consumer group "prod" (consumer_group), and its members rebalance`,
		},
		"keep nested resource": {
			src: "input:\n  broker:\n    inputs:\n      - resource: r\n" + franzRes, keep: true,
			resources: []string{"consumer_group: prod"}, absent: []string{"dbg"},
			classes: []InputClass{SideEffecting, SideEffecting, SideEffecting}, effect: `joins consumer group "prod"`,
		},
		"left alone": {
			src:     "input:\n  postgres_cdc: {dsn: postgres://p, slot_name: s, tables: [t]}\n",
			capture: []string{"slot_name: s"}, absent: []string{"dbg"},
			classes: []InputClass{SideEffecting}, effect: "replication slot",
		},
		"left alone pubsub": {
			src:     "input:\n  gcp_pubsub: {project: p, subscription: s}\n",
			capture: []string{"subscription: s"}, absent: []string{"dbg"},
			classes: []InputClass{SideEffecting}, effect: "subscription",
		},
		"latest": {
			src:      "input:\n  redpanda: {seed_brokers: [k:9092], topics: [t], start_offset: latest}\n",
			capture:  []string{"start_offset: latest"},
			classes:  []InputClass{ReplaySafe},
			warnings: []string{`input.redpanda.start_offset (line 2): start_offset: latest starts at the latest offset, so the capture waits for new records and the "Waiting for the input" note may follow`},
		},
		"oldest false": {
			src:      "input:\n  kafka: {addresses: [k:9092], topics: [t], start_from_oldest: false}\n",
			classes:  []InputClass{ReplaySafe},
			warnings: []string{`input.kafka.start_from_oldest (line 2): start_from_oldest: false starts at the latest offset, so the capture waits for new records and the "Waiting for the input" note may follow`},
		},
		"latest once": {
			src:      "input:\n  kafka_franz: {seed_brokers: [k:9092], topics: [t], start_offset: latest, start_from_oldest: false}\n",
			classes:  []InputClass{ReplaySafe},
			warnings: []string{`input.kafka_franz.start_offset (line 2): start_offset: latest starts at the latest offset, so the capture waits for new records and the "Waiting for the input" note may follow`},
		},
		"committed, oldest false": {
			src:     "input:\n  kafka_franz: {seed_brokers: [k:9092], topics: [t], start_offset: committed, start_from_oldest: false}\n",
			classes: []InputClass{SideEffecting}, effect: "start_offset: committed",
		},
		"latest kept": {
			src:  "input:\n  redpanda: {seed_brokers: [k:9092], topics: [t], consumer_group: prod, start_offset: latest}\n",
			keep: true, classes: []InputClass{SideEffecting}, effect: `joins consumer group "prod" (consumer_group), and its members rebalance`,
			warnings: []string{`input.redpanda.start_offset (line 2): start_offset: latest starts at the latest offset unless the kept group has committed offsets, so the capture may wait for new records and the "Waiting for the input" note may follow`},
		},
		"nats keep": {
			src: "input:\n  nats: {urls: [nats://n], subject: s, queue: workers}\n", keep: true,
			capture: []string{"queue: workers"}, absent: []string{"dbg"},
			classes: []InputClass{SideEffecting}, effect: `joins queue group "workers" and takes its share of the messages, which aren't redelivered after the nack`,
		},
		"nats interpolated": {
			src:     "input:\n  nats: {urls: [nats://n], subject: s, queue: '${Q}'}\n",
			classes: []InputClass{SideEffecting}, effect: "joins the queue group in queue, which the debugger can't override, and takes its share of the messages, which aren't redelivered after the nack",
		},
		"earliest": {
			src:     "input:\n  redpanda: {seed_brokers: [k:9092], topics: [t], start_offset: earliest, start_from_oldest: true}\n",
			classes: []InputClass{ReplaySafe},
		},
	} {
		t.Run(name, func(t *testing.T) {
			p, err := Rewrite([]byte(c.src), Options{DebugGroup: "dbg", KeepConsumerGroup: c.keep})
			if err != nil {
				t.Fatal(err)
			}
			for _, s := range c.capture {
				if !strings.Contains(p.Input, s) {
					t.Errorf("capture config has no %q:\n%s", s, p.Input)
				}
			}
			for _, s := range c.resources {
				if !strings.Contains(p.Resources, s) {
					t.Errorf("resources have no %q:\n%s", s, p.Resources)
				}
			}
			for _, s := range c.absent {
				if strings.Contains(p.Input, s) || strings.Contains(p.Resources, s) {
					t.Errorf("%q is still there:\n%s\n%s", s, p.Input, p.Resources)
				}
			}
			var classes []InputClass
			effect := ""
			for _, f := range p.Inputs {
				classes = append(classes, f.Class)
				if effect == "" && f.Class == SideEffecting {
					effect = f.Effect
				}
			}
			if !slices.Equal(classes, c.classes) {
				t.Errorf("classes %v, want %v (findings %+v)", classes, c.classes, p.Inputs)
			}
			if !strings.Contains(effect, c.effect) {
				t.Errorf("effect %q, want %q", effect, c.effect)
			}
			if !slices.Equal(p.Warnings, c.warnings) {
				t.Errorf("warnings:\n%q\nwant:\n%q", p.Warnings, c.warnings)
			}
		})
	}
}

// inputClasses replaces a component's own class before the rollup, and a
// finding records when it loosened the class (E3 entry 4).
func TestInputClassesOverride(t *testing.T) {
	type want struct {
		path     string
		class    InputClass
		override InputClass
		loosened bool
		effect   string // a substring; "" means empty
	}
	cases := map[string]struct {
		src       string
		overrides map[string]InputClass
		want      []want
	}{
		"loosen": {
			src:       "input:\n  aws_sqs: {url: http://q}\n",
			overrides: map[string]InputClass{"aws_sqs": ReplaySafe},
			want:      []want{{"input", ReplaySafe, ReplaySafe, true, ""}},
		},
		"tighten": {
			src:       "input:\n  generate: {mapping: 'root = 1'}\n",
			overrides: map[string]InputClass{"generate": Rejected},
			want:      []want{{"input", Rejected, Rejected, false, "inputClasses marks generate rejected"}},
		},
		"same class keeps the effect": {
			src:       "input:\n  aws_sqs: {url: http://q}\n",
			overrides: map[string]InputClass{"aws_sqs": SideEffecting},
			want:      []want{{"input", SideEffecting, SideEffecting, false, "receive count"}},
		},
		"rejected loosened to side-effecting": {
			src:       "input:\n  http_server: {}\n",
			overrides: map[string]InputClass{"http_server": SideEffecting},
			want:      []want{{"input", SideEffecting, SideEffecting, true, "waits for clients (inputClasses marks http_server side-effecting)"}},
		},
		"tightened keeps the effect": {
			src:       "input:\n  aws_sqs: {url: http://q}\n",
			overrides: map[string]InputClass{"aws_sqs": Rejected},
			want:      []want{{"input", Rejected, Rejected, false, "receive count rises"}},
		},
		"stdin stays rejected": {
			src:       "input:\n  stdin: {}\n",
			overrides: map[string]InputClass{"stdin": ReplaySafe},
			want:      []want{{"input", Rejected, "", false, "stdin carries the debugger's DAP stream"}},
		},
		"unknown loosened": {
			src:       "input:\n  no_such_input: {}\n",
			overrides: map[string]InputClass{"no_such_input": ReplaySafe},
			want:      []want{{"input", ReplaySafe, ReplaySafe, true, ""}},
		},
		"composite follows": {
			src:       "input:\n  broker:\n    inputs:\n      - generate: {mapping: 'root = 1'}\n      - aws_sqs: {url: http://q}\n",
			overrides: map[string]InputClass{"aws_sqs": ReplaySafe},
			want: []want{
				{"input", ReplaySafe, "", false, ""},
				{"input.broker.inputs[0]", ReplaySafe, "", false, ""},
				{"input.broker.inputs[1]", ReplaySafe, ReplaySafe, true, ""},
			},
		},
		"composite override keeps its children": {
			src:       "input:\n  broker:\n    inputs:\n      - aws_sqs: {url: http://q}\n",
			overrides: map[string]InputClass{"broker": ReplaySafe},
			want: []want{
				{"input", SideEffecting, ReplaySafe, false, "through input.broker.inputs[0]"},
				{"input.broker.inputs[0]", SideEffecting, "", false, "receive count"},
			},
		},
		"resource follows": {
			src:       "input:\n  resource: q\ninput_resources:\n  - label: q\n    aws_sqs: {url: http://q}\n",
			overrides: map[string]InputClass{"aws_sqs": ReplaySafe},
			want: []want{
				{"input", ReplaySafe, "", false, ""},
				{"input_resources.q", ReplaySafe, ReplaySafe, true, ""},
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			p, err := Rewrite([]byte(tc.src), Options{InputClasses: tc.overrides})
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Inputs) != len(tc.want) {
				t.Fatalf("findings %+v, want %d", p.Inputs, len(tc.want))
			}
			for i, w := range tc.want {
				f := p.Inputs[i]
				if f.UserPath != w.path || f.Class != w.class || f.Override != w.override || f.Loosened != w.loosened ||
					(w.effect == "") != (f.Effect == "") || !strings.Contains(f.Effect, w.effect) {
					t.Errorf("finding %d = %+v, want %+v", i, f, w)
				}
			}
		})
	}
}

// A composite's finding records that its class is a child's, so the
// Session names the child.
func TestInheritedFindings(t *testing.T) {
	src := "input:\n  broker:\n    inputs:\n      - aws_sqs: {url: http://q}\n      - resource: q\n" +
		"input_resources:\n  - label: q\n    redis_list: {url: redis://r, key: k}\n"
	p, err := Rewrite([]byte(src), Options{})
	if err != nil {
		t.Fatal(err)
	}
	got := map[string]bool{}
	for _, f := range p.Inputs {
		got[f.UserPath] = f.Inherited
	}
	want := map[string]bool{"input": true, "input.broker.inputs[0]": false, "input.broker.inputs[1]": true, "input_resources.q": false}
	if !maps.Equal(got, want) {
		t.Errorf("inherited = %v, want %v", got, want)
	}
}

// The rewrite's fixed rows are core.FixedInputs, with the same effect.
func TestFixedInputs(t *testing.T) {
	for name, why := range core.FixedInputs {
		row, ok := inputTable[name]
		if !ok || row.class == nil {
			t.Errorf("%s: no fixed row", name)
			continue
		}
		if c, e := row.class(nil); c != Rejected || e != why {
			t.Errorf("%s: row %s %q, want rejected %q", name, c, e, why)
		}
	}
}

// Every top-level section of the stock config schema has a decision in
// topLevel, or ends in _resources (carried), so a section Connect adds
// fails here until the rewrite decides what to do with it (AD-5).
func TestTopLevelAllowlist(t *testing.T) {
	td, err := schema.Standard("x", "x").TemplateData()
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, f := range td.Fields {
		name := f.FullName
		if strings.Contains(name, ".") {
			continue
		}
		seen++
		if _, ok := topLevel[name]; !ok && !strings.HasSuffix(name, "_resources") {
			t.Errorf("top-level section %q has no decision in topLevel", name)
		}
	}
	if seen == 0 {
		t.Fatal("no top-level sections in the stock schema")
	}
	// A dropped section's content reaches neither stream.
	for name, sec := range topLevel {
		if sec.use != dropped {
			continue
		}
		src := name + ":\n  marker_" + name + ": 1\ninput:\n  generate: {mapping: 'root = 1'}\n"
		p, err := Rewrite([]byte(src), Options{})
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if strings.Contains(p.Input+p.Resources+strings.Join(p.Steppers, ""), "marker_"+name) {
			t.Errorf("dropped section %s reached the engine", name)
		}
	}
}

// The top-level redpanda block is dropped with a warning: only rpk connect
// sets up its shared client and topics (E3 entry 6).
func TestRedpandaBlockDropped(t *testing.T) {
	src := "redpanda:\n  seed_brokers: [k:9092]\n  logs_topic: __logs\ninput:\n  generate: {mapping: 'root = 1'}\n"
	p, err := Rewrite([]byte(src), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(p.Input+p.Resources+strings.Join(p.Steppers, ""), "__logs") {
		t.Error("the redpanda block reached the engine")
	}
	want := "redpanda (line 1): the top-level redpanda block isn't used in a debug session"
	if !slices.ContainsFunc(p.Warnings, func(w string) bool { return strings.HasPrefix(w, want) }) {
		t.Errorf("warnings %q, want one starting %q", p.Warnings, want)
	}
}

// A redpanda input without seed_brokers can't run (it needs the shared
// client): its group isn't overridden and inputClasses can't loosen it.
func TestSeedlessRedpandaStaysRejected(t *testing.T) {
	src := "input:\n  broker:\n    inputs:\n      - redpanda: {topics: [t], consumer_group: a}\n      - kafka_franz: {seed_brokers: [k:9092], topics: [t], consumer_group: b}\n"
	p, err := Rewrite([]byte(src), Options{DebugGroup: "dbg", InputClasses: map[string]InputClass{"redpanda": ReplaySafe}})
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(p.Input, "consumer_group: a") || !strings.Contains(p.Input, "consumer_group: dbg-0") {
		t.Errorf("capture config: the seedless redpanda keeps a and kafka_franz gets dbg-0:\n%s", p.Input)
	}
	for _, f := range p.Inputs {
		if f.Component == "redpanda" && f.Class != Rejected {
			t.Errorf("seedless redpanda finding %+v, want rejected", f)
		}
	}
}
