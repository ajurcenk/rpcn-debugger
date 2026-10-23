package rewrite

import (
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
	// group is the DebugGroup option.
	group string
	want  InputClass
}

// inputCases covers every row of the input table. A field-dependent row
// (fieldDependent) has a case on each side of its predicate.
var inputCases = map[string][]inputCase{
	"kafka": {
		{conf: "{addresses: [k:9092], topics: [t]}", want: ReplaySafe},
		{conf: "{addresses: [k:9092], topics: [t], consumer_group: prod}", group: "dbg", want: SideEffecting},
		{conf: "{addresses: [k:9092], topics: [t], consumer_group: '${G}'}", want: SideEffecting},
	},
	"kafka_franz": {
		{conf: "{seed_brokers: [k:9092], topics: [t]}", want: ReplaySafe},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: prod}", group: "dbg", want: ReplaySafe},
		{conf: "{seed_brokers: [k:9092], topics: [t], consumer_group: prod}", want: SideEffecting},
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
	},
	"redpanda_common": {
		{conf: "{topics: [t]}", want: ReplaySafe},
		// Not overridden until E3 entry 5.
		{conf: "{topics: [t], consumer_group: prod}", group: "dbg", want: SideEffecting},
		{conf: "{topics: [t], start_offset: committed}", want: SideEffecting},
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
	"kafka", "kafka_franz", "redpanda", "redpanda_common", "mqtt", "nats", "nanomsg",
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
			p, err := Rewrite([]byte(src), Options{DebugGroup: c.group})
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
				t.Errorf("%s %s (group %q): finding %+v, want class %s", name, c.conf, c.group, f, c.want)
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

// Only the top-level redpanda/kafka_franz group is overridden (E3 entry 2);
// a nested one is kept, so the input is side-effecting (entry 5 overrides
// it).
func TestNestedGroupKept(t *testing.T) {
	src := "input:\n  broker:\n    inputs:\n      - redpanda: {seed_brokers: [k:9092], topics: [t], consumer_group: prod}\n"
	p, err := Rewrite([]byte(src), Options{DebugGroup: "dbg"})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(p.Input, "dbg") {
		t.Errorf("nested group overridden:\n%s", p.Input)
	}
	if len(p.Inputs) != 2 || p.Inputs[0].Class != SideEffecting || p.Inputs[1].Class != SideEffecting ||
		!strings.Contains(p.Inputs[1].Effect, `"prod"`) {
		t.Errorf("findings %+v, want both side-effecting with the kept group", p.Inputs)
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
