package rewrite

import (
	"slices"
	"strings"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/schema"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// effectResources are the resources every effect case can name.
const effectResources = `
cache_resources:
  - {label: mem, memory: {}}
  - {label: red, redis: {url: "redis://u:pw@cache:6379"}}
  - {label: fil, file: {directory: /tmp/c}}
  - {label: mlocal, multilevel: [mem, mem2]}
  - {label: mem2, lru: {}}
  - {label: mremote, multilevel: [mem, red]}
rate_limit_resources:
  - {label: loc, local: {count: 1, interval: 1s}}
  - {label: rrl, redis: {url: "redis://rl:6379", count: 1, interval: 1s}}
`

// effectCase is one processor config and the warning text it gives, or ""
// for none.
type effectCase struct {
	conf string
	want string
}

// effectCases has at least one case per effect-table row, and a case on
// each side of every predicate that can clear a processor.
var effectCases = map[string][]effectCase{
	"http": {
		{`{url: http://x}`, `sends POST requests to "http://x"`},
		{`{url: http://x, verb: PUT}`, `sends PUT requests`},
		{`{url: http://x, verb: get}`, ``},
		{`{url: http://x, verb: HEAD}`, ``},
		{`{url: http://x, verb: OPTIONS}`, `sends OPTIONS requests`},
		{`{url: http://x, verb: '${! meta("v") }'}`, `sends ${! meta("v") } requests`},
	},
	"sql_insert": {{`{driver: postgres, dsn: "postgres://u:pw@db/app", table: t, columns: [a], args_mapping: 'root = [1]'}`, `inserts rows into table "t" on the postgres database at "postgres://db/app"`}},
	"sql_raw": {
		{`{driver: mysql, dsn: "u:pw@tcp(db:3306)/app", query: "SELECT 1"}`, `runs SQL statements against the mysql database at "tcp(db:3306)/app"`},
	},
	"sql": {{`{driver: postgres, data_source_name: "host=db password=pw", query: "SELECT 1"}`, `at "host=db password=xxxxx"`}},
	"aws_dynamodb_partiql": {
		{`{query: "INSERT INTO t VALUE {'a': ?}", region: eu-west-1}`, `runs PartiQL INSERT statements against DynamoDB in "eu-west-1"`},
		{`{query: "SELECT * FROM t"}`, ``},
		{`{query: "SELECT * FROM t", unsafe_dynamic_query: true}`, `runs PartiQL statements against DynamoDB`},
	},
	"mongodb": {
		{`{url: "mongodb://m", database: d, collection: c}`, `runs insert-one on collection "c" of database "d" at "mongodb://m"`},
		{`{url: "mongodb://m", database: d, collection: c, operation: find-one}`, ``},
	},
	"couchbase": {
		{`{url: "couchbase://cb", bucket: b, operation: upsert}`, `runs upsert on documents in bucket "b" at "couchbase://cb"`},
		{`{url: "couchbase://cb", bucket: b}`, ``},
	},
	"azure_cosmosdb": {
		{`{endpoint: "https://acct.documents.azure.com", account_key: secret, database: d, container: c}`, `runs Create on items in container "c" of Azure Cosmos DB database "d" at "https://acct.documents.azure.com"`},
		{`{connection_string: "AccountKey=secret", database: d, container: c, operation: Read}`, ``},
	},
	"redis": {
		{`{url: "redis://r", command: set}`, `runs set against Redis at "redis://r"`},
		{`{url: "redis://r", command: GET}`, ``},
		{`{url: "redis://r", command: '${! meta("c") }'}`, `runs ${! meta("c") } against Redis`},
		{`{url: "redis://r", operator: incrby}`, `runs incrby against Redis`},
		{`{url: "redis://r", operator: scard}`, ``},
	},
	"redis_script":       {{`{url: "redis://r", script: "return 1"}`, `runs a Lua script against Redis at "redis://r"`}},
	"nats_kv":            {{`{urls: [nats://a, nats://b], bucket: b, key: k, operation: put}`, `runs put on key "k" in the NATS KV bucket "b" at "nats://a", "nats://b"`}, {`{urls: [nats://a], bucket: b, key: k, operation: get}`, ``}},
	"nats_request_reply": {{`{urls: [nats://a], subject: s}`, `publishes requests to the NATS subject "s" at "nats://a"`}},
	"aws_lambda":         {{`{function: f}`, `invokes the AWS Lambda function "f"`}},
	"sentry_capture":     {{`{dsn: "https://key@o1.ingest.sentry.io/1", message: m}`, `sends events to Sentry at "https://o1.ingest.sentry.io/1"`}},
	"command":            {{`{name: touch}`, `runs the command "touch"`}},
	"subprocess":         {{`{name: cat}`, `runs the subprocess "cat"`}},
	"cache": {
		{`{resource: red, operator: set, key: k, value: v}`, `sets keys in the redis cache "red"`},
		{`{resource: red, operator: delete, key: k}`, `deletes keys in the redis cache "red"`},
		{`{resource: fil, operator: add, key: k, value: v}`, `adds keys in the file cache "fil"`},
		{`{resource: mremote, operator: set, key: k, value: v}`, `sets keys in the multilevel cache "mremote"`},
		{`{resource: mlocal, operator: set, key: k, value: v}`, ``},
		{`{resource: mem, operator: set, key: k, value: v}`, ``},
		{`{resource: red, operator: get, key: k}`, ``},
		{`{resource: red, operator: '${! meta("o") }', key: k}`, `writes keys in the redis cache "red"`},
		{`{resource: '${! meta("c") }', operator: set, key: k, value: v}`, `cache named by`},
	},
	"cached": {
		{`{cache: red, key: k, processors: [{mapping: 'root = 1'}]}`, `stores processor results in the redis cache "red"`},
		{`{cache: mem, key: k, processors: [{mapping: 'root = 1'}]}`, ``},
	},
	"dedupe": {
		{`{cache: red, key: k}`, `adds dedupe keys to the redis cache "red"`},
		{`{cache: mem2, key: k}`, ``},
	},
	"schema_registry_encode": {
		{`{url: "http://sr", subject: s, schema_metadata: m}`, `registers schemas under subject "s" at the schema registry "http://sr"`},
		{`{url: "http://sr", subject: s}`, ``},
	},
	"rate_limit": {
		{`{resource: rrl}`, `consumes quota from the redis rate limit "rrl" at "redis://rl:6379"`},
		{`{resource: loc}`, ``},
	},
	"javascript": {
		{`{code: 'benthos.v0_fetch("http://y", {}, "GET", "")'}`, `may send HTTP requests from JavaScript (v0_fetch)`},
		{`{code: '(() => 1)()'}`, ``},
	},
	"awk": {
		{`{codec: none, program: '{ system("rm x") }'}`, `may run commands from awk (system)`},
		{`{codec: none, program: '{ print $0 }'}`, ``},
	},
	"ollama_chat":       {{`{model: llama3}`, `downloads and starts Ollama at startup, and pulls the model "llama3" into it`}},
	"ollama_embeddings": {{`{model: m, server_address: "http://o:11434"}`, `pulls the model "m" into the Ollama server at "http://o:11434" at startup`}},
	"ollama_moderation": {{`{model: m, server_address: "http://o:11434"}`, `pulls the model "m"`}},
}

// effectWarnings returns the effect and state-split warnings of src.
func effectWarnings(t *testing.T, src string, opts Options) []string {
	t.Helper()
	p, err := Rewrite([]byte(src), opts)
	if err != nil {
		t.Fatalf("%v\n%s", err, src)
	}
	var out []string
	for _, w := range p.Warnings {
		if strings.Contains(w, "this processor ") && !strings.Contains(w, "this processor list") || strings.Contains(w, "isn't shared between capture and stepping") {
			out = append(out, w)
		}
	}
	return out
}

func TestEffectRows(t *testing.T) {
	for name := range effects {
		if len(effectCases[name]) == 0 {
			t.Errorf("effect row %s has no test case", name)
		}
	}
	for name, cases := range effectCases {
		if _, ok := effects[name]; !ok {
			t.Errorf("test cases for %s, which has no row", name)
		}
		for _, c := range cases {
			src := "input:\n  generate: {mapping: 'root = 1'}\npipeline:\n  processors:\n    - " + name + ": " + c.conf + "\n" + effectResources
			got := effectWarnings(t, src, Options{})
			switch {
			case c.want == "" && len(got) != 0:
				t.Errorf("%s %s: warnings %q, want none", name, c.conf, got)
			case c.want != "" && (len(got) != 1 || !strings.Contains(got[0], c.want)):
				t.Errorf("%s %s: warnings %q, want one containing %q", name, c.conf, got, c.want)
			case c.want != "" && !strings.HasPrefix(got[0], "pipeline.processors[0]."+name+" (line 5): this processor "):
				t.Errorf("%s %s: warning %q is not at the processor", name, c.conf, got[0])
			}
			for _, g := range got {
				if strings.Contains(g, "pw") || strings.Contains(g, "secret") || strings.Contains(g, "key@") {
					t.Errorf("%s %s: warning %q shows a credential", name, c.conf, g)
				}
			}
		}
	}
}

// The effect table, the in-process cache kinds and the local rate limit
// kinds name stock components.
func TestEffectTableIsStock(t *testing.T) {
	env := schema.Standard("x", "x").Environment()
	var procs, caches, rls []string
	env.WalkProcessors(func(n string, _ *service.ConfigView) { procs = append(procs, n) })
	env.WalkCaches(func(n string, _ *service.ConfigView) { caches = append(caches, n) })
	env.WalkRateLimits(func(n string, _ *service.ConfigView) { rls = append(rls, n) })
	for n := range effects {
		if !slices.Contains(procs, n) {
			t.Errorf("effect row %s is not a stock processor", n)
		}
	}
	for n := range inProcessCaches {
		if !slices.Contains(caches, n) {
			t.Errorf("in-process cache %s is not a stock cache", n)
		}
	}
	for n := range localRateLimits {
		if !slices.Contains(rls, n) {
			t.Errorf("local rate limit %s is not a stock rate limit", n)
		}
	}
}

// Where a processor sits decides the marking, and nested lists of every
// kind are searched: opaque parents, AD-7 parents, tools.
func TestEffectPlaces(t *testing.T) {
	const post = "http: {url: http://x}"
	for name, c := range map[string]struct {
		src     string
		want    []string
		options Options
	}{
		"workflow branch": {
			src:  "input:\n  generate: {mapping: 'root = 1'}\npipeline:\n  processors:\n    - workflow:\n        branches:\n          a:\n            processors:\n              - " + post + "\n",
			want: []string{"pipeline.processors[0].workflow.branches.a.processors[0].http (line 9): this processor sends POST requests to \"http://x\": processors run for real in a debug session"},
		},
		"switch case and try": {
			src:  "input:\n  generate: {mapping: 'root = 1'}\npipeline:\n  processors:\n    - switch:\n        - check: 'true'\n          processors:\n            - try:\n                - " + post + "\n",
			want: []string{"pipeline.processors[0].switch[0].processors[0].try[0].http (line 9): this processor sends POST requests"},
		},
		"input processors are stepped": {
			src:  "input:\n  generate: {mapping: 'root = 1'}\n  processors:\n    - " + post + "\n",
			want: []string{"input.processors[0].http (line 4): this processor sends POST requests to \"http://x\": processors run for real"},
		},
		"resource through a resource from the input tree": {
			src: "input:\n  broker:\n    inputs:\n      - generate: {mapping: 'root = 1'}\n        processors:\n          - resource: a\n" +
				"processor_resources:\n  - label: a\n    resource: b\n  - label: b\n    " + post + "\n",
			want: []string{"processor_resources.b.http (line 11): this processor sends POST requests to \"http://x\": it runs during capture, not stepped"},
		},
		"unreferenced resource": {
			src: "input:\n  generate: {mapping: 'root = 1'}\nprocessor_resources:\n  - label: a\n    " + post + "\n",
		},
		"resource only from a nested output list in dry-run": {
			src: "input:\n  generate: {mapping: 'root = 1'}\noutput:\n  broker:\n    outputs:\n      - drop: {}\n        processors:\n          - resource: q\n" +
				"processor_resources:\n  - label: q\n    " + post + "\n",
		},
		"resource only from a nested output list under real": {
			src: "input:\n  generate: {mapping: 'root = 1'}\noutput:\n  broker:\n    outputs:\n      - drop: {}\n        processors:\n          - resource: q\n" +
				"processor_resources:\n  - label: q\n    " + post + "\n",
			options: Options{Output: core.OutputReal},
			want:    []string{"processor_resources.q.http (line 11): this processor sends POST requests to \"http://x\": processors run for real"},
		},
		"split through a multilevel cache": {
			src: "input:\n  broker:\n    inputs:\n      - generate: {mapping: 'root = 1'}\n        processors:\n          - cache: {resource: ml, operator: set, key: k, value: v}\n" +
				"pipeline:\n  processors:\n    - cache: {resource: ml, operator: get, key: k}\n" +
				"cache_resources:\n  - label: ml\n    multilevel: [a, b]\n  - label: a\n    memory: {}\n  - label: b\n    lru: {}\n",
			want: []string{
				"cache_resources.a (line 13): the memory cache \"a\" is used both during capture and during stepping",
				"cache_resources.b (line 15): the lru cache \"b\" is used both during capture and during stepping",
			},
		},
		"nested output list in dry-run": {
			src: "input:\n  generate: {mapping: 'root = 1'}\noutput:\n  broker:\n    outputs:\n      - drop: {}\n        processors:\n          - " + post + "\n",
		},
		"nested output list under real": {
			src:     "input:\n  generate: {mapping: 'root = 1'}\noutput:\n  broker:\n    outputs:\n      - drop: {}\n        processors:\n          - " + post + "\n",
			options: Options{Output: core.OutputReal},
			want:    []string{"output.broker.outputs[0].processors[0].http (line 8): this processor sends POST requests"},
		},
		"rate limit split through an input field": {
			src: "input:\n  http_client: {url: http://src, verb: GET, rate_limit: rl}\npipeline:\n  processors:\n    - rate_limit: {resource: rl}\n" +
				"rate_limit_resources:\n  - label: rl\n    local: {count: 1, interval: 1s}\n",
			want: []string{"rate_limit_resources.rl (line 7): the local rate limit \"rl\" is used both during capture and during stepping"},
		},
		"split through a processor resource": {
			src: "input:\n  broker:\n    inputs:\n      - generate: {mapping: 'root = 1'}\n        processors:\n          - resource: p\n" +
				"pipeline:\n  processors:\n    - resource: p\n" +
				"processor_resources:\n  - label: p\n    dedupe: {cache: c, key: k}\ncache_resources:\n  - label: c\n    memory: {}\n",
			want: []string{"cache_resources.c (line 14): the memory cache \"c\" is used both during capture and during stepping"},
		},
		"no split for a remote cache": {
			src: "input:\n  broker:\n    inputs:\n      - generate: {mapping: 'root = 1'}\n        processors:\n          - cache: {resource: c, operator: get, key: k}\n" +
				"pipeline:\n  processors:\n    - cache: {resource: c, operator: get, key: k}\n" +
				"cache_resources:\n  - label: c\n    redis: {url: redis://r}\n",
		},
	} {
		t.Run(name, func(t *testing.T) {
			got := effectWarnings(t, c.src, c.options)
			if len(got) != len(c.want) {
				t.Fatalf("warnings %q, want %d", got, len(c.want))
			}
			for i, w := range c.want {
				if !strings.HasPrefix(got[i], w) {
					t.Errorf("warning %d = %q, want prefix %q", i, got[i], w)
				}
			}
		})
	}
}

func TestRedact(t *testing.T) {
	for in, want := range map[string]string{
		"postgres://u:pw@db:5432/app?sslmode=disable": "postgres://db:5432/app?sslmode=disable",
		"u:pw@tcp(db:3306)/app":                       "tcp(db:3306)/app",
		"host=db user=u password=pw dbname=app":       "host=db user=u password=xxxxx dbname=app",
		"sqlserver://db?database=a&password=pw":       "sqlserver://db?database=a&password=xxxxx",
		"http://x/path":                               "http://x/path",
		"http://api/users/${! @id }":                  "http://api/users/${! @id }",
		"http://api/u?email=a@b.com":                  "http://api/u?email=a@b.com",
		"http://u:p@api/u?email=a@b.com#x@y":          "http://api/u?email=a@b.com#x@y",
		"host=db sslpassword=pw user=u":               "host=db sslpassword=xxxxx user=u",
		"host=db Pwd=pw":                              "host=db Pwd=xxxxx",
		"host=db password='a b' user=u":               "host=db password=xxxxx user=u",
		`host=db password="a b" user=u`:               "host=db password=xxxxx user=u",
	} {
		if got := redact(in); got != want {
			t.Errorf("redact(%q) = %q, want %q", in, got, want)
		}
	}
}
