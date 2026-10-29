package rewrite

import (
	"fmt"
	"regexp"
	"strings"

	"gopkg.in/yaml.v3"
)

// Launch warnings for side-effecting processors (AD-5, D7) and for
// in-process resources whose state is split between the capture and the
// step stream (AD-1, D9). They are warnings only: nothing is refused, and
// no config changes.
//
// Evidence: story-launch-warnings-side-effecting-processors-and-state-split-be-processor-effects.md
// (Connect v4.115.0, benthos v4.84.0). Paths there: C/ is
// connect/v4/internal/impl/, B/ is benthos/v4/internal/impl/, L/ is
// connect/v4/internal/llm/. The user's rules of 2026-10-29 override the
// research where they differ (sql_raw and sql always warn, http warns
// unless GET or HEAD, …).

// effectFunc returns what the processor with config conf writes to, as the
// end of "this processor …", and whether it writes at all. Only literal
// values clear a processor: an interpolated field that decides the
// predicate counts as the write case.
type effectFunc func(e *effectWalk, conf *yaml.Node) (string, bool)

// effects is the processor effect table (D7), keyed by processor name.
var effects = map[string]effectFunc{
	// B/io/processor_http.go:61: the verb defaults to POST.
	"http": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		if !strNotIn(c, "verb", "POST", "GET", "HEAD") {
			return "", false
		}
		return fmt.Sprintf("sends %s requests to %s", verbatim(c, "verb", "POST"), target(c, "url")), true
	},
	// C/sql/processor_sql_insert.go:271.
	"sql_insert": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return fmt.Sprintf("inserts rows into table %s on the %s database at %s", quoted(c, "table"), verbatim(c, "driver", ""), target(c, "dsn")), true
	},
	// C/sql/processor_sql_raw.go:324: any statement, a SELECT included,
	// may write (volatile functions, locks), so it always warns.
	"sql_raw": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return fmt.Sprintf("runs SQL statements against the %s database at %s", verbatim(c, "driver", ""), target(c, "dsn")), true
	},
	// C/sql/processor_sql_deprecated.go:33-41.
	"sql": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return fmt.Sprintf("runs SQL statements against the %s database at %s", verbatim(c, "driver", ""), target(c, "data_source_name")), true
	},
	// C/aws/dynamodb/processor_partiql.go:34-47: only a literal SELECT
	// reads; unsafe_dynamic_query makes the statement unknown.
	"aws_dynamodb_partiql": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		q := fieldAt(c, "query")
		word := ""
		if q != nil && q.Kind == yaml.ScalarNode && !isInterpolated(q) {
			if f := strings.Fields(q.Value); len(f) > 0 {
				word = strings.ToUpper(f[0])
			}
		}
		if word == "SELECT" && !boolIs(c, "unsafe_dynamic_query", true, false) {
			return "", false
		}
		where := "DynamoDB"
		if isSet(c, "region") {
			where += " in " + quoted(c, "region")
		}
		if word == "" || word == "SELECT" {
			return "runs PartiQL statements against " + where, true
		}
		return fmt.Sprintf("runs PartiQL %s statements against %s", word, where), true
	},
	// C/mongodb/processor.go:46-48: the operation defaults to insert-one.
	"mongodb": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		if !strNotIn(c, "operation", "insert-one", "find-one", "aggregate") {
			return "", false
		}
		return fmt.Sprintf("runs %s on collection %s of database %s at %s", verbatim(c, "operation", "insert-one"), quoted(c, "collection"), quoted(c, "database"), target(c, "url")), true
	},
	// C/couchbase/processor.go:49-55: the operation defaults to get.
	"couchbase": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		if !strNotIn(c, "operation", "get", "get") {
			return "", false
		}
		return fmt.Sprintf("runs %s on documents in bucket %s at %s", verbatim(c, "operation", "get"), quoted(c, "bucket"), target(c, "url")), true
	},
	// C/azure/cosmosdb/docs.go:50,58: the operation defaults to Create.
	// account_key and connection_string hold secrets, so only endpoint is
	// named.
	"azure_cosmosdb": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		if !strNotIn(c, "operation", "Create", "Read") {
			return "", false
		}
		at := ""
		if isSet(c, "endpoint") {
			at = " at " + target(c, "endpoint")
		}
		return fmt.Sprintf("runs %s on items in container %s of Azure Cosmos DB database %s%s", verbatim(c, "operation", "Create"), quoted(c, "container"), quoted(c, "database"), at), true
	},
	// C/redis/processor.go:42-60: any command that isn't a known read, and
	// the deprecated operators sadd and incrby.
	"redis": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		switch {
		case isSet(c, "command"):
			n := fieldAt(c, "command")
			if n.Kind == yaml.ScalarNode && !isInterpolated(n) && redisReads[strings.ToUpper(n.Value)] {
				return "", false
			}
			return fmt.Sprintf("runs %s against Redis at %s", verbatim(c, "command", ""), target(c, "url")), true
		case isSet(c, "operator"):
			if !strNotIn(c, "operator", "", "keys", "scard") {
				return "", false
			}
			return fmt.Sprintf("runs %s against Redis at %s", verbatim(c, "operator", ""), target(c, "url")), true
		}
		return "", false
	},
	// C/redis/script_processor.go:166: an arbitrary Lua script.
	"redis_script": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return "runs a Lua script against Redis at " + target(c, "url"), true
	},
	// C/nats/processor_kv.go:54-62.
	"nats_kv": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		if !strNotIn(c, "operation", "", "get", "get_revision", "history", "keys") {
			return "", false
		}
		return fmt.Sprintf("runs %s on key %s in the NATS KV bucket %s at %s", verbatim(c, "operation", ""), quoted(c, "key"), quoted(c, "bucket"), targets(c, "urls")), true
	},
	// C/nats/processor_request_reply.go:185: the responder may act on it.
	"nats_request_reply": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return fmt.Sprintf("publishes requests to the NATS subject %s at %s", quoted(c, "subject"), targets(c, "urls")), true
	},
	// C/aws/lambda/processor.go:302.
	"aws_lambda": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return "invokes the AWS Lambda function " + quoted(c, "function"), true
	},
	// C/sentry/processor_capture.go:294.
	"sentry_capture": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return "sends events to Sentry at " + target(c, "dsn"), true
	},
	// B/io/processor_command.go:153.
	"command": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return "runs the command " + quoted(c, "name"), true
	},
	// B/io/processor_subprocess.go:344.
	"subprocess": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		return "runs the subprocess " + quoted(c, "name"), true
	},
	// B/pure/processor_cache.go:284-292: set, add and delete write; only a
	// remote or persistent cache counts.
	"cache": func(e *effectWalk, c *yaml.Node) (string, bool) {
		op := fieldAt(c, "operator")
		if isNull(op) || !strNotIn(c, "operator", "", "get", "exists") {
			return "", false
		}
		cache, ok := e.remoteCache(fieldAt(c, "resource"))
		if !ok {
			return "", false
		}
		verb := "writes"
		if op.Kind == yaml.ScalarNode && !isInterpolated(op) {
			switch strings.ToLower(op.Value) {
			case "set":
				verb = "sets"
			case "add":
				verb = "adds"
			case "delete":
				verb = "deletes"
			}
		}
		return verb + " keys in the " + cache, true
	},
	// B/pure/processor_cached.go:223: Set on every miss.
	"cached": func(e *effectWalk, c *yaml.Node) (string, bool) {
		cache, ok := e.remoteCache(fieldAt(c, "cache"))
		if !ok {
			return "", false
		}
		return "stores processor results in the " + cache, true
	},
	// B/pure/processor_dedupe.go:144: Add per message.
	"dedupe": func(e *effectWalk, c *yaml.Node) (string, bool) {
		cache, ok := e.remoteCache(fieldAt(c, "cache"))
		if !ok {
			return "", false
		}
		return "adds dedupe keys to the " + cache, true
	},
	// C/confluent/processor_schema_registry_encode.go:107-110,688: with
	// schema_metadata it registers schemas.
	"schema_registry_encode": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		if !isSet(c, "schema_metadata") {
			return "", false
		}
		return fmt.Sprintf("registers schemas under subject %s at the schema registry %s", quoted(c, "subject"), target(c, "url")), true
	},
	// C/redis/rate_limit.go:100-104: every access INCRs a shared key.
	"rate_limit": func(e *effectWalk, c *yaml.Node) (string, bool) {
		r := fieldAt(c, "resource")
		if r == nil || r.Kind != yaml.ScalarNode || isNull(r) {
			return "", false
		}
		if isInterpolated(r) {
			return fmt.Sprintf("consumes quota from the rate limit named by %q, which may be a redis rate limit", r.Value), true
		}
		entry := e.rateLimits[r.Value]
		if k, conf := component(entry); k != nil && k.Value == "redis" {
			return fmt.Sprintf("consumes quota from the redis rate limit %q at %s", r.Value, target(conf, "url")), true
		}
		return "", false
	},
	// C/javascript/functions.go:102-140: v0_fetch sends any method. The
	// file field is matched as written; the rewrite reads no script files.
	"javascript": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		for _, f := range []string{"code", "file"} {
			if n := fieldAt(c, f); n != nil && n.Kind == yaml.ScalarNode && strings.Contains(n.Value, "v0_fetch") {
				return "may send HTTP requests from JavaScript (v0_fetch)", true
			}
		}
		return "", false
	},
	// C/awk/processor.go:773-777: goawk runs without NoExec.
	"awk": func(_ *effectWalk, c *yaml.Node) (string, bool) {
		if n := fieldAt(c, "program"); n != nil && n.Kind == yaml.ScalarNode && strings.Contains(n.Value, "system(") {
			return "may run commands from awk (system)", true
		}
		return "", false
	},
	// C/ollama/base_processor.go:347-351: at startup it pulls the model,
	// and without server_address downloads and starts Ollama first.
	"ollama_chat":       ollama,
	"ollama_embeddings": ollama,
	"ollama_moderation": ollama,
}

func ollama(_ *effectWalk, c *yaml.Node) (string, bool) {
	if !isSet(c, "server_address") {
		return fmt.Sprintf("downloads and starts Ollama at startup, and pulls the model %s into it", quoted(c, "model")), true
	}
	return fmt.Sprintf("pulls the model %s into the Ollama server at %s at startup", quoted(c, "model"), target(c, "server_address")), true
}

// redisReads are the Redis commands the redis processor may run without a
// warning: they read and change nothing.
var redisReads = setOf(
	"GET", "MGET", "GETRANGE", "STRLEN", "EXISTS", "TYPE", "TTL", "PTTL", "EXPIRETIME", "PEXPIRETIME",
	"KEYS", "SCAN", "DBSIZE", "PING", "ECHO", "TIME", "INFO",
	"HGET", "HMGET", "HGETALL", "HKEYS", "HVALS", "HLEN", "HEXISTS", "HSTRLEN", "HSCAN", "HRANDFIELD",
	"LRANGE", "LLEN", "LINDEX", "LPOS",
	"SMEMBERS", "SCARD", "SISMEMBER", "SMISMEMBER", "SRANDMEMBER", "SSCAN", "SINTER", "SUNION", "SDIFF", "SINTERCARD",
	"ZRANGE", "ZRANGEBYSCORE", "ZRANGEBYLEX", "ZREVRANGE", "ZREVRANGEBYSCORE", "ZREVRANGEBYLEX", "ZSCORE", "ZMSCORE",
	"ZCARD", "ZCOUNT", "ZLEXCOUNT", "ZRANK", "ZREVRANK", "ZSCAN", "ZRANDMEMBER",
	"XRANGE", "XREVRANGE", "XLEN", "XREAD", "XINFO", "XPENDING",
	"GETBIT", "BITCOUNT", "BITPOS", "PFCOUNT", "GEOPOS", "GEODIST", "GEOHASH", "GEOSEARCH",
	"JSON.GET", "JSON.MGET", "JSON.TYPE", "JSON.STRLEN", "JSON.ARRLEN", "JSON.OBJKEYS", "JSON.OBJLEN",
)

// inProcessCaches are the cache kinds whose state lives in the debugger's
// process (research D): they write nothing outside it. Every other kind,
// file included (it writes files on the host), is remote or persistent.
var inProcessCaches = setOf("memory", "lru", "ttlru", "ristretto", "noop")

// localRateLimits are the rate limit kinds held in process (research E).
var localRateLimits = setOf("local")

// ownListProcessors are the processors whose config is itself a processor
// list (AD-7 try, catch, for_each, and the processors processor).
var ownListProcessors = setOf("try", "catch", "for_each", "processors")

func setOf(vals ...string) map[string]bool {
	m := make(map[string]bool, len(vals))
	for _, v := range vals {
		m[v] = true
	}
	return m
}

// effectWalk gives every side-effecting processor in the pipeline one
// launch warning.
type effectWalk struct {
	w *walker
	// caches and rateLimits map the cache_resources and
	// rate_limit_resources labels to their entries.
	caches, rateLimits map[string]*yaml.Node
}

// remoteCache returns how to name the cache resource named by ref, and
// whether it is remote or persistent: not an in-process kind, or a
// multilevel cache with any such level. An interpolated name is the remote
// case; an undefined one is left to the engine.
func (e *effectWalk) remoteCache(ref *yaml.Node) (string, bool) {
	if ref == nil || ref.Kind != yaml.ScalarNode || isNull(ref) {
		return "", false
	}
	if isInterpolated(ref) {
		return fmt.Sprintf("cache named by %q, which may be remote", ref.Value), true
	}
	k, _ := component(e.caches[ref.Value])
	if k == nil || !e.cacheRemote(ref.Value, map[string]bool{}) {
		return "", false
	}
	return fmt.Sprintf("%s cache %q", k.Value, ref.Value), true
}

// cacheRemote reports whether the cache resource label is remote or
// persistent; seen ends a multilevel cycle.
func (e *effectWalk) cacheRemote(label string, seen map[string]bool) bool {
	if seen[label] {
		return false
	}
	seen[label] = true
	k, v := component(e.caches[label])
	switch {
	case k == nil:
		return false
	case k.Value == "multilevel":
		// B/pure/cache_multilevel.go:17,54: a list of cache names.
		if v == nil || v.Kind != yaml.SequenceNode {
			return false
		}
		for _, l := range v.Content {
			if l.Kind == yaml.ScalarNode && (isInterpolated(l) || e.cacheRemote(l.Value, seen)) {
				return true
			}
		}
		return false
	}
	return !inProcessCaches[k.Value]
}

// list checks each processor of the list l at path. capture marks
// processors that run during capture, and prefix starts each warning.
func (e *effectWalk) list(l *yaml.Node, path string, capture bool, prefix string) {
	if l == nil || l.Kind != yaml.SequenceNode {
		return
	}
	for i, p := range l.Content {
		e.processor(p, child(path, i), capture, prefix)
	}
}

// processor checks processor p at path against the effect table, then the
// processor lists inside it.
func (e *effectWalk) processor(p *yaml.Node, path string, capture bool, prefix string) {
	if p == nil || p.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(p.Content); i += 2 {
		k, v := p.Content[i], p.Content[i+1]
		if k.Value == "label" {
			continue
		}
		cp := path + "." + k.Value
		if fn, ok := effects[k.Value]; ok {
			if text, hit := fn(e, v); hit {
				suffix := ": processors run for real in a debug session"
				if capture {
					suffix = ": it runs during capture, not stepped"
				}
				e.w.warn(k.Line, cp, "%sthis processor %s%s", prefix, text, suffix)
			}
		}
		if ownListProcessors[k.Value] && v.Kind == yaml.SequenceNode {
			e.list(v, cp, capture, prefix)
		} else {
			e.search(v, cp, capture, prefix)
		}
	}
}

// search finds the processor lists in config n at path: every processors
// list (child lists of known, concurrent and unknown parents alike, and
// nested input or output lists), and the processors of each tool of a chat
// processor (L/tools.go:30,54), whose warnings name the tool.
func (e *effectWalk) search(n *yaml.Node, path string, capture bool, prefix string) {
	if n == nil {
		return
	}
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			cp := path + "." + k.Value
			switch {
			case k.Value == "processors" && v.Kind == yaml.SequenceNode:
				e.list(v, cp, capture, prefix)
			case k.Value == "tools" && v.Kind == yaml.SequenceNode:
				for j, t := range v.Content {
					pre := prefix
					if nm := value(t, "name"); nm != nil && nm.Kind == yaml.ScalarNode {
						pre += fmt.Sprintf("when the model calls the tool %q, ", nm.Value)
					}
					e.search(t, child(cp, j), capture, pre)
				}
			default:
				e.search(v, cp, capture, prefix)
			}
		}
	case yaml.SequenceNode:
		for i, c := range n.Content {
			e.search(c, child(path, i), capture, prefix)
		}
	}
}

// refKeys are the string fields that name a resource (D9): resource and
// cache, and rate_limit on inputs and processors.
var refKeys = setOf("resource", "cache", "rate_limit")

// labelRefs calls fn with every resource label named in n: a refKeys field
// with a literal string value, and each of a workflow's branch_resources.
func labelRefs(n *yaml.Node, fn func(label string)) {
	if n == nil {
		return
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i].Value, n.Content[i+1]
			switch {
			case refKeys[k] && v.Kind == yaml.ScalarNode && !isNull(v) && !isInterpolated(v):
				fn(v.Value)
			case k == "branch_resources" && v.Kind == yaml.SequenceNode:
				for _, l := range v.Content {
					if l.Kind == yaml.ScalarNode && !isInterpolated(l) {
						fn(l.Value)
					}
				}
			}
		}
	}
	for _, c := range n.Content {
		labelRefs(c, fn)
	}
}

// reached returns every label named from roots, following the
// processor_resources entries in bodies that are named, to a fixed point.
// A multilevel cache in caches names its levels too.
func reached(roots []*yaml.Node, bodies, caches map[string]*yaml.Node) map[string]bool {
	out := map[string]bool{}
	var queue []string
	var add func(l string)
	add = func(l string) {
		if out[l] {
			return
		}
		out[l] = true
		if _, ok := bodies[l]; ok {
			queue = append(queue, l)
		}
		if k, v := component(caches[l]); k != nil && k.Value == "multilevel" && v != nil && v.Kind == yaml.SequenceNode {
			for _, lv := range v.Content {
				if lv.Kind == yaml.ScalarNode && !isInterpolated(lv) {
					add(lv.Value)
				}
			}
		}
	}
	for _, r := range roots {
		labelRefs(r, add)
	}
	for len(queue) > 0 {
		l := queue[0]
		queue = queue[1:]
		labelRefs(bodies[l], add)
	}
	return out
}

// effectSections are the parts of the pipeline the effect and state-split
// passes read, before the rewrite wraps any list.
type effectSections struct {
	// input is the user's input block, and lists the top-level
	// input, pipeline and output processor lists (nil when absent).
	input *yaml.Node
	lists []*yaml.Node
	paths []string
	// The *_resources sections, nil when absent.
	inputRes, procRes, cacheRes, rateRes, outputRes *yaml.Node
	// output is the user's output block, and real is output: real: its
	// per-output processors then run.
	output *yaml.Node
	real   bool
}

// warnEffects gives each side-effecting processor one warning (D7), then
// each in-process cache or local rate limit used from both the input tree
// and the step tree one warning (D9). Processors in the input tree, and in
// processor resources reached only from it, run during capture.
func (w *walker) warnEffects(s effectSections) {
	e := &effectWalk{w: w, caches: resourceBodies(s.cacheRes), rateLimits: resourceBodies(s.rateRes)}
	bodies := resourceBodies(s.procRes)

	capture := []*yaml.Node{without(s.input, "processors")}
	if s.inputRes != nil && s.inputRes.Kind == yaml.SequenceNode {
		capture = append(capture, s.inputRes.Content...)
	}
	capRefs, stepRefs := reached(capture, bodies, e.caches), reached(s.lists, bodies, e.caches)
	// Under real the output tree runs too; otherwise its lists don't.
	outRefs := map[string]bool{}
	if s.real {
		var out []*yaml.Node
		if s.output != nil && s.output.Kind == yaml.MappingNode {
			out = append(out, without(s.output, "processors"))
		}
		if s.outputRes != nil && s.outputRes.Kind == yaml.SequenceNode {
			out = append(out, s.outputRes.Content...)
		}
		outRefs = reached(out, bodies, e.caches)
	}

	e.search(capture[0], "input", true, "")
	for i, l := range s.lists {
		e.list(l, s.paths[i], false, "")
	}
	if s.real && s.output != nil {
		e.search(without(s.output, "processors"), "output", false, "")
	}
	if s.procRes != nil && s.procRes.Kind == yaml.SequenceNode {
		for i, entry := range s.procRes.Content {
			label := ""
			if l := value(entry, "label"); l != nil && l.Kind == yaml.ScalarNode {
				label = l.Value
			}
			// Only an entry reached from something that runs runs.
			if label == "" || !capRefs[label] && !stepRefs[label] && !outRefs[label] {
				continue
			}
			e.processor(entry, resourcePath("processor_resources", i, entry), !stepRefs[label] && !outRefs[label], "")
		}
	}
	if s.inputRes != nil && s.inputRes.Kind == yaml.SequenceNode {
		for i, entry := range s.inputRes.Content {
			e.search(entry, resourcePath("input_resources", i, entry), true, "")
		}
	}
	if s.real && s.outputRes != nil && s.outputRes.Kind == yaml.SequenceNode {
		for i, entry := range s.outputRes.Content {
			e.search(entry, resourcePath("output_resources", i, entry), false, "")
		}
	}

	split := func(section, noun string, list *yaml.Node, local map[string]bool) {
		if list == nil || list.Kind != yaml.SequenceNode {
			return
		}
		for i, entry := range list.Content {
			l := value(entry, "label")
			k, _ := component(entry)
			if l == nil || l.Kind != yaml.ScalarNode || k == nil || !local[k.Value] || !capRefs[l.Value] || !stepRefs[l.Value] {
				continue
			}
			w.warn(entry.Line, resourcePath(section, i, entry), "the %s %s %q is used both during capture and during stepping: its state isn't shared between capture and stepping, so the output may differ from production", k.Value, noun, l.Value)
		}
	}
	split("cache_resources", "cache", s.cacheRes, inProcessCaches)
	split("rate_limit_resources", "rate limit", s.rateRes, localRateLimits)
}

// verbatim returns the scalar field key of c as written, def when absent.
func verbatim(c *yaml.Node, key, def string) string {
	n := fieldAt(c, key)
	if isNull(n) || n.Kind != yaml.ScalarNode {
		return def
	}
	return n.Value
}

// quoted returns the scalar field key of c, quoted.
func quoted(c *yaml.Node, key string) string { return fmt.Sprintf("%q", verbatim(c, key, "")) }

// target returns the address field key of c, quoted, without credentials.
func target(c *yaml.Node, key string) string {
	return fmt.Sprintf("%q", redact(verbatim(c, key, "")))
}

// targets is target for a field that holds a list of addresses (or one).
func targets(c *yaml.Node, key string) string {
	n := fieldAt(c, key)
	if n == nil || n.Kind != yaml.SequenceNode {
		return target(c, key)
	}
	var out []string
	for _, u := range n.Content {
		if u.Kind == yaml.ScalarNode {
			out = append(out, fmt.Sprintf("%q", redact(u.Value)))
		}
	}
	return strings.Join(out, ", ")
}

// passwordParam matches a parameter whose key ends in password or pwd
// (sslpassword, Pwd, …), with a quoted or unquoted value.
var passwordParam = regexp.MustCompile(`(?i)(\w*(?:password|pwd))=('[^']*'|"[^"]*"|[^\s&;]*)`)

// redact drops the credentials from an address or DSN before it goes into
// a warning: the userinfo of user:pass@host forms (URLs and the MySQL DSN)
// and password=… parameters. Only an @ in the authority (after any ://, up
// to the first /, ? or #) is userinfo; one in the path or query stays.
func redact(s string) string {
	s = passwordParam.ReplaceAllString(s, "${1}=xxxxx")
	start := 0
	if j := strings.Index(s, "://"); j >= 0 {
		start = j + 3
	}
	end := len(s)
	if k := strings.IndexAny(s[start:], "/?#"); k >= 0 {
		end = start + k
	}
	if at := strings.LastIndex(s[start:end], "@"); at >= 0 {
		s = s[:start] + s[start+at+1:]
	}
	return s
}
