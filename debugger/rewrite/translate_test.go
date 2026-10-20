package rewrite_test

import (
	"context"
	"log/slog"
	"os"
	"path/filepath"
	"regexp"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/engine"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

// throwing provokes one runtime error in every kind of place a processor
// can sit: input.processors, pipeline.processors (plain and literal
// mappings), a nested branch, a switch case, and processor_resources (a
// nested one and a direct one).
const throwing = `input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = {"a": 1}'
  processors:
    - mapping: root = throw("in input")
pipeline:
  processors:
    - mapping: 'root = this'
    - mapping: |
        root.x = 1
        root.y = throw("second")
    - branch:
        processors:
          - mapping: root = throw("in branch")
    - resource: res
    - switch:
        - processors:
            - mutation: |
                root.z = 1

                root.w = throw("in switch")
    - resource: plain
processor_resources:
  - label: res
    branch:
      processors:
        - mapping: root = throw("in resource")
  - label: plain
    mapping: root = throw("plain")
output:
  drop: {}
`

// collector keeps the engine path and message of every error record, the
// way the engine attaches them: each component adds its own path
// attribute, so the last one is the record's, and a resource is named by
// the label right after the resource path.
type collector struct {
	mu    *sync.Mutex
	attrs []slog.Attr
	got   *[][2]string
}

func (c *collector) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelError }

func (c *collector) Handle(_ context.Context, r slog.Record) error {
	path, label, afterRes := "", "", false
	for _, a := range c.attrs {
		switch a.Key {
		case "path":
			path = a.Value.String()
			afterRes = path == rewrite.ResourcePath
		case "label":
			if afterRes {
				label = a.Value.String()
			}
			afterRes = false
		}
	}
	c.mu.Lock()
	*c.got = append(*c.got, [2]string{rewrite.QualifyResource(path, label), r.Message})
	c.mu.Unlock()
	return nil
}

func (c *collector) WithAttrs(as []slog.Attr) slog.Handler {
	n := *c
	n.attrs = append(slices.Clone(c.attrs), as...)
	return &n
}

func (c *collector) WithGroup(string) slog.Handler { return c }

type nopArriver struct{}

func (nopArriver) Captured(int, int) {}
func (nopArriver) Enter(ctx context.Context, _, _ int) (context.Context, func()) {
	return ctx, func() {}
}
func (nopArriver) Arrive(context.Context, int, int, []core.Batch) error { return nil }

var lineRef = regexp.MustCompile(`\(line (\d+)\)`)

// Real engine errors, translated: every path names the user's processor,
// never a stepper, and every Bloblang line is the file line.
func TestTranslateEngineErrors(t *testing.T) {
	plan, err := rewrite.Rewrite([]byte(throwing))
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "pipeline.yaml")
	if err := os.WriteFile(path, []byte(throwing), 0o600); err != nil {
		t.Fatal(err)
	}
	load := func(string) (engine.Configs, core.LineMap, error) {
		return engine.Configs{Input: plan.Input, Resources: plan.Resources, Steppers: plan.Steppers}, plan, nil
	}
	var got [][2]string
	log := slog.New(&collector{mu: &sync.Mutex{}, got: &got})
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := engine.New(service.NewEnvironment(), log, load).Start(ctx, core.StartOptions{Pipeline: path}, nopArriver{}); err != nil {
		t.Fatal(err)
	}

	translated := map[string]string{}
	for _, rec := range got {
		user, ok := plan.Translate(rec[0])
		if !ok {
			t.Errorf("engine path %s did not translate", rec[0])
			continue
		}
		msg := lineRef.ReplaceAllStringFunc(rec[1], func(m string) string {
			n, _ := strconv.Atoi(lineRef.FindStringSubmatch(m)[1])
			if l, ok := plan.TranslateLine(rec[0], n); ok {
				return "(line " + strconv.Itoa(l) + ")"
			}
			return m
		})
		translated[user] = msg
	}
	want := map[string]string{
		"input.processors[0]":                            "failed assignment (line 7): in input",
		"pipeline.processors[1]":                         "failed assignment (line 13): second",
		"pipeline.processors[2].branch.processors[0]":    "failed assignment (line 16): in branch",
		"pipeline.processors[4].switch[0].processors[0]": "failed assignment (line 23): in switch",
		"processor_resources.res.branch.processors[0]":   "failed assignment (line 29): in resource",
		"processor_resources.plain":                      "failed assignment (line 31): plain",
	}
	for p, m := range want {
		if translated[p] != m {
			t.Errorf("%s: got %q, want %q", p, translated[p], m)
		}
	}
	if t.Failed() {
		t.Logf("records: %q", got)
	}
}

func TestTranslateTable(t *testing.T) {
	plan, err := rewrite.Rewrite([]byte(throwing))
	if err != nil {
		t.Fatal(err)
	}
	for engine, user := range map[string]string{
		"root.pipeline.processors.0":                                                     "input.processors",
		"root.pipeline.processors.1.rpcn_debug_stepper":                                  "pipeline.processors",
		"root.pipeline.processors.1.rpcn_debug_stepper.processors":                       "pipeline.processors",
		"root.pipeline.processors.1.rpcn_debug_stepper.processors.2.branch":              "pipeline.processors[2].branch",
		"root.pipeline.processors.1.rpcn_debug_stepper.processors.2.branch.processors.0": "pipeline.processors[2].branch.processors",
		"root.processor_resources.res":                                                   "processor_resources.res",
		"root.processor_resources.res.branch.processors":                                 "processor_resources.res.branch.processors",
	} {
		if got, ok := plan.Translate(engine); !ok || got != user {
			t.Errorf("Translate(%s) = %q, %v; want %q", engine, got, ok, user)
		}
	}
	for _, p := range []string{"root.input", "root.output", "root.pipeline", "elsewhere"} {
		if got, ok := plan.Translate(p); ok || got != p {
			t.Errorf("Translate(%s) = %q, %v; want it unchanged and not ok", p, got, ok)
		}
	}
	// Quoted and folded mappings aren't shifted.
	if _, ok := plan.TranslateLine("root.pipeline.processors.1.rpcn_debug_stepper.processors.0", 1); ok {
		t.Error("a quoted mapping got a base line")
	}
	if got := rewrite.QualifyResource("root.pipeline.processors.0", "x"); got != "root.pipeline.processors.0" {
		t.Errorf("QualifyResource changed a pipeline path: %s", got)
	}
}
