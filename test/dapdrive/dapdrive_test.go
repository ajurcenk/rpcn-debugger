package dapdrive_test

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ajurcenk/rpcn-debugger/debugger/engine"
	"github.com/ajurcenk/rpcn-debugger/test/dapdrive"
	"github.com/ajurcenk/rpcn-debugger/test/equivalence"
)

const (
	parents = "../../testdata/tracer/parents"
	nested  = "../../testdata/tracer/nested.yaml"
)

// trace renders each stop as "<line> <user path> <frames>", the frames
// innermost first with their labels, e.g.
// "15 pipeline.processors[0].for_each[0] pipeline.processors[0].for_each[msg 1/3] < pipeline.processors".
func trace(res dapdrive.Result) []string {
	out := make([]string, len(res.Stops))
	for i, s := range res.Stops {
		frames := make([]string, len(s.Frames))
		for j, f := range s.Frames {
			frames[j] = f.Name + f.Label
		}
		out[i] = fmt.Sprintf("%d %s %s", s.Line, s.UserPath, strings.Join(frames, " < "))
	}
	return out
}

func run(t *testing.T, pipeline string) dapdrive.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{Pipeline: pipeline})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// sameAsReference checks the delivered batches against the pipeline run
// without the debugger, with the AD-8 harness's one definition of equal.
func sameAsReference(t *testing.T, pipeline string, got []dapdrive.Batch) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	ref, err := equivalence.Case{Name: filepath.Base(pipeline), Pipeline: pipeline}.RunReference(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := equivalence.Compare(ref, got); err != nil {
		t.Error(err)
	}
}

func check(t *testing.T, res dapdrive.Result, want []string) {
	t.Helper()
	got := trace(res)
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("stops:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if len(res.Stops) == 0 || res.Stops[0].Reason != "entry" {
		t.Errorf("first stop = %+v, want reason entry", res.Stops)
	}
}

func outputs(res dapdrive.Result, prefix string) []string {
	var out []string
	for _, o := range res.Output {
		if strings.HasPrefix(o, prefix) {
			out = append(out, o)
		}
	}
	return out
}

// One scripted pipeline per in-scope AD-7 parent: Step In at every stop
// shows each entry the engine makes, with its repeat label (AD-10), and
// delivers what the engine delivers (AD-8).
func TestParents(t *testing.T) {
	const top = "pipeline.processors"
	cases := []struct {
		name  string
		stops []string
		more  func(t *testing.T, res dapdrive.Result)
	}{
		{
			name: "for_each",
			stops: []string{
				"14 pipeline.processors[0] " + top,
				"15 pipeline.processors[0].for_each[0] pipeline.processors[0].for_each[msg 1/3] < " + top,
				"14 pipeline.processors[0].for_each pipeline.processors[0].for_each[msg 1/3] < " + top,
				"15 pipeline.processors[0].for_each[0] pipeline.processors[0].for_each[msg 2/3] < " + top,
				"14 pipeline.processors[0].for_each pipeline.processors[0].for_each[msg 2/3] < " + top,
				"15 pipeline.processors[0].for_each[0] pipeline.processors[0].for_each[msg 3/3] < " + top,
				"14 pipeline.processors[0].for_each pipeline.processors[0].for_each[msg 3/3] < " + top,
				"13 pipeline.processors " + top,
			},
		},
		{
			name: "retry",
			stops: []string{
				"15 pipeline.processors[0] " + top,
				"21 pipeline.processors[0].retry.processors[0] pipeline.processors[0].retry.processors[attempt 1/3] < " + top,
				"20 pipeline.processors[0].retry.processors pipeline.processors[0].retry.processors[attempt 1/3] < " + top,
				"21 pipeline.processors[0].retry.processors[0] pipeline.processors[0].retry.processors[attempt 2/3] < " + top,
				"20 pipeline.processors[0].retry.processors pipeline.processors[0].retry.processors[attempt 2/3] < " + top,
				"21 pipeline.processors[0].retry.processors[0] pipeline.processors[0].retry.processors[attempt 3/3] < " + top,
				"20 pipeline.processors[0].retry.processors pipeline.processors[0].retry.processors[attempt 3/3] < " + top,
				"14 pipeline.processors " + top,
			},
			more: func(t *testing.T, res dapdrive.Result) {
				attempts := outputs(res, "retry ")
				if len(attempts) != 3 || attempts[2] != "retry pipeline.processors[0].retry.processors [attempt 3/3]\n" {
					t.Errorf("attempt output = %q, want 3 attempts", attempts)
				}
				if w := outputs(res, "Warning: pipeline.processors[0].retry.processors runs inside a retry"); len(w) != 1 {
					t.Errorf("time-budget warnings = %q, want one", w)
				}
			},
		},
		{
			name: "while",
			stops: []string{
				"12 pipeline.processors[0] " + top,
				"15 pipeline.processors[0].while.processors[0] pipeline.processors[0].while.processors[loop 1] < " + top,
				"14 pipeline.processors[0].while.processors pipeline.processors[0].while.processors[loop 1] < " + top,
				"15 pipeline.processors[0].while.processors[0] pipeline.processors[0].while.processors[loop 2] < " + top,
				"14 pipeline.processors[0].while.processors pipeline.processors[0].while.processors[loop 2] < " + top,
				"15 pipeline.processors[0].while.processors[0] pipeline.processors[0].while.processors[loop 3] < " + top,
				"14 pipeline.processors[0].while.processors pipeline.processors[0].while.processors[loop 3] < " + top,
				"11 pipeline.processors " + top,
			},
		},
		{
			name: "group_by",
			stops: []string{
				"14 pipeline.processors[0] " + top,
				"17 pipeline.processors[0].group_by[0].processors[0] pipeline.processors[0].group_by[0].processors[group 1] < " + top,
				"16 pipeline.processors[0].group_by[0].processors pipeline.processors[0].group_by[0].processors[group 1] < " + top,
				"20 pipeline.processors[0].group_by[1].processors[0] pipeline.processors[0].group_by[1].processors[group 2] < " + top,
				"19 pipeline.processors[0].group_by[1].processors pipeline.processors[0].group_by[1].processors[group 2] < " + top,
				"13 pipeline.processors " + top,
			},
			more: func(t *testing.T, res dapdrive.Result) {
				if len(res.Delivered) != 2 {
					t.Errorf("delivered %d batches, want 2 (one per group)", len(res.Delivered))
				}
			},
		},
		{
			name: "try",
			stops: []string{
				"14 pipeline.processors[0] " + top,
				"15 pipeline.processors[0].try[0] pipeline.processors[0].try[msg 1/3] < " + top,
				"16 pipeline.processors[0].try[1] pipeline.processors[0].try[msg 1/3] < " + top,
				"14 pipeline.processors[0].try pipeline.processors[0].try[msg 1/3] < " + top,
				// Message 2 fails try[0]: try[1] never stops on it.
				"15 pipeline.processors[0].try[0] pipeline.processors[0].try[msg 2/3] < " + top,
				"14 pipeline.processors[0].try pipeline.processors[0].try[msg 2/3] < " + top,
				"15 pipeline.processors[0].try[0] pipeline.processors[0].try[msg 3/3] < " + top,
				"16 pipeline.processors[0].try[1] pipeline.processors[0].try[msg 3/3] < " + top,
				"14 pipeline.processors[0].try pipeline.processors[0].try[msg 3/3] < " + top,
				"13 pipeline.processors " + top,
			},
			more: func(t *testing.T, res dapdrive.Result) {
				for _, s := range res.Stops {
					if s.UserPath == "pipeline.processors[0].try[1]" && strings.Contains(s.Batches[0][0].Content, `"n":2`) {
						t.Errorf("try[1] stopped on message 2: %+v", s)
					}
				}
				want := "Skipped message 2 of 3 at pipeline.processors[0].try[1] (line 16): it already failed, so try runs no later processor on it\n"
				if got := outputs(res, "Skipped"); len(got) != 1 || got[0] != want {
					t.Errorf("skipped output = %q, want %q", got, want)
				}
			},
		},
		{
			name: "catch",
			stops: []string{
				"14 pipeline.processors[0] " + top,
				"15 pipeline.processors[1] " + top,
				"16 pipeline.processors[1].catch[0] pipeline.processors[1].catch < " + top,
				"15 pipeline.processors[1].catch pipeline.processors[1].catch < " + top,
				"13 pipeline.processors " + top,
			},
			more: func(t *testing.T, res dapdrive.Result) {
				if c := res.Stops[2].Batches; len(c) != 1 || len(c[0]) != 1 || !strings.Contains(c[0][0].Content, `"n":2`) || c[0][0].Error == "" {
					t.Errorf("catch entry batches = %+v, want the failed message 2 only", c)
				}
				for _, b := range res.Delivered {
					for _, m := range b {
						if m.Error != "" {
							t.Errorf("delivered %+v with an error: catch clears it", m)
						}
					}
				}
			},
		},
		{
			name: "branch",
			stops: []string{
				"11 pipeline.processors[0] " + top,
				"14 pipeline.processors[0].branch.processors[0] pipeline.processors[0].branch.processors < " + top,
				"13 pipeline.processors[0].branch.processors pipeline.processors[0].branch.processors < " + top,
				"10 pipeline.processors " + top,
			},
		},
		{
			name: "switch",
			stops: []string{
				"13 pipeline.processors[0] " + top,
				"16 pipeline.processors[0].switch[0].processors[0] pipeline.processors[0].switch[0].processors < " + top,
				"15 pipeline.processors[0].switch[0].processors pipeline.processors[0].switch[0].processors < " + top,
				"18 pipeline.processors[0].switch[1].processors[0] pipeline.processors[0].switch[1].processors < " + top,
				"17 pipeline.processors[0].switch[1].processors pipeline.processors[0].switch[1].processors < " + top,
				"12 pipeline.processors " + top,
			},
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			pipeline := filepath.Join(parents, c.name+".yaml")
			res := run(t, pipeline)
			check(t, res, c.stops)
			if res.Delivered == nil {
				t.Fatal("nothing delivered")
			}
			sameAsReference(t, pipeline, res.Delivered)
			if c.more != nil {
				c.more(t, res)
			}
		})
	}
}

// The driver on the nested sample: every stop's line and user path in
// order, with its frame stack.
func TestNested(t *testing.T) {
	res := run(t, nested)
	check(t, res, []string{
		"12 input.processors[0] input.processors",
		"11 input.processors input.processors",
		"18 pipeline.processors[0] pipeline.processors",
		"21 pipeline.processors[0].branch.processors[0] pipeline.processors[0].branch.processors < pipeline.processors",
		"20 pipeline.processors[0].branch.processors pipeline.processors[0].branch.processors < pipeline.processors",
		"23 pipeline.processors[1] pipeline.processors",
		"26 pipeline.processors[1].switch[0].processors[0] pipeline.processors[1].switch[0].processors < pipeline.processors",
		"25 pipeline.processors[1].switch[0].processors pipeline.processors[1].switch[0].processors < pipeline.processors",
		"17 pipeline.processors pipeline.processors",
		"36 output.processors[0] output.processors",
		"35 output.processors output.processors",
	})
	if len(res.Delivered) != 1 || len(res.Delivered[0]) != 1 ||
		res.Delivered[0][0].Content != `{"branched":10,"case":"one","from_input":true,"from_output":true,"n":1}` {
		t.Errorf("delivered = %+v", res.Delivered)
	}
}

// Next at every stop never enters a child list.
func TestScriptNext(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{Pipeline: filepath.Join(parents, "for_each.yaml"), Script: dapdrive.Always(dapdrive.Next)})
	if err != nil {
		t.Fatal(err)
	}
	check(t, res, []string{
		"14 pipeline.processors[0] pipeline.processors",
		"13 pipeline.processors pipeline.processors",
	})
}

// A parent stop holding several batches calls the next processor once per
// batch: each call numbers its entries from 1 (AD-10).
func TestLabelsRestartPerCall(t *testing.T) {
	pipeline := filepath.Join(t.TempDir(), "pipeline.yaml")
	if err := os.WriteFile(pipeline, []byte(`
input:
  generate:
    count: 4
    batch_size: 4
    interval: ""
    mapping: 'root.n = counter()'
pipeline:
  processors:
    - split: {size: 2}
    - for_each:
        - mapping: 'root = this.merge({"each": true})'
    - group_by:
        - check: 'this.n % 2 == 0'
          processors:
            - mapping: 'root = this.merge({"group": "even"})'
        - check: 'this.n % 2 == 1'
          processors:
            - mapping: 'root = this.merge({"group": "odd"})'
    - while:
        check: 'this.loops.or(0) < 2'
        processors:
          - mapping: |
              root = this
              root.loops = this.loops.or(0) + 1
output:
  drop: {}
`), 0o600); err != nil {
		t.Fatal(err)
	}
	res := run(t, pipeline)
	var labels []string
	for _, s := range res.Stops {
		if !s.End && s.Frames[0].Label != "" {
			labels = append(labels, s.Frames[0].Label)
		}
	}
	// split makes 2 batches of 2; group_by makes 2 batches of each; while
	// then runs on 4 batches.
	want := []string{
		"[msg 1/2]", "[msg 2/2]", "[msg 1/2]", "[msg 2/2]",
		"[group 1]", "[group 2]", "[group 1]", "[group 2]",
		"[loop 1]", "[loop 2]", "[loop 1]", "[loop 2]", "[loop 1]", "[loop 2]", "[loop 1]", "[loop 2]",
	}
	if strings.Join(labels, " ") != strings.Join(want, " ") {
		t.Errorf("labels = %q, want %q", labels, want)
	}
	sameAsReference(t, pipeline, res.Delivered)
}

// runScript runs pipeline with script.
func runScript(t *testing.T, pipeline string, script dapdrive.Script) dapdrive.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{Pipeline: pipeline, Script: script})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

func writeYAML(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipeline.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// Continue on the tracer sample: one delivered batch in the Debug Console,
// with its content and metadata, and one rpcnDelivered event (AD-21).
func TestDeliveredTracer(t *testing.T) {
	pipeline := "../../testdata/tracer/pipeline.yaml"
	res := runScript(t, pipeline, dapdrive.Always(dapdrive.Continue))
	got := outputs(res, "Delivered")
	want := "Delivered batch 1 (1 messages) to dry-run output\n" +
		`  message 0: {"doubled":2,"greeting":"hello","n":1}` + "\n" +
		"    __rpcn_internal = x (internal)\n" +
		"    kafka_key = k1\n" +
		"    source = tracer\n"
	if len(got) != 1 || got[0] != want {
		t.Errorf("delivered output = %q, want %q", got, want)
	}
	if len(res.Delivered) != 1 || len(res.Delivered[0]) != 1 {
		t.Fatalf("delivered = %+v, want one batch of one message", res.Delivered)
	}
	if m := res.Delivered[0][0].Metadata["kafka_key"]; m != (dapdrive.Meta{Type: "bytes", Value: "k1", Lossless: "azE="}) {
		t.Errorf("kafka_key = %+v", m)
	}
	if n := len(outputs(res, "Nothing was delivered")); n != 0 {
		t.Errorf("%d nothing-delivered notes", n)
	}
	sameAsReference(t, pipeline, res.Delivered)
}

// Continue on group_by: one Delivered entry per output batch.
func TestDeliveredGroupBy(t *testing.T) {
	pipeline := filepath.Join(parents, "group_by.yaml")
	res := runScript(t, pipeline, dapdrive.Always(dapdrive.Continue))
	if got := outputs(res, "Delivered batch"); len(got) != 2 ||
		!strings.HasPrefix(got[0], "Delivered batch 1 ") || !strings.HasPrefix(got[1], "Delivered batch 2 ") {
		t.Errorf("delivered output = %q, want batches 1 and 2", got)
	}
	sameAsReference(t, pipeline, res.Delivered)
}

// A message error survives to the output: its error line shows under it.
func TestDeliveredError(t *testing.T) {
	pipeline := writeYAML(t, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.n = 1'
pipeline:
  processors:
    - mapping: 'root = throw("boom")'
output:
  drop: {}
`)
	res := runScript(t, pipeline, dapdrive.Always(dapdrive.Continue))
	got := outputs(res, "Delivered")
	if len(got) != 1 || !strings.Contains(got[0], "\n    error: failed assignment (line 1): boom\n") {
		t.Errorf("delivered output = %q, want the error line", got)
	}
	sameAsReference(t, pipeline, res.Delivered)
}

// S2 (the retro repro): an interpolated error_handling.strict that resolves
// to true through the launch env rejects the failed message as the literal
// does: nothing is delivered and the Debug Console shows the strict note.
func TestStrictInterpolatedRejects(t *testing.T) {
	const src = `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.n = 1'
pipeline:
  processors:
    - mapping: 'root = throw("boom")'
error_handling:
  strict: %s
output:
  drop: {}
`
	for _, strict := range []string{"true", "${STRICT}"} {
		t.Run(strict, func(t *testing.T) {
			pipeline := writeYAML(t, fmt.Sprintf(src, strict))
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			res, err := dapdrive.Run(ctx, dapdrive.Options{
				Pipeline: pipeline,
				Launch:   map[string]any{"env": map[string]any{"STRICT": "true"}},
				Script:   dapdrive.Always(dapdrive.Continue),
			})
			if err != nil {
				t.Fatal(err)
			}
			if res.Delivered != nil {
				t.Errorf("delivered = %+v, want nothing", res.Delivered)
			}
			if got := outputs(res, "error_handling.strict: the output rejected"); len(got) != 1 {
				t.Errorf("outputs = %q, want one strict-rejection note", res.Output)
			}
			if got := outputs(res, "Warning: error_handling.strict"); len(got) != 0 {
				t.Errorf("warnings = %q, want none for strict", got)
			}
		})
	}
}

// Every message deleted: nothing delivered, a note, and a clean end.
func TestNothingDelivered(t *testing.T) {
	pipeline := writeYAML(t, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root.n = 1'
pipeline:
  processors:
    - mapping: 'root = deleted()'
output:
  drop: {}
`)
	res := runScript(t, pipeline, dapdrive.Always(dapdrive.Continue))
	if res.Delivered != nil {
		t.Errorf("delivered = %+v, want nothing", res.Delivered)
	}
	if got := outputs(res, "Nothing was delivered"); len(got) != 1 {
		t.Errorf("outputs = %q, want one nothing-delivered note", res.Output)
	}
}

// An output with batching warns once at launch; the delivery is unbatched.
func TestBatchingWarning(t *testing.T) {
	pipeline := writeYAML(t, `
input:
  generate:
    count: 3
    batch_size: 3
    interval: ""
    mapping: 'root.n = counter()'
output:
  drop: {}
  batching: {count: 5}
`)
	res := runScript(t, pipeline, dapdrive.Always(dapdrive.Continue))
	want := "Warning: output.batching (line 10): output batching isn't applied in dry-run: the batches shown are the pipeline's own, and production batch boundaries may differ\n"
	if got := outputs(res, "Warning: output.batching"); len(got) != 1 || got[0] != want {
		t.Errorf("warnings = %q, want %q", got, want)
	}
	// The captured batch of 3 is delivered as is: no output batching
	// (count 5 would have held it until the stream closed).
	if len(res.Delivered) != 1 || len(res.Delivered[0]) != 3 {
		t.Errorf("delivered = %+v, want the one batch of 3", res.Delivered)
	}

	plain := runScript(t, filepath.Join(parents, "for_each.yaml"), dapdrive.Always(dapdrive.Continue))
	if got := outputs(plain, "Warning: "); len(got) != 0 {
		t.Errorf("warnings without batching = %q", got)
	}
}

// Large content: the console shows the first 4 KiB, the event all of it.
func TestDeliveredLarge(t *testing.T) {
	pipeline := writeYAML(t, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = "x".repeat(10240)'
output:
  drop: {}
`)
	res := runScript(t, pipeline, dapdrive.Always(dapdrive.Continue))
	got := outputs(res, "Delivered")
	want := "Delivered batch 1 (1 messages) to dry-run output\n  message 0: " + strings.Repeat("x", 4096) + "… 6144 more bytes\n"
	if len(got) != 1 || got[0] != want {
		t.Errorf("delivered output = %.120q…", got)
	}
	if len(res.Delivered) != 1 || len(res.Delivered[0][0].Content) != 10240 {
		t.Errorf("event content is not the full 10 KiB")
	}
}

// S1 (epic-tracer retrospective): a resource used in the input tree runs
// during capture without being stepped; the same resource used in the
// pipeline is still stepped. The resource nests a try, so its inner stepper
// checks for its parent's frame (AD-17) in both streams.
func TestInputResourceUnstepped(t *testing.T) {
	const src = `
input:
  broker:
    inputs:
      - generate:
          count: 1
          interval: ""
          mapping: 'root = {"a": 1}'
        processors:
          - resource: r
pipeline:
  processors:
    - mutation: 'root.b = 2'%s
processor_resources:
  - label: r
    branch:
      processors:
        - try:
            - mapping: 'root.c = 3'
      result_map: 'root.c = this.c'
output:
  drop: {}
`
	for name, c := range map[string]struct {
		extra    string
		resStops int
	}{
		"input only":         {"", 0},
		"input and pipeline": {"\n    - resource: r", 2},
	} {
		t.Run(name, func(t *testing.T) {
			res := runScript(t, writeYAML(t, fmt.Sprintf(src, c.extra)), dapdrive.Always(dapdrive.StepIn))
			if len(res.Stops) == 0 || res.Stops[0].UserPath != "pipeline.processors[0]" {
				t.Fatalf("stops = %+v, want the first at pipeline.processors[0]", res.Stops)
			}
			n := 0
			for _, s := range res.Stops {
				if strings.HasPrefix(s.UserPath, "processor_resources.r.") && !s.End {
					n++
				}
			}
			if n != c.resStops {
				t.Errorf("stops inside processor_resources.r = %d, want %d: %+v", n, c.resStops, res.Stops)
			}
			for _, o := range res.Output {
				if strings.Contains(o, "CTX_LOST") {
					t.Errorf("output %q, want no CTX_LOST", o)
				}
			}
			if len(res.Delivered) != 1 || len(res.Delivered[0]) != 1 || res.Delivered[0][0].Content != `{"a":1,"b":2,"c":3}` {
				t.Errorf("delivered = %+v, want one message with c set during capture", res.Delivered)
			}
		})
	}
}

const launchedPipeline = `input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = "a"'
pipeline:
  processors:
    - mapping: 'root = content().string() + "1"'
    - mapping: 'root = content().string() + "2"'
output:
  drop: {}
`

// An edit after launch shifts every line and replaces the processors.
const editedPipeline = `# edited


input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = "b"'
pipeline:
  processors:
    - mapping: 'root = "edited"'
output:
  drop: {}
`

// Start runs the configs Check produced at launch (AD-6: one rewrite per
// launch): a pipeline file edited or removed between launch and
// configurationDone changes neither the stops nor the output.
func TestOneRewritePerLaunch(t *testing.T) {
	for name, change := range map[string]func(path string) error{
		"edited":  func(path string) error { return os.WriteFile(path, []byte(editedPipeline), 0o600) },
		"deleted": os.Remove,
	} {
		t.Run(name, func(t *testing.T) {
			dir := t.TempDir()
			path := filepath.Join(dir, "pipeline.yaml")
			if err := os.WriteFile(path, []byte(launchedPipeline), 0o600); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			want, err := dapdrive.Run(ctx, dapdrive.Options{Pipeline: path, Script: dapdrive.Always(dapdrive.Next)})
			if err != nil {
				t.Fatal(err)
			}
			got, err := dapdrive.Run(ctx, dapdrive.Options{
				Pipeline: path, Script: dapdrive.Always(dapdrive.Next),
				AfterLaunch: func() error { return change(path) },
			})
			if err != nil {
				t.Fatal(err)
			}
			if g, w := strings.Join(trace(got), "\n"), strings.Join(trace(want), "\n"); g != w || len(want.Stops) < 3 {
				t.Errorf("stops:\n%s\nwant the launched pipeline's:\n%s", g, w)
			}
			if len(got.Delivered) != 1 || len(got.Delivered[0]) != 1 || got.Delivered[0][0].Content != "a12" {
				t.Errorf("delivered = %+v, want one batch holding a12", got.Delivered)
			}
		})
	}
}

// restartPipeline is a file-input pipeline for the restart tests; %s is
// the data file's path. The mapping is at line 9.
const restartPipeline = `input:
  file:
    paths: [ "%s" ]
    scanner:
      lines: {}

pipeline:
  processors:
    - mapping: |
        root.line = content().string()
        root.upper = content().string().uppercase()

output:
  drop: {}
`

// restartCopy writes restartPipeline and its data file into a temp dir,
// returning the pipeline's path, its source and the data file's path.
func restartCopy(t *testing.T) (pipeline, src, data string) {
	t.Helper()
	dir := t.TempDir()
	pipeline, data = filepath.Join(dir, "restart.yaml"), filepath.Join(dir, "restart-data.txt")
	src = fmt.Sprintf(restartPipeline, data)
	if err := os.WriteFile(pipeline, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(data, []byte("hello restart\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	return pipeline, src, data
}

func write(path, content string) error { return os.WriteFile(path, []byte(content), 0o600) }

// Restart at the mapping re-steps the captured batch (AD-20): the data
// file's edit is not read, nor is the input block (its path now names a
// missing file), the mapping's edit is in effect, and the batch is
// delivered as batch 1.
func TestRestartReStepsTheCapturedBatch(t *testing.T) {
	pipeline, src, data := restartCopy(t)
	edited := strings.Replace(src, "uppercase()", `uppercase() + "!"`, 1)
	edited = strings.Replace(edited, data, "/rpcn-debug-no-such-dir/data.txt", 1)
	var scriptErr error
	script := func(stops []dapdrive.Stop) dapdrive.Command {
		if len(stops) == 1 {
			scriptErr = errors.Join(write(data, "changed\n"), write(pipeline, edited))
			return dapdrive.Restart
		}
		return dapdrive.Continue
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{Pipeline: pipeline, Script: script})
	if err != nil {
		t.Fatal(err)
	}
	if scriptErr != nil {
		t.Fatal(scriptErr)
	}
	if len(res.Stops) != 2 || res.Stops[1].Reason != "entry" || res.Stops[1].Line != 9 || res.Stops[1].UserPath != "pipeline.processors[0]" {
		t.Errorf("stops = %+v, want the entry stop at the mapping twice", res.Stops)
	}
	if len(res.RestartErrors) != 0 {
		t.Errorf("restart errors = %q", res.RestartErrors)
	}
	if len(res.Delivered) != 1 || len(res.Delivered[0]) != 1 ||
		res.Delivered[0][0].Content != `{"line":"hello restart","upper":"HELLO RESTART!"}` {
		t.Errorf("delivered = %+v, want the original data through the new mapping", res.Delivered)
	}
	if notes := outputs(res, "Restart re-reads only the pipeline file"); len(notes) != 1 {
		t.Errorf("restart notes = %q, want one", notes)
	}
}

// A restart with a broken pipeline file fails, naming the line, and leaves
// the stop: a following Continue delivers with the old mapping.
func TestRestartBrokenPipeline(t *testing.T) {
	for name, tc := range map[string]struct {
		edit func(src string) string
		want string
	}{
		"lint": {
			edit: func(src string) string {
				return strings.Replace(src, "    - mapping: |", "    - no_such_processor: {}\n    - mapping: |", 1)
			},
			want: "line 9",
		},
		"yaml": {
			edit: func(src string) string { return strings.Replace(src, "  processors:", "  processors: [", 1) },
			want: "line 8",
		},
	} {
		t.Run(name, func(t *testing.T) {
			pipeline, src, _ := restartCopy(t)
			asked := 0
			var scriptErr error
			script := func([]dapdrive.Stop) dapdrive.Command {
				asked++
				if asked == 1 {
					scriptErr = write(pipeline, tc.edit(src))
					return dapdrive.Restart
				}
				return dapdrive.Continue
			}
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			defer cancel()
			res, err := dapdrive.Run(ctx, dapdrive.Options{Pipeline: pipeline, Script: script})
			if err != nil {
				t.Fatal(err)
			}
			if scriptErr != nil {
				t.Fatal(scriptErr)
			}
			if len(res.RestartErrors) != 1 || !strings.Contains(res.RestartErrors[0], tc.want) {
				t.Errorf("restart errors = %q, want one naming %s", res.RestartErrors, tc.want)
			}
			if len(res.Stops) != 1 || asked != 2 {
				t.Errorf("stops = %d, asked %d times; want one stop, asked twice", len(res.Stops), asked)
			}
			if len(res.Delivered) != 1 || res.Delivered[0][0].Content != `{"line":"hello restart","upper":"HELLO RESTART"}` {
				t.Errorf("delivered = %+v, want the old mapping's output", res.Delivered)
			}
		})
	}
}

// The end of a run doesn't terminate the session (D5): the run-finished
// note goes out, and AfterRun's Restart re-steps the kept batch with the
// edited mapping, delivered as batch 1 again. The second end of the run
// ends Run.
func TestAfterRunRestart(t *testing.T) {
	pipeline, src, data := restartCopy(t)
	edited := strings.Replace(src, "uppercase()", `uppercase() + "!"`, 1)
	var ends []int // the delivered count at each end of a run
	var editErr error
	afterRun := func(res dapdrive.Result) dapdrive.Command {
		ends = append(ends, len(res.Delivered))
		if len(ends) == 1 {
			editErr = errors.Join(write(data, "changed\n"), write(pipeline, edited))
			return dapdrive.Restart
		}
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{Pipeline: pipeline, Script: dapdrive.Always(dapdrive.Continue), AfterRun: afterRun})
	if err != nil {
		t.Fatal(err)
	}
	if editErr != nil {
		t.Fatal(editErr)
	}
	if res.Terminated {
		t.Error("the run ended with terminated, want the root thread's exit")
	}
	if len(ends) != 2 || ends[0] != 1 || ends[1] != 2 {
		t.Errorf("AfterRun saw %v delivered batches, want [1 2]", ends)
	}
	if len(res.RestartErrors) != 0 {
		t.Errorf("restart errors = %q", res.RestartErrors)
	}
	if len(res.Stops) != 2 || res.Stops[0].Reason != "entry" || res.Stops[1].Reason != "entry" {
		t.Errorf("stops = %+v, want the entry stop twice", res.Stops)
	}
	// Run checks each rpcnDelivered is numbered from 1 in its run.
	if len(res.Delivered) != 2 ||
		res.Delivered[0][0].Content != `{"line":"hello restart","upper":"HELLO RESTART"}` ||
		res.Delivered[1][0].Content != `{"line":"hello restart","upper":"HELLO RESTART!"}` {
		t.Errorf("delivered = %+v, want the same batch through the old, then the new mapping", res.Delivered)
	}
	if notes := outputs(res, "Run finished: Restart re-steps the same batch with the edited pipeline; Stop ends the session."); len(notes) != 2 {
		t.Errorf("run-finished notes = %q, want one per run", notes)
	}
}

// A launch that captures nothing ends as before, with terminated: the
// NOTHING_CAPTURED note, no run-finished note, and AfterRun is not asked.
func TestNothingCapturedEnds(t *testing.T) {
	pipeline, _, data := restartCopy(t)
	if err := write(data, ""); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	asked := 0
	res, err := dapdrive.Run(ctx, dapdrive.Options{
		Pipeline: pipeline,
		AfterRun: func(dapdrive.Result) dapdrive.Command { asked++; return dapdrive.Restart },
	})
	if err != nil {
		t.Fatal(err)
	}
	if got := outputs(res, "no batch was captured"); len(got) != 1 {
		t.Errorf("outputs = %q, want the nothing-captured note", res.Output)
	}
	if got := outputs(res, "Run finished"); len(got) != 0 {
		t.Errorf("run-finished notes = %q, want none", got)
	}
	if len(res.Stops) != 0 || res.Delivered != nil {
		t.Errorf("stops = %+v, delivered = %+v; want none", res.Stops, res.Delivered)
	}
	if !res.Terminated {
		t.Error("the run did not end with terminated")
	}
	if asked != 0 || len(res.RestartErrors) != 0 {
		t.Errorf("AfterRun asked %d times, restart errors = %q; want neither", asked, res.RestartErrors)
	}
}

// saveCapture with maxMessages below the batch size writes the kept batch
// to <cwd>/.rpcn-debug/ (AD-11, AD-22): the file is 0600, the folder's
// .gitignore holds *, the Debug Console names the file, and ReadCapture
// returns the kept messages with the batch's total.
func TestSaveCapture(t *testing.T) {
	pipeline := writeYAML(t, `
input:
  generate:
    count: 12
    batch_size: 12
    interval: ""
    mapping: |
      root.n = counter()
      meta source = "save"
pipeline:
  processors:
    - mapping: 'root.doubled = this.n * 2'
output:
  drop: {}
`)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{
		Pipeline: pipeline, Script: dapdrive.Always(dapdrive.Continue),
		Launch: map[string]any{"maxMessages": 10, "saveCapture": "orders.json"},
	})
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(filepath.Dir(pipeline), ".rpcn-debug")
	path := filepath.Join(dir, "orders.json")
	if notes := outputs(res, "Saved the capture to "); len(notes) != 1 || notes[0] != "Saved the capture to "+path+".\n" {
		t.Errorf("save notes = %q, want one naming %s", notes, path)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("capture file mode = %v, want 0600", fi.Mode().Perm())
	}
	if gi, err := os.ReadFile(filepath.Join(dir, ".gitignore")); err != nil || string(gi) != "*\n" {
		t.Errorf(".gitignore = %q, %v; want *", gi, err)
	}
	batch, total, err := engine.ReadCapture(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(batch) != 10 || total != 12 {
		t.Fatalf("ReadCapture = %d of %d messages, want 10 of 12", len(batch), total)
	}
	for i, m := range batch {
		raw, _ := m.AsBytes()
		if want := fmt.Sprintf(`{"n":%d}`, i+1); string(raw) != want {
			t.Errorf("message %d = %s, want %s", i, raw, want)
		}
		if v, ok := m.MetaGetMut("source"); !ok || v != "save" {
			t.Errorf("message %d source = %v, %v", i, v, ok)
		}
	}
	if len(res.Delivered) != 1 || len(res.Delivered[0]) != 10 {
		t.Errorf("delivered = %d batches, want one of 10 messages", len(res.Delivered))
	}
}

// replaySrc is a generate pipeline whose batch carries metadata of several
// types, saved by the replay tests.
const replaySrc = `
input:
  generate:
    count: 4
    batch_size: 4
    interval: ""
    mapping: |
      root.n = counter()
      root.bin = "ÿ"
      meta source = "replay"
      meta n = counter()
      meta f = 1.5
      meta ok = true
      meta obj = {"a": [1, 2.5, "x"], "b": null}
pipeline:
  processors:
    - mapping: 'root.doubled = this.n * 2'
    - mutation: 'meta seen = true'
output:
  drop: {}
`

// saveForReplay runs replaySrc with saveCapture orders.json and returns
// its run and its pipeline path; the file is <dir>/.rpcn-debug/orders.json.
func saveForReplay(t *testing.T) (dapdrive.Result, string) {
	t.Helper()
	pipeline := writeYAML(t, replaySrc)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{
		Pipeline: pipeline, Script: dapdrive.Always(dapdrive.Continue),
		Launch: map[string]any{"saveCapture": "orders.json"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Stops) == 0 || len(res.Delivered) != 1 {
		t.Fatalf("saving run: %d stops, %d delivered batches", len(res.Stops), len(res.Delivered))
	}
	// The comparisons below see the messages, and in the delivered batch
	// their raw bytes and lossless metadata.
	if b := res.Stops[0].Batches; len(b) != 1 || len(b[0]) != 4 {
		t.Fatalf("saving run's first stop = %+v, want one batch of 4 messages", b)
	}
	if d := res.Delivered[0]; len(d) != 4 || d[0].Raw == nil || d[0].Metadata["obj"].Lossless == "" {
		t.Fatalf("saving run's delivered batch = %+v, want 4 messages with raw bytes and lossless metadata", d)
	}
	return res, pipeline
}

// A replay of a saved capture (AD-11) stops first on the saved batch and
// delivers what the saving launch delivered, raw bytes and lossless
// metadata included. The Debug Console names the file, and the session
// ends like a captured run.
func TestReplayRoundTrip(t *testing.T) {
	saved, pipeline := saveForReplay(t)
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{
		Pipeline: pipeline, Script: dapdrive.Always(dapdrive.Continue),
		Launch: map[string]any{"replay": ".rpcn-debug/orders.json"},
	})
	if err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(filepath.Dir(pipeline), ".rpcn-debug", "orders.json")
	if notes := outputs(res, "Replaying "); len(notes) != 1 || notes[0] != "Replaying "+path+": 4 messages; the input is not read.\n" {
		t.Errorf("replay notes = %q", notes)
	}
	if notes := outputs(res, "Waiting for the input"); len(notes) != 0 {
		t.Errorf("waiting notes = %q, want none", notes)
	}
	if len(res.Stops) == 0 || !reflect.DeepEqual(res.Stops[0].Batches, saved.Stops[0].Batches) {
		t.Errorf("first stop batches differ:\n got  %+v\n want %+v", res.Stops[0].Batches, saved.Stops[0].Batches)
	}
	if !reflect.DeepEqual(res.Delivered, saved.Delivered) {
		t.Errorf("delivered differs:\n got  %+v\n want %+v", res.Delivered, saved.Delivered)
	}
	if res.Terminated {
		t.Error("the replay ended with terminated, want the session kept (D5)")
	}
}

// A pipeline whose input is rejected (http_server) replays a saved file:
// it steps, and its address is never bound.
func TestReplayRejectedInput(t *testing.T) {
	saved, pipeline := saveForReplay(t)
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	_ = l.Close()
	server := filepath.Join(filepath.Dir(pipeline), "server.yaml")
	_, processors, _ := strings.Cut(replaySrc, "pipeline:")
	src := "input:\n  http_server:\n    address: " + addr + "\n    path: /post\npipeline:" + processors
	if err := write(server, src); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	var listenErrs []error
	res, err := dapdrive.Run(ctx, dapdrive.Options{
		Pipeline: server,
		Script: func([]dapdrive.Stop) dapdrive.Command {
			l, err := net.Listen("tcp", addr)
			if err == nil {
				_ = l.Close()
			}
			listenErrs = append(listenErrs, err)
			return dapdrive.Continue
		},
		Launch: map[string]any{"replay": ".rpcn-debug/orders.json"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(listenErrs) == 0 {
		t.Fatal("no stop")
	}
	for _, err := range listenErrs {
		if err != nil {
			t.Errorf("listen on %s during a stop: %v, want the address free", addr, err)
		}
	}
	if !reflect.DeepEqual(res.Delivered, saved.Delivered) {
		t.Errorf("delivered differs:\n got  %+v\n want %+v", res.Delivered, saved.Delivered)
	}
}

const editSample = "../../testdata/tracer/edit.yaml"

func strp(s string) *string { return &s }

// runEdits runs pipeline with Always(script), making edits(stops) at each
// stop.
func runEdits(t *testing.T, pipeline string, script dapdrive.Command, edits func([]dapdrive.Stop) []dapdrive.Edit) dapdrive.Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	res, err := dapdrive.Run(ctx, dapdrive.Options{Pipeline: pipeline, Script: dapdrive.Always(script), Edits: edits})
	if err != nil {
		t.Fatal(err)
	}
	return res
}

// atStop returns the Edits func that makes edit at stop n (1-based) only.
func atStop(n int, edit ...dapdrive.Edit) func([]dapdrive.Stop) []dapdrive.Edit {
	return func(stops []dapdrive.Stop) []dapdrive.Edit {
		if len(stops) == n {
			return edit
		}
		return nil
	}
}

// deliveredOne returns the one message of the one delivered batch.
func deliveredOne(t *testing.T, res dapdrive.Result) dapdrive.Message {
	t.Helper()
	if len(res.Delivered) != 1 || len(res.Delivered[0]) != 1 {
		t.Fatalf("delivered = %+v, want one batch of one message", res.Delivered)
	}
	return res.Delivered[0][0]
}

// Edits at the stop before the second mapping (AD-19): the JSON content
// and an int metadata value reach the second mapping, which reads both;
// the first never saw them, and the stop's view shows them.
func TestEditContentAndMetadata(t *testing.T) {
	res := runEdits(t, editSample, dapdrive.Next, atStop(2, dapdrive.Edit{
		Content: strp(`{"first":1,"n":5}`), Meta: map[string]string{"count": "42"},
	}))
	if len(res.EditErrors) != 0 {
		t.Fatalf("edit errors = %q", res.EditErrors)
	}
	if len(res.Stops) < 2 || res.Stops[1].UserPath != "pipeline.processors[1]" {
		t.Fatalf("stops = %q, want the second stop before pipeline.processors[1]", trace(res))
	}
	if got := res.Stops[0].Batches[0][0]; got.Content != `{"n":1}` || got.Metadata["count"] != (dapdrive.Meta{Type: "int", Value: "7"}) {
		t.Errorf("first stop = %+v, want the message as captured", got)
	}
	if got := res.Stops[1].Batches[0][0]; got.Content != `{"first":1,"n":1}` {
		t.Errorf("second stop before the edit = %+v", got)
	}
	edited := res.Stops[1].Edited
	if len(edited) != 1 || edited[0][0].Content != `{"first":1,"n":5}` || edited[0][0].Metadata["count"] != (dapdrive.Meta{Type: "int", Value: "42"}) {
		t.Errorf("second stop after the edit = %+v, want the edits", edited)
	}
	m := deliveredOne(t, res)
	if m.Content != `{"count":42,"first":1,"n":5,"second":50}` {
		t.Errorf("delivered content = %s, want the edits passed through the second mapping", m.Content)
	}
	if got := m.Metadata["count"]; got.Type != "int" || got.Value != "42" {
		t.Errorf("delivered count = %+v, want int 42", got)
	}
	if got := m.Metadata["seen"]; got.Type != "int" || got.Value != "43" {
		t.Errorf("delivered seen = %+v, want int 43 (the edited count + 1)", got)
	}
}

// Refused edits (EDIT_INVALID) change nothing: a value that doesn't parse
// as the key's type and an unknown key.
func TestEditRefused(t *testing.T) {
	res := runEdits(t, editSample, dapdrive.Next, atStop(2,
		dapdrive.Edit{Meta: map[string]string{"count": "4x"}},
		dapdrive.Edit{Meta: map[string]string{"nope": "1"}},
		dapdrive.Edit{Batch: 3, Content: strp("x")},
	))
	if len(res.EditErrors) != 3 || !strings.Contains(res.EditErrors[0], `"count"`) ||
		!strings.Contains(res.EditErrors[1], `"nope"`) || !strings.Contains(res.EditErrors[2], "batch 3") {
		t.Errorf("edit errors = %q, want the bad int, the unknown key and the unknown batch", res.EditErrors)
	}
	if res.Stops[1].Edited != nil {
		t.Errorf("a stop with only refused edits re-read its batches: %+v", res.Stops[1].Edited)
	}
	if m := deliveredOne(t, res); m.Content != `{"count":7,"first":1,"n":1,"second":10}` {
		t.Errorf("delivered = %s, want the batch unedited", m.Content)
	}
}

// A parent frame of a nested stop can't be edited; its top frame can.
func TestEditNonTopFrame(t *testing.T) {
	pipeline := filepath.Join(parents, "branch.yaml")
	edits := func(stops []dapdrive.Stop) []dapdrive.Edit {
		if st := stops[len(stops)-1]; len(st.Frames) == 2 && !st.End {
			return []dapdrive.Edit{{Frame: 1, Content: strp(`{"n":2}`)}}
		}
		return nil
	}
	res := runEdits(t, pipeline, dapdrive.StepIn, edits)
	if len(res.EditErrors) != 1 || !strings.Contains(res.EditErrors[0], "top frame") {
		t.Errorf("edit errors = %q, want the parent frame refused", res.EditErrors)
	}
	sameAsReference(t, pipeline, res.Delivered)
}

// Content shown as base64 (not UTF-8) takes base64 text, and refuses text
// that isn't.
func TestEditBinaryContent(t *testing.T) {
	pipeline := writeYAML(t, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = "/wA=".decode("base64")'
pipeline:
  processors:
    - mutation: 'meta seen = "yes"'
output:
  drop: {}
`)
	for name, tc := range map[string]struct {
		content, want string
		refused       bool
	}{
		"valid":   {content: "aGk=", want: "hi"},
		"invalid": {content: "not base64!", want: "/wA=", refused: true},
	} {
		t.Run(name, func(t *testing.T) {
			res := runEdits(t, pipeline, dapdrive.Continue, atStop(1, dapdrive.Edit{Content: strp(tc.content)}))
			if tc.refused != (len(res.EditErrors) == 1) {
				t.Errorf("edit errors = %q, refused %v", res.EditErrors, tc.refused)
			}
			if got := res.Stops[0].Batches[0][0].Content; got != "/wA=" {
				t.Errorf("stop content = %q, want base64", got)
			}
			if m := deliveredOne(t, res); m.Content != tc.want {
				t.Errorf("delivered content = %q, want %q", m.Content, tc.want)
			}
		})
	}
}

// An edit at the end stop of a branch's list goes to the batches the list
// hands back: the branch's result_map sees it.
func TestEditAtEndStop(t *testing.T) {
	pipeline := filepath.Join(parents, "branch.yaml")
	done := false
	edits := func(stops []dapdrive.Stop) []dapdrive.Edit {
		st := stops[len(stops)-1]
		if done || !st.End || st.UserPath != "pipeline.processors[0].branch.processors" {
			return nil
		}
		done = true
		return []dapdrive.Edit{{Content: strp("99")}}
	}
	res := runEdits(t, pipeline, dapdrive.StepIn, edits)
	if !done || len(res.EditErrors) != 0 {
		t.Fatalf("edited %v, edit errors = %q; stops %q", done, res.EditErrors, trace(res))
	}
	if m := deliveredOne(t, res); m.Content != `{"branched":99,"n":1}` {
		t.Errorf("delivered = %s, want the end-stop edit in result_map", m.Content)
	}
}

// Restart re-steps the batch as captured, without the first run's edits
// (AD-20).
func TestEditThenRestart(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	edited, restarts := false, 0
	res, err := dapdrive.Run(ctx, dapdrive.Options{
		Pipeline: editSample,
		Script:   dapdrive.Always(dapdrive.Next),
		Edits: func(stops []dapdrive.Stop) []dapdrive.Edit {
			if edited || len(stops) != 2 {
				return nil
			}
			edited = true
			return []dapdrive.Edit{{Content: strp(`{"first":1,"n":5}`), Meta: map[string]string{"count": "42"}}}
		},
		AfterRun: func(dapdrive.Result) dapdrive.Command {
			restarts++
			if restarts == 1 {
				return dapdrive.Restart
			}
			return ""
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.EditErrors) != 0 || len(res.RestartErrors) != 0 {
		t.Fatalf("edit errors = %q, restart errors = %q", res.EditErrors, res.RestartErrors)
	}
	if len(res.Delivered) != 2 {
		t.Fatalf("delivered = %+v, want one batch per run", res.Delivered)
	}
	if got := res.Delivered[0][0].Content; got != `{"count":42,"first":1,"n":5,"second":50}` {
		t.Errorf("first run delivered %s, want the edits", got)
	}
	if got := res.Delivered[1][0].Content; got != `{"count":7,"first":1,"n":1,"second":10}` {
		t.Errorf("restarted run delivered %s, want the batch as captured", got)
	}
}
