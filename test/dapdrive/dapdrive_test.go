package dapdrive_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

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
