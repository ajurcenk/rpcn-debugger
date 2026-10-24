package rewrite

import (
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

const outputSrc = `input:
  generate: {mapping: 'root = 1', count: 1}
output:
  label: out
  processors:
    - mapping: 'root = this'
  broker:
    outputs:
      - stdout: {codec: lines}
      - redpanda:
          seed_brokers: [localhost:9092]
          topic: out
        processors:
          - log: {message: hi}
    batching: {count: 2}
output_resources:
  - label: res
    stderr: {}
`

// The step stream's output config per mode (AD-21): the mode only under
// dry-run and none; under real the user's output without its top-level
// processors, with stdout and stderr replaced by drop.
func TestPlanOutput(t *testing.T) {
	cases := map[core.OutputMode]string{
		"":                "rpcn_debug_sink:\n    mode: dry-run\n",
		core.OutputDryRun: "rpcn_debug_sink:\n    mode: dry-run\n",
		core.OutputNone:   "rpcn_debug_sink:\n    mode: none\n",
		core.OutputReal: `rpcn_debug_sink:
    mode: real
    output:
        label: out
        broker:
            outputs:
                - drop: {}
                - redpanda:
                    seed_brokers: ['localhost:9092']
                    topic: out
                  processors:
                    - log: {message: hi}
`,
	}
	for mode, want := range cases {
		t.Run(string(mode), func(t *testing.T) {
			p, err := Rewrite([]byte(outputSrc), Options{Output: mode})
			if err != nil {
				t.Fatal(err)
			}
			if p.Output != want {
				t.Errorf("Output =\n%s\nwant:\n%s", p.Output, want)
			}
			// The output stepper still holds the top-level processors.
			if len(p.Steppers) != 1 || !strings.Contains(p.Steppers[0], "root = this") {
				t.Errorf("Steppers = %q, want the output.processors stepper", p.Steppers)
			}
			// Under real the resource stderr output is replaced too; the
			// other modes carry the resources as today.
			if got := strings.Contains(p.Resources, "stderr"); got != (mode != core.OutputReal) {
				t.Errorf("Resources (mode %q) =\n%s", mode, p.Resources)
			}
		})
	}
}

// The rewrite never edits the parsed tree: a real rewrite and a dry-run one
// of the same source give the same steppers and resources, bar the
// resource stdio replacement.
func TestPlanOutputLeavesTheSourceAlone(t *testing.T) {
	dry, err := Rewrite([]byte(outputSrc), Options{})
	if err != nil {
		t.Fatal(err)
	}
	real, err := Rewrite([]byte(outputSrc), Options{Output: core.OutputReal})
	if err != nil {
		t.Fatal(err)
	}
	if !slices.Equal(dry.Steppers, real.Steppers) || dry.Input != real.Input {
		t.Error("output: real changed the input or the steppers")
	}
	if want := strings.Replace(dry.Resources, "stderr: {}", "drop: {}", 1); real.Resources != want {
		t.Errorf("real Resources =\n%s\nwant:\n%s", real.Resources, want)
	}
}

func TestPlanOutputWarnings(t *testing.T) {
	dryRun := []string{
		"output.broker.outputs[0].stdout (line 9): the stdout output is redirected to dry-run",
		"output.broker.outputs[1].processors (line 13): this processor list is not run in dry-run",
		"output.broker.batching (line 15): output batching isn't applied in dry-run",
		"output_resources[0].stderr (line 18): the stderr output is redirected to dry-run",
	}
	cases := map[core.OutputMode][]string{
		"":              dryRun,
		core.OutputNone: dryRun,
		core.OutputReal: {
			"output: real writes each delivered batch to the broker output at output (line 3)",
			"output.broker.outputs[0].stdout (line 9): the stdout output is replaced by drop under output: real",
			"output.broker.batching (line 15): output batching isn't applied under output: real: the captured batch is written as one batch",
			"output_resources[0].stderr (line 18): the stderr output is replaced by drop under output: real",
		},
	}
	for mode, want := range cases {
		t.Run(string(mode), func(t *testing.T) {
			p, err := Rewrite([]byte(outputSrc), Options{Output: mode})
			if err != nil {
				t.Fatal(err)
			}
			if len(p.Warnings) != len(want) {
				t.Fatalf("Warnings =\n%s\nwant %d", strings.Join(p.Warnings, "\n"), len(want))
			}
			for i, w := range want {
				if !strings.HasPrefix(p.Warnings[i], w) {
					t.Errorf("warning %d = %q, want it to start with %q", i, p.Warnings[i], w)
				}
			}
		})
	}
}

func TestPlanOutputRealWithoutOutput(t *testing.T) {
	p, err := Rewrite([]byte("input:\n  generate: {mapping: 'root = 1'}\n"), Options{Output: core.OutputReal})
	if err != nil {
		t.Fatal(err)
	}
	if p.Output != "rpcn_debug_sink:\n    mode: real\n" {
		t.Errorf("Output = %q", p.Output)
	}
	if len(p.Warnings) != 1 || !strings.Contains(p.Warnings[0], "has nothing to write to") {
		t.Errorf("Warnings = %q", p.Warnings)
	}
}

// The real warning names what is written: nothing when the output is
// empty or only writes to stdio, which is replaced by drop.
func TestPlanOutputRealNothingToWrite(t *testing.T) {
	for name, out := range map[string]string{
		"empty":  "output: {}\n",
		"stdout": "output:\n  stdout: {}\n",
		"file":   "output:\n  file: {path: /dev/stdout}\n",
	} {
		t.Run(name, func(t *testing.T) {
			p, err := Rewrite([]byte("input:\n  generate: {mapping: 'root = 1'}\n"+out), Options{Output: core.OutputReal})
			if err != nil {
				t.Fatal(err)
			}
			all := strings.Join(p.Warnings, "\n")
			if !strings.HasPrefix(all, "output: real has nothing to write to") || strings.Contains(all, "writes each delivered batch") {
				t.Errorf("Warnings = %q, want only that nothing is written", p.Warnings)
			}
		})
	}
	p, err := Rewrite([]byte("input:\n  generate: {mapping: 'root = 1'}\noutput:\n  drop: {}\n"), Options{Output: core.OutputReal})
	if err != nil || len(p.Warnings) != 1 || !strings.Contains(p.Warnings[0], "to the drop output at output (line 3)") {
		t.Errorf("a drop output: %v, %q", err, p.Warnings)
	}
}

// A file output to the process's stdout or stderr is replaced like stdout
// under real, with a warning.
func TestPlanOutputRealStdioFiles(t *testing.T) {
	for path := range stdioPaths {
		src := "input:\n  generate: {mapping: 'root = 1'}\noutput:\n  broker:\n    outputs:\n      - drop: {}\n      - file: {path: " + path + ", codec: lines}\n"
		p, err := Rewrite([]byte(src), Options{Output: core.OutputReal})
		if err != nil {
			t.Fatal(err)
		}
		if strings.Contains(p.Output, "file") || strings.Count(p.Output, "drop: {}") != 2 {
			t.Errorf("%s: Output =\n%s", path, p.Output)
		}
		if !strings.Contains(strings.Join(p.Warnings, "\n"), "output.broker.outputs[1].file (line 7): the file output to "+path+" is replaced by drop under output: real") {
			t.Errorf("%s: Warnings = %q", path, p.Warnings)
		}
	}
	// Any other path is written.
	p, err := Rewrite([]byte("input:\n  generate: {mapping: 'root = 1'}\noutput:\n  file: {path: /tmp/out}\n"), Options{Output: core.OutputReal})
	if err != nil || !strings.Contains(p.Output, "path: /tmp/out") {
		t.Errorf("a plain file output: %v\n%s", err, p.Output)
	}
}

// Every output-level batching field is removed under real, composite
// children included, with one warning each; dry-run keeps them.
func TestPlanOutputRealBatching(t *testing.T) {
	src := `input:
  generate: {mapping: 'root = 1'}
output:
  broker:
    outputs:
      - file:
          path: /tmp/a
          batching: {count: 5}
      - drop: {}
        batching: {count: 3}
    batching: {count: 5}
`
	p, err := Rewrite([]byte(src), Options{Output: core.OutputReal})
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(p.Output, "batching") || !strings.Contains(p.Output, "path: /tmp/a") {
		t.Errorf("Output =\n%s", p.Output)
	}
	var n int
	for _, w := range p.Warnings {
		if strings.Contains(w, "the captured batch is written as one batch") {
			n++
		}
	}
	if n != 3 {
		t.Errorf("Warnings = %q, want 3 batching warnings", p.Warnings)
	}
}

// An output that needs the shared client of the top-level redpanda block
// fails a real launch at its line; dry-run never builds it.
func TestPlanOutputRealSharedClient(t *testing.T) {
	cases := map[string]struct {
		out  string
		line int
		path string
	}{
		"redpanda_common":       {"output:\n  redpanda_common: {topic: out}\n", 4, "output.redpanda_common"},
		"redpanda without seed": {"output:\n  broker:\n    outputs:\n      - redpanda: {topic: out}\n", 6, "output.broker.outputs[0].redpanda"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			src := []byte("input:\n  generate: {mapping: 'root = 1'}\n" + tc.out)
			_, err := Rewrite(src, Options{Output: core.OutputReal})
			var ce core.Error
			if !errors.As(err, &ce) || ce.Code != core.CodePipelineInvalid || ce.Line != tc.line || ce.UserPath != tc.path ||
				!strings.Contains(ce.Message, "shared client of the top-level redpanda block") {
				t.Fatalf("Rewrite = %#v, want PIPELINE_INVALID at %s, line %d", err, tc.path, tc.line)
			}
			if _, err := Rewrite(src, Options{}); err != nil {
				t.Errorf("dry-run Rewrite = %v", err)
			}
		})
	}
	src := "input:\n  generate: {mapping: 'root = 1'}\noutput:\n  redpanda: {seed_brokers: [b], topic: out}\n"
	if _, err := Rewrite([]byte(src), Options{Output: core.OutputReal}); err != nil {
		t.Errorf("redpanda with seed_brokers: %v", err)
	}
}

// Positions in the emitted output config map to the user's file (AD-6),
// and engine paths under the sink's output field translate to output.
func TestPlanOutputPositions(t *testing.T) {
	p, err := Rewrite([]byte(outputSrc), Options{Output: core.OutputReal})
	if err != nil {
		t.Fatal(err)
	}
	line, col := at(t, p.Output, "topic: out")
	fl, fc, up, ok := p.FilePosition(core.PartOutput, line, col)
	if !ok || fl != 12 || fc != 11 || up != "output" {
		t.Errorf("FilePosition(topic) = %d, %d, %q, %v, want 12, 11, output, true", fl, fc, up, ok)
	}
	// The synthesized sink lines map to the output key.
	if fl, _, up, ok := p.FilePosition(core.PartOutput, 1, 1); !ok || fl != 3 || up != "output" {
		t.Errorf("FilePosition(1, 1) = %d, %q, %v, want line 3, output", fl, up, ok)
	}
	if got, ok := p.Translate("root.output.rpcn_debug_sink.output.broker.outputs.1"); !ok || got != "output.broker.outputs[1]" {
		t.Errorf("Translate = %q, %v", got, ok)
	}
	dry, err := Rewrite([]byte(outputSrc), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := dry.Translate("root.output.rpcn_debug_sink.output"); ok {
		t.Error("a dry-run plan translates the sink's output path")
	}
}
