package rewrite

import (
	"errors"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

var update = flag.Bool("update", false, "rewrite the .golden files")

// render prints a Plan (or the error) in the golden format.
func render(p Plan, err error) string {
	if err != nil {
		var ce core.Error
		if !errors.As(err, &ce) {
			return "error (not a core.Error): " + err.Error() + "\n"
		}
		return fmt.Sprintf("error: %s\ncode: %s\nline: %d\npath: %s\n", ce.Message, ce.Code, ce.Line, ce.UserPath)
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# input\n%s# resources\n%s", p.Input, p.Resources)
	for i, s := range p.Steppers {
		fmt.Fprintf(&b, "# stepper %d\n%s", i, s)
	}
	b.WriteString("# warnings\n")
	for _, w := range p.Warnings {
		fmt.Fprintf(&b, "%s\n", w)
	}
	fmt.Fprintf(&b, "# delivery\n%s line %d\n", p.Delivery.UserPath, p.Delivery.Line)
	b.WriteString("# lines\n")
	keys := make([]stop, 0, len(p.stops))
	for k := range p.stops {
		keys = append(keys, k)
	}
	slices.SortFunc(keys, func(a, b stop) int {
		if a.stepperID != b.stepperID {
			return a.stepperID - b.stepperID
		}
		return a.childIndex - b.childIndex
	})
	for _, k := range keys {
		pos := p.stops[k]
		fmt.Fprintf(&b, "(%d, %d) %s line %d\n", k.stepperID, k.childIndex, pos.UserPath, pos.Line)
	}
	return b.String()
}

func TestGolden(t *testing.T) {
	srcs, err := filepath.Glob(filepath.Join("testdata", "*.yaml"))
	if err != nil || len(srcs) == 0 {
		t.Fatalf("no golden cases: %v", err)
	}
	for _, src := range srcs {
		name := strings.TrimSuffix(filepath.Base(src), ".yaml")
		t.Run(name, func(t *testing.T) {
			raw, err := os.ReadFile(src)
			if err != nil {
				t.Fatal(err)
			}
			got := render(Rewrite(raw, Options{}))
			golden := strings.TrimSuffix(src, ".yaml") + ".golden"
			if *update {
				if err := os.WriteFile(golden, []byte(got), 0o644); err != nil {
					t.Fatal(err)
				}
				return
			}
			want, err := os.ReadFile(golden)
			if err != nil {
				t.Fatalf("%v (run go test -update)", err)
			}
			if got != string(want) {
				t.Errorf("rewrite of %s:\n%s\nwant:\n%s", src, got, want)
			}
		})
	}
}

// The tracer sample must stay in step with its golden copy.
func TestSampleIsTracerPipeline(t *testing.T) {
	a, err := os.ReadFile(filepath.Join("testdata", "sample.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	b, err := os.ReadFile(filepath.Join("..", "..", "testdata", "tracer", "pipeline.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	if string(a) != string(b) {
		t.Error("testdata/sample.yaml differs from testdata/tracer/pipeline.yaml")
	}
}

// The Plan keeps the options it was made with, and they change no output.
func TestPlanOptions(t *testing.T) {
	src := []byte("input:\n  generate: {mapping: 'root = 1'}\npipeline:\n  processors:\n    - mapping: 'root = 2'\n")
	opts := Options{DebugGroup: "rpcn-debug-jane"}
	p, err := Rewrite(src, opts)
	if err != nil {
		t.Fatal(err)
	}
	if p.Options != opts {
		t.Errorf("Plan.Options = %+v, want %+v", p.Options, opts)
	}
	if got, want := render(p, nil), render(Rewrite(src, Options{})); got != want {
		t.Errorf("rewrite with options:\n%s\nwant:\n%s", got, want)
	}
}

func TestPosition(t *testing.T) {
	p, err := Rewrite([]byte("input:\n  generate: {mapping: 'root = 1'}\npipeline:\n  processors:\n    - mapping: 'root = 2'\n"), Options{})
	if err != nil {
		t.Fatal(err)
	}
	if pos, ok := p.Position(0, 0); !ok || pos.Line != 5 || pos.UserPath != "pipeline.processors[0]" {
		t.Errorf("Position(0, 0) = %+v, %v", pos, ok)
	}
	if pos, ok := p.Position(0, 1); !ok || pos.Line != 4 || pos.UserPath != "pipeline.processors" {
		t.Errorf("Position(0, 1) = %+v, %v; want the end-of-entry stop at line 4", pos, ok)
	}
	for _, at := range [][2]int{{0, 2}, {1, 0}, {0, -1}} {
		if _, ok := p.Position(at[0], at[1]); ok {
			t.Errorf("Position(%d, %d) found", at[0], at[1])
		}
	}
}

func TestErrors(t *testing.T) {
	for name, src := range map[string]string{
		"not a mapping":       "- a\n",
		"no input":            "pipeline:\n  processors: []\n",
		"processors not list": "input:\n  generate: {}\npipeline:\n  processors: 3\n",
		"empty":               "",
		"try not list":        "input:\n  generate: {}\npipeline:\n  processors:\n    - try: {a: 1}\n",
		"branch not mapping":  "input:\n  generate: {}\npipeline:\n  processors:\n    - branch: [1]\n",
		"input procs mapping": "input:\n  generate: {}\n  processors: {a: 1}\n",
	} {
		_, err := Rewrite([]byte(src), Options{})
		if !core.HasCode(err, core.CodePipelineInvalid) {
			t.Errorf("%s: error = %v, want PIPELINE_INVALID", name, err)
		}
	}
}

// The nested case: every child of every wrapped parent, plus each entry's
// end-of-entry stop (childIndex = n) at the line of the key owning the list.
func TestNestedLineMap(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join("testdata", "nested.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	p, err := Rewrite(raw, Options{})
	if err != nil {
		t.Fatal(err)
	}
	for _, c := range []struct {
		id, child int
		path      string
		line      int
	}{
		{0, 0, "pipeline.processors[0]", 9},
		{0, 9, "pipeline.processors", 8},
		{1, 0, "pipeline.processors[0].branch.processors[0]", 12},
		{1, 1, "pipeline.processors[0].branch.processors", 11},
		{2, 0, "pipeline.processors[1].switch[0].processors[0]", 17},
		{3, 1, "pipeline.processors[1].switch[1].processors", 18},
		{4, 1, "pipeline.processors[2].try[1]", 22},
		{4, 2, "pipeline.processors[2].try", 20},
		{5, 0, "pipeline.processors[3].catch[0]", 24},
		{6, 1, "pipeline.processors[4].for_each", 26},
		{7, 0, "pipeline.processors[5].while.processors[0]", 31},
		{8, 0, "pipeline.processors[6].group_by[0].processors[0]", 35},
		{9, 1, "pipeline.processors[7].retry.processors", 38},
		{11, 0, "pipeline.processors[8].branch.processors[0].try[0]", 44},
	} {
		pos, ok := p.Position(c.id, c.child)
		if !ok || pos.UserPath != c.path || pos.Line != c.line {
			t.Errorf("Position(%d, %d) = %+v, %v; want %s line %d", c.id, c.child, pos, ok, c.path, c.line)
		}
	}
	for _, at := range [][2]int{{1, 2}, {12, 0}} {
		if _, ok := p.Position(at[0], at[1]); ok {
			t.Errorf("Position(%d, %d) found", at[0], at[1])
		}
	}
	if len(p.Warnings) != 0 {
		t.Errorf("warnings = %q, want none", p.Warnings)
	}
}

// S2: an interpolated error_handling.strict reaches the Plan verbatim (with
// its quoting) and its line, for the engine to resolve; a literal does not.
func TestStrictExpr(t *testing.T) {
	const src = "input:\n  generate: {mapping: 'root = {}'}\nerror_handling:\n  strict: %s\n"
	for _, tc := range []struct {
		strict, expr string
		line         int
		lit          bool
	}{
		{"${STRICT}", "${STRICT}", 4, false},
		{"${STRICT:true}", "${STRICT:true}", 4, false},
		{`"${STRICT}"`, `"${STRICT}"`, 4, false},
		{"\n    # c\n    ${STRICT} # d", "${STRICT}", 6, false},
		{"true", "", 0, true},
		{"false", "", 0, false},
	} {
		p, err := Rewrite(fmt.Appendf(nil, src, tc.strict), Options{})
		if err != nil {
			t.Fatalf("%s: %v", tc.strict, err)
		}
		if p.StrictExpr != tc.expr || p.StrictLine != tc.line || p.Strict != tc.lit {
			t.Errorf("%s: StrictExpr %q line %d Strict %v, want %q line %d Strict %v", tc.strict, p.StrictExpr, p.StrictLine, p.Strict, tc.expr, tc.line, tc.lit)
		}
		if len(p.Warnings) != 0 {
			t.Errorf("%s: warnings %q, want none", tc.strict, p.Warnings)
		}
	}
}

// E3 entry 2: the top-level input's literal consumer_group becomes
// <DebugGroup>-0 in the capture config, with one warning; anything else
// leaves the Plan as it is without options.
func TestDebugGroupOverride(t *testing.T) {
	const opt = "rpcn-debug-jo"
	const warn = `input.%s.consumer_group (line 5): consumer group "prod" is replaced by the debug group "rpcn-debug-jo-0" for the capture`
	pipe := func(in string) string {
		return "input:\n  " + in + ":\n    seed_brokers: [localhost:9092]\n    topics: [t]\n" +
			"pipeline:\n  processors:\n    - mapping: 'root = this'\n"
	}
	withGroup := func(in, group string) string {
		return strings.Replace(pipe(in), "    topics: [t]\n", "    topics: [t]\n    consumer_group: "+group+"\n", 1)
	}
	for _, c := range []struct {
		name, src, group string
		input            string // the component whose group is overridden, or ""
		valueLine        int    // the file line of the group's value
	}{
		{"override", withGroup("redpanda", "prod"), opt, "redpanda", 5},
		// The warning names the key's line, not the value's.
		{"value on its own line", withGroup("redpanda", "\n      prod"), opt, "redpanda", 6},
		{"deprecated input", withGroup("kafka_franz", "prod"), opt, "kafka_franz", 5},
		{"no group", pipe("redpanda"), opt, "", 0},
		{"empty group", withGroup("redpanda", `""`), opt, "", 0},
		{"interpolated group", withGroup("redpanda", "${GROUP}"), opt, "", 0},
		{"no option", withGroup("redpanda", "prod"), "", "", 0},
	} {
		t.Run(c.name, func(t *testing.T) {
			src := []byte(c.src)
			p, err := Rewrite(src, Options{DebugGroup: c.group})
			if err != nil {
				t.Fatal(err)
			}
			base, err := Rewrite(src, Options{})
			if err != nil {
				t.Fatal(err)
			}
			if c.input == "" {
				if got, want := render(p, nil), render(base, nil); got != want {
					t.Errorf("rewrite:\n%s\nwant (as without options):\n%s", got, want)
				}
				return
			}
			want := strings.Replace(base.Input, "consumer_group: prod", "consumer_group: rpcn-debug-jo-0", 1)
			if want == base.Input {
				t.Fatalf("no consumer_group in the capture config:\n%s", base.Input)
			}
			if p.Input != want {
				t.Errorf("capture config:\n%s\nwant:\n%s", p.Input, want)
			}
			if w := fmt.Sprintf(warn, c.input); !slices.Equal(p.Warnings, append(slices.Clone(base.Warnings), w)) {
				t.Errorf("warnings = %q, want %q plus %q", p.Warnings, base.Warnings, w)
			}
			if !slices.Equal(p.Steppers, base.Steppers) || p.Resources != base.Resources {
				t.Error("the override changed the step config")
			}
			// A lint error at the replaced value still names the user's line.
			encLine := strings.Split(p.Input, "\n")
			enc := slices.IndexFunc(encLine, func(l string) bool {
				return strings.Contains(l, "consumer_group:")
			})
			col := strings.Index(encLine[enc], "rpcn-debug-jo-0") + 1
			if line, _, _, ok := p.FilePosition(core.PartInput, enc+1, col); !ok || line != c.valueLine {
				t.Errorf("FilePosition(input, %d, %d) = line %d, %v; want line %d", enc+1, col, line, ok, c.valueLine)
			}
		})
	}
}
