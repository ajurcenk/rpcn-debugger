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
			got := render(Rewrite(raw))
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

func TestPosition(t *testing.T) {
	p, err := Rewrite([]byte("input:\n  generate: {mapping: 'root = 1'}\npipeline:\n  processors:\n    - mapping: 'root = 2'\n"))
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
		_, err := Rewrite([]byte(src))
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
	p, err := Rewrite(raw)
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
