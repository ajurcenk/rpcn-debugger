package rewrite

import (
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var update = flag.Bool("update", false, "rewrite the .golden files")

// render prints a Plan (or the error) in the golden format.
func render(p Plan, err error) string {
	if err != nil {
		return "error: " + err.Error() + "\n"
	}
	var b strings.Builder
	fmt.Fprintf(&b, "# input\n%s# resources\n%s# stepper\n%s# children\n", p.Input, p.Resources, p.Stepper)
	for _, c := range p.Children {
		fmt.Fprintf(&b, "%s line %d\n", c.Path, c.Line)
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
	if pos, ok := p.Position(0, 0); !ok || pos.Line != 5 || pos.Path != "pipeline.processors[0]" {
		t.Errorf("Position(0, 0) = %+v, %v", pos, ok)
	}
	for _, at := range [][2]int{{0, 1}, {1, 0}, {0, -1}} {
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
	} {
		if _, err := Rewrite([]byte(src)); err == nil {
			t.Errorf("%s: no error", name)
		}
	}
}
