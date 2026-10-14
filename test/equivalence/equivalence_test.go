package equivalence

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"
)

// corpus is the AD-8 corpus, relative to this package.
const corpus = "../../testdata/equivalence"

func writeFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func TestCasesDiscoversInNameOrder(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "b", PipelineFile), "")
	writeFile(t, filepath.Join(dir, "a", PipelineFile), "")
	writeFile(t, filepath.Join(dir, "README.md"), "stray")
	writeFile(t, filepath.Join(dir, "z.yaml"), "stray")

	got, err := Cases(dir)
	if err != nil {
		t.Fatal(err)
	}
	want := []Case{
		{Name: "a", Dir: filepath.Join(dir, "a"), Pipeline: filepath.Join(dir, "a", PipelineFile)},
		{Name: "b", Dir: filepath.Join(dir, "b"), Pipeline: filepath.Join(dir, "b", PipelineFile)},
	}
	if len(got) != len(want) {
		t.Fatalf("got %d cases %+v, want %+v", len(got), got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("case %d: got %+v, want %+v", i, got[i], want[i])
		}
	}
}

func TestCasesMissingPipeline(t *testing.T) {
	dir := t.TempDir()
	writeFile(t, filepath.Join(dir, "a", PipelineFile), "")
	writeFile(t, filepath.Join(dir, "c", "notes.txt"), "no pipeline here")

	got, err := Cases(dir)
	if err == nil {
		t.Fatalf("got cases %+v, want an error", got)
	}
	if !strings.Contains(err.Error(), `"c"`) {
		t.Errorf("error %q doesn't name case c", err)
	}
	if got != nil {
		t.Errorf("got cases %+v alongside the error", got)
	}
}

func TestCasesMissingRoot(t *testing.T) {
	got, err := Cases(filepath.Join(t.TempDir(), "nope"))
	if err == nil {
		t.Fatal("want an error for a missing root")
	}
	if len(got) != 0 {
		t.Errorf("got cases %+v, want none", got)
	}
}

func tempCase(t *testing.T, pipeline string) Case {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, PipelineFile)
	writeFile(t, path, pipeline)
	return Case{Name: "tmp", Dir: dir, Pipeline: path}
}

func TestRunReferenceBadPipeline(t *testing.T) {
	for name, conf := range map[string]string{
		"lint":     "input:\n  generate:\n    count: 1\n    interval: \"\"\n    mapping: root = 1\n    nope: true\n",
		"parse":    "input: [unclosed\n",
		"no-input": "pipeline:\n  processors: []\noutput:\n  drop: {}\n",
		"empty":    "",
		"blobl":    "input:\n  generate:\n    count: 1\n    interval: \"\"\n    mapping: root = (\n",
	} {
		t.Run(name, func(t *testing.T) {
			got, err := tempCase(t, conf).RunReference(t.Context())
			if err == nil {
				t.Fatal("want an error")
			}
			t.Log(err)
			if got != nil {
				t.Errorf("got batches %v alongside the error", got)
			}
		})
	}
}

func TestRunReferenceNeverEndingInput(t *testing.T) {
	c := tempCase(t, "input:\n  generate:\n    interval: 10ms\n    mapping: root = \"tick\"\n")
	ctx, cancel := context.WithTimeout(t.Context(), 300*time.Millisecond)
	defer cancel()

	got, err := c.RunReference(ctx)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got error %v, want %v", err, context.DeadlineExceeded)
	}
	if got != nil {
		t.Errorf("got batches %v alongside the error", got)
	}
}

func TestWithReferenceSinkKeepsOutputProcessors(t *testing.T) {
	conf, err := withReferenceSink([]byte("input:\n  generate:\n    mapping: root = 1\noutput:\n  label: out\n  drop: {}\n  processors:\n    - mapping: root = this\n"))
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{sinkName + ": {}", "label: out", "processors:"} {
		if !strings.Contains(conf, want) {
			t.Errorf("config %q lacks %q", conf, want)
		}
	}
	if strings.Contains(conf, "drop") {
		t.Errorf("config %q still holds the original output", conf)
	}
}

// TestCases runs the reference of every case in the corpus, one subtest each.
func TestCases(t *testing.T) {
	cases, err := Cases(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatalf("no cases in %s", corpus)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			batches, err := c.RunReference(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if check, ok := caseChecks[c.Name]; ok {
				check(t, batches)
			}
		})
	}
}

// caseChecks holds extra assertions on a case's reference output.
var caseChecks = map[string]func(*testing.T, []service.MessageBatch){
	"generate-mapping": func(t *testing.T, batches []service.MessageBatch) {
		var got []string
		for _, b := range batches {
			for _, m := range b {
				raw, err := m.AsBytes()
				if err != nil {
					t.Fatal(err)
				}
				got = append(got, string(raw))
			}
		}
		want := []string{`{"doubled":2,"n":1}`, `{"doubled":4,"n":2}`, `{"doubled":6,"n":3}`}
		if strings.Join(got, "\n") != strings.Join(want, "\n") {
			t.Errorf("got messages %q, want %q", got, want)
		}
	},
}
