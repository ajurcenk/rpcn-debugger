package engine_test

import (
	"errors"
	"strings"
	"testing"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/engine"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

// loadMapped is load with the translator and the position map wired, as
// debugger.Serve does.
func loadMapped(path string) (engine.Configs, core.LineMap, error) {
	c, l, err := load(path)
	if err == nil {
		p := l.(rewrite.Plan)
		c.Translator, c.Positions = p, p.FilePosition
	}
	return c, l, err
}

// checkError runs Check on src and returns its error as a core.Error.
func checkError(t *testing.T, src string, loader engine.Loader) core.Error {
	t.Helper()
	_, err := engine.New(standard(), testLogger(), loader).Check(core.StartOptions{Pipeline: writePipeline(t, src)})
	if err == nil {
		t.Fatal("Check passed")
	}
	ce, ok := core.AsError(err, core.CodeRunFailed).(core.Error)
	if !ok || ce.Code != core.CodePipelineInvalid {
		t.Fatalf("Check error = %#v, want PIPELINE_INVALID", err)
	}
	var inner core.Error
	if !errors.As(err, &inner) {
		t.Fatalf("Check error %q wraps no core.Error", err)
	}
	return ce
}

const genInput = `input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = {}'
`

// Each case is one row of the entry's I/O matrix: a lint error lands at
// the file line, under the processor's user path.
func TestCheckErrorPositions(t *testing.T) {
	cases := []struct {
		name     string
		src      string
		path     string
		line     int
		contains []string
		absent   []string
	}{
		{
			name: "bad mapping",
			src: genInput + `
pipeline:
  processors:
    - mapping: 'root = this'
    - mapping: 'root = '
output:
  drop: {}
`,
			path:     "pipeline.processors[1]",
			line:     10,
			contains: []string{"lint errors: pipeline.processors[1]: (line 10, column 24) expected query"},
		},
		{
			name: "nested unknown field",
			src: genInput + `
pipeline:
  processors:
    - branch:
        request_map: 'root = this'
        result_map: 'root.x = this'
        processors:
          - mapping: 'root = this'
            nope: 1
output:
  drop: {}
`,
			path:     "pipeline.processors[0].branch.processors[0]",
			line:     14,
			contains: []string{"pipeline.processors[0].branch.processors[0]: (line 14, column 13) field nope is invalid"},
		},
		{
			name: "bad input field",
			src: `input:
  generate:
    nope: 2
    mapping: 'root = {}'
`,
			path:     "input",
			line:     3,
			contains: []string{"lint errors: input: (line 3, column 5) field nope not recognised"},
		},
		{
			name: "bad resource",
			src: genInput + `
pipeline:
  processors:
    - resource: foo
output:
  drop: {}




processor_resources:
  - label: other
    mapping: 'root = this'
  - label: foo
    mapping: 'root = '
`,
			path:     "processor_resources.foo",
			line:     20,
			contains: []string{"processor_resources.foo: (line 20, column 22) expected query"},
		},
		{
			name: "output processor",
			src: genInput + `output:
  drop: {}
  processors:
    - mapping: 'root ='
`,
			path:     "output.processors[0]",
			line:     9,
			contains: []string{"output.processors[0]: (line 9, column "},
		},
		{
			name: "alias",
			src: genInput + `shared: &bad 'root = '
pipeline:
  processors:
    - mapping: 'root = this'
    - mapping: *bad
output:
  drop: {}
`,
			path:     "pipeline.processors[1]",
			line:     6,
			contains: []string{"pipeline.processors[1]: (line 6, column 22) expected query"},
		},
		{
			name: "several errors",
			src: genInput + `pipeline:
  processors:
    - mapping: 'root = '
    - mapping: 'root = this'
    - mapping: |
        root = this
        root.x =
output:
  drop: {}
`,
			path: "pipeline.processors[0]",
			line: 8,
			contains: []string{
				"lint errors: pipeline.processors[0]: (line 8, column 24) expected query",
				"\npipeline.processors[2]: (line 12, column 17) expected",
			},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			ce := checkError(t, tc.src, loadMapped)
			if ce.UserPath != tc.path || ce.Line != tc.line {
				t.Errorf("UserPath, Line = %q, %d, want %q, %d (%s)", ce.UserPath, ce.Line, tc.path, tc.line, ce.Message)
			}
			for _, s := range tc.contains {
				if !strings.Contains(ce.Message, s) {
					t.Errorf("message %q lacks %q", ce.Message, s)
				}
			}
			if strings.Contains(ce.Message, "rewritten config") || strings.Contains(ce.Message, "stepper") {
				t.Errorf("message %q names the rewritten config", ce.Message)
			}
			// Every location is inline: no suffix, no coarse label.
			if !ce.Inline || strings.Contains(ce.Error(), "(at ") || strings.Contains(ce.Message, ": lint errors:") && !strings.Contains(ce.Message, ".yaml: lint errors:") {
				t.Errorf("error %q repeats its location", ce.Error())
			}
		})
	}
}

// A position the map can't resolve is left as written, with a note.
func TestCheckErrorUnmappedPosition(t *testing.T) {
	src := genInput + `pipeline:
  processors:
    - mapping: 'root = '
`
	none := func(path string) (engine.Configs, core.LineMap, error) {
		c, l, err := loadMapped(path)
		c.Positions = func(string, int, int) (int, int, string, bool) { return 0, 0, "", false }
		return c, l, err
	}
	ce := checkError(t, src, none)
	if ce.Line != 0 || !strings.Contains(ce.Message, "lint errors: (") || !strings.Contains(ce.Message, "refer to the rewritten config") {
		t.Errorf("error = %q (line %d), want the rewritten position and the note", ce.Message, ce.Line)
	}
}

// A YAML parse error from an interpolated value names the file line.
func TestCheckErrorYAMLLine(t *testing.T) {
	src := `input:
  generate:
    count: 1
    mapping: 'root = {}'
    interval: ${RPCN_DEBUG_TEST_BAD}
`
	a := engine.New(standard(), testLogger(), loadMapped)
	_, err := a.Check(core.StartOptions{
		Pipeline: writePipeline(t, src),
		Lookup:   core.EnvLookup(nil, map[string]string{"RPCN_DEBUG_TEST_BAD": "a: b: c"}),
	})
	ce, _ := core.AsError(err, core.CodeRunFailed).(core.Error)
	if ce.Code != core.CodePipelineInvalid || ce.Line != 5 || !strings.Contains(ce.Message, "input: (line 5) yaml: ") {
		t.Errorf("error = %#v, want PIPELINE_INVALID at line 5", err)
	}
}

// A | block with a trailing space is encoded on one double-quoted line, but
// the engine still counts its lines: a position after its first line can't
// be placed in the file, so it is left as written, with the note.
func TestCheckErrorRequotedBlock(t *testing.T) {
	src := genInput + "pipeline:\n  processors:\n    - mapping: |\n        root = this \n        root.x = )\n        root.y = 1\n" +
		"    - mapping: 'root = this'\n    - mapping: 'root.z = 2'\n"
	ce := checkError(t, src, loadMapped)
	if ce.Line != 0 || !strings.Contains(ce.Message, "lint errors: (") || !strings.Contains(ce.Message, "refer to the rewritten config") {
		t.Errorf("error = %q (line %d), want the rewritten position and the note", ce.Message, ce.Line)
	}
}

// The lint items' own text is never rewritten, even where it looks like a
// position at the start of a line.
func TestCheckErrorEchoedPosition(t *testing.T) {
	src := genInput + "pipeline:\n  processors:\n    - mapping: 'root = this'\n      \"a\\n(1,2) b\": 1\n"
	ce := checkError(t, src, loadMapped)
	if !strings.Contains(ce.Message, "\n(1,2) b") || ce.Line != 9 {
		t.Errorf("error = %q (line %d), want the echoed (1,2) kept and line 9", ce.Message, ce.Line)
	}
}

// On a file line with a ${VAR}, the engine lints the substituted value, so
// only the line is given.
func TestCheckErrorInterpolatedLine(t *testing.T) {
	src := genInput + "pipeline:\n  processors:\n    - mapping: 'root = ${RPCN_DEBUG_TEST_MAP}'\n"
	a := engine.New(standard(), testLogger(), loadMapped)
	_, err := a.Check(core.StartOptions{
		Pipeline: writePipeline(t, src),
		Lookup:   core.EnvLookup(nil, map[string]string{"RPCN_DEBUG_TEST_MAP": "this.foo.bar.baz.qux + )"}),
	})
	ce, _ := core.AsError(err, core.CodeRunFailed).(core.Error)
	if ce.Line != 8 || !strings.Contains(ce.Message, "pipeline.processors[0]: (line 8) ") || strings.Contains(ce.Message, "column") {
		t.Errorf("error = %#v, want line 8 without a column", err)
	}
}
