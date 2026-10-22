package rewrite

import (
	"strings"
	"testing"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// at returns the 1-based line and column of the first occurrence of sub in
// text.
func at(t *testing.T, text, sub string) (int, int) {
	t.Helper()
	for i, l := range strings.Split(text, "\n") {
		if c := strings.Index(l, sub); c >= 0 {
			return i + 1, c + 1
		}
	}
	t.Fatalf("%q not in\n%s", sub, text)
	return 0, 0
}

const positionsSrc = `input:
  generate:
    count: 1
    mapping: 'root = {}'
  processors:
    - mapping: 'root = "in"'
base: &base
  mapping: 'root = "merged"'
bad: &bad 'root = "aliased"'
pipeline:
  processors:
    - mapping: 'root = this'
    - branch:
        processors:
          - mapping: *bad
    - <<: *base
    - mapping: |
        root = this
        root.x = 1
output:
  drop: {}
processor_resources:
  - label: foo
    mapping: 'root = "res"'
cache_resources:
  - label: c
    memory: {}
`

func TestFilePosition(t *testing.T) {
	p, err := Rewrite([]byte(positionsSrc))
	if err != nil {
		t.Fatal(err)
	}
	pipe := p.Steppers[1]
	type want struct {
		line, col int
		path      string
	}
	cases := []struct {
		name, part, text, sub string
		dl, dc                int // offset from sub's position
		want                  want
	}{
		{"input", core.PartInput, p.Input, "count", 0, 0, want{3, 5, "input"}},
		{"input processor", core.StepperPart(0), p.Steppers[0], `'root = "in"'`, 0, 0, want{6, 16, "input.processors[0]"}},
		{"processor key", core.StepperPart(1), pipe, `- mapping: 'root = this'`, 0, 2, want{12, 7, "pipeline.processors[0]"}},
		{"inside a value", core.StepperPart(1), pipe, `'root = this'`, 0, 8, want{12, 24, "pipeline.processors[0]"}},
		{"before a line's first node", core.StepperPart(1), pipe, `'root = this'`, 0, -100, want{12, 5, "pipeline.processors"}},
		{"alias", core.StepperPart(1), pipe, `'root = "aliased"'`, 0, 0, want{9, 11, "pipeline.processors[1].branch.processors[0]"}},
		{"merge", core.StepperPart(1), pipe, `'root = "merged"'`, 0, 0, want{8, 12, "pipeline.processors[2]"}},
		{"block line 2", core.StepperPart(1), pipe, `mapping: |`, 2, 9 + 5, want{19, 13, "pipeline.processors[3]"}},
		{"synthesized wrapper key", core.StepperPart(1), pipe, "mode: all", 0, 0, want{12, 5, "pipeline.processors"}},
		{"synthesized nested wrapper", core.StepperPart(1), pipe, "id: 2", 0, 0, want{15, 11, "pipeline.processors[1].branch.processors"}},
		{"processor resource", core.PartResources, p.Resources, `'root = "res"'`, 0, 0, want{24, 14, "processor_resources.foo"}},
		{"other resource", core.PartResources, p.Resources, "memory", 0, 0, want{27, 5, "cache_resources.c"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l, c := at(t, tc.text, tc.sub)
			fl, fc, up, ok := p.FilePosition(tc.part, l+tc.dl, max(c+tc.dc, 1))
			if got := (want{fl, fc, up}); !ok || got != tc.want {
				t.Errorf("FilePosition(%s, %d, %d) = %+v, %v, want %+v\n%s", tc.part, l+tc.dl, c+tc.dc, got, ok, tc.want, tc.text)
			}
		})
	}
	for _, bad := range []struct {
		part      string
		line, col int
	}{{core.StepperPart(1), 0, 1}, {core.StepperPart(1), 1000, 1}, {core.StepperPart(9), 1, 1}, {"nope", 1, 1}} {
		if _, _, _, ok := p.FilePosition(bad.part, bad.line, bad.col); ok {
			t.Errorf("FilePosition(%s, %d, %d) is ok, want not", bad.part, bad.line, bad.col)
		}
	}
}

// Every encoded node that came from the file maps exactly to its source
// node's position, in every part. Where several nodes start at one encoded
// position (a mapping and its first key), the innermost one wins.
func TestFilePositionExact(t *testing.T) {
	p, err := Rewrite([]byte(positionsSrc))
	if err != nil {
		t.Fatal(err)
	}
	for part, m := range p.positions {
		for i, e := range m.entries {
			if i+1 < len(m.entries) && m.entries[i+1].encLine == e.encLine && m.entries[i+1].encCol == e.encCol {
				continue
			}
			l, c, _, ok := p.FilePosition(part, e.encLine, e.encCol)
			if !ok {
				t.Errorf("%s (%d,%d) does not map", part, e.encLine, e.encCol)
				continue
			}
			wl, wc := e.line, e.col
			if e.block {
				wl, wc = e.offset(0, 1)
			}
			if l != wl || c != wc {
				t.Errorf("%s (%d,%d) = (%d,%d), want (%d,%d)", part, e.encLine, e.encCol, l, c, wl, wc)
			}
		}
	}
}

// A | block encoded on one double-quoted line (it has a trailing space):
// the lines its value spans in the engine's count don't map; the next
// processors' own lines further on still do.
func TestFilePositionRequoted(t *testing.T) {
	src := "input:\n  generate: {count: 1, mapping: 'root = ${X}'}\npipeline:\n  processors:\n" +
		"    - mapping: |\n        root = this \n        root.x = 1\n        root.y = 2\n" +
		"    - mapping: 'root = a'\n    - mapping: 'root = b'\n    - mapping: 'root = c'\n    - mapping: 'root = d'\n"
	p, err := Rewrite([]byte(src))
	if err != nil {
		t.Fatal(err)
	}
	st := p.Steppers[0]
	l, c := at(t, st, `"root = this \nroot.x = 1\nroot.y = 2\n"`)
	if fl, _, up, ok := p.FilePosition(core.StepperPart(0), l, c+3); !ok || fl != 6 || up != "pipeline.processors[0]" {
		t.Errorf("first line = %d %q %v, want 6 pipeline.processors[0]", fl, up, ok)
	}
	for d := 1; d <= 3; d++ {
		if fl, _, _, ok := p.FilePosition(core.StepperPart(0), l+d, 1); ok {
			t.Errorf("line +%d maps to %d, want unmapped", d, fl)
		}
	}
	if fl, _, up, ok := p.FilePosition(core.StepperPart(0), l+4, 30); !ok || fl != 12 || up != "pipeline.processors[4]" {
		t.Errorf("line +4 = %d %q %v, want 12 pipeline.processors[4]", fl, up, ok)
	}
	// A file line with a ${VAR} has no column.
	il, ic := at(t, p.Input, "root = ${X}")
	if fl, fc, _, ok := p.FilePosition(core.PartInput, il, ic+3); !ok || fl != 2 || fc != 0 {
		t.Errorf("interpolated = (%d,%d) %v, want (2,0)", fl, fc, ok)
	}
}
