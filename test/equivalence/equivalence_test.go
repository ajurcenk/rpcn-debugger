package equivalence

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/test/dapdrive"
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
	conf, err := referenceConfig([]byte("input:\n  generate:\n    mapping: root = 1\noutput:\n  label: out\n  drop: {}\n  processors:\n    - mapping: root = this\n"))
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

// ExpectFile is the name of a case's expectations, beside its pipeline.
const ExpectFile = "expect.yaml"

// expect is a case's expectations: what its reference run delivers and what
// the debugger runs must show, beyond equality with the reference.
type expect struct {
	// Batches are the reference run's per-batch message counts.
	Batches []int `yaml:"batches"`
	// Messages are the reference run's messages, in order across batches.
	Messages []struct {
		Content string `yaml:"content"`
		Error   string `yaml:"error"`
	} `yaml:"messages"`
	// Stops are the stops the Step In run must make: a user path, with the
	// stop's frame count.
	Stops []struct {
		Path   string `yaml:"path"`
		Frames int    `yaml:"frames"`
	} `yaml:"stops"`
	// NoStopsUnder are user paths no run may stop at or inside.
	NoStopsUnder []string `yaml:"no_stops_under"`
	// Output are substrings that every run's Debug Console output must
	// hold.
	Output []string `yaml:"output"`
}

func loadExpect(t *testing.T, c Case) expect {
	t.Helper()
	raw, err := os.ReadFile(filepath.Join(c.Dir, ExpectFile))
	if err != nil {
		t.Fatalf("case %s: every corpus case declares its expectations: %v", c.Name, err)
	}
	var e expect
	dec := yaml.NewDecoder(strings.NewReader(string(raw)))
	dec.KnownFields(true)
	if err := dec.Decode(&e); err != nil {
		t.Fatalf("case %s: %s: %v", c.Name, ExpectFile, err)
	}
	if len(e.Batches) == 0 {
		t.Fatalf("case %s: %s declares no batches", c.Name, ExpectFile)
	}
	return e
}

// caseEnv sets EQ_CASE_DIR, which a case's pipeline uses to name its files,
// to the case's absolute directory.
func caseEnv(t *testing.T, c Case) {
	t.Helper()
	dir, err := filepath.Abs(c.Dir)
	if err != nil {
		t.Fatal(err)
	}
	t.Setenv("EQ_CASE_DIR", dir)
}

// checkReference checks a reference run against the case's declared shape.
func checkReference(e expect, batches []service.MessageBatch) error {
	var errs []error
	counts := make([]int, len(batches))
	type msg struct{ content, err string }
	var got []msg
	for i, b := range batches {
		counts[i] = len(b)
		for _, m := range b {
			raw, err := m.AsBytes()
			if err != nil {
				return err
			}
			e := ""
			if err := m.GetError(); err != nil {
				e = err.Error()
			}
			got = append(got, msg{string(raw), e})
		}
	}
	if fmt.Sprint(counts) != fmt.Sprint(e.Batches) {
		errs = append(errs, fmt.Errorf("reference batches hold %v messages, want %v", counts, e.Batches))
	}
	if len(e.Messages) > 0 {
		var want []msg
		for _, m := range e.Messages {
			want = append(want, msg{m.Content, m.Error})
		}
		if fmt.Sprintf("%q", got) != fmt.Sprintf("%q", want) {
			errs = append(errs, fmt.Errorf("reference messages %q, want %q", got, want))
		}
	}
	return errors.Join(errs...)
}

// TestCases runs the reference of every case in the corpus, one subtest
// each, and checks it against the case's declared shape.
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
			e := loadExpect(t, c)
			caseEnv(t, c)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			batches, err := c.RunReference(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := checkReference(e, batches); err != nil {
				t.Errorf("case %s: %v", c.Name, err)
			}
		})
	}
}

// runs are the scripted debugger runs of every case (AD-8): Continue at the
// entry stop, Step Over at every stop, and Step In at every stop (which
// covers the nested pause paths).
var runs = []struct {
	name string
	cmd  dapdrive.Command
}{
	{"continue", dapdrive.Continue},
	{"next", dapdrive.Next},
	{"stepIn", dapdrive.StepIn},
}

// TestEquivalence runs every corpus case through the debugger three times
// (see runs) and checks each run against the case's reference (AD-8): the
// same delivered batches, the same-batch rule, no CTX_LOST, and the case's
// declared expectations (see expect), whose Step In stops prove ctx reaches
// each nested child (AD-17).
func TestEquivalence(t *testing.T) {
	cases, err := Cases(corpus)
	if err != nil {
		t.Fatal(err)
	}
	if len(cases) == 0 {
		t.Fatalf("no cases in %s", corpus)
	}
	for _, c := range cases {
		t.Run(c.Name, func(t *testing.T) {
			e := loadExpect(t, c)
			caseEnv(t, c)
			ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
			defer cancel()
			ref, err := c.Reference(ctx)
			if err != nil {
				t.Fatal(err)
			}
			if err := checkReference(e, ref.Batches); err != nil {
				t.Errorf("case %s: %v", c.Name, err)
			}
			for _, r := range runs {
				t.Run(r.name, func(t *testing.T) {
					var log strings.Builder
					res, err := dapdrive.Run(ctx, dapdrive.Options{
						Pipeline:  c.Pipeline,
						Script:    dapdrive.Always(r.cmd),
						LogOutput: &syncWriter{b: &log},
					})
					if err != nil {
						t.Fatalf("case %s, run %s: %v", c.Name, r.name, err)
					}
					if err := checkRun(e, ref, res, log.String(), r.cmd); err != nil {
						t.Errorf("case %s, run %s: %v", c.Name, r.name, err)
					}
				})
			}
		})
	}
}

// under reports whether path is p or inside it.
func under(path, p string) bool {
	return path == p || strings.HasPrefix(path, p+".") || strings.HasPrefix(path, p+"[")
}

// checkRun checks one debugger run of a case against its reference and its
// expectations.
func checkRun(e expect, ref Reference, res dapdrive.Result, log string, cmd dapdrive.Command) error {
	var errs []error
	if err := CheckSameBatch(ref, res.Output); err != nil {
		errs = append(errs, err)
	}
	if err := Compare(ref.Batches, res.Delivered); err != nil {
		errs = append(errs, err)
	}
	if strings.Contains(log, string(core.CodeCtxLost)) {
		errs = append(errs, fmt.Errorf("the log holds %s:\n%s", core.CodeCtxLost, log))
	}
	output := strings.Join(res.Output, "")
	if strings.Contains(output, string(core.CodeCtxLost)) {
		errs = append(errs, fmt.Errorf("the Debug Console shows %s:\n%s", core.CodeCtxLost, output))
	}
	// A run that behaves as in production never fails (entry 2.14: a strict
	// output rejection is an expected outcome, not a failure).
	if strings.Contains(output, "The debug run failed") {
		errs = append(errs, fmt.Errorf("the Debug Console shows a run failure:\n%s", output))
	}
	// Benthos's strict rejection record names the internal sink: it stays
	// on stderr, and the Session's strict note is the one console line.
	const reject = "for output 'rpcn_debug_sink'"
	if strings.Contains(output, reject) {
		errs = append(errs, fmt.Errorf("the Debug Console shows benthos's strict rejection:\n%s", output))
	}
	if strings.Contains(output, "the output rejected the messages") && !strings.Contains(log, reject) {
		errs = append(errs, fmt.Errorf("the log lacks benthos's strict rejection:\n%s", log))
	}
	if len(res.Stops) == 0 || res.Stops[0].Reason != "entry" {
		errs = append(errs, fmt.Errorf("the first stop is not the entry stop: %+v", res.Stops))
	}
	if cmd == dapdrive.Continue && len(res.Stops) != 1 {
		errs = append(errs, fmt.Errorf("Continue stopped %d times, want only the entry stop", len(res.Stops)))
	}
	for _, s := range res.Stops {
		for _, p := range e.NoStopsUnder {
			if under(s.UserPath, p) {
				errs = append(errs, fmt.Errorf("stopped at %s, inside %s", s.UserPath, p))
			}
		}
	}
	for _, o := range e.Output {
		if !strings.Contains(output, o) {
			errs = append(errs, fmt.Errorf("the Debug Console output lacks %q:\n%s", o, output))
		}
	}
	if cmd == dapdrive.StepIn {
		for _, want := range e.Stops {
			found := false
			for _, s := range res.Stops {
				if s.UserPath == want.Path && len(s.Frames) == want.Frames {
					found = true
					break
				}
			}
			if !found {
				errs = append(errs, fmt.Errorf("Step In never stopped at %s with %d frames: ctx didn't reach the child (AD-17)", want.Path, want.Frames))
			}
		}
	}
	return errors.Join(errs...)
}

// syncWriter serializes writes to b: Serve logs from several goroutines.
type syncWriter struct {
	mu sync.Mutex
	b  *strings.Builder
}

func (w *syncWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.b.Write(p)
}

// refBatches builds reference batches from contents, one inner slice per
// batch; each message has the int metadata k = 1.
func refBatches(batches ...[]string) []service.MessageBatch {
	out := []service.MessageBatch{}
	for _, b := range batches {
		mb := service.MessageBatch{}
		for _, c := range b {
			m := service.NewMessage([]byte(c))
			m.MetaSetMut("k", int64(1))
			mb = append(mb, m)
		}
		out = append(out, mb)
	}
	return out
}

// gotBatches builds delivered batches the way dapdrive decodes them.
func gotBatches(batches ...[]string) []dapdrive.Batch {
	out := []dapdrive.Batch{}
	for _, b := range batches {
		db := dapdrive.Batch{}
		for _, c := range b {
			db = append(db, dapdrive.Message{Content: c, Raw: []byte(c), Metadata: map[string]dapdrive.Meta{"k": {Type: "int", Value: "1", Lossless: "1"}}})
		}
		out = append(out, db)
	}
	return out
}

func TestCompareEqual(t *testing.T) {
	if err := Compare(refBatches([]string{"a", "b"}, []string{"c"}), gotBatches([]string{"a", "b"}, []string{"c"})); err != nil {
		t.Error(err)
	}
	if err := Compare(nil, nil); err != nil {
		t.Error(err)
	}
}

// Injected faults: each one fails, naming the difference.
func TestCompareFaults(t *testing.T) {
	ref := refBatches([]string{"a", "b"}, []string{"c"})
	withMsg := func(f func(g []dapdrive.Batch)) []dapdrive.Batch {
		g := gotBatches([]string{"a", "b"}, []string{"c"})
		f(g)
		return g
	}
	for name, c := range map[string]struct {
		ref  []service.MessageBatch
		got  []dapdrive.Batch
		want string
	}{
		"content":                       {ref, gotBatches([]string{"a", "B"}, []string{"c"}), `batch 0 message 1: content "B", want "b"`},
		"boundaries":                    {ref, gotBatches([]string{"a"}, []string{"b", "c"}), "batch 0 holds 1 messages, want 2"},
		"order":                         {ref, gotBatches([]string{"c"}, []string{"a", "b"}), "batch 0 holds 1 messages, want 2"},
		"missing":                       {ref, gotBatches([]string{"a", "b"}), "1 batches, want 2"},
		"no batches vs one empty batch": {refBatches(), gotBatches([]string{}), "1 batches, want 0"},
		"one empty batch vs none":       {refBatches([]string{}), gotBatches(), "0 batches, want 1"},
		// Joined with the old separators, these rendered the same.
		"separator in content": {refBatches([]string{"a error= meta={k:int=1} | b"}), gotBatches([]string{"a", "b"}), "batch 0 holds 2 messages, want 1"},
		"newline in content":   {refBatches([]string{"a error= meta={k:int=1}\nb"}), gotBatches([]string{"a"}, []string{"b"}), "2 batches, want 1"},
		"metadata value": {ref, withMsg(func(g []dapdrive.Batch) {
			g[1][0].Metadata["k"] = dapdrive.Meta{Type: "int", Value: "2", Lossless: "2"}
		}), `batch 1 message 0: metadata "k" value "2", want "1"`},
		"metadata type": {ref, withMsg(func(g []dapdrive.Batch) {
			g[1][0].Metadata["k"] = dapdrive.Meta{Type: "string", Value: "1", Lossless: "1"}
		}), `metadata "k" type string, want int`},
		"metadata key": {ref, withMsg(func(g []dapdrive.Batch) {
			g[1][0].Metadata["extra"] = dapdrive.Meta{Type: "string", Value: "x", Lossless: "x"}
		}), `metadata "extra" unexpected`},
		"metadata missing": {ref, withMsg(func(g []dapdrive.Batch) {
			delete(g[1][0].Metadata, "k")
		}), `metadata "k" missing`},
		"error": {ref, withMsg(func(g []dapdrive.Batch) {
			g[0][1].Error = "boom"
		}), `batch 0 message 1: error "boom", want ""`},
	} {
		t.Run(name, func(t *testing.T) {
			err := Compare(c.ref, c.got)
			if err == nil {
				t.Fatal("want a difference")
			}
			if !strings.Contains(err.Error(), c.want) {
				t.Errorf("error %q lacks %q", err, c.want)
			}
		})
	}
}

// Content compares as raw bytes: text equal to the base64 of the reference's
// bytes differs. Bytes metadata compares by its lossless (base64) value.
func TestCompareRaw(t *testing.T) {
	raw := []byte{0xff, 0x00}
	b64 := base64.StdEncoding.EncodeToString(raw)
	m := service.NewMessage(raw)
	ref := []service.MessageBatch{{m}}
	if err := Compare(ref, []dapdrive.Batch{{{Content: b64, Base64: true, Raw: raw, Metadata: map[string]dapdrive.Meta{}}}}); err != nil {
		t.Errorf("the same bytes: %v", err)
	}
	if err := Compare(ref, []dapdrive.Batch{{{Content: b64, Raw: []byte(b64), Metadata: map[string]dapdrive.Meta{}}}}); err == nil {
		t.Error("text equal to the base64 of the reference's bytes compared equal")
	}

	mb := service.NewMessage([]byte("x"))
	mb.MetaSetMut("b", raw)
	ref = []service.MessageBatch{{mb}}
	got := func(lossless string) []dapdrive.Batch {
		return []dapdrive.Batch{{{Content: "x", Raw: []byte("x"), Metadata: map[string]dapdrive.Meta{"b": {Type: "bytes", Value: lossless, Lossless: lossless}}}}}
	}
	if err := Compare(ref, got(b64)); err != nil {
		t.Errorf("the same bytes metadata: %v", err)
	}
	if err := Compare(ref, got(base64.StdEncoding.EncodeToString([]byte(b64)))); err == nil {
		t.Error("bytes metadata holding the base64 text compared equal")
	}
}

// refMeta matches engine.metaEntry on json.Number and NaN/Inf inside
// objects.
func TestRefMeta(t *testing.T) {
	for _, c := range []struct {
		in   any
		want meta
	}{
		{json.Number("12"), meta{"int", "12"}},
		{json.Number("1.5"), meta{"float", "1.5"}},
		{map[string]any{"x": math.NaN()}, meta{"object", `{"x":"NaN"}`}},
		{[]any{math.Inf(1)}, meta{"array", `["+Inf"]`}},
		{[]byte("hi"), meta{"bytes", "aGk="}},
		{int32(7), meta{"int", "7"}},
	} {
		if got := refMeta(c.in); got != c.want {
			t.Errorf("refMeta(%#v) = %+v, want %+v", c.in, got, c.want)
		}
	}
}

// backoff_duration is compared by key and type only.
func TestCompareJittered(t *testing.T) {
	m := service.NewMessage([]byte("a"))
	m.MetaSetMut("backoff_duration", int64(3))
	got := []dapdrive.Batch{{{Content: "a", Raw: []byte("a"), Metadata: map[string]dapdrive.Meta{"backoff_duration": {Type: "int", Value: "7", Lossless: "7"}}}}}
	if err := Compare([]service.MessageBatch{{m}}, got); err != nil {
		t.Error(err)
	}
	got[0][0].Metadata["backoff_duration"] = dapdrive.Meta{Type: "string", Value: "3", Lossless: "3"}
	if err := Compare([]service.MessageBatch{{m}}, got); err == nil {
		t.Error("a backoff_duration of another type compared equal")
	}
}

func TestCheckSameBatch(t *testing.T) {
	for name, c := range map[string]struct {
		ref    Reference
		output []string
		want   string
	}{
		"ok":                      {Reference{InputBatches: []int{10}}, []string{"Delivered batch 1\n"}, ""},
		"truncated":               {Reference{InputBatches: []int{12}}, []string{"captured 10 of 12 messages (maxMessages)\n"}, "at most 10"},
		"truncated debugger only": {Reference{InputBatches: []int{3}}, []string{"captured 2 of 3 messages (maxMessages)\n"}, "truncated"},
		"two batches":             {Reference{InputBatches: []int{1, 1}}, nil, "yielded 2 batches"},
		"no batch":                {Reference{}, nil, "yielded 0 batches"},
	} {
		t.Run(name, func(t *testing.T) {
			err := CheckSameBatch(c.ref, c.output)
			switch {
			case c.want == "" && err != nil:
				t.Errorf("got %v, want no error", err)
			case c.want != "" && (err == nil || !strings.Contains(err.Error(), c.want)):
				t.Errorf("got %v, want an error with %q", err, c.want)
			}
		})
	}
}

// The reference counts the input plugin's batches, before input.processors.
func TestReferenceCountsInputBatches(t *testing.T) {
	for name, c := range map[string]struct {
		conf string
		want []int
	}{
		"one batch":   {"input:\n  generate:\n    count: 3\n    batch_size: 3\n    interval: \"\"\n    mapping: root = counter()\n  processors:\n    - split: {size: 1}\n", []int{3}},
		"two batches": {"input:\n  generate:\n    count: 2\n    interval: \"\"\n    mapping: root = counter()\n", []int{1, 1}},
		"twelve":      {"input:\n  generate:\n    count: 12\n    batch_size: 12\n    interval: \"\"\n    mapping: root = counter()\n", []int{12}},
	} {
		t.Run(name, func(t *testing.T) {
			ref, err := tempCase(t, c.conf).Reference(t.Context())
			if err != nil {
				t.Fatal(err)
			}
			if fmt.Sprint(ref.InputBatches) != fmt.Sprint(c.want) {
				t.Errorf("input batches %v, want %v", ref.InputBatches, c.want)
			}
			if err := CheckSameBatch(ref, nil); (err == nil) != (len(c.want) == 1 && c.want[0] <= MaxMessages) {
				t.Errorf("CheckSameBatch = %v", err)
			}
		})
	}
}

// The same-batch rule, end to end: a 12-message input breaks it on both
// sides (the reference batch size and the debugger's truncation line).
func TestSameBatchRuleTruncated(t *testing.T) {
	c := tempCase(t, "input:\n  generate:\n    count: 12\n    batch_size: 12\n    interval: \"\"\n    mapping: root = counter()\n")
	ref, err := c.Reference(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	res, err := dapdrive.Run(t.Context(), dapdrive.Options{Pipeline: c.Pipeline, Script: dapdrive.Always(dapdrive.Continue)})
	if err != nil {
		t.Fatal(err)
	}
	err = CheckSameBatch(ref, res.Output)
	if err == nil || !strings.Contains(err.Error(), "at most 10") || !strings.Contains(err.Error(), "captured 10 of 12") {
		t.Errorf("CheckSameBatch = %v, want both rule failures", err)
	}
}
