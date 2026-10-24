package engine_test

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/engine"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

// probeOut is a fake user output: it records each batch it is written, with
// how many batches the arriver had been delivered at that moment, and
// fails every write when fail is set.
type probeOut struct {
	arr  *arriver
	fail bool

	mu     sync.Mutex
	writes []string
	before []int // len(arr.delivered) at each write
	closed bool
}

func (p *probeOut) Connect(context.Context) error { return nil }

func (p *probeOut) WriteBatch(_ context.Context, b service.MessageBatch) error {
	if p.fail {
		return errors.New("probe refused the batch")
	}
	var msgs []string
	for _, m := range b {
		raw, _ := m.AsBytes()
		msgs = append(msgs, string(raw))
	}
	p.arr.mu.Lock()
	n := len(p.arr.delivered)
	p.arr.mu.Unlock()
	p.mu.Lock()
	p.writes = append(p.writes, strings.Join(msgs, " | "))
	p.before = append(p.before, n)
	p.mu.Unlock()
	return nil
}

func (p *probeOut) Close(context.Context) error {
	p.mu.Lock()
	p.closed = true
	p.mu.Unlock()
	return nil
}

// probeEnv returns an environment with the probe_out output writing to p.
func probeEnv(t *testing.T, p *probeOut) *service.Environment {
	t.Helper()
	env := standard()
	err := env.RegisterBatchOutput("probe_out", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			return p, service.BatchPolicy{}, 1, nil
		})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

const probePipeline = `
input:
  generate:
    count: 2
    batch_size: 2
    interval: ""
    mapping: 'root.n = counter()'
output:
  processors:
    - mapping: 'root = this.merge({"seen": this.seen.or(0) + 1})'
  probe_out: {}
`

func runProbe(t *testing.T, mode core.OutputMode, fail bool) (*probeOut, *arriver, error) {
	t.Helper()
	arr := &arriver{}
	p := &probeOut{arr: arr, fail: fail}
	a := engine.New(probeEnv(t, p), testLogger(), load)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	opts := core.StartOptions{Pipeline: writePipeline(t, probePipeline), Output: mode}
	res, err := a.Check(opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.Prepared = res.Prepared
	return p, arr, a.Start(ctx, opts, arr)
}

// Under output: real the sink writes each batch to the user's output, with
// output.processors applied once, and reports it after the write (AD-21).
func TestSinkRealWritesThenDelivers(t *testing.T) {
	p, arr, err := runProbe(t, core.OutputReal, false)
	if err != nil {
		t.Fatal(err)
	}
	want := `{"n":1,"seen":1} | {"n":2,"seen":1}`
	if len(p.writes) != 1 || p.writes[0] != want {
		t.Errorf("writes = %q, want [%s]", p.writes, want)
	}
	if len(p.before) != 1 || p.before[0] != 0 {
		t.Errorf("delivered before the write = %v, want [0]", p.before)
	}
	if len(arr.delivered) != 1 || len(arr.delivered[0].Messages) != 2 {
		t.Fatalf("delivered = %+v, want one batch of 2", arr.delivered)
	}
	if got := string(arr.delivered[0].Messages[0].Content.Raw); got != `{"n":1,"seen":1}` {
		t.Errorf("delivered message 0 = %s", got)
	}
	if !p.closed {
		t.Error("the user's output was not closed")
	}
}

// Under dry-run and none the user's output is never built or written.
func TestSinkDryRunAndNoneWriteNothing(t *testing.T) {
	for _, mode := range []core.OutputMode{"", core.OutputDryRun, core.OutputNone} {
		t.Run(string(mode), func(t *testing.T) {
			p, arr, err := runProbe(t, mode, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(p.writes) != 0 {
				t.Errorf("writes = %q, want none", p.writes)
			}
			if len(arr.delivered) != 1 {
				t.Errorf("delivered %d batches, want 1", len(arr.delivered))
			}
		})
	}
}

// A failed write fails the run as a delivery failure, and reports nothing.
func TestSinkRealWriteFails(t *testing.T) {
	_, arr, err := runProbe(t, core.OutputReal, true)
	if err == nil || !strings.Contains(err.Error(), "deliver the batch") || !strings.Contains(err.Error(), "probe refused the batch") {
		t.Fatalf("Start = %v, want a delivery failure naming the write error", err)
	}
	if len(arr.delivered) != 0 {
		t.Errorf("delivered = %+v after a failed write, want nothing", arr.delivered)
	}
}

// A lint error in the emitted output config names the user's file line.
func TestCheckOutputLintError(t *testing.T) {
	path := writePipeline(t, `input:
  generate: {count: 1, interval: "", mapping: 'root = {}'}
output:
  retry:
    nope: 1
    output: {drop: {}}
`)
	withPos := func(p string, opts engine.LoadOptions) (engine.Configs, core.LineMap, error) {
		c, l, err := load(p, opts)
		if err == nil {
			c.Translator, c.Positions = l.(rewrite.Plan), l.(rewrite.Plan).FilePosition
		}
		return c, l, err
	}
	a := engine.New(standard(), testLogger(), withPos)
	if _, err := a.Check(core.StartOptions{Pipeline: path}); err != nil {
		t.Fatalf("dry-run Check = %v, want it to pass (the user's output is not built)", err)
	}
	_, err := a.Check(core.StartOptions{Pipeline: path, Output: core.OutputReal})
	var ce core.Error
	if !errors.As(err, &ce) || ce.Code != core.CodePipelineInvalid || ce.Line != 5 || ce.UserPath != "output" {
		t.Fatalf("real Check = %#v, want PIPELINE_INVALID at line 5, output", err)
	}
	if !strings.Contains(ce.Message, "nope") || strings.Contains(ce.Message, "rewritten config") {
		t.Errorf("message = %q, want the lint at the file line", ce.Message)
	}
}
