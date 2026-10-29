package engine_test

import (
	"context"
	"errors"
	"fmt"
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
// how many batches the arriver had been delivered at that moment. It fails
// every write when fail is set, answers the first notConnected writes with
// service.ErrNotConnected, and with block set never finishes a write until
// its ctx ends.
type probeOut struct {
	arr          *arriver
	fail         bool
	notConnected int
	block        bool

	mu       sync.Mutex
	writes   []string
	before   []int // len(arr.delivered) at each write
	attempts int
	connects int
	closed   bool
}

func (p *probeOut) Connect(context.Context) error {
	p.mu.Lock()
	p.connects++
	p.mu.Unlock()
	return nil
}

func (p *probeOut) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	p.mu.Lock()
	p.attempts++
	attempt := p.attempts
	p.mu.Unlock()
	if p.block {
		<-ctx.Done()
		return ctx.Err()
	}
	if attempt <= p.notConnected {
		return service.ErrNotConnected
	}
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
	return runProbeWith(t, probePipeline, mode, &probeOut{fail: fail}, 0)
}

// runProbeWith runs src, whose output is probe_out writing to p, under
// mode; a timeout > 0 sets the sink's write deadline.
func runProbeWith(t *testing.T, src string, mode core.OutputMode, p *probeOut, timeout time.Duration) (*probeOut, *arriver, error) {
	t.Helper()
	arr := &arriver{}
	p.arr = arr
	a := engine.New(probeEnv(t, p), testLogger(), load)
	if timeout > 0 {
		a.SetWriteTimeout(timeout)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	opts := core.StartOptions{Pipeline: writePipeline(t, src), Output: mode}
	res, err := a.Check(opts)
	if err != nil {
		t.Fatal(err)
	}
	opts.Prepared = res.Prepared
	return p, arr, a.Start(ctx, opts, arr)
}

// Each mode's batch is reported once, with its outcome (AD-21).
func TestSinkOutcomes(t *testing.T) {
	for mode, want := range map[core.OutputMode]core.Outcome{
		"": core.OutcomeRecorded, core.OutputDryRun: core.OutcomeRecorded,
		core.OutputReal: core.OutcomeWritten, core.OutputNone: core.OutcomeDiscarded,
	} {
		t.Run(string(mode), func(t *testing.T) {
			_, arr, err := runProbe(t, mode, false)
			if err != nil {
				t.Fatal(err)
			}
			if len(arr.outcomes) != 1 || arr.outcomes[0] != want || arr.errs[0] != nil {
				t.Errorf("outcomes = %v, errors = %v; want [%s] and no error", arr.outcomes, arr.errs, want)
			}
		})
	}
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

// wantOutputFailed checks that err is OUTPUT_FAILED at the output key (line
// line) naming
// want, and that the one batch was delivered as failed with that error.
func wantOutputFailed(t *testing.T, arr *arriver, err error, line int, want string) {
	t.Helper()
	var ce core.Error
	if !errors.As(err, &ce) || ce.Code != core.CodeOutputFailed || ce.UserPath != "output" || ce.Line != line || !strings.Contains(ce.Message, want) {
		t.Fatalf("Start = %#v, want OUTPUT_FAILED at output, line %d, naming %q", err, line, want)
	}
	if len(arr.outcomes) != 1 || arr.outcomes[0] != core.OutcomeFailed || arr.errs[0] == nil || !strings.Contains(arr.errs[0].Error(), want) {
		t.Fatalf("outcomes = %v, errors = %v; want one failed batch naming %q", arr.outcomes, arr.errs, want)
	}
}

// A write the output refuses is one failed Delivered and OUTPUT_FAILED.
func TestSinkRealWriteFails(t *testing.T) {
	_, arr, err := runProbe(t, core.OutputReal, true)
	wantOutputFailed(t, arr, err, 8, "probe refused the batch")
}

// A write still unfinished at the deadline (an unreachable broker) is a
// failed Delivered saying it timed out and that the output may still write
// it, and OUTPUT_FAILED. The run ends within the deadline plus the bounded
// close and stop.
func TestSinkRealWriteTimesOut(t *testing.T) {
	start := time.Now()
	_, arr, err := runProbeWith(t, probePipeline, core.OutputReal, &probeOut{block: true}, 200*time.Millisecond)
	wantOutputFailed(t, arr, err, 8, "timed out after 200ms; the output keeps retrying until the session's streams stop, so it may still write the batch")
	if d := time.Since(start); d > 15*time.Second {
		t.Errorf("the run took %v, want it bounded by the deadline and the stop timeouts", d)
	}
}

// An output whose first write answers not-connected, then succeeds, is
// written and reported once.
func TestSinkRealReconnect(t *testing.T) {
	p, arr, err := runProbeWith(t, probePipeline, core.OutputReal, &probeOut{notConnected: 1}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if p.attempts != 2 || len(p.writes) != 1 {
		t.Errorf("attempts = %d, writes = %q; want 2 attempts and one write", p.attempts, p.writes)
	}
	if len(arr.outcomes) != 1 || arr.outcomes[0] != core.OutcomeWritten {
		t.Errorf("outcomes = %v, want one written batch", arr.outcomes)
	}
}

// strictPipeline fails the messages the mapping throws on, under
// error_handling.strict. Its output.processors stepper is passed by when
// the batch's first message failed (the engine's strict rule).
const strictPipeline = `
error_handling:
  strict: true
input:
  generate:
    count: 2
    batch_size: 2
    interval: ""
    mapping: 'root.n = counter()'
pipeline:
  processors:
    - mapping: 'root = if %s { throw("n is " + this.n.string()) } else { this }'
output:
  processors:
    - mapping: 'root = this.merge({"out": true})'
  probe_out: {}
`

// The strict rows: STRICT_REJECTED comes from the errored messages that
// reach the output, and a failed write of the passing ones is OUTPUT_FAILED
// instead (AD-3).
func TestSinkStrict(t *testing.T) {
	t.Run("all failed", func(t *testing.T) {
		p, arr, err := runProbeWith(t, fmt.Sprintf(strictPipeline, "true"), core.OutputReal, &probeOut{}, 0)
		if !core.HasCode(err, core.CodeStrictRejected) {
			t.Fatalf("Start = %v, want STRICT_REJECTED", err)
		}
		if len(arr.delivered) != 0 || len(p.writes) != 0 {
			t.Errorf("delivered = %+v, writes = %q; want nothing", arr.delivered, p.writes)
		}
	})
	t.Run("mixed, write ok", func(t *testing.T) {
		p, arr, err := runProbeWith(t, fmt.Sprintf(strictPipeline, "this.n == 1"), core.OutputReal, &probeOut{}, 0)
		if !core.HasCode(err, core.CodeStrictRejected) {
			t.Fatalf("Start = %v, want STRICT_REJECTED", err)
		}
		if len(p.writes) != 1 || p.writes[0] != `{"n":2}` {
			t.Errorf("writes = %q, want the passing message only", p.writes)
		}
		if len(arr.outcomes) != 1 || arr.outcomes[0] != core.OutcomeWritten || len(arr.delivered[0].Messages) != 1 {
			t.Errorf("outcomes = %v, delivered = %+v; want one written batch of 1", arr.outcomes, arr.delivered)
		}
	})
	t.Run("mixed, later message failed", func(t *testing.T) {
		// The first message passes, so the output.processors stepper runs
		// on the batch and leaves the failed message counted.
		_, arr, err := runProbeWith(t, fmt.Sprintf(strictPipeline, "this.n == 2"), core.OutputReal, &probeOut{}, 0)
		if !core.HasCode(err, core.CodeStrictRejected) {
			t.Fatalf("Start = %v, want STRICT_REJECTED", err)
		}
		if len(arr.outcomes) != 1 || arr.outcomes[0] != core.OutcomeWritten {
			t.Errorf("outcomes = %v, want one written batch", arr.outcomes)
		}
	})
	t.Run("mixed, write fails", func(t *testing.T) {
		_, arr, err := runProbeWith(t, fmt.Sprintf(strictPipeline, "this.n == 1"), core.OutputReal, &probeOut{fail: true}, 0)
		wantOutputFailed(t, arr, err, 13, "probe refused the batch")
	})
	t.Run("none failed", func(t *testing.T) {
		_, arr, err := runProbeWith(t, fmt.Sprintf(strictPipeline, "false"), core.OutputReal, &probeOut{}, 0)
		if err != nil {
			t.Fatalf("Start = %v, want no error", err)
		}
		if len(arr.outcomes) != 1 || arr.outcomes[0] != core.OutcomeWritten {
			t.Errorf("outcomes = %v, want one written batch", arr.outcomes)
		}
	})
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
