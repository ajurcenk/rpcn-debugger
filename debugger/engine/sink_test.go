package engine

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// flakyOut fails its first Prime, then succeeds.
type flakyOut struct {
	primes, closes int
}

func (f *flakyOut) Prime() error {
	f.primes++
	if f.primes == 1 {
		return errors.New("not yet")
	}
	return nil
}

func (f *flakyOut) WriteBatch(context.Context, service.MessageBatch) error { return nil }

func (f *flakyOut) Close(context.Context) error {
	f.closes++
	return nil
}

// A failed Prime is retried on the next Connect, a primed output is never
// primed again, and Close closes it.
func TestSinkConnectRetriesPrime(t *testing.T) {
	out := &flakyOut{}
	s := &sink{out: out}
	ctx := context.Background()
	if err := s.Connect(ctx); err == nil {
		t.Fatal("first Connect succeeded, want the Prime error")
	}
	if err := s.Connect(ctx); err != nil {
		t.Fatalf("second Connect = %v, want the Prime retried", err)
	}
	if err := s.Connect(ctx); err != nil || out.primes != 2 {
		t.Errorf("third Connect = %v after %d primes, want no new Prime", err, out.primes)
	}
	if err := s.Close(ctx); err != nil || out.closes != 1 {
		t.Errorf("Close = %v, %d closes, want 1", err, out.closes)
	}
}

// Close closes an output whose Prime always failed.
func TestSinkCloseUnprimed(t *testing.T) {
	out := &flakyOut{}
	s := &sink{out: out}
	if err := s.Close(context.Background()); err != nil || out.closes != 1 {
		t.Errorf("Close = %v, %d closes, want 1", err, out.closes)
	}
}

// notConnectedOut answers its first n writes with service.ErrNotConnected,
// then succeeds; with never set, it is never connected.
type notConnectedOut struct {
	n, writes, closes int
	never             bool
}

func (*notConnectedOut) Prime() error { return nil }

func (o *notConnectedOut) WriteBatch(ctx context.Context, _ service.MessageBatch) error {
	o.writes++
	if o.never || o.writes <= o.n {
		if err := ctx.Err(); err != nil {
			return err
		}
		return service.ErrNotConnected
	}
	return nil
}

func (o *notConnectedOut) Close(context.Context) error {
	o.closes++
	return nil
}

// deliveries records the sink's reports.
type deliveries struct {
	nopArriver
	outcomes []core.Outcome
	errs     []error
}

func (d *deliveries) Deliver(_ context.Context, _ core.Batch, o core.Outcome, err error) {
	d.outcomes = append(d.outcomes, o)
	d.errs = append(d.errs, err)
}

// A not-connected write returns service.ErrNotConnected itself, so benthos
// reconnects and retries the batch, and reports nothing: the retry that
// succeeds reports the batch once.
func TestSinkNotConnectedRetries(t *testing.T) {
	d := &deliveries{}
	out := &notConnectedOut{n: 2}
	s := &sink{arr: d, mode: core.OutputReal, out: out}
	b := service.MessageBatch{service.NewMessage([]byte("a"))}
	for i := range 2 {
		if err := s.WriteBatch(context.Background(), b); err != service.ErrNotConnected {
			t.Fatalf("write %d = %v, want service.ErrNotConnected itself", i+1, err)
		}
	}
	if err := s.WriteBatch(context.Background(), b); err != nil {
		t.Fatal(err)
	}
	if len(d.outcomes) != 1 || d.outcomes[0] != core.OutcomeWritten || len(s.batches()) != 1 {
		t.Errorf("outcomes = %v, recorded %d; want one written batch", d.outcomes, len(s.batches()))
	}
}

// The deadline bounds the batch across not-connected retries: once it has
// passed, the batch fails as timed out, and the error doesn't make benthos
// retry again.
func TestSinkDeadlineSpansRetries(t *testing.T) {
	d := &deliveries{}
	out := &notConnectedOut{never: true}
	s := &sink{arr: d, mode: core.OutputReal, out: out, timeout: 50 * time.Millisecond}
	b := service.MessageBatch{service.NewMessage([]byte("a"))}
	var err error
	for start := time.Now(); time.Since(start) < 5*time.Second; {
		if err = s.WriteBatch(context.Background(), b); !errors.Is(err, service.ErrNotConnected) {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	want := "timed out after 50ms; the output keeps retrying until the session's streams stop, so it may still write the batch"
	if err == nil || errors.Is(err, service.ErrNotConnected) || !strings.Contains(err.Error(), want) {
		t.Fatalf("WriteBatch = %v, want a timeout that is not ErrNotConnected, saying %q", err, want)
	}
	if len(d.outcomes) != 1 || d.outcomes[0] != core.OutcomeFailed || s.failure() == nil {
		t.Errorf("outcomes = %v, failure = %v; want one failed batch", d.outcomes, s.failure())
	}
	if out.closes != 1 {
		t.Errorf("the output was closed %d times at the deadline, want 1", out.closes)
	}
	if len(s.batches()) != 0 {
		t.Errorf("recorded %d batches, want the failed one left out", len(s.batches()))
	}
	if err := s.Close(context.Background()); err != nil || out.closes != 1 {
		t.Errorf("Close = %v after %d closes, want the output closed only once", err, out.closes)
	}
}
