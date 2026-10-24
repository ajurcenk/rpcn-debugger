package engine

import (
	"context"
	"errors"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"
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
