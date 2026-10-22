package engine

import (
	"context"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// SinkName is the output every step config ends in (AD-21).
const SinkName = "rpcn_debug_sink"

// registerSink registers rpcn_debug_sink on env, delivering into s.
func registerSink(env *service.Environment, s *sink) error {
	return env.RegisterBatchOutput(SinkName, service.NewConfigSpec().
		Summary("Records the batches a debug session delivers (rpcn-debugger)."),
		func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			// One batch in flight keeps batches exactly as received.
			return s, service.BatchPolicy{}, 1, nil
		})
}

// sink records each batch exactly as the step stream delivers it, after
// output.processors, and reports a deep copy of it through arr (AD-21). It
// only observes: what it receives is acked unchanged (AD-8).
type sink struct {
	arr core.Arriver
	mu  sync.Mutex
	got []service.MessageBatch
}

func (s *sink) Connect(context.Context) error { return nil }

// WriteBatch records b and reports it. MaxInFlight 1 keeps the calls, and
// so the reports, in the order the batches are received.
func (s *sink) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	s.mu.Lock()
	s.got = append(s.got, b.Copy())
	s.mu.Unlock()
	if s.arr != nil {
		// snapshotBatches deep-copies content, metadata and errors (AD-19).
		s.arr.Deliver(ctx, snapshotBatches([]service.MessageBatch{b})[0])
	}
	return nil
}

func (s *sink) Close(context.Context) error { return nil }

func (s *sink) batches() []service.MessageBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]service.MessageBatch(nil), s.got...)
}
