package engine

import (
	"context"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
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

// sink records each batch exactly as the step stream delivers it.
type sink struct {
	mu  sync.Mutex
	got []service.MessageBatch
}

func (s *sink) Connect(context.Context) error { return nil }

func (s *sink) WriteBatch(_ context.Context, b service.MessageBatch) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.got = append(s.got, b.Copy())
	return nil
}

func (s *sink) Close(context.Context) error { return nil }

func (s *sink) batches() []service.MessageBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]service.MessageBatch(nil), s.got...)
}
