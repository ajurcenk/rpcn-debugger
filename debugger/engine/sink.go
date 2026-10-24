package engine

import (
	"context"
	"fmt"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// SinkName is the output every step config ends in (AD-21).
const SinkName = "rpcn_debug_sink"

// The rpcn_debug_sink fields. The rewrite writes them (AD-21); the engine
// never assembles or parses an output config.
const (
	sinkFieldMode   = "mode"
	sinkFieldOutput = "output"
)

// sinkSpec is rpcn_debug_sink's config spec: the output mode, and under
// output: real the user's output block to write to.
func sinkSpec() *service.ConfigSpec {
	modes := make([]string, len(core.OutputModes))
	for i, m := range core.OutputModes {
		modes[i] = string(m)
	}
	return service.NewConfigSpec().
		Summary("Records the batches a debug session delivers (rpcn-debugger).").
		Fields(
			service.NewStringEnumField(sinkFieldMode, modes...).Default(string(core.OutputDryRun)),
			service.NewOutputField(sinkFieldOutput).Optional(),
		)
}

// registerSink registers rpcn_debug_sink on env, delivering into s. Under
// output: real the constructor builds the configured output into s.
func registerSink(env *service.Environment, s *sink) error {
	return env.RegisterBatchOutput(SinkName, sinkSpec(),
		func(conf *service.ParsedConfig, _ *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			mode, err := conf.FieldString(sinkFieldMode)
			if err != nil {
				return nil, service.BatchPolicy{}, 0, err
			}
			s.mode = core.OutputMode(mode)
			if s.mode == core.OutputReal && conf.Contains(sinkFieldOutput) {
				if s.out, err = conf.FieldOutput(sinkFieldOutput); err != nil {
					return nil, service.BatchPolicy{}, 0, err
				}
			}
			// One batch in flight keeps batches exactly as received, in
			// every mode.
			return s, service.BatchPolicy{}, 1, nil
		})
}

// batchWriter is what the sink writes to under output: real: the user's
// output, as a *service.OwnedOutput.
type batchWriter interface {
	Prime() error
	WriteBatch(ctx context.Context, b service.MessageBatch) error
	Close(ctx context.Context) error
}

var _ batchWriter = (*service.OwnedOutput)(nil)

// sink records each batch exactly as the step stream delivers it, after
// output.processors, and reports a deep copy of it through arr (AD-21).
// Under output: real it first writes the batch to the user's output, out;
// under dry-run and none it only observes, and what it receives is acked
// unchanged (AD-8).
type sink struct {
	arr  core.Arriver
	mode core.OutputMode
	out  batchWriter // nil unless mode is real and there is an output

	primeMu sync.Mutex
	primed  bool

	mu  sync.Mutex
	got []service.MessageBatch
}

// Connect primes the user's output under output: real. A failed Prime is
// retried on the next Connect, until one succeeds.
func (s *sink) Connect(context.Context) error {
	if s.out == nil {
		return nil
	}
	s.primeMu.Lock()
	defer s.primeMu.Unlock()
	if s.primed {
		return nil
	}
	if err := s.out.Prime(); err != nil {
		return err
	}
	s.primed = true
	return nil
}

// WriteBatch records b, writes it to the user's output under output: real
// and then reports it. MaxInFlight 1 keeps the calls, and so the writes and
// reports, in the order the batches are received. A failed write returns
// its error, which fails the run, and reports nothing.
func (s *sink) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	s.mu.Lock()
	s.got = append(s.got, b.Copy())
	s.mu.Unlock()
	// The report is a deep copy taken before the write: the user's output
	// may change the messages it writes (AD-19).
	var report core.Batch
	if s.arr != nil {
		// snapshotBatches deep-copies content, metadata and errors.
		report = snapshotBatches([]service.MessageBatch{b})[0]
	}
	if s.out != nil {
		// The user's output gets its own copy, so the recorded batch stays
		// as received.
		if err := s.out.WriteBatch(ctx, b.Copy()); err != nil {
			return fmt.Errorf("write to the output: %w", err)
		}
	}
	if s.arr != nil {
		s.arr.Deliver(ctx, report)
	}
	return nil
}

// Close closes the user's output under output: real, whenever it was
// built. An unprimed one is primed first if it can be: closing an unprimed
// OwnedOutput only waits for ctx.
func (s *sink) Close(ctx context.Context) error {
	if s.out == nil {
		return nil
	}
	_ = s.Connect(ctx)
	return s.out.Close(ctx)
}

func (s *sink) batches() []service.MessageBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]service.MessageBatch(nil), s.got...)
}
