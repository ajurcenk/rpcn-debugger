package engine

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

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

// writeTimeout is how long, by default, the sink waits for the user's
// output to write one batch under output: real, reconnects included.
const writeTimeout = 30 * time.Second

// sink records each batch exactly as the step stream delivers it, after
// output.processors, and reports a deep copy of it through arr with its
// outcome (AD-21). Under output: real it first writes the batch to the
// user's output, out, within a deadline; under dry-run and none it only
// observes, and what it receives is acked unchanged (AD-8).
type sink struct {
	arr  core.Arriver
	mode core.OutputMode
	out  batchWriter // nil unless mode is real and there is an output
	// timeout bounds each batch's write under output: real; 0 means
	// writeTimeout. Tests shorten it.
	timeout time.Duration

	primeMu sync.Mutex
	primed  bool

	mu  sync.Mutex
	got []service.MessageBatch
	// failed is the write error of the first batch whose outcome was
	// OutcomeFailed, or nil: the run reads it to end with OUTPUT_FAILED.
	failed error
	// started is when the write of the batch in flight began, kept across
	// the retries benthos makes after ErrNotConnected, so the deadline
	// bounds the batch, not each attempt. Zero between batches.
	started time.Time

	closeOnce sync.Once
	closeErr  error
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

// WriteBatch writes b to the user's output under output: real, then records
// it and reports it once, with its outcome. MaxInFlight 1 keeps the calls,
// and so the writes and reports, in the order the batches are received.
//
// A write the output answers with ErrNotConnected returns that error
// unwrapped and records nothing: benthos reconnects and calls WriteBatch
// again with the same batch, so each batch is still reported exactly once.
// A write still unfinished at the deadline, or one that fails, is reported
// as OutcomeFailed and returns its error, which fails the run.
func (s *sink) WriteBatch(ctx context.Context, b service.MessageBatch) error {
	// The report is a deep copy taken before the write: the user's output
	// may change the messages it writes (AD-19).
	var report core.Batch
	if s.arr != nil {
		// snapshotBatches deep-copies content, metadata and errors.
		report = snapshotBatches([]service.MessageBatch{b})[0]
	}
	outcome := core.OutcomeOf(s.mode)
	var werr error
	if s.out != nil {
		var retry bool
		if retry, werr = s.write(ctx, b); retry {
			return service.ErrNotConnected
		}
		if werr != nil {
			outcome = core.OutcomeFailed
		}
	}
	s.mu.Lock()
	if werr == nil {
		// A batch the output failed to write is reported, but isn't counted
		// as delivered.
		s.got = append(s.got, b.Copy())
	} else if s.failed == nil {
		s.failed = werr
	}
	s.mu.Unlock()
	if s.arr != nil {
		s.arr.Deliver(ctx, report, outcome, werr)
	}
	if werr != nil {
		// Not wrapped: an ErrNotConnected in the chain would make benthos
		// retry the batch, and report it again.
		return fmt.Errorf("write to the output: %v", werr)
	}
	return nil
}

// write writes b to the user's output within the batch's deadline. retry is
// true when the output is not connected and time remains: benthos then
// retries the same batch.
func (s *sink) write(ctx context.Context, b service.MessageBatch) (retry bool, err error) {
	timeout := s.timeout
	if timeout <= 0 {
		timeout = writeTimeout
	}
	s.mu.Lock()
	if s.started.IsZero() {
		s.started = time.Now()
	}
	deadline := s.started.Add(timeout)
	s.mu.Unlock()

	wctx, cancel := context.WithDeadline(ctx, deadline)
	defer cancel()
	// The user's output gets its own copy, so the recorded batch stays as
	// received.
	err = s.out.WriteBatch(wctx, b.Copy())
	timedOut := ctx.Err() == nil && wctx.Err() != nil
	if errors.Is(err, service.ErrNotConnected) && !timedOut {
		return true, nil
	}
	s.mu.Lock()
	s.started = time.Time{}
	s.mu.Unlock()
	if err != nil && timedOut {
		// The batch is already queued in the output, which goes on retrying
		// it: close the output now, within stopTimeout, so it takes no
		// more. OwnedOutput.Close doesn't end the write in flight, so the
		// output may still write the batch until the streams stop.
		cctx, ccancel := context.WithTimeout(context.Background(), stopTimeout)
		defer ccancel()
		_ = s.closeOut(cctx)
		err = fmt.Errorf("the write timed out after %v; the output keeps retrying until the session's streams stop, so it may still write the batch", timeout)
	}
	return false, err
}

// failure returns the write error of the first failed batch, or nil.
func (s *sink) failure() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.failed
}

// Close closes the user's output under output: real, whenever it was
// built. An unprimed one is primed first if it can be: closing an unprimed
// OwnedOutput only waits for ctx.
func (s *sink) Close(ctx context.Context) error {
	if s.out == nil {
		return nil
	}
	_ = s.Connect(ctx)
	return s.closeOut(ctx)
}

// closeOut closes the user's output once, within ctx.
func (s *sink) closeOut(ctx context.Context) error {
	s.closeOnce.Do(func() {
		s.closeErr = s.out.Close(ctx)
	})
	return s.closeErr
}

func (s *sink) batches() []service.MessageBatch {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]service.MessageBatch(nil), s.got...)
}
