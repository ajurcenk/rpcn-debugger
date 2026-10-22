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

// errNotAcked is what the capture consumer returns for every batch, so the
// input sees a nack and never an ack (AD-4, AD-5).
var errNotAcked = errors.New("rpcn-debug captures batches without acknowledging them")

// configureCapture sets up, without building, the capture stream: the
// user's input block, then a consumer func that keeps the first batch it
// receives in got and nacks every batch. The Add* calls parse and lint the
// config, so Check uses this alone: Build would open every resource.
func (r *run) configureCapture(got chan<- service.MessageBatch) (*service.StreamBuilder, error) {
	b := r.builder()
	if r.conf.Resources != "" {
		if err := b.AddResourcesYAML(r.conf.Resources); err != nil {
			return nil, r.configError(core.PartResources, "resources", err)
		}
	}
	if err := b.AddInputYAML(r.conf.Input); err != nil {
		return nil, r.configError(core.PartInput, "input", err)
	}
	var once sync.Once
	if err := b.AddBatchConsumerFunc(func(_ context.Context, batch service.MessageBatch) error {
		r.consumed.Add(1)
		if got != nil {
			once.Do(func() { got <- batch.Copy() })
		}
		return errNotAcked
	}); err != nil {
		return nil, err
	}
	return b, nil
}

// buildCapture builds the capture stream (see configureCapture).
func (r *run) buildCapture(got chan<- service.MessageBatch) (*service.Stream, error) {
	b, err := r.configureCapture(got)
	if err != nil {
		return nil, err
	}
	strm, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("build the capture stream: %w", err)
	}
	return strm, nil
}

// capture runs the capture stream until it delivers its first batch, then
// stops it before stepping starts. It keeps the first maxMessages messages
// of the batch (all of them when maxMessages is 0) and also returns the
// batch's total count. The dropped messages are nacked like the rest: no
// message is ever acked (AD-4, AD-5). It returns a core.Error with code
// NOTHING_CAPTURED when the input closes first.
func (r *run) capture(ctx context.Context, maxMessages int) (batch service.MessageBatch, total int, err error) {
	batch, err = r.captureFirst(ctx)
	if err != nil {
		return nil, 0, err
	}
	total = len(batch)
	if maxMessages > 0 && total > maxMessages {
		batch = batch[:maxMessages:maxMessages]
	}
	return batch, total, nil
}

// captureFirst runs the capture stream and returns its first batch.
func (r *run) captureFirst(ctx context.Context) (service.MessageBatch, error) {
	got := make(chan service.MessageBatch, 1)
	strm, err := r.buildCapture(got)
	if err != nil {
		return nil, err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- strm.Run(runCtx) }()

	select {
	case batch := <-got:
		r.stopCapture(strm, runErr, cancel)
		return batch, nil
	case err := <-runErr:
		// The input closed (or the stream failed) on its own.
		select {
		case batch := <-got:
			return batch, nil
		default:
		}
		if ctx.Err() != nil {
			_ = strm.StopWithin(stopTimeout)
			return nil, ctx.Err()
		}
		if err != nil {
			return nil, fmt.Errorf("capture: %w", err)
		}
		return nil, core.NewError(core.CodeNothingCaptured, "no batch was captured: the input closed before it produced one")
	case <-ctx.Done():
		if err := r.stop(strm, runErr, cancel); err != nil {
			return nil, errors.Join(ctx.Err(), err)
		}
		return nil, ctx.Err()
	}
}

// captureStopTimeout bounds the graceful part of stopping the capture stream.
const captureStopTimeout = time.Second

// stopCapture stops the capture stream once its batch is in hand. The input
// keeps re-sending the batch it was nacked for, so a graceful stop can stall
// waiting for that transaction; Stop then turns forceful. That is harmless
// here: nothing is ever acked, so nothing is committed (AD-4, AD-5).
func (r *run) stopCapture(strm *service.Stream, runErr <-chan error, cancel context.CancelFunc) {
	if err := strm.StopWithin(captureStopTimeout); err != nil {
		r.log.Debug("capture stream stopped forcefully", "error", err)
	}
	cancel()
	<-runErr
}
