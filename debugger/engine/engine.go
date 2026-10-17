package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// stopTimeout bounds the shutdown of each stream.
const stopTimeout = 5 * time.Second

// Configs are the rewritten parts the engine builds its streams from. Only
// debugger/rewrite produces them (AD-6); the engine never parses YAML.
type Configs struct {
	// Input is the capture config: the user's input block.
	Input string
	// Resources holds the top-level *_resources sections, or "".
	Resources string
	// Stepper is the rpcn_debug_stepper processor config.
	Stepper string
}

// Loader reads and rewrites a pipeline file.
type Loader func(pipeline string) (Configs, core.LineMap, error)

// Adapter implements core.Engine on clones of one Connect environment.
type Adapter struct {
	env  *service.Environment
	log  *slog.Logger
	load Loader
}

var _ core.Engine = (*Adapter)(nil)

// New returns an Adapter that runs pipelines loaded by load against clones
// of env, logging to log. env is never modified (AD-1).
func New(env *service.Environment, log *slog.Logger, load Loader) *Adapter {
	return &Adapter{env: env, log: log, load: load}
}

// Check loads the pipeline and parses and lints both stream configs, so
// config errors fail the launch. It never calls Build: Build opens every
// *_resources entry (caches, connection pools), which would leak.
func (a *Adapter) Check(pipeline, _ string) (core.LineMap, error) {
	conf, lines, err := a.load(pipeline)
	if err != nil {
		return nil, err
	}
	r, err := a.newRun(conf, nopArriver{})
	if err != nil {
		return nil, err
	}
	if _, err := r.configureCapture(nil); err != nil {
		return nil, fmt.Errorf("%s: %w", pipeline, err)
	}
	if _, _, err := r.configureStep(); err != nil {
		return nil, fmt.Errorf("%s: %w", pipeline, err)
	}
	return lines, nil
}

// Start captures one batch without acking it, stops the capture stream, then
// feeds the batch through the stepper into rpcn_debug_sink. It returns nil
// once the sink has the result.
func (a *Adapter) Start(ctx context.Context, pipeline, _ string, arr core.Arriver) error {
	conf, _, err := a.load(pipeline)
	if err != nil {
		return err
	}
	r, err := a.newRun(conf, arr)
	if err != nil {
		return err
	}
	batch, err := r.capture(ctx)
	if err != nil {
		return err
	}
	if err := r.step(ctx, batch); err != nil {
		return err
	}
	a.log.Info("debug run delivered", "batches", len(r.sink.batches()))
	return nil
}

// run is one debug run on its own env clone (AD-1).
type run struct {
	env      *service.Environment
	log      *slog.Logger
	conf     Configs
	sink     *sink
	consumed atomic.Int64
}

func (a *Adapter) newRun(conf Configs, arr core.Arriver) (*run, error) {
	r := &run{env: a.env.Clone(), log: a.log, conf: conf, sink: &sink{}}
	if err := registerStepper(r.env, arr); err != nil {
		return nil, fmt.Errorf("register %s: %w", StepperName, err)
	}
	if err := registerSink(r.env, r.sink); err != nil {
		return nil, fmt.Errorf("register %s: %w", SinkName, err)
	}
	return r, nil
}

// builder returns a StreamBuilder that logs to the session's stderr logger
// and serves its endpoints on a private mux (AD-15): the default logger
// writes to stdout and the default http server binds :4195.
func (r *run) builder() *service.StreamBuilder {
	b := r.env.NewStreamBuilder()
	b.SetLogger(r.log)
	b.SetHTTPMux(http.NewServeMux())
	return b
}

// configureStep sets up, without building, the step stream: a producer
// func, the stepper and the sink, plus the shared resources.
func (r *run) configureStep() (*service.StreamBuilder, service.MessageBatchHandlerFunc, error) {
	b := r.builder()
	produce, err := b.AddBatchProducerFunc()
	if err != nil {
		return nil, nil, err
	}
	if r.conf.Resources != "" {
		if err := b.AddResourcesYAML(r.conf.Resources); err != nil {
			return nil, nil, fmt.Errorf("resources: %w", err)
		}
	}
	if err := b.AddProcessorYAML(r.conf.Stepper); err != nil {
		return nil, nil, fmt.Errorf("pipeline.processors: %w", err)
	}
	if err := b.AddOutputYAML(SinkName + ": {}"); err != nil {
		return nil, nil, err
	}
	return b, produce, nil
}

// buildStep builds the step stream (see configureStep).
func (r *run) buildStep() (*service.Stream, service.MessageBatchHandlerFunc, error) {
	b, produce, err := r.configureStep()
	if err != nil {
		return nil, nil, err
	}
	strm, err := b.Build()
	if err != nil {
		return nil, nil, fmt.Errorf("build the step stream: %w", err)
	}
	return strm, produce, nil
}

// step feeds batch through the step stream and waits for its delivery.
func (r *run) step(ctx context.Context, batch service.MessageBatch) error {
	strm, produce, err := r.buildStep()
	if err != nil {
		return err
	}
	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- strm.Run(runCtx) }()

	// The producer gets its own ctx: abandoning a transaction leaves the
	// pipeline waiting to hand back its ack, which stalls the stop. When ctx
	// ends, the session has released the stepper (Arrive returns), so the
	// transaction finishes on its own; the wait for it is bounded anyway.
	prodDone := make(chan error, 1)
	go func() { prodDone <- produce(context.Background(), batch) }()
	var prodErr error
	select {
	case prodErr = <-prodDone:
	case <-ctx.Done():
		select {
		case prodErr = <-prodDone:
		case <-time.After(stopTimeout):
		}
	}
	stopErr := r.stop(strm, runErr, cancel)
	if ctx.Err() != nil {
		return ctx.Err()
	}
	if prodErr != nil {
		return fmt.Errorf("deliver the batch: %w", prodErr)
	}
	return stopErr
}

// stop stops strm and waits for its Run to return.
func (r *run) stop(strm *service.Stream, runErr <-chan error, cancel context.CancelFunc) error {
	err := strm.StopWithin(stopTimeout)
	cancel()
	<-runErr
	if err != nil {
		return fmt.Errorf("stop stream: %w", err)
	}
	return nil
}

// nopArriver never stops; Check builds streams with it and never runs them.
type nopArriver struct{}

func (nopArriver) Arrive(context.Context, int, int, []core.Batch) error {
	return errors.New("not running")
}
