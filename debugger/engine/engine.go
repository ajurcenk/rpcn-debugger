package engine

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"slices"
	"strings"
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
	// Input is the capture config: the user's input block, without
	// input.processors (AD-4).
	Input string
	// Resources holds the top-level *_resources sections, or "".
	Resources string
	// Steppers are the top-level rpcn_debug_stepper processor configs, in
	// step order (input, pipeline, then output processors).
	Steppers []string
	// Output is the step stream's output config: rpcn_debug_sink with its
	// mode and, under output: real, the user's output block (AD-21), added
	// as given. "" means rpcn_debug_sink in dry-run.
	Output string
	// Warnings are the launch warnings for the user.
	Warnings []string
	// Strict is error_handling.strict, applied to the step stream (AD-4(b)).
	// StrictExpr overrides it when set.
	Strict bool
	// StrictExpr is error_handling.strict verbatim when it is a ${VAR}
	// interpolation, or "". The engine resolves it through the run's lookup
	// (AD-22), the way the stock stream builder does, and uses the result
	// instead of Strict.
	StrictExpr string
	// StrictLine is the file line of StrictExpr, for its error.
	StrictLine int
	// Translator maps engine paths and Bloblang lines back to the user's
	// file (AD-6). Check returns it in its CheckResult; it may be nil.
	Translator core.Translate
	// Delivery positions the Delivered Snapshots: the top-level output
	// key (AD-21). Check returns it in its CheckResult.
	Delivery core.Position
	// Positions maps a line and column in the YAML of one config part
	// (core.PartInput, core.StepperPart(k), core.PartResources or
	// core.PartOutput) to the user's file, with the user path of the innermost processor there (AD-6). It may be nil;
	// config errors then keep the rewritten config's positions.
	Positions func(part string, line, col int) (fileLine, fileCol int, userPath string, ok bool)
	// Inputs are the AD-5 findings of the capture tree, in walker order.
	// Check returns them in its CheckResult, for the Session to decide on.
	Inputs []core.InputFinding
}

// LoadOptions are the rewrite options a Loader passes on, from the
// Session's StartOptions (AD-5).
type LoadOptions struct {
	// DebugGroup is the debug-group base rpcn-debug-<os-user>.
	DebugGroup string
	// InputClasses is the launch's inputClasses: the class each named
	// input component gets instead of the input table's. Every name is an
	// input component of the Adapter's environment.
	InputClasses map[string]core.InputClass
	// KeepConsumerGroup keeps the capture tree's consumer groups as
	// configured instead of overriding them with the debug group.
	KeepConsumerGroup bool
	// Output is where rpcn_debug_sink sends the batches it receives
	// (AD-21). "" is dry-run.
	Output core.OutputMode
}

// Loader reads and rewrites a pipeline file with opts.
type Loader func(pipeline string, opts LoadOptions) (Configs, core.LineMap, error)

// Adapter implements core.Engine on clones of one Connect environment.
type Adapter struct {
	env  *service.Environment
	log  *slog.Logger
	load Loader
	// writeTimeout bounds each batch's write under output: real; 0 means
	// the sink's default (30 s).
	writeTimeout time.Duration
}

var _ core.Engine = (*Adapter)(nil)

// New returns an Adapter that runs pipelines loaded by load against clones
// of env, logging to log. env is never modified (AD-1).
func New(env *service.Environment, log *slog.Logger, load Loader) *Adapter {
	return &Adapter{env: env, log: log, load: load}
}

// SetWriteTimeout sets how long the sink waits under output: real for the
// user's output to write one batch, reconnects included, before the batch
// fails with OUTPUT_FAILED (default 30 s). It is for tests, and must be
// called before Start.
func (a *Adapter) SetWriteTimeout(d time.Duration) { a.writeTimeout = d }

// Check loads the pipeline and parses and lints both stream configs, so
// config errors fail the launch. ${VAR} resolves through opts.Lookup, as in
// Start (AD-22). It never calls Build: Build opens every *_resources entry
// (caches, connection pools), which would leak. The loaded Configs come
// back as the result's Prepared, for Start (AD-6: one rewrite per launch).
func (a *Adapter) Check(opts core.StartOptions) (core.CheckResult, error) {
	pipeline := opts.Pipeline
	lo, err := a.loadOptions(opts)
	if err != nil {
		return core.CheckResult{}, err
	}
	conf, lines, err := a.load(pipeline, lo)
	if err != nil {
		return core.CheckResult{}, err
	}
	r, err := a.newRun(conf, opts.Lookup, nopArriver{})
	if err != nil {
		return core.CheckResult{Translator: conf.Translator}, fmt.Errorf("%s: %w", pipeline, err)
	}
	// A failed Check still carries the translator, so the Session can name
	// user paths in its error (AD-6).
	failed := core.CheckResult{Translator: conf.Translator}
	if _, err := r.configureCapture(nil); err != nil {
		return failed, fmt.Errorf("%s: %w", pipeline, err)
	}
	if _, _, err := r.configureStep(); err != nil {
		return failed, fmt.Errorf("%s: %w", pipeline, err)
	}
	return core.CheckResult{
		Lines: lines, Translator: conf.Translator, Warnings: conf.Warnings, Delivery: conf.Delivery,
		Inputs: conf.Inputs, Prepared: conf,
	}, nil
}

// loadOptions picks the rewrite options out of opts. An inputClasses name
// that isn't an input component of the environment is LAUNCH_INVALID.
func (a *Adapter) loadOptions(opts core.StartOptions) (LoadOptions, error) {
	names := make([]string, 0, len(opts.InputClasses))
	for name := range opts.InputClasses {
		names = append(names, name)
	}
	slices.Sort(names)
	for _, name := range names {
		if _, ok := a.env.GetInputConfig(name); !ok {
			return LoadOptions{}, core.NewError(core.CodeLaunchInvalid, `"inputClasses.%s": %s is not an input component`, name, name)
		}
	}
	return LoadOptions{
		DebugGroup: opts.DebugGroup, InputClasses: opts.InputClasses, KeepConsumerGroup: opts.KeepConsumerGroup,
		Output: opts.Output,
	}, nil
}

// Start captures one batch without acking it, stops the capture stream,
// truncates the batch to opts.MaxMessages, then feeds it through the stepper
// into rpcn_debug_sink. It returns nil once the sink has the result. It
// runs opts.Prepared, the Configs Check returned, and loads the pipeline
// itself only when Prepared is nil.
func (a *Adapter) Start(ctx context.Context, opts core.StartOptions, arr core.Arriver) error {
	var conf Configs
	switch p := opts.Prepared.(type) {
	case Configs:
		conf = p
	case nil:
		lo, err := a.loadOptions(opts)
		if err != nil {
			return err
		}
		if conf, _, err = a.load(opts.Pipeline, lo); err != nil {
			return err
		}
	default:
		return fmt.Errorf("start: Prepared is a %T, not the Configs Check returns", opts.Prepared)
	}
	r, err := a.newRun(conf, opts.Lookup, arr)
	if err != nil {
		return fmt.Errorf("%s: %w", opts.Pipeline, err)
	}
	batch, total, err := r.capture(ctx, opts.MaxMessages)
	if err != nil {
		return err
	}
	arr.Captured(len(batch), total)
	if err := r.step(ctx, batch); err != nil {
		return err
	}
	a.log.Info("debug run delivered", "batches", len(r.sink.batches()))
	return nil
}

// run is one debug run on its own env clones (AD-1).
type run struct {
	env *service.Environment
	// capEnv builds the capture stream. Its rpcn_debug_stepper never stops:
	// processors in the input tree, resources they use included, run during
	// capture without being stepped.
	capEnv   *service.Environment
	log      *slog.Logger
	conf     Configs
	lookup   func(string) (string, bool) // nil: the process environment
	strict   bool                        // the effective error_handling.strict
	sink     *sink
	consumed atomic.Int64
	// atOutput counts the errored messages on their way to the output: the
	// top-level steppers keep it (stepper.atOutput), and once the batch is
	// delivered it holds how many reached the output.
	atOutput atomic.Int64
}

func (a *Adapter) newRun(conf Configs, lookup func(string) (string, bool), arr core.Arriver) (*run, error) {
	r := &run{env: a.env.Clone(), capEnv: a.env.Clone(), log: a.log, conf: conf, lookup: lookup, sink: &sink{arr: arr, timeout: a.writeTimeout}}
	var err error
	if r.strict, err = r.resolveStrict(); err != nil {
		return nil, err
	}
	if err = registerStepper(r.env, arr, &r.atOutput); err != nil {
		return nil, fmt.Errorf("register %s: %w", StepperName, err)
	}
	if err = registerStepper(r.capEnv, nopArriver{}, nil); err != nil {
		return nil, fmt.Errorf("register %s: %w", StepperName, err)
	}
	if err := registerSink(r.env, r.sink); err != nil {
		return nil, fmt.Errorf("register %s: %w", SinkName, err)
	}
	return r, nil
}

// resolveStrict returns the effective error_handling.strict: conf.Strict,
// or conf.StrictExpr resolved with benthos's own interpolation through the
// run's lookup (AD-2, AD-22). It matches the stock stream builder, probed on
// benthos v4.84: an unset variable without a default resolves to "", which
// is false, and a value that isn't a bool fails the config.
func (r *run) resolveStrict() (bool, error) {
	if r.conf.StrictExpr == "" {
		return r.conf.Strict, nil
	}
	q := r.env.FullConfigSchema("", "").NewConfigQuerier()
	if r.lookup != nil {
		q.SetEnvVarLookupFunc(func(_ context.Context, k string) (string, bool) { return r.lookup(k) })
	}
	// The padding puts the scalar on its file line, so a YAML error names
	// the user's line.
	pad := strings.Repeat("\n", max(r.conf.StrictLine-2, 0))
	strictErr := func(err error) error {
		e := core.NewError(core.CodePipelineInvalid, "error_handling.strict %s does not resolve to a bool: %v", r.conf.StrictExpr, err)
		e.Line, e.UserPath = r.conf.StrictLine, "error_handling.strict"
		return e
	}
	f, err := q.ParseYAML(pad + "error_handling:\n  strict: " + r.conf.StrictExpr + "\n")
	if err != nil {
		return false, strictErr(err)
	}
	pc, err := f.FieldAtPath("error_handling", "strict")
	if err != nil {
		return false, strictErr(err)
	}
	b, err := pc.FieldBool()
	if err != nil {
		return false, strictErr(err)
	}
	return b, nil
}

// builder returns a StreamBuilder that logs to the session's logger (the
// AD-15 handler) and serves its endpoints on a private mux: the default
// logger writes to stdout and the default http server binds :4195. It
// resolves ${VAR} through the session's lookup (AD-22); the builder applies
// it in every Add*YAML call, so it is set before any of them.
func (r *run) builder(env *service.Environment) *service.StreamBuilder {
	b := env.NewStreamBuilder()
	b.SetLogger(r.log)
	b.SetHTTPMux(http.NewServeMux())
	if r.lookup != nil {
		b.SetEnvVarLookupFunc(r.lookup)
	}
	return b
}

// stepperName names top-level stepper i in errors: the user path of the
// list it wraps (AD-6), or "processors (stepper i)" without a translator.
func (r *run) stepperName(i int) string {
	if tr := r.conf.Translator; tr != nil {
		if up, ok := tr.Translate(fmt.Sprintf("root.pipeline.processors.%d", i)); ok {
			return up
		}
	}
	return fmt.Sprintf("processors (stepper %d)", i)
}

// configureStep sets up, without building, the step stream: a producer
// func, the steppers in order and the sink, plus the shared resources.
func (r *run) configureStep() (*service.StreamBuilder, service.MessageBatchHandlerFunc, error) {
	b := r.builder(r.env)
	b.SetStrict(r.strict)
	produce, err := b.AddBatchProducerFunc()
	if err != nil {
		return nil, nil, err
	}
	if r.conf.Resources != "" {
		if err := b.AddResourcesYAML(r.conf.Resources); err != nil {
			return nil, nil, r.configError(core.PartResources, "resources", err)
		}
	}
	for i, st := range r.conf.Steppers {
		if err := b.AddProcessorYAML(st); err != nil {
			return nil, nil, r.configError(core.StepperPart(i), r.stepperName(i), err)
		}
	}
	out := r.conf.Output
	if out == "" {
		out = SinkName + ": {}"
	}
	if err := b.AddOutputYAML(out); err != nil {
		return nil, nil, r.configError(core.PartOutput, "output", err)
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

	// The producer gets its own ctx, not ctx: abandoning a transaction
	// leaves the pipeline waiting to hand back its ack, which stalls the
	// stop. When ctx ends, the session has released the stepper (Arrive
	// returns), so the transaction finishes on its own; the wait for it is
	// bounded anyway. prodCtx is cancelled once the step stream has
	// stopped, so the producer never outlives the run.
	prodCtx, prodCancel := context.WithCancel(context.Background())
	defer prodCancel()
	r.atOutput.Store(int64(erroredIn(batch)))
	prodDone := make(chan error, 1)
	go func() { prodDone <- produce(prodCtx, batch) }()
	var prodErr error
	select {
	case prodErr = <-prodDone:
	case err := <-runErr:
		// The stream ended before delivering, e.g. the user's output under
		// output: real failed to init: the producer would wait for ever.
		if err == nil {
			err = errors.New("it stopped before delivering the batch")
		}
		return fmt.Errorf("run the step stream: %w", err)
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
	if err := r.deliveryError(prodErr); err != nil {
		return err
	}
	return stopErr
}

// deliveryError decides how the run ends from what happened to the batch,
// never from error text (AD-3, AD-21), given the producer's error:
//
//   - a batch the user's output failed to write is OUTPUT_FAILED, naming
//     the output and the write error;
//   - otherwise, under strict with errored messages at the output, the
//     output rejected them: STRICT_REJECTED, an outcome and no failure;
//   - otherwise any producer error is a delivery failure.
func (r *run) deliveryError(prodErr error) error {
	if werr := r.sink.failure(); werr != nil {
		e := core.NewError(core.CodeOutputFailed, "the output failed to write a delivered batch: %v", werr)
		e.UserPath, e.Line = r.conf.Delivery.UserPath, r.conf.Delivery.Line
		if e.UserPath == "" {
			e.UserPath = "output"
		}
		return e
	}
	if prodErr == nil {
		return nil
	}
	if r.strict && r.atOutput.Load() > 0 {
		// Expected strict behaviour, not a failure: the output nacks the
		// messages that failed a processing step.
		return core.NewError(core.CodeStrictRejected, "%v", prodErr)
	}
	return fmt.Errorf("deliver the batch: %w", prodErr)
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

// nopArriver never stops. Check builds streams with it and never runs them,
// and the capture stream runs its steppers with it, so input-tree processors
// run unstepped.
type nopArriver struct{}

func (nopArriver) Captured(int, int) {}

// Enter pushes a frame, so a nested stepper finds one and does not report
// CTX_LOST (AD-17). Nothing reads it: only the Session uses frames.
func (nopArriver) Enter(ctx context.Context, _ core.Entry) (context.Context, func(any)) {
	return core.PushFrame(ctx, core.FrameRef{}), func(any) {}
}

func (nopArriver) Skip(context.Context, int, int, []core.Skipped, int) {}

func (nopArriver) Arrive(context.Context, int, int, []core.Batch) error { return nil }

func (nopArriver) Deliver(context.Context, core.Batch, core.Outcome, error) {}
