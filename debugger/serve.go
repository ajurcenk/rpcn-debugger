package debugger

import (
	"errors"
	"io"
	"os"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/dap"
	"github.com/ajurcenk/rpcn-debugger/debugger/engine"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

// Option configures Serve.
type Option func(*config)

type config struct {
	logOutput io.Writer
	// captureWait, when set, replaces the Session's 5 s wait before its
	// waiting note (withCaptureWait, for tests).
	captureWait time.Duration
}

// withCaptureWait shortens the Session's wait before its "Waiting for the
// input" note. It is a test hook.
func withCaptureWait(d time.Duration) Option {
	return func(c *config) { c.captureWait = d }
}

// WithLogOutput sends Serve's log output (the stderr side of the AD-15
// handler) to w instead of os.Stderr. A nil w keeps os.Stderr.
func WithLogOutput(w io.Writer) Option {
	return func(c *config) {
		if w != nil {
			c.logOutput = w
		}
	}
}

// Serve runs one debug adapter session, speaking DAP on in and out, until the
// client disconnects or in reaches EOF. env is the Connect environment the
// pipelines run against; Serve never modifies it: each run works on
// env.Clone() (AD-1). Only the DAP encoder writes to out (AD-15).
//
// Every log record goes through one handler (AD-15), which translates
// engine paths and Bloblang lines to the user's file (AD-6). It writes Info
// and above to stderr (or the WithLogOutput writer) and sends the engine's
// warnings and errors to the client as output events.
//
// Serve returns nil when the session ends normally, and an error when the
// stream is malformed or can't be written.
func Serve(env *service.Environment, in io.Reader, out io.Writer, opts ...Option) error {
	cfg := &config{logOutput: os.Stderr}
	for _, opt := range opts {
		opt(cfg)
	}

	sink := &logSink{w: cfg.logOutput}
	// The DAP server's own diagnostics stay off the DAP stream: it logs
	// while it holds its write lock, and its failures are already
	// responses.
	dapLog := newLogger(sink, false)
	engineLog := newLogger(sink, true)
	attach := sink.attach
	if d := cfg.captureWait; d > 0 {
		attach = func(sess *core.Session) {
			sess.SetCaptureWait(d)
			sink.attach(sess)
		}
	}
	return dap.Serve(in, out, dapLog, engine.New(env, engineLog, loadPipeline), dap.OnSession(attach))
}

// loadPipeline reads the pipeline file and rewrites it with opts (AD-6). The
// user's file is only read, never written.
func loadPipeline(path string, opts engine.LoadOptions) (engine.Configs, core.LineMap, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		// The launch's "pipeline" names no readable file.
		return engine.Configs{}, nil, core.NewError(core.CodeLaunchInvalid, "read the pipeline: %v", err)
	}
	plan, err := rewrite.Rewrite(raw, rewrite.Options{DebugGroup: opts.DebugGroup, InputClasses: opts.InputClasses})
	if err != nil {
		// Keep the rewrite's Line and UserPath.
		var ce core.Error
		if errors.As(err, &ce) {
			ce.Message = path + ": " + ce.Message
			return engine.Configs{}, nil, ce
		}
		return engine.Configs{}, nil, core.NewError(core.CodePipelineInvalid, "%s: %v", path, err)
	}
	return engine.Configs{
		Input:      plan.Input,
		Resources:  plan.Resources,
		Steppers:   plan.Steppers,
		Warnings:   plan.Warnings,
		Strict:     plan.Strict,
		StrictExpr: plan.StrictExpr,
		StrictLine: plan.StrictLine,
		Translator: plan,
		Delivery:   plan.Delivery,
		Positions:  plan.FilePosition,
		Inputs:     plan.Inputs,
	}, plan, nil
}
