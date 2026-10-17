package debugger

import (
	"io"
	"log/slog"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/dap"
	"github.com/ajurcenk/rpcn-debugger/debugger/engine"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

// Option configures Serve. There are no options yet.
type Option func(*config)

type config struct{}

// Serve runs one debug adapter session, speaking DAP on in and out, until the
// client disconnects or in reaches EOF. env is the Connect environment the
// pipelines run against; Serve never modifies it: each run works on
// env.Clone() (AD-1). Only the DAP encoder writes to out (AD-15), and
// diagnostics, the engine's logs included, go to stderr.
//
// Serve returns nil when the session ends normally, and an error when the
// stream is malformed or can't be written.
func Serve(env *service.Environment, in io.Reader, out io.Writer, opts ...Option) error {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return dap.Serve(in, out, log, engine.New(env, log, loadPipeline))
}

// loadPipeline reads the pipeline file and rewrites it (AD-6). The user's
// file is only read, never written.
func loadPipeline(path string) (engine.Configs, core.LineMap, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		// The launch's "pipeline" names no readable file.
		return engine.Configs{}, nil, core.NewError(core.CodeLaunchInvalid, "read the pipeline: %v", err)
	}
	plan, err := rewrite.Rewrite(raw)
	if err != nil {
		return engine.Configs{}, nil, core.NewError(core.CodePipelineInvalid, "%s: %v", path, err)
	}
	return engine.Configs{Input: plan.Input, Resources: plan.Resources, Stepper: plan.Stepper}, plan, nil
}
