package debugger

import (
	"io"
	"log/slog"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/dap"
)

// Option configures Serve. There are no options yet.
type Option func(*config)

type config struct{}

// Serve runs one debug adapter session, speaking DAP on in and out, until the
// client disconnects or in reaches EOF. env is the Connect environment the
// pipelines run against; Serve never modifies it (AD-1). Only the DAP encoder
// writes to out (AD-15), and diagnostics go to stderr.
//
// Serve returns nil when the session ends normally, and an error when the
// stream is malformed or can't be written.
func Serve(env *service.Environment, in io.Reader, out io.Writer, opts ...Option) error {
	cfg := &config{}
	for _, opt := range opts {
		opt(cfg)
	}
	// env is held for the engine, which arrives in a later epic. Each session
	// will work on env.Clone().
	_ = env

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))
	return dap.Serve(in, out, log)
}
