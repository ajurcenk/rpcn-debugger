// Command rpcn-debug is the stock Redpanda Connect pipeline debugger. It is a
// thin binary (AD-1): it builds the standard Connect environment with every
// component compiled in and hands it to the debugger package.
//
// stdout belongs to DAP (AD-15): with no arguments the binary serves DAP over
// stdin/stdout, and nothing but the DAP stream or the --version output is ever
// written to it.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/schema"

	"github.com/ajurcenk/rpcn-debugger/debugger"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdin, os.Stdout, os.Stderr))
}

// run executes the command and returns its exit code. stdout receives only
// the DAP stream or the --version output.
func run(args []string, stdin io.Reader, stdout, stderr io.Writer) int {
	fs := flag.NewFlagSet("rpcn-debug", flag.ContinueOnError)
	fs.SetOutput(stderr)
	showVersion := fs.Bool("version", false, "print the version and exit")
	if err := fs.Parse(args); err != nil {
		if err == flag.ErrHelp {
			return 0
		}
		return 2
	}

	// --version exits before any DAP mode (AD-15).
	if *showVersion {
		_, _ = fmt.Fprintln(stdout, versionLine())
		return 0
	}

	if err := debugger.Serve(standardEnvironment(), stdin, stdout, debugger.WithLogOutput(stderr)); err != nil {
		_, _ = fmt.Fprintf(stderr, "rpcn-debug: %v\n", err)
		return 1
	}
	return 0
}

// standardEnvironment builds the stock Connect environment (AD-1).
func standardEnvironment() *service.Environment {
	return schema.Standard(version, date).Environment()
}
