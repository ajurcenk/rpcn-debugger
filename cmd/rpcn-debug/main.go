// Command rpcn-debug is the stock Redpanda Connect pipeline debugger. It is a
// thin binary (AD-1): it builds the standard Connect environment with every
// component compiled in and hands it to the debugger package.
//
// stdout belongs to DAP (AD-15): nothing but the --version output is ever
// written to it.
package main

import (
	"flag"
	"fmt"
	"io"
	"os"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/schema"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stdout, os.Stderr))
}

// run executes the command and returns its exit code. stdout receives only
// the --version output.
func run(args []string, stdout, stderr io.Writer) int {
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

	env := standardEnvironment()
	_ = env // handed to debugger.Serve once it exists (entry 1.4).

	_, _ = fmt.Fprintln(stderr, "rpcn-debug: the DAP server is not implemented yet")
	return 1
}

// standardEnvironment builds the stock Connect environment (AD-1).
func standardEnvironment() *service.Environment {
	return schema.Standard(version, date).Environment()
}
