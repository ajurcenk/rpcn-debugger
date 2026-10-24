package core

import (
	"slices"
	"strings"
)

// OutputMode is where the step stream's rpcn_debug_sink sends what it
// receives (AD-5, AD-21): the launch key output.
type OutputMode string

// The output modes. The zero value means OutputDryRun.
const (
	// OutputDryRun records each batch and shows it in the frontend only.
	OutputDryRun OutputMode = "dry-run"
	// OutputReal also writes each batch to the user's output.
	OutputReal OutputMode = "real"
	// OutputNone writes nothing anywhere but the frontend.
	OutputNone OutputMode = "none"
)

// OutputModes are the values the launch key output may take, in schema
// order.
var OutputModes = []OutputMode{OutputDryRun, OutputReal, OutputNone}

// Effective returns m, with "" read as OutputDryRun.
func (m OutputMode) Effective() OutputMode {
	if m == "" {
		return OutputDryRun
	}
	return m
}

// checkOutputMode returns a LAUNCH_INVALID error naming output when m is
// not "" or one of OutputModes.
func checkOutputMode(m OutputMode) error {
	if m == "" || slices.Contains(OutputModes, m) {
		return nil
	}
	names := make([]string, len(OutputModes))
	for i, o := range OutputModes {
		names[i] = string(o)
	}
	return NewError(CodeLaunchInvalid, `"output" must be one of %s`, strings.Join(names, ", "))
}
