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

// Outcome is what rpcn_debug_sink did with one batch it received (AD-21).
type Outcome string

// The outcomes, one per Delivered.
const (
	// OutcomeRecorded: under dry-run, the batch was recorded for the
	// frontend only.
	OutcomeRecorded Outcome = "recorded"
	// OutcomeWritten: under output: real, the user's output wrote the batch.
	OutcomeWritten Outcome = "written"
	// OutcomeDiscarded: under output: none, the batch was written nowhere.
	OutcomeDiscarded Outcome = "discarded"
	// OutcomeFailed: under output: real, the user's output failed to write
	// the batch, or didn't finish within the write deadline. The run ends
	// with OUTPUT_FAILED.
	OutcomeFailed Outcome = "failed"
)

// OutcomeOf returns the outcome of a batch the sink handled without a write
// failure under mode m.
func OutcomeOf(m OutputMode) Outcome {
	switch m.Effective() {
	case OutputReal:
		return OutcomeWritten
	case OutputNone:
		return OutcomeDiscarded
	default:
		return OutcomeRecorded
	}
}

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
