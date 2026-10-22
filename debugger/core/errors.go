package core

import (
	"errors"
	"fmt"
	"slices"
	"strconv"
	"strings"
)

// Code is a registry code: the stable, UPPER_SNAKE name of one kind of
// Session failure (AD-3).
type Code string

// The error registry. Every code has a fixed number (see Number), which is
// the DAP error id. A code's number is never changed or reused; a new code
// takes the next free number.
const (
	// CodeLaunchInvalid: the launch configuration is invalid (a missing or
	// unreadable pipeline, a malformed argument, a command out of order).
	CodeLaunchInvalid Code = "LAUNCH_INVALID"
	// CodePipelineInvalid: the pipeline YAML can't be parsed, rewritten or
	// linted.
	CodePipelineInvalid Code = "PIPELINE_INVALID"
	// CodeNothingCaptured: the input closed before it produced a batch.
	CodeNothingCaptured Code = "NOTHING_CAPTURED"
	// CodeSessionEnded: the session ended (terminate) while a stepper waited
	// at a stop, or a command arrived after the end.
	CodeSessionEnded Code = "SESSION_ENDED"
	// CodeNotSupported: the command is part of the protocol but not
	// implemented yet, or the frontend request has no protocol command.
	CodeNotSupported Code = "NOT_SUPPORTED"
	// CodeNotStopped: the command needs a stopped session.
	CodeNotStopped Code = "NOT_STOPPED"
	// CodeRunFailed: the debug run failed.
	CodeRunFailed Code = "RUN_FAILED"
	// CodeCtxLost: a stepper with a non-root parent found no frame in ctx
	// (AD-17).
	CodeCtxLost Code = "CTX_LOST"
	// CodeLicenseRequired: an enterprise component needs a license (AD-22).
	CodeLicenseRequired Code = "LICENSE_REQUIRED"
)

// numbers is the registry: code → stable DAP error id.
var numbers = map[Code]int{
	CodeLaunchInvalid:   1001,
	CodePipelineInvalid: 1002,
	CodeNothingCaptured: 1003,
	CodeSessionEnded:    1004,
	CodeNotSupported:    1005,
	CodeNotStopped:      1006,
	CodeRunFailed:       1007,
	CodeCtxLost:         1008,
	CodeLicenseRequired: 1009,
}

// Codes returns every registry code, in number order.
func Codes() []Code {
	out := make([]Code, 0, len(numbers))
	for c := range numbers {
		out = append(out, c)
	}
	slices.SortFunc(out, func(a, b Code) int { return numbers[a] - numbers[b] })
	return out
}

// Number returns code's stable number, or 0 for a code not in the registry.
func Number(code Code) int { return numbers[code] }

// Error is a Session failure (AD-3). Processor errors on messages are data
// (Message.Error in a Snapshot), never an Error.
type Error struct {
	// Code is a registry code.
	Code Code
	// Message is the human-readable text.
	Message string
	// UserPath is the user path the failure is about, e.g.
	// pipeline.processors[0], or "" when unknown.
	UserPath string
	// Line is the 1-based line in the user's file, or 0 when unknown.
	Line int
	// Inline is set when Message already names every location inline (a
	// config error with one file position per lint error): Error then
	// omits the location suffix, which would read as the last one's.
	// UserPath and Line still hold the first location.
	Inline bool
}

// NewError returns an Error with code and a formatted message.
func NewError(code Code, format string, args ...any) Error {
	return Error{Code: code, Message: fmt.Sprintf(format, args...)}
}

// Error returns the message, followed by the user path and line when known.
func (e Error) Error() string { return e.Message + e.location() }

// location is the " (at <UserPath>, line N)" suffix of Error, or "" (also
// when Inline is set).
func (e Error) location() string {
	if e.Inline {
		return ""
	}
	var at []string
	if e.UserPath != "" {
		at = append(at, e.UserPath)
	}
	if e.Line > 0 {
		at = append(at, "line "+strconv.Itoa(e.Line))
	}
	if len(at) == 0 {
		return ""
	}
	return " (at " + strings.Join(at, ", ") + ")"
}

// Is reports whether target is an Error with the same code and no message,
// so errors.Is(err, Error{Code: c}) tests for a code.
func (e Error) Is(target error) bool {
	t, ok := target.(Error)
	return ok && t.Message == "" && t.UserPath == "" && t.Line == 0 && t.Code == e.Code
}

// HasCode reports whether err is, or wraps, an Error with code.
func HasCode(err error, code Code) bool {
	return errors.Is(err, Error{Code: code})
}

// AsError returns err as an Error. An Error is returned as is. For an error
// that wraps one, the result keeps the wrapped Code, UserPath and Line, with
// err's full text (minus the location suffix Error adds back) as Message.
// Any other error becomes an Error with code fallback and err's text. It
// returns nil for a nil err.
func AsError(err error, fallback Code) error {
	if err == nil {
		return nil
	}
	if e, ok := err.(Error); ok {
		return e
	}
	var e Error
	if errors.As(err, &e) {
		e.Message = strings.TrimSuffix(err.Error(), e.location())
		return e
	}
	return Error{Code: fallback, Message: err.Error()}
}
