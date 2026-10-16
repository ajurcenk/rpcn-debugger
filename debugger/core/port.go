package core

import (
	"context"
	"errors"
)

// ErrNothingCaptured is returned by an Engine whose input closed before it
// produced a batch.
var ErrNothingCaptured = errors.New("no batch was captured: the input closed before it produced one")

// ErrSessionEnded is returned by Arrive when the session ended (disconnect)
// while a stepper waited at a stop.
var ErrSessionEnded = errors.New("debug session ended")

// MessageCopy is a stepper's deep copy of one message (AD-19). It never
// references engine objects.
type MessageCopy struct {
	Content []byte
}

// BatchCopy is a stepper's deep copy of the batch it is processing.
type BatchCopy []MessageCopy

// Arriver is what a stepper calls at each stop position (AD-9). The Session
// implements it.
type Arriver interface {
	// Arrive is called before child childIndex of stepper stepperID runs,
	// with a copy of the batch. It blocks while the session is stopped there.
	// A non-nil error means the session ended and the stepper must deliver
	// nothing.
	Arrive(ctx context.Context, stepperID, childIndex int, batch BatchCopy) error
}

// Position is where a stop position is in the user's file.
type Position struct {
	// Path is the user path of the child, e.g. pipeline.processors[0].
	Path string
	// Line is the 1-based line of the child in the user's file.
	Line int
}

// LineMap maps a stop position to the user's file (AD-6, AD-16).
type LineMap interface {
	Position(stepperID, childIndex int) (Position, bool)
}

// Engine is the port the Session reaches the engine through. The adapter
// behind it owns the rewrite and the Connect engine.
type Engine interface {
	// Check reads and rewrites the pipeline file without running anything.
	// Its error fails the launch.
	Check(pipeline, cwd string) (LineMap, error)
	// Start captures one batch from the pipeline's input and steps it
	// through the processors, calling a.Arrive at every stop position. It
	// returns nil once the batch is delivered, ErrNothingCaptured when the
	// input yields nothing, or an error when ctx ends or the run fails.
	Start(ctx context.Context, pipeline, cwd string, a Arriver) error
}

// Frontend receives the Session's events. Its methods may be called from any
// goroutine, but never while the Session holds its state lock.
type Frontend interface {
	// Stopped reports a stop on threadID.
	Stopped(reason string, threadID int)
	// Output reports a message for the user.
	Output(category, text string)
	// Terminated reports the end of the debuggee run.
	Terminated()
}
