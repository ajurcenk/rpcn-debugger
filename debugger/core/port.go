package core

import "context"

// Arriver is what a stepper calls at each stop position (AD-9). The Session
// implements it.
type Arriver interface {
	// Arrive is called before child childIndex of stepper stepperID runs,
	// with the stepper's batches, built from its deep copy (AD-19). It
	// blocks while the session is stopped there. A non-nil error (code
	// SESSION_ENDED, or ctx's error) means the session ended and the stepper
	// must deliver nothing.
	Arrive(ctx context.Context, stepperID, childIndex int, batches []Batch) error
}

// LineMap maps a stop position to the user's file (AD-6, AD-16). Position
// fills only UserPath and Line.
type LineMap interface {
	Position(stepperID, childIndex int) (Position, bool)
}

// Translate maps an engine path (as in engine logs and errors) to the
// user's path (AD-6). debugger/rewrite implements it with entry 2.5.
type Translate interface {
	Translate(enginePath string) (userPath string, ok bool)
}

// Engine is the port the Session reaches the engine through. The adapter
// behind it owns the rewrite and the Connect engine.
type Engine interface {
	// Check reads and rewrites the pipeline file without running anything.
	// Its error fails the launch; a non-Error one is PIPELINE_INVALID.
	Check(pipeline, cwd string) (LineMap, error)
	// Start captures one batch from the pipeline's input and steps it
	// through the processors, calling a.Arrive at every stop position. It
	// returns nil once the batch is delivered, an Error with code
	// NOTHING_CAPTURED when the input yields nothing, or an error when ctx
	// ends or the run fails.
	Start(ctx context.Context, pipeline, cwd string, a Arriver) error
}

// Frontend receives the Session's events. Emit may be called from any
// goroutine, but never while the Session holds its state lock.
type Frontend interface {
	Emit(Event)
}
