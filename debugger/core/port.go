package core

import "context"

// Arriver is what a stepper calls at each stop position (AD-9). The Session
// implements it.
type Arriver interface {
	// Captured is called once the batch is captured, before the first
	// Arrive: kept of its total messages are stepped (AD-4 maxMessages).
	Captured(kept, total int)
	// Enter starts one entry of stepper stepperID, whose list has children
	// child processors (AD-17). It returns ctx with the entry's frame pushed
	// on the frame stack, which the stepper passes to every Arrive and to
	// its children, and the exit func it calls when the entry returns. An
	// entry whose ctx has no frame runs on the root thread.
	Enter(ctx context.Context, stepperID, children int) (context.Context, func())
	// Arrive is called at each stop position of the entry whose frame is on
	// top of ctx: before child childIndex runs, and after the last child
	// (childIndex = children, AD-10). batches are the stepper's batches,
	// built from its deep copy (AD-19). It blocks while the session is
	// stopped there. A non-nil error (code SESSION_ENDED, or ctx's error)
	// means the session ended and the stepper must deliver nothing.
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

// CheckResult is what Engine.Check found in a pipeline that can run.
type CheckResult struct {
	// Lines maps the rewrite's stop positions to the user's file.
	Lines LineMap
	// Warnings are the launch warnings for the user (opaque parents, stdio
	// redirects), in rewrite order. The Session emits each as an Output.
	Warnings []string
}

// Engine is the port the Session reaches the engine through. The adapter
// behind it owns the rewrite and the Connect engine.
type Engine interface {
	// Check reads and rewrites the pipeline file without running anything.
	// Its error fails the launch; a non-Error one is PIPELINE_INVALID.
	Check(pipeline, cwd string) (CheckResult, error)
	// Start captures one batch from the pipeline's input, keeps at most
	// opts.MaxMessages of it, and steps it through the processors, calling
	// a.Captured once and a.Arrive at every stop position. It returns nil
	// once the batch is delivered, an Error with code NOTHING_CAPTURED when
	// the input yields nothing, or an error when ctx ends or the run fails.
	Start(ctx context.Context, opts StartOptions, a Arriver) error
}

// StartOptions are the launch options an Engine runs with. The Session
// fills them from Launch, with paths resolved.
type StartOptions struct {
	// Pipeline is the absolute path of the pipeline file.
	Pipeline string
	// Cwd is the absolute directory relative paths resolve against.
	Cwd string
	// MaxMessages is the most messages of the captured batch to step; the
	// rest are dropped, never acked. 0 means no limit.
	MaxMessages int
}

// Frontend receives the Session's events. Emit may be called from any
// goroutine, but never while the Session holds its state lock.
type Frontend interface {
	Emit(Event)
}
