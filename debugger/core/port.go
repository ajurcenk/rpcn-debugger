package core

import "context"

// Arriver is what a stepper calls at each stop position (AD-9). The Session
// implements it.
type Arriver interface {
	// Captured is called once the batch is captured, before the first
	// Arrive: kept of its total messages are stepped (AD-4 maxMessages).
	Captured(kept, total int)
	// Enter starts entry e (AD-17). It returns ctx with the entry's frame
	// pushed on the frame stack, which the stepper passes to every Arrive
	// and to its children, and the exit func it calls when the entry
	// returns, with the memo the next entry at the same call site sees in
	// Site.Memo (nil for none). An entry whose ctx has no frame runs on the
	// root thread.
	Enter(ctx context.Context, e Entry) (context.Context, func(memo any))
	// Skip reports messages that the entry on top of ctx passes through
	// child childIndex without running it, because the first message of
	// their batch already failed (AD-7: try mode, or strict). Each message
	// is reported once per entry. With of > 0, the entry is one message of
	// its parent's batch of of messages, and skipped names it by its index
	// there; otherwise indices count across the entry's batches.
	Skip(ctx context.Context, stepperID, childIndex int, skipped []Skipped, of int)
	// Arrive is called at each stop position of the entry whose frame is on
	// top of ctx: before child childIndex runs, and after the last child
	// (childIndex = children, AD-10). batches are the stepper's batches,
	// built from its deep copy (AD-19). It blocks while the session is
	// stopped there. A non-nil error (code SESSION_ENDED, or ctx's error)
	// means the session ended and the stepper must deliver nothing.
	Arrive(ctx context.Context, stepperID, childIndex int, batches []Batch) error
	// Deliver reports one batch rpcn_debug_sink received, after
	// output.processors, built from its deep copy (AD-19, AD-21), with what
	// became of it: outcome, and under OutcomeFailed the write error err
	// (nil otherwise). The sink calls it exactly once per batch, after the
	// write under output: real, in the order received; the Session numbers
	// it and emits it as Delivered.
	Deliver(ctx context.Context, batch Batch, outcome Outcome, err error)
}

// LineMap maps a stop position to the user's file (AD-6, AD-16). Position
// fills only UserPath and Line.
type LineMap interface {
	Position(stepperID, childIndex int) (Position, bool)
}

// Translate maps what the engine reports to the user's file (AD-6):
// debugger/rewrite implements it. The single slog handler translates every
// log record through it before the record reaches stderr or a frontend.
type Translate interface {
	// Translate maps an engine path (the path attribute of an engine log
	// record, e.g. root.pipeline.processors.0.rpcn_debug_stepper.processors.1)
	// to the user's path (pipeline.processors[1]). ok is false, and
	// enginePath comes back unchanged, when it maps to nothing.
	Translate(enginePath string) (userPath string, ok bool)
	// TranslateLine maps line, a 1-based line inside the Bloblang mapping of
	// the processor at enginePath (a mapping, mutation or bloblang
	// processor), to its line in the pipeline file. ok is false when the
	// processor has no such mapping or its value isn't a literal block or a
	// plain scalar on the key's line (folded and quoted values).
	TranslateLine(enginePath string, line int) (fileLine int, ok bool)
}

// CheckResult is what Engine.Check found in a pipeline that can run.
type CheckResult struct {
	// Lines maps the rewrite's stop positions to the user's file.
	Lines LineMap
	// Translator maps engine paths and Bloblang lines to the user's file.
	// It may be nil.
	Translator Translate
	// Warnings are the launch warnings for the user (opaque parents, stdio
	// redirects), in rewrite order. The Session emits each as an Output.
	Warnings []string
	// Delivery is where Delivered Snapshots are positioned (AD-21): the
	// user path output and the line of the top-level output key (0 when
	// the pipeline has none). Only UserPath and Line are read; an empty
	// UserPath means output.
	Delivery Position
	// Inputs are the AD-5 findings of the capture tree, one per input in
	// walker order. The Session refuses the launch on them, or warns.
	Inputs []InputFinding
	// Prepared is the engine's own result of the rewrite: the configs Check
	// linted. It is opaque to the Session, which keeps it and passes it back
	// in StartOptions.Prepared, so Start runs exactly what Check saw (AD-6:
	// one rewrite per launch). It may be nil.
	Prepared any
}

// Engine is the port the Session reaches the engine through. The adapter
// behind it owns the rewrite and the Connect engine.
type Engine interface {
	// Check reads and rewrites the pipeline file opts.Pipeline, with the
	// rewrite options in opts (DebugGroup, InputClasses, KeepConsumerGroup,
	// Output),
	// and parses and
	// lints it without running anything, resolving ${VAR} through
	// opts.Lookup as Start does (AD-22). It ignores opts.MaxMessages and
	// opts.Prepared. It returns the rewritten configs in
	// CheckResult.Prepared and the input findings in CheckResult.Inputs.
	// Its error fails the launch; a non-Error one is PIPELINE_INVALID.
	Check(opts StartOptions) (CheckResult, error)
	// Start captures one batch from the pipeline's input, keeps at most
	// opts.MaxMessages of it, and steps it through the processors, calling
	// a.Captured once, a.Arrive at every stop position and a.Deliver for
	// every batch rpcn_debug_sink receives. It runs opts.Prepared, the
	// configs a Check returned, without reading the file again (AD-6); with
	// a nil Prepared it reads and rewrites opts.Pipeline itself. It returns
	// nil once the batch is delivered, an Error with code NOTHING_CAPTURED
	// when the input yields nothing, or an error when ctx ends or the run
	// fails.
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
	// Lookup resolves the pipeline's ${VAR} interpolations (AD-22): the
	// launch's env, then its envFile, then the process environment. nil
	// means the process environment only.
	Lookup func(name string) (string, bool)
	// DebugGroup is the debug-group base rpcn-debug-<os-user> (AD-5), a
	// rewrite option. The Session fills it from the process user.
	DebugGroup string
	// InputClasses are the launch's inputClasses (AD-5), a rewrite option:
	// the class each named input component gets instead of the input
	// table's. A name that isn't an input component is LAUNCH_INVALID.
	InputClasses map[string]InputClass
	// KeepConsumerGroup is the launch's keepConsumerGroup (AD-5), a rewrite
	// option: the capture keeps every consumer group as configured instead
	// of overriding it with the debug group.
	KeepConsumerGroup bool
	// Output is the launch's output (AD-5, AD-21), a rewrite option:
	// where rpcn_debug_sink sends the batches it receives. "" is dry-run.
	Output OutputMode
	// Prepared is CheckResult.Prepared from this launch's Check, passed
	// back to Start unchanged. nil makes Start load the pipeline itself.
	Prepared any
}

// Frontend receives the Session's events. Emit may be called from any
// goroutine, but never while the Session holds its state lock.
type Frontend interface {
	Emit(Event)
}
