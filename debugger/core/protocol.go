package core

// The closed protocol (AD-3). A frontend drives the Session only with these
// commands, through Session.Do, and the Session reports only with these
// events, through Frontend.Emit. Adding a member is a spine change.

// Command is one protocol command. The set is sealed.
type Command interface{ command() }

// Launch checks the pipeline file. Nothing runs until ConfigurationDone.
// Frontends decode and validate their launch arguments into it (for DAP,
// dap.LaunchConfig, which owns the defaults; AD-12).
type Launch struct {
	// Pipeline is the pipeline YAML file; a relative one resolves against
	// Cwd.
	Pipeline string
	// Cwd is the directory relative paths resolve against. A relative Cwd
	// resolves against the process's working directory, and "" means that
	// directory.
	Cwd string
	// MaxMessages is the most messages of the captured batch to step
	// (AD-4); the rest are dropped, never acked. 0 means no limit.
	MaxMessages int
	// Env holds environment variables for the pipeline's ${VAR}
	// interpolations. Carried, not applied yet (entry 2.4, AD-22).
	Env map[string]string
	// EnvFile names a file of environment variables; a relative one
	// resolves against Cwd, and it must exist. Carried, not applied yet
	// (entry 2.4, AD-22).
	EnvFile string
}

// ConfigurationDone starts the debuggee.
type ConfigurationDone struct{}

// SetBreakpoints replaces the breakpoints of one file (AD-18). Every
// breakpoint is answered unverified until epic-breakpoints-and-evaluation
// resolves them.
type SetBreakpoints struct {
	File       string
	Lines      []int
	Conditions []string
}

// SetExceptionBreakpoints sets the exception filters. No filter is offered
// yet; on-error arrives with epic-breakpoints-and-evaluation.
type SetExceptionBreakpoints struct {
	Filters []string
}

// Next steps over (AD-10). In this entry it runs to the end.
type Next struct{ ThreadID int }

// StepIn steps into a child stepper or Bloblang statements. Not supported
// yet: entry 2.6 (frames and threads) implements it.
type StepIn struct{ ThreadID int }

// StepOut runs to the end of the current entry. Not supported yet: entry
// 2.6 implements it.
type StepOut struct{ ThreadID int }

// Continue resumes all threads.
type Continue struct{ ThreadID int }

// Pause stops the next Arrive on any thread. Not supported yet: entry 2.6
// implements it.
type Pause struct{ ThreadID int }

// Evaluate evaluates a Bloblang expression against a frame (AD-13). Not
// supported yet: epic-breakpoints-and-evaluation implements it.
type Evaluate struct {
	FrameID    int
	Expression string
	// Context is the frontend's context, e.g. watch, repl or hover.
	Context string
}

// SetMessage edits a message of the top frame of the stopped thread
// (AD-19). Not supported yet: epic-edit-restart-replay implements it.
type SetMessage struct {
	FrameID int
	Batch   int
	Index   int
	// Content replaces the content when non-nil.
	Content []byte
	// Metadata replaces the metadata when non-nil.
	Metadata []Meta
}

// SaveCapture writes the captured batch to a capture file (AD-11, AD-22).
// Not supported yet: epic-edit-restart-replay implements it.
type SaveCapture struct{ Path string }

// Restart re-steps the captured batch (AD-20). Not supported yet:
// epic-edit-restart-replay implements it.
type Restart struct{}

// Terminate ends the session: it releases a stopped stepper, stops the
// engine and waits for its streams. No event follows it.
type Terminate struct{}

func (Launch) command()                  {}
func (ConfigurationDone) command()       {}
func (SetBreakpoints) command()          {}
func (SetExceptionBreakpoints) command() {}
func (Next) command()                    {}
func (StepIn) command()                  {}
func (StepOut) command()                 {}
func (Continue) command()                {}
func (Pause) command()                   {}
func (Evaluate) command()                {}
func (SetMessage) command()              {}
func (SaveCapture) command()             {}
func (Restart) command()                 {}
func (Terminate) command()               {}

// Result is what a command answers with. Commands that answer nothing
// return a nil Result. The set is sealed.
type Result interface{ result() }

// SetBreakpointsResult answers SetBreakpoints, one result per line, in
// order.
type SetBreakpointsResult struct {
	Breakpoints []BreakpointResult
}

func (SetBreakpointsResult) result() {}

// BreakpointResult is the resolution of one breakpoint.
type BreakpointResult struct {
	// Line is the requested line, or the line it was moved to when
	// verified.
	Line     int
	Verified bool
	// Message tells the user why a breakpoint is unverified.
	Message string
}

// Event is one protocol event. The set is sealed.
type Event interface{ event() }

// ThreadStarted reports a new thread (AD-17).
type ThreadStarted struct {
	ThreadID int
	Name     string
}

// ThreadExited reports the end of a thread.
type ThreadExited struct{ ThreadID int }

// StepStarted reports a new stepper entry (frame) with its repeat label
// (AD-10).
type StepStarted struct {
	ThreadID    int
	FrameID     int
	StepperID   int
	RepeatLabel string
}

// StepFinished reports the end of a stepper entry.
type StepFinished struct {
	ThreadID int
	FrameID  int
}

// Paused reports a stop. Only the thread that stopped is stopped (AD-9).
type Paused struct {
	ThreadID int
	// Reason is one of entry, step, breakpoint, exception or pause.
	Reason string
}

// Continued reports a resume. Every resume is process-wide (AD-9).
type Continued struct{ ThreadID int }

// MessagesSkipped reports messages a try stepper skipped (AD-11).
type MessagesSkipped struct {
	ThreadID int
	FrameID  int
	Skipped  []Skipped
}

// AttemptStarted reports a new retry attempt with its repeat label.
type AttemptStarted struct {
	ThreadID    int
	FrameID     int
	RepeatLabel string
}

// StatementStepped reports a Bloblang statement step (AD-13).
type StatementStepped struct {
	ThreadID int
	FrameID  int
	Line     int
}

// BreakpointResolved reports a breakpoint resolved after Launch (AD-18).
type BreakpointResolved struct {
	File       string
	Breakpoint BreakpointResult
}

// Delivered reports what rpcn_debug_sink received (AD-21).
type Delivered struct{ Snapshot Snapshot }

// Output reports a message for the user.
type Output struct {
	// Category is console, stdout or stderr.
	Category string
	Text     string
}

// Terminated reports the end of the debuggee run.
type Terminated struct{}

func (ThreadStarted) event()      {}
func (ThreadExited) event()       {}
func (StepStarted) event()        {}
func (StepFinished) event()       {}
func (Paused) event()             {}
func (Continued) event()          {}
func (MessagesSkipped) event()    {}
func (AttemptStarted) event()     {}
func (StatementStepped) event()   {}
func (BreakpointResolved) event() {}
func (Delivered) event()          {}
func (Output) event()             {}
func (Terminated) event()         {}
