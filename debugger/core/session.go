package core

import (
	"context"
	"os"
	"path/filepath"
	"sync"
)

// ThreadID is the one (root) thread of this entry. Threads per stepper entry
// arrive with AD-17 (entry 2.6).
const ThreadID = 1

// unverifiedMessage is the reason every breakpoint is unverified for now.
const unverifiedMessage = "Breakpoints are not resolved yet: this version of rpcn-debug doesn't stop at them"

// Frame is the stop the Session is paused at: one stepper entry, positioned
// before child Snapshot.Position.ChildIndex. Its ID becomes invalid on
// resume (AD-17).
type Frame struct {
	// ID is the frame id, also in Snapshot.Position.FrameID.
	ID int
	// Snapshot is the message state at the stop (AD-11).
	Snapshot Snapshot
}

// Session owns all debug state (AD-3). A frontend drives it only through Do
// with the protocol commands, and reads the paused frame with Frame;
// steppers reach it through Arrive; it reaches the engine only through the
// Engine port and reports only through Frontend.Emit.
//
// Its stop predicate is the tracer one: after ConfigurationDone the first
// Arrive stops with reason "entry", and Next or Continue both run to the end.
type Session struct {
	engine Engine
	fe     Frontend
	gate   gate

	mu          sync.Mutex
	pipeline    string
	cwd         string
	lines       LineMap
	launched    bool
	configured  bool
	stopNext    bool
	breakpoints map[string][]int
	exceptions  []string
	frameSeq    int
	frame       *Frame
	resume      chan struct{}
	ended       bool
	endedCh     chan struct{}
	cancel      context.CancelFunc
	done        chan struct{}
}

// NewSession returns a Session that runs pipelines through engine and sends
// its events to fe.
func NewSession(engine Engine, fe Frontend) *Session {
	return &Session{
		engine:      engine,
		fe:          fe,
		endedCh:     make(chan struct{}),
		breakpoints: map[string][]int{},
	}
}

// Do runs one protocol command. Its error is always an Error. Commands that
// a later entry implements return NOT_SUPPORTED and leave the session
// unchanged.
func (s *Session) Do(_ context.Context, cmd Command) (Result, error) {
	switch c := cmd.(type) {
	case Launch:
		return nil, s.launch(c.Pipeline, c.Cwd)
	case ConfigurationDone:
		return nil, s.configurationDone()
	case SetBreakpoints:
		return s.setBreakpoints(c), nil
	case SetExceptionBreakpoints:
		s.mu.Lock()
		s.exceptions = append([]string(nil), c.Filters...)
		s.mu.Unlock()
		return nil, nil
	case Next, Continue:
		return nil, s.resumeRun()
	case Terminate:
		s.terminate()
		return nil, nil
	case StepIn:
		return nil, notSupported("Step In", "entry 2.6 (frames and threads)")
	case StepOut:
		return nil, notSupported("Step Out", "entry 2.6 (frames and threads)")
	case Pause:
		return nil, notSupported("Pause", "entry 2.6 (frames and threads)")
	case Evaluate:
		return nil, notSupported("Evaluate", "epic-breakpoints-and-evaluation")
	case SetMessage:
		return nil, notSupported("Editing a message", "epic-edit-restart-replay")
	case SaveCapture:
		return nil, notSupported("Saving a capture", "epic-edit-restart-replay")
	case Restart:
		return nil, notSupported("Restart", "epic-edit-restart-replay")
	default:
		return nil, NewError(CodeNotSupported, "unknown command %T", cmd)
	}
}

func notSupported(what, by string) error {
	return NewError(CodeNotSupported, "%s is not supported yet: it arrives with %s", what, by)
}

// launch checks the pipeline file. A relative pipeline resolves against cwd,
// or against the process's working directory when cwd is empty. Nothing runs
// until ConfigurationDone.
func (s *Session) launch(pipeline, cwd string) error {
	if pipeline == "" {
		return NewError(CodeLaunchInvalid, `the launch configuration has no "pipeline": set it to the pipeline YAML file to debug`)
	}
	if !filepath.IsAbs(pipeline) {
		base := cwd
		if base == "" {
			wd, err := os.Getwd()
			if err != nil {
				return NewError(CodeLaunchInvalid, "resolve pipeline %q: %v", pipeline, err)
			}
			base = wd
		}
		pipeline = filepath.Join(base, pipeline)
	}
	// A relative cwd must not leave a relative path: frontends open and
	// highlight the frame's file by it.
	abs, err := filepath.Abs(pipeline)
	if err != nil {
		return NewError(CodeLaunchInvalid, "resolve pipeline %q: %v", pipeline, err)
	}
	pipeline = abs

	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return NewError(CodeSessionEnded, "the session has ended")
	}
	if s.launched {
		s.mu.Unlock()
		return NewError(CodeLaunchInvalid, "the session was already launched")
	}
	s.mu.Unlock()

	lines, err := s.engine.Check(pipeline, cwd)
	if err != nil {
		return AsError(err, CodePipelineInvalid)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pipeline, s.cwd, s.lines, s.launched = pipeline, cwd, lines, true
	return nil
}

// configurationDone starts the debuggee: the engine captures one batch and
// steps it. It returns at once; the run reports through the Frontend.
func (s *Session) configurationDone() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.launched {
		return NewError(CodeLaunchInvalid, "configurationDone before a successful launch")
	}
	if s.configured || s.ended {
		return nil
	}
	s.configured, s.stopNext = true, true

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	go s.run(ctx, s.pipeline, s.cwd, s.done)
	return nil
}

// setBreakpoints stores the file's breakpoints and answers each one
// unverified: resolution arrives with epic-breakpoints-and-evaluation.
func (s *Session) setBreakpoints(c SetBreakpoints) Result {
	s.mu.Lock()
	s.breakpoints[c.File] = append([]int(nil), c.Lines...)
	s.mu.Unlock()

	res := SetBreakpointsResult{Breakpoints: make([]BreakpointResult, len(c.Lines))}
	for i, l := range c.Lines {
		res.Breakpoints[i] = BreakpointResult{Line: l, Message: unverifiedMessage}
	}
	return res
}

func (s *Session) run(ctx context.Context, pipeline, cwd string, done chan struct{}) {
	defer close(done)
	err := s.engine.Start(ctx, pipeline, cwd, s)

	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if ended {
		return // Terminate: the frontend is gone.
	}
	switch {
	case HasCode(err, CodeNothingCaptured):
		s.fe.Emit(Output{Category: "console", Text: err.Error() + "\n"})
	case err != nil:
		s.fe.Emit(Output{Category: "stderr", Text: "The debug run failed: " + err.Error() + "\n"})
	}
	s.fe.Emit(Terminated{})
}

// Arrive implements Arriver. It takes the gate, decides whether to stop and,
// if it stops, keeps a Snapshot of batches for the frame, emits Paused and
// blocks under the gate until Next, Continue, Terminate or ctx's end (AD-9).
func (s *Session) Arrive(ctx context.Context, stepperID, childIndex int, batches []Batch) error {
	s.gate.lock()
	defer s.gate.unlock()

	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return NewError(CodeSessionEnded, "the debug session ended")
	}
	if !s.configured || !s.stopNext {
		s.mu.Unlock()
		return nil
	}
	s.stopNext = false

	s.frameSeq++
	pos := Position{}
	if s.lines != nil {
		if p, ok := s.lines.Position(stepperID, childIndex); ok {
			pos = p
		}
	}
	pos.ThreadID, pos.FrameID = ThreadID, s.frameSeq
	pos.StepperID, pos.ChildIndex, pos.File = stepperID, childIndex, s.pipeline
	if batches == nil {
		batches = []Batch{}
	}
	f := &Frame{ID: s.frameSeq, Snapshot: Snapshot{
		SchemaVersion: SchemaVersion,
		Position:      pos,
		Batches:       batches,
	}}
	resume := make(chan struct{})
	s.frame, s.resume = f, resume
	s.mu.Unlock()

	s.fe.Emit(Paused{ThreadID: ThreadID, Reason: "entry"})

	select {
	case <-resume:
		return nil
	case <-s.endedCh:
		return NewError(CodeSessionEnded, "the debug session ended")
	case <-ctx.Done():
		s.mu.Lock()
		if s.resume == resume {
			s.frame, s.resume = nil, nil
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

// Frame returns the stop the session is paused at, if any. The Snapshot is
// immutable: callers must not modify it.
func (s *Session) Frame() (Frame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.frame == nil {
		return Frame{}, false
	}
	return *s.frame, true
}

func (s *Session) resumeRun() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return NewError(CodeSessionEnded, "the debug session ended")
	}
	if s.resume == nil {
		return NewError(CodeNotStopped, "the session is not stopped")
	}
	close(s.resume)
	s.frame, s.resume = nil, nil
	return nil
}

// terminate ends the session: it releases a stopped stepper, stops the
// engine and waits until its streams are stopped. Nothing is delivered after
// it, and no further event is sent. It is safe to call more than once.
func (s *Session) terminate() {
	s.mu.Lock()
	if !s.ended {
		s.ended = true
		close(s.endedCh)
	}
	s.frame, s.resume = nil, nil
	cancel, done := s.cancel, s.done
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
