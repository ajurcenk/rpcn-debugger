package core

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"maps"
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
	maxMessages int
	env         map[string]string // carried for entry 2.4, not applied
	envFile     string            // carried for entry 2.4, not applied
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
		return nil, s.launch(c)
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

// launch checks the launch and the pipeline file. A relative cwd resolves
// against the process's working directory, and relative pipeline and
// envFile paths against cwd. Nothing runs until ConfigurationDone.
func (s *Session) launch(c Launch) error {
	if c.Pipeline == "" {
		return NewError(CodeLaunchInvalid, `the launch configuration has no "pipeline": set it to the pipeline YAML file to debug`)
	}
	if c.MaxMessages < 0 {
		return NewError(CodeLaunchInvalid, `"maxMessages" must be at least 1, not %d`, c.MaxMessages)
	}
	// Every path ends up absolute: frontends open and highlight the frame's
	// file by it.
	cwd, err := filepath.Abs(c.Cwd)
	if err != nil {
		return NewError(CodeLaunchInvalid, `resolve "cwd" %q: %v`, c.Cwd, err)
	}
	pipeline := resolve(cwd, c.Pipeline)
	envFile := ""
	if c.EnvFile != "" {
		envFile = resolve(cwd, c.EnvFile)
		fi, err := os.Stat(envFile)
		if errors.Is(err, fs.ErrNotExist) {
			return NewError(CodeLaunchInvalid, `"envFile" %s does not exist`, envFile)
		}
		if err != nil {
			return NewError(CodeLaunchInvalid, `"envFile": %v`, err)
		}
		if fi.IsDir() {
			return NewError(CodeLaunchInvalid, `"envFile" %s is a directory, not a file`, envFile)
		}
	}
	var env map[string]string
	if c.Env != nil {
		env = make(map[string]string, len(c.Env))
		maps.Copy(env, c.Env)
	}

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

	res, err := s.engine.Check(pipeline, cwd)
	if err != nil {
		return AsError(err, CodePipelineInvalid)
	}

	s.mu.Lock()
	s.pipeline, s.cwd, s.lines, s.launched = pipeline, cwd, res.Lines, true
	s.maxMessages, s.env, s.envFile = c.MaxMessages, env, envFile
	s.mu.Unlock()
	// Outside the lock: Emit is never called while it is held.
	for _, w := range res.Warnings {
		s.fe.Emit(Output{Category: "console", Text: "Warning: " + w + "\n"})
	}
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
	opts := StartOptions{Pipeline: s.pipeline, Cwd: s.cwd, MaxMessages: s.maxMessages}
	go s.run(ctx, opts, s.done)
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

func (s *Session) run(ctx context.Context, opts StartOptions, done chan struct{}) {
	defer close(done)
	err := s.engine.Start(ctx, opts, s)

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

// Captured implements Arriver. When the capture dropped messages for
// maxMessages, it tells the user how many it kept.
func (s *Session) Captured(kept, total int) {
	if kept >= total {
		return
	}
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if ended {
		return
	}
	s.fe.Emit(Output{Category: "console", Text: fmt.Sprintf("captured %d of %d messages (maxMessages)\n", kept, total)})
}

// resolve returns path made absolute against the absolute directory base.
func resolve(base, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(base, path)
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
