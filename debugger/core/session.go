package core

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"maps"
	"os"
	"path/filepath"
	"slices"
	"sync"
)

// ThreadID is the root thread: every stepper entry whose ctx has no frame
// runs on it (AD-17). Its id is stable for the whole run.
const ThreadID = 1

// rootThreadName is the root thread's name in frontends.
const rootThreadName = "batch"

// unverifiedMessage is the reason every breakpoint is unverified for now.
const unverifiedMessage = "Breakpoints are not resolved yet: this version of rpcn-debug doesn't stop at them"

// Frame is one live stepper entry at a stop, with the Snapshot of its own
// latest Arrive (AD-11): a parent frame shows its batches before the child
// that is running.
type Frame struct {
	// ID is the entry's frame id (also in Snapshot.Position.FrameID). It
	// is stable for the entry; frontends mint their own per-stop ids.
	ID int
	// StepperID is the rewrite's id of the entry's stepper.
	StepperID int
	// Name is the user path of the entry's child list, e.g.
	// pipeline.processors[0].branch.processors.
	Name string
	// RepeatLabel is the entry's repeat label; empty until entry 2.7.
	RepeatLabel string
	// Snapshot is the message state at the frame's latest stop position.
	Snapshot Snapshot
}

// Stop is the stop the Session is paused at: the stopped thread and its
// frame stack. It becomes invalid on resume (AD-17).
type Stop struct {
	// ID is minted per stop: frontends derive their frame ids and
	// variablesReference values from it.
	ID       int
	ThreadID int
	Reason   string
	// Frames is the stopped thread's stack, innermost first.
	Frames []Frame

	// The position the step commands measure from (AD-10).
	depth   int
	frameID int
	atEnd   bool
}

// Thread is one live thread (AD-17).
type Thread struct {
	ID   int
	Name string
}

// stepMode is what a thread's last resume asked for.
type stepMode int

const (
	stepNone stepMode = iota // Continue: stop nowhere
	stepNext
	stepIn
	stepOut
)

// stepState is one thread's step state (AD-9), read in Arrive under the
// gate. depth, frameID and atEnd are those of the stop the command was
// issued at.
type stepState struct {
	mode    stepMode
	depth   int
	frameID int
	atEnd   bool
}

type thread struct {
	id      int
	name    string
	started bool
	exited  bool
	step    stepState
}

// liveFrame is the Session's record of one live stepper entry.
type liveFrame struct {
	ref      FrameRef
	children int // -1 when unknown
	name     string
	snap     Snapshot
}

// Session owns all debug state (AD-3). A frontend drives it only through Do
// with the protocol commands, and reads the stop with Stopped and the
// threads with Threads;
// steppers reach it through Arrive; it reaches the engine only through the
// Engine port and reports only through Frontend.Emit.
//
// Its stop predicate is AD-10's: after ConfigurationDone the first Arrive
// stops with reason entry; then Next, StepIn, StepOut, Continue and Pause
// decide the next stop, per thread, under the pause gate (AD-9).
type Session struct {
	engine Engine
	fe     Frontend
	gate   gate

	mu          sync.Mutex
	pipeline    string
	cwd         string
	maxMessages int
	env         map[string]string
	envFile     string
	lookup      func(string) (string, bool) // env, then envFile, then the process (AD-22)
	lines       LineMap
	translate   Translate
	runDone     bool // the run emitted Terminated: Log sends nothing more
	launched    bool
	configured  bool
	breakpoints map[string][]int
	exceptions  []string

	// Stepping (AD-9, AD-10, AD-17). The stop predicate reads it in Arrive
	// while the gate is held.
	entryPending bool // the first Arrive after ConfigurationDone stops
	pausePending bool // the next Arrive on any thread stops
	frameSeq     int
	frames       map[int]*liveFrame
	threads      map[int]*thread
	stopSeq      int
	stop         *Stop
	resume       chan struct{}
	ended        bool
	endedCh      chan struct{}
	cancel       context.CancelFunc
	done         chan struct{}
}

// NewSession returns a Session that runs pipelines through engine and sends
// its events to fe.
func NewSession(engine Engine, fe Frontend) *Session {
	return &Session{
		engine:      engine,
		fe:          fe,
		endedCh:     make(chan struct{}),
		breakpoints: map[string][]int{},
		frames:      map[int]*liveFrame{},
		threads:     map[int]*thread{},
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
	case Next:
		return nil, s.resumeRun(c.ThreadID, stepNext)
	case StepIn:
		return nil, s.resumeRun(c.ThreadID, stepIn)
	case StepOut:
		return nil, s.resumeRun(c.ThreadID, stepOut)
	case Continue:
		return nil, s.resumeRun(c.ThreadID, stepNone)
	case Pause:
		return nil, s.pause()
	case Terminate:
		s.terminate()
		return nil, nil
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
	var fileEnv map[string]string
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
		raw, err := os.ReadFile(envFile)
		if err != nil {
			return NewError(CodeLaunchInvalid, `"envFile": %v`, err)
		}
		// The error names the line, never its value (AD-22).
		if fileEnv, err = parseDotenv(string(raw)); err != nil {
			return NewError(CodeLaunchInvalid, `envFile %v (in %s)`, err, envFile)
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

	// Check and Start resolve ${VAR} through the same lookup, so lint and
	// parse see the values the run gets (AD-22).
	lookup := EnvLookup(fileEnv, env)
	res, err := s.engine.Check(StartOptions{Pipeline: pipeline, Cwd: cwd, Lookup: lookup})
	if err != nil {
		ce, _ := AsError(err, CodePipelineInvalid).(Error)
		ce.Message = TranslateText(res.Translator, ce.Message)
		return ce
	}

	s.mu.Lock()
	s.pipeline, s.cwd, s.lines, s.translate, s.launched = pipeline, cwd, res.Lines, res.Translator, true
	s.maxMessages, s.env, s.envFile, s.lookup = c.MaxMessages, env, envFile, lookup
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
	s.configured, s.entryPending = true, true
	// The root thread exists from here on, while the input is still being
	// captured, so a frontend can list it and Pause it. Its ThreadStarted
	// goes out from the run, after the response to this command.
	s.threadLocked(ThreadID).started = true

	ctx, cancel := context.WithCancel(context.Background())
	s.cancel, s.done = cancel, make(chan struct{})
	opts := StartOptions{Pipeline: s.pipeline, Cwd: s.cwd, MaxMessages: s.maxMessages, Lookup: s.lookup}
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
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if !ended {
		s.fe.Emit(ThreadStarted{ThreadID: ThreadID, Name: rootThreadName})
	}
	err := s.engine.Start(ctx, opts, s)

	s.mu.Lock()
	ended = s.ended
	s.mu.Unlock()
	if ended {
		return // Terminate: the frontend is gone.
	}
	switch {
	case HasCode(err, CodeNothingCaptured):
		s.fe.Emit(Output{Category: "console", Text: err.Error() + "\n"})
	case err != nil:
		s.mu.Lock()
		tr := s.translate
		s.mu.Unlock()
		s.fe.Emit(Output{Category: "stderr", Text: "The debug run failed: " + TranslateText(tr, err.Error()) + "\n"})
	}
	// The root thread ends with the run (AD-17).
	s.mu.Lock()
	root := s.threads[ThreadID]
	exitRoot := root != nil && root.started && !root.exited
	if exitRoot {
		root.exited = true
	}
	s.mu.Unlock()
	if exitRoot {
		s.fe.Emit(ThreadExited{ThreadID: ThreadID})
	}
	// Records the streams log while shutting down would follow Terminated.
	s.mu.Lock()
	s.runDone = true
	s.mu.Unlock()
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

// Translator returns the Translate of the launched pipeline, or nil before
// a successful launch. The log handler translates engine records with it
// (AD-6, AD-15).
func (s *Session) Translator() Translate {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.translate
}

// Log reports a log record's text to the frontend as an Output event:
// category stderr for errors, console otherwise (AD-15). It sends nothing
// once the session has ended or its run has emitted Terminated. It must not be called while the caller
// holds a lock the frontend's Emit takes.
func (s *Session) Log(level slog.Level, text string) {
	s.mu.Lock()
	ended := s.ended || s.runDone
	s.mu.Unlock()
	if ended {
		return
	}
	category := "console"
	if level >= slog.LevelError {
		category = "stderr"
	}
	s.fe.Emit(Output{Category: category, Text: text})
}

// resolve returns path made absolute against the absolute directory base.
func resolve(base, path string) string {
	if filepath.IsAbs(path) {
		return filepath.Clean(path)
	}
	return filepath.Join(base, path)
}

// ForkThread starts a thread for a stepper entry that has no frame in its
// ctx, or whose config is concurrent (AD-17), and returns its id. In this
// epic every entry runs on the root thread, which ConfigurationDone already
// started: ForkThread returns ThreadID (starting it, with a ThreadStarted
// event, only if it isn't yet). Concurrent
// forking arrives with epic-concurrency.
func (s *Session) ForkThread() int {
	s.mu.Lock()
	started := s.forkThreadLocked()
	s.mu.Unlock()
	if started {
		s.fe.Emit(ThreadStarted{ThreadID: ThreadID, Name: rootThreadName})
	}
	return ThreadID
}

// forkThreadLocked marks the root thread started and reports whether it
// just started.
func (s *Session) forkThreadLocked() bool {
	t := s.threadLocked(ThreadID)
	if t.started || s.ended {
		return false
	}
	t.started = true
	return true
}

// ExitThread is called by the entry that forked thread id when it returns
// (AD-17). The root thread is shared by every top-level entry, so it ends
// with the run instead, and ExitThread leaves it running.
func (s *Session) ExitThread(id int) {
	if id == ThreadID {
		return
	}
	s.mu.Lock()
	t := s.threads[id]
	exit := t != nil && t.started && !t.exited && !s.ended
	if exit {
		t.exited = true
	}
	s.mu.Unlock()
	if exit {
		s.fe.Emit(ThreadExited{ThreadID: id})
	}
}

// Threads returns the live threads, in id order.
func (s *Session) Threads() []Thread {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Thread{}
	for _, t := range s.threads {
		if t.started && !t.exited {
			out = append(out, Thread{ID: t.id, Name: t.name})
		}
	}
	slices.SortFunc(out, func(a, b Thread) int { return a.ID - b.ID })
	return out
}

// threadLocked returns thread id's record, creating it unstarted.
func (s *Session) threadLocked(id int) *thread {
	t := s.threads[id]
	if t == nil {
		name := rootThreadName
		if id != ThreadID {
			name = fmt.Sprintf("thread %d", id)
		}
		t = &thread{id: id, name: name}
		s.threads[id] = t
	}
	return t
}

// Enter implements Arriver. It mints the entry's frame, registers it as
// live and pushes it on ctx's frame stack (AD-17). An entry with no frame in
// ctx runs on the root thread (ForkThread); otherwise it inherits its parent
// frame's thread, and its call site is the parent's latest stop.
func (s *Session) Enter(ctx context.Context, stepperID, children int) (context.Context, func()) {
	parent, _, nested := TopFrame(ctx)
	var tid int
	if nested {
		tid = parent.ThreadID
	} else {
		tid = s.ForkThread()
	}

	s.mu.Lock()
	s.frameSeq++
	ref := FrameRef{FrameID: s.frameSeq, StepperID: stepperID, ThreadID: tid}
	if nested {
		ref.CallSite = CallSite{FrameID: parent.FrameID, StepperID: parent.StepperID}
		if pf := s.frames[parent.FrameID]; pf != nil {
			ref.CallSite.ChildIndex = pf.snap.Position.ChildIndex
		}
	}
	s.frames[ref.FrameID] = &liveFrame{ref: ref, children: children, name: s.frameName(stepperID, children)}
	s.mu.Unlock()

	exit := func() {
		s.mu.Lock()
		delete(s.frames, ref.FrameID)
		s.mu.Unlock()
		if !nested {
			s.ExitThread(tid)
		}
	}
	return PushFrame(ctx, ref), exit
}

// frameName is the user path of stepper stepperID's child list: the
// LineMap's end-of-entry stop (AD-10). s.mu must be held.
func (s *Session) frameName(stepperID, children int) string {
	if s.lines != nil && children >= 0 {
		if p, ok := s.lines.Position(stepperID, children); ok && p.UserPath != "" {
			return p.UserPath
		}
	}
	return fmt.Sprintf("stepper %d", stepperID)
}

// Arrive implements Arriver. It takes the gate and, while holding it,
// records batches as the Snapshot of the frame on top of ctx and decides
// whether to stop (AD-9, AD-10). If it stops, it emits Paused and blocks
// under the gate until a resume, Terminate or ctx's end. It emits nothing
// when it doesn't stop.
func (s *Session) Arrive(ctx context.Context, stepperID, childIndex int, batches []Batch) error {
	s.gate.lock()
	defer s.gate.unlock()

	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return NewError(CodeSessionEnded, "the debug session ended")
	}
	if batches == nil {
		batches = []Batch{}
	}

	ref, depth, ok := TopFrame(ctx)
	lf := s.frames[ref.FrameID]
	if !ok || lf == nil || lf.ref.StepperID != stepperID {
		// An Arrive outside any entry (no Enter): a one-off frame on the
		// root thread.
		ref, depth = FrameRef{StepperID: stepperID, ThreadID: ThreadID}, 1
		lf = &liveFrame{ref: ref, children: -1, name: s.frameName(stepperID, -1)}
	}
	lf.snap = s.snapshot(ref, childIndex, batches)

	if !s.configured {
		s.mu.Unlock()
		return nil
	}
	t := s.threadLocked(ref.ThreadID)
	atEnd := lf.children >= 0 && childIndex == lf.children
	reason, stop := s.shouldStop(t.step, ref, depth, atEnd)
	if !stop {
		s.mu.Unlock()
		return nil
	}
	t.step = stepState{}

	frames := []Frame{lfFrame(lf)}
	if ok && s.frames[ref.FrameID] == lf {
		frames = frames[:0]
		for _, r := range FrameStack(ctx) {
			if f := s.frames[r.FrameID]; f != nil {
				frames = append(frames, lfFrame(f))
			}
		}
	}
	s.stopSeq++
	st := &Stop{
		ID: s.stopSeq, ThreadID: ref.ThreadID, Reason: reason, Frames: frames,
		depth: depth, frameID: ref.FrameID, atEnd: atEnd,
	}
	resume := make(chan struct{})
	s.stop, s.resume = st, resume
	s.mu.Unlock()

	s.fe.Emit(Paused{ThreadID: ref.ThreadID, Reason: reason})

	select {
	case <-resume:
		return nil
	case <-s.endedCh:
		return NewError(CodeSessionEnded, "the debug session ended")
	case <-ctx.Done():
		s.mu.Lock()
		if s.resume == resume {
			s.stop, s.resume = nil, nil
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

// shouldStop is the stop predicate (AD-9, AD-10) for an Arrive of frame ref
// at stack depth depth; atEnd is true at the frame's end stop (childIndex
// = n). step is the arriving thread's step state, set at the stop the
// last command was issued at (depth d0, frame f0):
//
//   - Next stops at depth < d0 or in f0.
//   - StepIn stops at the very next Arrive.
//   - StepOut stops at depth < d0, or at f0's end stop when the command
//     wasn't issued there.
//
// A top-level frame's parent is the run itself, so for d0 = 1 "the
// parent's next stop" is the next stop of another top-level entry, e.g.
// the output.processors stepper after pipeline.processors.
//
// A pending entry or Pause stops any Arrive first. s.mu must be held.
func (s *Session) shouldStop(step stepState, ref FrameRef, depth int, atEnd bool) (string, bool) {
	if s.entryPending {
		s.entryPending, s.pausePending = false, false
		return ReasonEntry, true
	}
	if s.pausePending {
		s.pausePending = false
		return ReasonPause, true
	}
	same := ref.FrameID == step.frameID
	topLevelSibling := step.depth == 1 && depth == 1 && !same
	switch step.mode {
	case stepIn:
		return ReasonStep, true
	case stepNext:
		return ReasonStep, depth < step.depth || same || topLevelSibling
	case stepOut:
		return ReasonStep, depth < step.depth || topLevelSibling || (same && atEnd && !step.atEnd)
	default:
		return "", false
	}
}

// snapshot builds the Snapshot of frame ref at childIndex. s.mu must be
// held.
func (s *Session) snapshot(ref FrameRef, childIndex int, batches []Batch) Snapshot {
	pos := Position{}
	if s.lines != nil {
		if p, ok := s.lines.Position(ref.StepperID, childIndex); ok {
			pos = p
		}
	}
	pos.ThreadID, pos.FrameID = ref.ThreadID, ref.FrameID
	pos.StepperID, pos.ChildIndex, pos.File = ref.StepperID, childIndex, s.pipeline
	return Snapshot{SchemaVersion: SchemaVersion, Position: pos, Batches: batches}
}

func lfFrame(f *liveFrame) Frame {
	return Frame{ID: f.ref.FrameID, StepperID: f.ref.StepperID, Name: f.name, RepeatLabel: f.ref.RepeatLabel, Snapshot: f.snap}
}

// Stopped returns the stop the session is paused at, if any. Its Snapshots
// are immutable: callers must not modify them.
func (s *Session) Stopped() (Stop, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.stop == nil {
		return Stop{}, false
	}
	return *s.stop, true
}

// resumeRun resumes every thread (AD-9) and gives threadID (0: the stopped
// thread) the step state mode, measured from the current stop.
func (s *Session) resumeRun(threadID int, mode stepMode) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return NewError(CodeSessionEnded, "the debug session ended")
	}
	if s.resume == nil {
		return NewError(CodeNotStopped, "the session is not stopped")
	}
	st := s.stop
	if threadID == 0 || s.threads[threadID] == nil {
		threadID = st.ThreadID
	}
	t := s.threadLocked(threadID)
	t.step = stepState{mode: mode}
	if threadID == st.ThreadID {
		t.step.depth, t.step.frameID, t.step.atEnd = st.depth, st.frameID, st.atEnd
	}
	close(s.resume)
	s.stop, s.resume = nil, nil
	return nil
}

// pause makes the next Arrive on any thread stop with reason pause. While
// the session is stopped, or before it runs, it changes nothing.
func (s *Session) pause() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.ended {
		return NewError(CodeSessionEnded, "the debug session ended")
	}
	if s.stop != nil || !s.configured {
		return nil
	}
	s.pausePending = true
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
	s.stop, s.resume = nil, nil
	cancel, done := s.cancel, s.done
	s.mu.Unlock()

	if cancel != nil {
		cancel()
	}
	if done != nil {
		<-done
	}
}
