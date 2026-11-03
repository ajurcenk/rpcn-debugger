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
	"time"
)

// captureWait is how long a capture may deliver nothing before the Session
// says it is waiting for the input (AD-4).
const captureWait = 5 * time.Second

// unverifiedMessage is the reason every breakpoint is unverified for now.
const unverifiedMessage = "Breakpoints are not resolved yet: this version of rpcn-debug doesn't stop at them"

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
	// captureWait is how long the run may go without Captured before the
	// Session emits its one waiting note. Tests shorten it.
	captureWait time.Duration

	mu           sync.Mutex
	pipeline     string
	cwd          string
	maxMessages  int
	env          map[string]string
	envFile      string
	lookup       func(string) (string, bool) // env, then envFile, then the process (AD-22)
	lines        LineMap
	translate    Translate
	debugGroup   string                // the debug-group base rewrite option (AD-5)
	inputClasses map[string]InputClass // the inputClasses rewrite option (AD-5)
	keepGroup    bool                  // the keepConsumerGroup rewrite option (AD-5)
	output       OutputMode            // the output rewrite option (AD-21)
	saveName     string                // the saveCapture file name under <cwd>/.rpcn-debug/, or "" (AD-11, AD-22)
	replay       *CapturedBatch        // the replay file's batch, stepped instead of a capture (AD-11); nil without replay
	replayNotes  []string              // the replay's start notes, sent by its run
	prepared     any                   // Check's configs, run by Capture and Step (AD-6: one rewrite per launch)
	delivery     Position              // where Delivered Snapshots are positioned (AD-21)
	delivered    int                   // batches delivered in this run, numbering them (AD-3); Restart resets it
	runDone      bool                  // the run ended: Log sends nothing more until a Restart
	captured     bool                  // the batch is captured: the waiting note is not sent
	kept         *CapturedBatch        // the captured batch, re-stepped by Restart (AD-20); nil before the capture
	gen          int                   // the run generation: Restart bumps it, and a stale run ends silently
	restartNoted bool                  // the first Restart's note went out (D4)
	launched     bool
	configured   bool
	breakpoints  map[string][]int
	exceptions   []string

	// Stepping (AD-9, AD-10, AD-17). The stop predicate reads it in Arrive
	// while the gate is held.
	entryPending bool // the first Arrive after ConfigurationDone stops
	pausePending bool // the next Arrive on any thread stops
	frameSeq     int
	frames       map[int]*liveFrame
	rootSites    map[siteKey]*siteCount // counters of entries with no parent frame
	budgetWarned map[int]bool           // steppers whose retry time-budget warning went out (AD-8)
	threads      map[int]*thread
	stopSeq      int
	stop         *Stop
	resume       chan struct{}
	ended        bool
	endedCh      chan struct{}
	cancel       context.CancelFunc
	done         chan struct{}
	replaced     chan struct{} // closed when a Restart replaces the run: its Arrives fail

	// restartMu serializes Restarts: one replaces the run at a time.
	restartMu sync.Mutex
}

// NewSession returns a Session that runs pipelines through engine and sends
// its events to fe.
func NewSession(engine Engine, fe Frontend) *Session {
	return &Session{
		engine:       engine,
		fe:           fe,
		captureWait:  captureWait,
		endedCh:      make(chan struct{}),
		breakpoints:  map[string][]int{},
		frames:       map[int]*liveFrame{},
		rootSites:    map[siteKey]*siteCount{},
		budgetWarned: map[int]bool{},
		threads:      map[int]*thread{},
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
		return nil, s.saveCaptureCmd(c.Path)
	case Restart:
		return nil, s.restart()
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
	if err := checkInputClasses(c.InputClasses); err != nil {
		return err
	}
	if err := checkOutputMode(c.Output); err != nil {
		return err
	}
	if c.SaveCapture != "" {
		if c.Replay != "" {
			return NewError(CodeLaunchInvalid, `"replay" and "saveCapture" can't be set together: a replay steps a capture file, and nothing is captured to save`)
		}
		if err := checkCaptureName(`"saveCapture"`, c.SaveCapture); err != nil {
			return err
		}
	}
	// Every path ends up absolute: frontends open and highlight the frame's
	// file by it.
	cwd, err := filepath.Abs(c.Cwd)
	if err != nil {
		return NewError(CodeLaunchInvalid, `resolve "cwd" %q: %v`, c.Cwd, err)
	}
	pipeline := resolve(cwd, c.Pipeline)
	replayPath := ""
	if c.Replay != "" {
		replayPath = resolve(cwd, c.Replay)
		if err := checkFile(`"replay"`, replayPath); err != nil {
			return err
		}
	}
	envFile := ""
	var fileEnv map[string]string
	if c.EnvFile != "" {
		envFile = resolve(cwd, c.EnvFile)
		if err := checkFile(`"envFile"`, envFile); err != nil {
			return err
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

	// Check, Capture and Step resolve ${VAR} through the same lookup, so
	// lint and parse see the values the run gets (AD-22).
	lookup := EnvLookup(fileEnv, env)
	group := debugGroup()
	var classes map[string]InputClass
	if c.InputClasses != nil {
		classes = maps.Clone(c.InputClasses)
	}
	// A replay runs as dry-run: its batch was already delivered once.
	output := c.Output.Effective()
	replayReal := replayPath != "" && output == OutputReal
	if replayReal {
		output = OutputDryRun
	}
	res, err := s.engine.Check(StartOptions{
		Pipeline: pipeline, Cwd: cwd, Lookup: lookup, DebugGroup: group, InputClasses: classes,
		KeepConsumerGroup: c.KeepConsumerGroup, Output: output, StepOnly: replayPath != "",
	})
	if err != nil {
		ce, _ := AsError(err, CodePipelineInvalid).(Error)
		ce.Message = TranslateText(res.Translator, ce.Message)
		return ce
	}
	var inputWarnings, replayNotes []string
	var replay *CapturedBatch
	if replayPath == "" {
		// The Session alone refuses on the input findings (AD-5, D1).
		if inputWarnings, err = inputDecision(pipeline, res.Inputs, c.AllowSideEffects); err != nil {
			return err
		}
	} else {
		// A replay never reads the input, so its classes are not decided
		// (AD-11): a rejected input replays.
		b, err := s.engine.LoadCapture(replayPath, c.MaxMessages)
		if err != nil {
			// The engine's error names the file.
			return NewError(CodeLaunchInvalid, `"replay": %v`, err)
		}
		replay = &b
		replayNotes = append(replayNotes, fmt.Sprintf("Replaying %s: %d messages; the input is not read.\n", replayPath, b.Kept))
		if replayReal {
			replayNotes = append(replayNotes, replayDryRunNote)
		}
	}

	s.mu.Lock()
	s.pipeline, s.cwd, s.lines, s.translate, s.launched = pipeline, cwd, res.Lines, res.Translator, true
	s.delivery, s.debugGroup, s.prepared, s.inputClasses = res.Delivery, group, res.Prepared, classes
	s.keepGroup, s.output, s.saveName = c.KeepConsumerGroup, output, c.SaveCapture
	s.replay, s.replayNotes = replay, replayNotes
	s.maxMessages, s.env, s.envFile, s.lookup = c.MaxMessages, env, envFile, lookup
	s.mu.Unlock()
	// Outside the lock: Emit is never called while it is held.
	for _, w := range slices.Concat(res.Warnings, inputWarnings) {
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
	s.cancel, s.done, s.replaced = cancel, make(chan struct{}), make(chan struct{})
	// Capture and Step run the configs Check produced: an edit to the file
	// after launch never desyncs them from s.lines (AD-6).
	opts := s.optionsLocked()
	opts.Prepared = s.prepared
	go s.run(ctx, opts, s.replay, s.gen, s.done)
	return nil
}

// replayDryRunNote is the console note of a replay launched with output:
// real.
const replayDryRunNote = "Replay runs as output: dry-run, so the batch is not written to the output again.\n"

// checkFile checks that path, the absolute path of the launch setting key,
// is an existing file.
func checkFile(key, path string) error {
	fi, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return NewError(CodeLaunchInvalid, `%s %s does not exist`, key, path)
	}
	if err != nil {
		return NewError(CodeLaunchInvalid, `%s: %v`, key, err)
	}
	if fi.IsDir() {
		return NewError(CodeLaunchInvalid, `%s %s is a directory, not a file`, key, path)
	}
	return nil
}

// optionsLocked returns the launch's options as launched, without
// Prepared. s.mu must be held.
func (s *Session) optionsLocked() StartOptions {
	return StartOptions{
		Pipeline: s.pipeline, Cwd: s.cwd, MaxMessages: s.maxMessages, Lookup: s.lookup,
		DebugGroup: s.debugGroup, InputClasses: s.inputClasses, KeepConsumerGroup: s.keepGroup,
		Output: s.output,
	}
}

// restartWindDown is how long a Restart lets the replaced run wind down
// before it cancels it.
const restartWindDown = 2 * time.Second

// restartNote is the console note of a session's first Restart (D4).
const restartNote = "Restart re-reads only the pipeline file; changes to launch.json need a new launch.\n"

// restartDryRunNote is the console note of a Restart under output: real.
const restartDryRunNote = "Restart runs as output: dry-run, so the batch is not written to the output again.\n"

// restart replaces the run with a new one that re-steps the captured batch
// (AD-20). It re-checks only the pipeline file, with the launch's options:
// the input block is not linted, the input classes are not decided again
// and nothing is captured. A failed Check leaves the current run as it is.
// Otherwise the old run is released (each of its Arrives fails, so it
// delivers nothing), cancelled if it lingers, and waited for. Its threads
// exit, the stepping state and the delivered-batch number start afresh
// (breakpoints and exception filters stay), and the new run stops at the
// entry stop. Under output: real the new run is dry-run.
func (s *Session) restart() error {
	s.restartMu.Lock()
	defer s.restartMu.Unlock()

	s.mu.Lock()
	switch {
	case s.ended:
		s.mu.Unlock()
		return NewError(CodeSessionEnded, "the debug session ended")
	case s.kept == nil:
		s.mu.Unlock()
		return NewError(CodeNotSupported, "Restart is not supported before the batch is captured")
	}
	opts := s.optionsLocked()
	wasReal := opts.Output.Effective() == OutputReal
	if wasReal {
		opts.Output = OutputDryRun
	}
	opts.StepOnly = true
	kept := *s.kept
	s.mu.Unlock()

	res, err := s.engine.Check(opts)
	if err != nil {
		ce, _ := AsError(err, CodePipelineInvalid).(Error)
		ce.Message = TranslateText(res.Translator, ce.Message)
		return ce
	}

	// Replace the run: a stale generation ends without a word.
	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return NewError(CodeSessionEnded, "the debug session ended")
	}
	s.gen++
	cancel, done, replaced := s.cancel, s.done, s.replaced
	s.mu.Unlock()
	// Releasing the run's stop lets it wind down on its own, which stops
	// its streams quickly; cancelling them mid-transaction stalls their
	// shutdown. A run that doesn't end soon (a write under output: real)
	// is cancelled.
	close(replaced)
	select {
	case <-done:
	case <-time.After(restartWindDown):
	}
	cancel()
	<-done

	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return NewError(CodeSessionEnded, "the debug session ended")
	}
	// A run that ended before it saw the new generation (runDone) is
	// replaced like a live one.
	exited := s.liveThreadsLocked()
	s.resetThreadsLocked()
	s.resetSteppingLocked()
	s.delivered, s.runDone = 0, false
	s.lines, s.translate, s.prepared, s.delivery = res.Lines, res.Translator, res.Prepared, res.Delivery
	s.entryPending = true
	s.threadLocked(ThreadID).started = true
	notes := []string{}
	if !s.restartNoted {
		s.restartNoted = true
		notes = append(notes, restartNote)
	}
	if wasReal {
		notes = append(notes, restartDryRunNote)
	}
	ctx, cancelRun := context.WithCancel(context.Background())
	s.cancel, s.done, s.replaced = cancelRun, make(chan struct{}), make(chan struct{})
	opts.Prepared, opts.StepOnly = res.Prepared, false
	gen, runDone := s.gen, s.done
	s.mu.Unlock()

	// Outside the lock: Emit is never called while it is held.
	for _, id := range exited {
		s.fe.Emit(ThreadExited{ThreadID: id})
	}
	for _, n := range notes {
		s.fe.Emit(Output{Category: "console", Text: n})
	}
	for _, w := range res.Warnings {
		s.fe.Emit(Output{Category: "console", Text: "Warning: " + w + "\n"})
	}
	go s.run(ctx, opts, &kept, gen, runDone)
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

// run is one debug run of generation gen: it captures the batch, unless
// kept is the batch a Restart re-steps or the launch's replay batch (which
// it keeps as captured, after the replay notes), saves it when the launch
// sets saveCapture, then steps it. It closes done when
// it returns. A run that a Restart replaced (its gen is stale) ends without
// a word: no note, ThreadExited or Terminated.
//
// A run that captured its batch doesn't terminate when it ends (D5, AD-20):
// after its own end notes it emits the run-finished note and the root
// ThreadExited, and the session waits for Restart or Terminate. A run that
// captured nothing, or failed before the capture, ends with Terminated.
func (s *Session) run(ctx context.Context, opts StartOptions, kept *CapturedBatch, gen int, done chan struct{}) {
	defer close(done)
	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if !ended {
		s.fe.Emit(ThreadStarted{ThreadID: ThreadID, Name: rootThreadName})
	}
	var err error
	s.mu.Lock()
	replay := kept != nil && kept == s.replay
	notes := s.replayNotes
	ended = s.ended
	s.mu.Unlock()
	if replay {
		// The launch's replay: its batch is kept as if captured, after the
		// replay's notes (AD-11). No capture runs, so no waiting note.
		if !ended {
			for _, n := range notes {
				s.fe.Emit(Output{Category: "console", Text: n})
			}
		}
		s.keepCaptured(*kept)
	}
	if kept == nil {
		var b CapturedBatch
		if b, err = s.capture(ctx, opts); err == nil {
			kept = &b
			// Only the launch's own capture is saved: a Restart re-steps
			// the batch already saved.
			s.autoSave(b)
		}
	}
	if err == nil {
		err = s.engine.Step(ctx, opts, kept.Batch, s)
	}

	s.mu.Lock()
	ended = s.ended || s.gen != gen
	s.mu.Unlock()
	if ended {
		return // Terminate (the frontend is gone) or Restart (a new run took over).
	}
	if HasCode(err, CodeStrictRejected) {
		s.mu.Lock()
		tr := s.translate
		s.mu.Unlock()
		var ce Error
		detail := err.Error()
		if errors.As(err, &ce) {
			detail = ce.Message
		}
		s.fe.Emit(Output{Category: "console", Text: "error_handling.strict: the output rejected the messages that failed a processing step, " +
			"so they were not delivered (in production the input would see them nacked): " + TranslateText(tr, detail) + "\n"})
		err = nil
	}
	switch {
	case err == nil:
		s.mu.Lock()
		none := s.delivered == 0
		s.mu.Unlock()
		if none {
			s.fe.Emit(Output{Category: "console", Text: nothingDelivered(opts.Output) + "\n"})
		}
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
	// Records the streams log while shutting down would follow the run's
	// last event: Log is muted from here until a Restart.
	captured := kept != nil
	if captured {
		s.fe.Emit(Output{Category: "console", Text: RunFinishedNote})
	}
	s.mu.Lock()
	s.runDone = true
	s.mu.Unlock()
	if exitRoot {
		s.fe.Emit(ThreadExited{ThreadID: ThreadID})
	}
	if !captured {
		s.fe.Emit(Terminated{})
	}
}

// RunFinishedNote is the console note at the end of a run that captured its
// batch: the session stays for a Restart (D5).
const RunFinishedNote = "Run finished: Restart re-steps the same batch with the edited pipeline; Stop ends the session.\n"

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
// once the session has ended or its run has ended, until a Restart. It must not be called while the caller
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
