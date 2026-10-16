package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
)

// ThreadID is the one thread of this entry. Threads per stepper entry arrive
// with AD-17.
const ThreadID = 1

// Frame is the stop the Session is paused at: one stepper entry, positioned
// before child ChildIndex.
type Frame struct {
	StepperID  int
	ChildIndex int
	// Path is the child's user path, e.g. pipeline.processors[0].
	Path string
	// File is the pipeline file, as an absolute path.
	File string
	// Line is the child's 1-based line in File, or 0 when unknown.
	Line int
	// Messages holds the content of each message of the stopped batch copy.
	Messages [][]byte
}

// Session owns all debug state (AD-3). The frontend drives it through
// Launch, ConfigurationDone, Next, Continue and Disconnect; steppers reach it
// through Arrive; it reaches the engine only through the Engine port.
//
// Its stop predicate is the tracer one: after ConfigurationDone the first
// Arrive stops with reason "entry", and Next or Continue both run to the end.
type Session struct {
	engine Engine
	fe     Frontend
	gate   gate

	mu         sync.Mutex
	pipeline   string
	cwd        string
	lines      LineMap
	launched   bool
	configured bool
	stopNext   bool
	frame      *Frame
	resume     chan struct{}
	ended      bool
	endedCh    chan struct{}
	cancel     context.CancelFunc
	done       chan struct{}
}

// NewSession returns a Session that runs pipelines through engine and sends
// its events to fe.
func NewSession(engine Engine, fe Frontend) *Session {
	return &Session{engine: engine, fe: fe, endedCh: make(chan struct{})}
}

// Launch checks the pipeline file. A relative pipeline resolves against cwd,
// or against the process's working directory when cwd is empty. Nothing runs
// until ConfigurationDone.
func (s *Session) Launch(pipeline, cwd string) error {
	if pipeline == "" {
		return errors.New(`the launch configuration has no "pipeline": set it to the pipeline YAML file to debug`)
	}
	if !filepath.IsAbs(pipeline) {
		base := cwd
		if base == "" {
			wd, err := os.Getwd()
			if err != nil {
				return fmt.Errorf("resolve pipeline %q: %w", pipeline, err)
			}
			base = wd
		}
		pipeline = filepath.Join(base, pipeline)
	}
	// A relative cwd must not leave a relative path: frontends open and
	// highlight the frame's file by it.
	abs, err := filepath.Abs(pipeline)
	if err != nil {
		return fmt.Errorf("resolve pipeline %q: %w", pipeline, err)
	}
	pipeline = abs

	s.mu.Lock()
	if s.launched || s.ended {
		s.mu.Unlock()
		return errors.New("the session was already launched")
	}
	s.mu.Unlock()

	lines, err := s.engine.Check(pipeline, cwd)
	if err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.pipeline, s.cwd, s.lines, s.launched = pipeline, cwd, lines, true
	return nil
}

// ConfigurationDone starts the debuggee: the engine captures one batch and
// steps it. It returns at once; the run reports through the Frontend.
func (s *Session) ConfigurationDone() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if !s.launched {
		return errors.New("configurationDone before a successful launch")
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

func (s *Session) run(ctx context.Context, pipeline, cwd string, done chan struct{}) {
	defer close(done)
	err := s.engine.Start(ctx, pipeline, cwd, s)

	s.mu.Lock()
	ended := s.ended
	s.mu.Unlock()
	if ended {
		return // Disconnect: the frontend is gone.
	}
	switch {
	case errors.Is(err, ErrNothingCaptured):
		s.fe.Output("console", err.Error()+"\n")
	case err != nil:
		s.fe.Output("stderr", "The debug run failed: "+err.Error()+"\n")
	}
	s.fe.Terminated()
}

// Arrive implements Arriver. It takes the gate, decides whether to stop and,
// if it stops, emits Stopped and blocks under the gate until Next, Continue,
// Disconnect or ctx's end (AD-9).
func (s *Session) Arrive(ctx context.Context, stepperID, childIndex int, batch BatchCopy) error {
	s.gate.lock()
	defer s.gate.unlock()

	s.mu.Lock()
	if s.ended {
		s.mu.Unlock()
		return ErrSessionEnded
	}
	if !s.configured || !s.stopNext {
		s.mu.Unlock()
		return nil
	}
	s.stopNext = false

	f := &Frame{StepperID: stepperID, ChildIndex: childIndex, File: s.pipeline}
	if s.lines != nil {
		if pos, ok := s.lines.Position(stepperID, childIndex); ok {
			f.Path, f.Line = pos.Path, pos.Line
		}
	}
	for _, m := range batch {
		f.Messages = append(f.Messages, m.Content)
	}
	resume := make(chan struct{})
	s.frame, s.resume = f, resume
	s.mu.Unlock()

	s.fe.Stopped("entry", ThreadID)

	select {
	case <-resume:
		return nil
	case <-s.endedCh:
		return ErrSessionEnded
	case <-ctx.Done():
		s.mu.Lock()
		if s.resume == resume {
			s.frame, s.resume = nil, nil
		}
		s.mu.Unlock()
		return ctx.Err()
	}
}

// Frame returns the stop the session is paused at, if any.
func (s *Session) Frame() (Frame, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.frame == nil {
		return Frame{}, false
	}
	return *s.frame, true
}

// Next resumes a stopped session. In this entry it runs to the end.
func (s *Session) Next() error { return s.resumeRun() }

// Continue resumes a stopped session and runs to the end.
func (s *Session) Continue() error { return s.resumeRun() }

func (s *Session) resumeRun() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.resume == nil {
		return errors.New("the session is not stopped")
	}
	close(s.resume)
	s.frame, s.resume = nil, nil
	return nil
}

// Disconnect ends the session: it releases a stopped stepper, stops the
// engine and waits until its streams are stopped. Nothing is delivered after
// it, and no further event is sent. It is safe to call more than once.
func (s *Session) Disconnect() {
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
