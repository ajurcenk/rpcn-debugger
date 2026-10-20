package core

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

type lines map[[2]int]Position

func (l lines) Position(stepperID, childIndex int) (Position, bool) {
	p, ok := l[[2]int{stepperID, childIndex}]
	return p, ok
}

// fakeStepper is one stepper of a scripted tree: it enters its frame,
// arrives before each child and after the last, and runs a child's
// sub-stepper (a parent processor) with the entry's ctx.
type fakeStepper struct {
	id       int
	children []fakeChild
}

type fakeChild struct {
	sub   *fakeStepper  // nil: a leaf processor
	sleep time.Duration // how long the child runs
}

// batchAt is the batch a fake stepper passes at (stepperID, childIndex):
// one message whose content names the position.
func batchAt(stepperID, childIndex int) []Batch {
	return []Batch{{Messages: []Message{{Content: Content{Raw: fmt.Appendf(nil, "%d/%d", stepperID, childIndex)}}}}}
}

func (st *fakeStepper) run(ctx context.Context, a Arriver, batch []Batch) error {
	ctx, exit := a.Enter(ctx, st.id, len(st.children))
	defer exit()
	for i, c := range st.children {
		b := batch
		if b == nil {
			b = batchAt(st.id, i)
		}
		if err := a.Arrive(ctx, st.id, i, b); err != nil {
			return err
		}
		if c.sub != nil {
			if err := c.sub.run(ctx, a, batch); err != nil {
				return err
			}
		}
		time.Sleep(c.sleep)
	}
	b := batch
	if b == nil {
		b = batchAt(st.id, len(st.children))
	}
	return a.Arrive(ctx, st.id, len(st.children), b)
}

// fakeEngine runs its top-level steppers in order on one batch, then
// reports delivered. With no steppers it runs one stepper 0 of children
// leaves.
type fakeEngine struct {
	checkErr  error
	warnings  []string
	startErr  error
	children  int
	steppers  []*fakeStepper
	lines     lines // nil: pipeline.processors with children leaves
	batch     []Batch
	delivered chan []Batch
	// captured is the (kept, total) the engine reports; zero: the batch's
	// own size, untruncated.
	captured [2]int
	opts     chan StartOptions
}

func (e *fakeEngine) Check(string, string) (CheckResult, error) {
	if e.checkErr != nil {
		return CheckResult{}, e.checkErr
	}
	l := e.lines
	if l == nil {
		l = lines{
			{0, 0}:          {UserPath: "pipeline.processors[0]", Line: 12},
			{0, e.children}: {UserPath: "pipeline.processors", Line: 11},
		}
	}
	return CheckResult{Lines: l, Warnings: e.warnings}, nil
}

func (e *fakeEngine) Start(ctx context.Context, opts StartOptions, a Arriver) error {
	if e.opts != nil {
		e.opts <- opts
	}
	if e.startErr != nil {
		return e.startErr
	}
	kept, total := e.captured[0], e.captured[1]
	if kept == 0 && total == 0 && len(e.batch) > 0 {
		kept = len(e.batch[0].Messages)
		total = kept
	}
	a.Captured(kept, total)
	steppers := e.steppers
	if steppers == nil {
		steppers = []*fakeStepper{{id: 0, children: make([]fakeChild, e.children)}}
	}
	for _, st := range steppers {
		if err := st.run(ctx, a, e.batch); err != nil {
			return err
		}
	}
	e.delivered <- e.batch
	return nil
}

type event struct {
	kind, detail string
}

type recorder struct {
	events chan event
	// threads records ThreadStarted and ThreadExited, which are dropped
	// otherwise.
	threads bool
}

func newRecorder() *recorder { return &recorder{events: make(chan event, 64)} }

func (r *recorder) Emit(ev Event) {
	switch e := ev.(type) {
	case ThreadStarted:
		if r.threads {
			r.events <- event{"thread", fmt.Sprintf("started %d", e.ThreadID)}
		}
	case ThreadExited:
		if r.threads {
			r.events <- event{"thread", fmt.Sprintf("exited %d", e.ThreadID)}
		}
	case Paused:
		r.events <- event{"stopped", e.Reason}
	case Output:
		r.events <- event{"output", e.Text}
	case Terminated:
		r.events <- event{"terminated", ""}
	default:
		r.events <- event{fmt.Sprintf("%T", ev), ""}
	}
}

func do(s *Session, cmd Command) error {
	_, err := s.Do(context.Background(), cmd)
	return err
}

func (r *recorder) next(t *testing.T) event {
	t.Helper()
	select {
	case e := <-r.events:
		return e
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for an event")
		return event{}
	}
}

func (r *recorder) none(t *testing.T) {
	t.Helper()
	select {
	case e := <-r.events:
		t.Fatalf("unexpected event %+v", e)
	case <-time.After(50 * time.Millisecond):
	}
}

func newEngine() *fakeEngine {
	return &fakeEngine{
		children:  1,
		batch:     []Batch{{Messages: []Message{{Content: Content{Raw: []byte(`{"n":1}`)}}}}},
		delivered: make(chan []Batch, 1),
	}
}

func launched(t *testing.T, e *fakeEngine) (*Session, *recorder) {
	t.Helper()
	rec := newRecorder()
	s := NewSession(e, rec)
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	return s, rec
}

func TestStopThenResume(t *testing.T) {
	for name, resume := range map[string]Command{
		"next":     Next{ThreadID: ThreadID},
		"continue": Continue{ThreadID: ThreadID},
	} {
		t.Run(name, func(t *testing.T) {
			e := newEngine()
			s, rec := launched(t, e)
			if ev := rec.next(t); ev != (event{"stopped", "entry"}) {
				t.Fatalf("event = %+v, want stopped entry", ev)
			}
			st, ok := s.Stopped()
			if !ok || len(st.Frames) != 1 {
				t.Fatalf("stop = %+v, %v; want one frame", st, ok)
			}
			f := st.Frames[0]
			if f.Name != "pipeline.processors" || st.ThreadID != ThreadID || st.Reason != ReasonEntry {
				t.Errorf("stop = %+v", st)
			}
			p := f.Snapshot.Position
			if p.Line != 12 || p.UserPath != "pipeline.processors[0]" || p.File != "/p/pipeline.yaml" ||
				p.ThreadID != ThreadID || p.FrameID != f.ID || p.StepperID != 0 || p.ChildIndex != 0 {
				t.Errorf("position = %+v", p)
			}
			if f.Snapshot.SchemaVersion != SchemaVersion || len(f.Snapshot.Batches) != 1 ||
				len(f.Snapshot.Batches[0].Messages) != 1 || string(f.Snapshot.Batches[0].Messages[0].Content.Raw) != `{"n":1}` {
				t.Errorf("snapshot = %+v", f.Snapshot)
			}
			if err := do(s, resume); err != nil {
				t.Fatal(err)
			}
			if _, ok := resume.(Next); ok {
				// Next stops at the end stop, at the list's key line.
				if ev := rec.next(t); ev != (event{"stopped", ReasonStep}) {
					t.Fatalf("event = %+v, want stopped step", ev)
				}
				if p := stopAt(t, s); p.ChildIndex != 1 || p.Line != 11 {
					t.Errorf("end stop = %+v, want child 1 at line 11", p)
				}
				if err := do(s, resume); err != nil {
					t.Fatal(err)
				}
			}
			if ev := rec.next(t); ev.kind != "terminated" {
				t.Fatalf("event = %+v, want terminated", ev)
			}
			if got := <-e.delivered; len(got) != 1 {
				t.Errorf("delivered %v", got)
			}
			if _, ok := s.Stopped(); ok {
				t.Error("stop still set after resume")
			}
			_ = do(s, Terminate{})
		})
	}
}

// Continue stops nowhere: the run goes to the end.
func TestContinueRunsToTheEnd(t *testing.T) {
	e := newEngine()
	e.children = 3
	s, rec := launched(t, e)
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Fatalf("event = %+v", ev)
	}
	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev.kind != "terminated" {
		t.Fatalf("event = %+v, want terminated", ev)
	}
}

func TestLaunchErrors(t *testing.T) {
	s := NewSession(newEngine(), newRecorder())
	if err := do(s, Launch{}); !HasCode(err, CodeLaunchInvalid) || !strings.Contains(err.Error(), `"pipeline"`) {
		t.Errorf("Launch without pipeline = %v, want LAUNCH_INVALID naming pipeline", err)
	}

	e := newEngine()
	e.checkErr = errors.New("/p/pipeline.yaml: yaml: line 3: bad")
	s = NewSession(e, newRecorder())
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); !HasCode(err, CodePipelineInvalid) || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("Launch = %v, want PIPELINE_INVALID with the check error", err)
	}
	if err := do(s, ConfigurationDone{}); !HasCode(err, CodeLaunchInvalid) {
		t.Error("ConfigurationDone after a failed launch succeeded")
	}
}

func TestRelativePipeline(t *testing.T) {
	var got string
	e := &pathEngine{got: &got}
	s := NewSession(e, newRecorder())
	if err := do(s, Launch{Pipeline: "sub/p.yaml", Cwd: "/work"}); err != nil {
		t.Fatal(err)
	}
	if got != "/work/sub/p.yaml" {
		t.Errorf("pipeline = %q, want /work/sub/p.yaml", got)
	}
}

func TestRelativeCwdGivesAbsolutePipeline(t *testing.T) {
	var got string
	s := NewSession(&pathEngine{got: &got}, newRecorder())
	if err := do(s, Launch{Pipeline: "p.yaml", Cwd: "rel/dir"}); err != nil {
		t.Fatal(err)
	}
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(wd, "rel", "dir", "p.yaml"); got != want {
		t.Errorf("pipeline = %q, want %q", got, want)
	}
	s.mu.Lock()
	file := s.pipeline
	s.mu.Unlock()
	if !filepath.IsAbs(file) {
		t.Errorf("frame file %q is not absolute", file)
	}
}

type pathEngine struct{ got *string }

func (e *pathEngine) Check(p, _ string) (CheckResult, error) {
	*e.got = p
	return CheckResult{Lines: lines{}}, nil
}
func (e *pathEngine) Start(context.Context, StartOptions, Arriver) error {
	return nil
}

func TestNothingCaptured(t *testing.T) {
	e := newEngine()
	e.startErr = NewError(CodeNothingCaptured, "no batch was captured: the input closed")
	_, rec := launched(t, e)
	if ev := rec.next(t); ev.kind != "output" || !strings.Contains(ev.detail, "no batch was captured") {
		t.Fatalf("event = %+v, want an output naming the empty capture", ev)
	}
	if ev := rec.next(t); ev.kind != "terminated" {
		t.Fatalf("event = %+v, want terminated", ev)
	}
}

func TestDisconnectWhileStopped(t *testing.T) {
	e := newEngine()
	s, rec := launched(t, e)
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Fatalf("event = %+v", ev)
	}
	_ = do(s, Terminate{}) // returns only once the engine run has returned
	select {
	case b := <-e.delivered:
		t.Fatalf("delivered %v after disconnect", b)
	default:
	}
	rec.none(t)
	if err := do(s, Next{}); !HasCode(err, CodeSessionEnded) {
		t.Errorf("Next after terminate = %v, want SESSION_ENDED", err)
	}
	_ = do(s, Terminate{})
}

func TestResumeWhenNotStopped(t *testing.T) {
	s := NewSession(newEngine(), newRecorder())
	if err := do(s, Next{}); !HasCode(err, CodeNotStopped) {
		t.Errorf("Next before a stop = %v, want NOT_STOPPED", err)
	}
	if err := do(s, Continue{}); !HasCode(err, CodeNotStopped) {
		t.Errorf("Continue before a stop = %v, want NOT_STOPPED", err)
	}
	_ = do(s, Terminate{})
}

// The gate is held while paused: a second Arrive (another thread) waits until
// the first one is resumed.
func TestGateHeldWhilePaused(t *testing.T) {
	e := newEngine()
	rec := newRecorder()
	s := NewSession(e, rec)
	if err := do(s, Launch{Pipeline: "/p.yaml"}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	s.configured, s.entryPending = true, true
	s.mu.Unlock()

	first := make(chan error, 1)
	go func() { first <- s.Arrive(context.Background(), 0, 0, nil) }()
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Fatalf("event = %+v", ev)
	}
	second := make(chan error, 1)
	go func() { second <- s.Arrive(context.Background(), 0, 1, nil) }()
	select {
	case <-second:
		t.Fatal("second Arrive passed the gate while the first was paused")
	case <-time.After(50 * time.Millisecond):
	}
	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}

func TestNotSupported(t *testing.T) {
	e := newEngine()
	s, rec := launched(t, e)
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Fatalf("event = %+v", ev)
	}
	before, _ := s.Stopped()
	for _, cmd := range []Command{
		Evaluate{Expression: "this"}, SetMessage{}, SaveCapture{}, Restart{},
	} {
		res, err := s.Do(context.Background(), cmd)
		var ce Error
		if !errors.As(err, &ce) || ce.Code != CodeNotSupported || res != nil {
			t.Errorf("%T = %v, %v; want NOT_SUPPORTED", cmd, res, err)
		}
	}
	after, ok := s.Stopped()
	if !ok || after.ID != before.ID {
		t.Errorf("frame after unsupported commands = %+v, %v; want unchanged", after, ok)
	}
	rec.none(t)
	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev.kind != "terminated" {
		t.Fatalf("event = %+v, want terminated", ev)
	}
}

func TestSetBreakpointsUnverified(t *testing.T) {
	s := NewSession(newEngine(), newRecorder())
	res, err := s.Do(context.Background(), SetBreakpoints{File: "/p.yaml", Lines: []int{3, 7, 13}})
	if err != nil {
		t.Fatal(err)
	}
	bps := res.(SetBreakpointsResult).Breakpoints
	if len(bps) != 3 {
		t.Fatalf("breakpoints = %+v, want 3", bps)
	}
	for i, want := range []int{3, 7, 13} {
		if bps[i].Line != want || bps[i].Verified || bps[i].Message == "" {
			t.Errorf("breakpoint %d = %+v, want unverified at line %d with a message", i, bps[i], want)
		}
	}
	if res, err := s.Do(context.Background(), SetExceptionBreakpoints{}); res != nil || err != nil {
		t.Errorf("SetExceptionBreakpoints = %v, %v", res, err)
	}
	_ = do(s, Terminate{})
}

// Launch warnings reach the frontend as console Output events, before the
// run starts.
func TestLaunchWarnings(t *testing.T) {
	e := newEngine()
	e.warnings = []string{"pipeline.processors[0].parallel (line 9): parallel runs its processors concurrently"}
	rec := newRecorder()
	s := NewSession(e, rec)
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev.kind != "output" || ev.detail != "Warning: "+e.warnings[0]+"\n" {
		t.Fatalf("event = %+v, want the warning as output", ev)
	}
	rec.none(t)
	s.terminate()
}

func TestLaunchOptionsReachTheEngine(t *testing.T) {
	e := newEngine()
	e.opts = make(chan StartOptions, 1)
	s := NewSession(e, newRecorder())
	if err := do(s, Launch{Pipeline: "p.yaml", Cwd: "/w", MaxMessages: 7}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	select {
	case got := <-e.opts:
		if want := (StartOptions{Pipeline: "/w/p.yaml", Cwd: "/w", MaxMessages: 7}); got != want {
			t.Errorf("Start options = %+v, want %+v", got, want)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the engine did not start")
	}
}

func TestLaunchEnvFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("A=1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var got string
	s := NewSession(&pathEngine{got: &got}, newRecorder())
	if err := do(s, Launch{Pipeline: "p.yaml", Cwd: dir, EnvFile: ".env", Env: map[string]string{"B": "2"}}); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	envFile, env := s.envFile, s.env
	s.mu.Unlock()
	if want := filepath.Join(dir, ".env"); envFile != want {
		t.Errorf("envFile = %q, want %q", envFile, want)
	}
	if env["B"] != "2" {
		t.Errorf("env = %v, want B=2", env)
	}
	if got != filepath.Join(dir, "p.yaml") {
		t.Errorf("pipeline = %q", got)
	}
}

func TestLaunchInvalidOptions(t *testing.T) {
	dir := t.TempDir()
	for name, tc := range map[string]struct {
		launch Launch
		want   string
	}{
		"missing env file":  {Launch{Pipeline: "p.yaml", Cwd: dir, EnvFile: ".env"}, `"envFile"`},
		"env file is a dir": {Launch{Pipeline: "p.yaml", Cwd: dir, EnvFile: "."}, `"envFile"`},
		"negative max":      {Launch{Pipeline: "p.yaml", Cwd: dir, MaxMessages: -1}, `"maxMessages"`},
	} {
		t.Run(name, func(t *testing.T) {
			var got string
			s := NewSession(&pathEngine{got: &got}, newRecorder())
			err := do(s, tc.launch)
			if !HasCode(err, CodeLaunchInvalid) || !strings.Contains(err.Error(), tc.want) {
				t.Errorf("launch = %v, want LAUNCH_INVALID naming %s", err, tc.want)
			}
			if got != "" {
				t.Error("the engine checked the pipeline after an invalid launch")
			}
		})
	}
}

func TestTruncationOutput(t *testing.T) {
	e := newEngine()
	e.captured = [2]int{10, 12}
	s, rec := launched(t, e)
	if ev := rec.next(t); ev != (event{"output", "captured 10 of 12 messages (maxMessages)\n"}) {
		t.Errorf("first event = %+v, want the truncation output", ev)
	}
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Errorf("second event = %+v, want stopped", ev)
	}
	_ = do(s, Terminate{})
}

func TestNoTruncationNoOutput(t *testing.T) {
	e := newEngine()
	e.captured = [2]int{3, 3}
	s, rec := launched(t, e)
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Errorf("first event = %+v, want stopped and no output", ev)
	}
	_ = do(s, Terminate{})
}
