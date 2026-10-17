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

// fakeEngine arrives once per child for one batch, then reports delivered.
type fakeEngine struct {
	checkErr  error
	warnings  []string
	startErr  error
	children  int
	batch     []Batch
	delivered chan []Batch
}

func (e *fakeEngine) Check(string, string) (CheckResult, error) {
	if e.checkErr != nil {
		return CheckResult{}, e.checkErr
	}
	return CheckResult{
		Lines:    lines{{0, 0}: {UserPath: "pipeline.processors[0]", Line: 12}},
		Warnings: e.warnings,
	}, nil
}

func (e *fakeEngine) Start(ctx context.Context, _, _ string, a Arriver) error {
	if e.startErr != nil {
		return e.startErr
	}
	for i := range e.children {
		if err := a.Arrive(ctx, 0, i, e.batch); err != nil {
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
}

func newRecorder() *recorder { return &recorder{events: make(chan event, 16)} }

func (r *recorder) Emit(ev Event) {
	switch e := ev.(type) {
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
			f, ok := s.Frame()
			if !ok {
				t.Fatal("no frame at the stop")
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
			if ev := rec.next(t); ev.kind != "terminated" {
				t.Fatalf("event = %+v, want terminated", ev)
			}
			if got := <-e.delivered; len(got) != 1 {
				t.Errorf("delivered %v", got)
			}
			if _, ok := s.Frame(); ok {
				t.Error("frame still set after resume")
			}
			_ = do(s, Terminate{})
		})
	}
}

func TestOnlyFirstArriveStops(t *testing.T) {
	e := newEngine()
	e.children = 3
	s, rec := launched(t, e)
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Fatalf("event = %+v", ev)
	}
	if err := do(s, Next{}); err != nil {
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
func (e *pathEngine) Start(context.Context, string, string, Arriver) error {
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
	s.configured, s.stopNext = true, true
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
	before, _ := s.Frame()
	for _, cmd := range []Command{
		StepIn{ThreadID: ThreadID}, StepOut{ThreadID: ThreadID}, Pause{ThreadID: ThreadID},
		Evaluate{Expression: "this"}, SetMessage{}, SaveCapture{}, Restart{},
	} {
		res, err := s.Do(context.Background(), cmd)
		var ce Error
		if !errors.As(err, &ce) || ce.Code != CodeNotSupported || res != nil {
			t.Errorf("%T = %v, %v; want NOT_SUPPORTED", cmd, res, err)
		}
	}
	after, ok := s.Frame()
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
