package core

import (
	"context"
	"errors"
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
	startErr  error
	children  int
	batch     BatchCopy
	delivered chan BatchCopy
}

func (e *fakeEngine) Check(string, string) (LineMap, error) {
	if e.checkErr != nil {
		return nil, e.checkErr
	}
	return lines{{0, 0}: {Path: "pipeline.processors[0]", Line: 12}}, nil
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

func (r *recorder) Stopped(reason string, _ int) { r.events <- event{"stopped", reason} }
func (r *recorder) Output(_, text string)        { r.events <- event{"output", text} }
func (r *recorder) Terminated()                  { r.events <- event{"terminated", ""} }

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
		batch:     BatchCopy{{Content: []byte(`{"n":1}`)}},
		delivered: make(chan BatchCopy, 1),
	}
}

func launched(t *testing.T, e *fakeEngine) (*Session, *recorder) {
	t.Helper()
	rec := newRecorder()
	s := NewSession(e, rec)
	if err := s.Launch("/p/pipeline.yaml", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.ConfigurationDone(); err != nil {
		t.Fatal(err)
	}
	return s, rec
}

func TestStopThenResume(t *testing.T) {
	for name, resume := range map[string]func(*Session) error{
		"next":     (*Session).Next,
		"continue": (*Session).Continue,
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
			if f.Line != 12 || f.Path != "pipeline.processors[0]" || f.File != "/p/pipeline.yaml" ||
				len(f.Messages) != 1 || string(f.Messages[0]) != `{"n":1}` {
				t.Errorf("frame = %+v", f)
			}
			if err := resume(s); err != nil {
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
			s.Disconnect()
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
	if err := s.Next(); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev.kind != "terminated" {
		t.Fatalf("event = %+v, want terminated", ev)
	}
}

func TestLaunchErrors(t *testing.T) {
	s := NewSession(newEngine(), newRecorder())
	if err := s.Launch("", ""); err == nil || !strings.Contains(err.Error(), `"pipeline"`) {
		t.Errorf("Launch without pipeline = %v, want an error naming pipeline", err)
	}

	e := newEngine()
	e.checkErr = errors.New("/p/pipeline.yaml: yaml: line 3: bad")
	s = NewSession(e, newRecorder())
	if err := s.Launch("/p/pipeline.yaml", ""); err == nil || !strings.Contains(err.Error(), "line 3") {
		t.Errorf("Launch = %v, want the check error", err)
	}
	if err := s.ConfigurationDone(); err == nil {
		t.Error("ConfigurationDone after a failed launch succeeded")
	}
}

func TestRelativePipeline(t *testing.T) {
	var got string
	e := &pathEngine{got: &got}
	s := NewSession(e, newRecorder())
	if err := s.Launch("sub/p.yaml", "/work"); err != nil {
		t.Fatal(err)
	}
	if got != "/work/sub/p.yaml" {
		t.Errorf("pipeline = %q, want /work/sub/p.yaml", got)
	}
}

func TestRelativeCwdGivesAbsolutePipeline(t *testing.T) {
	var got string
	s := NewSession(&pathEngine{got: &got}, newRecorder())
	if err := s.Launch("p.yaml", "rel/dir"); err != nil {
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

func (e *pathEngine) Check(p, _ string) (LineMap, error) { *e.got = p; return lines{}, nil }
func (e *pathEngine) Start(context.Context, string, string, Arriver) error {
	return nil
}

func TestNothingCaptured(t *testing.T) {
	e := newEngine()
	e.startErr = ErrNothingCaptured
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
	s.Disconnect() // returns only once the engine run has returned
	select {
	case b := <-e.delivered:
		t.Fatalf("delivered %v after disconnect", b)
	default:
	}
	rec.none(t)
	if err := s.Next(); err == nil {
		t.Error("Next after disconnect succeeded")
	}
	s.Disconnect()
}

func TestResumeWhenNotStopped(t *testing.T) {
	s := NewSession(newEngine(), newRecorder())
	if err := s.Next(); err == nil {
		t.Error("Next before a stop succeeded")
	}
	if err := s.Continue(); err == nil {
		t.Error("Continue before a stop succeeded")
	}
	s.Disconnect()
}

// The gate is held while paused: a second Arrive (another thread) waits until
// the first one is resumed.
func TestGateHeldWhilePaused(t *testing.T) {
	e := newEngine()
	rec := newRecorder()
	s := NewSession(e, rec)
	if err := s.Launch("/p.yaml", ""); err != nil {
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
	if err := s.Continue(); err != nil {
		t.Fatal(err)
	}
	if err := <-first; err != nil {
		t.Fatal(err)
	}
	if err := <-second; err != nil {
		t.Fatal(err)
	}
}
