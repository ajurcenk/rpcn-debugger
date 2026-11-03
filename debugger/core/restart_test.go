package core

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"
)

// restartEngine is a pipeline whose Check "reads" mapping, the pipeline
// file's mapping, into Prepared. Its Step enters stepper 0 (one child),
// delivers the captured content with the mapping appended, then arrives
// before the child and at the end stop.
type restartEngine struct {
	mu       sync.Mutex
	mapping  string
	checkErr error
	checks   []StartOptions
	steps    []StartOptions
	captures int
	// hold, when set, makes Capture wait for it: a quiet input.
	hold chan struct{}
	// torn, when set, is closed once a Step's stop was released with an
	// error, and the Step then waits for release before it returns.
	torn, release chan struct{}
}

var restartLines = lines{
	{0, 0}: {UserPath: "pipeline.processors[0]", Line: 12},
	{0, 1}: {UserPath: "pipeline.processors", Line: 11},
}

func (e *restartEngine) Check(opts StartOptions) (CheckResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()
	e.checks = append(e.checks, opts)
	if e.checkErr != nil {
		return CheckResult{}, e.checkErr
	}
	return CheckResult{Lines: restartLines, Prepared: e.mapping, Warnings: []string{"w " + e.mapping}}, nil
}

func (e *restartEngine) Capture(ctx context.Context, _ StartOptions) (CapturedBatch, error) {
	if e.hold != nil {
		select {
		case <-e.hold:
		case <-ctx.Done():
			return CapturedBatch{}, ctx.Err()
		}
	}
	e.mu.Lock()
	e.captures++
	e.mu.Unlock()
	return CapturedBatch{Batch: []Batch{{Messages: []Message{{Content: Content{Raw: []byte("in")}}}}}, Kept: 1, Total: 1}, nil
}

func (e *restartEngine) Step(ctx context.Context, opts StartOptions, batch any, a Arriver) error {
	e.mu.Lock()
	e.steps = append(e.steps, opts)
	e.mu.Unlock()
	b := batch.([]Batch)
	mapping, _ := opts.Prepared.(string)
	ctx, exit := a.Enter(ctx, Entry{StepperID: 0, Children: 1})
	defer exit(nil)
	content := string(b[0].Messages[0].Content.Raw) + "/" + mapping
	a.Deliver(ctx, Batch{Messages: []Message{{Content: Content{Raw: []byte(content)}}}}, OutcomeRecorded, nil)
	if err := a.Arrive(ctx, 0, 0, b); err != nil {
		if e.torn != nil {
			close(e.torn)
			<-e.release
		}
		return err
	}
	return a.Arrive(ctx, 0, 1, b)
}

func (e *restartEngine) set(mapping string, checkErr error) {
	e.mu.Lock()
	e.mapping, e.checkErr = mapping, checkErr
	e.mu.Unlock()
}

func (e *restartEngine) counts() (checks []StartOptions, steps []StartOptions, captures int) {
	e.mu.Lock()
	defer e.mu.Unlock()
	return append([]StartOptions(nil), e.checks...), append([]StartOptions(nil), e.steps...), e.captures
}

// launchRestart launches e with launch and runs it, recording threads and
// deliveries.
func launchRestart(t *testing.T, e *restartEngine, launch Launch) (*Session, *recorder) {
	t.Helper()
	rec := newRecorder()
	rec.threads, rec.delivery = true, true
	s := NewSession(e, rec)
	if launch.Pipeline == "" {
		launch.Pipeline = "/p/pipeline.yaml"
	}
	if err := do(s, launch); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	return s, rec
}

// expect reads the next events and checks each against want: an event
// kind, with "kind:text" also requiring the detail to contain text.
func expect(t *testing.T, rec *recorder, want ...string) {
	t.Helper()
	for _, w := range want {
		kind, text, _ := strings.Cut(w, ":")
		ev := rec.next(t)
		if ev.kind != kind || !strings.Contains(ev.detail, text) {
			t.Fatalf("event = %+v, want %s", ev, w)
		}
	}
}

// Restart at a stop re-steps the captured batch with the edited mapping:
// the root thread exits and starts again, the first Restart's note goes
// out, the batch is delivered as batch 1 again, and the run stops at the
// entry stop. The input is not captured again, and Check lints for a
// Restart. Breakpoints stay; a second Restart sends no note.
func TestRestartReStepsTheBatch(t *testing.T) {
	e := &restartEngine{mapping: "v1"}
	s, rec := launchRestart(t, e, Launch{})
	expect(t, rec, "output:w v1", "thread:started 1", "delivered:1 recorded", "stopped:entry")
	_ = do(s, SetBreakpoints{File: "/p/pipeline.yaml", Lines: []int{12}})
	first, _ := s.Stopped()

	e.set("v2", nil)
	if err := do(s, Restart{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "thread:exited 1", "output:"+restartNote, "output:w v2", "thread:started 1", "delivered:1 recorded", "stopped:entry")
	st, ok := s.Stopped()
	if !ok || st.ID <= first.ID || st.Frames[0].ID <= first.Frames[0].ID {
		t.Errorf("stop = %+v, want new stop and frame ids after %+v", st, first)
	}
	checks, steps, captures := e.counts()
	if captures != 1 {
		t.Errorf("captures = %d, want 1", captures)
	}
	if len(checks) != 2 || !checks[1].Restart || checks[1].Pipeline != "/p/pipeline.yaml" || checks[1].Lookup == nil {
		t.Errorf("checks = %+v, want the launch's then a Restart's", checks)
	}
	if len(steps) != 2 || steps[1].Prepared != "v2" || steps[1].Restart {
		t.Errorf("steps = %+v, want the second on the new Prepared", steps)
	}
	if got := string(st.Frames[0].Snapshot.Batches[0].Messages[0].Content.Raw); got != "in" {
		t.Errorf("stop content = %q, want the batch as captured", got)
	}
	s.mu.Lock()
	bps := s.breakpoints["/p/pipeline.yaml"]
	s.mu.Unlock()
	if len(bps) != 1 || bps[0] != 12 {
		t.Errorf("breakpoints = %v, want kept", bps)
	}

	if err := do(s, Restart{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "thread:exited 1", "output:w v2", "thread:started 1", "delivered:1 recorded", "stopped:entry")

	// The run ends without Terminated: a Restart after it re-steps the
	// batch as from a stop (D5).
	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "output:"+RunFinishedNote, "thread:exited 1")
	rec.none(t)
	if err := do(s, Restart{}); err != nil {
		t.Fatalf("Restart after the run = %v", err)
	}
	expect(t, rec, "output:w v2", "thread:started 1", "delivered:1 recorded", "stopped:entry")
	if checks, steps, captures := e.counts(); len(checks) != 4 || len(steps) != 4 || captures != 1 {
		t.Errorf("checks %d, steps %d, captures %d; want 4, 4, 1", len(checks), len(steps), captures)
	}
}

// A Restart whose Check fails returns the error and changes nothing: the
// stop stays, and a later Restart or Next works.
func TestRestartBrokenPipeline(t *testing.T) {
	e := &restartEngine{mapping: "v1"}
	s, rec := launchRestart(t, e, Launch{})
	expect(t, rec, "output", "thread", "delivered", "stopped:entry")
	before, _ := s.Stopped()

	bad := Error{Code: CodePipelineInvalid, Message: "yaml: did not find expected key", UserPath: "pipeline.processors[0]", Line: 13}
	e.set("v1", bad)
	err := do(s, Restart{})
	var ce Error
	if !errors.As(err, &ce) || ce.Code != CodePipelineInvalid || ce.Line != 13 || ce.UserPath != "pipeline.processors[0]" {
		t.Fatalf("Restart = %v, want PIPELINE_INVALID at line 13", err)
	}
	if after, ok := s.Stopped(); !ok || after.ID != before.ID {
		t.Errorf("stop after a failed Restart = %+v, %v; want unchanged", after, ok)
	}
	rec.none(t)

	e.set("v2", nil)
	if err := do(s, Restart{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "thread:exited 1", "output:"+restartNote, "output:w v2", "thread:started 1", "delivered:in/v2", "stopped:entry")
	if err := do(s, Next{ThreadID: ThreadID}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "stopped:step")
}

// Under output: real the restarted run is dry-run, with a note.
func TestRestartOutputReal(t *testing.T) {
	e := &restartEngine{mapping: "v1"}
	s, rec := launchRestart(t, e, Launch{Output: OutputReal})
	expect(t, rec, "output", "thread", "delivered", "stopped:entry")
	if err := do(s, Restart{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "thread:exited 1", "output:"+restartNote, "output:"+restartDryRunNote, "output:w v1", "thread:started 1", "delivered:1 recorded", "stopped:entry")
	checks, steps, _ := e.counts()
	if checks[0].Output != OutputReal || checks[1].Output != OutputDryRun || steps[0].Output != OutputReal || steps[1].Output != OutputDryRun {
		t.Errorf("outputs: checks %q, %q, steps %q, %q; want real then dry-run", checks[0].Output, checks[1].Output, steps[0].Output, steps[1].Output)
	}
}

// Restart before the batch is captured is not supported, and the capture
// goes on.
func TestRestartDuringCapture(t *testing.T) {
	e := &restartEngine{mapping: "v1", hold: make(chan struct{})}
	s, rec := launchRestart(t, e, Launch{})
	expect(t, rec, "output", "thread:started 1")
	if err := do(s, Restart{}); !HasCode(err, CodeNotSupported) {
		t.Errorf("Restart during capture = %v, want NOT_SUPPORTED", err)
	}
	close(e.hold)
	expect(t, rec, "delivered", "stopped:entry")
	if checks, _, _ := e.counts(); len(checks) != 1 {
		t.Errorf("checks = %d, want only the launch's", len(checks))
	}
}

// Terminate while a Restart waits for the old run ends the session: no new
// run starts.
func TestTerminateDuringRestart(t *testing.T) {
	e := &restartEngine{mapping: "v1", torn: make(chan struct{}), release: make(chan struct{})}
	s, rec := launchRestart(t, e, Launch{})
	expect(t, rec, "output", "thread", "delivered", "stopped:entry")

	restarted := make(chan error, 1)
	go func() { restarted <- do(s, Restart{}) }()
	<-e.torn
	terminated := make(chan struct{})
	go func() {
		_ = do(s, Terminate{})
		close(terminated)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for {
		s.mu.Lock()
		ended := s.ended
		s.mu.Unlock()
		if ended {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("Terminate did not end the session")
		}
		time.Sleep(time.Millisecond)
	}
	close(e.release)
	if err := <-restarted; !HasCode(err, CodeSessionEnded) {
		t.Errorf("Restart = %v, want SESSION_ENDED", err)
	}
	<-terminated
	if _, steps, _ := e.counts(); len(steps) != 1 {
		t.Errorf("steps = %d, want no new run", len(steps))
	}
	rec.none(t)
}

// hookFrontend passes events to rec, and calls hook (once) on the first
// ThreadExited before passing it on.
type hookFrontend struct {
	rec  *recorder
	once sync.Once
	hook func()
}

func (f *hookFrontend) Emit(ev Event) {
	if _, ok := ev.(ThreadExited); ok && f.hook != nil {
		f.once.Do(f.hook)
	}
	f.rec.Emit(ev)
}

// A Restart that comes as the run ends (the run already passed its
// generation check, so it goes on to its end) waits for the run and then
// replaces it like a live one (D5).
func TestRestartAsTheRunEnds(t *testing.T) {
	e := &restartEngine{mapping: "v1"}
	rec := newRecorder()
	fe := &hookFrontend{rec: rec}
	s := NewSession(e, fe)
	restarted := make(chan error, 1)
	// The root thread's exit comes after the run's generation check: start
	// the Restart there, and let the run go on once it has bumped gen.
	fe.hook = func() {
		go func() { restarted <- do(s, Restart{}) }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			s.mu.Lock()
			bumped := s.gen > 0
			s.mu.Unlock()
			if bumped || time.Now().After(deadline) {
				return
			}
			time.Sleep(time.Millisecond)
		}
	}
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	expect(t, rec, "output", "stopped:entry")
	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "output:"+RunFinishedNote, "rootExited", "output:"+restartNote, "output:w v1", "stopped:entry")
	if err := <-restarted; err != nil {
		t.Errorf("Restart as the run ends = %v, want it to succeed", err)
	}
	if _, steps, captures := e.counts(); len(steps) != 2 || captures != 1 {
		t.Errorf("steps = %d, captures = %d; want a new run on the kept batch", len(steps), captures)
	}
	rec.none(t)
}
