package core

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"
)

// The scripted tree of the stepping matrix:
//
//	stepper 0 pipeline.processors (key line 10)
//	  [0] leaf                    line 11
//	  [1] branch                  line 12
//	      stepper 1 pipeline.processors[1].branch.processors (key line 13)
//	        [0] leaf              line 14
//	        [1] leaf              line 15
//	  [2] leaf                    line 16
//	stepper 2 output.processors   (key line 20)
//	  [0] leaf                    line 21
//
// Its Arrives, in order: 0/0 0/1 1/0 1/1 1/2 0/2 0/3 2/0 2/1.
func treeEngine() *fakeEngine {
	branch := &fakeStepper{id: 1, children: make([]fakeChild, 2)}
	e := newEngine()
	e.batch = nil // each Arrive passes batchAt
	e.steppers = []*fakeStepper{
		{id: 0, children: []fakeChild{{}, {sub: branch}, {}}},
		{id: 2, children: make([]fakeChild, 1)},
	}
	e.lines = lines{
		{0, 0}: {UserPath: "pipeline.processors[0]", Line: 11},
		{0, 1}: {UserPath: "pipeline.processors[1]", Line: 12},
		{0, 2}: {UserPath: "pipeline.processors[2]", Line: 16},
		{0, 3}: {UserPath: "pipeline.processors", Line: 10},
		{1, 0}: {UserPath: "pipeline.processors[1].branch.processors[0]", Line: 14},
		{1, 1}: {UserPath: "pipeline.processors[1].branch.processors[1]", Line: 15},
		{1, 2}: {UserPath: "pipeline.processors[1].branch.processors", Line: 13},
		{2, 0}: {UserPath: "output.processors[0]", Line: 21},
		{2, 1}: {UserPath: "output.processors", Line: 20},
	}
	return e
}

// stopAt returns the position of the innermost frame of the current stop.
func stopAt(t *testing.T, s *Session) Position {
	t.Helper()
	st, ok := s.Stopped()
	if !ok || len(st.Frames) == 0 {
		t.Fatalf("not stopped: %+v, %v", st, ok)
	}
	return st.Frames[0].Snapshot.Position
}

// pos is a (stepperID, childIndex) stop position.
type pos [2]int

func (p pos) String() string { return fmt.Sprintf("%d/%d", p[0], p[1]) }

// step issues cmd and checks the next stop is at want, with reason step and
// depth frames on the stack.
func step(t *testing.T, s *Session, rec *recorder, cmd Command, want pos, depth int) Stop {
	t.Helper()
	if err := do(s, cmd); err != nil {
		t.Fatalf("%T: %v", cmd, err)
	}
	if ev := rec.next(t); ev != (event{"stopped", ReasonStep}) {
		t.Fatalf("%T: event = %+v, want stopped step at %s", cmd, ev, want)
	}
	st, _ := s.Stopped()
	p := stopAt(t, s)
	if got := (pos{p.StepperID, p.ChildIndex}); got != want || len(st.Frames) != depth {
		t.Fatalf("%T: stopped at %s with %d frames, want %s with %d", cmd, got, len(st.Frames), want, depth)
	}
	return st
}

// toEnd issues cmd and checks the run terminates with no further stop.
func toEnd(t *testing.T, s *Session, rec *recorder, cmd Command) {
	t.Helper()
	if err := do(s, cmd); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev.kind != "terminated" {
		t.Fatalf("event = %+v, want terminated", ev)
	}
}

func treeLaunched(t *testing.T) (*Session, *recorder) {
	t.Helper()
	s, rec := launched(t, treeEngine())
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	if ev := rec.next(t); ev != (event{"stopped", ReasonEntry}) {
		t.Fatalf("event = %+v, want stopped entry", ev)
	}
	if p := stopAt(t, s); p.StepperID != 0 || p.ChildIndex != 0 || p.Line != 11 {
		t.Fatalf("entry stop = %+v, want 0/0 at line 11", p)
	}
	return s, rec
}

// Next walks the list, steps over the branch, stops at the end stop (the
// list's key line) and then at the next top-level stepper.
func TestNextThroughList(t *testing.T) {
	s, rec := treeLaunched(t)
	step(t, s, rec, Next{}, pos{0, 1}, 1)
	step(t, s, rec, Next{}, pos{0, 2}, 1) // over the branch: no stop inside
	step(t, s, rec, Next{}, pos{0, 3}, 1)
	if p := stopAt(t, s); p.Line != 10 || p.UserPath != "pipeline.processors" {
		t.Errorf("end stop = %+v, want the processors: key line 10", p)
	}
	step(t, s, rec, Next{}, pos{2, 0}, 1)
	step(t, s, rec, Next{}, pos{2, 1}, 1)
	toEnd(t, s, rec, Next{})
}

// StepIn enters the branch with 2 frames; on a leaf it is Next.
func TestStepIn(t *testing.T) {
	s, rec := treeLaunched(t)
	step(t, s, rec, StepIn{}, pos{0, 1}, 1) // a leaf: same as Next
	st := step(t, s, rec, StepIn{}, pos{1, 0}, 2)

	// The call stack: [branch frame at the child's line, pipeline frame at
	// the branch's line], each with its own batch.
	inner, outer := st.Frames[0], st.Frames[1]
	if inner.Name != "pipeline.processors[1].branch.processors" || inner.Snapshot.Position.Line != 14 {
		t.Errorf("inner frame = %+v", inner)
	}
	if outer.Name != "pipeline.processors" || outer.Snapshot.Position.Line != 12 {
		t.Errorf("outer frame = %+v", outer)
	}
	if got := string(inner.Snapshot.Batches[0].Messages[0].Content.Raw); got != "1/0" {
		t.Errorf("inner batch = %s, want the branch's 1/0", got)
	}
	if got := string(outer.Snapshot.Batches[0].Messages[0].Content.Raw); got != "0/1" {
		t.Errorf("outer batch = %s, want the batch before the branch (0/1)", got)
	}
	if inner.ID == outer.ID || inner.Snapshot.Position.FrameID != inner.ID || outer.Snapshot.Position.FrameID != outer.ID {
		t.Errorf("frame ids: inner %d (%d), outer %d (%d)", inner.ID, inner.Snapshot.Position.FrameID, outer.ID, outer.Snapshot.Position.FrameID)
	}

	step(t, s, rec, StepIn{}, pos{1, 1}, 2)
	step(t, s, rec, StepIn{}, pos{1, 2}, 2) // the branch's end stop
	step(t, s, rec, StepIn{}, pos{0, 2}, 1)
	toEnd(t, s, rec, Continue{})
}

// Next over a parent from inside it: Next at the branch's end stop goes to
// the parent's next stop.
func TestNextLeavesTheEntry(t *testing.T) {
	s, rec := treeLaunched(t)
	step(t, s, rec, StepIn{}, pos{0, 1}, 1)
	step(t, s, rec, StepIn{}, pos{1, 0}, 2)
	step(t, s, rec, Next{}, pos{1, 1}, 2)
	step(t, s, rec, Next{}, pos{1, 2}, 2)
	step(t, s, rec, Next{}, pos{0, 2}, 1)
	toEnd(t, s, rec, Continue{})
}

// StepOut inside the branch stops at its end stop; from there, at the
// parent's next stop. At the top level it goes to the end stop, then the
// next top-level stepper.
func TestStepOut(t *testing.T) {
	s, rec := treeLaunched(t)
	step(t, s, rec, StepIn{}, pos{0, 1}, 1)
	step(t, s, rec, StepIn{}, pos{1, 0}, 2)
	step(t, s, rec, StepOut{}, pos{1, 2}, 2)
	step(t, s, rec, StepOut{}, pos{0, 2}, 1)
	step(t, s, rec, StepOut{}, pos{0, 3}, 1)
	step(t, s, rec, StepOut{}, pos{2, 0}, 1)
	step(t, s, rec, StepOut{}, pos{2, 1}, 1)
	toEnd(t, s, rec, StepOut{})
}

// StepOut from the top-level list's first child skips the branch entirely.
func TestStepOutTopLevel(t *testing.T) {
	s, rec := treeLaunched(t)
	step(t, s, rec, StepOut{}, pos{0, 3}, 1)
	toEnd(t, s, rec, Continue{})
}

// Pause while running stops at the next Arrive with reason pause; Pause
// while stopped succeeds and changes nothing.
func TestPause(t *testing.T) {
	e := treeEngine()
	e.steppers[0].children[0].sleep = 300 * time.Millisecond // a slow processor
	s, rec := launched(t, e)
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Fatalf("event = %+v", ev)
	}
	before, _ := s.Stopped()
	if err := do(s, Pause{ThreadID: ThreadID}); err != nil {
		t.Fatalf("Pause while stopped = %v", err)
	}
	if after, ok := s.Stopped(); !ok || after.ID != before.ID {
		t.Errorf("stop after Pause = %+v, want unchanged", after)
	}
	rec.none(t)

	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	// The slow child is running: Pause now stops the next Arrive.
	if err := do(s, Pause{ThreadID: ThreadID}); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev != (event{"stopped", ReasonPause}) {
		t.Fatalf("event = %+v, want stopped pause", ev)
	}
	if p := stopAt(t, s); p.StepperID != 0 || p.ChildIndex != 1 {
		t.Errorf("paused at %+v, want 0/1", p)
	}
	// The pause stopped once: Continue runs to the end.
	toEnd(t, s, rec, Continue{})
}

// The root thread starts once, before the first stop, and exits at the end
// of the run, before terminated; Threads lists it while it runs.
func TestThreads(t *testing.T) {
	rec := newRecorder()
	rec.threads = true
	s := NewSession(treeEngine(), rec)
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	if got := s.Threads(); len(got) != 0 {
		t.Errorf("threads before the run = %+v", got)
	}
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	// The root thread is listed from ConfigurationDone on, before the
	// capture ends, so it can be paused.
	if got := s.Threads(); len(got) != 1 || got[0].ID != ThreadID {
		t.Errorf("threads right after ConfigurationDone = %+v, want the root thread", got)
	}
	if ev := rec.next(t); ev != (event{"thread", "started 1"}) {
		t.Fatalf("event = %+v, want thread started 1", ev)
	}
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Fatalf("event = %+v, want stopped", ev)
	}
	if got := s.Threads(); len(got) != 1 || got[0].ID != ThreadID || got[0].Name == "" {
		t.Errorf("threads = %+v, want the root thread", got)
	}
	// Three top-level entries share the root thread: no more thread events.
	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev != (event{"thread", "exited 1"}) {
		t.Fatalf("event = %+v, want thread exited 1", ev)
	}
	if ev := rec.next(t); ev.kind != "terminated" {
		t.Fatalf("event = %+v, want terminated", ev)
	}
	if got := s.Threads(); len(got) != 0 {
		t.Errorf("threads after the run = %+v", got)
	}
}

// Disconnect two frames deep releases the gate and ends the run with
// nothing delivered.
func TestDisconnectWhileNested(t *testing.T) {
	e := treeEngine()
	s, rec := launched(t, e)
	if ev := rec.next(t); ev.kind != "stopped" {
		t.Fatalf("event = %+v", ev)
	}
	step(t, s, rec, StepIn{}, pos{0, 1}, 1)
	step(t, s, rec, StepIn{}, pos{1, 0}, 2)
	done := make(chan struct{})
	go func() { _ = do(s, Terminate{}); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Terminate did not return")
	}
	select {
	case b := <-e.delivered:
		t.Fatalf("delivered %v after disconnect", b)
	default:
	}
	rec.none(t)
	// The gate is free: an Arrive returns at once.
	if err := s.Arrive(context.Background(), 0, 0, nil); !HasCode(err, CodeSessionEnded) {
		t.Errorf("Arrive after terminate = %v, want SESSION_ENDED", err)
	}
	s.mu.Lock()
	live := len(s.frames)
	s.mu.Unlock()
	if live != 0 {
		t.Errorf("%d frames still live after the run", live)
	}
}

// The frame stack in ctx is immutable: a push never changes the parent's
// stack, and each frame's call site is its parent's stop.
func TestFrameStack(t *testing.T) {
	root := context.Background()
	if FrameStack(root) != nil {
		t.Fatal("empty ctx has frames")
	}
	a := PushFrame(root, FrameRef{FrameID: 1, StepperID: 0, ThreadID: 1})
	b := PushFrame(a, FrameRef{FrameID: 2, StepperID: 3, ThreadID: 1, CallSite: CallSite{FrameID: 1, ChildIndex: 2}})
	c := PushFrame(a, FrameRef{FrameID: 3, StepperID: 4, ThreadID: 1})
	if got := FrameStack(a); len(got) != 1 || got[0].FrameID != 1 {
		t.Errorf("stack a = %+v", got)
	}
	if got := FrameStack(b); len(got) != 2 || got[0].FrameID != 2 || got[1].FrameID != 1 {
		t.Errorf("stack b = %+v, want 2 then 1", got)
	}
	if f, depth, ok := TopFrame(c); !ok || depth != 2 || f.FrameID != 3 {
		t.Errorf("top of c = %+v, %d, %v", f, depth, ok)
	}

	// Enter records the call site from the parent's latest stop.
	e := treeEngine()
	s := NewSession(e, newRecorder())
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	ctx, exit := s.Enter(root, Entry{StepperID: 0, Children: 3})
	defer exit(nil)
	if err := s.Arrive(ctx, 0, 1, nil); err != nil {
		t.Fatal(err)
	}
	inner, exitInner := s.Enter(ctx, Entry{StepperID: 1, Children: 2})
	defer exitInner(nil)
	f, depth, _ := TopFrame(inner)
	parent, _, _ := TopFrame(ctx)
	if depth != 2 || f.ThreadID != ThreadID || f.CallSite != (CallSite{FrameID: parent.FrameID, StepperID: 0, ChildIndex: 1}) {
		t.Errorf("inner frame = %+v at depth %d", f, depth)
	}
	_ = do(s, Terminate{})
}

// funcEngine runs start as its pipeline, with lines as its LineMap.
type funcEngine struct {
	lines lines
	start func(ctx context.Context, a Arriver) error
}

func (e funcEngine) Check(StartOptions) (CheckResult, error) { return CheckResult{Lines: e.lines}, nil }

func (e funcEngine) Start(ctx context.Context, _ StartOptions, a Arriver) error {
	return e.start(ctx, a)
}

func messages(n int) []Batch {
	b := Batch{}
	for i := range n {
		b.Messages = append(b.Messages, Message{Index: i, Content: Content{Raw: []byte("{}")}})
	}
	return []Batch{b}
}

// Repeat labels come from per-call-site counters in the parent frame
// (AD-10, AD-17): the Nth entry of a stepper from the same parent stop,
// with the parent's message count there and the previous entry's memo.
func TestEntryCounters(t *testing.T) {
	rec := newRecorder()
	rec.steps = true
	s := NewSession(nil, rec)
	label := func(site Site) string { return fmt.Sprintf("%d/%d %v", site.Entries+1, site.Messages, site.Memo) }

	enter := func(ctx context.Context, e Entry, memo any) string {
		t.Helper()
		inner, exit := s.Enter(ctx, e)
		f, _, _ := TopFrame(inner)
		exit(memo)
		return f.RepeatLabel
	}
	for entry := range 2 {
		ctx, exit := s.Enter(context.Background(), Entry{StepperID: 0, Children: 2})
		if err := s.Arrive(ctx, 0, 1, messages(3)); err != nil {
			t.Fatal(err)
		}
		var got []string
		for i := range 3 {
			got = append(got, enter(ctx, Entry{StepperID: 1, Children: 1, Label: label}, i))
		}
		// Another stepper at the same stop counts on its own.
		got = append(got, enter(ctx, Entry{StepperID: 2, Children: 1, Label: label}, nil))
		// Steppers sharing a Site share the counter.
		got = append(got, enter(ctx, Entry{StepperID: 3, Children: 1, Label: label, Site: "group"}, nil))
		got = append(got, enter(ctx, Entry{StepperID: 4, Children: 1, Label: label, Site: "group"}, nil))
		// Another stop of the parent is another call site.
		if err := s.Arrive(ctx, 0, 0, messages(1)); err != nil {
			t.Fatal(err)
		}
		got = append(got, enter(ctx, Entry{StepperID: 1, Children: 1, Label: label}, nil))
		got = append(got, enter(ctx, Entry{StepperID: 1, Children: 1}, nil))
		exit(nil)
		// Each parent entry starts its counters afresh.
		want := []string{"1/3 <nil>", "2/3 0", "3/3 1", "1/3 <nil>", "1/3 <nil>", "2/3 <nil>", "1/1 <nil>", ""}
		if fmt.Sprint(got) != fmt.Sprint(want) {
			t.Errorf("parent entry %d: labels = %q, want %q", entry, got, want)
		}
	}

	// StepStarted carries the label; StepFinished follows each exit.
	if ev := rec.next(t); ev != (event{"step", "started 0 "}) {
		t.Errorf("first event = %+v, want step started 0", ev)
	}
	if ev := rec.next(t); ev != (event{"step", "started 1 1/3 <nil>"}) {
		t.Errorf("second event = %+v, want step started 1 with its label", ev)
	}
	if ev := rec.next(t); ev != (event{"step", "finished"}) {
		t.Errorf("third event = %+v, want step finished", ev)
	}
}

// A retry attempt emits AttemptStarted, and the first stop of each stepper
// inside a retry warns once about its time budget (AD-8).
func TestAttemptsAndTimeBudget(t *testing.T) {
	e := funcEngine{
		lines: lines{
			{0, 0}: {UserPath: "pipeline.processors[0]", Line: 11},
			{0, 1}: {UserPath: "pipeline.processors", Line: 10},
			{1, 0}: {UserPath: "pipeline.processors[0].retry.processors[0]", Line: 13},
			{1, 1}: {UserPath: "pipeline.processors[0].retry.processors", Line: 12},
			{2, 0}: {UserPath: "pipeline.processors[0].retry.processors[0].try[0]", Line: 14},
			{2, 1}: {UserPath: "pipeline.processors[0].retry.processors[0].try", Line: 14},
		},
		start: func(ctx context.Context, a Arriver) error {
			ctx, exit := a.Enter(ctx, Entry{StepperID: 0, Children: 1})
			defer exit(nil)
			if err := a.Arrive(ctx, 0, 0, messages(1)); err != nil {
				return err
			}
			for i := range 2 {
				in, exitIn := a.Enter(ctx, Entry{StepperID: 1, Children: 1, Attempt: true, Label: func(Site) string { return fmt.Sprintf("[attempt %d/2]", i+1) }})
				if err := a.Arrive(in, 1, 0, messages(1)); err != nil {
					return err
				}
				// A try inside the attempt is inside the retry too.
				try, exitTry := a.Enter(in, Entry{StepperID: 2, Children: 1})
				a.Skip(try, 2, 0, []Skipped{{Index: 0, Reason: SkipTry}}, 0)
				if err := a.Arrive(try, 2, 1, messages(1)); err != nil {
					return err
				}
				exitTry(nil)
				exitIn(nil)
			}
			return a.Arrive(ctx, 0, 1, messages(1))
		},
	}
	rec := &recorder{events: make(chan event, 64)}
	s := NewSession(e, &skipRecorder{rec})
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = do(s, Terminate{}) })

	var got []string
	for {
		ev := rec.next(t)
		got = append(got, ev.kind+" "+ev.detail)
		if ev.kind == "terminated" {
			break
		}
		if ev.kind == "stopped" {
			if err := do(s, StepIn{}); err != nil {
				t.Fatal(err)
			}
		}
	}
	const warn = "output Warning: pipeline.processors[0].retry.processors runs inside a retry."
	const warnTry = "output Warning: pipeline.processors[0].retry.processors[0].try runs inside a retry."
	const skipped = "skipped 2/0 pipeline.processors[0].retry.processors[0].try[0] line 14 [{0 try}]"
	want := []string{
		"stopped entry",
		"attempt [attempt 1/2] pipeline.processors[0].retry.processors",
		warn, "stopped step",
		skipped,
		warnTry, "stopped step",
		"attempt [attempt 2/2] pipeline.processors[0].retry.processors",
		"stopped step",
		skipped,
		"stopped step",
		"stopped step",
		"terminated ",
	}
	for i := range got {
		// The rest of the warning is prose.
		if j := strings.Index(got[i], "inside a retry."); strings.HasPrefix(got[i], "output Warning") && j > 0 {
			got[i] = got[i][:j+len("inside a retry.")]
		}
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("events:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
	if st, ok := s.Stopped(); ok {
		t.Errorf("still stopped at %+v", st)
	}
}

// skipRecorder records AttemptStarted and MessagesSkipped in detail.
type skipRecorder struct{ *recorder }

func (r *skipRecorder) Emit(ev Event) {
	switch e := ev.(type) {
	case AttemptStarted:
		r.events <- event{"attempt", e.RepeatLabel + " " + e.UserPath}
	case MessagesSkipped:
		r.events <- event{"skipped", fmt.Sprintf("%d/%d %s line %d %v", e.StepperID, e.ChildIndex, e.UserPath, e.Line, e.Skipped)}
	default:
		r.recorder.Emit(ev)
	}
}
