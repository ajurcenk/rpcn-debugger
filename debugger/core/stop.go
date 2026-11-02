package core

import (
	"context"
	"fmt"
	"slices"
)

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
	// RepeatLabel is the entry's repeat label (AD-10), computed by its
	// stepper, or "".
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

// liveFrame is the Session's record of one live stepper entry.
type liveFrame struct {
	ref      FrameRef
	children int // -1 when unknown
	name     string
	snap     Snapshot
	// sites are the per-call-site entry counters of the entries this frame
	// entered (AD-10, AD-17).
	sites map[siteKey]*siteCount
}

// siteKey names one call-site counter in a parent frame: the parent's stop
// (childIndex), the call of the child processor there (MarkCall, or nil)
// and the counter (a shared Entry.Site, or one stepper's).
type siteKey struct {
	childIndex int
	call       *call
	site       string
	stepperID  int // -1 for a shared Site
}

// siteCount is one call site's counter.
type siteCount struct {
	entries int
	memo    any
}

// Enter implements Arriver. It mints the entry's frame, registers it as
// live and pushes it on ctx's frame stack (AD-17). An entry with no frame in
// ctx runs on the root thread (ForkThread); otherwise it inherits its parent
// frame's thread, and its call site is the parent's latest stop. The
// entry's repeat label comes from e.Label, given the call site's counter in
// the parent frame (AD-10). Enter emits StepStarted, plus AttemptStarted for
// a retry attempt; the exit func emits StepFinished.
func (s *Session) Enter(ctx context.Context, e Entry) (context.Context, func(memo any)) {
	parent, _, nested := TopFrame(ctx)
	var tid int
	if nested {
		tid = parent.ThreadID
	} else {
		tid = s.ForkThread()
	}

	s.mu.Lock()
	s.frameSeq++
	ref := FrameRef{FrameID: s.frameSeq, StepperID: e.StepperID, ThreadID: tid, InRetry: e.Attempt}
	sites, messages := s.rootSites, 0
	if nested {
		ref.CallSite = CallSite{FrameID: parent.FrameID, StepperID: parent.StepperID}
		ref.InRetry = ref.InRetry || parent.InRetry
		if pf := s.frames[parent.FrameID]; pf != nil {
			ref.CallSite.ChildIndex = pf.snap.Position.ChildIndex
			if pf.sites == nil {
				pf.sites = map[siteKey]*siteCount{}
			}
			sites = pf.sites
			for _, b := range pf.snap.Batches {
				messages += len(b.Messages)
			}
		}
	}
	c := callOf(ctx)
	if c != nil {
		messages = c.messages
	}
	key := siteKey{childIndex: ref.CallSite.ChildIndex, call: c, site: e.Site, stepperID: -1}
	if e.Site == "" {
		key.stepperID = e.StepperID
	}
	site := sites[key]
	if site == nil {
		site = &siteCount{}
		sites[key] = site
	}
	if e.Label != nil {
		ref.RepeatLabel = e.Label(Site{Entries: site.entries, Messages: messages, Memo: site.memo})
	}
	site.entries++
	lf := &liveFrame{ref: ref, children: e.Children, name: s.frameName(e.StepperID, e.Children)}
	s.frames[ref.FrameID] = lf
	ended := s.ended
	s.mu.Unlock()

	if !ended {
		s.fe.Emit(StepStarted{ThreadID: tid, FrameID: ref.FrameID, StepperID: e.StepperID, RepeatLabel: ref.RepeatLabel})
		if e.Attempt {
			s.fe.Emit(AttemptStarted{ThreadID: tid, FrameID: ref.FrameID, StepperID: e.StepperID, RepeatLabel: ref.RepeatLabel, UserPath: lf.name})
		}
	}

	exit := func(memo any) {
		s.mu.Lock()
		delete(s.frames, ref.FrameID)
		site.memo = memo
		ended := s.ended
		s.mu.Unlock()
		if !ended {
			s.fe.Emit(StepFinished{ThreadID: tid, FrameID: ref.FrameID})
		}
		if !nested {
			s.ExitThread(tid)
		}
	}
	return PushFrame(ctx, ref), exit
}

// Skip implements Arriver: it reports skipped as one MessagesSkipped event
// at child childIndex of the entry on top of ctx.
func (s *Session) Skip(ctx context.Context, stepperID, childIndex int, skipped []Skipped, of int) {
	if len(skipped) == 0 {
		return
	}
	ev := MessagesSkipped{ThreadID: ThreadID, StepperID: stepperID, ChildIndex: childIndex, Skipped: slices.Clone(skipped), Of: of}
	if ref, _, ok := TopFrame(ctx); ok && ref.StepperID == stepperID {
		ev.ThreadID, ev.FrameID = ref.ThreadID, ref.FrameID
	}
	s.mu.Lock()
	ended := s.ended
	ev.File = s.pipeline
	if s.lines != nil {
		if p, ok := s.lines.Position(stepperID, childIndex); ok {
			ev.UserPath, ev.Line = p.UserPath, p.Line
		}
	}
	s.mu.Unlock()
	if ev.UserPath == "" {
		ev.UserPath = fmt.Sprintf("stepper %d child %d", stepperID, childIndex)
	}
	if !ended {
		s.fe.Emit(ev)
	}
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
	// The first stop of each stepper inside a retry warns once (AD-8).
	warnBudget := ref.InRetry && !s.budgetWarned[ref.StepperID]
	if warnBudget {
		s.budgetWarned[ref.StepperID] = true
	}
	name := lf.name
	s.mu.Unlock()

	if warnBudget {
		s.fe.Emit(Output{Category: "console", Text: "Warning: " + name + " runs inside a retry. Pausing here holds up the retry, which changes its backoff and max_elapsed_time behaviour, so the output isn't guaranteed to match a real run while you pause inside it.\n"})
	}
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
