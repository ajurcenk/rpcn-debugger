package engine

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// StepperName is the stepper plugin the rewrite wraps child lists in.
const StepperName = "rpcn_debug_stepper"

// The stepper modes (AD-7).
const (
	modeAll = "all"
	modeTry = "try"
)

// The repeat label kinds (AD-10): what the stepper's parent repeats it by.
const (
	repeatNone    = ""
	repeatMsg     = "msg"     // for_each, try: one entry per message
	repeatAttempt = "attempt" // retry: one entry per attempt
	repeatLoop    = "loop"    // while: one entry per loop
	repeatGroup   = "group"   // group_by: one entry per group
)

func stepperSpec() *service.ConfigSpec {
	return service.NewConfigSpec().
		Summary("Runs its child processors one at a time and stops before each one (rpcn-debugger).").
		Field(service.NewIntField("id")).
		// parent, mode, strict, concurrent, repeat and max_attempts come from
		// the rewrite (AD-7, AD-10, AD-16).
		Field(service.NewIntField("parent").Default(-1)).
		Field(service.NewStringEnumField("mode", modeAll, modeTry).Default(modeAll)).
		Field(service.NewBoolField("strict").Default(false)).
		Field(service.NewBoolField("concurrent").Default(false)).
		Field(service.NewStringEnumField("repeat", repeatNone, repeatMsg, repeatAttempt, repeatLoop, repeatGroup).Default(repeatNone)).
		Field(service.NewIntField("max_attempts").Default(0)).
		Field(service.NewIntListField("lines").Default([]any{})).
		Field(service.NewProcessorListField("processors"))
}

// registerStepper registers rpcn_debug_stepper on env. Every stepper built
// from env reports its stop positions to a.
func registerStepper(env *service.Environment, a core.Arriver) error {
	return env.RegisterBatchProcessor(StepperName, stepperSpec(),
		func(conf *service.ParsedConfig, res *service.Resources) (service.BatchProcessor, error) {
			return newStepper(conf, res, a)
		})
}

func newStepper(conf *service.ParsedConfig, res *service.Resources, a core.Arriver) (*stepper, error) {
	s := &stepper{arriver: a, log: res.Logger()}
	var err error
	if s.id, err = conf.FieldInt("id"); err != nil {
		return nil, err
	}
	if s.parent, err = conf.FieldInt("parent"); err != nil {
		return nil, err
	}
	if s.mode, err = conf.FieldString("mode"); err != nil {
		return nil, err
	}
	if s.strict, err = conf.FieldBool("strict"); err != nil {
		return nil, err
	}
	if s.concurrent, err = conf.FieldBool("concurrent"); err != nil {
		return nil, err
	}
	if s.repeat, err = conf.FieldString("repeat"); err != nil {
		return nil, err
	}
	if s.maxAttempts, err = conf.FieldInt("max_attempts"); err != nil {
		return nil, err
	}
	// Children are built only through the stepper's own field (AD-4).
	if s.procs, err = conf.FieldProcessorList("processors"); err != nil {
		return nil, err
	}
	return s, nil
}

// stepper runs its child list the way the engine runs its parent's (AD-7):
//
//   - mode "all" mirrors ExecuteAll: each child runs on every batch the
//     previous one produced. With strict, a batch whose first message has
//     an error passes through the later children without running them, as
//     ExecuteAll does under error_handling.strict.
//   - mode "try" mirrors ExecuteTryAll: a batch whose first message has an
//     error always passes through the later children.
//
// In both, the outer loop is over children and the inner over batches, and
// it stops on a child's error or when ctx ends after a child, as the engine
// does. The stepper changes nothing its children don't (AD-8).
//
// Each ProcessBatch is one entry (AD-17): it enters its frame, which goes on
// ctx for every Arrive and every child, and exits it on return. It calls
// Arrive before each child that runs on at least one batch, and once after
// the last (childIndex = len(procs), AD-10), with Snapshot batches
// deep-copied from all of them (AD-19). The gate is never held while a child
// runs: it is held only inside Arrive (AD-9). Messages that pass a child
// without running it are reported through Skip.
//
// The entry's repeat label (AD-10) comes from repeat and the call site's
// counter. concurrent is stored but not acted on until epic-concurrency.
type stepper struct {
	id          int
	parent      int
	mode        string
	strict      bool
	concurrent  bool
	repeat      string
	maxAttempts int
	procs       []*service.OwnedProcessor
	arriver     core.Arriver
	log         *service.Logger
}

// attemptMemo is what a retry attempt leaves at its call site, so the next
// entry there can tell a further attempt of the same message from the first
// attempt of the next one.
type attemptMemo struct {
	attempt int
	failed  bool
	input   string // the attempt's input, as Snapshot JSON
}

// item is one batch in the loop. skipped is true once it has passed a child
// without running it, so each message is reported skipped only once.
type item struct {
	batch   service.MessageBatch
	skipped bool
}

func (s *stepper) ProcessBatch(ctx context.Context, batch service.MessageBatch) ([]service.MessageBatch, error) {
	if _, _, ok := core.TopFrame(ctx); !ok && s.parent >= 0 {
		// Its parent dropped the ctx the parent's stepper passed it (AD-17).
		s.log.With("code", string(core.CodeCtxLost)).Warnf(
			"%s: stepper %d (inside stepper %d) found no frame in its context, so it runs on the root thread", core.CodeCtxLost, s.id, s.parent)
	}

	in := snapshotBatches([]service.MessageBatch{batch})
	entry, memo, msg := s.entry(in)
	ctx, exit := s.arriver.Enter(ctx, entry)
	var out []service.MessageBatch
	var done bool
	defer func() {
		if !done {
			exit(nil)
			return
		}
		exit(memo(out))
	}()

	items := []item{{batch: batch}}
	snap := in
	for i, p := range s.procs {
		if len(items) == 0 {
			break
		}
		if i > 0 {
			snap = snapshotBatches(batchesOf(items))
		}
		var skipped []core.Skipped
		runs := false
		offset := 0
		for _, it := range items {
			if s.skips(it.batch) {
				if !it.skipped {
					for j := range it.batch {
						skipped = append(skipped, core.Skipped{Index: offset + j, Reason: s.skipReason()})
					}
				}
			} else {
				runs = true
			}
			offset += len(it.batch)
		}
		of := 0
		if msg.of > 0 && len(skipped) > 0 {
			// The entry is one message of its parent's batch: name it by
			// its place there, as its label does.
			skipped, of = []core.Skipped{{Index: msg.index, Reason: s.skipReason()}}, msg.of
		}
		s.arriver.Skip(ctx, s.id, i, skipped, of)
		if runs {
			if err := s.arriver.Arrive(ctx, s.id, i, snap); err != nil {
				// The session ended: deliver nothing.
				return nil, nil
			}
		}
		var next []item
		for _, it := range items {
			if s.skips(it.batch) {
				next = append(next, item{batch: it.batch, skipped: true})
				continue
			}
			// Each call counts its own entries (AD-10).
			res, err := p.ProcessBatch(core.MarkCall(ctx, len(it.batch)), it.batch)
			if err != nil {
				return nil, err
			}
			if err := ctx.Err(); err != nil {
				return nil, err
			}
			for _, b := range res {
				next = append(next, item{batch: b})
			}
		}
		items = next
	}
	out = batchesOf(items)
	// The end-of-entry stop only observes: it runs even when a child left
	// no batches, and changes nothing (AD-8).
	if err := s.arriver.Arrive(ctx, s.id, len(s.procs), snapshotBatches(out)); err != nil {
		return nil, nil
	}
	done = true
	return out, nil
}

// skips reports whether b passes the next child without running it (AD-7).
func (s *stepper) skips(b service.MessageBatch) bool {
	if s.mode != modeTry && !s.strict {
		return false
	}
	return len(b) > 0 && b[0].GetError() != nil
}

func (s *stepper) skipReason() string {
	if s.mode == modeTry {
		return core.SkipTry
	}
	return core.SkipStrict
}

// msgEntry is the place of a msg entry in its parent's batch: index is
// 0-based, and of is the batch's message count, or 0 when unknown.
type msgEntry struct{ index, of int }

// entry describes this entry to Enter: its repeat label (AD-10) and, for a
// retry attempt, the memo its exit leaves for the next entry at the call
// site. in is the entry's input. For a msg entry, the msgEntry is filled in
// by Enter.
func (s *stepper) entry(in []core.Batch) (core.Entry, func(out []service.MessageBatch) any, *msgEntry) {
	e := core.Entry{StepperID: s.id, Children: len(s.procs)}
	none := func([]service.MessageBatch) any { return nil }
	msg := &msgEntry{}
	switch s.repeat {
	case repeatMsg:
		e.Label = func(site core.Site) string {
			i := site.Entries + 1
			if site.Messages < i {
				return fmt.Sprintf("[msg %d]", i)
			}
			msg.index, msg.of = i-1, site.Messages
			return fmt.Sprintf("[msg %d/%d]", i, site.Messages)
		}
	case repeatLoop:
		e.Label = func(site core.Site) string { return fmt.Sprintf("[loop %d]", site.Entries+1) }
	case repeatGroup:
		// The groups of one group_by share the counter.
		e.Site = repeatGroup
		e.Label = func(site core.Site) string { return fmt.Sprintf("[group %d]", site.Entries+1) }
	case repeatAttempt:
		e.Attempt = true
		raw, _ := json.Marshal(in)
		input := string(raw)
		attempt := 1
		e.Label = func(site core.Site) string {
			// retry runs the same message again while an attempt fails and
			// attempts remain; otherwise this is the next message.
			if m, ok := site.Memo.(attemptMemo); ok && m.failed && m.input == input &&
				(s.maxAttempts <= 0 || m.attempt < s.maxAttempts) {
				attempt = m.attempt + 1
			}
			if s.maxAttempts > 0 {
				return fmt.Sprintf("[attempt %d/%d]", attempt, s.maxAttempts)
			}
			return fmt.Sprintf("[attempt %d]", attempt)
		}
		return e, func(out []service.MessageBatch) any {
			return attemptMemo{attempt: attempt, failed: anyFailed(out), input: input}
		}, msg
	}
	return e, none, msg
}

// anyFailed reports whether a message of batches has an error: what retry
// tests to try again.
func anyFailed(batches []service.MessageBatch) bool {
	for _, b := range batches {
		for _, m := range b {
			if m.GetError() != nil {
				return true
			}
		}
	}
	return false
}

func batchesOf(items []item) []service.MessageBatch {
	if len(items) == 0 {
		return nil
	}
	out := make([]service.MessageBatch, len(items))
	for i, it := range items {
		out[i] = it.batch
	}
	return out
}

func (s *stepper) Close(ctx context.Context) error {
	var errs []error
	for _, p := range s.procs {
		errs = append(errs, p.Close(ctx))
	}
	return errors.Join(errs...)
}
