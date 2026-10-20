package core

import "context"

// FrameRef is one entry of the frame stack a stepper entry carries in its
// context.Context (AD-17). The Session mints it in Enter.
type FrameRef struct {
	// FrameID identifies the stepper entry for the life of the run. It is
	// not a DAP frame id: frontends mint those per stop.
	FrameID int
	// StepperID is the rewrite's id of the stepper (AD-16).
	StepperID int
	// ThreadID is the thread the entry runs on.
	ThreadID int
	// RepeatLabel is the entry's repeat label ([msg i/n], …), computed by
	// the stepper (AD-10), or "" when its parent gives none.
	RepeatLabel string
	// InRetry is true for a retry attempt and every entry below one: a
	// stop there pauses a time-budgeted parent (AD-8).
	InRetry bool
	// CallSite is the parent frame's stop the entry was entered from. It
	// is the zero CallSite for a top-level entry.
	CallSite CallSite
}

// Entry describes one stepper entry to Arriver.Enter.
type Entry struct {
	// StepperID is the rewrite's id of the stepper (AD-16).
	StepperID int
	// Children is the number of child processors in its list.
	Children int
	// Label computes the entry's repeat label (AD-10) from its call site's
	// counts. The Session calls it once, inside Enter, and only displays
	// the result; nil means no label.
	Label func(Site) string
	// Site names the counter the entry counts in at its call site: entries
	// with the same non-empty Site at the same parent stop share it (the
	// groups of one group_by). "" means one counter per stepper.
	Site string
	// Attempt marks a retry attempt: the Session emits AttemptStarted for
	// it, and a stop in it or below warns about the retry's time budget.
	Attempt bool
}

// callKey is the context key of the current child call (MarkCall).
type callKey struct{}

// call is one call of a child processor by a stepper entry.
type call struct{ messages int }

// MarkCall returns a copy of ctx that marks one call of a child processor
// by the entry on top of ctx, on a batch of messages messages. A stepper
// marks each call, so the entries a parent processor makes in one call
// count from 1 and the next call (e.g. on the next batch) starts afresh
// (AD-10).
func MarkCall(ctx context.Context, messages int) context.Context {
	return context.WithValue(ctx, callKey{}, &call{messages: messages})
}

// callOf returns the call ctx marks, or nil.
func callOf(ctx context.Context) *call {
	c, _ := ctx.Value(callKey{}).(*call)
	return c
}

// Site is what a stepper computes its repeat label from (AD-10, AD-17): the
// counter of its call site, which lives in the parent frame.
type Site struct {
	// Entries is how many entries the call site's counter counted before
	// this one in the same call of the parent processor (MarkCall).
	Entries int
	// Messages is the number of messages of the batch the parent processor
	// was called with, or, with no call marked, of the parent frame's
	// batches at the call site (0 with no parent frame).
	Messages int
	// Memo is what the previous entry counted at the call site passed to
	// its exit func, or nil.
	Memo any
}

// CallSite is a stop position of the parent frame.
type CallSite struct {
	// FrameID is the parent entry's frame id, or 0 at the top level.
	FrameID    int
	StepperID  int
	ChildIndex int
}

// frameKey is the context key of the frame stack. It is owned by
// debugger/core and unexported: steppers reach the stack only through
// PushFrame and FrameStack.
type frameKey struct{}

// frameNode is one node of the immutable frame stack: each push makes a new
// node that points at its parent, so a parent's ctx never sees its
// children's frames.
type frameNode struct {
	ref    FrameRef
	parent *frameNode
	depth  int
}

// PushFrame returns a copy of ctx whose frame stack has f on top. The
// stack in ctx is unchanged.
func PushFrame(ctx context.Context, f FrameRef) context.Context {
	parent := topNode(ctx)
	n := &frameNode{ref: f, parent: parent, depth: 1}
	if parent != nil {
		n.depth = parent.depth + 1
	}
	return context.WithValue(ctx, frameKey{}, n)
}

// FrameStack returns the frame stack in ctx, innermost first, or nil when
// ctx carries no frame.
func FrameStack(ctx context.Context) []FrameRef {
	n := topNode(ctx)
	if n == nil {
		return nil
	}
	out := make([]FrameRef, 0, n.depth)
	for ; n != nil; n = n.parent {
		out = append(out, n.ref)
	}
	return out
}

// TopFrame returns the innermost frame in ctx and the stack's depth, or
// false when ctx carries no frame.
func TopFrame(ctx context.Context) (f FrameRef, depth int, ok bool) {
	n := topNode(ctx)
	if n == nil {
		return FrameRef{}, 0, false
	}
	return n.ref, n.depth, true
}

func topNode(ctx context.Context) *frameNode {
	n, _ := ctx.Value(frameKey{}).(*frameNode)
	return n
}
