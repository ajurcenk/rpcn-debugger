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
	// RepeatLabel is the entry's repeat label ([msg i/n], …). It stays
	// empty until the stepper computes labels (entry 2.7).
	RepeatLabel string
	// CallSite is the parent frame's stop the entry was entered from. It
	// is the zero CallSite for a top-level entry.
	CallSite CallSite
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
