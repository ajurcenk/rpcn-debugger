package core

import "sync"

// gate is the pause gate (AD-9). It is process-wide for the debuggee: every
// stepper, on every thread, takes the same gate in Arrive, holds it while the
// stop predicate is decided and for as long as it is paused, and releases it
// before the next processor runs. It is never held while a processor runs.
type gate struct {
	mu sync.Mutex
}

func (g *gate) lock()   { g.mu.Lock() }
func (g *gate) unlock() { g.mu.Unlock() }
