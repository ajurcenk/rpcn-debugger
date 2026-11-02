package core

import (
	"fmt"
	"slices"
)

// ThreadID is the root thread: every stepper entry whose ctx has no frame
// runs on it (AD-17). Its id is stable for the whole run.
const ThreadID = 1

// rootThreadName is the root thread's name in frontends.
const rootThreadName = "batch"

// Thread is one live thread (AD-17).
type Thread struct {
	ID   int
	Name string
}

type thread struct {
	id      int
	name    string
	started bool
	exited  bool
	step    stepState
}

// ForkThread starts a thread for a stepper entry that has no frame in its
// ctx, or whose config is concurrent (AD-17), and returns its id. In this
// epic every entry runs on the root thread, which ConfigurationDone already
// started: ForkThread returns ThreadID (starting it, with a ThreadStarted
// event, only if it isn't yet). Concurrent
// forking arrives with epic-concurrency.
func (s *Session) ForkThread() int {
	s.mu.Lock()
	started := s.forkThreadLocked()
	s.mu.Unlock()
	if started {
		s.fe.Emit(ThreadStarted{ThreadID: ThreadID, Name: rootThreadName})
	}
	return ThreadID
}

// forkThreadLocked marks the root thread started and reports whether it
// just started.
func (s *Session) forkThreadLocked() bool {
	t := s.threadLocked(ThreadID)
	if t.started || s.ended {
		return false
	}
	t.started = true
	return true
}

// ExitThread is called by the entry that forked thread id when it returns
// (AD-17). The root thread is shared by every top-level entry, so it ends
// with the run instead, and ExitThread leaves it running.
func (s *Session) ExitThread(id int) {
	if id == ThreadID {
		return
	}
	s.mu.Lock()
	t := s.threads[id]
	exit := t != nil && t.started && !t.exited && !s.ended
	if exit {
		t.exited = true
	}
	s.mu.Unlock()
	if exit {
		s.fe.Emit(ThreadExited{ThreadID: id})
	}
}

// Threads returns the live threads, in id order.
func (s *Session) Threads() []Thread {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := []Thread{}
	for _, t := range s.threads {
		if t.started && !t.exited {
			out = append(out, Thread{ID: t.id, Name: t.name})
		}
	}
	slices.SortFunc(out, func(a, b Thread) int { return a.ID - b.ID })
	return out
}

// threadLocked returns thread id's record, creating it unstarted.
func (s *Session) threadLocked(id int) *thread {
	t := s.threads[id]
	if t == nil {
		name := rootThreadName
		if id != ThreadID {
			name = fmt.Sprintf("thread %d", id)
		}
		t = &thread{id: id, name: name}
		s.threads[id] = t
	}
	return t
}
