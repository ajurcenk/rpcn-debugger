package dap

import (
	"testing"
	"time"

	"github.com/google/go-dap"
)

// stack returns the stopped thread's stack trace.
func (c *client) stack() []dap.StackFrame {
	c.t.Helper()
	c.send("stackTrace", map[string]any{"threadId": 1})
	return c.response("stackTrace", true).(*dap.StackTraceResponse).Body.StackFrames
}

// stepTo sends command and checks the next stop has reason and its
// innermost frame is at line, with depth frames.
func (c *client) stepTo(command, reason string, line, depth int) []dap.StackFrame {
	c.t.Helper()
	c.send(command, map[string]any{"threadId": 1})
	c.response(command, true)
	st := c.event("stopped").(*dap.StoppedEvent)
	if st.Body.Reason != reason || st.Body.ThreadId != 1 || st.Body.AllThreadsStopped {
		c.t.Errorf("%s: stopped = %+v, want reason %s on thread 1 only", command, st.Body, reason)
	}
	frames := c.stack()
	if len(frames) != depth || frames[0].Line != line {
		c.t.Fatalf("%s: stack = %+v, want %d frames with the innermost at line %d", command, frames, depth, line)
	}
	return frames
}

// content returns the content of message 0 of batch 0 of frame frameID.
func (c *client) content(frameID int) string {
	c.t.Helper()
	return c.child(c.message(frameID, 0, 0), "content").Value
}

// Step In enters the branch: the call stack shows the branch frame at the
// child's line on top of the pipeline frame at the branch's line, each with
// its own Variables. Step Out returns to the pipeline level, and ids of an
// earlier stop resolve to nothing.
func TestNestedStack(t *testing.T) {
	eng := newFakeEngine()
	eng.nested = true
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	if st := c.configured(); st.Body.Reason != "entry" {
		t.Errorf("first stop = %+v, want entry", st.Body)
	}
	c.stepTo("stepIn", "step", 12, 1) // a leaf: same as next
	frames := c.stepTo("stepIn", "step", 14, 2)
	inner, outer := frames[0], frames[1]
	if inner.Name != "pipeline.processors[1].branch.processors" || outer.Name != "pipeline.processors" || outer.Line != 12 {
		t.Errorf("stack = %+v, want the branch frame on the pipeline frame at line 12", frames)
	}
	if inner.Id == outer.Id {
		t.Errorf("frames share id %d", inner.Id)
	}
	if got := c.content(inner.Id); got != `{"at":"1/0"}` {
		t.Errorf("branch frame content = %s, want 1/0", got)
	}
	if got := c.content(outer.Id); got != `{"at":"0/1"}` {
		t.Errorf("pipeline frame content = %s, want the batch before the branch (0/1)", got)
	}
	staleScope := c.scopes(outer.Id).VariablesReference

	c.stepTo("stepOut", "step", 13, 2) // the branch's end stop
	// The previous stop's frame id and references resolve to nothing.
	c.send("scopes", map[string]any{"frameId": outer.Id})
	if sc := c.response("scopes", true).(*dap.ScopesResponse); len(sc.Body.Scopes) != 0 {
		t.Errorf("scopes of a stale frame = %+v", sc.Body.Scopes)
	}
	if vars := c.variables(map[string]any{"variablesReference": staleScope}); len(vars) != 0 {
		t.Errorf("variables of a stale reference = %+v", vars)
	}
	c.stepTo("stepOut", "step", 10, 1) // the pipeline's end stop
	c.send("next", map[string]any{"threadId": 1})
	c.response("next", true)
	c.delivery(1)
	c.threadEvent("exited")
	c.event("terminated")

	c.send("threads", map[string]any{})
	if th := c.response("threads", true).(*dap.ThreadsResponse); len(th.Body.Threads) != 0 {
		t.Errorf("threads after the run = %+v", th.Body.Threads)
	}
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
}

// Pause while running stops at the next Arrive with reason pause; pause
// while stopped succeeds and changes nothing.
func TestPause(t *testing.T) {
	eng := newFakeEngine()
	eng.nested = true
	eng.slow = 300 * time.Millisecond
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.configured()
	frame := c.frameIDs()[0]
	c.send("pause", map[string]any{"threadId": 1})
	c.response("pause", true)
	if got := c.frameIDs()[0]; got != frame {
		t.Errorf("frame after pause while stopped = %d, want %d", got, frame)
	}

	c.send("continue", map[string]any{"threadId": 1})
	if resp := c.response("continue", true).(*dap.ContinueResponse); !resp.Body.AllThreadsContinued {
		t.Error("continue response without allThreadsContinued")
	}
	c.send("pause", map[string]any{"threadId": 1})
	c.response("pause", true)
	st := c.event("stopped").(*dap.StoppedEvent)
	if st.Body.Reason != "pause" {
		t.Errorf("stopped = %+v, want reason pause", st.Body)
	}
	if frames := c.stack(); len(frames) != 1 || frames[0].Line != 12 {
		t.Errorf("paused stack = %+v, want one frame at line 12", frames)
	}
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
}

// Disconnect while stopped two frames deep ends the run and Serve returns
// nil.
func TestDisconnectWhileNested(t *testing.T) {
	eng := newFakeEngine()
	eng.nested = true
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.configured()
	c.stepTo("stepIn", "step", 12, 1)
	c.stepTo("stepIn", "step", 14, 2)
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
	select {
	case err := <-eng.ended:
		if err == nil {
			t.Error("the run ended normally after disconnect")
		}
	default:
		t.Error("the run was still going after disconnect")
	}
}

// frameIDs returns the ids of the stopped thread's frames.
func (c *client) frameIDs() []int {
	c.t.Helper()
	var ids []int
	for _, f := range c.stack() {
		ids = append(ids, f.Id)
	}
	if len(ids) == 0 {
		c.t.Fatal("no frames")
	}
	return ids
}
