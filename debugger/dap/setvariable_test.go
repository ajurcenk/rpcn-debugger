package dap

import (
	"testing"

	"github.com/google/go-dap"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// setVariable sends a setVariable request and reads its response.
func (c *client) setVariable(ref int, name, value string, success bool) dap.ResponseMessage {
	c.t.Helper()
	c.send("setVariable", map[string]any{"variablesReference": ref, "name": name, "value": value})
	return c.response("setVariable", success)
}

// editInvalid expects a failed setVariable with error id EDIT_INVALID.
func (c *client) editInvalid(ref int, name, value, what string) {
	c.t.Helper()
	resp := c.setVariable(ref, name, value, false).(*dap.ErrorResponse)
	if resp.Body.Error == nil || resp.Body.Error.Id != core.Number(core.CodeEditInvalid) || resp.Message == "" {
		c.t.Errorf("%s: setVariable error = %+v (%s), want EDIT_INVALID (%d)", what, resp.Body.Error, resp.Message, core.Number(core.CodeEditInvalid))
	}
}

// setVariable edits a message's content and metadata values at the stop
// (AD-19): the response carries the new value and type, the Variables view
// shows the edit, an invalidated event follows when the client supports
// it, and every other variable is refused with EDIT_INVALID.
func TestSetVariable(t *testing.T) {
	eng := newFakeEngine()
	first := jsonMessage(0, `{"n":1}`)
	first.Metadata = []core.Meta{
		{Key: "__x", Type: core.MetaString, Value: "x", Internal: true},
		{Key: "count", Type: core.MetaInt, Value: "1"},
	}
	second := core.Message{Index: 1, Content: core.Content{Raw: []byte{0xff, 'a'}}, Metadata: []core.Meta{}, Error: "boom"}
	eng.batches = []core.Batch{{Messages: []core.Message{first, second}}}
	c := startServer(t, eng)
	c.send("initialize", map[string]any{"adapterID": "redpanda-connect", "supportsInvalidatedEvent": true})
	if resp := c.response("initialize", true).(*dap.InitializeResponse); !resp.Body.SupportsSetVariable {
		t.Error("initialize does not advertise supportsSetVariable")
	}
	c.send("launch", map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.response("launch", true)
	c.event("initialized")
	c.configured()
	frame := c.frameID()

	sc := c.scopes(frame)
	batches := c.variables(map[string]any{"variablesReference": sc.VariablesReference})
	batch := c.child(batches, "batch 0")
	msgs := c.variables(map[string]any{"variablesReference": batch.VariablesReference})
	m0 := c.child(msgs, "message 0").VariablesReference
	m1 := c.child(msgs, "message 1").VariablesReference
	md := c.child(c.variables(map[string]any{"variablesReference": m0}), "metadata").VariablesReference
	internal := c.child(c.variables(map[string]any{"variablesReference": md}), "internal").VariablesReference

	invalidated := func() {
		t.Helper()
		ev := c.event("invalidated").(*dap.InvalidatedEvent)
		if len(ev.Body.Areas) != 1 || ev.Body.Areas[0] != "variables" {
			t.Errorf("invalidated = %+v, want the variables area", ev.Body)
		}
	}
	resp := c.setVariable(m0, "content", `{"n":5}`, true).(*dap.SetVariableResponse)
	if resp.Body.Value != `{"n":5}` || resp.Body.Type != "json" {
		t.Errorf("content response = %+v", resp.Body)
	}
	invalidated()
	resp = c.setVariable(md, "count", "42", true).(*dap.SetVariableResponse)
	if resp.Body.Value != "42" || resp.Body.Type != "int" {
		t.Errorf("count response = %+v", resp.Body)
	}
	invalidated()
	resp = c.setVariable(internal, "__x", "y", true).(*dap.SetVariableResponse)
	if resp.Body.Value != "y" || resp.Body.Type != "string" {
		t.Errorf("__x response = %+v", resp.Body)
	}
	invalidated()
	resp = c.setVariable(m1, "content", "/wA=", true).(*dap.SetVariableResponse)
	if resp.Body.Value != "/wA=" || resp.Body.Type != "bytes" {
		t.Errorf("binary content response = %+v", resp.Body)
	}
	invalidated()

	// The view and the message's summary show the edits.
	msgs = c.variables(map[string]any{"variablesReference": batch.VariablesReference})
	if v := c.child(msgs, "message 0"); v.Value != `{"n":5}` {
		t.Errorf("message 0 summary = %q, want the edited content", v.Value)
	}
	if v := c.child(c.variables(map[string]any{"variablesReference": md}), "count"); v.Value != "42" || v.Type != "int" {
		t.Errorf("count = %+v", v)
	}

	c.editInvalid(md, "count", "4x", "bad int")
	c.editInvalid(md, "nope", "1", "unknown key")
	c.editInvalid(md, "__x", "z", "an internal key under metadata")
	c.editInvalid(internal, "count", "5", "a normal key under internal")
	c.editInvalid(md, "internal", "1", "the internal group")
	c.editInvalid(m1, "content", "not base64!", "bad base64")
	c.editInvalid(m1, "error", "fixed", "error")
	c.editInvalid(m0, "metadata", "{}", "the metadata node")
	c.editInvalid(batch.VariablesReference, "message 0", "x", "a message summary")
	c.editInvalid(sc.VariablesReference, "batch 0", "x", "a batch")
	c.editInvalid(0, "content", "x", "no reference")
	if v := c.child(c.variables(map[string]any{"variablesReference": md}), "count"); v.Value != "42" {
		t.Errorf("count after refused edits = %+v, want 42", v)
	}

	// A reference from an earlier stop, and any edit while running, is
	// refused.
	c.send("next", map[string]any{"threadId": 1})
	c.response("next", true)
	c.event("stopped")
	c.editInvalid(m0, "content", "x", "stale stop")
	c.send("continue", map[string]any{"threadId": 1})
	c.response("continue", true)
	c.delivery(1)
	c.runEnds()
	c.editInvalid(m0, "content", "x", "not stopped")
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
}

// Only the top frame of a nested stop is editable; a client that doesn't
// support the invalidated event gets none.
func TestSetVariableTopFrameOnly(t *testing.T) {
	eng := newFakeEngine()
	eng.nested = true
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.configured()
	c.send("next", map[string]any{"threadId": 1})
	c.response("next", true)
	c.event("stopped")
	c.send("stepIn", map[string]any{"threadId": 1})
	c.response("stepIn", true)
	c.event("stopped")
	c.send("stackTrace", map[string]any{"threadId": 1})
	st := c.response("stackTrace", true).(*dap.StackTraceResponse)
	if len(st.Body.StackFrames) != 2 {
		t.Fatalf("stack = %+v, want two frames", st.Body.StackFrames)
	}
	msgRef := func(frame int) int {
		sc := c.scopes(frame)
		b := c.child(c.variables(map[string]any{"variablesReference": sc.VariablesReference}), "batch 0")
		return c.child(c.variables(map[string]any{"variablesReference": b.VariablesReference}), "message 0").VariablesReference
	}
	top, parent := msgRef(st.Body.StackFrames[0].Id), msgRef(st.Body.StackFrames[1].Id)
	c.editInvalid(parent, "content", "x", "parent frame")
	if resp := c.setVariable(top, "content", "x", true).(*dap.SetVariableResponse); resp.Body.Value != "x" || resp.Body.Type != "text" {
		t.Errorf("top frame response = %+v", resp.Body)
	}
	// No invalidated event: the next message is the stopped event of the
	// next stop.
	c.send("next", map[string]any{"threadId": 1})
	c.response("next", true)
	c.event("stopped")
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
}
