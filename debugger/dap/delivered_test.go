package dap

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/google/go-dap"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// runToEnd launches eng's pipeline, continues from the entry stop and
// returns the events up to terminated.
func runToEnd(t *testing.T, eng *fakeEngine) []dap.Message {
	t.Helper()
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.configured()
	c.send("continue", map[string]any{"threadId": 1})
	c.response("continue", true)
	var got []dap.Message
	for {
		m := c.next()
		got = append(got, m)
		if e, ok := m.(dap.EventMessage); ok && e.GetEvent().Event == "terminated" {
			break
		}
	}
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	return got
}

// Each delivered batch shows in the Debug Console, numbered from 1, with
// each message's content, metadata (internal keys marked) and error, and
// goes out as an rpcnDelivered event with the full Snapshot (AD-21).
func TestDelivered(t *testing.T) {
	eng := newFakeEngine()
	big := strings.Repeat("x", 10*1024)
	msg := jsonMessage(0, `{"greeting":"hello"}`)
	msg.Metadata = []core.Meta{
		{Key: "__rpcn_internal", Type: core.MetaString, Value: "x", Internal: true},
		{Key: "kafka_key", Type: core.MetaBytes, Value: "azE=", Text: text("k1")},
		{Key: "n", Type: core.MetaInt, Value: "2"},
	}
	failed := core.Message{Index: 1, Content: core.Content{Raw: []byte{0xff, 0x00}}, Metadata: []core.Meta{}, Error: "boom"}
	large := core.Message{Index: 0, Content: core.Content{Raw: []byte(big), Text: text(big)}, Metadata: []core.Meta{}}
	eng.deliver = []core.Batch{{Messages: []core.Message{msg, failed}}, {Messages: []core.Message{large}}}

	var outputs []string
	var events []*deliveredEvent
	for _, m := range runToEnd(t, eng) {
		switch e := m.(type) {
		case *dap.OutputEvent:
			if strings.HasPrefix(e.Body.Output, "Delivered") {
				outputs = append(outputs, e.Body.Output)
				if e.Body.Source == nil || e.Body.Source.Path != "/p/pipeline.yaml" || e.Body.Line != 20 {
					t.Errorf("output source = %+v line %d, want the output key", e.Body.Source, e.Body.Line)
				}
			}
		case *deliveredEvent:
			events = append(events, e)
		}
	}
	if len(outputs) != 2 || len(events) != 2 {
		t.Fatalf("outputs = %q, events = %d; want 2 of each", outputs, len(events))
	}
	want := "Delivered batch 1 (2 messages) to dry-run output\n" +
		"  message 0: {\"greeting\":\"hello\"}\n" +
		"    __rpcn_internal = x (internal)\n" +
		"    kafka_key = k1\n" +
		"    n = 2\n" +
		"  message 1 (base64): /wA=\n" +
		"    error: boom\n"
	if outputs[0] != want {
		t.Errorf("batch 1 text:\n%s\nwant:\n%s", outputs[0], want)
	}
	want = "Delivered batch 2 (1 messages) to dry-run output\n" +
		"  message 0: " + big[:4096] + "… 6144 more bytes\n"
	if outputs[1] != want {
		t.Errorf("batch 2 text = %.200q…, want the first 4 KiB and the rest counted", outputs[1])
	}

	for i, e := range events {
		if e.Body.Batch != i+1 {
			t.Errorf("event %d batch = %d", i, e.Body.Batch)
		}
		p := e.Body.Snapshot.Position
		if p.UserPath != "output" || p.Line != 20 || p.File != "/p/pipeline.yaml" || e.Body.Snapshot.SchemaVersion != core.SchemaVersion {
			t.Errorf("event %d snapshot = %+v", i, e.Body.Snapshot)
		}
		if len(e.Body.Snapshot.Batches) != 1 {
			t.Fatalf("event %d batches = %d, want 1", i, len(e.Body.Snapshot.Batches))
		}
	}
	// The event carries the full content, losslessly.
	if got := events[1].Body.Snapshot.Batches[0].Messages[0].Content.Raw; !bytes.Equal(got, []byte(big)) {
		t.Errorf("event content is %d bytes, want %d", len(got), len(big))
	}
	if got := events[0].Body.Snapshot.Batches[0].Messages[1]; !bytes.Equal(got.Content.Raw, []byte{0xff, 0x00}) || got.Error != "boom" {
		t.Errorf("event message 1 = %+v", got)
	}
	if got := events[0].Body.Snapshot.Batches[0].Messages[0].Metadata; len(got) != 3 || !got[0].Internal || got[1].Value != "azE=" {
		t.Errorf("event metadata = %+v", got)
	}
}

// The console header and the rpcnDelivered body follow each batch's
// outcome, and the batch number is the Session's (AD-3, AD-21).
func TestDeliveredOutcomes(t *testing.T) {
	eng := newFakeEngine()
	b := core.Batch{Messages: []core.Message{jsonMessage(0, `{"n":1}`)}}
	eng.deliver = []core.Batch{b, b, b, b}
	eng.outcomes = []core.Outcome{core.OutcomeRecorded, core.OutcomeWritten, core.OutcomeDiscarded, core.OutcomeFailed}
	eng.writeErr = "refused"
	var headers []string
	var events []*deliveredEvent
	for _, m := range runToEnd(t, eng) {
		switch e := m.(type) {
		case *dap.OutputEvent:
			if strings.HasPrefix(e.Body.Output, "Delivered") {
				headers = append(headers, strings.SplitN(e.Body.Output, "\n", 2)[0])
			}
		case *deliveredEvent:
			events = append(events, e)
		}
	}
	want := []string{
		"Delivered batch 1 (1 messages) to dry-run output",
		"Delivered batch 2 (1 messages), written to the output",
		"Delivered batch 3 (1 messages), not written (output: none)",
		"Delivered batch 4 (1 messages), failed to write: refused",
	}
	if strings.Join(headers, "\n") != strings.Join(want, "\n") {
		t.Errorf("headers:\n%s\nwant:\n%s", strings.Join(headers, "\n"), strings.Join(want, "\n"))
	}
	if len(events) != 4 {
		t.Fatalf("%d rpcnDelivered events, want 4", len(events))
	}
	for i, e := range events {
		wantErr := ""
		if i == 3 {
			wantErr = "refused"
		}
		if e.Body.Batch != i+1 || e.Body.Outcome != string(eng.outcomes[i]) || e.Body.Error != wantErr {
			t.Errorf("event %d = batch %d, outcome %q, error %q", i, e.Body.Batch, e.Body.Outcome, e.Body.Error)
		}
	}
}

// The rpcnDelivered body is {batch, outcome, error?, snapshot} with the
// AD-11 Snapshot JSON; error is left out unless the write failed.
func TestDeliveredEventJSON(t *testing.T) {
	ev := &deliveredEvent{Event: dap.Event{ProtocolMessage: dap.ProtocolMessage{Seq: 3, Type: "event"}, Event: deliveredEventName}}
	ev.Body = deliveredBody{Batch: 1, Outcome: "recorded", Snapshot: core.Snapshot{SchemaVersion: core.SchemaVersion, Batches: []core.Batch{}}}
	raw, err := json.Marshal(ev)
	if err != nil {
		t.Fatal(err)
	}
	var got map[string]any
	if err := json.Unmarshal(raw, &got); err != nil {
		t.Fatal(err)
	}
	body, _ := got["body"].(map[string]any)
	snap, _ := body["snapshot"].(map[string]any)
	_, hasErr := body["error"]
	if got["event"] != "rpcnDelivered" || got["type"] != "event" || body["batch"] != float64(1) || body["outcome"] != "recorded" || hasErr ||
		snap["schemaVersion"] != core.SchemaVersion {
		t.Errorf("rpcnDelivered = %s", raw)
	}
}

// A cut never splits a UTF-8 character.
func TestConsoleContentCut(t *testing.T) {
	s := strings.Repeat("a", 4095) + "é" + "b"
	text, b64, more := consoleContent(core.Content{Raw: []byte(s), Text: &s})
	if b64 || text != strings.Repeat("a", 4095) || more != 3 {
		t.Errorf("consoleContent = %d bytes, base64 %v, %d more; want 4095 bytes and 3 more", len(text), b64, more)
	}
	short := "hi"
	if text, _, more := consoleContent(core.Content{Raw: []byte(short), Text: &short}); text != "hi" || more != 0 {
		t.Errorf("consoleContent(hi) = %q, %d", text, more)
	}
}

// A run that delivers nothing says so, then terminates.
func TestNothingDelivered(t *testing.T) {
	eng := newFakeEngine()
	eng.deliver = []core.Batch{}
	var texts []string
	for _, m := range runToEnd(t, eng) {
		switch e := m.(type) {
		case *dap.OutputEvent:
			texts = append(texts, e.Body.Output)
		case *deliveredEvent:
			t.Errorf("rpcnDelivered %+v, want none", e.Body)
		}
	}
	if len(texts) != 1 || texts[0] != "Nothing was delivered to dry-run output\n" {
		t.Errorf("outputs = %q, want the nothing-delivered note", texts)
	}
}
