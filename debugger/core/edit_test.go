package core

import (
	"context"
	"encoding/json"
	"errors"
	"reflect"
	"testing"
)

func strp(s string) *string { return &s }

// Every metadata type parses its accepted text into its lossless value,
// and refuses the rest.
func TestEditMeta(t *testing.T) {
	for _, tc := range []struct {
		cur     Meta
		entered string
		want    string  // the lossless value; "" with bad
		text    *string // the display text, for bytes
		bad     bool
	}{
		{cur: Meta{Key: "s", Type: MetaString, Value: "a"}, entered: "4x", want: "4x"},
		{cur: Meta{Key: "s", Type: MetaString, Value: "a"}, entered: "", want: ""},
		{cur: Meta{Key: "b", Type: MetaBytes, Value: "aGk=", Text: strp("hi")}, entered: "yo", want: "eW8=", text: strp("yo")},
		{cur: Meta{Key: "b", Type: MetaBytes, Value: "/w=="}, entered: "/wA=", want: "/wA="},
		{cur: Meta{Key: "b", Type: MetaBytes, Value: "/w=="}, entered: "aGk=", want: "aGk=", text: strp("hi")},
		{cur: Meta{Key: "b", Type: MetaBytes, Value: "/w=="}, entered: "not base64!", bad: true},
		{cur: Meta{Key: "i", Type: MetaInt, Value: "1"}, entered: "42", want: "42"},
		{cur: Meta{Key: "i", Type: MetaInt, Value: "1"}, entered: "-7", want: "-7"},
		{cur: Meta{Key: "i", Type: MetaInt, Value: "1"}, entered: "18446744073709551615", want: "18446744073709551615"},
		{cur: Meta{Key: "i", Type: MetaInt, Value: "1"}, entered: "4x", bad: true},
		{cur: Meta{Key: "i", Type: MetaInt, Value: "1"}, entered: "1.5", bad: true},
		{cur: Meta{Key: "f", Type: MetaFloat, Value: "1.5"}, entered: "2.25", want: "2.25"},
		{cur: Meta{Key: "f", Type: MetaFloat, Value: "1.5"}, entered: "1e3", want: "1000"},
		{cur: Meta{Key: "f", Type: MetaFloat, Value: "1.5"}, entered: "x", bad: true},
		{cur: Meta{Key: "t", Type: MetaBool, Value: "true"}, entered: "false", want: "false"},
		{cur: Meta{Key: "t", Type: MetaBool, Value: "true"}, entered: "1", bad: true},
		{cur: Meta{Key: "t", Type: MetaBool, Value: "true"}, entered: "True", bad: true},
		{cur: Meta{Key: "ts", Type: MetaTimestamp, Value: "2026-01-01T00:00:00Z"}, entered: "2026-11-03T10:00:00+01:00", want: "2026-11-03T10:00:00+01:00"},
		{cur: Meta{Key: "ts", Type: MetaTimestamp, Value: "2026-01-01T00:00:00Z"}, entered: "2026-11-03T10:00:00.5Z", want: "2026-11-03T10:00:00.5Z"},
		{cur: Meta{Key: "ts", Type: MetaTimestamp, Value: "2026-01-01T00:00:00Z"}, entered: "2026-11-03", bad: true},
		{cur: Meta{Key: "o", Type: MetaObject, Value: "{}"}, entered: `{ "a": 1 }`, want: `{"a":1}`},
		{cur: Meta{Key: "o", Type: MetaObject, Value: "{}"}, entered: `{"b":1,"a":2.50}`, want: `{"a":2.50,"b":1}`},
		{cur: Meta{Key: "o", Type: MetaObject, Value: "{}"}, entered: `[1]`, bad: true},
		{cur: Meta{Key: "o", Type: MetaObject, Value: "{}"}, entered: `{"a":`, bad: true},
		{cur: Meta{Key: "o", Type: MetaObject, Value: "{}"}, entered: `{} {}`, bad: true},
		{cur: Meta{Key: "a", Type: MetaArray, Value: "[]"}, entered: `[1, "x"]`, want: `[1,"x"]`},
		{cur: Meta{Key: "a", Type: MetaArray, Value: "[]"}, entered: `{}`, bad: true},
		{cur: Meta{Key: "n", Type: MetaNull, Value: "null"}, entered: "null", want: "null"},
		{cur: Meta{Key: "n", Type: MetaNull, Value: "null"}, entered: "1", bad: true},
	} {
		got, err := editMeta(tc.cur, tc.entered)
		if tc.bad {
			if err == nil {
				t.Errorf("%s %s := %q = %+v, want refused", tc.cur.Type, tc.cur.Key, tc.entered, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%s %s := %q: %v", tc.cur.Type, tc.cur.Key, tc.entered, err)
			continue
		}
		if got.Key != tc.cur.Key || got.Type != tc.cur.Type || got.Value != tc.want || !reflect.DeepEqual(got.Text, tc.text) {
			t.Errorf("%s %s := %q = %+v, want value %q text %v", tc.cur.Type, tc.cur.Key, tc.entered, got, tc.want, tc.text)
		}
	}
}

// Content shown as text takes the entered bytes; content shown as base64
// takes the decoded text, or refuses it.
func TestEditContent(t *testing.T) {
	text := contentOf([]byte("hi"))
	if raw, err := editContent(text, []byte("aGk=")); err != nil || string(raw) != "aGk=" {
		t.Errorf("text content := aGk= = %q, %v; want the text as is", raw, err)
	}
	bin := contentOf([]byte{0xff, 0x00})
	if bin.Text != nil {
		t.Fatal("binary content has a text view")
	}
	if raw, err := editContent(bin, []byte("AAE=")); err != nil || !reflect.DeepEqual(raw, []byte{0, 1}) {
		t.Errorf("binary content := AAE= = %v, %v; want [0 1]", raw, err)
	}
	if _, err := editContent(bin, []byte("not base64!")); err == nil {
		t.Error("binary content := invalid base64 was accepted")
	}
	if c := contentOf([]byte(`{"n":5}`)); c.Text == nil || string(c.JSON) != `{"n":5}` {
		t.Errorf("contentOf JSON = %+v, want text and JSON views", c)
	}
}

// editRun receives what each Arrive of editEngine returned, in order.
type editRun struct {
	edits chan []SetMessage
}

// editEngine runs one stepper 0 of two children on batch, entering a
// nested stepper 1 of one child at child 1.
func editEngine(batch []Batch, r *editRun) funcEngine {
	return funcEngine{
		lines: lines{
			{0, 0}: {UserPath: "pipeline.processors[0]", Line: 11},
			{0, 1}: {UserPath: "pipeline.processors[1]", Line: 12},
			{0, 2}: {UserPath: "pipeline.processors", Line: 10},
			{1, 0}: {UserPath: "pipeline.processors[1].branch.processors[0]", Line: 14},
			{1, 1}: {UserPath: "pipeline.processors[1].branch.processors", Line: 13},
		},
		start: func(ctx context.Context, a Arriver) error {
			ctx, exit := a.Enter(ctx, Entry{StepperID: 0, Children: 2})
			defer exit(nil)
			arrive := func(ctx context.Context, stepper, child int) error {
				e, err := a.Arrive(ctx, stepper, child, batch)
				r.edits <- e
				return err
			}
			if err := arrive(ctx, 0, 0); err != nil {
				return err
			}
			if err := arrive(ctx, 0, 1); err != nil {
				return err
			}
			in, exitIn := a.Enter(ctx, Entry{StepperID: 1, Children: 1})
			if err := arrive(in, 1, 0); err != nil {
				return err
			}
			if err := arrive(in, 1, 1); err != nil {
				return err
			}
			exitIn(nil)
			return arrive(ctx, 0, 2)
		},
	}
}

func editBatch() []Batch {
	return []Batch{{Messages: []Message{{
		Index:   0,
		Content: contentOf([]byte(`{"n":1}`)),
		Metadata: []Meta{
			{Key: "__internal", Type: MetaString, Value: "x", Internal: true},
			{Key: "count", Type: MetaInt, Value: "1"},
			{Key: "name", Type: MetaString, Value: "a"},
		},
	}}}}
}

func editLaunched(t *testing.T) (*Session, *recorder, *editRun) {
	t.Helper()
	r := &editRun{edits: make(chan []SetMessage, 16)}
	rec := newRecorder()
	s := NewSession(editEngine(editBatch(), r), rec)
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	if ev := rec.next(t); ev != (event{"stopped", ReasonEntry}) {
		t.Fatalf("event = %+v, want stopped entry", ev)
	}
	return s, rec, r
}

func wantEditInvalid(t *testing.T, s *Session, c SetMessage, what string) {
	t.Helper()
	err := do(s, c)
	var ce Error
	if !errors.As(err, &ce) || ce.Code != CodeEditInvalid || ce.Message == "" {
		t.Errorf("%s: SetMessage = %v, want EDIT_INVALID", what, err)
	}
}

// An edit shows in the stop's top-frame Snapshot at once, leaves the
// Snapshot a frontend read before it (and every other frame's) as it was,
// and goes to the stepper, in order, when the stop resumes.
func TestSetMessage(t *testing.T) {
	s, rec, r := editLaunched(t)
	before, _ := s.Stopped()
	top := before.Frames[0]

	if err := do(s, SetMessage{StopID: before.ID, FrameID: top.ID, Content: []byte(`{"n":5}`)}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, SetMessage{StopID: before.ID, FrameID: top.ID, Metadata: []Meta{{Key: "count", Type: MetaInt, Value: "42"}, {Key: "__internal", Value: "y"}}}); err != nil {
		t.Fatal(err)
	}
	after, _ := s.Stopped()
	m := after.Frames[0].Snapshot.Batches[0].Messages[0]
	if m.Content.Text == nil || *m.Content.Text != `{"n":5}` || string(m.Content.JSON) != `{"n":5}` {
		t.Errorf("edited content = %+v, want {\"n\":5} with its views", m.Content)
	}
	if m.Metadata[1] != (Meta{Key: "count", Type: MetaInt, Value: "42"}) || m.Metadata[0].Value != "y" || !m.Metadata[0].Internal {
		t.Errorf("edited metadata = %+v", m.Metadata)
	}
	if got := before.Frames[0].Snapshot.Batches[0].Messages[0]; string(got.Content.Raw) != `{"n":1}` || got.Metadata[1].Value != "1" {
		t.Errorf("the Snapshot read before the edit changed: %+v", got)
	}
	// A refused edit changes nothing, not even the keys before the bad one.
	wantEditInvalid(t, s, SetMessage{StopID: before.ID, FrameID: top.ID, Metadata: []Meta{{Key: "name", Value: "b"}, {Key: "count", Value: "4x"}}}, "bad int")
	if now, _ := s.Stopped(); !reflect.DeepEqual(now.Frames, after.Frames) {
		t.Errorf("a refused edit changed the stop: %+v", now.Frames[0].Snapshot.Batches)
	}

	if err := do(s, Next{}); err != nil {
		t.Fatal(err)
	}
	got := <-r.edits
	want := []SetMessage{
		{StopID: before.ID, FrameID: top.ID, Content: []byte(`{"n":5}`)},
		{StopID: before.ID, FrameID: top.ID, Metadata: []Meta{{Key: "count", Type: MetaInt, Value: "42"}, {Key: "__internal", Type: MetaString, Value: "y", Internal: true}}},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("Arrive's edits = %+v, want %+v", got, want)
	}
	if ev := rec.next(t); ev != (event{"stopped", ReasonStep}) {
		t.Fatalf("event = %+v", ev)
	}
	// The next stop starts with no edits, and its Snapshot is the stepper's.
	if err := do(s, Next{}); err != nil {
		t.Fatal(err)
	}
	if got := <-r.edits; got != nil {
		t.Errorf("edits of a stop with none = %+v", got)
	}
}

// Only the top frame of the current stop, its batches, messages and keys,
// can be edited, with a value of the key's type, and only while stopped.
func TestSetMessageRefusals(t *testing.T) {
	s, rec, r := editLaunched(t)
	// Step into the branch: two frames.
	if err := do(s, Next{}); err != nil {
		t.Fatal(err)
	}
	<-r.edits
	rec.next(t)
	if err := do(s, StepIn{}); err != nil {
		t.Fatal(err)
	}
	<-r.edits
	rec.next(t)
	st, _ := s.Stopped()
	if len(st.Frames) != 2 {
		t.Fatalf("stop has %d frames, want 2", len(st.Frames))
	}
	top, parent := st.Frames[0].ID, st.Frames[1].ID
	for what, c := range map[string]SetMessage{
		"parent frame":   {StopID: st.ID, FrameID: parent, Content: []byte("x")},
		"unknown frame":  {StopID: st.ID, FrameID: 999, Content: []byte("x")},
		"unknown batch":  {StopID: st.ID, FrameID: top, Batch: 1, Content: []byte("x")},
		"unknown msg":    {StopID: st.ID, FrameID: top, Index: 1, Content: []byte("x")},
		"negative msg":   {StopID: st.ID, FrameID: top, Index: -1, Content: []byte("x")},
		"unknown key":    {StopID: st.ID, FrameID: top, Metadata: []Meta{{Key: "nope", Value: "1"}}},
		"type change":    {StopID: st.ID, FrameID: top, Metadata: []Meta{{Key: "count", Type: MetaString, Value: "1"}}},
		"bad int":        {StopID: st.ID, FrameID: top, Metadata: []Meta{{Key: "count", Value: "4x"}}},
		"nothing to set": {StopID: st.ID, FrameID: top},
		"stale stop":     {StopID: st.ID - 1, FrameID: top, Content: []byte("x")},
	} {
		wantEditInvalid(t, s, c, what)
	}
	// The end stop of the branch takes edits too: they go to the batches
	// the entry hands back.
	if err := do(s, Next{}); err != nil {
		t.Fatal(err)
	}
	<-r.edits
	rec.next(t)
	if p := stopAt(t, s); p.StepperID != 1 || p.ChildIndex != 1 {
		t.Fatalf("stop = %+v, want the branch's end stop", p)
	}
	end, _ := s.Stopped()
	if err := do(s, SetMessage{StopID: end.ID, FrameID: end.Frames[0].ID, Metadata: []Meta{{Key: "name", Value: "end"}}}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	if got := <-r.edits; len(got) != 1 || got[0].Metadata[0].Value != "end" {
		t.Errorf("end-stop edits = %+v", got)
	}
	<-r.edits
	rec.runEnds(t)
	wantEditInvalid(t, s, SetMessage{StopID: end.ID, FrameID: top, Content: []byte("x")}, "not stopped")
}

// An edit at a later stop of an entry leaves the Snapshot of its earlier
// stop as it was: the entry's frame lives on across its stops, and the
// stepper passes the same batches to both.
func TestSetMessageLeavesEarlierStops(t *testing.T) {
	s, rec, r := editLaunched(t)
	first, _ := s.Stopped()
	want, err := json.Marshal(first.Frames[0].Snapshot)
	if err != nil {
		t.Fatal(err)
	}
	if err := do(s, Next{}); err != nil {
		t.Fatal(err)
	}
	<-r.edits
	rec.next(t)
	second, _ := s.Stopped()
	if second.Frames[0].ID != first.Frames[0].ID || second.ID == first.ID {
		t.Fatalf("stops %d and %d, frames %d and %d: want two stops of one entry", first.ID, second.ID, first.Frames[0].ID, second.Frames[0].ID)
	}
	// The frame id alone doesn't name the stop: stop 1's edit is stale.
	wantEditInvalid(t, s, SetMessage{StopID: first.ID, FrameID: first.Frames[0].ID, Content: []byte("x")}, "earlier stop")
	if err := do(s, SetMessage{StopID: second.ID, FrameID: second.Frames[0].ID, Content: []byte(`{"n":9}`)}); err != nil {
		t.Fatal(err)
	}
	if got, _ := json.Marshal(first.Frames[0].Snapshot); string(got) != string(want) {
		t.Errorf("stop 1's Snapshot changed:\n%s\nwant:\n%s", got, want)
	}
	now, _ := s.Stopped()
	if c := now.Frames[0].Snapshot.Batches[0].Messages[0].Content; c.Text == nil || *c.Text != `{"n":9}` {
		t.Errorf("stop 2's content = %+v, want the edit", c)
	}
}
