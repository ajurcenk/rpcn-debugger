package engine

import (
	"encoding/base64"
	"encoding/json"
	"errors"
	"reflect"
	"strconv"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// decode turns a lossless Meta value back into the Go value it came from.
func decode(t *testing.T, e core.Meta) any {
	t.Helper()
	switch e.Type {
	case core.MetaString:
		return e.Value
	case core.MetaBytes:
		b, err := base64.StdEncoding.DecodeString(e.Value)
		if err != nil {
			t.Fatal(err)
		}
		return b
	case core.MetaInt:
		n, err := strconv.ParseInt(e.Value, 10, 64)
		if err != nil {
			t.Fatal(err)
		}
		return n
	case core.MetaFloat:
		f, err := strconv.ParseFloat(e.Value, 64)
		if err != nil {
			t.Fatal(err)
		}
		return f
	case core.MetaBool:
		b, err := strconv.ParseBool(e.Value)
		if err != nil {
			t.Fatal(err)
		}
		return b
	case core.MetaTimestamp:
		ts, err := time.Parse(time.RFC3339Nano, e.Value)
		if err != nil {
			t.Fatal(err)
		}
		return ts
	case core.MetaObject:
		var m map[string]any
		if err := json.Unmarshal([]byte(e.Value), &m); err != nil {
			t.Fatal(err)
		}
		return m
	case core.MetaArray:
		var a []any
		if err := json.Unmarshal([]byte(e.Value), &a); err != nil {
			t.Fatal(err)
		}
		return a
	case core.MetaNull:
		return nil
	}
	t.Fatalf("type %q is not in the closed set", e.Type)
	return nil
}

func TestMetadataRoundTrip(t *testing.T) {
	ts := time.Date(2026, 10, 16, 12, 30, 45, 123456789, time.FixedZone("x", 3600))
	values := map[string]struct {
		in   any
		typ  core.MetaType
		want any // decoded value; in when nil-able types differ
	}{
		"s":   {"hello", core.MetaString, "hello"},
		"b":   {[]byte{0, 1, 0xff}, core.MetaBytes, []byte{0, 1, 0xff}},
		"i":   {int64(-9007199254740993), core.MetaInt, int64(-9007199254740993)},
		"f":   {0.1 + 0.2, core.MetaFloat, 0.1 + 0.2},
		"t":   {true, core.MetaBool, true},
		"ts":  {ts, core.MetaTimestamp, ts},
		"obj": {map[string]any{"a": "b", "n": []any{"x"}}, core.MetaObject, map[string]any{"a": "b", "n": []any{"x"}}},
		"arr": {[]any{"a", true}, core.MetaArray, []any{"a", true}},
		"nil": {nil, core.MetaNull, nil},
	}
	m := service.NewMessage([]byte("x"))
	for k, v := range values {
		m.MetaSetMut(k, v.in)
	}
	got := snapshotBatches([]service.MessageBatch{{m}})
	meta := got[0].Messages[0].Metadata
	if len(meta) != len(values) {
		t.Fatalf("metadata = %+v, want %d keys", meta, len(values))
	}
	for i := 1; i < len(meta); i++ {
		if meta[i-1].Key >= meta[i].Key {
			t.Errorf("metadata not sorted: %q before %q", meta[i-1].Key, meta[i].Key)
		}
	}
	for _, e := range meta {
		v := values[e.Key]
		if e.Type != v.typ {
			t.Errorf("%s: type = %q, want %q", e.Key, e.Type, v.typ)
			continue
		}
		dec := decode(t, e)
		if wt, ok := v.want.(time.Time); ok {
			if !dec.(time.Time).Equal(wt) || dec.(time.Time).Nanosecond() != wt.Nanosecond() {
				t.Errorf("%s: %v, want %v", e.Key, dec, wt)
			}
			continue
		}
		if !reflect.DeepEqual(dec, v.want) {
			t.Errorf("%s: decoded %#v, want %#v (value %q)", e.Key, dec, v.want, e.Value)
		}
		if e.Internal {
			t.Errorf("%s: internal", e.Key)
		}
	}
}

func TestOtherNumberTypes(t *testing.T) {
	for _, tc := range []struct {
		in   any
		typ  core.MetaType
		want string
	}{
		{int(7), core.MetaInt, "7"},
		{int32(-7), core.MetaInt, "-7"},
		{uint64(18446744073709551615), core.MetaInt, "18446744073709551615"},
		{float32(1.5), core.MetaFloat, "1.5"},
		{json.Number("12"), core.MetaInt, "12"},
		{json.Number("1.25"), core.MetaFloat, "1.25"},
	} {
		e := metaEntry("k", tc.in)
		if e.Type != tc.typ || e.Value != tc.want {
			t.Errorf("metaEntry(%T %v) = %+v, want %s %s", tc.in, tc.in, e, tc.typ, tc.want)
		}
	}
}

func TestInternalAndBytesText(t *testing.T) {
	m := service.NewMessage([]byte("x"))
	m.MetaSetMut("__rpcn_x", "v")
	m.MetaSetMut("kafka_key", []byte("abc"))
	m.MetaSetMut("bin", []byte{0xff, 0xfe})
	meta := map[string]core.Meta{}
	for _, e := range snapshotBatches([]service.MessageBatch{{m}})[0].Messages[0].Metadata {
		meta[e.Key] = e
	}
	if e := meta["__rpcn_x"]; !e.Internal {
		t.Errorf("__rpcn_x = %+v, want internal", e)
	}
	if e := meta["kafka_key"]; e.Internal || e.Text == nil || *e.Text != "abc" || e.Value != "YWJj" {
		t.Errorf("kafka_key = %+v, want text abc and base64 value", e)
	}
	if e := meta["bin"]; e.Text != nil || e.Value != "//4=" {
		t.Errorf("bin = %+v, want no text and a base64 value", e)
	}
}

func TestContentViewsAndErrors(t *testing.T) {
	j := service.NewMessage([]byte(`{"a":1}`))
	raw := service.NewMessage([]byte{0xff, 'a'})
	text := service.NewMessage([]byte("plain text"))
	text.SetError(errors.New("boom"))
	got := snapshotBatches([]service.MessageBatch{{j, raw}, {text}})
	if len(got) != 2 || len(got[0].Messages) != 2 || len(got[1].Messages) != 1 {
		t.Fatalf("batches = %+v", got)
	}
	c := got[0].Messages[0].Content
	if string(c.Raw) != `{"a":1}` || c.Text == nil || *c.Text != `{"a":1}` || string(c.JSON) != `{"a":1}` {
		t.Errorf("json content = %+v", c)
	}
	if got[0].Messages[0].Error != "" {
		t.Errorf("error = %q, want none", got[0].Messages[0].Error)
	}
	c = got[0].Messages[1].Content
	if string(c.Raw) != "\xffa" || c.Text != nil || c.JSON != nil {
		t.Errorf("binary content = %+v, want raw only", c)
	}
	m := got[1].Messages[0]
	if m.Index != 0 || got[0].Messages[1].Index != 1 {
		t.Errorf("indices = %d, %d", got[0].Messages[1].Index, m.Index)
	}
	if m.Content.Text == nil || *m.Content.Text != "plain text" || m.Content.JSON != nil {
		t.Errorf("text content = %+v, want text without json", m.Content)
	}
	if m.Error != "boom" {
		t.Errorf("error = %q, want boom", m.Error)
	}
}

// The Snapshot is a deep copy: changing the message afterwards doesn't show.
func TestSnapshotIsACopy(t *testing.T) {
	raw := []byte("abc")
	m := service.NewMessage(raw)
	m.MetaSetMut("b", []byte("x"))
	got := snapshotBatches([]service.MessageBatch{{m}})
	raw[0] = 'z'
	m.SetBytes([]byte("changed"))
	m.MetaSetMut("b", []byte("y"))
	msg := got[0].Messages[0]
	if string(msg.Content.Raw) != "abc" || *msg.Content.Text != "abc" || msg.Metadata[0].Value != "eA==" {
		t.Errorf("snapshot changed with the message: %+v", msg)
	}
}
