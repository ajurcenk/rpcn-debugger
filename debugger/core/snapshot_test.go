package core

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func ptr[T any](v T) *T { return &v }

func TestSnapshotJSONRoundTrip(t *testing.T) {
	in := Snapshot{
		SchemaVersion: SchemaVersion,
		Position: Position{
			ThreadID: 1, FrameID: 2, StepperID: 3, ChildIndex: 4,
			UserPath: "pipeline.processors[4]", File: "/p.yaml", Line: 13,
		},
		Batches: []Batch{{Messages: []Message{
			{
				Index: 0,
				Content: Content{
					Raw:  []byte(`{"a":1}`),
					Text: ptr(`{"a":1}`),
					JSON: json.RawMessage(`{"a":1}`),
				},
				Metadata: []Meta{
					{Key: "kafka_key", Type: MetaBytes, Value: "YWJj", Text: ptr("abc")},
					{Key: "__rpcn_x", Type: MetaString, Value: "x", Internal: true},
				},
			},
			{Index: 1, Content: Content{Raw: []byte{0xff}}, Metadata: []Meta{}, Error: "failed"},
		}}},
		Skipped: []Skipped{{Index: 2, Reason: "maxMessages"}},
		Bloblang: &Bloblang{
			Unassigned: true,
			Vars:       map[string]json.RawMessage{"x": json.RawMessage(`1`)},
			Meta:       []Meta{{Key: "k", Type: MetaInt, Value: "5"}},
			Recomputed: []string{"uuid_v4"},
		},
	}
	raw, err := json.Marshal(in)
	if err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{
		`"schemaVersion":"1.0"`, `"threadId":1`, `"frameId":2`, `"stepperId":3`, `"childIndex":4`,
		`"userPath":"pipeline.processors[4]"`, `"file":"/p.yaml"`, `"line":13`,
		`"raw":"eyJhIjoxfQ=="`, `"text":"{\"a\":1}"`, `"json":{"a":1}`,
		`"key":"kafka_key","type":"bytes","value":"YWJj","text":"abc","internal":false`,
		`"internal":true`, `"error":"failed"`, `"skipped":[{"index":2,"reason":"maxMessages"}]`,
		`"unassigned":true`, `"recomputed":["uuid_v4"]`,
	} {
		if !strings.Contains(string(raw), field) {
			t.Errorf("JSON %s lacks %s", raw, field)
		}
	}
	var out Snapshot
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(in, out) {
		t.Errorf("round trip:\n got %+v\nwant %+v", out, in)
	}
	again, err := json.Marshal(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != string(raw) {
		t.Errorf("re-encoding differs:\n%s\n%s", again, raw)
	}
}
