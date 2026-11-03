package engine

import (
	"bytes"
	"encoding/json"
	"errors"
	"math"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// captureBatch is a batch with every metadata type, binary and JSON
// content, and a message error.
func captureBatch() service.MessageBatch {
	m0 := service.NewMessage([]byte{0, 1, 0xfe, 0xff})
	ts := time.Date(2026, 11, 3, 9, 8, 7, 654321, time.FixedZone("x", -5*3600))
	for k, v := range map[string]any{
		"str":       "hello",
		"bytes":     []byte{0, 0xff, 'a'},
		"textbytes": []byte("k1"),
		"int":       int64(-42),
		"int32":     int32(7),
		"uint":      uint64(math.MaxUint64),
		"float":     1.25,
		"float32":   float32(0.1),
		"nan":       math.NaN(),
		"inf":       math.Inf(-1),
		"bool":      true,
		"ts":        ts,
		"utc":       time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
		"obj": map[string]any{"a": int64(1), "b": 2.5, "c": []any{"x", nil, true}, "d": map[string]any{"e": 1e21},
			"u": uint64(math.MaxUint64), "n1": json.Number("1.0"), "n2": json.Number("1e2")},
		"arr":      []any{int64(1), 1.5, "s", map[string]any{"k": false}},
		"null":     nil,
		"__intern": "x",
		"num":      json.Number("12"),
	} {
		m0.MetaSetMut(k, v)
	}
	m1 := service.NewMessage([]byte(`{"n":1}`))
	m1.MetaSetMut("source", "tracer")
	m1.SetError(errors.New("boom: failed"))
	m2 := service.NewMessage(nil)
	return service.MessageBatch{m0, m1, m2}
}

// Every metadata type, binary content and an error read back so that
// their Snapshot is byte-identical, and the file is the AD-11 Snapshot:
// version 1.0, one batch, the dropped indices in skipped, a position
// naming only the pipeline file.
func TestCaptureFileRoundTrip(t *testing.T) {
	batch := captureBatch()
	path := filepath.Join(t.TempDir(), "c.json")
	if err := writeCapture(path, "/w/pipeline.yaml", batch, 5); err != nil {
		t.Fatal(err)
	}
	got, total, err := ReadCapture(path)
	if err != nil {
		t.Fatal(err)
	}
	if total != 5 || len(got) != 3 {
		t.Fatalf("read %d messages of %d, want 3 of 5", len(got), total)
	}
	want, _ := json.Marshal(snapshotBatches([]service.MessageBatch{batch}))
	have, _ := json.Marshal(snapshotBatches([]service.MessageBatch{got}))
	if !bytes.Equal(want, have) {
		t.Errorf("read-back Snapshot differs:\n got  %s\n want %s", have, want)
	}

	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var snap core.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		t.Fatal(err)
	}
	if snap.SchemaVersion != "1.0" || len(snap.Batches) != 1 ||
		snap.Position != (core.Position{File: "/w/pipeline.yaml"}) {
		t.Errorf("snapshot = %+v", snap)
	}
	wantSkipped := []core.Skipped{{Index: 3, Reason: "maxMessages"}, {Index: 4, Reason: "maxMessages"}}
	if len(snap.Skipped) != 2 || snap.Skipped[0] != wantSkipped[0] || snap.Skipped[1] != wantSkipped[1] {
		t.Errorf("skipped = %+v, want %+v", snap.Skipped, wantSkipped)
	}
}

// Read-back values have the plan's types: int64, uint64 beyond int64,
// float64, and Bloblang objects and arrays.
func TestCaptureFileTypes(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	if err := writeCapture(path, "/p.yaml", captureBatch()[:1], 1); err != nil {
		t.Fatal(err)
	}
	got, _, err := ReadCapture(path)
	if err != nil {
		t.Fatal(err)
	}
	m := got[0]
	for key, want := range map[string]any{"int": int64(-42), "int32": int64(7), "uint": uint64(math.MaxUint64), "float": 1.25, "num": int64(12)} {
		if v, _ := m.MetaGetMut(key); v != want {
			t.Errorf("%s = %#v, want %#v", key, v, want)
		}
	}
	obj, _ := m.MetaGetMut("obj")
	if o, ok := obj.(map[string]any); !ok || o["a"] != int64(1) || o["b"] != 2.5 {
		t.Errorf("obj = %#v", obj)
	}
	if v, ok := m.MetaGetMut("null"); !ok || v != nil {
		t.Errorf("null = %#v, %v", v, ok)
	}
}

// An existing file is replaced, with mode 0600, and no temp file stays.
func TestCaptureFileOverwrite(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "c.json")
	if err := os.WriteFile(path, []byte("old"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o644); err != nil {
		t.Fatal(err)
	}
	if err := writeCapture(path, "/p.yaml", captureBatch(), 3); err != nil {
		t.Fatal(err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o600 {
		t.Errorf("mode = %v, want 0600", fi.Mode().Perm())
	}
	if _, total, err := ReadCapture(path); err != nil || total != 3 {
		t.Errorf("ReadCapture = %d, %v", total, err)
	}
	entries, _ := os.ReadDir(dir)
	if len(entries) != 1 {
		t.Errorf("dir holds %d files, want only the capture", len(entries))
	}
}

// A failed write leaves no temp file and no capture.
func TestCaptureFileUnwritable(t *testing.T) {
	path := filepath.Join(t.TempDir(), "missing", "c.json")
	if err := writeCapture(path, "/p.yaml", captureBatch(), 3); err == nil {
		t.Fatal("writeCapture into a missing folder succeeded")
	}
}

// The reader refuses a newer major schemaVersion and a missing or
// unparseable one; an older or equal major, a newer minor included, is
// read.
func TestCaptureFileVersion(t *testing.T) {
	cases := map[string][]string{
		"2.0":  {"2.0", "(1.0)", "newer"},
		"":     {"no schemaVersion"},
		"one":  {`"one"`},
		"1":    {`"1"`},
		"1.x":  {`"1.x"`},
		"-1.0": {`"-1.0"`},
		"1.0":  nil,
		"1.7":  nil,
		"0.9":  nil,
	}
	for v, want := range cases {
		t.Run(v, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "c.json")
			doc := `{"schemaVersion":"` + v + `","position":{},"batches":[{"messages":[{"index":0,"content":{"raw":"aGk="},"metadata":[]}]}]}`
			if v == "" {
				doc = `{"position":{},"batches":[{"messages":[]}]}`
			}
			if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
				t.Fatal(err)
			}
			_, _, err := ReadCapture(path)
			if want == nil {
				if err != nil {
					t.Fatal(err)
				}
				return
			}
			if err == nil {
				t.Fatal("read succeeded")
			}
			for _, w := range want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not name %s", err, w)
				}
			}
		})
	}
}

// The Adapter saves the CapturedBatch Capture returns, and refuses another
// batch type.
func TestAdapterSaveCapture(t *testing.T) {
	a := &Adapter{}
	path := filepath.Join(t.TempDir(), "c.json")
	if err := a.SaveCapture(core.CapturedBatch{Batch: captureBatch(), Kept: 3, Total: 4}, "/p.yaml", path); err != nil {
		t.Fatal(err)
	}
	if b, total, err := ReadCapture(path); err != nil || len(b) != 3 || total != 4 {
		t.Errorf("ReadCapture = %d of %d, %v", len(b), total, err)
	}
	if err := a.SaveCapture(core.CapturedBatch{Batch: "x"}, "/p.yaml", path); err == nil {
		t.Error("SaveCapture of a string succeeded")
	}
}

// LoadCapture keeps the first maxMessages of the file's messages, and
// Total counts the file's skipped ones too (AD-11).
func TestLoadCaptureMaxMessages(t *testing.T) {
	path := filepath.Join(t.TempDir(), "c.json")
	if err := writeCapture(path, "/p.yaml", captureBatch(), 5); err != nil {
		t.Fatal(err)
	}
	a := &Adapter{}
	for _, tc := range []struct{ max, kept int }{{0, 3}, {2, 2}, {3, 3}, {10, 3}} {
		b, err := a.LoadCapture(path, tc.max)
		if err != nil {
			t.Fatal(err)
		}
		batch, ok := b.Batch.(service.MessageBatch)
		if !ok || len(batch) != tc.kept || b.Kept != tc.kept || b.Total != 5 {
			t.Errorf("maxMessages %d: %T of %d, Kept %d, Total %d; want %d of 5", tc.max, b.Batch, len(batch), b.Kept, b.Total, tc.kept)
		}
	}
	if _, err := a.LoadCapture(filepath.Join(t.TempDir(), "none.json"), 0); err == nil {
		t.Error("LoadCapture of a missing file succeeded")
	}
	dir := t.TempDir()
	for name, tc := range map[string]struct{ content, reason string }{
		"bad JSON": {"{not json", "invalid character"},
		"newer":    {`{"schemaVersion":"2.0","position":{},"batches":[{"messages":[]}]}`, "is newer than this rpcn-debug reads"},
	} {
		bad := filepath.Join(dir, name+".json")
		if err := os.WriteFile(bad, []byte(tc.content), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := a.LoadCapture(bad, 0); err == nil || !strings.Contains(err.Error(), tc.reason) {
			t.Errorf("%s: LoadCapture = %v, want an error containing %q", name, err, tc.reason)
		}
	}
}
