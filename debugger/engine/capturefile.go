package engine

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// The capture file (AD-11) is a core.Snapshot as JSON: schemaVersion, one
// batch holding the kept messages, skipped listing the messages maxMessages
// dropped, and a position that is zero apart from file, the pipeline. Its
// metadata values are lossless (see core.Meta), so ReadCapture rebuilds
// messages whose Snapshot is the one written.

// SkipMaxMessages is the skipped reason of a message maxMessages dropped.
const SkipMaxMessages = "maxMessages"

// SaveCapture writes b, a CapturedBatch Capture returned, as a capture file
// at path.
func (a *Adapter) SaveCapture(b core.CapturedBatch, pipeline, path string) error {
	batch, ok := b.Batch.(service.MessageBatch)
	if !ok {
		return fmt.Errorf("save the capture: the batch is a %T, not the service.MessageBatch Capture returns", b.Batch)
	}
	return writeCapture(path, pipeline, batch, b.Total)
}

// captureSnapshot is the capture file's Snapshot of batch, the kept
// messages of a captured batch of total messages.
func captureSnapshot(pipeline string, batch service.MessageBatch, total int) core.Snapshot {
	snap := core.Snapshot{
		SchemaVersion: core.SchemaVersion,
		Position:      core.Position{File: pipeline},
		Batches:       snapshotBatches([]service.MessageBatch{batch}),
	}
	for i := len(batch); i < total; i++ {
		snap.Skipped = append(snap.Skipped, core.Skipped{Index: i, Reason: SkipMaxMessages})
	}
	return snap
}

// writeCapture writes batch, the kept messages of a captured batch of total
// messages, as a capture file at path, positioned at the pipeline file
// pipeline. It writes a temp file (mode 0600) in path's directory and
// renames it over path, so an existing file is replaced whole, also with
// mode 0600.
func writeCapture(path, pipeline string, batch service.MessageBatch, total int) (err error) {
	raw, err := json.MarshalIndent(captureSnapshot(pipeline, batch, total), "", "  ")
	if err != nil {
		return fmt.Errorf("encode the capture: %w", err)
	}
	raw = append(raw, '\n')
	f, err := os.CreateTemp(filepath.Dir(path), "."+filepath.Base(path)+".*.tmp")
	if err != nil {
		return err
	}
	tmp := f.Name()
	defer func() {
		if err != nil {
			_ = os.Remove(tmp)
		}
	}()
	// CreateTemp makes the file 0600; Chmod keeps it so whatever the umask.
	if err := f.Chmod(0o600); err != nil {
		_ = f.Close()
		return err
	}
	if _, err := f.Write(raw); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, path)
}

// ReadCapture reads the capture file at path: the kept messages, as
// messages whose Snapshot is the one written, and the captured batch's
// total count (kept plus skipped). A newer major schemaVersion, and a
// missing or unparseable one, is refused.
func ReadCapture(path string) (service.MessageBatch, int, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, 0, err
	}
	var snap core.Snapshot
	if err := json.Unmarshal(raw, &snap); err != nil {
		return nil, 0, fmt.Errorf("capture file %s: %w", path, err)
	}
	if err := core.CheckSchemaVersion(snap.SchemaVersion); err != nil {
		return nil, 0, fmt.Errorf("capture file %s: %w", path, err)
	}
	if len(snap.Batches) != 1 {
		return nil, 0, fmt.Errorf("capture file %s: it holds %d batches, not one", path, len(snap.Batches))
	}
	msgs := snap.Batches[0].Messages
	batch := make(service.MessageBatch, len(msgs))
	for i, m := range msgs {
		msg, err := captureMessage(m)
		if err != nil {
			return nil, 0, fmt.Errorf("capture file %s: message %d: %w", path, i, err)
		}
		batch[i] = msg
	}
	return batch, len(msgs) + len(snap.Skipped), nil
}

// captureMessage rebuilds a message from its Snapshot.
func captureMessage(m core.Message) (*service.Message, error) {
	raw := m.Content.Raw
	if raw == nil {
		raw = []byte{}
	}
	msg := service.NewMessage(raw)
	for _, e := range m.Metadata {
		v, err := metaValue(e)
		if err != nil {
			return nil, fmt.Errorf("metadata %q: %w", e.Key, err)
		}
		msg.MetaSetMut(e.Key, v)
	}
	if m.Error != "" {
		msg.SetError(errors.New(m.Error))
	}
	return msg, nil
}

// metaValue returns the typed value of e, chosen so that metaEntry maps it
// back to e: an int as int64 (uint64 beyond its range), a float as
// float64, a timestamp as time.Time, bytes decoded, and an object or array
// as a Bloblang value (integral numbers int64, others float64).
func metaValue(e core.Meta) (any, error) {
	switch e.Type {
	case core.MetaString:
		return e.Value, nil
	case core.MetaBytes:
		return base64.StdEncoding.DecodeString(e.Value)
	case core.MetaInt:
		if n, err := strconv.ParseInt(e.Value, 10, 64); err == nil {
			return n, nil
		}
		return strconv.ParseUint(e.Value, 10, 64)
	case core.MetaFloat:
		return strconv.ParseFloat(e.Value, 64)
	case core.MetaBool:
		return strconv.ParseBool(e.Value)
	case core.MetaTimestamp:
		return time.Parse(time.RFC3339Nano, e.Value)
	case core.MetaObject, core.MetaArray:
		dec := json.NewDecoder(bytes.NewReader([]byte(e.Value)))
		dec.UseNumber()
		var v any
		if err := dec.Decode(&v); err != nil {
			return nil, err
		}
		if dec.More() {
			return nil, errors.New("trailing data after the JSON value")
		}
		return bloblValue(v), nil
	case core.MetaNull:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown metadata type %q", e.Type)
	}
}

// bloblValue converts each json.Number of a decoded JSON value to the
// first of int64, uint64 and a float64 whose JSON is the number's text,
// and keeps it as a json.Number otherwise, so metaEntry re-encodes the
// value as written.
func bloblValue(v any) any {
	switch t := v.(type) {
	case json.Number:
		if n, err := t.Int64(); err == nil {
			return n
		}
		if n, err := strconv.ParseUint(t.String(), 10, 64); err == nil {
			return n
		}
		if f, err := t.Float64(); err == nil {
			if raw, err := json.Marshal(f); err == nil && string(raw) == t.String() {
				return f
			}
		}
		return t
	case map[string]any:
		for k, x := range t {
			t[k] = bloblValue(x)
		}
		return t
	case []any:
		for i, x := range t {
			t[i] = bloblValue(x)
		}
		return t
	}
	return v
}
