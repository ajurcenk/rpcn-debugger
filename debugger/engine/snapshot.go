package engine

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// snapshotBatches deep-copies batches into Snapshot batches (AD-11, AD-19):
// the content, every metadata value (mapped to the closed type set) and the
// error of each message. The result never references engine objects.
func snapshotBatches(batches []service.MessageBatch) []core.Batch {
	out := make([]core.Batch, len(batches))
	for i, b := range batches {
		msgs := make([]core.Message, len(b))
		for j, m := range b {
			msgs[j] = snapshotMessage(j, m)
		}
		out[i] = core.Batch{Messages: msgs}
	}
	return out
}

func snapshotMessage(index int, m *service.Message) core.Message {
	msg := core.Message{Index: index, Metadata: []core.Meta{}}
	if raw, err := m.AsBytes(); err == nil {
		msg.Content = contentViews(append([]byte{}, raw...))
	} else {
		msg.Content = core.Content{Raw: []byte{}}
	}
	_ = m.MetaWalkMut(func(key string, value any) error {
		msg.Metadata = append(msg.Metadata, metaEntry(key, value))
		return nil
	})
	// MetaWalkMut walks a map: sort for a deterministic Snapshot.
	sort.Slice(msg.Metadata, func(a, b int) bool { return msg.Metadata[a].Key < msg.Metadata[b].Key })
	if err := m.GetError(); err != nil {
		msg.Error = err.Error()
	}
	return msg
}

// contentViews sets the text view when raw is valid UTF-8, and the JSON view
// when it is also a valid JSON document.
func contentViews(raw []byte) core.Content {
	c := core.Content{Raw: raw}
	if utf8.Valid(raw) {
		text := string(raw)
		c.Text = &text
		if json.Valid(raw) {
			c.JSON = json.RawMessage(append([]byte{}, raw...))
		}
	}
	return c
}

// metaEntry maps one metadata value to the closed type set, with a lossless
// value (see core.Meta).
func metaEntry(key string, value any) core.Meta {
	e := core.Meta{Key: key, Internal: strings.HasPrefix(key, "__")}
	switch v := value.(type) {
	case nil:
		e.Type, e.Value = core.MetaNull, "null"
	case string:
		e.Type, e.Value = core.MetaString, v
	case []byte:
		e.Type, e.Value = core.MetaBytes, base64.StdEncoding.EncodeToString(v)
		if utf8.Valid(v) {
			text := string(v)
			e.Text = &text
		}
	case bool:
		e.Type, e.Value = core.MetaBool, strconv.FormatBool(v)
	case int:
		e.Type, e.Value = core.MetaInt, strconv.FormatInt(int64(v), 10)
	case int8:
		e.Type, e.Value = core.MetaInt, strconv.FormatInt(int64(v), 10)
	case int16:
		e.Type, e.Value = core.MetaInt, strconv.FormatInt(int64(v), 10)
	case int32:
		e.Type, e.Value = core.MetaInt, strconv.FormatInt(int64(v), 10)
	case int64:
		e.Type, e.Value = core.MetaInt, strconv.FormatInt(v, 10)
	case uint:
		e.Type, e.Value = core.MetaInt, strconv.FormatUint(uint64(v), 10)
	case uint8:
		e.Type, e.Value = core.MetaInt, strconv.FormatUint(uint64(v), 10)
	case uint16:
		e.Type, e.Value = core.MetaInt, strconv.FormatUint(uint64(v), 10)
	case uint32:
		e.Type, e.Value = core.MetaInt, strconv.FormatUint(uint64(v), 10)
	case uint64:
		e.Type, e.Value = core.MetaInt, strconv.FormatUint(v, 10)
	case float32:
		e.Type, e.Value = core.MetaFloat, strconv.FormatFloat(float64(v), 'g', -1, 32)
	case float64:
		e.Type, e.Value = core.MetaFloat, strconv.FormatFloat(v, 'g', -1, 64)
	case json.Number:
		if _, err := v.Int64(); err == nil {
			e.Type, e.Value = core.MetaInt, v.String()
		} else {
			e.Type, e.Value = core.MetaFloat, v.String()
		}
	case time.Time:
		e.Type, e.Value = core.MetaTimestamp, v.Format(time.RFC3339Nano)
	case map[string]any:
		e.Type, e.Value = core.MetaObject, jsonText(v)
	case []any:
		e.Type, e.Value = core.MetaArray, jsonText(v)
	default:
		// Not a Bloblang value type: show it as text.
		e.Type, e.Value = core.MetaString, fmt.Sprint(v)
	}
	return e
}

// jsonText encodes an object or array metadata value as JSON. Values JSON
// can't hold (NaN, ±Inf) fall back to Go's text form.
func jsonText(v any) string {
	raw, err := json.Marshal(sanitize(v))
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(raw)
}

// sanitize replaces NaN and infinite floats, which JSON can't encode, with
// their text.
func sanitize(v any) any {
	switch t := v.(type) {
	case float64:
		if math.IsNaN(t) || math.IsInf(t, 0) {
			return strconv.FormatFloat(t, 'g', -1, 64)
		}
	case map[string]any:
		out := make(map[string]any, len(t))
		for k, x := range t {
			out[k] = sanitize(x)
		}
		return out
	case []any:
		out := make([]any, len(t))
		for i, x := range t {
			out[i] = sanitize(x)
		}
		return out
	}
	return v
}
