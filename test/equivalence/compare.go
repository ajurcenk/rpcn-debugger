package equivalence

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/test/dapdrive"
)

// jittered names metadata whose value the engine makes differently on every
// run, so only its key and type are compared: retry's backoff_duration sums
// backoff sleeps that the engine randomizes (the exponential backoff's
// default randomization factor of 0.5, which the retry config can't set).
var jittered = map[string]bool{"backoff_duration": true}

// meta is one metadata value in the AD-11 form: its type class and its
// lossless value.
type meta struct{ typ, value string }

// message is one message reduced to what AD-8 compares.
type message struct {
	raw  []byte
	err  string
	meta map[string]meta
}

// Compare checks the batches a debugger run delivered against the batches of
// the reference run (AD-8), structurally: the batch count, each batch's
// message count, and in each message its raw content bytes, its error text
// and its metadata (the same keys, each with the same AD-11 type and
// lossless value; see jittered). got must come from delivered messages,
// which carry the raw content and lossless metadata values. The error names
// the first difference and shows both batch lists, one batch per line, with
// the differing lines marked.
func Compare(ref []service.MessageBatch, got []dapdrive.Batch) error {
	want := make([][]message, len(ref))
	for i, b := range ref {
		for _, m := range b {
			raw, err := m.AsBytes()
			if err != nil {
				return fmt.Errorf("reference message content: %w", err)
			}
			msg := message{raw: raw, meta: map[string]meta{}}
			if err := m.GetError(); err != nil {
				msg.err = err.Error()
			}
			_ = m.MetaWalkMut(func(k string, v any) error {
				msg.meta[k] = refMeta(v)
				return nil
			})
			want[i] = append(want[i], msg)
		}
	}
	have := make([][]message, len(got))
	for i, b := range got {
		for _, m := range b {
			msg := message{raw: m.Raw, err: m.Error, meta: map[string]meta{}}
			for k, v := range m.Metadata {
				msg.meta[k] = meta{v.Type, v.Lossless}
			}
			have[i] = append(have[i], msg)
		}
	}
	first := firstDiff(want, have)
	if first == "" {
		return nil
	}
	return fmt.Errorf("delivered batches differ from the reference run: %s (- reference, + delivered):\n%s", first, diff(want, have))
}

// firstDiff describes the first difference between want and have, or ""
// when they are equal.
func firstDiff(want, have [][]message) string {
	if len(want) != len(have) {
		return fmt.Sprintf("%d batches, want %d", len(have), len(want))
	}
	for i := range want {
		if len(want[i]) != len(have[i]) {
			return fmt.Sprintf("batch %d holds %d messages, want %d", i, len(have[i]), len(want[i]))
		}
		for j := range want[i] {
			w, h := want[i][j], have[i][j]
			switch {
			case !bytes.Equal(w.raw, h.raw):
				return fmt.Sprintf("batch %d message %d: content %q, want %q", i, j, h.raw, w.raw)
			case w.err != h.err:
				return fmt.Sprintf("batch %d message %d: error %q, want %q", i, j, h.err, w.err)
			}
			if d := metaDiff(w.meta, h.meta); d != "" {
				return fmt.Sprintf("batch %d message %d: %s", i, j, d)
			}
		}
	}
	return ""
}

func metaDiff(want, have map[string]meta) string {
	for _, k := range sortedKeys(want) {
		h, ok := have[k]
		switch w := want[k]; {
		case !ok:
			return fmt.Sprintf("metadata %q missing", k)
		case w.typ != h.typ:
			return fmt.Sprintf("metadata %q type %s, want %s", k, h.typ, w.typ)
		case !jittered[k] && w.value != h.value:
			return fmt.Sprintf("metadata %q value %q, want %q", k, h.value, w.value)
		}
	}
	for _, k := range sortedKeys(have) {
		if _, ok := want[k]; !ok {
			return fmt.Sprintf("metadata %q unexpected", k)
		}
	}
	return ""
}

func sortedKeys(m map[string]meta) []string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// diff renders want and have batch by batch: equal batches with a "  "
// prefix, differing ones as a "-" line (want) and a "+" line (have).
func diff(want, have [][]message) string {
	var sb strings.Builder
	for i := range max(len(want), len(have)) {
		var w, h string
		if i < len(want) {
			w = renderBatch(want[i])
		}
		if i < len(have) {
			h = renderBatch(have[i])
		}
		switch {
		case i < len(want) && i < len(have) && firstDiff(want[i:i+1], have[i:i+1]) == "":
			fmt.Fprintf(&sb, "  batch %d: %s\n", i, w)
		default:
			if i < len(want) {
				fmt.Fprintf(&sb, "- batch %d: %s\n", i, w)
			}
			if i < len(have) {
				fmt.Fprintf(&sb, "+ batch %d: %s\n", i, h)
			}
		}
	}
	return sb.String()
}

// renderBatch renders a batch for the diff: each message's content and
// error quoted, and its metadata sorted.
func renderBatch(b []message) string {
	msgs := make([]string, len(b))
	for i, m := range b {
		parts := make([]string, 0, len(m.meta))
		for _, k := range sortedKeys(m.meta) {
			v := m.meta[k].value
			if jittered[k] {
				v = "*"
			}
			parts = append(parts, fmt.Sprintf("%s:%s=%q", k, m.meta[k].typ, v))
		}
		msgs[i] = fmt.Sprintf("%q error=%q meta={%s}", m.raw, m.err, strings.Join(parts, ","))
	}
	return "[" + strings.Join(msgs, ", ") + "]"
}

// refMeta maps a reference metadata value to its AD-11 type and lossless
// value, as engine.metaEntry does.
func refMeta(v any) meta {
	switch x := v.(type) {
	case nil:
		return meta{"null", "null"}
	case string:
		return meta{"string", x}
	case []byte:
		return meta{"bytes", base64.StdEncoding.EncodeToString(x)}
	case bool:
		return meta{"bool", strconv.FormatBool(x)}
	case int, int8, int16, int32, int64, uint, uint8, uint16, uint32, uint64:
		return meta{"int", fmt.Sprint(x)}
	case float32:
		return meta{"float", strconv.FormatFloat(float64(x), 'g', -1, 32)}
	case float64:
		return meta{"float", strconv.FormatFloat(x, 'g', -1, 64)}
	case json.Number:
		if _, err := x.Int64(); err == nil {
			return meta{"int", x.String()}
		}
		return meta{"float", x.String()}
	case time.Time:
		return meta{"timestamp", x.Format(time.RFC3339Nano)}
	case map[string]any:
		return meta{"object", jsonText(x)}
	case []any:
		return meta{"array", jsonText(x)}
	}
	return meta{"string", fmt.Sprint(v)}
}

// jsonText encodes an object or array as engine.jsonText does: NaN and ±Inf
// become their text, and a value JSON can't hold falls back to Go's text.
func jsonText(v any) string {
	raw, err := json.Marshal(sanitize(v))
	if err != nil {
		return fmt.Sprint(v)
	}
	return string(raw)
}

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

// CheckSameBatch enforces the corpus's same-batch rule on a case: its
// reference run's input yields exactly one batch of at most MaxMessages
// messages, and the debugger run (its Debug Console output) reports no
// truncation, so the batch the debugger captured is the reference's whole
// input.
func CheckSameBatch(ref Reference, output []string) error {
	var errs []error
	switch n := len(ref.InputBatches); {
	case n != 1:
		errs = append(errs, fmt.Errorf("the reference input yielded %d batches %v, want exactly 1 (same-batch rule)", n, ref.InputBatches))
	case ref.InputBatches[0] > MaxMessages:
		errs = append(errs, fmt.Errorf("the reference input batch holds %d messages, want at most %d (same-batch rule)", ref.InputBatches[0], MaxMessages))
	}
	for _, o := range output {
		if strings.HasPrefix(o, "captured ") && strings.Contains(o, "(maxMessages)") {
			errs = append(errs, fmt.Errorf("the debugger truncated the captured batch (same-batch rule): %q", strings.TrimSpace(o)))
		}
	}
	return errors.Join(errs...)
}
