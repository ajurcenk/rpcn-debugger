package debugger

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"regexp"
	"slices"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

// The single log handler (AD-15). Every record from the debugger and the
// engine goes through it: it translates the record (AD-6), formats it once
// as text, writes it to the stderr side at Info and above, and, for the
// engine's records, sends Warn and above to the frontend as an Output
// event (category stderr for errors, console for warnings). It never writes
// to Serve's out.

// frontendLevel is the lowest level the frontend sees. Connect logs several
// Info lines per stream start, which stay on stderr only.
const frontendLevel = slog.LevelWarn

// logSink is what every handler derived from one Serve shares: the stderr
// side and the Session, set once the DAP server builds it.
type logSink struct {
	mu   sync.Mutex // guards writes to w
	w    io.Writer
	sess atomic.Pointer[core.Session]
}

// attach makes sess the frontend side and its launch the translator.
func (s *logSink) attach(sess *core.Session) { s.sess.Store(sess) }

// newLogger returns a logger on sink. With frontend set, its Warn+ records
// also reach the frontend.
func newLogger(sink *logSink, frontend bool) *slog.Logger {
	return slog.New(&logHandler{sink: sink, frontend: frontend})
}

type logHandler struct {
	sink     *logSink
	frontend bool
	attrs    []slog.Attr // from WithAttrs, keys group-qualified
	group    string      // "a.b." for WithGroup("a").WithGroup("b")
}

func (h *logHandler) Enabled(_ context.Context, l slog.Level) bool { return l >= slog.LevelInfo }

func (h *logHandler) WithAttrs(as []slog.Attr) slog.Handler {
	c := *h
	c.attrs = slices.Clone(h.attrs)
	for _, a := range as {
		c.attrs = append(c.attrs, h.qualify(a))
	}
	return &c
}

func (h *logHandler) WithGroup(name string) slog.Handler {
	if name == "" {
		return h
	}
	c := *h
	c.group += name + "."
	return &c
}

func (h *logHandler) qualify(a slog.Attr) slog.Attr {
	a.Value = a.Value.Resolve()
	if h.group != "" {
		a.Key = h.group + a.Key
	}
	return a
}

func (h *logHandler) Handle(_ context.Context, r slog.Record) error {
	attrs := slices.Clone(h.attrs)
	r.Attrs(func(a slog.Attr) bool {
		attrs = append(attrs, h.qualify(a))
		return true
	})

	var tr core.Translate
	sess := h.sink.sess.Load()
	if sess != nil {
		tr = sess.Translator()
	}
	msg, attrs := translateRecord(tr, r.Message, attrs)
	body := formatRecord(r.Level, msg, attrs)

	ts := r.Time
	if ts.IsZero() {
		ts = time.Now()
	}
	h.sink.mu.Lock()
	_, err := io.WriteString(h.sink.w, "time="+ts.Format("2006-01-02T15:04:05.000Z07:00")+" "+body)
	h.sink.mu.Unlock()

	if h.frontend && sess != nil && r.Level >= frontendLevel && !strictRejection(r.Level, r.Message) {
		sess.Log(r.Level, body)
	}
	return err
}

// strictRejection reports benthos's strict-mode record for the output
// rejecting errored messages ("Rejecting N of M message(s) for output
// 'rpcn_debug_sink' ..."). It stays on stderr: it names the internal sink,
// and its (line N) belongs to the failing processor, which the record
// doesn't name. The Session's strict note (STRICT_REJECTED) is the one
// Debug Console line for it.
func strictRejection(level slog.Level, msg string) bool {
	return level == slog.LevelWarn && strings.HasPrefix(msg, "Rejecting ") &&
		strings.Contains(msg, "for output '"+rewrite.SinkName+"'")
}

// lineRef finds a Bloblang error's own line reference, as in
// "failed assignment (line 2): ...": a (line N) followed by a colon. Only
// the first one is Bloblang's; later ones are the user's text.
var lineRef = regexp.MustCompile(`\(line (\d+)\):`)

// translateRecord rewrites a record for the user (AD-6). The engine adds a
// path attribute per component it nests, so the last one is the record's;
// a processor_resources entry is told apart only by the label that follows
// its path, which qualifies the path (rewrite.QualifyResource). The path
// becomes the user's path, a Bloblang line in msg the file line, and every
// engine path in msg its user path; what doesn't map stays as it is.
// Repeated keys keep their last value, and empty labels are dropped.
func translateRecord(tr core.Translate, msg string, attrs []slog.Attr) (string, []slog.Attr) {
	path, label, afterResource := "", "", false
	for _, a := range attrs {
		switch a.Key {
		case "path":
			path = a.Value.String()
			afterResource = path == rewrite.ResourcePath
		case "label":
			if afterResource {
				label = a.Value.String()
			}
			afterResource = false
		}
	}
	userPath := path
	if tr != nil && path != "" {
		eng := rewrite.QualifyResource(path, label)
		if up, ok := tr.Translate(eng); ok {
			userPath = up
		}
		if m := lineRef.FindStringSubmatchIndex(msg); m != nil {
			if n, err := strconv.Atoi(msg[m[2]:m[3]]); err == nil {
				if l, ok := tr.TranslateLine(eng, n); ok {
					msg = msg[:m[2]] + strconv.Itoa(l) + msg[m[3]:]
				}
			}
		}
	}
	msg = core.TranslateText(tr, msg)

	out := make([]slog.Attr, 0, len(attrs))
	at := map[string]int{}
	for _, a := range attrs {
		switch {
		case a.Key == "path":
			a = slog.String("path", userPath)
		case a.Key == "label" && a.Value.Kind() == slog.KindString && a.Value.String() == "":
			continue
		}
		if i, ok := at[a.Key]; ok {
			out[i] = a
			continue
		}
		at[a.Key] = len(out)
		out = append(out, a)
	}
	return msg, out
}

// formatRecord formats a record once, as slog text without the time:
// level=ERROR msg="..." path=pipeline.processors[1]. It ends in a newline.
func formatRecord(level slog.Level, msg string, attrs []slog.Attr) string {
	var buf bytes.Buffer
	th := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})
	r := slog.NewRecord(time.Time{}, level, msg, 0)
	r.AddAttrs(attrs...)
	_ = th.Handle(context.Background(), r)
	return buf.String()
}
