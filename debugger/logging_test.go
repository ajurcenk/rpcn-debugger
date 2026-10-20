package debugger

import (
	"context"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

// outputs records the Session's Output events.
type outputs struct {
	mu  sync.Mutex
	got []core.Output
}

func (o *outputs) Emit(ev core.Event) {
	if out, ok := ev.(core.Output); ok {
		o.mu.Lock()
		o.got = append(o.got, out)
		o.mu.Unlock()
	}
}

func (o *outputs) all() []core.Output {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]core.Output(nil), o.got...)
}

// checkEngine is an Engine whose Check hands out a rewrite's Translate.
type checkEngine struct{ tr core.Translate }

func (e checkEngine) Check(core.StartOptions) (core.CheckResult, error) {
	return core.CheckResult{Translator: e.tr}, nil
}

func (checkEngine) Start(context.Context, core.StartOptions, core.Arriver) error { return nil }

const logPipeline = `input:
  generate: {mapping: 'root = {}'}
pipeline:
  processors:
    - mapping: 'root = this'
    - mapping: |
        root.a = 1
        root.b = throw("x")
processor_resources:
  - label: res
    mapping: root = throw("r")
`

// newTestLogging returns an engine logger and a debugger logger on one
// sink writing to buf, attached to a launched Session whose frontend is fe.
func newTestLogging(t *testing.T) (engineLog, dapLog *slog.Logger, buf *syncBuffer, fe *outputs) {
	t.Helper()
	plan, err := rewrite.Rewrite([]byte(logPipeline))
	if err != nil {
		t.Fatal(err)
	}
	fe = &outputs{}
	sess := core.NewSession(checkEngine{plan}, fe)
	if _, err := sess.Do(context.Background(), core.Launch{Pipeline: "p.yaml", Cwd: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	buf = &syncBuffer{}
	sink := &logSink{w: buf}
	sink.attach(sess)
	return newLogger(sink, true), newLogger(sink, false), buf, fe
}

func TestLogLevels(t *testing.T) {
	log, dapLog, buf, fe := newTestLogging(t)
	log.Debug("debug line")
	log.Info("info line")
	log.Warn("warn line")
	log.Error("error line")
	dapLog.Error("dap line")

	got := buf.String()
	if strings.Contains(got, "debug line") {
		t.Errorf("Debug reached stderr:\n%s", got)
	}
	for _, w := range []string{"info line", "warn line", "error line", "dap line"} {
		if !strings.Contains(got, w) {
			t.Errorf("stderr lacks %q:\n%s", w, got)
		}
	}
	for _, line := range strings.Split(strings.TrimSpace(got), "\n") {
		if !strings.HasPrefix(line, "time=") {
			t.Errorf("stderr line without a time: %q", line)
		}
	}
	out := fe.all()
	if len(out) != 2 {
		t.Fatalf("outputs = %+v, want the warning and the error only", out)
	}
	if out[0].Category != "console" || out[0].Text != "level=WARN msg=\"warn line\"\n" {
		t.Errorf("warning output = %+v", out[0])
	}
	if out[1].Category != "stderr" || out[1].Text != "level=ERROR msg=\"error line\"\n" {
		t.Errorf("error output = %+v", out[1])
	}
	// The DAP side gets exactly the stderr text, after the time.
	if !strings.Contains(got, " "+out[1].Text) {
		t.Errorf("stderr lacks the output text %q:\n%s", out[1].Text, got)
	}
}

// Engine records carry one path per nested component, and a resource's
// label after its path: the handler keeps the last path, translated, and
// shifts the Bloblang line.
func TestLogTranslation(t *testing.T) {
	log, _, buf, fe := newTestLogging(t)
	stepper := "root.pipeline.processors.0.rpcn_debug_stepper"
	log.With("path", "root.pipeline", "path", "root.pipeline.processors.0", "label", "",
		"path", stepper, "path", stepper+".processors", "path", stepper+".processors.1", "label", "").
		Error("failed assignment (line 2): x")
	log.With("path", rewrite.ResourcePath, "label", "res").Error("failed assignment (line 1): r")
	log.With("path", "root.input").Warn("about " + stepper + ".processors.0 and (line 3)")
	log.WithGroup("g").With("path", "nested").Warn("grouped")
	log.With("path", stepper+".processors.1").Error(`failed assignment (line 2): expected (line 2): x`)

	out := fe.all()
	if len(out) != 5 {
		t.Fatalf("outputs = %+v", out)
	}
	for i, want := range []string{
		"level=ERROR msg=\"failed assignment (line 8): x\" path=pipeline.processors[1]\n",
		"level=ERROR msg=\"failed assignment (line 11): r\" path=processor_resources.res label=res\n",
		"level=WARN msg=\"about pipeline.processors[0] and (line 3)\" path=root.input\n",
		"level=WARN msg=grouped g.path=nested\n",
		"level=ERROR msg=\"failed assignment (line 8): expected (line 2): x\" path=pipeline.processors[1]\n",
	} {
		if out[i].Text != want {
			t.Errorf("output %d = %q, want %q", i, out[i].Text, want)
		}
	}
	if got := buf.String(); strings.Contains(got, "rpcn_debug_stepper") {
		t.Errorf("stderr has an engine path:\n%s", got)
	}
}

// Before a session is attached, records reach stderr untranslated.
func TestLogWithoutSession(t *testing.T) {
	buf := &syncBuffer{}
	log := newLogger(&logSink{w: buf}, true)
	log.With("path", "root.pipeline.processors.0").Error("boom")
	if got := buf.String(); !strings.Contains(got, "path=root.pipeline.processors.0") {
		t.Errorf("stderr = %q", got)
	}
}

// Once the session has ended, nothing more reaches the frontend.
func TestLogAfterTerminate(t *testing.T) {
	plan, err := rewrite.Rewrite([]byte(logPipeline))
	if err != nil {
		t.Fatal(err)
	}
	fe := &outputs{}
	sess := core.NewSession(checkEngine{plan}, fe)
	sink := &logSink{w: &syncBuffer{}}
	sink.attach(sess)
	_, _ = sess.Do(context.Background(), core.Terminate{})
	newLogger(sink, true).Error("late")
	if out := fe.all(); len(out) != 0 {
		t.Errorf("outputs after terminate = %+v", out)
	}
}
