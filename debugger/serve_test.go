package debugger

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// msg is one decoded DAP message.
type msg struct {
	Type       string          `json:"type"`
	Command    string          `json:"command"`
	Event      string          `json:"event"`
	RequestSeq int             `json:"request_seq"`
	Success    bool            `json:"success"`
	Message    string          `json:"message"`
	Body       json.RawMessage `json:"body"`
}

type wire struct {
	t    *testing.T
	w    io.WriteCloser
	msgs chan msg
	seq  int
	done chan error
}

func serve(t *testing.T, opts ...Option) *wire {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &wire{t: t, w: inW, msgs: make(chan msg, 64), done: make(chan error, 1)}
	go func() {
		err := Serve(service.NewEnvironment(), inR, outW, opts...)
		_ = outW.Close()
		c.done <- err
	}()
	go func() {
		defer close(c.msgs)
		r := bufio.NewReader(outR)
		for {
			header, err := r.ReadString('\n')
			if err != nil {
				return
			}
			n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "Content-Length:")))
			if err != nil {
				t.Errorf("bad header %q", header)
				return
			}
			if _, err := r.ReadString('\n'); err != nil {
				return
			}
			body := make([]byte, n)
			if _, err := io.ReadFull(r, body); err != nil {
				return
			}
			var m msg
			if err := json.Unmarshal(body, &m); err != nil {
				t.Errorf("bad body %s: %v", body, err)
				return
			}
			c.msgs <- m
		}
	}()
	t.Cleanup(func() { _ = inW.Close() })
	return c
}

func (c *wire) send(command string, args any) {
	c.t.Helper()
	c.seq++
	raw, _ := json.Marshal(args)
	body := fmt.Sprintf(`{"seq":%d,"type":"request","command":%q,"arguments":%s}`, c.seq, command, raw)
	if _, err := fmt.Fprintf(c.w, "Content-Length: %d\r\n\r\n%s", len(body), body); err != nil {
		c.t.Fatal(err)
	}
}

func (c *wire) next(kind, name string) msg {
	c.t.Helper()
	select {
	case m, ok := <-c.msgs:
		if !ok {
			c.t.Fatalf("stream closed, want %s %s", kind, name)
		}
		if m.Type != kind || (kind == "response" && m.Command != name) || (kind == "event" && m.Event != name) {
			c.t.Fatalf("got %+v, want %s %s", m, kind, name)
		}
		return m
	case <-time.After(30 * time.Second):
		c.t.Fatalf("timed out waiting for %s %s", kind, name)
	}
	return msg{}
}

// The whole path: rewrite, capture, stepper, gate, sink and DAP, on the
// tracer sample.
func TestServeTracerSample(t *testing.T) {
	pipeline, err := filepath.Abs(filepath.Join("..", "testdata", "tracer", "pipeline.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	before, err := os.ReadFile(pipeline)
	if err != nil {
		t.Fatal(err)
	}

	for _, resume := range []string{"next", "continue", "disconnect"} {
		t.Run(resume, func(t *testing.T) {
			c := serve(t)
			c.send("initialize", map[string]any{"adapterID": "redpanda-connect"})
			c.next("response", "initialize")
			c.send("launch", map[string]any{"pipeline": "pipeline.yaml", "cwd": filepath.Dir(pipeline)})
			if m := c.next("response", "launch"); !m.Success {
				t.Fatalf("launch failed: %s", m.Message)
			}
			c.next("event", "initialized")
			c.send("configurationDone", map[string]any{})
			c.next("response", "configurationDone")
			c.next("event", "thread")
			c.next("event", "stopped")

			c.send("stackTrace", map[string]any{"threadId": 1})
			var st struct {
				StackFrames []struct {
					ID     int `json:"id"`
					Line   int `json:"line"`
					Source struct {
						Path string `json:"path"`
					} `json:"source"`
				} `json:"stackFrames"`
			}
			_ = json.Unmarshal(c.next("response", "stackTrace").Body, &st)
			if len(st.StackFrames) != 1 || st.StackFrames[0].Line != 16 || st.StackFrames[0].Source.Path != pipeline {
				t.Fatalf("stack = %+v, want one frame at %s:16", st, pipeline)
			}
			checkVariables(t, c, st.StackFrames[0].ID)

			start := time.Now()
			if resume != "disconnect" {
				c.send(resume, map[string]any{"threadId": 1})
				c.next("response", resume)
				if resume == "next" {
					// The end stop, at the processors: key line.
					c.next("event", "stopped")
					if frames := c.stack(); len(frames) != 1 || frames[0].Line != 15 || frames[0].Name != "pipeline.processors" {
						t.Fatalf("end stop = %+v, want pipeline.processors at line 15", frames)
					}
					c.send(resume, map[string]any{"threadId": 1})
					c.next("response", resume)
				}
				c.delivered(`{"doubled":2,"greeting":"hello","n":1}`)
				c.next("event", "thread")
				c.next("event", "terminated")
			}
			c.send("disconnect", map[string]any{})
			c.next("response", "disconnect")
			// A stalled stream stop would take the whole stop timeout.
			if d := time.Since(start); d > 2*time.Second {
				t.Errorf("%s: ending the session took %v", resume, d)
			}
			select {
			case err := <-c.done:
				if err != nil {
					t.Errorf("Serve = %v", err)
				}
			case <-time.After(30 * time.Second):
				t.Fatal("Serve did not return")
			}
		})
	}

	after, err := os.ReadFile(pipeline)
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Error("the pipeline file was modified")
	}
}

func TestServeBadYAML(t *testing.T) {
	path := filepath.Join(t.TempDir(), "bad.yaml")
	if err := os.WriteFile(path, []byte("input:\n  generate:\n    count: 1\n  mapping: : nope\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := serve(t)
	c.send("initialize", map[string]any{})
	c.next("response", "initialize")
	c.send("launch", map[string]any{"pipeline": path})
	m := c.next("response", "launch")
	if m.Success || !strings.Contains(m.Message, path) || !strings.Contains(m.Message, "line 4") || errorID(m) != core.Number(core.CodePipelineInvalid) {
		t.Errorf("launch = %+v, want PIPELINE_INVALID naming %s and line 4", m, path)
	}
	c.send("launch", map[string]any{"pipeline": filepath.Join(t.TempDir(), "missing.yaml")})
	if m := c.next("response", "launch"); m.Success || !strings.Contains(m.Message, "missing.yaml") || errorID(m) != core.Number(core.CodeLaunchInvalid) {
		t.Errorf("launch = %+v, want LAUNCH_INVALID naming missing.yaml", m)
	}
	c.send("disconnect", map[string]any{})
	c.next("response", "disconnect")
}

func errorID(m msg) int {
	var body struct {
		Error struct {
			ID int `json:"id"`
		} `json:"error"`
	}
	_ = json.Unmarshal(m.Body, &body)
	return body.Error.ID
}

type variable struct {
	Name               string `json:"name"`
	Value              string `json:"value"`
	VariablesReference int    `json:"variablesReference"`
}

func (c *wire) variables(ref int) map[string]variable {
	c.t.Helper()
	c.send("variables", map[string]any{"variablesReference": ref})
	var body struct {
		Variables []variable `json:"variables"`
	}
	if err := json.Unmarshal(c.next("response", "variables").Body, &body); err != nil {
		c.t.Fatal(err)
	}
	out := map[string]variable{}
	for _, v := range body.Variables {
		out[v.Name] = v
	}
	return out
}

// checkVariables walks Batches › batch 0 › message 0 of the tracer sample:
// its content, its metadata (bytes as text) and the internal node.
func checkVariables(t *testing.T, c *wire, frameID int) {
	t.Helper()
	c.send("scopes", map[string]any{"frameId": frameID})
	var scopes struct {
		Scopes []struct {
			Name               string `json:"name"`
			VariablesReference int    `json:"variablesReference"`
		} `json:"scopes"`
	}
	_ = json.Unmarshal(c.next("response", "scopes").Body, &scopes)
	if len(scopes.Scopes) != 1 || scopes.Scopes[0].Name != "Batches" {
		t.Fatalf("scopes = %+v, want Batches", scopes)
	}
	batch, ok := c.variables(scopes.Scopes[0].VariablesReference)["batch 0"]
	if !ok {
		t.Fatal("no batch 0")
	}
	m, ok := c.variables(batch.VariablesReference)["message 0"]
	if !ok {
		t.Fatal("no message 0")
	}
	fields := c.variables(m.VariablesReference)
	if !strings.Contains(fields["content"].Value, `"greeting":"hello"`) {
		t.Errorf("content = %+v", fields["content"])
	}
	meta := c.variables(fields["metadata"].VariablesReference)
	if meta["source"].Value != "tracer" || meta["kafka_key"].Value != "k1" {
		t.Errorf("metadata = %+v, want source tracer and kafka_key k1", meta)
	}
	if _, ok := meta["__rpcn_internal"]; ok {
		t.Error("the internal key is not under the internal node")
	}
	internal := c.variables(meta["internal"].VariablesReference)
	if internal["__rpcn_internal"].Value != "x" {
		t.Errorf("internal = %+v", internal)
	}
}

// launchAndStop launches pipeline, starts it and returns the first frame
// it stops at, failing on any event before the stop other than the
// expected outputs.
func launchAndStop(t *testing.T, c *wire, pipeline string, outputs int) (frames []stackFrame, out []string) {
	t.Helper()
	c.send("initialize", map[string]any{"adapterID": "redpanda-connect"})
	c.next("response", "initialize")
	c.send("launch", map[string]any{"pipeline": pipeline})
	for range outputs {
		var body struct {
			Category string `json:"category"`
			Output   string `json:"output"`
		}
		_ = json.Unmarshal(c.next("event", "output").Body, &body)
		if body.Category != "console" {
			t.Errorf("output category = %q, want console", body.Category)
		}
		out = append(out, body.Output)
	}
	if m := c.next("response", "launch"); !m.Success {
		t.Fatalf("launch failed: %s", m.Message)
	}
	c.next("event", "initialized")
	c.send("configurationDone", map[string]any{})
	c.next("response", "configurationDone")
	c.next("event", "thread")
	c.next("event", "stopped")
	return c.stack(), out
}

// stack returns the stopped thread's stack trace.
func (c *wire) stack() []stackFrame {
	c.t.Helper()
	c.send("stackTrace", map[string]any{"threadId": 1})
	var st struct {
		StackFrames []stackFrame `json:"stackFrames"`
	}
	if err := json.Unmarshal(c.next("response", "stackTrace").Body, &st); err != nil {
		c.t.Fatal(err)
	}
	return st.StackFrames
}

// stepTo sends command and checks the stop it leads to: the innermost frame
// at line, with depth frames.
func (c *wire) stepTo(command string, line, depth int) []stackFrame {
	c.t.Helper()
	c.send(command, map[string]any{"threadId": 1})
	c.next("response", command)
	var stopped struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(c.next("event", "stopped").Body, &stopped)
	if stopped.Reason != "step" {
		c.t.Errorf("%s: stop reason = %q, want step", command, stopped.Reason)
	}
	frames := c.stack()
	if len(frames) != depth || frames[0].Line != line {
		c.t.Fatalf("%s: stack = %+v, want %d frames with the innermost at line %d", command, frames, depth, line)
	}
	return frames
}

// content returns message 0's content in frame frameID.
func (c *wire) content(frameID int) string {
	c.t.Helper()
	c.send("scopes", map[string]any{"frameId": frameID})
	var scopes struct {
		Scopes []struct {
			VariablesReference int `json:"variablesReference"`
		} `json:"scopes"`
	}
	_ = json.Unmarshal(c.next("response", "scopes").Body, &scopes)
	if len(scopes.Scopes) != 1 {
		c.t.Fatalf("scopes of frame %d = %+v", frameID, scopes)
	}
	batch := c.variables(scopes.Scopes[0].VariablesReference)["batch 0"]
	m := c.variables(batch.VariablesReference)["message 0"]
	return c.variables(m.VariablesReference)["content"].Value
}

type stackFrame struct {
	ID     int    `json:"id"`
	Name   string `json:"name"`
	Line   int    `json:"line"`
	Source struct {
		Path string `json:"path"`
	} `json:"source"`
}

func continueToEnd(t *testing.T, c *wire) {
	t.Helper()
	c.send("continue", map[string]any{"threadId": 1})
	c.next("response", "continue")
	c.delivered("")
	c.next("event", "thread")
	c.next("event", "terminated")
	c.send("disconnect", map[string]any{})
	c.next("response", "disconnect")
}

// delivered reads the one delivered batch's Debug Console text and its
// rpcnDelivered event; with content set, the batch's one message must
// have it.
func (c *wire) delivered(content string) {
	c.t.Helper()
	var out struct {
		Output string `json:"output"`
	}
	_ = json.Unmarshal(c.next("event", "output").Body, &out)
	if !strings.HasPrefix(out.Output, "Delivered batch 1 (1 messages) to dry-run output\n") {
		c.t.Errorf("output = %q, want delivered batch 1", out.Output)
	}
	var ev struct {
		Batch    int           `json:"batch"`
		Snapshot core.Snapshot `json:"snapshot"`
	}
	_ = json.Unmarshal(c.next("event", "rpcnDelivered").Body, &ev)
	if ev.Batch != 1 || len(ev.Snapshot.Batches) != 1 || len(ev.Snapshot.Batches[0].Messages) != 1 {
		c.t.Fatalf("rpcnDelivered = %+v, want batch 1 of one message", ev)
	}
	if got := string(ev.Snapshot.Batches[0].Messages[0].Content.Raw); content != "" && got != content {
		c.t.Errorf("delivered content = %s, want %s", got, content)
	}
}

// F5 on the nested demo stops before the first input.processors child,
// with no warning, and Continue delivers.
func TestServeNested(t *testing.T) {
	pipeline, err := filepath.Abs(filepath.Join("..", "testdata", "tracer", "nested.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	c := serve(t)
	frames, _ := launchAndStop(t, c, pipeline, 0)
	if len(frames) != 1 || frames[0].Line != 12 || frames[0].Source.Path != pipeline {
		t.Fatalf("stack = %+v, want one frame at %s:12", frames, pipeline)
	}
	continueToEnd(t, c)
}

// Stepping through the nested demo with Next, Step In and Step Out: each
// processor's line in order, 2 frames inside the branch and the switch
// case, each frame with its own batch, and back to the pipeline level.
func TestServeNestedStepping(t *testing.T) {
	pipeline, err := filepath.Abs(filepath.Join("..", "testdata", "tracer", "nested.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	c := serve(t)
	frames, _ := launchAndStop(t, c, pipeline, 0)
	if len(frames) != 1 || frames[0].Line != 12 {
		t.Fatalf("stack = %+v, want one frame at line 12", frames)
	}
	c.stepTo("next", 11, 1) // input.processors' end stop
	c.stepTo("next", 18, 1) // before the branch
	frames = c.stepTo("stepIn", 21, 2)
	if frames[0].Name != "pipeline.processors[0].branch.processors" || frames[1].Name != "pipeline.processors" || frames[1].Line != 18 {
		t.Errorf("stack in the branch = %+v", frames)
	}
	// The branch frame shows its request_map result; the pipeline frame
	// the batch before the branch.
	if got := c.content(frames[0].ID); got != "1" {
		t.Errorf("branch frame content = %s, want 1", got)
	}
	if got := c.content(frames[1].ID); !strings.Contains(got, `"from_input":true`) || strings.Contains(got, "branched") {
		t.Errorf("pipeline frame content = %s, want the message before the branch", got)
	}
	c.stepTo("stepOut", 20, 2) // the branch's end stop
	c.stepTo("stepOut", 23, 1) // back at the pipeline level, before the switch
	c.stepTo("stepIn", 26, 2)  // into the matching case
	c.stepTo("next", 25, 2)    // the case's end stop
	c.stepTo("next", 17, 1)    // pipeline.processors' end stop
	c.stepTo("next", 36, 1)    // output.processors[0]
	c.stepTo("next", 35, 1)    // output.processors' end stop
	c.send("next", map[string]any{"threadId": 1})
	c.next("response", "next")
	c.delivered(`{"branched":10,"case":"one","from_input":true,"from_output":true,"n":1}`)
	c.next("event", "thread")
	c.next("event", "terminated")
	c.send("disconnect", map[string]any{})
	c.next("response", "disconnect")
}

// Pause on a running session stops at the next processor: after the entry
// stop before a sleep, continue, and pause while the sleep runs.
func TestServePause(t *testing.T) {
	path := filepath.Join(t.TempDir(), "sleep.yaml")
	src := "input:\n  generate: {count: 1, interval: \"\", mapping: 'root = {}'}\n" +
		"pipeline:\n  processors:\n    - sleep:\n        duration: 1s\n    - mapping: 'root.a = 1'\n" +
		"output:\n  drop: {}\n"
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	c := serve(t)
	frames, _ := launchAndStop(t, c, path, 0)
	if len(frames) != 1 || frames[0].Line != 5 {
		t.Fatalf("stack = %+v, want one frame before the sleep at line 5", frames)
	}
	c.send("continue", map[string]any{"threadId": 1})
	c.next("response", "continue")
	c.send("pause", map[string]any{"threadId": 1})
	c.next("response", "pause")
	var stopped struct {
		Reason string `json:"reason"`
	}
	_ = json.Unmarshal(c.next("event", "stopped").Body, &stopped)
	if stopped.Reason != "pause" {
		t.Errorf("stop reason = %q, want pause", stopped.Reason)
	}
	if frames := c.stack(); len(frames) != 1 || frames[0].Line != 7 {
		t.Fatalf("paused stack = %+v, want one frame before the mapping at line 7", frames)
	}
	continueToEnd(t, c)
}

// A parallel processor is opaque: the launch warns, naming it and its
// line, and the session still runs.
func TestServeParallelWarns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "parallel.yaml")
	src := "input:\n  generate: {count: 1, interval: \"\", mapping: 'root = {}'}\npipeline:\n  processors:\n    - parallel:\n        processors:\n          - mapping: 'root.a = 1'\noutput:\n  drop: {}\n"
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	c := serve(t)
	frames, out := launchAndStop(t, c, path, 1)
	if !strings.Contains(out[0], "parallel") || !strings.Contains(out[0], "line 5") {
		t.Errorf("warning = %q, want one naming parallel and line 5", out[0])
	}
	if len(frames) != 1 || frames[0].Line != 5 {
		t.Fatalf("stack = %+v, want one frame at line 5", frames)
	}
	continueToEnd(t, c)
}

// A rewrite error keeps its line through to the DAP error.
func TestServeStdinInBroker(t *testing.T) {
	path := filepath.Join(t.TempDir(), "stdin.yaml")
	src := "input:\n  broker:\n    inputs:\n      - stdin: {}\n"
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	c := serve(t)
	c.send("initialize", map[string]any{})
	c.next("response", "initialize")
	c.send("launch", map[string]any{"pipeline": path})
	m := c.next("response", "launch")
	if m.Success || errorID(m) != core.Number(core.CodePipelineInvalid) || !strings.Contains(m.Message, path) ||
		!strings.Contains(m.Message, "input.broker.inputs[0].stdin, line 4") {
		t.Errorf("launch = %+v, want PIPELINE_INVALID at input.broker.inputs[0].stdin, line 4", m)
	}
	c.send("disconnect", map[string]any{})
	c.next("response", "disconnect")
}

// TestServeUndefinedResource is entry 2.13: a reference to an undefined
// processor resource fails the launch instead of hanging the run.
func TestServeUndefinedResource(t *testing.T) {
	path := filepath.Join(t.TempDir(), "undefined.yaml")
	src := "input:\n  generate:\n    count: 1\n    interval: \"\"\n    mapping: 'root = {}'\n" +
		"pipeline:\n  processors:\n    - resource: nope\noutput:\n  drop: {}\n"
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	c := serve(t)
	c.send("initialize", map[string]any{})
	c.next("response", "initialize")
	c.send("launch", map[string]any{"pipeline": path})
	m := c.next("response", "launch")
	if m.Success || errorID(m) != core.Number(core.CodePipelineInvalid) ||
		!strings.Contains(m.Message, `"nope"`) || !strings.Contains(m.Message, "pipeline.processors[0], line 8") {
		t.Errorf("launch = %+v, want PIPELINE_INVALID naming nope at pipeline.processors[0], line 8", m)
	}
	c.send("disconnect", map[string]any{})
	c.next("response", "disconnect")
}

// TestServeLintErrorAtFileLine is entry 12: a lint error in a processor
// fails the launch at the file line and the processor's user path, not at
// a position in the rewritten config.
func TestServeLintErrorAtFileLine(t *testing.T) {
	path := filepath.Join(t.TempDir(), "lint.yaml")
	src := "input:\n  generate:\n    count: 1\n    interval: \"\"\n    mapping: 'root = {}'\n\n" +
		"pipeline:\n  processors:\n    - mapping: 'root = this'\n    - mapping: 'root = '\noutput:\n  drop: {}\n"
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	c := serve(t)
	c.send("initialize", map[string]any{})
	c.next("response", "initialize")
	c.send("launch", map[string]any{"pipeline": path})
	m := c.next("response", "launch")
	if m.Success || errorID(m) != core.Number(core.CodePipelineInvalid) ||
		!strings.Contains(m.Message, path+": lint errors: pipeline.processors[1]: (line 10, column 24) expected query") ||
		strings.Contains(m.Message, "(at ") || strings.Contains(m.Message, "rewritten config") {
		t.Errorf("launch = %+v, want PIPELINE_INVALID at pipeline.processors[1], line 10", m)
	}
	c.send("disconnect", map[string]any{})
	c.next("response", "disconnect")
}

// syncBuffer is a bytes.Buffer safe for the log handler's writes and the
// test's reads.
type syncBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

// A processor error reaches the Debug Console as an output event naming
// the user's processor and file line, never a stepper, and the same text
// reaches the log output; ${VAR} resolves from the launch's env; and the
// engine's Info chatter stays off the Debug Console.
func TestServeLogsAndEnv(t *testing.T) {
	path := filepath.Join(t.TempDir(), "throw.yaml")
	src := `input:
  generate: {count: 1, interval: "", mapping: 'root = {}'}
pipeline:
  processors:
    - mapping: 'root.g = "${RPCN_DEBUG_TEST_GREETING}"'
    - mapping: |
        root.x = 1
        root.y = throw("boom")
output:
  drop: {}
`
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	var logs syncBuffer
	c := serve(t, WithLogOutput(&logs))
	c.send("initialize", map[string]any{"adapterID": "redpanda-connect"})
	c.next("response", "initialize")
	c.send("launch", map[string]any{"pipeline": path, "env": map[string]string{"RPCN_DEBUG_TEST_GREETING": "hi"}})
	if m := c.next("response", "launch"); !m.Success {
		t.Fatalf("launch failed: %s", m.Message)
	}
	c.next("event", "initialized")
	c.send("configurationDone", map[string]any{})
	c.next("response", "configurationDone")
	c.next("event", "thread")
	c.next("event", "stopped")
	frames := c.stepTo("next", 6, 1)
	if got := c.content(frames[0].ID); !strings.Contains(got, `"g":"hi"`) {
		t.Errorf("content before the second mapping = %s, want g: hi", got)
	}

	c.send("continue", map[string]any{"threadId": 1})
	c.next("response", "continue")
	var outputs []struct {
		Category string `json:"category"`
		Output   string `json:"output"`
	}
	for done := false; !done; {
		select {
		case m, ok := <-c.msgs:
			if !ok {
				t.Fatal("stream closed before terminated")
			}
			switch {
			case m.Type == "event" && m.Event == "output":
				var body struct {
					Category string `json:"category"`
					Output   string `json:"output"`
				}
				_ = json.Unmarshal(m.Body, &body)
				outputs = append(outputs, body)
			case m.Type == "event" && m.Event == "terminated":
				done = true
			case m.Type == "event" && (m.Event == "thread" || m.Event == "rpcnDelivered"):
			default:
				t.Fatalf("unexpected %+v", m)
			}
		case <-time.After(30 * time.Second):
			t.Fatal("timed out waiting for terminated")
		}
	}
	c.send("disconnect", map[string]any{})
	c.next("response", "disconnect")

	var errLine string
	for _, o := range outputs {
		if strings.Contains(o.Output, "Input type") || strings.Contains(o.Output, "now active") {
			t.Errorf("Info chatter reached the Debug Console: %q", o.Output)
		}
		if strings.Contains(o.Output, "boom") && !strings.HasPrefix(o.Output, "Delivered") {
			errLine = o.Output
			if o.Category != "stderr" {
				t.Errorf("error output category = %q, want stderr", o.Category)
			}
		}
	}
	if errLine == "" {
		t.Fatalf("no output event for the processor error; outputs %+v", outputs)
	}
	if !strings.Contains(errLine, "path=pipeline.processors[1]") || !strings.Contains(errLine, "(line 8)") ||
		strings.Contains(errLine, "rpcn_debug_stepper") {
		t.Errorf("error output = %q, want pipeline.processors[1] and line 8, without rpcn_debug_stepper", errLine)
	}
	t.Logf("error output: %q", errLine)
	got := logs.String()
	if !strings.Contains(got, errLine) {
		t.Errorf("log output lacks the error line %q:\n%s", errLine, got)
	}
	if !strings.Contains(got, "now active") {
		t.Errorf("log output lacks the engine's Info lines:\n%s", got)
	}
	if strings.Contains(got, "rpcn_debug_stepper") || strings.Contains(got, "hi\"") {
		t.Errorf("log output has a stepper path or the env value:\n%s", got)
	}
}
