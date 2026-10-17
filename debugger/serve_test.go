package debugger

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
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

func serve(t *testing.T) *wire {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &wire{t: t, w: inW, msgs: make(chan msg, 64), done: make(chan error, 1)}
	go func() {
		err := Serve(service.NewEnvironment(), inR, outW)
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
