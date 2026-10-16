package dap

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/google/go-dap"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

type fakeLines map[[2]int]core.Position

func (l fakeLines) Position(stepperID, childIndex int) (core.Position, bool) {
	p, ok := l[[2]int{stepperID, childIndex}]
	return p, ok
}

// fakeEngine arrives once before child 0 with one message, then delivers.
type fakeEngine struct {
	checkErr  error
	startErr  error
	batch     core.BatchCopy // nil: one {"greeting":"hello"} message
	delivered chan core.BatchCopy
	ended     chan error
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{delivered: make(chan core.BatchCopy, 1), ended: make(chan error, 1)}
}

func (e *fakeEngine) Check(string, string) (core.LineMap, error) {
	if e.checkErr != nil {
		return nil, e.checkErr
	}
	return fakeLines{{0, 0}: {Path: "pipeline.processors[0]", Line: 13}}, nil
}

func (e *fakeEngine) Start(ctx context.Context, _, _ string, a core.Arriver) error {
	err := e.start(ctx, a)
	e.ended <- err
	return err
}

func (e *fakeEngine) start(ctx context.Context, a core.Arriver) error {
	if e.startErr != nil {
		return e.startErr
	}
	batch := e.batch
	if batch == nil {
		batch = core.BatchCopy{{Content: []byte(`{"greeting":"hello"}`)}}
	}
	if err := a.Arrive(ctx, 0, 0, batch); err != nil {
		return err
	}
	e.delivered <- batch
	return nil
}

// client is an in-memory DAP client talking to Serve over pipes.
type client struct {
	t    *testing.T
	w    *io.PipeWriter
	msgs chan dap.Message
	seq  int
	done chan error
}

func startServer(t *testing.T, eng core.Engine) *client {
	t.Helper()
	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	c := &client{t: t, w: inW, msgs: make(chan dap.Message, 32), done: make(chan error, 1)}
	log := slog.New(slog.NewTextHandler(io.Discard, nil))
	go func() {
		err := Serve(inR, outW, log, eng)
		_ = outW.Close()
		c.done <- err
	}()
	go func() {
		r := bufio.NewReader(outR)
		for {
			m, err := dap.ReadProtocolMessage(r)
			if err != nil {
				close(c.msgs)
				return
			}
			c.msgs <- m
		}
	}()
	t.Cleanup(func() { _ = inW.Close() })
	return c
}

func (c *client) send(command string, args any) {
	c.t.Helper()
	c.seq++
	raw, err := json.Marshal(args)
	if err != nil {
		c.t.Fatal(err)
	}
	body := fmt.Sprintf(`{"seq":%d,"type":"request","command":%q,"arguments":%s}`, c.seq, command, raw)
	if _, err := fmt.Fprintf(c.w, "Content-Length: %d\r\n\r\n%s", len(body), body); err != nil {
		c.t.Fatal(err)
	}
}

func (c *client) next() dap.Message {
	c.t.Helper()
	select {
	case m, ok := <-c.msgs:
		if !ok {
			c.t.Fatal("server closed the stream")
		}
		return m
	case <-time.After(5 * time.Second):
		c.t.Fatal("timed out waiting for a DAP message")
	}
	return nil
}

// response reads the next message, which must be a response to command.
func (c *client) response(command string, success bool) dap.ResponseMessage {
	c.t.Helper()
	m := c.next()
	r, ok := m.(dap.ResponseMessage)
	if !ok {
		c.t.Fatalf("got %T, want a %s response", m, command)
	}
	resp := r.GetResponse()
	if resp.Command != command || resp.Success != success || resp.RequestSeq != c.seq {
		c.t.Fatalf("response = %+v, want %s (success %v) to seq %d", resp, command, success, c.seq)
	}
	return r
}

// event reads the next message, which must be the event name.
func (c *client) event(name string) dap.EventMessage {
	c.t.Helper()
	m := c.next()
	e, ok := m.(dap.EventMessage)
	if !ok || e.GetEvent().Event != name {
		c.t.Fatalf("got %#v, want a %s event", m, name)
	}
	return e
}

func (c *client) serveResult() error {
	c.t.Helper()
	select {
	case err := <-c.done:
		return err
	case <-time.After(5 * time.Second):
		c.t.Fatal("Serve did not return")
	}
	return nil
}

func (c *client) launch(args any) {
	c.t.Helper()
	c.send("initialize", map[string]any{"adapterID": "redpanda-connect"})
	c.response("initialize", true)
	c.send("launch", args)
	c.response("launch", true)
	c.event("initialized")
}

// toStop runs the F5 happy path up to the stop and checks what VS Code shows.
func (c *client) toStop() {
	c.t.Helper()
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.send("configurationDone", map[string]any{})
	c.response("configurationDone", true)
	stopped := c.event("stopped").(*dap.StoppedEvent)
	if stopped.Body.Reason != "entry" || stopped.Body.ThreadId != 1 {
		c.t.Errorf("stopped = %+v, want reason entry on thread 1", stopped.Body)
	}

	c.send("threads", map[string]any{})
	threads := c.response("threads", true).(*dap.ThreadsResponse)
	if len(threads.Body.Threads) != 1 || threads.Body.Threads[0].Id != 1 {
		c.t.Errorf("threads = %+v", threads.Body.Threads)
	}

	c.send("stackTrace", map[string]any{"threadId": 1})
	st := c.response("stackTrace", true).(*dap.StackTraceResponse)
	if len(st.Body.StackFrames) != 1 {
		c.t.Fatalf("stack = %+v, want one frame", st.Body.StackFrames)
	}
	f := st.Body.StackFrames[0]
	if f.Line != 13 || f.Source == nil || f.Source.Path != "/p/pipeline.yaml" || f.Name != "pipeline.processors[0]" {
		c.t.Errorf("frame = %+v (source %+v)", f, f.Source)
	}

	c.send("scopes", map[string]any{"frameId": f.Id})
	sc := c.response("scopes", true).(*dap.ScopesResponse)
	if len(sc.Body.Scopes) != 1 || sc.Body.Scopes[0].Name != "Batch" {
		c.t.Fatalf("scopes = %+v", sc.Body.Scopes)
	}

	c.send("variables", map[string]any{"variablesReference": sc.Body.Scopes[0].VariablesReference})
	vars := c.response("variables", true).(*dap.VariablesResponse)
	if len(vars.Body.Variables) != 1 || vars.Body.Variables[0].Value != `{"greeting":"hello"}` {
		c.t.Errorf("variables = %+v", vars.Body.Variables)
	}
}

func TestF5StopThenResume(t *testing.T) {
	for _, command := range []string{"next", "continue"} {
		t.Run(command, func(t *testing.T) {
			eng := newFakeEngine()
			c := startServer(t, eng)
			c.toStop()

			c.send(command, map[string]any{"threadId": 1})
			resp := c.response(command, true)
			if cr, ok := resp.(*dap.ContinueResponse); ok && !cr.Body.AllThreadsContinued {
				t.Error("continue response without allThreadsContinued")
			}
			c.event("terminated")
			select {
			case <-eng.delivered:
			default:
				t.Error("nothing was delivered")
			}

			c.send("disconnect", map[string]any{})
			c.response("disconnect", true)
			if err := c.serveResult(); err != nil {
				t.Errorf("Serve = %v", err)
			}
		})
	}
}

func TestLaunchFailures(t *testing.T) {
	cases := map[string]struct {
		args     any
		checkErr error
		want     []string
	}{
		"no pipeline":  {args: map[string]any{}, want: []string{`"pipeline"`}},
		"invalid yaml": {args: map[string]any{"pipeline": "/p/bad.yaml"}, checkErr: errors.New("/p/bad.yaml: yaml: line 4: mapping values are not allowed in this context"), want: []string{"/p/bad.yaml", "line 4"}},
		"bad args":     {args: "not an object", want: []string{"launch arguments"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			eng := newFakeEngine()
			eng.checkErr = tc.checkErr
			c := startServer(t, eng)
			c.send("initialize", map[string]any{})
			c.response("initialize", true)
			c.send("launch", tc.args)
			resp := c.response("launch", false).(*dap.ErrorResponse)
			for _, w := range tc.want {
				if !strings.Contains(resp.Message, w) || resp.Body.Error == nil || !strings.Contains(resp.Body.Error.Format, w) {
					t.Errorf("launch error = %q / %+v, want it to name %s", resp.Message, resp.Body.Error, w)
				}
			}
			c.send("configurationDone", map[string]any{})
			c.response("configurationDone", false)
			c.send("disconnect", map[string]any{})
			c.response("disconnect", true)
			if err := c.serveResult(); err != nil {
				t.Errorf("Serve = %v", err)
			}
			select {
			case <-eng.ended:
				t.Error("the engine ran after a failed launch")
			default:
			}
		})
	}
}

func TestNothingCaptured(t *testing.T) {
	eng := newFakeEngine()
	eng.startErr = core.ErrNothingCaptured
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.send("configurationDone", map[string]any{})
	c.response("configurationDone", true)
	out := c.event("output").(*dap.OutputEvent)
	if !strings.Contains(out.Body.Output, "no batch was captured") {
		t.Errorf("output = %q", out.Body.Output)
	}
	c.event("terminated")
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
}

func TestDisconnectWhileStopped(t *testing.T) {
	eng := newFakeEngine()
	c := startServer(t, eng)
	c.toStop()
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
	// Disconnect waited for the run: it has already ended, with nothing
	// delivered.
	select {
	case err := <-eng.ended:
		if err == nil {
			t.Error("the run ended normally after disconnect")
		}
	default:
		t.Error("the run was still going after disconnect")
	}
	select {
	case <-eng.delivered:
		t.Error("delivered after disconnect")
	default:
	}
	if m, ok := <-c.msgs; ok {
		t.Errorf("message after disconnect: %#v", m)
	}
}

// A client that vanishes while stopped still ends the run.
func TestEOFWhileStopped(t *testing.T) {
	eng := newFakeEngine()
	c := startServer(t, eng)
	c.toStop()
	_ = c.w.Close()
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
	select {
	case <-eng.ended:
	default:
		t.Error("the run was still going after EOF")
	}
}

func TestResumeWhenNotStopped(t *testing.T) {
	c := startServer(t, newFakeEngine())
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.send("next", map[string]any{"threadId": 1})
	c.response("next", false)
	c.send("stackTrace", map[string]any{"threadId": 1})
	if st := c.response("stackTrace", true).(*dap.StackTraceResponse); len(st.Body.StackFrames) != 0 {
		t.Errorf("stack before a stop = %+v", st.Body.StackFrames)
	}
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
}

func TestContentText(t *testing.T) {
	if got := contentText([]byte("plain")); got != "plain" {
		t.Errorf("contentText = %q", got)
	}
	if got := contentText([]byte{0xff, 'a'}); !bytes.Contains([]byte(got), []byte(`\xff`)) {
		t.Errorf("contentText of invalid UTF-8 = %q", got)
	}
}

func TestVariablesPaging(t *testing.T) {
	eng := newFakeEngine()
	for i := range 5 {
		eng.batch = append(eng.batch, core.MessageCopy{Content: []byte(fmt.Sprint(i))})
	}
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.send("configurationDone", map[string]any{})
	c.response("configurationDone", true)
	c.event("stopped")

	cases := []struct {
		args map[string]any
		want []string
	}{
		{map[string]any{}, []string{"0", "1", "2", "3", "4"}},
		{map[string]any{"filter": "indexed", "start": 1, "count": 2}, []string{"1", "2"}},
		{map[string]any{"filter": "indexed", "start": 3}, []string{"3", "4"}},
		{map[string]any{"start": 4, "count": 10}, []string{"4"}},
		{map[string]any{"start": 9, "count": 2}, nil},
		{map[string]any{"filter": "named"}, nil},
	}
	for _, tc := range cases {
		tc.args["variablesReference"] = batchScopeRef
		c.send("variables", tc.args)
		vars := c.response("variables", true).(*dap.VariablesResponse)
		var got []string
		for _, v := range vars.Body.Variables {
			if v.Name != "["+v.Value+"]" {
				t.Errorf("variable %+v: name does not match its index", v)
			}
			got = append(got, v.Value)
		}
		if fmt.Sprint(got) != fmt.Sprint(tc.want) {
			t.Errorf("variables %v = %v, want %v", tc.args, got, tc.want)
		}
	}
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
}
