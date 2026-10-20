package dap

import (
	"bufio"
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

// fakeEngine enters stepper 0, arrives before its one child and at its end
// stop with one message, then delivers. With nested set, it runs the
// nested tree instead (see startNested).
type fakeEngine struct {
	checkErr  error
	startErr  error
	nested    bool
	slow      time.Duration // how long the nested tree's first child runs
	batches   []core.Batch  // nil: one batch of one {"greeting":"hello"} message
	delivered chan []core.Batch
	ended     chan error
	opts      chan core.StartOptions // nil: not recorded
}

func newFakeEngine() *fakeEngine {
	return &fakeEngine{delivered: make(chan []core.Batch, 1), ended: make(chan error, 1)}
}

func text(s string) *string { return &s }

// jsonMessage is a Snapshot message with JSON content.
func jsonMessage(i int, content string) core.Message {
	return core.Message{Index: i, Content: core.Content{
		Raw: []byte(content), Text: text(content), JSON: []byte(content),
	}, Metadata: []core.Meta{}}
}

func (e *fakeEngine) Check(string, string) (core.CheckResult, error) {
	if e.checkErr != nil {
		return core.CheckResult{}, e.checkErr
	}
	if e.nested {
		return core.CheckResult{Lines: nestedLines}, nil
	}
	return core.CheckResult{Lines: fakeLines{
		{0, 0}: {UserPath: "pipeline.processors[0]", Line: 13},
		{0, 1}: {UserPath: "pipeline.processors", Line: 12},
	}}, nil
}

func (e *fakeEngine) Start(ctx context.Context, opts core.StartOptions, a core.Arriver) error {
	if e.opts != nil {
		e.opts <- opts
	}
	err := e.start(ctx, a)
	e.ended <- err
	return err
}

func (e *fakeEngine) start(ctx context.Context, a core.Arriver) error {
	if e.startErr != nil {
		return e.startErr
	}
	if e.nested {
		return e.startNested(ctx, a)
	}
	batches := e.batches
	if batches == nil {
		batches = []core.Batch{{Messages: []core.Message{jsonMessage(0, `{"greeting":"hello"}`)}}}
	}
	ctx, exit := a.Enter(ctx, 0, 1)
	defer exit()
	if err := a.Arrive(ctx, 0, 0, batches); err != nil {
		return err
	}
	if err := a.Arrive(ctx, 0, 1, batches); err != nil {
		return err
	}
	e.delivered <- batches
	return nil
}

// The nested tree: stepper 0 (pipeline.processors, key line 10) with a
// leaf at line 11 and a branch at line 12 whose stepper 1 (key line 13)
// has one leaf at line 14. Each stop's message is {"at":"<stepper>/<child>"}.
var nestedLines = fakeLines{
	{0, 0}: {UserPath: "pipeline.processors[0]", Line: 11},
	{0, 1}: {UserPath: "pipeline.processors[1]", Line: 12},
	{0, 2}: {UserPath: "pipeline.processors", Line: 10},
	{1, 0}: {UserPath: "pipeline.processors[1].branch.processors[0]", Line: 14},
	{1, 1}: {UserPath: "pipeline.processors[1].branch.processors", Line: 13},
}

func atBatch(stepper, child int) []core.Batch {
	return []core.Batch{{Messages: []core.Message{jsonMessage(0, fmt.Sprintf(`{"at":"%d/%d"}`, stepper, child))}}}
}

func (e *fakeEngine) startNested(ctx context.Context, a core.Arriver) error {
	ctx, exit := a.Enter(ctx, 0, 2)
	defer exit()
	if err := a.Arrive(ctx, 0, 0, atBatch(0, 0)); err != nil {
		return err
	}
	time.Sleep(e.slow)
	if err := a.Arrive(ctx, 0, 1, atBatch(0, 1)); err != nil {
		return err
	}
	inner, exitInner := a.Enter(ctx, 1, 1)
	for i := range 2 {
		if err := a.Arrive(inner, 1, i, atBatch(1, i)); err != nil {
			exitInner()
			return err
		}
	}
	exitInner()
	if err := a.Arrive(ctx, 0, 2, atBatch(0, 2)); err != nil {
		return err
	}
	e.delivered <- atBatch(0, 2)
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
	c.threadEvent("started")
	stopped := c.event("stopped").(*dap.StoppedEvent)
	if stopped.Body.Reason != "entry" || stopped.Body.ThreadId != 1 || stopped.Body.AllThreadsStopped {
		c.t.Errorf("stopped = %+v, want reason entry on thread 1 only", stopped.Body)
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
	if f.Line != 13 || f.Source == nil || f.Source.Path != "/p/pipeline.yaml" || f.Name != "pipeline.processors" {
		c.t.Errorf("frame = %+v (source %+v)", f, f.Source)
	}

	msg := c.message(f.Id, 0, 0)
	if len(msg) < 2 || msg[0].Name != "content" || msg[0].Value != `{"greeting":"hello"}` {
		c.t.Errorf("message 0 = %+v", msg)
	}
}

// threadEvent reads the next message, which must be a thread event for
// thread 1 with reason.
func (c *client) threadEvent(reason string) {
	c.t.Helper()
	e := c.event("thread").(*dap.ThreadEvent)
	if e.Body.Reason != reason || e.Body.ThreadId != 1 {
		c.t.Errorf("thread event = %+v, want %s for thread 1", e.Body, reason)
	}
}

// configured sends configurationDone and reads up to the first stop.
func (c *client) configured() *dap.StoppedEvent {
	c.t.Helper()
	c.send("configurationDone", map[string]any{})
	c.response("configurationDone", true)
	c.threadEvent("started")
	return c.event("stopped").(*dap.StoppedEvent)
}

// scopes returns the scopes of frameID, which must be one "Batches" scope.
func (c *client) scopes(frameID int) dap.Scope {
	c.t.Helper()
	c.send("scopes", map[string]any{"frameId": frameID})
	sc := c.response("scopes", true).(*dap.ScopesResponse)
	if len(sc.Body.Scopes) != 1 || sc.Body.Scopes[0].Name != "Batches" {
		c.t.Fatalf("scopes = %+v, want one Batches scope", sc.Body.Scopes)
	}
	return sc.Body.Scopes[0]
}

func (c *client) variables(args map[string]any) []dap.Variable {
	c.t.Helper()
	c.send("variables", args)
	return c.response("variables", true).(*dap.VariablesResponse).Body.Variables
}

// child returns the variable called name in vars.
func (c *client) child(vars []dap.Variable, name string) dap.Variable {
	c.t.Helper()
	for _, v := range vars {
		if v.Name == name {
			return v
		}
	}
	c.t.Fatalf("no variable %q in %+v", name, vars)
	return dap.Variable{}
}

// message walks Batches › batch b › message m and returns its children.
func (c *client) message(frameID, b, m int) []dap.Variable {
	c.t.Helper()
	sc := c.scopes(frameID)
	batch := c.child(c.variables(map[string]any{"variablesReference": sc.VariablesReference}), fmt.Sprintf("batch %d", b))
	msg := c.child(c.variables(map[string]any{"variablesReference": batch.VariablesReference}), fmt.Sprintf("message %d", m))
	if msg.VariablesReference == 0 {
		c.t.Fatalf("message %d is not expandable", m)
	}
	return c.variables(map[string]any{"variablesReference": msg.VariablesReference})
}

// frameID returns the id of the stopped thread's frame.
func (c *client) frameID() int {
	c.t.Helper()
	c.send("stackTrace", map[string]any{"threadId": 1})
	st := c.response("stackTrace", true).(*dap.StackTraceResponse)
	if len(st.Body.StackFrames) != 1 {
		c.t.Fatalf("stack = %+v, want one frame", st.Body.StackFrames)
	}
	return st.Body.StackFrames[0].Id
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
			if command == "next" {
				// The end stop, at the list's key line.
				if st := c.event("stopped").(*dap.StoppedEvent); st.Body.Reason != "step" {
					t.Errorf("stopped = %+v, want reason step", st.Body)
				}
				c.send("stackTrace", map[string]any{"threadId": 1})
				if st := c.response("stackTrace", true).(*dap.StackTraceResponse); len(st.Body.StackFrames) != 1 || st.Body.StackFrames[0].Line != 12 {
					t.Errorf("stack at the end stop = %+v, want one frame at line 12", st.Body.StackFrames)
				}
				c.send(command, map[string]any{"threadId": 1})
				c.response(command, true)
			}
			c.threadEvent("exited")
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
		code     core.Code
		want     []string
	}{
		"no pipeline": {args: map[string]any{}, code: core.CodeLaunchInvalid, want: []string{`"pipeline"`}},
		"invalid yaml": {
			args:     map[string]any{"pipeline": "/p/bad.yaml"},
			checkErr: errors.New("/p/bad.yaml: yaml: line 4: mapping values are not allowed in this context"),
			code:     core.CodePipelineInvalid, want: []string{"/p/bad.yaml", "line 4"},
		},
		"located error": {
			args:     map[string]any{"pipeline": "/p/bad.yaml"},
			checkErr: core.Error{Code: core.CodePipelineInvalid, Message: "bad processor", UserPath: "pipeline.processors[1]", Line: 9},
			code:     core.CodePipelineInvalid, want: []string{"bad processor", "pipeline.processors[1]", "line 9"},
		},
		"bad args": {args: "not an object", code: core.CodeLaunchInvalid, want: []string{"launch arguments"}},
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
			if resp.Body.Error == nil || resp.Body.Error.Id != core.Number(tc.code) {
				t.Errorf("launch error = %+v, want id %d (%s)", resp.Body.Error, core.Number(tc.code), tc.code)
			}
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
	eng.startErr = core.NewError(core.CodeNothingCaptured, "no batch was captured: the input closed")
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.send("configurationDone", map[string]any{})
	c.response("configurationDone", true)
	c.threadEvent("started")
	out := c.event("output").(*dap.OutputEvent)
	if !strings.Contains(out.Body.Output, "no batch was captured") {
		t.Errorf("output = %q", out.Body.Output)
	}
	c.threadEvent("exited")
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
	if resp := c.response("next", false).(*dap.ErrorResponse); resp.Body.Error.Id != core.Number(core.CodeNotStopped) {
		t.Errorf("next error = %+v, want NOT_STOPPED", resp.Body.Error)
	}
	c.send("stackTrace", map[string]any{"threadId": 1})
	if st := c.response("stackTrace", true).(*dap.StackTraceResponse); len(st.Body.StackFrames) != 0 {
		t.Errorf("stack before a stop = %+v", st.Body.StackFrames)
	}
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
}

func TestVariablesPaging(t *testing.T) {
	eng := newFakeEngine()
	var msgs []core.Message
	for i := range 5 {
		msgs = append(msgs, jsonMessage(i, fmt.Sprint(i)))
	}
	eng.batches = []core.Batch{{Messages: msgs}}
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.configured()
	sc := c.scopes(c.frameID())
	if sc.IndexedVariables != 1 {
		t.Errorf("scope = %+v, want 1 indexed batch", sc)
	}
	batch := c.child(c.variables(map[string]any{"variablesReference": sc.VariablesReference}), "batch 0")
	if batch.IndexedVariables != 5 {
		t.Errorf("batch 0 = %+v, want 5 indexed messages", batch)
	}

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
		tc.args["variablesReference"] = batch.VariablesReference
		var got []string
		for _, v := range c.variables(tc.args) {
			if v.Name != "message "+v.Value {
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

// The Variables tree of the matrix: Batches › batch › message › content,
// metadata (with a collapsed internal node) and error.
func TestVariablesTree(t *testing.T) {
	eng := newFakeEngine()
	first := jsonMessage(0, `{"a":1}`)
	first.Metadata = []core.Meta{
		{Key: "__rpcn_x", Type: core.MetaString, Value: "x", Internal: true},
		{Key: "bin", Type: core.MetaBytes, Value: "//4="},
		{Key: "kafka_key", Type: core.MetaBytes, Value: "YWJj", Text: text("abc")},
		{Key: "n", Type: core.MetaInt, Value: "5"},
	}
	second := core.Message{Index: 1, Content: core.Content{Raw: []byte{0xff, 'a'}}, Metadata: []core.Meta{}, Error: "boom"}
	eng.batches = []core.Batch{{Messages: []core.Message{first, second}}}
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.configured()
	frame := c.frameID()

	m0 := c.message(frame, 0, 0)
	if names := varNames(m0); names != "content metadata" {
		t.Errorf("message 0 children = %s, want content metadata", names)
	}
	if v := c.child(m0, "content"); v.Value != `{"a":1}` || v.Type != "json" {
		t.Errorf("content = %+v", v)
	}
	md := c.variables(map[string]any{"variablesReference": c.child(m0, "metadata").VariablesReference})
	if names := varNames(md); names != "bin kafka_key n internal" {
		t.Errorf("metadata = %s, want bin kafka_key n internal", names)
	}
	if v := c.child(md, "kafka_key"); v.Value != "abc" || v.Type != "bytes" {
		t.Errorf("kafka_key = %+v, want its text", v)
	}
	if v := c.child(md, "bin"); v.Value != "//4=" {
		t.Errorf("bin = %+v, want base64", v)
	}
	internal := c.child(md, "internal")
	in := c.variables(map[string]any{"variablesReference": internal.VariablesReference})
	if len(in) != 1 || in[0].Name != "__rpcn_x" || in[0].Value != "x" {
		t.Errorf("internal = %+v", in)
	}

	m1 := c.message(frame, 0, 1)
	if names := varNames(m1); names != "content metadata error" {
		t.Errorf("message 1 children = %s", names)
	}
	if v := c.child(m1, "error"); v.Value != "boom" {
		t.Errorf("error = %+v", v)
	}
	if v := c.child(m1, "content"); v.Value != "/2E=" || v.Type != "bytes" {
		t.Errorf("binary content = %+v, want base64", v)
	}
	if v := c.child(m1, "metadata"); v.VariablesReference != 0 {
		t.Errorf("empty metadata = %+v, want a leaf", v)
	}

	// References that point nowhere answer nothing.
	w, _ := decodeFrameID(frame)
	for _, ref := range []int{0, 2, 7, encodeRef(w, kindMessage, 0, 9), encodeRef(w, kindBatch, 3, 0), encodeRef(w, 6, 0, 0), encodeRef(at{w.stop, 1}, kindBatches, 0, 0)} {
		if vars := c.variables(map[string]any{"variablesReference": ref}); len(vars) != 0 {
			t.Errorf("variables(%d) = %+v, want none", ref, vars)
		}
	}
	// The frame id of the stop is the only valid one.
	c.send("scopes", map[string]any{"frameId": frame + 1})
	if sc := c.response("scopes", true).(*dap.ScopesResponse); len(sc.Body.Scopes) != 0 {
		t.Errorf("scopes of a stale frame = %+v", sc.Body.Scopes)
	}
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
}

func varNames(vars []dap.Variable) string {
	names := make([]string, len(vars))
	for i, v := range vars {
		names[i] = v.Name
	}
	return strings.Join(names, " ")
}

func TestRefEncoding(t *testing.T) {
	for _, tc := range []struct {
		stop int
		r    ref
	}{
		{1, ref{at{1, 0}, kindBatches, 0, 0}},
		{2, ref{at{2, 3}, kindBatch, 0, 0}},
		{maxRefStops + 2, ref{at{2, 1}, kindMessage, 3, 7}},
		{maxRefStops - 1, ref{at{maxRefStops - 1, maxRefFrames - 1}, kindInternal, maxRefBatches - 1, maxRefMessages - 1}},
	} {
		w := at{tc.stop, tc.r.frame}
		n := encodeRef(w, tc.r.kind, tc.r.batch, tc.r.msg)
		if n < refBase || n >= 1<<31 {
			t.Errorf("encodeRef(%+v, %+v) = %d", w, tc.r, n)
		}
		if got, ok := decodeRef(n); !ok || got != tc.r {
			t.Errorf("decodeRef(%d) = %+v, %v; want %+v", n, got, ok, tc.r)
		}
		id := frameID(tc.stop, tc.r.frame)
		if got, ok := decodeFrameID(id); !ok || got != tc.r.at {
			t.Errorf("decodeFrameID(%d) = %+v, %v; want %+v", id, got, ok, tc.r.at)
		}
	}
	if encodeRef(at{1, 0}, kindMessage, maxRefBatches, 0) != 0 || encodeRef(at{1, 0}, kindMessage, 0, maxRefMessages) != 0 ||
		encodeRef(at{1, maxRefFrames}, kindBatches, 0, 0) != 0 {
		t.Error("out-of-range references are expandable")
	}
	if frameID(1, maxRefFrames) != 0 {
		t.Error("a frame too deep has an id")
	}
	if _, ok := decodeFrameID(0); ok {
		t.Error("frame id 0 decodes")
	}
}

// A reference or frame id handed out at one stop resolves to nothing at
// the next (AD-17).
func TestStaleReferences(t *testing.T) {
	snap := core.Snapshot{Batches: []core.Batch{{Messages: []core.Message{jsonMessage(0, `{"a":1}`)}}}}
	old := core.Stop{ID: 1, Frames: []core.Frame{{ID: 1, Snapshot: snap}}}
	cur := core.Stop{ID: 2, Frames: []core.Frame{{ID: 1, Snapshot: snap}}}
	all := func(st core.Stop, ref int) []dap.Variable {
		return variables(st, dap.VariablesArguments{VariablesReference: ref})
	}
	w := at{old.ID, 0}
	refs := []int{
		batchesRef(w),
		encodeRef(w, kindBatch, 0, 0),
		encodeRef(w, kindMessage, 0, 0),
	}
	for _, ref := range refs {
		if len(all(old, ref)) == 0 {
			t.Errorf("reference %d is empty at its own stop", ref)
		}
		if vars := all(cur, ref); len(vars) != 0 {
			t.Errorf("reference %d from the previous stop = %+v, want nothing", ref, vars)
		}
	}
	if len(all(cur, batchesRef(at{cur.ID, 0}))) != 1 {
		t.Error("the current stop's scope is empty")
	}
	if _, ok := frameOf(cur, at{stopOf(old.ID), 0}); ok {
		t.Error("a frame id of the previous stop resolves")
	}
}

func TestSetBreakpointsUnverified(t *testing.T) {
	c := startServer(t, newFakeEngine())
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml"})
	c.send("setBreakpoints", map[string]any{
		"source":      map[string]any{"path": "/p/pipeline.yaml"},
		"breakpoints": []map[string]any{{"line": 3}, {"line": 13}, {"line": 14, "condition": "this.a > 1"}},
	})
	bps := c.response("setBreakpoints", true).(*dap.SetBreakpointsResponse).Body.Breakpoints
	if len(bps) != 3 {
		t.Fatalf("breakpoints = %+v, want 3", bps)
	}
	for i, line := range []int{3, 13, 14} {
		b := bps[i]
		if b.Verified || b.Line != line || b.Message == "" || b.Source == nil || b.Source.Path != "/p/pipeline.yaml" {
			t.Errorf("breakpoint %d = %+v, want unverified at line %d with a message", i, b, line)
		}
	}
	c.send("setExceptionBreakpoints", map[string]any{"filters": []string{}})
	c.response("setExceptionBreakpoints", true)
	c.configured()
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
}

// Commands a later entry implements answer NOT_SUPPORTED and leave the stop
// as it was.
func TestUnsupportedCommands(t *testing.T) {
	c := startServer(t, newFakeEngine())
	c.toStop()
	frame := c.frameID()
	for _, req := range []struct {
		command string
		args    map[string]any
	}{
		{"evaluate", map[string]any{"expression": "this", "frameId": frame}},
		{"restart", map[string]any{}},
		{"setVariable", map[string]any{"variablesReference": 1, "name": "x", "value": "1"}},
	} {
		c.send(req.command, req.args)
		resp := c.response(req.command, false).(*dap.ErrorResponse)
		if resp.Body.Error == nil || resp.Body.Error.Id != core.Number(core.CodeNotSupported) {
			t.Errorf("%s error = %+v, want NOT_SUPPORTED (%d)", req.command, resp.Body.Error, core.Number(core.CodeNotSupported))
		}
	}
	if got := c.frameID(); got != frame {
		t.Errorf("frame = %d after unsupported commands, want %d", got, frame)
	}
	c.send("continue", map[string]any{"threadId": 1})
	c.response("continue", true)
	c.threadEvent("exited")
	c.event("terminated")
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
}
