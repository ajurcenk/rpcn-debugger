// Package dapdrive is a scripted DAP client for end-to-end tests. Run starts
// debugger.Serve over in-memory pipes, launches a pipeline, answers every
// stop with a Script (Step In at every stop, say) until the run terminates,
// and returns what each stop showed: its line, user path, frame stack with
// repeat labels and batches, plus the Debug Console output and the
// delivered batches (from the rpcnDelivered events).
//
// It is a plain (non-test) package so other packages' tests can import it.
// It speaks DAP as raw JSON, so it needs no go-dap (AD-3).
package dapdrive

import (
	"bufio"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/schema"

	_ "github.com/redpanda-data/connect/v4/public/components/all"

	"github.com/ajurcenk/rpcn-debugger/debugger"
	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

// Command is what a Script answers a stop with.
type Command string

// The commands a Script can issue.
const (
	StepIn   Command = "stepIn"
	Next     Command = "next"
	StepOut  Command = "stepOut"
	Continue Command = "continue"
)

// Script decides the command at each stop. stops are the stops so far, the
// current one last.
type Script func(stops []Stop) Command

// Always is the Script that issues c at every stop.
func Always(c Command) Script { return func([]Stop) Command { return c } }

// Options configure Run.
type Options struct {
	// Pipeline is the pipeline YAML file.
	Pipeline string
	// Launch holds extra launch arguments (maxMessages, env, …). pipeline
	// and cwd (the pipeline's directory) are set by Run.
	Launch map[string]any
	// Script answers each stop. nil means Always(StepIn).
	Script Script
	// Env is the Connect environment. nil means the stock binary's:
	// schema.Standard with components/all (AD-1).
	Env *service.Environment
	// LogOutput receives Serve's stderr log. nil discards it.
	LogOutput io.Writer
	// MaxStops bounds the run: Run fails after this many stops. 0 means
	// 10000.
	MaxStops int
}

// Frame is one stack frame of a stop, innermost first.
type Frame struct {
	// Name is the frame's child list path, e.g.
	// pipeline.processors[0].for_each, without the repeat label.
	Name string
	// Label is the repeat label, e.g. [msg 1/3], or "".
	Label string
	// Line is the frame's current line.
	Line int
}

// Message is one message as the Variables view shows it.
type Message struct {
	// Content is the content's text, or base64 when it isn't UTF-8.
	Content string
	// Base64 is true when Content is the base64 of the raw content. Only
	// delivered messages set it.
	Base64 bool
	// Raw is the content's exact bytes, from the Snapshot's raw view. Only
	// delivered messages carry it; it is nil for the Variables view.
	Raw []byte
	// Metadata maps each key, internal ones included, to its type and
	// display value.
	Metadata map[string]Meta
	// Error is the message's error, or "".
	Error string
}

// Meta is one metadata value as the Variables view shows it: its AD-11
// type and its display text (or its lossless value when it has none).
type Meta struct {
	Type  string
	Value string
	// Lossless is the Snapshot's lossless value (AD-11: bytes as base64,
	// ints as decimal, …). Only delivered messages carry it; it is "" for
	// the Variables view.
	Lossless string
}

// Batch is one batch of messages.
type Batch []Message

// Stop is one stop of the run.
type Stop struct {
	// Reason is the stopped event's reason: entry, step, pause, ….
	Reason string
	// Line is the line of the stop (the innermost frame's).
	Line int
	// UserPath is the stop's user path: the child about to run, e.g.
	// pipeline.processors[0], or the child list at its end-of-entry stop,
	// e.g. pipeline.processors (AD-10).
	UserPath string
	// End is true at an end-of-entry stop.
	End bool
	// Frames is the stopped thread's stack, innermost first.
	Frames []Frame
	// Batches are the innermost frame's batches.
	Batches []Batch
}

// Result is what a run showed.
type Result struct {
	Stops []Stop
	// Output holds the text of every output event, in order.
	Output []string
	// Delivered are the batches rpcn_debug_sink received (AD-21), in
	// order, from the rpcnDelivered events. It is nil when nothing was
	// delivered.
	Delivered []Batch
}

// standardEnv is the stock binary's component set, built once. Runs never
// modify it: Serve works on clones (AD-1).
var standardEnv = sync.OnceValue(func() *service.Environment {
	return schema.Standard("dev", "").Environment()
})

// Run debugs opts.Pipeline over DAP until the run terminates or ctx ends.
func Run(ctx context.Context, opts Options) (Result, error) {
	pipeline, err := filepath.Abs(opts.Pipeline)
	if err != nil {
		return Result{}, err
	}
	paths, err := userPaths(pipeline)
	if err != nil {
		return Result{}, err
	}
	env := opts.Env
	if env == nil {
		env = standardEnv()
	}
	logOut := opts.LogOutput
	if logOut == nil {
		logOut = io.Discard
	}
	script := opts.Script
	if script == nil {
		script = Always(StepIn)
	}
	maxStops := opts.MaxStops
	if maxStops == 0 {
		maxStops = 10000
	}

	inR, inW := io.Pipe()
	outR, outW := io.Pipe()
	served := make(chan error, 1)
	go func() {
		err := debugger.Serve(env, inR, outW, debugger.WithLogOutput(logOut))
		_ = outW.Close()
		served <- err
	}()
	c := &client{ctx: ctx, w: inW, msgs: make(chan message, 256)}
	go c.read(outR)
	defer func() {
		_ = inW.Close()
		_ = outR.Close()
		<-served
	}()

	res, err := c.run(pipeline, opts.Launch, script, paths, maxStops)
	if err != nil {
		return res, err
	}
	_ = inW.Close()
	if err := <-served; err != nil {
		return res, fmt.Errorf("serve: %w", err)
	}
	served <- nil // for the deferred wait
	return res, nil
}

// message is one decoded DAP message.
type message struct {
	Type       string          `json:"type"`
	Command    string          `json:"command"`
	Event      string          `json:"event"`
	RequestSeq int             `json:"request_seq"`
	Success    bool            `json:"success"`
	Message    string          `json:"message"`
	Body       json.RawMessage `json:"body"`
}

type client struct {
	ctx     context.Context
	w       io.Writer
	msgs    chan message
	readErr error
	seq     int
	events  []message // events read while waiting for a response
}

func (c *client) read(r io.Reader) {
	defer close(c.msgs)
	br := bufio.NewReader(r)
	for {
		header, err := br.ReadString('\n')
		if err != nil {
			return
		}
		n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "Content-Length:")))
		if err != nil {
			c.readErr = fmt.Errorf("bad DAP header %q", header)
			return
		}
		if _, err := br.ReadString('\n'); err != nil {
			return
		}
		body := make([]byte, n)
		if _, err := io.ReadFull(br, body); err != nil {
			return
		}
		var m message
		if err := json.Unmarshal(body, &m); err != nil {
			c.readErr = fmt.Errorf("bad DAP body %s: %w", body, err)
			return
		}
		c.msgs <- m
	}
}

// receive returns the next message from the server.
func (c *client) receive() (message, error) {
	select {
	case m, ok := <-c.msgs:
		if !ok {
			if c.readErr != nil {
				return message{}, c.readErr
			}
			return message{}, errors.New("the DAP stream closed")
		}
		return m, nil
	case <-c.ctx.Done():
		return message{}, c.ctx.Err()
	}
}

// request sends command and returns its response's body; the events read
// meanwhile are queued. A failed response is an error.
func (c *client) request(command string, args any) (json.RawMessage, error) {
	c.seq++
	raw, err := json.Marshal(args)
	if err != nil {
		return nil, err
	}
	body := fmt.Sprintf(`{"seq":%d,"type":"request","command":%q,"arguments":%s}`, c.seq, command, raw)
	if _, err := fmt.Fprintf(c.w, "Content-Length: %d\r\n\r\n%s", len(body), body); err != nil {
		return nil, fmt.Errorf("%s: %w", command, err)
	}
	for {
		m, err := c.receive()
		if err != nil {
			return nil, fmt.Errorf("waiting for the %s response: %w", command, err)
		}
		switch {
		case m.Type == "event":
			c.events = append(c.events, m)
		case m.Type == "response" && m.RequestSeq == c.seq:
			if !m.Success {
				return nil, fmt.Errorf("%s failed: %s", command, m.Message)
			}
			return m.Body, nil
		}
	}
}

// event returns the next event, queued or read.
func (c *client) event() (message, error) {
	if len(c.events) > 0 {
		m := c.events[0]
		c.events = c.events[1:]
		return m, nil
	}
	for {
		m, err := c.receive()
		if err != nil {
			return message{}, err
		}
		if m.Type == "event" {
			return m, nil
		}
	}
}

func (c *client) run(pipeline string, launch map[string]any, script Script, paths map[pathKey]string, maxStops int) (Result, error) {
	var res Result
	if _, err := c.request("initialize", map[string]any{"adapterID": "redpanda-connect", "linesStartAt1": true}); err != nil {
		return res, err
	}
	args := map[string]any{}
	for k, v := range launch {
		args[k] = v
	}
	args["pipeline"], args["cwd"] = pipeline, filepath.Dir(pipeline)
	if _, err := c.request("launch", args); err != nil {
		return res, err
	}
	if _, err := c.request("configurationDone", map[string]any{}); err != nil {
		return res, err
	}
	for {
		ev, err := c.event()
		if err != nil {
			return res, err
		}
		switch ev.Event {
		case "output":
			var b struct {
				Output string `json:"output"`
			}
			_ = json.Unmarshal(ev.Body, &b)
			res.Output = append(res.Output, b.Output)
		case "stopped":
			if len(res.Stops) >= maxStops {
				return res, fmt.Errorf("more than %d stops", maxStops)
			}
			var b struct {
				Reason   string `json:"reason"`
				ThreadID int    `json:"threadId"`
			}
			if err := json.Unmarshal(ev.Body, &b); err != nil {
				return res, err
			}
			st, err := c.stop(b.ThreadID, paths)
			if err != nil {
				return res, err
			}
			st.Reason = b.Reason
			res.Stops = append(res.Stops, st)
			cmd := script(res.Stops)
			if _, err := c.request(string(cmd), map[string]any{"threadId": b.ThreadID}); err != nil {
				return res, err
			}
		case "rpcnDelivered":
			b, err := deliveredBatches(ev.Body, len(res.Delivered)+1)
			if err != nil {
				return res, err
			}
			res.Delivered = append(res.Delivered, b...)
		case "terminated":
			if _, err := c.request("disconnect", map[string]any{}); err != nil {
				return res, err
			}
			return res, nil
		}
	}
}

// deliveredBatches decodes the body of rpcnDelivered event number n,
// {batch, snapshot}, into the batches its AD-11 Snapshot holds, shown the
// way the Variables view shows messages.
func deliveredBatches(body json.RawMessage, n int) ([]Batch, error) {
	var b struct {
		Batch    int           `json:"batch"`
		Snapshot core.Snapshot `json:"snapshot"`
	}
	if err := json.Unmarshal(body, &b); err != nil {
		return nil, fmt.Errorf("bad rpcnDelivered body %s: %w", body, err)
	}
	if b.Batch != n {
		return nil, fmt.Errorf("rpcnDelivered batch %d, want %d", b.Batch, n)
	}
	var out []Batch
	for _, sb := range b.Snapshot.Batches {
		batch := Batch{}
		for _, m := range sb.Messages {
			msg := Message{Error: m.Error, Metadata: map[string]Meta{}, Raw: append([]byte{}, m.Content.Raw...)}
			if m.Content.Text != nil {
				msg.Content = *m.Content.Text
			} else {
				msg.Content, msg.Base64 = base64.StdEncoding.EncodeToString(m.Content.Raw), true
			}
			for _, md := range m.Metadata {
				v := md.Value
				if md.Text != nil {
					v = *md.Text
				}
				msg.Metadata[md.Key] = Meta{Type: string(md.Type), Value: v, Lossless: md.Value}
			}
			batch = append(batch, msg)
		}
		out = append(out, batch)
	}
	return out, nil
}

// stop reads the stack and the innermost frame's batches of the stop on
// thread tid.
func (c *client) stop(tid int, paths map[pathKey]string) (Stop, error) {
	raw, err := c.request("stackTrace", map[string]any{"threadId": tid})
	if err != nil {
		return Stop{}, err
	}
	var trace struct {
		StackFrames []struct {
			ID   int    `json:"id"`
			Name string `json:"name"`
			Line int    `json:"line"`
		} `json:"stackFrames"`
	}
	if err := json.Unmarshal(raw, &trace); err != nil {
		return Stop{}, err
	}
	if len(trace.StackFrames) == 0 {
		return Stop{}, errors.New("a stop with no stack frames")
	}
	var st Stop
	for _, f := range trace.StackFrames {
		name, label := f.Name, ""
		if i := strings.LastIndex(f.Name, " ["); i >= 0 && strings.HasSuffix(f.Name, "]") {
			name, label = f.Name[:i], f.Name[i+1:]
		}
		st.Frames = append(st.Frames, Frame{Name: name, Label: label, Line: f.Line})
	}
	top := st.Frames[0]
	st.Line = top.Line
	if p, ok := paths[pathKey{top.Name, top.Line, false}]; ok {
		st.UserPath = p
	} else if _, ok := paths[pathKey{top.Name, top.Line, true}]; ok {
		st.UserPath, st.End = top.Name, true
	}
	st.Batches, err = c.batches(trace.StackFrames[0].ID)
	return st, err
}

// variable is one DAP variable.
type variable struct {
	Name               string `json:"name"`
	Value              string `json:"value"`
	Type               string `json:"type"`
	VariablesReference int    `json:"variablesReference"`
}

func (c *client) variables(ref int) ([]variable, error) {
	raw, err := c.request("variables", map[string]any{"variablesReference": ref})
	if err != nil {
		return nil, err
	}
	var b struct {
		Variables []variable `json:"variables"`
	}
	err = json.Unmarshal(raw, &b)
	return b.Variables, err
}

// batches reads the Batches scope of DAP frame id.
func (c *client) batches(frameID int) ([]Batch, error) {
	raw, err := c.request("scopes", map[string]any{"frameId": frameID})
	if err != nil {
		return nil, err
	}
	var scopes struct {
		Scopes []struct {
			Name               string `json:"name"`
			VariablesReference int    `json:"variablesReference"`
		} `json:"scopes"`
	}
	if err := json.Unmarshal(raw, &scopes); err != nil {
		return nil, err
	}
	out := []Batch{}
	for _, sc := range scopes.Scopes {
		if sc.Name != "Batches" {
			continue
		}
		bvs, err := c.variables(sc.VariablesReference)
		if err != nil {
			return nil, err
		}
		for _, bv := range bvs {
			batch := Batch{}
			mvs, err := c.variables(bv.VariablesReference)
			if err != nil {
				return nil, err
			}
			for _, mv := range mvs {
				m, err := c.message(mv.VariablesReference)
				if err != nil {
					return nil, err
				}
				batch = append(batch, m)
			}
			out = append(out, batch)
		}
	}
	return out, nil
}

func (c *client) message(ref int) (Message, error) {
	m := Message{Metadata: map[string]Meta{}}
	vs, err := c.variables(ref)
	if err != nil {
		return m, err
	}
	for _, v := range vs {
		switch v.Name {
		case "content":
			m.Content = v.Value
		case "error":
			m.Error = v.Value
		case "metadata":
			if v.VariablesReference == 0 {
				continue
			}
			if err := c.metadata(v.VariablesReference, m.Metadata); err != nil {
				return m, err
			}
		}
	}
	return m, nil
}

// metadata reads the metadata keys under ref into into, descending into
// the internal group.
func (c *client) metadata(ref int, into map[string]Meta) error {
	vs, err := c.variables(ref)
	if err != nil {
		return err
	}
	for _, v := range vs {
		if v.Name == "internal" && v.VariablesReference != 0 {
			if err := c.metadata(v.VariablesReference, into); err != nil {
				return err
			}
			continue
		}
		into[v.Name] = Meta{Type: v.Type, Value: v.Value}
	}
	return nil
}

// pathKey finds a stop's user path from its frame: the child list's path,
// the stop's line, and whether it is the end-of-entry stop.
type pathKey struct {
	list string
	line int
	end  bool
}

// userPaths maps every stop position of the pipeline's rewrite to its user
// path. The rewrite numbers steppers densely from 0 and every stepper has
// at least one child (AD-16).
func userPaths(pipeline string) (map[pathKey]string, error) {
	raw, err := os.ReadFile(pipeline)
	if err != nil {
		return nil, err
	}
	plan, err := rewrite.Rewrite(raw)
	if err != nil {
		return nil, err
	}
	out := map[pathKey]string{}
	for id := 0; ; id++ {
		var stops []string
		var lines []int
		for i := 0; ; i++ {
			p, ok := plan.Position(id, i)
			if !ok {
				break
			}
			stops, lines = append(stops, p.UserPath), append(lines, p.Line)
		}
		if len(stops) == 0 {
			return out, nil
		}
		n := len(stops) - 1
		list := stops[n]
		for i := range n {
			if _, ok := out[pathKey{list, lines[i], false}]; !ok {
				out[pathKey{list, lines[i], false}] = stops[i]
			}
		}
		out[pathKey{list, lines[n], true}] = list
	}
}
