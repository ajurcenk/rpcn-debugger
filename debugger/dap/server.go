package dap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/google/go-dap"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// Serve runs the DAP server (U-DAP) on in and out until the client
// disconnects or in reaches EOF. It drives one core.Session, which reaches
// the pipeline through eng.
//
// One goroutine reads and handles requests; Session events arrive from other
// goroutines. Every write goes through one mutex, so only this server writes
// to out (AD-15). Serve returns nil after disconnect or on a clean EOF, and
// an error on a malformed frame or a failed write. Either way the session is
// disconnected first.
//
// log receives the server's own diagnostics. It must not send records back
// to this server as events: the server logs while it holds its write lock.
func Serve(in io.Reader, out io.Writer, log *slog.Logger, eng core.Engine, opts ...Option) error {
	s := &server{r: bufio.NewReader(in), w: out, log: log}
	s.sess = core.NewSession(eng, frontend{s})
	for _, opt := range opts {
		opt(s)
	}
	defer func() { _, _ = s.do(core.Terminate{}) }()
	return s.run()
}

// Option configures Serve.
type Option func(*server)

// OnSession calls fn with the Session once Serve has built it, before the
// first request is read. The debugger's log handler uses it to send engine
// logs as Output events through the Session (AD-15).
func OnSession(fn func(*core.Session)) Option {
	return func(s *server) { fn(s.sess) }
}

// do runs one protocol command on the Session.
func (s *server) do(cmd core.Command) (core.Result, error) {
	return s.sess.Do(context.Background(), cmd)
}

type server struct {
	r    *bufio.Reader
	w    io.Writer
	log  *slog.Logger
	sess *core.Session

	mu  sync.Mutex // guards seq and every write to w
	seq int
}

func (s *server) run() error {
	for {
		// EOF is clean only on a frame boundary; an EOF inside a frame is a
		// malformed frame.
		if _, err := s.r.Peek(1); err != nil {
			if errors.Is(err, io.EOF) {
				s.log.Info("DAP client closed the stream before disconnect")
				return nil
			}
			return fmt.Errorf("reading DAP message: %w", err)
		}
		msg, err := dap.ReadProtocolMessage(s.r)
		if err != nil {
			if errors.Is(err, io.EOF) {
				err = io.ErrUnexpectedEOF
			}
			var fieldErr *dap.DecodeProtocolMessageFieldError
			if errors.As(err, &fieldErr) && fieldErr.SubType == "Request" && fieldErr.FieldName == "command" {
				if err := s.sendUnknown(fieldErr.Seq, fieldErr.FieldValue); err != nil {
					return err
				}
				continue
			}
			return fmt.Errorf("reading DAP message: %w", err)
		}

		req, ok := msg.(dap.RequestMessage)
		if !ok {
			s.log.Warn("ignoring non-request DAP message", "seq", msg.GetSeq())
			continue
		}
		done, err := s.handle(req)
		if err != nil {
			return err
		}
		if done {
			return nil
		}
	}
}

// handle answers one request: it maps the request to a protocol command
// (AD-3) and the Session's answer to a response. It reports done once the
// session is over.
func (s *server) handle(req dap.RequestMessage) (done bool, err error) {
	r := req.GetRequest()
	switch q := req.(type) {
	case *dap.InitializeRequest:
		resp := &dap.InitializeResponse{Response: s.response(r)}
		resp.Body.SupportsConfigurationDoneRequest = true
		return false, s.send(resp)

	case *dap.LaunchRequest:
		cfg, err := decodeLaunch(q.Arguments)
		if err != nil {
			return false, s.sendFailure(r, err)
		}
		launch := core.Launch{
			Pipeline:    cfg.Pipeline,
			Cwd:         cfg.Cwd,
			MaxMessages: cfg.MaxMessages,
			Env:         cfg.Env,
			EnvFile:     cfg.EnvFile,
		}
		if _, err := s.do(launch); err != nil {
			return false, s.sendFailure(r, err)
		}
		if err := s.send(&dap.LaunchResponse{Response: s.response(r)}); err != nil {
			return false, err
		}
		return false, s.send(&dap.InitializedEvent{Event: s.event("initialized")})

	case *dap.SetBreakpointsRequest:
		return false, s.setBreakpoints(q)

	case *dap.SetExceptionBreakpointsRequest:
		if _, err := s.do(core.SetExceptionBreakpoints{Filters: q.Arguments.Filters}); err != nil {
			return false, s.sendFailure(r, err)
		}
		return false, s.send(&dap.SetExceptionBreakpointsResponse{Response: s.response(r)})

	case *dap.ConfigurationDoneRequest:
		// The response goes out before any event the run triggers.
		return false, s.respondLocked(r, core.ConfigurationDone{}, func() dap.Message {
			return &dap.ConfigurationDoneResponse{Response: s.response(r)}
		})

	case *dap.ThreadsRequest:
		resp := &dap.ThreadsResponse{Response: s.response(r)}
		resp.Body.Threads = []dap.Thread{}
		for _, t := range s.sess.Threads() {
			resp.Body.Threads = append(resp.Body.Threads, dap.Thread{Id: t.ID, Name: t.Name})
		}
		return false, s.send(resp)

	case *dap.StackTraceRequest:
		resp := &dap.StackTraceResponse{Response: s.response(r)}
		resp.Body.StackFrames = []dap.StackFrame{}
		if st, ok := s.sess.Stopped(); ok && q.Arguments.ThreadId == st.ThreadID {
			resp.Body.StackFrames = stackFrames(st)
		}
		resp.Body.TotalFrames = len(resp.Body.StackFrames)
		return false, s.send(resp)

	case *dap.ScopesRequest:
		resp := &dap.ScopesResponse{Response: s.response(r)}
		resp.Body.Scopes = []dap.Scope{}
		if st, ok := s.sess.Stopped(); ok {
			if w, ok := decodeFrameID(q.Arguments.FrameId); ok {
				if f, ok := frameOf(st, w); ok {
					resp.Body.Scopes = []dap.Scope{{
						Name:               "Batches",
						VariablesReference: batchesRef(w),
						IndexedVariables:   len(f.Snapshot.Batches),
					}}
				}
			}
		}
		return false, s.send(resp)

	case *dap.VariablesRequest:
		resp := &dap.VariablesResponse{Response: s.response(r)}
		resp.Body.Variables = []dap.Variable{}
		if st, ok := s.sess.Stopped(); ok {
			resp.Body.Variables = variables(st, q.Arguments)
		}
		return false, s.send(resp)

	// Every resume is process-wide (AD-9). Only the continue response has
	// an allThreadsContinued field; next, stepIn and stepOut resume all
	// threads too, since single-thread execution isn't advertised.
	case *dap.NextRequest:
		return false, s.respondLocked(r, core.Next{ThreadID: q.Arguments.ThreadId}, func() dap.Message {
			return &dap.NextResponse{Response: s.response(r)}
		})

	case *dap.ContinueRequest:
		return false, s.respondLocked(r, core.Continue{ThreadID: q.Arguments.ThreadId}, func() dap.Message {
			resp := &dap.ContinueResponse{Response: s.response(r)}
			resp.Body.AllThreadsContinued = true
			return resp
		})

	case *dap.StepInRequest:
		return false, s.respondLocked(r, core.StepIn{ThreadID: q.Arguments.ThreadId}, func() dap.Message {
			return &dap.StepInResponse{Response: s.response(r)}
		})

	case *dap.StepOutRequest:
		return false, s.respondLocked(r, core.StepOut{ThreadID: q.Arguments.ThreadId}, func() dap.Message {
			return &dap.StepOutResponse{Response: s.response(r)}
		})

	case *dap.PauseRequest:
		return false, s.respondLocked(r, core.Pause{ThreadID: q.Arguments.ThreadId}, func() dap.Message {
			return &dap.PauseResponse{Response: s.response(r)}
		})

	case *dap.EvaluateRequest:
		a := q.Arguments
		return false, s.respondLocked(r, core.Evaluate{FrameID: a.FrameId, Expression: a.Expression, Context: a.Context}, func() dap.Message {
			return &dap.EvaluateResponse{Response: s.response(r)}
		})

	case *dap.RestartRequest:
		return false, s.respondLocked(r, core.Restart{}, func() dap.Message {
			return &dap.RestartResponse{Response: s.response(r)}
		})

	case *dap.TerminateRequest:
		// Releases a stopped stepper and waits for the streams to stop; the
		// client follows up with disconnect.
		_, _ = s.do(core.Terminate{})
		return false, s.send(&dap.TerminateResponse{Response: s.response(r)})

	case *dap.DisconnectRequest:
		// Releases a stopped stepper and waits for the streams to stop.
		_, _ = s.do(core.Terminate{})
		return true, s.send(&dap.DisconnectResponse{Response: s.response(r)})

	default:
		return false, s.sendUnknown(r.Seq, r.Command)
	}
}

// setBreakpoints answers with the Session's results, in request order. Every
// breakpoint is unverified for now, with a message saying why.
func (s *server) setBreakpoints(q *dap.SetBreakpointsRequest) error {
	a := q.Arguments
	cmd := core.SetBreakpoints{File: a.Source.Path}
	if len(a.Breakpoints) > 0 {
		for _, b := range a.Breakpoints {
			cmd.Lines = append(cmd.Lines, b.Line)
			cmd.Conditions = append(cmd.Conditions, b.Condition)
		}
	} else {
		cmd.Lines = a.Lines
		cmd.Conditions = make([]string, len(a.Lines))
	}
	res, err := s.do(cmd)
	if err != nil {
		return s.sendFailure(&q.Request, err)
	}
	resp := &dap.SetBreakpointsResponse{Response: s.response(&q.Request)}
	resp.Body.Breakpoints = []dap.Breakpoint{}
	if br, ok := res.(core.SetBreakpointsResult); ok {
		src := a.Source
		for _, b := range br.Breakpoints {
			resp.Body.Breakpoints = append(resp.Body.Breakpoints, dap.Breakpoint{
				Verified: b.Verified,
				Message:  b.Message,
				Source:   &src,
				Line:     b.Line,
			})
		}
	}
	return s.send(resp)
}

// respondLocked runs cmd and writes its response while holding the write
// lock, so the response precedes every event that cmd sets off.
func (s *server) respondLocked(r *dap.Request, cmd core.Command, ok func() dap.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, err := s.do(cmd); err != nil {
		return s.writeLocked(s.failure(r, err))
	}
	return s.writeLocked(ok())
}

func (s *server) sendUnknown(requestSeq int, command string) error {
	s.log.Warn("unsupported DAP request", "command", command, "seq", requestSeq)
	resp := &dap.ErrorResponse{
		Response: dap.Response{
			ProtocolMessage: dap.ProtocolMessage{Type: "response"},
			RequestSeq:      requestSeq,
			Command:         command,
			Success:         false,
			Message:         fmt.Sprintf("unsupported request %q", command),
		},
	}
	resp.Body.Error = &dap.ErrorMessage{
		Id:     core.Number(core.CodeNotSupported),
		Format: fmt.Sprintf("Request %q is not supported", command),
	}
	return s.send(resp)
}

func (s *server) sendFailure(r *dap.Request, err error) error {
	return s.send(s.failure(r, err))
}

// failure maps err to an error response whose id is the registry number of
// its code (AD-3). An error that is no core.Error is RUN_FAILED.
func (s *server) failure(r *dap.Request, err error) dap.Message {
	var ce core.Error
	if !errors.As(err, &ce) {
		ce = core.Error{Code: core.CodeRunFailed, Message: err.Error()}
	}
	s.log.Warn("DAP request failed", "command", r.Command, "seq", r.Seq, "code", ce.Code, "error", err)
	resp := &dap.ErrorResponse{Response: s.response(r)}
	resp.Success = false
	resp.Message = err.Error()
	resp.Body.Error = &dap.ErrorMessage{Id: core.Number(ce.Code), Format: err.Error(), ShowUser: true}
	return resp
}

func (s *server) response(r *dap.Request) dap.Response {
	return dap.Response{
		ProtocolMessage: dap.ProtocolMessage{Type: "response"},
		RequestSeq:      r.Seq,
		Command:         r.Command,
		Success:         true,
	}
}

func (s *server) event(name string) dap.Event {
	return dap.Event{ProtocolMessage: dap.ProtocolMessage{Type: "event"}, Event: name}
}

// send stamps the next outgoing seq on msg and writes it to out.
func (s *server) send(msg dap.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.writeLocked(msg)
}

func (s *server) writeLocked(msg dap.Message) error {
	s.seq++
	switch m := msg.(type) {
	case dap.ResponseMessage:
		m.GetResponse().Seq = s.seq
	case dap.EventMessage:
		m.GetEvent().Seq = s.seq
	}
	if err := dap.WriteProtocolMessage(s.w, msg); err != nil {
		return fmt.Errorf("writing DAP message: %w", err)
	}
	return nil
}

// frontend turns Session events into DAP events (AD-3). A failed write is
// logged: the read loop sees the broken stream on its next request.
type frontend struct{ s *server }

// Emit implements core.Frontend. Events with no DAP rendering yet are
// dropped here; the entries that emit them add it.
func (f frontend) Emit(ev core.Event) {
	var msg dap.Message
	switch e := ev.(type) {
	case core.Paused:
		// Only the thread that stopped is stopped (AD-9).
		m := &dap.StoppedEvent{Event: f.s.event("stopped")}
		m.Body.Reason = e.Reason
		m.Body.ThreadId = e.ThreadID
		m.Body.AllThreadsStopped = false
		msg = m
	case core.Continued:
		m := &dap.ContinuedEvent{Event: f.s.event("continued")}
		m.Body.ThreadId = e.ThreadID
		m.Body.AllThreadsContinued = true
		msg = m
	case core.ThreadStarted:
		m := &dap.ThreadEvent{Event: f.s.event("thread")}
		m.Body.Reason, m.Body.ThreadId = "started", e.ThreadID
		msg = m
	case core.ThreadExited:
		m := &dap.ThreadEvent{Event: f.s.event("thread")}
		m.Body.Reason, m.Body.ThreadId = "exited", e.ThreadID
		msg = m
	case core.Output:
		m := &dap.OutputEvent{Event: f.s.event("output")}
		m.Body.Category = e.Category
		m.Body.Output = e.Text
		msg = m
	case core.MessagesSkipped:
		m := &dap.OutputEvent{Event: f.s.event("output")}
		m.Body.Category = "console"
		m.Body.Output = skippedText(e)
		if e.File != "" && e.Line > 0 {
			m.Body.Source = &dap.Source{Name: filepath.Base(e.File), Path: e.File}
			m.Body.Line = e.Line
		}
		msg = m
	case core.AttemptStarted:
		m := &dap.OutputEvent{Event: f.s.event("output")}
		m.Body.Category = "console"
		m.Body.Output = "retry " + e.UserPath + " " + e.RepeatLabel + "\n"
		msg = m
	case core.Terminated:
		msg = &dap.TerminatedEvent{Event: f.s.event("terminated")}
	default:
		// StepStarted and StepFinished (the repeat label shows in the stack
		// frame names instead), StatementStepped, BreakpointResolved,
		// Delivered.
		f.s.log.Debug("Session event without a DAP rendering", "event", fmt.Sprintf("%T", ev))
		return
	}
	if err := f.s.send(msg); err != nil {
		f.s.log.Warn("sending DAP event failed", "error", err)
	}
}

// skippedText tells the user which messages skipped a child, and why. A
// message of a try (or strict for_each) entry is named by its place in the
// parent's batch, as its [msg i/n] label is; other skips are a whole batch
// whose first message failed (the engine's rule, AD-7), named by index in
// the entry's batches as the Variables view does.
func skippedText(e core.MessagesSkipped) string {
	if len(e.Skipped) == 0 {
		return ""
	}
	rule := "try"
	if e.Skipped[0].Reason == core.SkipStrict {
		rule = "error_handling.strict"
	}
	at := e.UserPath
	if e.Line > 0 {
		at += fmt.Sprintf(" (line %d)", e.Line)
	}
	if e.Of > 0 {
		return fmt.Sprintf("Skipped message %d of %d at %s: it already failed, so %s runs no later processor on it\n",
			e.Skipped[0].Index+1, e.Of, at, rule)
	}
	idx := make([]string, len(e.Skipped))
	for i, sk := range e.Skipped {
		idx[i] = strconv.Itoa(sk.Index)
	}
	what := "message " + idx[0]
	if len(idx) > 1 {
		what = "messages " + strings.Join(idx, ", ")
	}
	return fmt.Sprintf("Skipped %s at %s: the first message of the batch already failed, so %s runs no later processor on the batch\n", what, at, rule)
}

// stackFrames renders the stopped thread's stack, innermost first (AD-17):
// each frame is named by its child list's user path, with its repeat label
// when it has one, and sits at the line of its own latest stop: a parent at
// the child that is running. Frames too
// deep to address are left out.
func stackFrames(st core.Stop) []dap.StackFrame {
	out := []dap.StackFrame{}
	for i, f := range st.Frames {
		id := frameID(st.ID, i)
		if id == 0 {
			break
		}
		pos := f.Snapshot.Position
		name := f.Name
		if f.RepeatLabel != "" {
			name += " " + f.RepeatLabel
		}
		out = append(out, dap.StackFrame{
			Id:     id,
			Name:   name,
			Source: &dap.Source{Name: filepath.Base(pos.File), Path: pos.File},
			Line:   pos.Line,
			Column: 1,
		})
	}
	return out
}
