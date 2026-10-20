package dap

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
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
func Serve(in io.Reader, out io.Writer, log *slog.Logger, eng core.Engine) error {
	s := &server{r: bufio.NewReader(in), w: out, log: log}
	s.sess = core.NewSession(eng, frontend{s})
	defer func() { _, _ = s.do(core.Terminate{}) }()
	return s.run()
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
		resp.Body.Threads = []dap.Thread{{Id: core.ThreadID, Name: "batch"}}
		return false, s.send(resp)

	case *dap.StackTraceRequest:
		resp := &dap.StackTraceResponse{Response: s.response(r)}
		resp.Body.StackFrames = []dap.StackFrame{}
		if f, ok := s.sess.Frame(); ok && q.Arguments.ThreadId == f.Snapshot.Position.ThreadID {
			pos := f.Snapshot.Position
			name := pos.UserPath
			if name == "" {
				name = fmt.Sprintf("processor %d", pos.ChildIndex)
			}
			resp.Body.StackFrames = []dap.StackFrame{{
				Id:     f.ID,
				Name:   name,
				Source: &dap.Source{Name: filepath.Base(pos.File), Path: pos.File},
				Line:   pos.Line,
				Column: 1,
			}}
		}
		resp.Body.TotalFrames = len(resp.Body.StackFrames)
		return false, s.send(resp)

	case *dap.ScopesRequest:
		resp := &dap.ScopesResponse{Response: s.response(r)}
		resp.Body.Scopes = []dap.Scope{}
		if f, ok := s.sess.Frame(); ok && q.Arguments.FrameId == f.ID {
			resp.Body.Scopes = []dap.Scope{{
				Name:               "Batches",
				VariablesReference: batchesRef(f.ID),
				IndexedVariables:   len(f.Snapshot.Batches),
			}}
		}
		return false, s.send(resp)

	case *dap.VariablesRequest:
		resp := &dap.VariablesResponse{Response: s.response(r)}
		resp.Body.Variables = []dap.Variable{}
		if f, ok := s.sess.Frame(); ok {
			resp.Body.Variables = variables(f, q.Arguments)
		}
		return false, s.send(resp)

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
		m := &dap.StoppedEvent{Event: f.s.event("stopped")}
		m.Body.Reason = e.Reason
		m.Body.ThreadId = e.ThreadID
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
	case core.Terminated:
		msg = &dap.TerminatedEvent{Event: f.s.event("terminated")}
	default:
		// StepStarted, StepFinished, MessagesSkipped, AttemptStarted,
		// StatementStepped, BreakpointResolved, Delivered.
		f.s.log.Debug("Session event without a DAP rendering", "event", fmt.Sprintf("%T", ev))
		return
	}
	if err := f.s.send(msg); err != nil {
		f.s.log.Warn("sending DAP event failed", "error", err)
	}
}
