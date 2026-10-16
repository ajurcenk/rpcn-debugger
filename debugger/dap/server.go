package dap

import (
	"bufio"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"path/filepath"
	"sync"
	"unicode/utf8"

	"github.com/google/go-dap"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// Error ids of the error responses.
const (
	// errUnknownCommand is sent for a request the server doesn't handle.
	errUnknownCommand = 1
	// errRequestFailed is sent when the Session refuses a request.
	errRequestFailed = 2
)

// batchScopeRef is the variablesReference of the "Batch" scope. The one
// frame of this entry is frameId 1; both become invalid on resume (AD-17).
const (
	frameID       = 1
	batchScopeRef = 1
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
	defer s.sess.Disconnect()
	return s.run()
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

// launchArgs are the launch keys this entry reads. Other keys are ignored;
// the strict LaunchConfig arrives later (AD-12).
type launchArgs struct {
	Pipeline string `json:"pipeline"`
	Cwd      string `json:"cwd"`
}

// handle answers one request. It reports done once the session is over.
func (s *server) handle(req dap.RequestMessage) (done bool, err error) {
	r := req.GetRequest()
	switch q := req.(type) {
	case *dap.InitializeRequest:
		resp := &dap.InitializeResponse{Response: s.response(r)}
		resp.Body.SupportsConfigurationDoneRequest = true
		return false, s.send(resp)

	case *dap.LaunchRequest:
		var args launchArgs
		if len(q.Arguments) > 0 {
			if err := json.Unmarshal(q.Arguments, &args); err != nil {
				return false, s.sendFailure(r, fmt.Errorf("invalid launch arguments: %w", err))
			}
		}
		if err := s.sess.Launch(args.Pipeline, args.Cwd); err != nil {
			return false, s.sendFailure(r, err)
		}
		if err := s.send(&dap.LaunchResponse{Response: s.response(r)}); err != nil {
			return false, err
		}
		return false, s.send(&dap.InitializedEvent{Event: s.event("initialized")})

	case *dap.ConfigurationDoneRequest:
		// The response goes out before any event the run triggers.
		return false, s.respondLocked(r, s.sess.ConfigurationDone, func() dap.Message {
			return &dap.ConfigurationDoneResponse{Response: s.response(r)}
		})

	case *dap.ThreadsRequest:
		resp := &dap.ThreadsResponse{Response: s.response(r)}
		resp.Body.Threads = []dap.Thread{{Id: core.ThreadID, Name: "batch"}}
		return false, s.send(resp)

	case *dap.StackTraceRequest:
		resp := &dap.StackTraceResponse{Response: s.response(r)}
		resp.Body.StackFrames = []dap.StackFrame{}
		if f, ok := s.sess.Frame(); ok && q.Arguments.ThreadId == core.ThreadID {
			name := f.Path
			if name == "" {
				name = fmt.Sprintf("processor %d", f.ChildIndex)
			}
			resp.Body.StackFrames = []dap.StackFrame{{
				Id:     frameID,
				Name:   name,
				Source: &dap.Source{Name: filepath.Base(f.File), Path: f.File},
				Line:   f.Line,
				Column: 1,
			}}
		}
		resp.Body.TotalFrames = len(resp.Body.StackFrames)
		return false, s.send(resp)

	case *dap.ScopesRequest:
		resp := &dap.ScopesResponse{Response: s.response(r)}
		resp.Body.Scopes = []dap.Scope{}
		if f, ok := s.sess.Frame(); ok && q.Arguments.FrameId == frameID {
			resp.Body.Scopes = []dap.Scope{{
				Name:               "Batch",
				VariablesReference: batchScopeRef,
				IndexedVariables:   len(f.Messages),
			}}
		}
		return false, s.send(resp)

	case *dap.VariablesRequest:
		resp := &dap.VariablesResponse{Response: s.response(r)}
		resp.Body.Variables = []dap.Variable{}
		// The scope advertises indexed variables, so VS Code pages them with
		// start/count (count 0 means all) and may ask for named ones: none.
		a := q.Arguments
		if f, ok := s.sess.Frame(); ok && a.VariablesReference == batchScopeRef && a.Filter != "named" {
			start := min(max(a.Start, 0), len(f.Messages))
			end := len(f.Messages)
			if a.Count > 0 {
				end = min(start+a.Count, end)
			}
			for i := start; i < end; i++ {
				resp.Body.Variables = append(resp.Body.Variables, dap.Variable{
					Name:  fmt.Sprintf("[%d]", i),
					Value: contentText(f.Messages[i]),
				})
			}
		}
		return false, s.send(resp)

	case *dap.NextRequest:
		return false, s.respondLocked(r, s.sess.Next, func() dap.Message {
			return &dap.NextResponse{Response: s.response(r)}
		})

	case *dap.ContinueRequest:
		return false, s.respondLocked(r, s.sess.Continue, func() dap.Message {
			resp := &dap.ContinueResponse{Response: s.response(r)}
			resp.Body.AllThreadsContinued = true
			return resp
		})

	case *dap.DisconnectRequest:
		// Releases a stopped stepper and waits for the streams to stop.
		s.sess.Disconnect()
		return true, s.send(&dap.DisconnectResponse{Response: s.response(r)})

	default:
		return false, s.sendUnknown(r.Seq, r.Command)
	}
}

// respondLocked runs act and writes its response while holding the write
// lock, so the response precedes every event that act sets off.
func (s *server) respondLocked(r *dap.Request, act func() error, ok func() dap.Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := act(); err != nil {
		return s.writeLocked(s.failure(r, err))
	}
	return s.writeLocked(ok())
}

// contentText renders a message's content for the Variables view.
func contentText(b []byte) string {
	if utf8.Valid(b) {
		return string(b)
	}
	return fmt.Sprintf("%q", b)
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
		Id:     errUnknownCommand,
		Format: fmt.Sprintf("Request %q is not supported", command),
	}
	return s.send(resp)
}

func (s *server) sendFailure(r *dap.Request, err error) error {
	return s.send(s.failure(r, err))
}

func (s *server) failure(r *dap.Request, err error) dap.Message {
	s.log.Warn("DAP request failed", "command", r.Command, "seq", r.Seq, "error", err)
	resp := &dap.ErrorResponse{Response: s.response(r)}
	resp.Success = false
	resp.Message = err.Error()
	resp.Body.Error = &dap.ErrorMessage{Id: errRequestFailed, Format: err.Error(), ShowUser: true}
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

// frontend turns Session events into DAP events. A failed write is logged:
// the read loop sees the broken stream on its next request.
type frontend struct{ s *server }

func (f frontend) Stopped(reason string, threadID int) {
	ev := &dap.StoppedEvent{Event: f.s.event("stopped")}
	ev.Body.Reason = reason
	ev.Body.ThreadId = threadID
	f.emit(ev)
}

func (f frontend) Output(category, text string) {
	ev := &dap.OutputEvent{Event: f.s.event("output")}
	ev.Body.Category = category
	ev.Body.Output = text
	f.emit(ev)
}

func (f frontend) Terminated() {
	f.emit(&dap.TerminatedEvent{Event: f.s.event("terminated")})
}

func (f frontend) emit(msg dap.Message) {
	if err := f.s.send(msg); err != nil {
		f.s.log.Warn("sending DAP event failed", "error", err)
	}
}
