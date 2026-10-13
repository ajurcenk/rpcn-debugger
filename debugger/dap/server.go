package dap

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"log/slog"

	"github.com/google/go-dap"
)

// errUnknownCommand is the id of the error message sent for a request the
// seed server doesn't handle.
const errUnknownCommand = 1

// Serve runs the DAP server seed (U-DAP) on in and out until the client
// disconnects or in reaches EOF. It answers initialize (with empty
// capabilities), launch (success followed by a terminated event) and
// disconnect, and sends an error response for any other request.
//
// The loop is serial: one goroutine reads, handles and writes. Only Serve
// writes to out (AD-15); diagnostics go to log. Serve returns nil after
// disconnect or on a clean EOF, and an error on a malformed frame or a failed
// write.
func Serve(in io.Reader, out io.Writer, log *slog.Logger) error {
	s := &server{r: bufio.NewReader(in), w: out, log: log}
	return s.run()
}

type server struct {
	r   *bufio.Reader
	w   io.Writer
	log *slog.Logger
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

// handle answers one request. It reports done once the session is over.
func (s *server) handle(req dap.RequestMessage) (done bool, err error) {
	r := req.GetRequest()
	switch req.(type) {
	case *dap.InitializeRequest:
		return false, s.send(&dap.InitializeResponse{Response: s.response(r)})
	case *dap.LaunchRequest:
		// Seed behaviour: the session ends at once. epic-tracer replaces this
		// with the real launch.
		if err := s.send(&dap.LaunchResponse{Response: s.response(r)}); err != nil {
			return false, err
		}
		return false, s.send(&dap.TerminatedEvent{Event: s.event("terminated")})
	case *dap.DisconnectRequest:
		return true, s.send(&dap.DisconnectResponse{Response: s.response(r)})
	default:
		return false, s.sendUnknown(r.Seq, r.Command)
	}
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
