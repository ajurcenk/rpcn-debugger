package dap

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/google/go-dap"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// frame encodes a raw JSON body as one DAP frame.
func frame(body string) string {
	return fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(body), body)
}

func request(seq int, command string) string {
	return frame(fmt.Sprintf(`{"seq":%d,"type":"request","command":%q,"arguments":{}}`, seq, command))
}

// want describes one expected outgoing message.
type want struct {
	kind       string // "response" or "event"
	command    string // response command or event name
	requestSeq int
	success    bool
}

func TestServe(t *testing.T) {
	cases := []struct {
		name    string
		input   string
		want    []want
		wantErr bool
	}{
		{
			name:  "handshake",
			input: request(1, "initialize") + request(2, "disconnect"),
			want: []want{
				{"response", "initialize", 1, true},
				{"response", "disconnect", 2, true},
			},
		},
		{
			name:  "launch without pipeline",
			input: request(1, "initialize") + request(2, "launch") + request(3, "disconnect"),
			want: []want{
				{"response", "initialize", 1, true},
				{"response", "launch", 2, false},
				{"response", "disconnect", 3, true},
			},
		},
		{
			name:  "client vanishes",
			input: request(1, "initialize"),
			want:  []want{{"response", "initialize", 1, true}},
		},
		{
			name:  "empty stream",
			input: "",
		},
		{
			name:  "unknown request",
			input: request(1, "restart") + request(2, "noSuchCommand") + request(3, "disconnect"),
			want: []want{
				{"response", "restart", 1, false},
				{"response", "noSuchCommand", 2, false},
				{"response", "disconnect", 3, true},
			},
		},
		{
			name:    "bad content length",
			input:   "Content-Length: abc\r\n\r\n{}",
			wantErr: true,
		},
		{
			name:    "bad json",
			input:   frame(`{"seq":1,"type":`),
			wantErr: true,
		},
		{
			name:    "truncated body",
			input:   "Content-Length: 100\r\n\r\n{}",
			wantErr: true,
		},
		{name: "headerless text", input: "hello world", wantErr: true},
		{name: "header without delimiter", input: "Content-Length: 10", wantErr: true},
		{name: "header cut after CR", input: "Content-Length: 10\r", wantErr: true},
		{
			name:    "partial header after valid",
			input:   request(1, "initialize") + "Content-Len",
			want:    []want{{"response", "initialize", 1, true}},
			wantErr: true,
		},
		{
			name:    "malformed after valid",
			input:   request(1, "initialize") + frame("not json"),
			want:    []want{{"response", "initialize", 1, true}},
			wantErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var out, logs bytes.Buffer
			log := slog.New(slog.NewTextHandler(&logs, nil))
			err := Serve(strings.NewReader(tc.input), &out, log, newFakeEngine())
			if tc.wantErr != (err != nil) {
				t.Fatalf("Serve error = %v, wantErr %v", err, tc.wantErr)
			}

			r := bufio.NewReader(&out)
			for i, w := range tc.want {
				raw, err := dap.ReadBaseMessage(r)
				if err != nil {
					t.Fatalf("message %d: %v", i, err)
				}
				checkMessage(t, i, raw, w)
			}
			if rest, _ := io.ReadAll(r); len(rest) != 0 {
				t.Errorf("unexpected extra output: %q", rest)
			}
		})
	}
}

func checkMessage(t *testing.T, i int, raw []byte, w want) {
	t.Helper()
	var m struct {
		Seq        int             `json:"seq"`
		Type       string          `json:"type"`
		Command    string          `json:"command"`
		Event      string          `json:"event"`
		RequestSeq int             `json:"request_seq"`
		Success    bool            `json:"success"`
		Message    string          `json:"message"`
		Body       json.RawMessage `json:"body"`
	}
	if err := json.Unmarshal(raw, &m); err != nil {
		t.Fatalf("message %d: %v", i, err)
	}
	if m.Seq != i+1 {
		t.Errorf("message %d: seq = %d, want %d", i, m.Seq, i+1)
	}
	if m.Type != w.kind {
		t.Fatalf("message %d: type = %q, want %q (%s)", i, m.Type, w.kind, raw)
	}
	if w.kind == "event" {
		if m.Event != w.command {
			t.Errorf("message %d: event = %q, want %q", i, m.Event, w.command)
		}
		return
	}
	if m.Command != w.command || m.RequestSeq != w.requestSeq || m.Success != w.success {
		t.Errorf("message %d = %s, want command %q request_seq %d success %v",
			i, raw, w.command, w.requestSeq, w.success)
	}
	if !w.success && w.command != "launch" {
		var body struct {
			Error struct {
				ID int `json:"id"`
			} `json:"error"`
		}
		if err := json.Unmarshal(m.Body, &body); err != nil || body.Error.ID != core.Number(core.CodeNotSupported) {
			t.Errorf("message %d: error %s, want id NOT_SUPPORTED (%d)", i, m.Body, core.Number(core.CodeNotSupported))
		}
		if m.Message == "" {
			t.Errorf("message %d: no error message", i)
		}
	}
	if w.command == "initialize" {
		var caps map[string]any
		if err := json.Unmarshal(m.Body, &caps); err != nil {
			t.Fatalf("message %d: body %s: %v", i, m.Body, err)
		}
		if len(caps) != 1 || caps["supportsConfigurationDoneRequest"] != true {
			t.Errorf("initialize capabilities = %s, want only supportsConfigurationDoneRequest", m.Body)
		}
	}
}

// Repeat labels go into the stack frame names, and skipped messages and
// retry attempts reach the Debug Console as output events.
func TestLabelsAndStepEvents(t *testing.T) {
	st := core.Stop{ID: 1, ThreadID: 1, Frames: []core.Frame{
		{Name: "pipeline.processors[0].for_each", RepeatLabel: "[msg 2/3]"},
		{Name: "pipeline.processors"},
	}}
	frames := stackFrames(st)
	if len(frames) != 2 || frames[0].Name != "pipeline.processors[0].for_each [msg 2/3]" || frames[1].Name != "pipeline.processors" {
		t.Errorf("frames = %+v", frames)
	}

	var out bytes.Buffer
	s := &server{w: &out, log: slog.New(slog.DiscardHandler)}
	fe := frontend{s}
	fe.Emit(core.MessagesSkipped{
		StepperID: 1, ChildIndex: 1, UserPath: "pipeline.processors[0].try[1]", File: "/p/pipeline.yaml", Line: 9,
		Skipped: []core.Skipped{{Index: 1, Reason: core.SkipTry}}, Of: 3,
	})
	fe.Emit(core.MessagesSkipped{
		UserPath: "pipeline.processors[1]", Skipped: []core.Skipped{{Index: 1, Reason: core.SkipStrict}, {Index: 2, Reason: core.SkipStrict}},
	})
	fe.Emit(core.AttemptStarted{UserPath: "pipeline.processors[0].retry.processors", RepeatLabel: "[attempt 2/3]"})
	fe.Emit(core.StepStarted{RepeatLabel: "[msg 1/3]"}) // shown in frames only

	r := bufio.NewReader(&out)
	var got []*dap.OutputEvent
	for {
		m, err := dap.ReadProtocolMessage(r)
		if err != nil {
			break
		}
		got = append(got, m.(*dap.OutputEvent))
	}
	if len(got) != 3 {
		t.Fatalf("got %d output events, want 3", len(got))
	}
	if b := got[0].Body; b.Output != "Skipped message 2 of 3 at pipeline.processors[0].try[1] (line 9): it already failed, so try runs no later processor on it\n" ||
		b.Line != 9 || b.Source == nil || b.Source.Path != "/p/pipeline.yaml" || b.Category != "console" {
		t.Errorf("try skip = %+v", b)
	}
	if b := got[1].Body; b.Output != "Skipped messages 1, 2 at pipeline.processors[1]: the first message of the batch already failed, so error_handling.strict runs no later processor on the batch\n" || b.Source != nil {
		t.Errorf("strict skip = %+v", b)
	}
	if b := got[2].Body; b.Output != "retry pipeline.processors[0].retry.processors [attempt 2/3]\n" {
		t.Errorf("attempt = %+v", b)
	}
}
