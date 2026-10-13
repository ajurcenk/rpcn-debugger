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
			name:  "F5 launch",
			input: request(1, "initialize") + request(2, "launch") + request(3, "disconnect"),
			want: []want{
				{"response", "initialize", 1, true},
				{"response", "launch", 2, true},
				{"event", "terminated", 0, false},
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
			input: request(1, "threads") + request(2, "noSuchCommand") + request(3, "disconnect"),
			want: []want{
				{"response", "threads", 1, false},
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
			err := Serve(strings.NewReader(tc.input), &out, log)
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
	if !w.success && !strings.Contains(m.Message, w.command) {
		t.Errorf("message %d: error message %q does not name %q", i, m.Message, w.command)
	}
	if w.command == "initialize" {
		var caps map[string]any
		if err := json.Unmarshal(m.Body, &caps); err != nil {
			t.Fatalf("message %d: body %s: %v", i, m.Body, err)
		}
		if len(caps) != 0 {
			t.Errorf("initialize capabilities = %s, want empty", m.Body)
		}
	}
}
