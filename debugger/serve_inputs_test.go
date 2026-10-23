package debugger

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/connect/v4/public/components/aws"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// unknownInput is an input component with no input-table row.
const unknownInput = "rpcn_test_unknown"

// inputsEnv is the test env plus unknownInput.
func inputsEnv(t *testing.T) *service.Environment {
	t.Helper()
	env := service.NewEnvironment()
	// Any fields, so a stdin can sit below it.
	spec := service.NewConfigSpec().Field(service.NewAnyMapField("child").Optional())
	err := env.RegisterInput(unknownInput, spec, func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
		return nil, errors.New("never built: the launch is only checked")
	})
	if err != nil {
		t.Fatal(err)
	}
	err = env.RegisterInput(silentInput, service.NewConfigSpec(), func(*service.ParsedConfig, *service.Resources) (service.Input, error) {
		return silent{}, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	return env
}

// silentInput is an input that never yields a message.
const silentInput = "rpcn_test_silent"

type silent struct{}

func (silent) Connect(context.Context) error { return nil }

func (silent) Read(ctx context.Context) (*service.Message, service.AckFunc, error) {
	<-ctx.Done()
	return nil, nil, ctx.Err()
}

func (silent) Close(context.Context) error { return nil }

// The silent-input row through Serve: a capture that yields nothing within
// the (shortened) wait gets one note on the DAP output stream, and the
// session goes on waiting until it is ended.
func TestServeCaptureWaitNote(t *testing.T) {
	path := filepath.Join(t.TempDir(), "pipeline.yaml")
	if err := os.WriteFile(path, []byte("input:\n  "+silentInput+": {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := serveEnv(t, inputsEnv(t), withCaptureWait(20*time.Millisecond))
	c.send("initialize", map[string]any{})
	c.next("response", "initialize")
	c.send("launch", map[string]any{"pipeline": path, "allowSideEffects": true})
	notes := 0
	for configured := false; ; {
		m := c.nextAny()
		switch {
		case m.Type == "response" && m.Command == "launch":
			if !m.Success {
				t.Fatalf("launch = %+v", m)
			}
		case m.Type == "event" && m.Event == "initialized":
			c.send("configurationDone", map[string]any{})
		case m.Type == "response" && m.Command == "configurationDone":
			configured = true
		case m.Type == "event" && m.Event == "output":
			var b struct {
				Output string `json:"output"`
			}
			_ = json.Unmarshal(m.Body, &b)
			if strings.HasPrefix(b.Output, "Waiting for the input") {
				if !configured || b.Output != "Waiting for the input: nothing captured yet after 0.02 s.\n" {
					t.Errorf("note %q (configured %v)", b.Output, configured)
				}
				notes++
			}
		}
		if notes == 1 {
			break
		}
	}
	// No second note while the capture goes on waiting.
	time.Sleep(100 * time.Millisecond)
	c.send("disconnect", map[string]any{})
	for {
		m := c.nextAny()
		if m.Type == "event" && m.Event == "output" && strings.Contains(string(m.Body), "Waiting for the input") {
			t.Error("a second waiting note")
		}
		if m.Type == "response" && m.Command == "disconnect" {
			break
		}
	}
}

// launchOnly launches src with the extra launch args and returns the
// launch response and the output events sent before it. It never sends
// configurationDone, so nothing is captured.
func launchOnly(t *testing.T, src string, args map[string]any) (msg, []string, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipeline.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	c := serveEnv(t, inputsEnv(t))
	c.send("initialize", map[string]any{})
	c.next("response", "initialize")
	launch := map[string]any{"pipeline": path}
	for k, v := range args {
		launch[k] = v
	}
	c.send("launch", launch)
	var out []string
	var resp msg
	for {
		m := c.nextAny()
		if m.Type == "event" && m.Event == "output" {
			var b struct {
				Output string `json:"output"`
			}
			_ = json.Unmarshal(m.Body, &b)
			out = append(out, b.Output)
			continue
		}
		if m.Type == "response" && m.Command == "launch" {
			resp = m
			break
		}
		t.Fatalf("got %+v, want output events and the launch response", m)
	}
	if resp.Success {
		c.next("event", "initialized")
	}
	c.send("disconnect", map[string]any{})
	c.next("response", "disconnect")
	return resp, out, path
}

// nextAny returns the next message, whatever it is.
func (c *wire) nextAny() msg {
	c.t.Helper()
	select {
	case m, ok := <-c.msgs:
		if !ok {
			c.t.Fatal("stream closed")
		}
		return m
	case <-time.After(30 * time.Second):
		c.t.Fatal("timed out waiting for a message")
	}
	return msg{}
}

const (
	genInput = "input:\n  generate: {count: 1, interval: \"\", mapping: 'root = {}'}\n"
	sqsInput = "input:\n  aws_sqs: {url: http://localhost:1/q}\n"
)

// The E3 entry 4 matrix, through debugger.Serve: the Session refuses or
// warns on the input findings at launch.
func TestServeInputClasses(t *testing.T) {
	cases := map[string]struct {
		src  string
		args map[string]any
		// code is the launch error's code, "" for a launch that succeeds.
		code core.Code
		// text must all be in the error, or in the warnings.
		text     []string
		warnings int
	}{
		"safe": {src: genInput},
		"rejected stdin": {
			src: "input:\n  stdin: {}\n", code: core.CodeInputRejected,
			text: []string{"the stdin input at input, line 2", "stdin carries the debugger's DAP stream", "replay a saved capture instead, with the `replay` launch key"},
		},
		"rejected http_server": {
			src: "input:\n  http_server: {path: /post}\n", code: core.CodeInputRejected,
			text: []string{"the http_server input at input, line 2", "it is a server and waits for clients", "`replay` launch key"},
		},
		"side-effecting": {
			src: sqsInput, code: core.CodeSideEffectsNotAllowed,
			text: []string{"aws_sqs input at input, line 2", "receive count", `"allowSideEffects": true`},
		},
		"unknown": {
			src: "input:\n  " + unknownInput + ": {}\n", code: core.CodeSideEffectsNotAllowed,
			text: []string{unknownInput + " input at input, line 2", "treated as side-effecting"},
		},
		"opt-in": {
			src: sqsInput, args: map[string]any{"allowSideEffects": true}, warnings: 1,
			text: []string{"aws_sqs input at input (line 2) has side effects", "receive count"},
		},
		"rejected beats opt-in": {
			src: "input:\n  http_server: {path: /post}\n", args: map[string]any{"allowSideEffects": true}, code: core.CodeInputRejected,
		},
		"loosen": {
			src: sqsInput, args: map[string]any{"inputClasses": map[string]any{"aws_sqs": "replay-safe"}}, warnings: 1,
			text: []string{"inputClasses marks aws_sqs at input replay-safe; the debugger can't prove it"},
		},
		"tighten": {
			src: genInput, args: map[string]any{"inputClasses": map[string]any{"generate": "rejected"}}, code: core.CodeInputRejected,
			text: []string{"inputClasses marks generate rejected"},
		},
		"composite follows": {
			src:  "input:\n  broker:\n    inputs:\n      - generate: {count: 1, interval: \"\", mapping: 'root = {}'}\n      - aws_sqs: {url: http://localhost:1/q}\n",
			args: map[string]any{"inputClasses": map[string]any{"aws_sqs": "replay-safe"}}, warnings: 1,
			text: []string{"inputClasses marks aws_sqs at input.broker.inputs[1] replay-safe"},
		},
		"composite names its child": {
			src:  "input:\n  broker:\n    inputs:\n      - generate: {count: 1, interval: \"\", mapping: 'root = {}'}\n      - aws_sqs: {url: http://localhost:1/q}\n",
			code: core.CodeSideEffectsNotAllowed, text: []string{"aws_sqs input at input.broker.inputs[1], line 5"},
		},
		"bad override name": {
			src: genInput, args: map[string]any{"inputClasses": map[string]any{"no_such": "replay-safe"}}, code: core.CodeLaunchInvalid,
			text: []string{`"inputClasses.no_such"`, "not an input component"},
		},
		"stdin loosened": {
			src: "input:\n  stdin: {}\n", args: map[string]any{"inputClasses": map[string]any{"stdin": "replay-safe"}}, code: core.CodeLaunchInvalid,
			text: []string{`"inputClasses.stdin"`, "can only be rejected", "DAP stream"},
		},
		"stdin side-effecting with opt-in": {
			src:  "input:\n  stdin: {}\n",
			args: map[string]any{"allowSideEffects": true, "inputClasses": map[string]any{"stdin": "side-effecting"}}, code: core.CodeLaunchInvalid,
			text: []string{`"inputClasses.stdin"`, "can only be rejected"},
		},
		"stdin under an unknown composite": {
			src:  "input:\n  " + unknownInput + ":\n    child:\n      stdin: {}\n",
			args: map[string]any{"allowSideEffects": true}, code: core.CodeInputRejected,
			text: []string{"the stdin input at input." + unknownInput + ".child.stdin, line 4", "DAP stream"},
		},
		"bad override value": {
			src: sqsInput, args: map[string]any{"inputClasses": map[string]any{"aws_sqs": "maybe"}}, code: core.CodeLaunchInvalid,
			text: []string{`"inputClasses.aws_sqs"`, "one of replay-safe, side-effecting, rejected"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resp, out, path := launchOnly(t, tc.src, tc.args)
			if tc.code != "" {
				if resp.Success || errorID(resp) != core.Number(tc.code) {
					t.Fatalf("launch = %+v, want %s", resp, tc.code)
				}
				if tc.code != core.CodeLaunchInvalid && !strings.Contains(resp.Message, path) {
					t.Errorf("error %q does not name the file %s", resp.Message, path)
				}
				for _, w := range tc.text {
					if !strings.Contains(resp.Message, w) {
						t.Errorf("error %q does not name %q", resp.Message, w)
					}
				}
				return
			}
			if !resp.Success {
				t.Fatalf("launch = %+v, want success", resp)
			}
			if len(out) != tc.warnings {
				t.Fatalf("output = %q, want %d warnings", out, tc.warnings)
			}
			for _, w := range tc.text {
				if !strings.Contains(strings.Join(out, ""), w) {
					t.Errorf("warnings %q do not name %q", out, w)
				}
			}
		})
	}
}
