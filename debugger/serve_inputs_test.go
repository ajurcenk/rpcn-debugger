package debugger

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/io"
	_ "github.com/redpanda-data/connect/v4/public/components/aws"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka"
	_ "github.com/redpanda-data/connect/v4/public/components/kafka/enterprise"
	_ "github.com/redpanda-data/connect/v4/public/components/nats"
	_ "github.com/redpanda-data/connect/v4/public/components/postgresql"

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
	// franzInput has a production consumer group.
	franzInput = "input:\n  kafka_franz: {seed_brokers: [localhost:1], topics: [t], consumer_group: prod}\n"
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
		"rejected redpanda_common": {
			src: "input:\n  redpanda_common: {topics: [t], consumer_group: prod}\n", code: core.CodeInputRejected,
			text: []string{"the redpanda_common input at input, line 2", "shared client of the top-level redpanda block, which only rpk connect sets up"},
		},
		"rejected redpanda without seed_brokers": {
			src: "input:\n  redpanda: {topics: [t], consumer_group: prod}\n", code: core.CodeInputRejected,
			text: []string{"the redpanda input at input, line 2", "without seed_brokers it reads through the shared client"},
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
		// E3 entry 5: the group is overridden unless keepConsumerGroup keeps
		// it, which then needs allowSideEffects.
		"group overridden": {
			src: franzInput, warnings: 1,
			text: []string{`consumer group "prod" is replaced by the debug group "rpcn-debug-`},
		},
		"keep": {
			src: franzInput, args: map[string]any{"keepConsumerGroup": true}, code: core.CodeSideEffectsNotAllowed,
			text: []string{"kafka_franz input at input, line 2", `joins consumer group "prod"`, "members rebalance", `"allowSideEffects": true`},
		},
		"interpolated group": {
			src:  strings.Replace(franzInput, "consumer_group: prod", "consumer_group: '${GROUP:prod}'", 1),
			code: core.CodeSideEffectsNotAllowed,
			text: []string{"kafka_franz input at input, line 2", "which the debugger can't override", "members rebalance"},
		},
		"keep opt-in": {
			src: franzInput, args: map[string]any{"keepConsumerGroup": true, "allowSideEffects": true}, warnings: 1,
			text: []string{"kafka_franz input at input (line 2) has side effects", `joins consumer group "prod"`},
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

// The E3 entry 5 matrix, launched through debugger.Serve: the launch
// outcome and the warnings of each group-override row.
func TestServeGroupOverride(t *testing.T) {
	const rp = "{seed_brokers: [localhost:1], topics: [t], consumer_group: %s}"
	twoGroups := func(kind string) string {
		return "input:\n  " + kind + ":\n    inputs:\n" +
			"      - redpanda: " + fmt.Sprintf(rp, "a") + "\n      - redpanda: " + fmt.Sprintf(rp, "b") + "\n"
	}
	replaced := func(path, noun, from string, k int) string {
		return fmt.Sprintf(`%s %s %q is replaced by the debug group "rpcn-debug-`, path, noun, from) + "%s" + fmt.Sprintf(`-%d"`, k)
	}
	cases := map[string]struct {
		src string
		// code is the launch error's code, "" for a launch that succeeds.
		code core.Code
		// warnings are the expected warnings, in order, each a format with
		// one %s; text must be in the error.
		warnings []string
		text     []string
	}{
		"nested": {
			src: twoGroups("broker"),
			warnings: []string{
				replaced("input.broker.inputs[0].redpanda.consumer_group (line 4):", "consumer group", "a", 0),
				replaced("input.broker.inputs[1].redpanda.consumer_group (line 5):", "consumer group", "b", 1),
			},
		},
		"sequence": {
			src: twoGroups("sequence"),
			warnings: []string{
				replaced("input.sequence.inputs[0].redpanda.consumer_group (line 4):", "consumer group", "a", 0),
				replaced("input.sequence.inputs[1].redpanda.consumer_group (line 5):", "consumer group", "b", 1),
			},
		},
		"resource": {
			src:      "input:\n  resource: r\ninput_resources:\n  - label: r\n    kafka_franz: " + fmt.Sprintf(rp, "prod") + "\n",
			warnings: []string{replaced("input_resources.r.kafka_franz.consumer_group (line 5):", "consumer group", "prod", 0)},
		},
		"legacy kafka": {
			src:      "input:\n  kafka: {addresses: [localhost:1], topics: [t], consumer_group: prod}\n",
			warnings: []string{replaced("input.kafka.consumer_group (line 2):", "consumer group", "prod", 0)},
		},
		"nats queue": {
			src:      "input:\n  nats: {urls: [nats://localhost:1], subject: s, queue: workers}\n",
			warnings: []string{replaced("input.nats.queue (line 2):", "queue group", "workers", 0)},
		},
		"left alone": {
			src:  "input:\n  postgres_cdc: {dsn: postgres://localhost:1/db, slot_name: s, tables: [t], schema: public}\n",
			code: core.CodeSideEffectsNotAllowed,
			text: []string{"postgres_cdc input at input, line 2", "replication slot"},
		},
		"latest": {
			src: "input:\n  redpanda: {seed_brokers: [localhost:1], topics: [t], consumer_group: prod, start_offset: latest}\n",
			warnings: []string{
				replaced("input.redpanda.consumer_group (line 2):", "consumer group", "prod", 0),
				"input.redpanda.start_offset (line 2): start_offset: latest starts at the latest offset, so the capture waits for new records%s",
			},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resp, out, path := launchOnly(t, tc.src, nil)
			if tc.code != "" {
				if resp.Success || errorID(resp) != core.Number(tc.code) {
					t.Fatalf("launch = %+v, want %s", resp, tc.code)
				}
				for _, w := range append(tc.text, path) {
					if !strings.Contains(resp.Message, w) {
						t.Errorf("error %q does not name %q", resp.Message, w)
					}
				}
				return
			}
			if !resp.Success {
				t.Fatalf("launch = %+v, want success", resp)
			}
			if len(out) != len(tc.warnings) {
				t.Fatalf("output = %q, want %d warnings", out, len(tc.warnings))
			}
			for i, w := range tc.warnings {
				// The %s stands for the debug group's user part, or the
				// rest of the latest warning.
				pre, post, _ := strings.Cut(w, "%s")
				if !strings.HasPrefix(out[i], "Warning: "+pre) || !strings.Contains(out[i], post) {
					t.Errorf("warning %d = %q, want %q", i, out[i], w)
				}
			}
		})
	}
}

// An input resource named only at run time may reach any input resource,
// so with allowSideEffects every one of them still reads under a debug
// group and never acks (E3 retro R1).
func TestServeRuntimeResource(t *testing.T) {
	src := "input:\n  resource: ${IN:r}\ninput_resources:\n  - label: r\n" +
		"    kafka_franz: {seed_brokers: [localhost:1], topics: [t], consumer_group: prod, auto_replay_nacks: false}\n"
	resp, out, _ := launchOnly(t, src, map[string]any{"allowSideEffects": true})
	if !resp.Success {
		t.Fatalf("launch = %+v, want success", resp)
	}
	all := strings.Join(out, "")
	for _, w := range []string{
		`input_resources.r.kafka_franz.consumer_group (line 5): consumer group "prod" is replaced by the debug group "rpcn-debug-`,
		"input_resources.r.kafka_franz.auto_replay_nacks (line 5): auto_replay_nacks is set to true for the capture",
		"the input resource is only known at runtime",
	} {
		if !strings.Contains(all, w) {
			t.Errorf("output %q does not name %q", out, w)
		}
	}
}
