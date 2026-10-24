package dap

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"

	"github.com/google/go-dap"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

func TestDecodeLaunch(t *testing.T) {
	cases := map[string]struct {
		args string
		want LaunchConfig
	}{
		"defaults": {
			args: `{"type":"redpanda-connect","request":"launch","name":"Debug pipeline","pipeline":"/w/p.yaml"}`,
			want: LaunchConfig{Pipeline: "/w/p.yaml", MaxMessages: 10, Output: "dry-run"},
		},
		"vs code keys": {
			args: `{"type":"redpanda-connect","request":"launch","name":"n","pipeline":"p.yaml",
				"__sessionId":"abc","__configurationTarget":6,"presentation":{"hidden":true},
				"preLaunchTask":"build","postDebugTask":"clean","internalConsoleOptions":"openOnSessionStart",
				"debugServer":4711,"noDebug":false,"suppressMultipleSessionWarning":true}`,
			want: LaunchConfig{Pipeline: "p.yaml", MaxMessages: 10, Output: "dry-run"},
		},
		"every key": {
			args: `{"pipeline":"p.yaml","cwd":"/w","maxMessages":3,"env":{"A":"x","B":"${env:B}"},"envFile":".env",
				"keepConsumerGroup":true,"allowSideEffects":true,"inputClasses":{"aws_sqs":"replay-safe","generate":"rejected","nats":"side-effecting"},
				"output":"real"}`,
			want: LaunchConfig{Pipeline: "p.yaml", Cwd: "/w", MaxMessages: 3, Output: "real",
				Env: map[string]string{"A": "x", "B": "${env:B}"}, EnvFile: ".env", KeepConsumerGroup: true, AllowSideEffects: true,
				InputClasses: map[string]string{"aws_sqs": "replay-safe", "generate": "rejected", "nats": "side-effecting"}},
		},
		"no arguments": {args: ``, want: LaunchConfig{MaxMessages: 10, Output: "dry-run"}},
		"null":         {args: `null`, want: LaunchConfig{MaxMessages: 10, Output: "dry-run"}},
		"output none":  {args: `{"output":"none"}`, want: LaunchConfig{MaxMessages: 10, Output: "none"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			got, err := decodeLaunch(json.RawMessage(tc.args))
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tc.want) {
				t.Errorf("decodeLaunch = %+v, want %+v", got, tc.want)
			}
		})
	}
}

func TestDecodeLaunchInvalid(t *testing.T) {
	cases := map[string]struct {
		args string
		want []string
	}{
		"unknown key":     {`{"pipeline":"p.yaml","maxMesages":5}`, []string{`"maxMesages"`, "unknown key"}},
		"unknown keys":    {`{"pipeline":"p.yaml","zz":1,"aa":2}`, []string{`"aa", "zz"`, "unknown keys"}},
		"max zero":        {`{"pipeline":"p.yaml","maxMessages":0}`, []string{`"maxMessages"`, "at least 1"}},
		"max too big":     {`{"pipeline":"p.yaml","maxMessages":1025}`, []string{`"maxMessages"`, "at most 1024"}},
		"max string":      {`{"pipeline":"p.yaml","maxMessages":"5"}`, []string{`"maxMessages"`, "an integer", "not a string"}},
		"max fraction":    {`{"pipeline":"p.yaml","maxMessages":2.5}`, []string{`"maxMessages"`, "an integer"}},
		"max null":        {`{"pipeline":"p.yaml","maxMessages":null}`, []string{`"maxMessages"`, "null"}},
		"env number":      {`{"pipeline":"p.yaml","env":{"A":1}}`, []string{`"env.A"`, "a string", "a number"}},
		"env not object":  {`{"pipeline":"p.yaml","env":["A"]}`, []string{`"env"`, "an array"}},
		"empty envFile":   {`{"pipeline":"p.yaml","envFile":""}`, []string{`"envFile"`, "non-empty"}},
		"empty pipeline":  {`{"pipeline":""}`, []string{`"pipeline"`, "non-empty"}},
		"pipeline number": {`{"pipeline":3}`, []string{`"pipeline"`, "a string"}},
		"cwd number":      {`{"pipeline":"p.yaml","cwd":1}`, []string{`"cwd"`, "a string"}},
		"envFile bool":    {`{"pipeline":"p.yaml","envFile":true}`, []string{`"envFile"`, "a string"}},
		"not an object":   {`"not an object"`, []string{"launch arguments"}},
		"class maybe":     {`{"pipeline":"p.yaml","inputClasses":{"aws_sqs":"maybe"}}`, []string{`"inputClasses.aws_sqs"`, "one of replay-safe, side-effecting, rejected"}},
		"class unknown":   {`{"pipeline":"p.yaml","inputClasses":{"aws_sqs":"unknown"}}`, []string{`"inputClasses.aws_sqs"`, "one of"}},
		"class number":    {`{"pipeline":"p.yaml","inputClasses":{"aws_sqs":1}}`, []string{`"inputClasses.aws_sqs"`, "a string"}},
		"classes array":   {`{"pipeline":"p.yaml","inputClasses":["aws_sqs"]}`, []string{`"inputClasses"`, "an array"}},
		"allow string":    {`{"pipeline":"p.yaml","allowSideEffects":"yes"}`, []string{`"allowSideEffects"`, "a boolean", "a string"}},
		"allow null":      {`{"pipeline":"p.yaml","allowSideEffects":null}`, []string{`"allowSideEffects"`, "null"}},
		"keep string":     {`{"pipeline":"p.yaml","keepConsumerGroup":"yes"}`, []string{`"keepConsumerGroup"`, "a boolean", "a string"}},
		"output maybe":    {`{"pipeline":"p.yaml","output":"maybe"}`, []string{`"output"`, "one of dry-run, real, none"}},
		"output bool":     {`{"pipeline":"p.yaml","output":true}`, []string{`"output"`, "a string"}},
		"output null":     {`{"pipeline":"p.yaml","output":null}`, []string{`"output"`, "null"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			_, err := decodeLaunch(json.RawMessage(tc.args))
			if !core.HasCode(err, core.CodeLaunchInvalid) {
				t.Fatalf("decodeLaunch = %v, want LAUNCH_INVALID", err)
			}
			for _, w := range tc.want {
				if !strings.Contains(err.Error(), w) {
					t.Errorf("error %q does not name %s", err, w)
				}
			}
		})
	}
}

// Launch errors never echo values: env values may be substituted secrets
// (AD-22).
func TestDecodeLaunchErrorsHideValues(t *testing.T) {
	const secret = "s3cr3t-token"
	for name, args := range map[string]string{
		"env entry":  `{"pipeline":"p.yaml","env":{"OK":"` + secret + `","BAD":{"x":"` + secret + `"}}}`,
		"env array":  `{"pipeline":"p.yaml","env":["` + secret + `"]}`,
		"cwd object": `{"pipeline":"p.yaml","cwd":{"x":"` + secret + `"}}`,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := decodeLaunch(json.RawMessage(args))
			if !core.HasCode(err, core.CodeLaunchInvalid) {
				t.Fatalf("decodeLaunch = %v, want LAUNCH_INVALID", err)
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error %q leaks the value", err)
			}
		})
	}
}

func TestSchema(t *testing.T) {
	props := Schema()["properties"].(map[string]any)
	var keys []string
	for k := range props {
		keys = append(keys, k)
	}
	for _, k := range []string{"pipeline", "cwd", "maxMessages", "env", "envFile", "keepConsumerGroup", "allowSideEffects", "inputClasses", "output"} {
		p, ok := props[k].(map[string]any)
		if !ok {
			t.Errorf("schema has no %q (has %v)", k, keys)
			continue
		}
		if d, _ := p["description"].(string); d == "" {
			t.Errorf("%q has no description", k)
		}
	}
	if len(props) != 9 {
		t.Errorf("schema keys = %v, want exactly the nine launch keys", keys)
	}
	if out := props["output"].(map[string]any); out["type"] != "string" || out["default"] != "dry-run" ||
		!reflect.DeepEqual(out["enum"], []string{"dry-run", "real", "none"}) {
		t.Errorf("output = %v, want a string enum dry-run, real, none, default dry-run", out)
	}
	for _, k := range []string{"keepConsumerGroup", "allowSideEffects"} {
		if p := props[k].(map[string]any); p["type"] != "boolean" || p["default"] != false {
			t.Errorf("%s = %v, want a boolean, default false", k, p)
		}
	}
	ic := props["inputClasses"].(map[string]any)
	want := map[string]any{"type": "string", "enum": []string{"replay-safe", "side-effecting", "rejected"}}
	if ic["type"] != "object" || !reflect.DeepEqual(ic["additionalProperties"], want) {
		t.Errorf("inputClasses = %v, want an object of %v", ic, want)
	}
	mm := props["maxMessages"].(map[string]any)
	if mm["type"] != "integer" || mm["minimum"] != 1 || mm["maximum"] != maxRefMessages || mm["default"] != 10 {
		t.Errorf("maxMessages = %v, want an integer, minimum 1, maximum 1024, default 10", mm)
	}
	if p := props["pipeline"].(map[string]any); p["type"] != "string" || p["minLength"] != 1 {
		t.Errorf("pipeline = %v, want a non-empty string", p)
	}
	env := props["env"].(map[string]any)
	if env["type"] != "object" || !reflect.DeepEqual(env["additionalProperties"], map[string]any{"type": "string"}) {
		t.Errorf("env = %v, want an object of strings", env)
	}
	for _, k := range []string{"env", "envFile"} {
		d := props[k].(map[string]any)["description"].(string)
		if !strings.Contains(d, "secrets") {
			t.Errorf("%q description %q says nothing about secrets (AD-22)", k, d)
		}
	}
	if d := env["description"].(string); !strings.Contains(d, "${env:NAME}") {
		t.Errorf("env description %q does not mention ${env:NAME}", d)
	}
}

func TestInitialConfigurations(t *testing.T) {
	got := InitialConfigurations()
	want := []any{map[string]any{"type": "redpanda-connect", "request": "launch", "name": "Debug pipeline", "pipeline": "${file}"}}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("InitialConfigurations = %v, want %v", got, want)
	}
	// The generated entry is a valid launch.
	raw, err := json.Marshal(got[0])
	if err != nil {
		t.Fatal(err)
	}
	if _, err := decodeLaunch(raw); err != nil {
		t.Errorf("the initial configuration does not decode: %v", err)
	}
}

func TestLaunchUnknownKeyOverDAP(t *testing.T) {
	eng := newFakeEngine()
	c := startServer(t, eng)
	c.send("initialize", map[string]any{})
	c.response("initialize", true)
	c.send("launch", map[string]any{"pipeline": "/p/pipeline.yaml", "maxMesages": 5})
	resp := c.response("launch", false).(*dap.ErrorResponse)
	if resp.Body.Error == nil || resp.Body.Error.Id != core.Number(core.CodeLaunchInvalid) ||
		!strings.Contains(resp.Message, "maxMesages") {
		t.Errorf("launch error = %q / %+v, want LAUNCH_INVALID naming maxMesages", resp.Message, resp.Body.Error)
	}
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
}

func TestLaunchDefaultsReachTheEngine(t *testing.T) {
	eng := newFakeEngine()
	eng.opts = make(chan core.StartOptions, 1)
	c := startServer(t, eng)
	c.launch(map[string]any{"type": "redpanda-connect", "request": "launch", "name": "n",
		"pipeline": "/p/pipeline.yaml", "__sessionId": "s"})
	c.send("configurationDone", map[string]any{})
	c.response("configurationDone", true)
	got := <-eng.opts
	if got.MaxMessages != 10 || got.Pipeline != "/p/pipeline.yaml" {
		t.Errorf("Start options = %+v, want maxMessages 10 for /p/pipeline.yaml", got)
	}
	c.threadEvent("started")
	c.event("stopped")
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
}

// The inputClasses enum is the set core accepts.
func TestInputClassesEnum(t *testing.T) {
	var enum []string
	for _, f := range launchFields() {
		if f.key == "inputClasses" {
			enum = f.enum
		}
	}
	var want []string
	for _, c := range core.OverrideClasses {
		want = append(want, string(c))
	}
	if !reflect.DeepEqual(enum, want) {
		t.Errorf("inputClasses enum = %v, want core.OverrideClasses %v", enum, want)
	}
}

// The output enum is the set core accepts.
func TestOutputEnum(t *testing.T) {
	var enum []string
	for _, f := range launchFields() {
		if f.key == "output" {
			enum = f.enum
		}
	}
	var want []string
	for _, m := range core.OutputModes {
		want = append(want, string(m))
	}
	if !reflect.DeepEqual(enum, want) {
		t.Errorf("output enum = %v, want core.OutputModes %v", enum, want)
	}
}

func TestLaunchInputKeysReachTheSession(t *testing.T) {
	eng := newFakeEngine()
	eng.opts = make(chan core.StartOptions, 1)
	c := startServer(t, eng)
	c.launch(map[string]any{"pipeline": "/p/pipeline.yaml", "allowSideEffects": true, "keepConsumerGroup": true,
		"inputClasses": map[string]any{"aws_sqs": "replay-safe"}, "output": "real"})
	c.send("configurationDone", map[string]any{})
	c.response("configurationDone", true)
	got := <-eng.opts
	if !reflect.DeepEqual(got.InputClasses, map[string]core.InputClass{"aws_sqs": core.ClassReplaySafe}) {
		t.Errorf("Start options InputClasses = %v, want aws_sqs replay-safe", got.InputClasses)
	}
	if !got.KeepConsumerGroup {
		t.Error("Start options KeepConsumerGroup = false, want true")
	}
	if got.Output != core.OutputReal {
		t.Errorf("Start options Output = %q, want real", got.Output)
	}
	c.threadEvent("started")
	c.event("stopped")
	c.send("disconnect", map[string]any{})
	c.response("disconnect", true)
	if err := c.serveResult(); err != nil {
		t.Errorf("Serve = %v", err)
	}
}
