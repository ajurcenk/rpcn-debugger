package core

import (
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"
)

var (
	sqs = InputFinding{UserPath: "input", Line: 2, Component: "aws_sqs", Class: ClassSideEffecting, Effect: "receives messages from the queue"}
	gen = InputFinding{UserPath: "input", Line: 2, Component: "generate", Class: ClassReplaySafe}
)

// The Session's AD-5 decision on the findings Check returns (E3 entry 4).
func TestLaunchInputDecision(t *testing.T) {
	httpServer := InputFinding{UserPath: "input.broker.inputs[1]", Line: 6, Component: "http_server", Class: ClassRejected, Effect: "it is a server and waits for clients"}
	broker := InputFinding{UserPath: "input", Line: 2, Component: "broker", Class: ClassRejected, Effect: "through input.broker.inputs[1]: it is a server", Inherited: true}
	unknown := InputFinding{UserPath: "input", Line: 2, Component: "no_such", Class: ClassUnknown, Effect: "the debugger doesn't know what the no_such input does"}
	redis := InputFinding{UserPath: "input.broker.inputs[2]", Line: 9, Component: "redis_list", Class: ClassSideEffecting, Effect: "pops the items"}
	loosened := InputFinding{UserPath: "input", Line: 2, Component: "aws_sqs", Class: ClassReplaySafe, Override: ClassReplaySafe, Loosened: true}
	cases := map[string]struct {
		inputs   []InputFinding
		allow    bool
		code     Code     // "": the launch succeeds
		at       [2]any   // the error's UserPath and Line
		text     []string // in the error, or in the one warning each
		warnings int
	}{
		"safe":                  {inputs: []InputFinding{gen}},
		"rejected":              {inputs: []InputFinding{broker, gen, httpServer}, code: CodeInputRejected, at: [2]any{"input.broker.inputs[1]", 6}, text: []string{"http_server", "it is a server and waits for clients", "replay a saved capture instead", "`replay` launch key", "input.broker.inputs[1], line 6", "/p/pipeline.yaml"}},
		"rejected beats opt-in": {inputs: []InputFinding{httpServer}, allow: true, code: CodeInputRejected, at: [2]any{"input.broker.inputs[1]", 6}},
		"side-effecting":        {inputs: []InputFinding{sqs}, code: CodeSideEffectsNotAllowed, at: [2]any{"input", 2}, text: []string{"aws_sqs", "receives messages from the queue", `"allowSideEffects": true`}},
		"lists every one":       {inputs: []InputFinding{sqs, redis}, code: CodeSideEffectsNotAllowed, at: [2]any{"input", 2}, text: []string{"aws_sqs", "receives messages", "redis_list", "pops the items", "input.broker.inputs[2], line 9"}},
		"unknown":               {inputs: []InputFinding{unknown}, code: CodeSideEffectsNotAllowed, at: [2]any{"input", 2}, text: []string{"no_such"}},
		"opt-in":                {inputs: []InputFinding{sqs}, allow: true, warnings: 1, text: []string{"aws_sqs", "receives messages from the queue"}},
		"loosen":                {inputs: []InputFinding{loosened}, warnings: 1, text: []string{"inputClasses marks aws_sqs at input replay-safe; the debugger can't prove it"}},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			e := newEngine()
			e.inputs = tc.inputs
			rec := newRecorder()
			s := NewSession(e, rec)
			err := do(s, Launch{Pipeline: "/p/pipeline.yaml", AllowSideEffects: tc.allow})
			if tc.code != "" {
				var ce Error
				if !errors.As(err, &ce) || ce.Code != tc.code || ce.UserPath != tc.at[0] || ce.Line != tc.at[1] {
					t.Fatalf("launch = %#v, want %s at %v", err, tc.code, tc.at)
				}
				for _, w := range tc.text {
					if !strings.Contains(err.Error(), w) {
						t.Errorf("error %q does not name %q", err, w)
					}
				}
				if err := do(s, ConfigurationDone{}); !HasCode(err, CodeLaunchInvalid) {
					t.Errorf("configurationDone after a refused launch = %v, want LAUNCH_INVALID", err)
				}
				rec.none(t)
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			for range tc.warnings {
				ev := rec.next(t)
				if ev.kind != "output" || !strings.HasPrefix(ev.detail, "Warning: ") {
					t.Fatalf("event %+v, want a warning", ev)
				}
				for _, w := range tc.text {
					if !strings.Contains(ev.detail, w) {
						t.Errorf("warning %q does not name %q", ev.detail, w)
					}
				}
			}
			rec.none(t)
		})
	}
}

// A composite takes its child's class; the refusal names the child.
func TestLaunchInputDecisionNamesTheChild(t *testing.T) {
	broker := InputFinding{UserPath: "input", Line: 2, Component: "broker", Class: ClassSideEffecting, Effect: "through input.broker.inputs[0]: receives", Inherited: true}
	child := sqs
	child.UserPath, child.Line = "input.broker.inputs[0]", 4
	e := newEngine()
	e.inputs = []InputFinding{broker, child}
	err := do(NewSession(e, newRecorder()), Launch{Pipeline: "/p/pipeline.yaml"})
	var ce Error
	if !errors.As(err, &ce) || ce.Code != CodeSideEffectsNotAllowed || ce.UserPath != "input.broker.inputs[0]" || ce.Line != 4 ||
		strings.Contains(ce.Message, "broker input") {
		t.Errorf("launch = %#v, want SIDE_EFFECTS_NOT_ALLOWED at the aws_sqs child only", err)
	}

	// With the opt-in, one warning: the child's.
	rec := newRecorder()
	if err := do(NewSession(e, rec), Launch{Pipeline: "/p/pipeline.yaml", AllowSideEffects: true}); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); !strings.Contains(ev.detail, "aws_sqs input at input.broker.inputs[0]") {
		t.Errorf("warning %+v, want the aws_sqs child's", ev)
	}
	rec.none(t)
}

func TestLaunchInputClasses(t *testing.T) {
	e := newEngine()
	e.checked = make(chan StartOptions, 2)
	s := NewSession(e, newRecorder())
	err := do(s, Launch{Pipeline: "/p/pipeline.yaml", InputClasses: map[string]InputClass{"aws_sqs": "maybe"}})
	if !HasCode(err, CodeLaunchInvalid) || !strings.Contains(err.Error(), `"inputClasses.aws_sqs"`) {
		t.Fatalf("launch = %v, want LAUNCH_INVALID naming inputClasses.aws_sqs", err)
	}
	if len(e.checked) != 0 {
		t.Error("Check ran on an invalid inputClasses")
	}
	for _, c := range []Launch{
		{Pipeline: "/p/pipeline.yaml", InputClasses: map[string]InputClass{"stdin": ClassReplaySafe}},
		{Pipeline: "/p/pipeline.yaml", InputClasses: map[string]InputClass{"stdin": ClassSideEffecting}, AllowSideEffects: true},
		{Pipeline: "/p/pipeline.yaml", InputClasses: map[string]InputClass{"dynamic": ClassReplaySafe}},
		{Pipeline: "/p/pipeline.yaml", InputClasses: map[string]InputClass{"redpanda_migrator": ClassSideEffecting}},
	} {
		err := do(s, c)
		if !HasCode(err, CodeLaunchInvalid) || !strings.Contains(err.Error(), "can only be rejected") {
			t.Errorf("launch %v = %v, want LAUNCH_INVALID: it can only be rejected", c.InputClasses, err)
		}
	}
	if len(e.checked) != 0 {
		t.Error("Check ran on a loosened fixed input")
	}
	classes := map[string]InputClass{"aws_sqs": ClassReplaySafe, "generate": ClassRejected, "stdin": ClassRejected}
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml", InputClasses: classes}); err != nil {
		t.Fatal(err)
	}
	if got := <-e.checked; !reflect.DeepEqual(got.InputClasses, classes) {
		t.Errorf("Check InputClasses = %v, want %v", got.InputClasses, classes)
	}
	e.opts = make(chan StartOptions, 1)
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	if got := <-e.opts; !reflect.DeepEqual(got.InputClasses, classes) {
		t.Errorf("Start InputClasses = %v, want %v", got.InputClasses, classes)
	}
	s.terminate()
}

// A capture that delivers nothing within the delay gets one note, and the
// run then continues as before (AD-4).
func TestCaptureWaitNote(t *testing.T) {
	e := newEngine()
	e.hold = make(chan struct{})
	rec := newRecorder()
	s := NewSession(e, rec)
	s.captureWait = 20 * time.Millisecond
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev != (event{"output", "Waiting for the input: nothing captured yet after 0.02 s.\n"}) {
		t.Fatalf("event = %+v, want the waiting note", ev)
	}
	rec.none(t) // once only
	close(e.hold)
	if ev := rec.next(t); ev != (event{"stopped", "entry"}) {
		t.Fatalf("event = %+v, want stopped entry", ev)
	}
	if err := do(s, Continue{ThreadID: ThreadID}); err != nil {
		t.Fatal(err)
	}
	<-e.delivered
	if ev := rec.next(t); ev.kind != "terminated" {
		t.Fatalf("event = %+v, want terminated", ev)
	}
}

func TestCaptureWaitNoteNotWhenCaptured(t *testing.T) {
	e := newEngine()
	rec := newRecorder()
	s := NewSession(e, rec)
	s.captureWait = 100 * time.Millisecond
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml"}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	if ev := rec.next(t); ev != (event{"stopped", "entry"}) {
		t.Fatalf("event = %+v, want stopped entry", ev)
	}
	time.Sleep(2 * s.captureWait) // stopped, after the capture: no note
	rec.none(t)
	s.terminate()
}

// The default delay is AD-4's 5 s, and the note says so.
func TestCaptureWaitDefault(t *testing.T) {
	s := NewSession(newEngine(), newRecorder())
	if s.captureWait != 5*time.Second {
		t.Errorf("captureWait = %v, want 5s", s.captureWait)
	}
}
