package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/engine"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

var (
	samplePipeline = filepath.Join("..", "..", "testdata", "tracer", "pipeline.yaml")
	batch12        = filepath.Join("..", "..", "testdata", "tracer", "batch12.yaml")
	nestedPipeline = filepath.Join("..", "..", "testdata", "tracer", "nested.yaml")
)

func load(path string, opts engine.LoadOptions) (engine.Configs, core.LineMap, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return engine.Configs{}, nil, err
	}
	p, err := rewrite.Rewrite(raw, rewrite.Options{DebugGroup: opts.DebugGroup, InputClasses: opts.InputClasses, Output: opts.Output})
	if err != nil {
		return engine.Configs{}, nil, err
	}
	return engine.Configs{Input: p.Input, Resources: p.Resources, Steppers: p.Steppers, Output: p.Output, Warnings: p.Warnings, Strict: p.Strict, StrictExpr: p.StrictExpr, StrictLine: p.StrictLine, Delivery: p.Delivery, Inputs: p.Inputs}, p, nil
}

func writePipeline(t *testing.T, src string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "pipeline.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn}))
}

type arrival struct {
	stepperID, childIndex int
	batches               []core.Batch
	// stack is the frame stack in the Arrive's ctx, innermost first.
	stack []core.FrameRef
}

// arriver records arrivals and, when block is set, waits on it at each one.
// Its Enter pushes a frame like the Session's.
type arriver struct {
	mu       sync.Mutex
	frames   int
	exits    int
	got      []arrival
	captured [][2]int // kept, total
	block    chan struct{}
	seen     chan struct{}
	// delivered are the batches the sink reported, in order, with their
	// outcomes and write errors.
	delivered []core.Batch
	outcomes  []core.Outcome
	errs      []error
}

func (a *arriver) Deliver(_ context.Context, b core.Batch, o core.Outcome, err error) {
	a.mu.Lock()
	a.delivered = append(a.delivered, b)
	a.outcomes = append(a.outcomes, o)
	a.errs = append(a.errs, err)
	a.mu.Unlock()
}

func (a *arriver) Captured(kept, total int) {
	a.mu.Lock()
	a.captured = append(a.captured, [2]int{kept, total})
	a.mu.Unlock()
}

func (a *arriver) Enter(ctx context.Context, e core.Entry) (context.Context, func(any)) {
	a.mu.Lock()
	a.frames++
	ref := core.FrameRef{FrameID: a.frames, StepperID: e.StepperID, ThreadID: core.ThreadID}
	a.mu.Unlock()
	return core.PushFrame(ctx, ref), func(any) {
		a.mu.Lock()
		a.exits++
		a.mu.Unlock()
	}
}

func (a *arriver) Skip(context.Context, int, int, []core.Skipped, int) {}

func (a *arriver) Arrive(ctx context.Context, stepperID, childIndex int, batches []core.Batch) error {
	a.mu.Lock()
	a.got = append(a.got, arrival{stepperID, childIndex, batches, core.FrameStack(ctx)})
	a.mu.Unlock()
	if a.seen != nil {
		select {
		case a.seen <- struct{}{}:
		default:
		}
	}
	if a.block != nil {
		select {
		case <-a.block:
			return core.NewError(core.CodeSessionEnded, "ended")
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func standard() *service.Environment { return service.NewEnvironment() }

// Start runs Check's Prepared without loading again (AD-6), and without
// one (a caller that skipped Check) loads the pipeline itself. The loader
// gets the rewrite options from StartOptions.
func TestStartRunsPrepared(t *testing.T) {
	var loads []engine.LoadOptions
	counting := func(p string, o engine.LoadOptions) (engine.Configs, core.LineMap, error) {
		loads = append(loads, o)
		return load(p, o)
	}
	a := engine.New(standard(), testLogger(), counting)
	opts := core.StartOptions{Pipeline: samplePipeline, DebugGroup: "rpcn-debug-jane"}
	res, err := a.Check(opts)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := res.Prepared.(engine.Configs); !ok {
		t.Fatalf("Check Prepared = %T, want engine.Configs", res.Prepared)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	prepared := opts
	prepared.Prepared = res.Prepared
	arr := &arriver{}
	if err := a.Start(ctx, prepared, arr); err != nil {
		t.Fatal(err)
	}
	if len(loads) != 1 || loads[0].DebugGroup != "rpcn-debug-jane" {
		t.Errorf("loads = %+v, want only Check's, with the debug group", loads)
	}
	if len(arr.delivered) != 1 {
		t.Errorf("delivered %d batches from Prepared, want 1", len(arr.delivered))
	}

	arr = &arriver{}
	if err := a.Start(ctx, opts, arr); err != nil {
		t.Fatal(err)
	}
	if len(loads) != 2 || loads[1].DebugGroup != "rpcn-debug-jane" {
		t.Errorf("loads = %+v, want Start without Prepared to load with the debug group", loads)
	}
	if len(arr.delivered) != 1 {
		t.Errorf("delivered %d batches without Prepared, want 1", len(arr.delivered))
	}

	if err := a.Start(ctx, core.StartOptions{Pipeline: samplePipeline, Prepared: "configs"}, &arriver{}); err == nil {
		t.Error("Start ran a Prepared that is not engine.Configs")
	}
}

func TestSampleStepsAndDelivers(t *testing.T) {
	env := standard()
	a := engine.New(env, testLogger(), load)
	res, err := a.Check(core.StartOptions{Pipeline: samplePipeline})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("warnings = %q, want none", res.Warnings)
	}
	if pos, ok := res.Lines.Position(0, 0); !ok || pos.Line != 16 {
		t.Errorf("Position(0, 0) = %+v, %v; want line 16", pos, ok)
	}

	arr := &arriver{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx, core.StartOptions{Pipeline: samplePipeline}, arr); err != nil {
		t.Fatal(err)
	}
	if len(arr.got) != 2 || arr.got[0].stepperID != 0 || arr.got[0].childIndex != 0 ||
		len(arr.got[0].batches) != 1 || len(arr.got[0].batches[0].Messages) != 1 {
		t.Fatalf("arrivals = %+v, want one batch of one message before child 0, then the end stop", arr.got)
	}
	// The end stop observes the mapping's result.
	if end := arr.got[1]; end.stepperID != 0 || end.childIndex != 1 || len(end.batches) != 1 ||
		!strings.Contains(string(end.batches[0].Messages[0].Content.Raw), `"doubled":2`) {
		t.Errorf("end stop = %+v, want child 1 with the mapped message", end)
	}
	if pos, ok := res.Lines.Position(0, 1); !ok || pos.Line != 15 || pos.UserPath != "pipeline.processors" {
		t.Errorf("Position(0, 1) = %+v, %v; want pipeline.processors at line 15", pos, ok)
	}
	if arr.frames != 1 || arr.exits != 1 {
		t.Errorf("entries = %d, exits = %d; want 1 and 1", arr.frames, arr.exits)
	}
	m := arr.got[0].batches[0].Messages[0]
	var msg map[string]any
	if err := json.Unmarshal(m.Content.Raw, &msg); err != nil {
		t.Fatal(err)
	}
	if msg["greeting"] != "hello" || msg["doubled"] != nil {
		t.Errorf("message at the stop = %v, want the input before the mapping", msg)
	}
	meta := map[string]core.Meta{}
	for _, e := range m.Metadata {
		meta[e.Key] = e
	}
	if e := meta["source"]; e.Type != core.MetaString || e.Value != "tracer" || e.Internal {
		t.Errorf("source = %+v", e)
	}
	if e := meta["kafka_key"]; e.Type != core.MetaBytes || e.Value != "azE=" || e.Text == nil || *e.Text != "k1" {
		t.Errorf("kafka_key = %+v, want bytes k1", e)
	}
	if e := meta["__rpcn_internal"]; e.Value != "x" || !e.Internal {
		t.Errorf("__rpcn_internal = %+v, want internal", e)
	}

	// The sink reports the one delivered batch (AD-21), positioned at the
	// top-level output key.
	if res.Delivery.UserPath != "output" || res.Delivery.Line != 20 {
		t.Errorf("Delivery = %+v, want output at line 20", res.Delivery)
	}
	if len(arr.delivered) != 1 || len(arr.delivered[0].Messages) != 1 ||
		!strings.Contains(string(arr.delivered[0].Messages[0].Content.Raw), `"doubled":2`) {
		t.Fatalf("delivered = %+v, want the mapped message", arr.delivered)
	}
	dm := map[string]core.Meta{}
	for _, e := range arr.delivered[0].Messages[0].Metadata {
		dm[e.Key] = e
	}
	if dm["source"].Value != "tracer" || !dm["__rpcn_internal"].Internal {
		t.Errorf("delivered metadata = %+v", dm)
	}
}

// The sink reports every batch it receives, in order, after
// output.processors (AD-21), and each report is a deep copy (AD-19).
func TestSinkDeliversInOrder(t *testing.T) {
	path := writePipeline(t, `
input:
  generate:
    count: 3
    batch_size: 3
    interval: ""
    mapping: 'root.n = counter()'
pipeline:
  processors:
    - group_by:
        - check: 'this.n == 2'
    - mapping: 'root = this'
output:
  processors:
    - mapping: 'root = this.merge({"from_output": true})'
    - mapping: 'root = if this.n == 3 { throw("boom") } else { this }'
  drop: {}
`)
	a := engine.New(standard(), testLogger(), load)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	arr := &arriver{}
	if err := a.Start(ctx, core.StartOptions{Pipeline: path}, arr); err != nil {
		t.Fatal(err)
	}
	var got []string
	for _, b := range arr.delivered {
		var msgs []string
		for _, m := range b.Messages {
			msgs = append(msgs, fmt.Sprintf("%d:%s:%s", m.Index, m.Content.Raw, m.Error))
		}
		got = append(got, strings.Join(msgs, " | "))
	}
	// group_by puts the matching group first, then the rest.
	want := []string{
		`0:{"from_output":true,"n":2}:`,
		`0:{"from_output":true,"n":1}: | 1:{"from_output":true,"n":3}:failed assignment (line 1): boom`,
	}
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Errorf("delivered:\n%s\nwant:\n%s", strings.Join(got, "\n"), strings.Join(want, "\n"))
	}
}

// A run whose messages are all deleted delivers nothing.
func TestSinkNothingDelivered(t *testing.T) {
	path := writePipeline(t, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = 1'
pipeline:
  processors:
    - mapping: 'root = deleted()'
output:
  drop: {}
`)
	a := engine.New(standard(), testLogger(), load)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	arr := &arriver{}
	if err := a.Start(ctx, core.StartOptions{Pipeline: path}, arr); err != nil {
		t.Fatal(err)
	}
	if len(arr.delivered) != 0 {
		t.Errorf("delivered = %+v, want nothing", arr.delivered)
	}
}

// The stepper registered on a clone must not leak into the caller's env.
func TestEnvUntouched(t *testing.T) {
	env := standard()
	a := engine.New(env, testLogger(), load)
	if _, err := a.Check(core.StartOptions{Pipeline: samplePipeline}); err != nil {
		t.Fatal(err)
	}
	if err := env.NewStreamBuilder().AddProcessorYAML("rpcn_debug_stepper: {id: 0, processors: []}"); err == nil {
		t.Error("rpcn_debug_stepper is registered on the caller's env")
	}
}

func TestNothingCaptured(t *testing.T) {
	env := standard()
	if err := env.RegisterBatchInput("closed", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error) { return closedInput{}, nil }); err != nil {
		t.Fatal(err)
	}
	path := writePipeline(t, `
input:
  closed: {}
pipeline:
  processors:
    - mapping: root = this
output:
  drop: {}
`)
	a := engine.New(env, testLogger(), load)
	arr := &arriver{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx, core.StartOptions{Pipeline: path}, arr); !core.HasCode(err, core.CodeNothingCaptured) {
		t.Fatalf("Start = %v, want NOTHING_CAPTURED", err)
	}
	if len(arr.got) != 0 {
		t.Errorf("arrivals = %+v, want none", arr.got)
	}
}

// closedInput is an input that closes before it produces anything.
type closedInput struct{}

func (closedInput) Connect(context.Context) error { return nil }
func (closedInput) ReadBatch(context.Context) (service.MessageBatch, service.AckFunc, error) {
	return nil, nil, service.ErrEndOfInput
}
func (closedInput) Close(context.Context) error { return nil }

// input.processors run in the step stream, not at capture (AD-4): a
// mapping that deletes the message there still captures it.
func TestInputProcessorsAreStepped(t *testing.T) {
	path := writePipeline(t, `
input:
  generate:
    count: 1
    interval: ""
    mapping: root = 1
  processors:
    - mapping: root = deleted()
pipeline:
  processors:
    - mapping: root = this
output:
  drop: {}
`)
	a := engine.New(standard(), testLogger(), load)
	arr := &arriver{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx, core.StartOptions{Pipeline: path}, arr); err != nil {
		t.Fatal(err)
	}
	// The input stepper stops before its child, then at its end stop with
	// no batches; the deleted message never reaches the pipeline stepper.
	if len(arr.got) != 2 || arr.got[0].stepperID != 0 || arr.got[0].childIndex != 0 ||
		arr.got[1].stepperID != 0 || arr.got[1].childIndex != 1 || len(arr.got[1].batches) != 0 {
		t.Fatalf("arrivals = %+v, want input.processors[0], then its end stop with no batches", arr.got)
	}
}

// The nested demo runs input, pipeline and output processors, with a
// branch and a switch, through the engine: every stepper reports in step
// order, and the batch is delivered.
func TestNestedStepsAndDelivers(t *testing.T) {
	a := engine.New(standard(), testLogger(), load)
	res, err := a.Check(core.StartOptions{Pipeline: nestedPipeline})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Warnings) != 0 {
		t.Errorf("warnings = %q, want none", res.Warnings)
	}
	if pos, ok := res.Lines.Position(0, 0); !ok || pos.Line != 12 || pos.UserPath != "input.processors[0]" {
		t.Errorf("Position(0, 0) = %+v, %v; want input.processors[0] line 12", pos, ok)
	}

	arr := &arriver{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx, core.StartOptions{Pipeline: nestedPipeline}, arr); err != nil {
		t.Fatal(err)
	}
	// Ids: 0 input, 1 pipeline, 2 branch, 3 and 4 the switch cases, 5
	// output. Each entry ends with its end stop (childIndex = n), and an
	// entry nested in a parent processor carries its parent's frame.
	want := [][3]int{
		{0, 0, 1}, {0, 1, 1},
		{1, 0, 1}, {2, 0, 2}, {2, 1, 2}, {1, 1, 1}, {3, 0, 2}, {3, 1, 2}, {1, 2, 1},
		{5, 0, 1}, {5, 1, 1},
	}
	var got [][3]int
	for _, g := range arr.got {
		got = append(got, [3]int{g.stepperID, g.childIndex, len(g.stack)})
	}
	if len(got) != len(want) {
		t.Fatalf("arrivals = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("arrivals = %v, want %v", got, want)
		}
	}
	// At the branch's child, the stack is [branch, pipeline].
	if st := arr.got[3].stack; st[0].StepperID != 2 || st[1].StepperID != 1 {
		t.Errorf("stack at the branch's child = %+v, want stepper 2 inside stepper 1", st)
	}
	if arr.frames != 5 || arr.exits != 5 {
		t.Errorf("entries = %d, exits = %d; want 5 and 5", arr.frames, arr.exits)
	}
	last := arr.got[len(arr.got)-2].batches
	if len(last) != 1 || len(last[0].Messages) != 1 {
		t.Fatalf("batches before output.processors[0] = %+v", last)
	}
	var msg map[string]any
	if err := json.Unmarshal(last[0].Messages[0].Content.Raw, &msg); err != nil {
		t.Fatal(err)
	}
	if msg["from_input"] != true || msg["branched"] != float64(10) || msg["case"] != "one" {
		t.Errorf("message before output.processors[0] = %v", msg)
	}
}

// Every rewrite matrix pipeline that should build does build and deliver:
// aliases, opaque parents and processor_resources included.
func TestRewrittenConfigsBuild(t *testing.T) {
	for _, name := range []string{"nested", "alias", "concurrent", "processor-resources", "resource-concurrent", "strict", "top-level", "dropped", "stdout", "merge", "composite", "unknown-taint", "strict-parents"} {
		t.Run(name, func(t *testing.T) {
			a := engine.New(standard(), testLogger(), load)
			path := filepath.Join("..", "rewrite", "testdata", name+".yaml")
			if _, err := a.Check(core.StartOptions{Pipeline: path}); err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := a.Start(ctx, core.StartOptions{Pipeline: path}, &arriver{}); err != nil {
				t.Fatal(err)
			}
		})
	}
}

// error_handling.strict carries over to the step stream (AD-4(b)): there a
// message that fails a processor is rejected instead of delivered.
func TestStrictStepStream(t *testing.T) {
	for _, strict := range []bool{false, true} {
		t.Run(fmt.Sprint(strict), func(t *testing.T) {
			path := writePipeline(t, fmt.Sprintf(`
input:
  generate: {count: 1, interval: "", mapping: 'root = {}'}
pipeline:
  processors:
    - mapping: 'root = throw("boom")'
error_handling:
  strict: %v
output:
  drop: {}
`, strict))
			a := engine.New(standard(), testLogger(), load)
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			err := a.Start(ctx, core.StartOptions{Pipeline: path}, &arriver{})
			if strict && err == nil {
				t.Error("strict: the failed message was delivered")
			}
			if !strict && err != nil {
				t.Errorf("not strict: Start = %v", err)
			}
		})
	}
}

func TestCheckErrors(t *testing.T) {
	a := engine.New(standard(), testLogger(), load)
	if _, err := a.Check(core.StartOptions{Pipeline: filepath.Join(t.TempDir(), "missing.yaml")}); err == nil {
		t.Error("Check of a missing file succeeded")
	}
	bad := writePipeline(t, "input:\n  generate: {}\npipeline:\n  processors:\n    - no_such_processor: {}\n")
	if _, err := a.Check(core.StartOptions{Pipeline: bad}); err == nil {
		t.Error("Check of an unknown processor succeeded")
	}
	badInput := writePipeline(t, "input:\n  no_such_input: {}\n")
	if _, err := a.Check(core.StartOptions{Pipeline: badInput}); err == nil {
		t.Error("Check of an unknown input succeeded")
	}
}

// Ending the session while stopped delivers nothing and stops the streams.
func TestEndWhileStopped(t *testing.T) {
	a := engine.New(standard(), testLogger(), load)
	arr := &arriver{block: make(chan struct{}), seen: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() { done <- a.Start(context.Background(), core.StartOptions{Pipeline: samplePipeline}, arr) }()
	select {
	case <-arr.seen:
	case <-time.After(30 * time.Second):
		t.Fatal("no arrival")
	}
	close(arr.block)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Start = %v", err)
		}
	case <-time.After(30 * time.Second):
		t.Fatal("Start did not return")
	}
}

// ackProbe is an input that emits one batch and records every ack result.
type ackProbe struct {
	mu   sync.Mutex
	sent bool
	acks []error
}

func (p *ackProbe) Connect(context.Context) error { return nil }

func (p *ackProbe) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	p.mu.Lock()
	sent := p.sent
	p.sent = true
	p.mu.Unlock()
	if sent {
		<-ctx.Done()
		return nil, nil, ctx.Err()
	}
	return service.MessageBatch{service.NewMessage([]byte(`{"n":1}`))},
		func(_ context.Context, err error) error {
			p.mu.Lock()
			defer p.mu.Unlock()
			p.acks = append(p.acks, err)
			return nil
		}, nil
}

func (p *ackProbe) Close(context.Context) error { return nil }

// The captured batch is nacked: the consumer returns an error and no ack
// reaches the input (AD-4).
func TestCaptureNeverAcks(t *testing.T) {
	probe := &ackProbe{}
	env := standard()
	if err := env.RegisterBatchInput("ack_probe", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchInput, error) { return probe, nil }); err != nil {
		t.Fatal(err)
	}
	path := writePipeline(t, "input:\n  ack_probe: {}\npipeline:\n  processors:\n    - mapping: root.m = 1\noutput:\n  drop: {}\n")
	a := engine.New(env, testLogger(), load)
	arr := &arriver{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx, core.StartOptions{Pipeline: path}, arr); err != nil {
		t.Fatal(err)
	}
	if len(arr.got) != 2 {
		t.Fatalf("arrivals = %d, want 2 (child 0 and the end stop)", len(arr.got))
	}
	probe.mu.Lock()
	defer probe.mu.Unlock()
	if len(probe.acks) == 0 {
		t.Fatal("the input saw no nack")
	}
	for i, err := range probe.acks {
		if err == nil {
			t.Errorf("ack %d reached the input without an error", i)
		}
	}
}

// probeCache counts how many times a cache resource is opened.
type probeCache struct{}

func (probeCache) Get(context.Context, string) ([]byte, error)               { return nil, service.ErrKeyNotFound }
func (probeCache) Set(context.Context, string, []byte, *time.Duration) error { return nil }
func (probeCache) Add(context.Context, string, []byte, *time.Duration) error { return nil }
func (probeCache) Delete(context.Context, string) error                      { return nil }
func (probeCache) Close(context.Context) error                               { return nil }

// Check validates without Build, so no resource is opened at launch.
func TestCheckOpensNoResources(t *testing.T) {
	env := standard()
	var mu sync.Mutex
	opened := 0
	if err := env.RegisterCache("probe_cache", service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.Cache, error) {
			mu.Lock()
			defer mu.Unlock()
			opened++
			return probeCache{}, nil
		}); err != nil {
		t.Fatal(err)
	}
	path := writePipeline(t, `
input:
  generate: {count: 1, interval: "", mapping: 'root = 1'}
pipeline:
  processors:
    - mapping: root = this
cache_resources:
  - label: c
    probe_cache: {}
output:
  drop: {}
`)
	a := engine.New(env, testLogger(), load)
	if _, err := a.Check(core.StartOptions{Pipeline: path}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if opened != 0 {
		t.Errorf("Check opened the cache resource %d times", opened)
	}
}

// The capture keeps the first maxMessages messages of the batch and reports
// the batch's full count; without a limit it keeps all of them.
func TestCaptureTruncatesToMaxMessages(t *testing.T) {
	for name, tc := range map[string]struct {
		max, kept int
	}{
		"default limit": {max: 10, kept: 10},
		"no limit":      {max: 0, kept: 12},
		"above batch":   {max: 20, kept: 12},
	} {
		t.Run(name, func(t *testing.T) {
			a := engine.New(standard(), testLogger(), load)
			arr := &arriver{}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := a.Start(ctx, core.StartOptions{Pipeline: batch12, MaxMessages: tc.max}, arr); err != nil {
				t.Fatal(err)
			}
			if len(arr.captured) != 1 || arr.captured[0] != [2]int{tc.kept, 12} {
				t.Errorf("Captured calls = %v, want one with %d of 12", arr.captured, tc.kept)
			}
			if len(arr.got) != 2 || len(arr.got[0].batches) != 1 {
				t.Fatalf("arrivals = %+v, want a stop with one batch, then the end stop", arr.got)
			}
			msgs := arr.got[0].batches[0].Messages
			if len(msgs) != tc.kept {
				t.Fatalf("messages at the stop = %d, want %d", len(msgs), tc.kept)
			}
			for i, m := range msgs {
				var v struct{ N int }
				if err := json.Unmarshal(m.Content.Raw, &v); err != nil {
					t.Fatal(err)
				}
				if v.N != i+1 {
					t.Errorf("message %d has n = %d, want %d (the first messages are kept, in order)", i, v.N, i+1)
				}
			}
		})
	}
}

// ${VAR} resolves through the session's lookup in Start: env over envFile
// over the process environment (AD-22), and a default when it is unset
// everywhere.
func TestEnvLookupPrecedence(t *testing.T) {
	path := writePipeline(t, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = {}'
pipeline:
  processors:
    - mapping: |
        root.a = "${RPCN_DEBUG_TEST_A}"
        root.b = "${RPCN_DEBUG_TEST_B:fallback}"
output:
  drop: {}
`)
	t.Setenv("RPCN_DEBUG_TEST_A", "p")
	for name, tc := range map[string]struct {
		file, env map[string]string
		want      string
	}{
		"env wins":      {map[string]string{"RPCN_DEBUG_TEST_A": "f"}, map[string]string{"RPCN_DEBUG_TEST_A": "e"}, "e"},
		"envFile next":  {map[string]string{"RPCN_DEBUG_TEST_A": "f"}, nil, "f"},
		"process last":  {nil, nil, "p"},
		"other env key": {nil, map[string]string{"OTHER": "x"}, "p"},
	} {
		t.Run(name, func(t *testing.T) {
			opts := core.StartOptions{Pipeline: path, Lookup: core.EnvLookup(tc.file, tc.env)}
			a := engine.New(standard(), testLogger(), load)
			if _, err := a.Check(opts); err != nil {
				t.Fatal(err)
			}
			arr := &arriver{}
			ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
			defer cancel()
			if err := a.Start(ctx, opts, arr); err != nil {
				t.Fatal(err)
			}
			end := arr.got[len(arr.got)-1]
			var msg map[string]any
			if err := json.Unmarshal(end.batches[0].Messages[0].Content.Raw, &msg); err != nil {
				t.Fatal(err)
			}
			if msg["a"] != tc.want || msg["b"] != "fallback" {
				t.Errorf("message = %v, want a = %s and b = fallback", msg, tc.want)
			}
		})
	}
}

// Check lints with the same lookup, so a ${VAR} the config needs at lint
// time comes from the launch's env.
func TestCheckSeesEnv(t *testing.T) {
	path := writePipeline(t, `
input:
  generate:
    count: 1
    interval: ""
    mapping: 'root = {}'
pipeline:
  processors:
    - ${RPCN_DEBUG_TEST_PROC}: 'root = this'
output:
  drop: {}
`)
	a := engine.New(standard(), testLogger(), load)
	if _, err := a.Check(core.StartOptions{Pipeline: path, Lookup: core.EnvLookup(nil, nil)}); err == nil {
		t.Error("Check passed with the processor name unset")
	}
	lookup := core.EnvLookup(nil, map[string]string{"RPCN_DEBUG_TEST_PROC": "mapping"})
	if _, err := a.Check(core.StartOptions{Pipeline: path, Lookup: lookup}); err != nil {
		t.Errorf("Check with env set = %v", err)
	}
}

// A config error in a stepper's children names the user's list, never a
// stepper (AD-6).
func TestCheckErrorNamesUserPath(t *testing.T) {
	path := writePipeline(t, `
input:
  generate: {count: 1, interval: "", mapping: 'root = {}'}
pipeline:
  processors:
    - nope_not_a_processor: {}
output:
  drop: {}
`)
	withTr := func(p string, opts engine.LoadOptions) (engine.Configs, core.LineMap, error) {
		c, l, err := load(p, opts)
		if err == nil {
			c.Translator = l.(rewrite.Plan)
		}
		return c, l, err
	}
	res, err := engine.New(standard(), testLogger(), withTr).Check(core.StartOptions{Pipeline: path})
	if err == nil {
		t.Fatal("Check passed")
	}
	if !strings.Contains(err.Error(), "pipeline.processors:") || strings.Contains(err.Error(), "stepper") {
		t.Errorf("Check error = %q, want it to name pipeline.processors and no stepper", err)
	}
	if res.Translator == nil {
		t.Error("a failed Check carries no translator")
	}
}

// Entry 2.14: under error_handling.strict, the output rejecting a message that
// failed a processing step ends the run with STRICT_REJECTED, not a delivery
// failure; without strict the same pipeline delivers the failed message.
func TestStrictRejection(t *testing.T) {
	src := `
input:
  generate: {count: 1, interval: "", mapping: 'root = {"n": 1}'}
pipeline:
  processors:
    - mapping: 'root = throw("boom")'
output:
  drop: {}
`
	for _, strict := range []bool{true, false} {
		body := src
		if strict {
			body += "error_handling:\n  strict: true\n"
		}
		path := writePipeline(t, body)
		a := engine.New(standard(), testLogger(), load)
		ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
		err := a.Start(ctx, core.StartOptions{Pipeline: path, Lookup: core.EnvLookup(nil, nil)}, &arriver{})
		cancel()
		switch {
		case strict && !core.HasCode(err, core.CodeStrictRejected):
			t.Errorf("strict: Start = %v, want STRICT_REJECTED", err)
		case !strict && err != nil:
			t.Errorf("not strict: Start = %v, want nil", err)
		}
	}
}

// S2: an interpolated error_handling.strict resolves through the run's
// lookup (AD-22) and drives the step stream and the STRICT_REJECTED gate,
// as the literal does. An unset variable without a default is false and a
// non-bool fails Check and Start with PIPELINE_INVALID at its line, as the
// stock stream builder does.
func TestStrictInterpolated(t *testing.T) {
	src := `
input:
  generate: {count: 1, interval: "", mapping: 'root = {"n": 1}'}
pipeline:
  processors:
    - mapping: 'root = throw("boom")'
error_handling:
  strict: %s
output:
  drop: {}
`
	const v = "RPCN_DEBUG_TEST_STRICT_S2"
	for name, tc := range map[string]struct {
		expr      string
		file, env map[string]string
		want      string // "rejected", "delivered" or "invalid"
	}{
		"true from env":       {"${" + v + "}", nil, map[string]string{v: "true"}, "rejected"},
		"false from env":      {"${" + v + "}", nil, map[string]string{v: "false"}, "delivered"},
		"true from envFile":   {"${" + v + "}", map[string]string{v: "true"}, nil, "rejected"},
		"default true":        {"${" + v + ":true}", nil, nil, "rejected"},
		"env beats default":   {"${" + v + ":true}", nil, map[string]string{v: "false"}, "delivered"},
		"unset":               {"${" + v + "}", nil, nil, "delivered"},
		"empty":               {"${" + v + "}", nil, map[string]string{v: ""}, "delivered"},
		"not a bool":          {"${" + v + "}", nil, map[string]string{v: "maybe"}, "invalid"},
		"quoted is a string":  {`"${` + v + `}"`, nil, map[string]string{v: "true"}, "invalid"},
		"unknown decode func": {"${" + v + "|nope}", nil, map[string]string{v: "true"}, "invalid"},
		"commented":           {"# c\n    ${" + v + "} # d", nil, map[string]string{v: "true"}, "rejected"},
	} {
		t.Run(name, func(t *testing.T) {
			path := writePipeline(t, fmt.Sprintf(src, tc.expr))
			opts := core.StartOptions{Pipeline: path, Lookup: core.EnvLookup(tc.file, tc.env)}
			a := engine.New(standard(), testLogger(), load)
			_, checkErr := a.Check(opts)
			arr := &arriver{}
			ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
			defer cancel()
			err := a.Start(ctx, opts, arr)
			switch tc.want {
			case "invalid":
				for what, e := range map[string]error{"Check": checkErr, "Start": err} {
					var ce core.Error
					if !errors.As(e, &ce) || ce.Code != core.CodePipelineInvalid || ce.UserPath != "error_handling.strict" || ce.Line != 8 {
						t.Errorf("%s = %v, want PIPELINE_INVALID at error_handling.strict line 8", what, e)
					}
				}
			case "rejected":
				if checkErr != nil {
					t.Fatalf("Check = %v", checkErr)
				}
				if !core.HasCode(err, core.CodeStrictRejected) || len(arr.delivered) != 0 {
					t.Errorf("Start = %v with %d deliveries, want STRICT_REJECTED and none", err, len(arr.delivered))
				}
			case "delivered":
				if checkErr != nil {
					t.Fatalf("Check = %v", checkErr)
				}
				if err != nil {
					t.Fatalf("Start = %v, want nil", err)
				}
				if len(arr.delivered) != 1 || len(arr.delivered[0].Messages) != 1 || arr.delivered[0].Messages[0].Error == "" {
					t.Errorf("delivered = %+v, want the failed message with its error", arr.delivered)
				}
			}
		})
	}
}

// Check returns the input findings, with the inputClasses overrides
// applied, and refuses a name that isn't an input component (E3 entry 4).
func TestCheckInputs(t *testing.T) {
	path := writePipeline(t, "input:\n  generate: {count: 1, interval: \"\", mapping: 'root = {}'}\n")
	a := engine.New(standard(), testLogger(), load)
	res, err := a.Check(core.StartOptions{Pipeline: path, InputClasses: map[string]core.InputClass{"generate": core.ClassRejected}})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Inputs) != 1 || res.Inputs[0].Component != "generate" || res.Inputs[0].Class != core.ClassRejected {
		t.Errorf("Inputs = %+v, want generate overridden to rejected", res.Inputs)
	}
	_, err = a.Check(core.StartOptions{Pipeline: path, InputClasses: map[string]core.InputClass{"no_such": core.ClassReplaySafe}})
	if !core.HasCode(err, core.CodeLaunchInvalid) || !strings.Contains(err.Error(), "inputClasses.no_such") {
		t.Errorf("Check = %v, want LAUNCH_INVALID naming inputClasses.no_such", err)
	}
}
