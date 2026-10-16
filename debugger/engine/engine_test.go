package engine_test

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
	"github.com/ajurcenk/rpcn-debugger/debugger/engine"
	"github.com/ajurcenk/rpcn-debugger/debugger/rewrite"
)

var samplePipeline = filepath.Join("..", "..", "testdata", "tracer", "pipeline.yaml")

func load(path string) (engine.Configs, core.LineMap, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return engine.Configs{}, nil, err
	}
	p, err := rewrite.Rewrite(raw)
	if err != nil {
		return engine.Configs{}, nil, err
	}
	return engine.Configs{Input: p.Input, Resources: p.Resources, Stepper: p.Stepper}, p, nil
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
	batch                 core.BatchCopy
}

// arriver records arrivals and, when block is set, waits on it at each one.
type arriver struct {
	mu    sync.Mutex
	got   []arrival
	block chan struct{}
	seen  chan struct{}
}

func (a *arriver) Arrive(ctx context.Context, stepperID, childIndex int, batch core.BatchCopy) error {
	a.mu.Lock()
	a.got = append(a.got, arrival{stepperID, childIndex, batch})
	a.mu.Unlock()
	if a.seen != nil {
		a.seen <- struct{}{}
	}
	if a.block != nil {
		select {
		case <-a.block:
			return core.ErrSessionEnded
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return nil
}

func standard() *service.Environment { return service.NewEnvironment() }

func TestSampleStepsAndDelivers(t *testing.T) {
	env := standard()
	a := engine.New(env, testLogger(), load)
	lines, err := a.Check(samplePipeline, "")
	if err != nil {
		t.Fatal(err)
	}
	if pos, ok := lines.Position(0, 0); !ok || pos.Line != 13 {
		t.Errorf("Position(0, 0) = %+v, %v; want line 13", pos, ok)
	}

	arr := &arriver{}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.Start(ctx, samplePipeline, "", arr); err != nil {
		t.Fatal(err)
	}
	if len(arr.got) != 1 || arr.got[0].stepperID != 0 || arr.got[0].childIndex != 0 || len(arr.got[0].batch) != 1 {
		t.Fatalf("arrivals = %+v, want one before child 0", arr.got)
	}
	var msg map[string]any
	if err := json.Unmarshal(arr.got[0].batch[0].Content, &msg); err != nil {
		t.Fatal(err)
	}
	if msg["greeting"] != "hello" || msg["doubled"] != nil {
		t.Errorf("message at the stop = %v, want the input before the mapping", msg)
	}
}

// The stepper registered on a clone must not leak into the caller's env.
func TestEnvUntouched(t *testing.T) {
	env := standard()
	a := engine.New(env, testLogger(), load)
	if _, err := a.Check(samplePipeline, ""); err != nil {
		t.Fatal(err)
	}
	if err := env.NewStreamBuilder().AddProcessorYAML("rpcn_debug_stepper: {id: 0, processors: []}"); err == nil {
		t.Error("rpcn_debug_stepper is registered on the caller's env")
	}
}

func TestNothingCaptured(t *testing.T) {
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
	if err := a.Start(ctx, path, "", arr); !errors.Is(err, core.ErrNothingCaptured) {
		t.Fatalf("Start = %v, want ErrNothingCaptured", err)
	}
	if len(arr.got) != 0 {
		t.Errorf("arrivals = %+v, want none", arr.got)
	}
}

func TestCheckErrors(t *testing.T) {
	a := engine.New(standard(), testLogger(), load)
	if _, err := a.Check(filepath.Join(t.TempDir(), "missing.yaml"), ""); err == nil {
		t.Error("Check of a missing file succeeded")
	}
	bad := writePipeline(t, "input:\n  generate: {}\npipeline:\n  processors:\n    - no_such_processor: {}\n")
	if _, err := a.Check(bad, ""); err == nil {
		t.Error("Check of an unknown processor succeeded")
	}
	badInput := writePipeline(t, "input:\n  no_such_input: {}\n")
	if _, err := a.Check(badInput, ""); err == nil {
		t.Error("Check of an unknown input succeeded")
	}
}

// Ending the session while stopped delivers nothing and stops the streams.
func TestEndWhileStopped(t *testing.T) {
	a := engine.New(standard(), testLogger(), load)
	arr := &arriver{block: make(chan struct{}), seen: make(chan struct{}, 1)}
	done := make(chan error, 1)
	go func() { done <- a.Start(context.Background(), samplePipeline, "", arr) }()
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
	if err := a.Start(ctx, path, "", arr); err != nil {
		t.Fatal(err)
	}
	if len(arr.got) != 1 {
		t.Fatalf("arrivals = %d, want 1", len(arr.got))
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
	if _, err := a.Check(path, ""); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if opened != 0 {
		t.Errorf("Check opened the cache resource %d times", opened)
	}
}
