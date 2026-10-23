package engine

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"reflect"
	"strings"
	"sync"
	"testing"

	"github.com/redpanda-data/benthos/v4/public/service"

	_ "github.com/redpanda-data/benthos/v4/public/components/pure"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// The stepper must give the same output as the engine on the same input
// (AD-7, AD-8). The references: service.ExecuteProcessors (ExecuteAll) for
// mode all, a port of benthos v4.84.0's ExecuteTryAll for mode try and for
// strict (whose ExecuteAll skip is the same test), and a real try processor
// for single-message batches, which is how try feeds its list.

// skipCall is one Skip.
type skipCall struct {
	child   int
	of      int
	indices []int
	reason  string
}

// spyArriver records what a stepper reports. Its Enter keeps one call
// site per stepper id, like a parent frame's counter, and gives each entry
// Site.Messages = messages.
type spyArriver struct {
	mu        sync.Mutex
	messages  int
	sites     map[int]*core.Site
	labels    []string
	attempts  int
	skips     []skipCall
	arrives   []int // childIndex of each Arrive
	threads   []int // the thread on top of ctx at each Arrive
	delivered []core.Batch
	inner     core.Arriver
}

func (a *spyArriver) Deliver(ctx context.Context, b core.Batch) {
	a.mu.Lock()
	a.delivered = append(a.delivered, b)
	a.mu.Unlock()
	if a.inner != nil {
		a.inner.Deliver(ctx, b)
	}
}

func (a *spyArriver) Captured(int, int) {}

func (a *spyArriver) Enter(ctx context.Context, e core.Entry) (context.Context, func(any)) {
	if a.inner != nil {
		return a.inner.Enter(ctx, e)
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	if a.sites == nil {
		a.sites = map[int]*core.Site{}
	}
	site := a.sites[e.StepperID]
	if site == nil {
		site = &core.Site{}
		a.sites[e.StepperID] = site
	}
	label := ""
	if e.Label != nil {
		label = e.Label(core.Site{Entries: site.Entries, Messages: a.messages, Memo: site.Memo})
	}
	site.Entries++
	a.labels = append(a.labels, label)
	if e.Attempt {
		a.attempts++
	}
	ref := core.FrameRef{FrameID: len(a.labels), StepperID: e.StepperID, ThreadID: core.ThreadID, RepeatLabel: label}
	return core.PushFrame(ctx, ref), func(memo any) {
		a.mu.Lock()
		site.Memo = memo
		a.mu.Unlock()
	}
}

func (a *spyArriver) Skip(ctx context.Context, stepperID, childIndex int, skipped []core.Skipped, of int) {
	if len(skipped) == 0 {
		return
	}
	c := skipCall{child: childIndex, reason: skipped[0].Reason, of: of}
	for _, s := range skipped {
		c.indices = append(c.indices, s.Index)
	}
	a.mu.Lock()
	a.skips = append(a.skips, c)
	a.mu.Unlock()
}

func (a *spyArriver) Arrive(ctx context.Context, stepperID, childIndex int, batches []core.Batch) error {
	f, _, _ := core.TopFrame(ctx)
	a.mu.Lock()
	a.arrives = append(a.arrives, childIndex)
	a.threads = append(a.threads, f.ThreadID)
	a.mu.Unlock()
	if a.inner != nil {
		return a.inner.Arrive(ctx, stepperID, childIndex, batches)
	}
	return nil
}

// newTestStepper builds a stepper from its config fields and children.
func newTestStepper(t *testing.T, fields, children string, a core.Arriver, res *service.Resources) *stepper {
	t.Helper()
	conf, err := stepperSpec().ParseYAML("id: 0\n"+fields+"\nprocessors:\n"+children, service.NewEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	if res == nil {
		res = service.MockResources()
	}
	s, err := newStepper(conf, res, a)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = s.Close(context.Background()) })
	return s
}

// procs builds the processor list children (YAML list items).
func procs(t *testing.T, children string) []*service.OwnedProcessor {
	t.Helper()
	spec := service.NewConfigSpec().Field(service.NewProcessorListField("p"))
	conf, err := spec.ParseYAML("p:\n"+children, service.NewEnvironment())
	if err != nil {
		t.Fatal(err)
	}
	ps, err := conf.FieldProcessorList("p")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		for _, p := range ps {
			_ = p.Close(context.Background())
		}
	})
	return ps
}

// executeTryAll is benthos v4.84.0 processor.ExecuteTryAll on the public
// types (internal/component/processor/execute.go).
func executeTryAll(ctx context.Context, procs []*service.OwnedProcessor, msgs ...service.MessageBatch) ([]service.MessageBatch, error) {
	resultMsgs := make([]service.MessageBatch, len(msgs))
	copy(resultMsgs, msgs)
	for i := 0; len(resultMsgs) > 0 && i < len(procs); i++ {
		var nextResultMsgs []service.MessageBatch
		for _, m := range resultMsgs {
			if len(m) > 0 && m[0].GetError() != nil {
				nextResultMsgs = append(nextResultMsgs, m)
				continue
			}
			rMsgs, err := procs[i].ProcessBatch(ctx, m)
			if err != nil {
				return nil, err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			nextResultMsgs = append(nextResultMsgs, rMsgs...)
		}
		resultMsgs = nextResultMsgs
	}
	return resultMsgs, nil
}

// input is a batch of the messages {"n":i} for each i in ns.
func input(ns ...int) service.MessageBatch {
	b := service.MessageBatch{}
	for _, n := range ns {
		m := service.NewMessage(fmt.Appendf(nil, `{"n":%d}`, n))
		m.MetaSetMut("n", n)
		b = append(b, m)
	}
	return b
}

// state is batches as JSON: content, metadata, errors and boundaries.
func state(t *testing.T, batches []service.MessageBatch) string {
	t.Helper()
	raw, err := json.Marshal(snapshotBatches(batches))
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

const (
	throwOn = `
  - mapping: 'root = if this.n == %d { throw("boom %d") } else { this }'
`
	after = `
  - mapping: 'root = this.merge({"after": true})'
  - mutation: 'meta seen = "yes"'
`
)

// childLists are the children the mode tests run: one batch out, several
// batches out (split), and a failure on a batch's first message.
var childLists = map[string]string{
	"fail-second":     fmt.Sprintf(throwOn, 1, 1) + after,
	"fail-first":      fmt.Sprintf(throwOn, 0, 0) + after,
	"split-then-fail": "\n  - split: {size: 1}\n" + fmt.Sprintf(throwOn, 1, 1) + after,
	"split2-fail-3rd": "\n  - split: {size: 2}\n" + fmt.Sprintf(throwOn, 2, 2) + after,
	"none-fail":       after,
}

func TestModesMatchTheEngine(t *testing.T) {
	inputs := map[string][]int{"three": {0, 1, 2}, "first": {0}, "second": {1}}
	for name, children := range childLists {
		for in, ns := range inputs {
			for _, mode := range []struct {
				name, fields string
				ref          func(context.Context, []*service.OwnedProcessor, ...service.MessageBatch) ([]service.MessageBatch, error)
			}{
				{"all", "mode: all", service.ExecuteProcessors},
				{"strict", "mode: all\nstrict: true", executeTryAll},
				{"try", "mode: try", executeTryAll},
				{"try-strict", "mode: try\nstrict: true", executeTryAll},
			} {
				t.Run(name+"/"+in+"/"+mode.name, func(t *testing.T) {
					ctx := context.Background()
					want, err := mode.ref(ctx, procs(t, children), input(ns...))
					if err != nil {
						t.Fatal(err)
					}
					s := newTestStepper(t, mode.fields, children, &spyArriver{}, nil)
					got, err := s.ProcessBatch(ctx, input(ns...))
					if err != nil {
						t.Fatal(err)
					}
					if g, w := state(t, got), state(t, want); g != w {
						t.Errorf("stepper output\n%s\nwant (engine)\n%s", g, w)
					}
				})
			}
		}
	}
}

// try feeds its list one message per batch: the stepper under it must
// match the real try processor on each.
func TestTryMatchesTheTryProcessor(t *testing.T) {
	for name, children := range childLists {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			try := procs(t, "\n  - try:"+strings.ReplaceAll(children, "\n  ", "\n      "))
			s := newTestStepper(t, "mode: try", children, &spyArriver{}, nil)
			for n := range 3 {
				want, err := service.ExecuteProcessors(ctx, try, input(n))
				if err != nil {
					t.Fatal(err)
				}
				got, err := s.ProcessBatch(ctx, input(n))
				if err != nil {
					t.Fatal(err)
				}
				if g, w := state(t, got), state(t, want); g != w {
					t.Errorf("message %d: stepper output\n%s\nwant (try processor)\n%s", n, g, w)
				}
			}
		})
	}
}

// A try stepper reports the message it skips once, at the first child it
// skips, and doesn't stop before a child that runs on no batch.
func TestTrySkips(t *testing.T) {
	children := fmt.Sprintf(throwOn, 1, 1) + after
	a := &spyArriver{}
	s := newTestStepper(t, "mode: try", children, a, nil)
	if _, err := s.ProcessBatch(context.Background(), input(1)); err != nil {
		t.Fatal(err)
	}
	if want := []skipCall{{child: 1, indices: []int{0}, reason: core.SkipTry}}; !reflect.DeepEqual(a.skips, want) {
		t.Errorf("skips = %+v, want %+v", a.skips, want)
	}
	if want := []int{0, 3}; !reflect.DeepEqual(a.arrives, want) {
		t.Errorf("arrives at %v, want %v (child 0, then the end stop)", a.arrives, want)
	}

	// Several batches: only the failed one is skipped, by its index across
	// the batches; the stepper still stops before each child.
	a = &spyArriver{}
	s = newTestStepper(t, "mode: try", "\n  - split: {size: 1}\n"+children, a, nil)
	if _, err := s.ProcessBatch(context.Background(), input(0, 1, 2)); err != nil {
		t.Fatal(err)
	}
	if want := []skipCall{{child: 2, indices: []int{1}, reason: core.SkipTry}}; !reflect.DeepEqual(a.skips, want) {
		t.Errorf("skips = %+v, want %+v", a.skips, want)
	}
	if want := []int{0, 1, 2, 3, 4}; !reflect.DeepEqual(a.arrives, want) {
		t.Errorf("arrives at %v, want %v", a.arrives, want)
	}

	// Under try's msg label, the skip names the entry's message by its
	// place in the parent's batch.
	a = &spyArriver{messages: 3}
	s = newTestStepper(t, "mode: try\nrepeat: msg", children, a, nil)
	for n := range 3 {
		if _, err := s.ProcessBatch(context.Background(), input(n)); err != nil {
			t.Fatal(err)
		}
	}
	if want := []skipCall{{child: 1, of: 3, indices: []int{1}, reason: core.SkipTry}}; !reflect.DeepEqual(a.skips, want) {
		t.Errorf("msg skips = %+v, want %+v", a.skips, want)
	}

	// Mode all without strict skips nothing.
	a = &spyArriver{}
	s = newTestStepper(t, "mode: all", children, a, nil)
	if _, err := s.ProcessBatch(context.Background(), input(1)); err != nil {
		t.Fatal(err)
	}
	if len(a.skips) != 0 {
		t.Errorf("mode all skips = %+v, want none", a.skips)
	}

	// Strict reports its own reason.
	a = &spyArriver{}
	s = newTestStepper(t, "mode: all\nstrict: true", children, a, nil)
	if _, err := s.ProcessBatch(context.Background(), input(1)); err != nil {
		t.Fatal(err)
	}
	if want := []skipCall{{child: 1, indices: []int{0}, reason: core.SkipStrict}}; !reflect.DeepEqual(a.skips, want) {
		t.Errorf("strict skips = %+v, want %+v", a.skips, want)
	}
}

// Every Arrive gets a deep copy: changing the batch afterwards doesn't
// change what was reported (AD-19).
func TestArriveDeepCopies(t *testing.T) {
	var got []core.Batch
	a := &recordArriver{arrive: func(b []core.Batch) {
		if got == nil {
			got = b
		}
	}}
	s := newTestStepper(t, "mode: all", after, a, nil)
	in := input(1)
	if _, err := s.ProcessBatch(context.Background(), in); err != nil {
		t.Fatal(err)
	}
	in[0].SetBytes([]byte(`{"changed":true}`))
	in[0].MetaSetMut("n", "changed")
	if c := string(got[0].Messages[0].Content.Raw); c != `{"n":1}` {
		t.Errorf("snapshot content = %s, want the input as it was", c)
	}
	for _, m := range got[0].Messages[0].Metadata {
		if m.Key == "n" && m.Value != "1" {
			t.Errorf("snapshot metadata n = %s, want 1", m.Value)
		}
	}
}

type recordArriver struct {
	spyArriver
	arrive func([]core.Batch)
}

func (a *recordArriver) Arrive(ctx context.Context, stepperID, childIndex int, batches []core.Batch) error {
	a.arrive(batches)
	return nil
}

func TestRepeatLabels(t *testing.T) {
	ctx := context.Background()
	run := func(t *testing.T, s *stepper, ins ...service.MessageBatch) {
		t.Helper()
		for _, in := range ins {
			if _, err := s.ProcessBatch(ctx, in); err != nil {
				t.Fatal(err)
			}
		}
	}

	t.Run("msg", func(t *testing.T) {
		a := &spyArriver{messages: 3}
		run(t, newTestStepper(t, "repeat: msg", after, a, nil), input(0), input(1), input(2))
		if want := []string{"[msg 1/3]", "[msg 2/3]", "[msg 3/3]"}; !reflect.DeepEqual(a.labels, want) {
			t.Errorf("labels = %q, want %q", a.labels, want)
		}
	})
	t.Run("loop", func(t *testing.T) {
		a := &spyArriver{}
		run(t, newTestStepper(t, "repeat: loop", after, a, nil), input(0), input(0), input(0))
		if want := []string{"[loop 1]", "[loop 2]", "[loop 3]"}; !reflect.DeepEqual(a.labels, want) {
			t.Errorf("labels = %q, want %q", a.labels, want)
		}
	})
	t.Run("none", func(t *testing.T) {
		a := &spyArriver{messages: 3}
		run(t, newTestStepper(t, "mode: try", after, a, nil), input(0), input(1))
		if want := []string{"", ""}; !reflect.DeepEqual(a.labels, want) {
			t.Errorf("labels = %q, want %q", a.labels, want)
		}
	})
	t.Run("attempt", func(t *testing.T) {
		// Message 1 always fails: retry tries it max_attempts times, then
		// the next message starts again at attempt 1.
		a := &spyArriver{}
		s := newTestStepper(t, "repeat: attempt\nmax_attempts: 3", fmt.Sprintf(throwOn, 1, 1), a, nil)
		run(t, s, input(1), input(1), input(1), input(1), input(0), input(1))
		want := []string{"[attempt 1/3]", "[attempt 2/3]", "[attempt 3/3]", "[attempt 1/3]", "[attempt 1/3]", "[attempt 1/3]"}
		if !reflect.DeepEqual(a.labels, want) {
			t.Errorf("labels = %q, want %q", a.labels, want)
		}
		if a.attempts != len(want) {
			t.Errorf("attempts = %d, want %d", a.attempts, len(want))
		}
	})
	t.Run("attempt-unbounded", func(t *testing.T) {
		a := &spyArriver{}
		s := newTestStepper(t, "repeat: attempt", fmt.Sprintf(throwOn, 1, 1), a, nil)
		run(t, s, input(1), input(1), input(1), input(0))
		if want := []string{"[attempt 1]", "[attempt 2]", "[attempt 3]", "[attempt 1]"}; !reflect.DeepEqual(a.labels, want) {
			t.Errorf("labels = %q, want %q", a.labels, want)
		}
	})
}

// A nested stepper whose ctx has no frame logs CTX_LOST at Warn and runs
// on the root thread (AD-17).
func TestCtxLost(t *testing.T) {
	var buf bytes.Buffer
	log := service.NewLoggerFromSlog(slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug})))
	res := service.MockResources(service.MockResourcesOptUseLogger(log))
	sess := core.NewSession(nil, nopFrontend{})
	a := &spyArriver{inner: sess}
	s := newTestStepper(t, "parent: 3", after, a, res)
	out, err := s.ProcessBatch(context.Background(), input(1))
	if err != nil {
		t.Fatal(err)
	}
	if len(out) != 1 {
		t.Fatalf("output = %d batches, want 1", len(out))
	}
	logged := buf.String()
	if !strings.Contains(logged, "level=WARN") || !strings.Contains(logged, string(core.CodeCtxLost)) {
		t.Errorf("log = %q, want a WARN with CTX_LOST", logged)
	}
	for _, tid := range a.threads {
		if tid != core.ThreadID {
			t.Errorf("ran on thread %d, want the root thread", tid)
		}
	}
	if len(a.threads) == 0 {
		t.Error("no Arrive")
	}

	// A top-level stepper (parent -1) has no frame by design.
	buf.Reset()
	s = newTestStepper(t, "parent: -1", after, &spyArriver{}, res)
	if _, err := s.ProcessBatch(context.Background(), input(1)); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(buf.String(), string(core.CodeCtxLost)) {
		t.Errorf("top-level stepper logged %q", buf.String())
	}
}

type nopFrontend struct{}

func (nopFrontend) Emit(core.Event) {}
