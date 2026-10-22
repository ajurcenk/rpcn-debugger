// Package equivalence is the AD-8 equivalence harness. It discovers the cases
// in testdata/equivalence/<name>/pipeline.yaml and produces each case's
// reference output: the batches its output layer receives when the pipeline
// runs without the debugger.
//
// Compare checks a debugger run's delivered batches against a reference, and
// CheckSameBatch enforces the corpus's same-batch rule: each case's input
// yields one batch of at most MaxMessages messages, so the batch the debugger
// captures is the reference run's whole input.
//
// It is a plain (non-test) package so other packages' tests can import it.
package equivalence

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sync"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/redpanda-data/benthos/v4/public/service"
	"github.com/redpanda-data/connect/v4/public/schema"

	_ "github.com/redpanda-data/connect/v4/public/components/all"
)

// PipelineFile is the name of the pipeline config inside a case directory.
const PipelineFile = "pipeline.yaml"

// sinkName is the batch output that replaces each case's output section.
const sinkName = "rpcn_reference_sink"

// inputName is the batch input that wraps each case's input plugin to count
// the batches it yields, before input.processors.
const inputName = "rpcn_reference_input"

// MaxMessages is the debugger's default maxMessages: the most messages a
// corpus case's one input batch may hold.
const MaxMessages = 10

// stopTimeout bounds the shutdown of a stream whose ctx ended first.
const stopTimeout = 5 * time.Second

// Case is one equivalence case.
type Case struct {
	// Name is the case directory's name.
	Name string
	// Dir is the case directory.
	Dir string
	// Pipeline is the path to the case's pipeline.yaml.
	Pipeline string
}

// Cases returns the cases under dir, in name order. Every subdirectory of dir
// is a case and must hold a pipeline.yaml; plain files in dir are ignored.
func Cases(dir string) ([]Case, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("read equivalence cases: %w", err)
	}

	var cases []Case
	for _, e := range entries { // os.ReadDir sorts by name.
		if !e.IsDir() {
			continue
		}
		caseDir := filepath.Join(dir, e.Name())
		pipeline := filepath.Join(caseDir, PipelineFile)
		info, err := os.Stat(pipeline)
		if err != nil {
			return nil, fmt.Errorf("equivalence case %q: %w", e.Name(), err)
		}
		if info.IsDir() {
			return nil, fmt.Errorf("equivalence case %q: %s is a directory", e.Name(), PipelineFile)
		}
		cases = append(cases, Case{Name: e.Name(), Dir: caseDir, Pipeline: pipeline})
	}
	return cases, nil
}

// standardEnv is the stock binary's component set (AD-1), built once. Runs
// never modify it; each one works on its own Clone.
var standardEnv = sync.OnceValue(func() *service.Environment {
	return schema.Standard("dev", "").Environment()
})

// Reference is what a reference run produced.
type Reference struct {
	// Batches are the batches the output layer received, in order and with
	// their boundaries.
	Batches []service.MessageBatch
	// InputBatches holds the size of each batch the input plugin yielded,
	// before input.processors, in order.
	InputBatches []int
}

// RunReference runs the case's pipeline without the debugger and returns the
// batches its output layer received (see Reference).
func (c Case) RunReference(ctx context.Context) ([]service.MessageBatch, error) {
	ref, err := c.Reference(ctx)
	if err != nil {
		return nil, err
	}
	return ref.Batches, nil
}

// Reference runs the case's pipeline without the debugger and returns the
// batches its output layer received, in order and with their boundaries,
// plus the sizes of the batches its input yielded. Only the output plugin
// is replaced, by a collector with one batch in flight, and the input plugin
// is wrapped by a counter that passes its batches and acks through as they
// are; the input's and output's processors and the rest of the config
// (error_handling.strict included) run as written.
//
// It returns when the input is exhausted and the pipeline has drained, or
// with ctx's error when ctx ends first.
func (c Case) Reference(ctx context.Context) (Reference, error) {
	raw, err := os.ReadFile(c.Pipeline)
	if err != nil {
		return Reference{}, fmt.Errorf("equivalence case %q: %w", c.Name, err)
	}
	conf, err := referenceConfig(raw)
	if err != nil {
		return Reference{}, fmt.Errorf("equivalence case %q: %w", c.Name, err)
	}

	col := &collector{}
	cnt := &counter{}
	env := standardEnv().Clone()
	if err := env.RegisterBatchOutput(sinkName, service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			return col, service.BatchPolicy{}, 1, nil
		}); err != nil {
		return Reference{}, fmt.Errorf("register %s: %w", sinkName, err)
	}
	if err := env.RegisterBatchInput(inputName, service.NewConfigSpec().Field(service.NewInputField("input")),
		func(pc *service.ParsedConfig, _ *service.Resources) (service.BatchInput, error) {
			in, err := pc.FieldInput("input")
			if err != nil {
				return nil, err
			}
			return &countingInput{in: in, cnt: cnt}, nil
		}); err != nil {
		return Reference{}, fmt.Errorf("register %s: %w", inputName, err)
	}

	b := env.NewStreamBuilder()
	b.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	// Endpoints go to a private mux, so no run binds the config's HTTP port.
	b.SetHTTPMux(http.NewServeMux())
	if err := b.SetYAML(conf); err != nil {
		return Reference{}, fmt.Errorf("equivalence case %q (line:col positions refer to the rewritten config, not %s): %w", c.Name, PipelineFile, err)
	}
	strm, err := b.Build()
	if err != nil {
		return Reference{}, fmt.Errorf("equivalence case %q: build: %w", c.Name, err)
	}

	if err := strm.Run(ctx); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Run returns at once on ctx end and leaves the stream running.
			if stopErr := strm.StopWithin(stopTimeout); stopErr != nil {
				return Reference{}, errors.Join(ctxErr, fmt.Errorf("equivalence case %q: stop: %w", c.Name, stopErr))
			}
			return Reference{}, ctxErr
		}
		return Reference{}, fmt.Errorf("equivalence case %q: run: %w", c.Name, err)
	}
	return Reference{Batches: col.batches(), InputBatches: cnt.sizes()}, nil
}

// referenceConfig returns the config with its output plugin replaced by the
// reference sink and its input plugin wrapped by the counting input.
func referenceConfig(raw []byte) (string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(raw, &doc); err != nil {
		return "", fmt.Errorf("parse %s: %w", PipelineFile, err)
	}
	if doc.Kind == 0 {
		doc = yaml.Node{Kind: yaml.DocumentNode, Content: []*yaml.Node{{Kind: yaml.MappingNode}}}
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return "", errors.New(PipelineFile + " is not a YAML mapping")
	}
	root := doc.Content[0]
	if !hasKey(root, "input") {
		// Without one the engine falls back to stdin and the run looks empty.
		return "", errors.New(PipelineFile + " has no top-level input")
	}
	if err := wrapInput(root); err != nil {
		return "", err
	}
	withReferenceSink(root)
	return encode(&doc)
}

// wrapInput moves the input's plugin (every key but label and processors)
// into the counting input, which takes its place.
func wrapInput(root *yaml.Node) error {
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "input" {
			continue
		}
		old := root.Content[i+1]
		if old.Kind != yaml.MappingNode {
			return errors.New(PipelineFile + ": input is not a mapping")
		}
		inner := &yaml.Node{Kind: yaml.MappingNode}
		outer := &yaml.Node{Kind: yaml.MappingNode}
		var procs []*yaml.Node
		for j := 0; j+1 < len(old.Content); j += 2 {
			switch old.Content[j].Value {
			case "label":
				outer.Content = append(outer.Content, old.Content[j], old.Content[j+1])
			case "processors":
				procs = append(procs, old.Content[j], old.Content[j+1])
			default:
				inner.Content = append(inner.Content, old.Content[j], old.Content[j+1])
			}
		}
		outer.Content = append(outer.Content, scalar(inputName), &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
			scalar("input"), inner,
		}})
		outer.Content = append(outer.Content, procs...)
		root.Content[i+1] = outer
		return nil
	}
	return nil
}

// withReferenceSink replaces root's output plugin by the reference sink. The
// output's label and processors are kept.
func withReferenceSink(root *yaml.Node) {
	sink := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
		scalar(sinkName), {Kind: yaml.MappingNode, Style: yaml.FlowStyle},
	}}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if root.Content[i].Value != "output" {
			continue
		}
		if old := root.Content[i+1]; old.Kind == yaml.MappingNode {
			for j := 0; j+1 < len(old.Content); j += 2 {
				if k := old.Content[j].Value; k == "label" || k == "processors" {
					sink.Content = append(sink.Content, old.Content[j], old.Content[j+1])
				}
			}
		}
		root.Content[i+1] = sink
		return
	}
	root.Content = append(root.Content, scalar("output"), sink)
}

func hasKey(m *yaml.Node, key string) bool {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return true
		}
	}
	return false
}

func scalar(v string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: "!!str", Value: v}
}

func encode(doc *yaml.Node) (string, error) {
	out, err := yaml.Marshal(doc)
	if err != nil {
		return "", fmt.Errorf("encode %s: %w", PipelineFile, err)
	}
	return string(out), nil
}

// collector is the reference sink. It records each batch exactly as the
// output layer hands it over.
type collector struct {
	mu  sync.Mutex
	got []service.MessageBatch
}

func (c *collector) Connect(context.Context) error { return nil }

func (c *collector) WriteBatch(_ context.Context, b service.MessageBatch) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.got = append(c.got, b.Copy())
	return nil
}

func (c *collector) Close(context.Context) error { return nil }

func (c *collector) batches() []service.MessageBatch {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]service.MessageBatch(nil), c.got...)
}

// counter records the size of each batch the input yields.
type counter struct {
	mu sync.Mutex
	n  []int
}

func (c *counter) add(n int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.n = append(c.n, n)
}

func (c *counter) sizes() []int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return append([]int(nil), c.n...)
}

// countingInput passes the wrapped input's batches and acks through
// unchanged, counting each batch.
type countingInput struct {
	in  *service.OwnedInput
	cnt *counter
}

func (c *countingInput) Connect(context.Context) error { return nil }

func (c *countingInput) ReadBatch(ctx context.Context) (service.MessageBatch, service.AckFunc, error) {
	b, ack, err := c.in.ReadBatch(ctx)
	if err != nil {
		return nil, nil, err
	}
	c.cnt.add(len(b))
	return b, ack, nil
}

func (c *countingInput) Close(ctx context.Context) error { return c.in.Close(ctx) }
