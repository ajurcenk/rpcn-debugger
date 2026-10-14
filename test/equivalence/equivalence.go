// Package equivalence is the AD-8 equivalence harness. It discovers the cases
// in testdata/equivalence/<name>/pipeline.yaml and produces each case's
// reference output: the batches its output layer receives when the pipeline
// runs without the debugger.
//
// It is a plain (non-test) package so other packages' tests can import it.
// Comparing against a debugger run belongs to the tracer work (AD-11).
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

// RunReference runs the case's pipeline without the debugger and returns the
// batches its output layer received, in order and with their boundaries. Only
// the output plugin is replaced, by a collector with one batch in flight; the
// output's processors and the rest of the config run as written.
//
// It returns when the input is exhausted and the pipeline has drained, or
// with ctx's error when ctx ends first.
func (c Case) RunReference(ctx context.Context) ([]service.MessageBatch, error) {
	raw, err := os.ReadFile(c.Pipeline)
	if err != nil {
		return nil, fmt.Errorf("equivalence case %q: %w", c.Name, err)
	}
	conf, err := withReferenceSink(raw)
	if err != nil {
		return nil, fmt.Errorf("equivalence case %q: %w", c.Name, err)
	}

	col := &collector{}
	env := standardEnv().Clone()
	if err := env.RegisterBatchOutput(sinkName, service.NewConfigSpec(),
		func(*service.ParsedConfig, *service.Resources) (service.BatchOutput, service.BatchPolicy, int, error) {
			return col, service.BatchPolicy{}, 1, nil
		}); err != nil {
		return nil, fmt.Errorf("register %s: %w", sinkName, err)
	}

	b := env.NewStreamBuilder()
	b.SetLogger(slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelWarn})))
	// Endpoints go to a private mux, so no run binds the config's HTTP port.
	b.SetHTTPMux(http.NewServeMux())
	if err := b.SetYAML(conf); err != nil {
		return nil, fmt.Errorf("equivalence case %q (line:col positions refer to the rewritten config, not %s): %w", c.Name, PipelineFile, err)
	}
	strm, err := b.Build()
	if err != nil {
		return nil, fmt.Errorf("equivalence case %q: build: %w", c.Name, err)
	}

	if err := strm.Run(ctx); err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			// Run returns at once on ctx end and leaves the stream running.
			if stopErr := strm.StopWithin(stopTimeout); stopErr != nil {
				return nil, errors.Join(ctxErr, fmt.Errorf("equivalence case %q: stop: %w", c.Name, stopErr))
			}
			return nil, ctxErr
		}
		return nil, fmt.Errorf("equivalence case %q: run: %w", c.Name, err)
	}
	return col.batches(), nil
}

// withReferenceSink returns the config with its output plugin replaced by the
// reference sink. The output's label and processors are kept.
func withReferenceSink(raw []byte) (string, error) {
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
		return encode(&doc)
	}
	root.Content = append(root.Content, scalar("output"), sink)
	return encode(&doc)
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
