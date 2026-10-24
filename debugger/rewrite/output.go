package rewrite

import (
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// SinkName is the output every step config ends in (AD-21).
const SinkName = "rpcn_debug_sink"

// sinkOutputPath is the engine path of the user's output inside the sink
// under output: real.
const sinkOutputPath = "root.output." + SinkName + ".output"

// stdioPaths are the literal file output paths that write to the process's
// stdout or stderr, which carry the DAP stream (AD-15).
var stdioPaths = map[string]bool{
	"/dev/stdout": true, "/dev/stderr": true, "/dev/fd/1": true, "/dev/fd/2": true,
	"/proc/self/fd/1": true, "/proc/self/fd/2": true,
}

// sinkConfig returns the step stream's output config (AD-21): rpcn_debug_sink
// with its mode and, under output: real, user: the copy of the user's
// output block realOutput made (nil when there is none). k is the user's
// output key, or nil.
func (w *walker) sinkConfig(k, user *yaml.Node, mode core.OutputMode) *yaml.Node {
	body := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map"}
	body.Content = append(body.Content, scalar("!!str", "mode"), scalar("!!str", string(mode)))
	if mode == core.OutputReal && user != nil {
		w.procs[user] = "output"
		w.paths[sinkOutputPath] = "output"
		body.Content = append(body.Content, scalar("!!str", "output"), user)
	}
	n := &yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Content: []*yaml.Node{scalar("!!str", SinkName), body}}
	if k != nil {
		// The synthesized nodes map to the user's output key.
		w.anchors[n] = k
		w.procs[k] = "output"
	}
	return n
}

// realOutput returns the copy of the output tree n at path that output:
// real writes to (AD-21): every stdout and stderr output, and every file
// output to a stdioPaths path, replaced by drop: {} (AD-15), and every
// output-level batching field removed, each with a warning. The parsed
// pipeline is never edited. With shared, an output that needs the
// top-level redpanda block's shared client (redpanda_common, or redpanda
// without seed_brokers) is PIPELINE_INVALID: a debug session never has it,
// so the output would retry for ever.
func (w *walker) realOutput(n *yaml.Node, path string, shared bool) (*yaml.Node, error) {
	c := resolve(n)
	var err error
	walkPairs(c, path, func(k, v *yaml.Node, p string) bool {
		switch {
		case shared && k.Value == "redpanda_common" && v.Kind == yaml.MappingNode:
			err = invalid(k.Line, p, "output: real can't write to the redpanda_common output at %s: %s", p, noSharedClientOut)
			return true
		case shared && k.Value == "redpanda" && v.Kind == yaml.MappingNode && !isSet(v, "seed_brokers"):
			err = invalid(k.Line, p, "output: real can't write to the redpanda output at %s: without seed_brokers %s", p, noSharedClientOut)
			return true
		case k.Value == "stdout" || k.Value == "stderr":
			w.warn(k.Line, p, "the %s output is replaced by drop under output: real: %s carries the debugger's DAP stream, so nothing is written to it", k.Value, k.Value)
		case k.Value == "file" && isStdioFile(v):
			w.warn(k.Line, p, "the file output to %s is replaced by drop under output: real: it is the debugger's DAP stream, so nothing is written to it", value(v, "path").Value)
		default:
			return false
		}
		k.Value = "drop"
		k.Tag, k.Style = "!!str", 0
		*v = yaml.Node{Kind: yaml.MappingNode, Tag: "!!map", Style: yaml.FlowStyle, Line: v.Line, Column: v.Column}
		return false
	})
	if err != nil {
		return nil, err
	}
	switch c.Kind {
	case yaml.MappingNode:
		w.warnBatching(c, path, true)
	case yaml.SequenceNode: // output_resources
		for i, e := range c.Content {
			w.warnBatching(e, resourcePath(path, i, e), true)
		}
	}
	return c, nil
}

// isStdioFile reports whether file output config v has a literal
// stdioPaths path.
func isStdioFile(v *yaml.Node) bool {
	p := value(v, "path")
	return p != nil && p.Kind == yaml.ScalarNode && stdioPaths[p.Value]
}

// noSharedClientOut is noSharedClient for an output.
const noSharedClientOut = "it writes through the shared client of the top-level redpanda block, which only rpk connect sets up, not a debug session"

// warnReal gives the one output: real launch warning: what each delivered
// batch is written to. k is the user's output key and orig its block, and
// user the copy realOutput made of it, or nil.
func (w *walker) warnReal(k, orig, user *yaml.Node) {
	wasK, _ := component(orig)
	compK, _ := component(user)
	if compK == nil {
		w.warnings = append(w.warnings, "output: real has nothing to write to: the pipeline's output is empty or missing, so delivered batches are only shown")
		return
	}
	was, comp := "", compK.Value
	if wasK != nil {
		was = wasK.Value
	}
	if comp == "drop" && was != "drop" {
		w.warnings = append(w.warnings, "output: real has nothing to write to: the pipeline's output only writes to the DAP stream, which is replaced by drop, so delivered batches are only shown")
		return
	}
	w.warnings = append(w.warnings, fmt.Sprintf("output: real writes each delivered batch to the %s output at output (line %d)", comp, k.Line))
}
