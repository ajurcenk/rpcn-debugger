package rewrite

import (
	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// In stdio mode stdin and stdout carry the DAP stream (AD-15). A stdin
// input is the input table's rejected row; the Session refuses it.

// warnStdout warns about every stdout or stderr output in the output tree n
// at path: the step config always ends in rpcn_debug_sink (AD-21), so under
// dry-run and none they are redirected to the sink, and under real they are
// replaced by drop (dropStdio).
func (w *walker) warnStdout(n *yaml.Node, path string, mode core.OutputMode) {
	walkKeys(n, path, func(k *yaml.Node, p string) bool {
		if k.Value != "stdout" && k.Value != "stderr" {
			return false
		}
		if mode == core.OutputReal {
			w.warn(k.Line, p, "the %s output is replaced by drop under output: real: %s carries the debugger's DAP stream, so nothing is written to it", k.Value, k.Value)
		} else {
			w.warn(k.Line, p, "the %s output is redirected to dry-run: %s carries the debugger's DAP stream, so results go to VS Code only", k.Value, k.Value)
		}
		return false
	})
}

// warnBatching warns about every non-null batching field of an output in
// the output block n at path, the children of composite outputs (broker,
// switch, fallback, retry, …) included: rpcn_debug_sink records the batches
// as received and applies no output batching (AD-21). Only an output's own
// batching field counts, not a user map key that happens to be named
// batching (e.g. an HTTP header). With strip (output: real, on the emitted
// copy only) it also removes each of them: a count with no period would
// never flush the one captured batch.
func (w *walker) warnBatching(n *yaml.Node, path string, strip bool) {
	if n == nil || n.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		p := path + "." + k.Value
		switch k.Value {
		case "label", "processors":
		case "batching":
			w.batchingField(k, v, p, strip)
		default:
			w.outputConfig(v, p, strip)
		}
	}
	if strip {
		withoutBatching(n)
	}
}

// outputConfig checks the config v of the output component at path: its own
// batching field, then the output blocks it holds.
func (w *walker) outputConfig(v *yaml.Node, path string, strip bool) {
	switch {
	case v == nil:
	case v.Kind == yaml.SequenceNode: // fallback: a list of outputs
		for i, o := range v.Content {
			w.warnBatching(o, child(path, i), strip)
		}
	case v.Kind == yaml.MappingNode:
		for i := 0; i+1 < len(v.Content); i += 2 {
			k, c := v.Content[i], v.Content[i+1]
			p := path + "." + k.Value
			switch {
			case k.Value == "batching":
				w.batchingField(k, c, p, strip)
			case k.Value == "outputs" && c.Kind == yaml.SequenceNode:
				for j, o := range c.Content {
					w.warnBatching(o, child(p, j), strip)
				}
			case k.Value == "output" && c.Kind == yaml.MappingNode:
				w.warnBatching(c, p, strip)
			case k.Value == "cases" && c.Kind == yaml.SequenceNode:
				for j, cs := range c.Content {
					if o := value(cs, "output"); o != nil {
						w.warnBatching(o, child(p, j)+".output", strip)
					}
				}
			}
		}
		if strip {
			withoutBatching(v)
		}
	}
}

func (w *walker) batchingField(k, v *yaml.Node, path string, strip bool) {
	switch {
	case isNull(v):
	case strip:
		w.warn(k.Line, path, "output batching isn't applied under output: real: the captured batch is written as one batch")
	default:
		w.warn(k.Line, path, "output batching isn't applied in dry-run: the batches shown are the pipeline's own, and production batch boundaries may differ")
	}
}

// withoutBatching removes the batching key of mapping m in place.
func withoutBatching(m *yaml.Node) {
	out := m.Content[:0]
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value != "batching" {
			out = append(out, m.Content[i], m.Content[i+1])
		}
	}
	m.Content = out
}

// walkKeys calls fn on every mapping key in n, with its user path, in file
// order, skipping processors lists (they hold no inputs or outputs). It
// stops when fn returns true, and reports whether it did.
func walkKeys(n *yaml.Node, path string, fn func(k *yaml.Node, path string) bool) bool {
	return walkPairs(n, path, func(k, _ *yaml.Node, p string) bool { return fn(k, p) })
}

// walkPairs is walkKeys with each key's value.
func walkPairs(n *yaml.Node, path string, fn func(k, v *yaml.Node, path string) bool) bool {
	if n == nil {
		return false
	}
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			k := n.Content[i]
			if k.Value == "processors" {
				continue
			}
			p := path + "." + k.Value
			if fn(k, n.Content[i+1], p) || walkPairs(n.Content[i+1], p, fn) {
				return true
			}
		}
	case yaml.SequenceNode:
		for i, c := range n.Content {
			if walkPairs(c, child(path, i), fn) {
				return true
			}
		}
	}
	return false
}

// warnNestedLists warns about every non-empty processors list in the input
// or output tree n at path below the top-level one (broker, switch and
// other composite children): it is never wrapped; why says what happens.
func (w *walker) warnNestedLists(n *yaml.Node, path, why string) {
	var visit func(n *yaml.Node, path string, top bool)
	visit = func(n *yaml.Node, path string, top bool) {
		switch n.Kind {
		case yaml.MappingNode:
			for i := 0; i+1 < len(n.Content); i += 2 {
				k, v := n.Content[i], n.Content[i+1]
				p := path + "." + k.Value
				if k.Value == "processors" && v.Kind == yaml.SequenceNode {
					if !top && len(v.Content) > 0 {
						w.warn(k.Line, p, "this processor list %s", why)
					}
					continue
				}
				visit(v, p, false)
			}
		case yaml.SequenceNode:
			for i, c := range n.Content {
				visit(c, child(path, i), false)
			}
		}
	}
	visit(n, path, true)
}
