package rewrite

import (
	"gopkg.in/yaml.v3"
)

// In stdio mode stdin and stdout carry the DAP stream (AD-15).

// rejectStdin returns a PIPELINE_INVALID error at the first stdin input in
// the input tree n at path, broker and sequence children included. stdin is
// a rejected input (AD-5).
func rejectStdin(n *yaml.Node, path string) error {
	k, p := find(n, path, "stdin")
	if k == nil {
		return nil
	}
	return invalid(k.Line, p, "the stdin input can't be debugged: stdin carries the debugger's DAP stream")
}

// warnStdout warns about every stdout or stderr output in the output tree n
// at path: the step config always ends in rpcn_debug_sink (AD-21), so they
// are redirected to the dry-run sink.
func (w *walker) warnStdout(n *yaml.Node, path string) {
	walkKeys(n, path, func(k *yaml.Node, p string) bool {
		if k.Value == "stdout" || k.Value == "stderr" {
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
// batching (e.g. an HTTP header).
func (w *walker) warnBatching(n *yaml.Node, path string) {
	if n == nil || n.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(n.Content); i += 2 {
		k, v := n.Content[i], n.Content[i+1]
		p := path + "." + k.Value
		switch k.Value {
		case "label", "processors":
		case "batching":
			w.batchingField(k, v, p)
		default:
			w.outputConfig(v, p)
		}
	}
}

// outputConfig checks the config v of the output component at path: its own
// batching field, then the output blocks it holds.
func (w *walker) outputConfig(v *yaml.Node, path string) {
	switch {
	case v == nil:
	case v.Kind == yaml.SequenceNode: // fallback: a list of outputs
		for i, o := range v.Content {
			w.warnBatching(o, child(path, i))
		}
	case v.Kind == yaml.MappingNode:
		for i := 0; i+1 < len(v.Content); i += 2 {
			k, c := v.Content[i], v.Content[i+1]
			p := path + "." + k.Value
			switch {
			case k.Value == "batching":
				w.batchingField(k, c, p)
			case k.Value == "outputs" && c.Kind == yaml.SequenceNode:
				for j, o := range c.Content {
					w.warnBatching(o, child(p, j))
				}
			case k.Value == "output" && c.Kind == yaml.MappingNode:
				w.warnBatching(c, p)
			case k.Value == "cases" && c.Kind == yaml.SequenceNode:
				for j, cs := range c.Content {
					if o := value(cs, "output"); o != nil {
						w.warnBatching(o, child(p, j)+".output")
					}
				}
			}
		}
	}
}

func (w *walker) batchingField(k, v *yaml.Node, path string) {
	if !isNull(v) {
		w.warn(k.Line, path, "output batching isn't applied in dry-run: the batches shown are the pipeline's own, and production batch boundaries may differ")
	}
}

// find returns the first key named name in n (outside processor lists) and
// its user path.
func find(n *yaml.Node, path, name string) (*yaml.Node, string) {
	var key *yaml.Node
	var at string
	walkKeys(n, path, func(k *yaml.Node, p string) bool {
		if k.Value == name {
			key, at = k, p
			return true
		}
		return false
	})
	return key, at
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
