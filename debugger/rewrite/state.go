package rewrite

import (
	"gopkg.in/yaml.v3"
)

// stateful is the closed table of processors that keep state across
// messages (AD-7). rate_limit is left out: it changes only timing, not
// output (AD-8).
var stateful = map[string]bool{
	"cache":        true,
	"cached":       true,
	"dedupe":       true,
	"redis":        true,
	"redis_script": true,
	"sql_raw":      true,
	"sql_insert":   true,
	"sql_select":   true,
}

// resourceBodies maps each processor_resources label to its entry. The
// taint scan and the stateful check both look resources up in it.
func resourceBodies(resources *yaml.Node) map[string]*yaml.Node {
	bodies := map[string]*yaml.Node{}
	if resources == nil || resources.Kind != yaml.SequenceNode {
		return bodies
	}
	for _, e := range resources.Content {
		if l := resourceLabel(e); l != nil {
			bodies[l.Value] = e
		}
	}
	return bodies
}

// statefulResources returns the processor_resources labels whose entry
// holds a stateful processor, directly or through the resources it
// references. Each entry is scanned once, then statefulness is propagated
// to the referrers to a fixed point, so a resource referenced by several
// routes is not searched again and a reference cycle ends.
func statefulResources(bodies map[string]*yaml.Node) map[string]bool {
	out := map[string]bool{}
	referrers := map[string][]string{}
	var queue []string
	for label, body := range bodies {
		direct := holdsStateful(body, func(r string) bool {
			referrers[r] = append(referrers[r], label)
			return false
		})
		if direct {
			out[label] = true
			queue = append(queue, label)
		}
	}
	for len(queue) > 0 {
		l := queue[0]
		queue = queue[1:]
		for _, r := range referrers[l] {
			if !out[r] {
				out[r] = true
				queue = append(queue, r)
			}
		}
	}
	return out
}

// warnStatefulOrder warns when the try or catch list v (key k, user path
// path) has two or more children and one of them holds a stateful
// processor: the engine runs the list processor-major (ExecuteTryAll,
// ExecuteCatchAll) and the debugger steps it message-major (AD-7).
func (w *walker) warnStatefulOrder(k, v *yaml.Node, path string) {
	if v == nil || v.Kind != yaml.SequenceNode || len(v.Content) < 2 {
		return
	}
	ref := func(label string) bool { return w.statefulRes[label] }
	for _, c := range v.Content {
		if holdsStateful(c, ref) {
			w.warn(k.Line, path, "the engine runs the %s children over every message one child at a time, while the debugger steps one message through all of them, so children that share state across messages (such as a cache) may give different output", k.Value)
			return
		}
	}
}

// holdsStateful reports whether processor p is stateful: a component in the
// stateful table, a parent (known, concurrent or unknown) whose child lists
// hold a stateful processor at any depth, or a processor resource reference
// for which ref returns true. Interpolated references are skipped.
func holdsStateful(p *yaml.Node, ref func(label string) bool) bool {
	if p == nil || p.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(p.Content); i += 2 {
		name, v := p.Content[i].Value, p.Content[i+1]
		if name == "label" {
			continue
		}
		if stateful[name] {
			return true
		}
		if name == "resource" {
			if refStateful(v, ref) {
				return true
			}
			continue
		}
		switch class, _ := classify(name, v); class {
		case parent:
			for cl := range childLists(p, i) {
				if listStateful(cl.list, ref) {
					return true
				}
			}
		case concurrent, unknown:
			if opaqueStateful(name, v, ref) {
				return true
			}
		}
	}
	return false
}

// refStateful reports ref for the processor resource named by scalar v.
func refStateful(v *yaml.Node, ref func(string) bool) bool {
	return v != nil && v.Kind == yaml.ScalarNode && !isInterpolated(v) && ref(v.Value)
}

func listStateful(l *yaml.Node, ref func(string) bool) bool {
	if l == nil || l.Kind != yaml.SequenceNode {
		return false
	}
	for _, c := range l.Content {
		if holdsStateful(c, ref) {
			return true
		}
	}
	return false
}

// opaqueStateful reports whether the config v of the opaque parent name
// holds a stateful processor: in any processors list inside it (the
// processors processor is one itself), or in a workflow's branch_resources.
func opaqueStateful(name string, v *yaml.Node, ref func(string) bool) bool {
	if name == "processors" {
		return listStateful(v, ref)
	}
	var search func(n *yaml.Node) bool
	search = func(n *yaml.Node) bool {
		if n == nil {
			return false
		}
		switch n.Kind {
		case yaml.MappingNode:
			for i := 0; i+1 < len(n.Content); i += 2 {
				k, c := n.Content[i].Value, n.Content[i+1]
				switch {
				case k == "processors" && c.Kind == yaml.SequenceNode:
					if listStateful(c, ref) {
						return true
					}
				case k == "branch_resources" && c.Kind == yaml.SequenceNode:
					for _, r := range c.Content {
						if refStateful(r, ref) {
							return true
						}
					}
				default:
					if search(c) {
						return true
					}
				}
			}
		case yaml.SequenceNode:
			for _, c := range n.Content {
				if search(c) {
					return true
				}
			}
		}
		return false
	}
	return search(v)
}

// warnBuffer warns when the top-level buffer (key k, value v) is set: the
// rewrite drops it (AD-5), so the captured batch goes straight to the
// processors. A null buffer, an empty one and none are the engine's default.
func (w *walker) warnBuffer(k, v *yaml.Node) {
	if k == nil || isNull(v) {
		return
	}
	if v.Kind == yaml.MappingNode {
		set := false
		for i := 0; i+1 < len(v.Content); i += 2 {
			if n := v.Content[i].Value; n != "label" && n != "none" {
				set = true
			}
		}
		if !set {
			return
		}
	}
	w.warn(k.Line, "buffer", "the buffer is dropped, so the captured batch goes straight to the processors, and production batch boundaries may differ")
}
