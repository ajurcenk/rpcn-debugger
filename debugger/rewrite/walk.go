package rewrite

import (
	"fmt"
	"strconv"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// The stepper modes (AD-7).
const (
	modeAll = "all"
	modeTry = "try"
)

// walker wraps child processor lists in steppers (AD-6), giving each a
// dense id in depth-first pre-order (AD-16) and recording its stops.
type walker struct {
	next int
	// strict is the value of every stepper's strict field: a bool, or the
	// interpolation as written.
	strict   *yaml.Node
	stops    map[stop]core.Position
	warnings []string
	// tainted holds the processor_resources labels used inside a
	// concurrent parent; they stay unwrapped.
	tainted map[string]bool
}

// wrap returns the stepper processor around list, the child list at path
// whose key is on keyLine, and wraps the lists inside its children.
func (w *walker) wrap(list *yaml.Node, path string, keyLine, parent int, mode string) (*yaml.Node, error) {
	id := w.next
	w.next++
	lines := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Style: yaml.FlowStyle}
	for i, c := range list.Content {
		cp := child(path, i)
		w.stops[stop{id, i}] = core.Position{UserPath: cp, Line: c.Line}
		lines.Content = append(lines.Content, scalar("!!int", strconv.Itoa(c.Line)))
		if err := w.processor(c, cp, id); err != nil {
			return nil, err
		}
	}
	// The end-of-entry stop (AD-10).
	w.stops[stop{id, len(list.Content)}] = core.Position{UserPath: path, Line: keyLine}

	return &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
		scalar("!!str", StepperName),
		{Kind: yaml.MappingNode, Content: []*yaml.Node{
			scalar("!!str", "id"), scalar("!!int", strconv.Itoa(id)),
			scalar("!!str", "parent"), scalar("!!int", strconv.Itoa(parent)),
			scalar("!!str", "mode"), scalar("!!str", mode),
			scalar("!!str", "strict"), w.strict,
			scalar("!!str", "concurrent"), scalar("!!bool", "false"),
			scalar("!!str", "lines"), lines,
			scalar("!!str", "processors"), list,
		}},
	}}, nil
}

// list wraps the child list v at path (key on keyLine) and returns its
// replacement, a one-element list holding the stepper, or nil when there is
// nothing to step.
func (w *walker) list(v *yaml.Node, path string, keyLine, parent int, mode string) (*yaml.Node, error) {
	if isNull(v) {
		return nil, nil
	}
	if v.Kind != yaml.SequenceNode {
		return nil, invalid(v.Line, path, "%s is not a list", path)
	}
	if len(v.Content) == 0 {
		return nil, nil
	}
	st, err := w.wrap(v, path, keyLine, parent, mode)
	if err != nil {
		return nil, err
	}
	return &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: []*yaml.Node{st}}, nil
}

// inner wraps the processors field of mapping m (a branch, while, retry,
// switch case or group_by group) at path.
func (w *walker) inner(m *yaml.Node, path string, parent int, mode string) error {
	if isNull(m) {
		return nil
	}
	if m.Kind != yaml.MappingNode {
		return invalid(m.Line, path, "%s is not a mapping", path)
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if k := m.Content[i]; k.Value == "processors" {
			st, err := w.list(m.Content[i+1], path+".processors", k.Line, parent, mode)
			if err != nil {
				return err
			}
			if st != nil {
				m.Content[i+1] = st
			}
		}
	}
	return nil
}

// What classify finds a processor component to be.
const (
	leaf       = iota // no child list
	parent            // a parent in the AD-7 table
	concurrent        // a concurrent parent: opaque
	unknown           // an unknown parent: opaque
)

// classify returns what the component name with config v is and, for an
// opaque one, the warning text.
func classify(name string, v *yaml.Node) (int, string) {
	const opaque = ", so it runs as one step and nothing inside it is stepped"
	switch name {
	case "branch", "while", "try", "catch", "for_each", "switch", "group_by":
		return parent, ""
	case "retry":
		p := value(v, "parallel")
		if isInterpolated(p) {
			return concurrent, "retry with an interpolated parallel may run its processors concurrently (decided at runtime)" + opaque
		}
		var b bool
		if p != nil && p.Decode(&b) == nil && b {
			return concurrent, "retry with parallel: true runs its processors concurrently" + opaque
		}
		return parent, ""
	case "workflow", "parallel":
		return concurrent, name + " runs its processors concurrently" + opaque
	}
	if (name == "processors" && v.Kind == yaml.SequenceNode) || holdsProcessors(v) {
		return unknown, name + " is not a parent processor the debugger knows" + opaque
	}
	return leaf, ""
}

// processor wraps the child lists of processor p at path, inside stepper
// parent, per the AD-7 table.
func (w *walker) processor(p *yaml.Node, path string, parent int) error {
	if p.Kind != yaml.MappingNode {
		return nil // Not a processor config: the engine reports it.
	}
	for i := 0; i+1 < len(p.Content); i += 2 {
		k, v := p.Content[i], p.Content[i+1]
		if k.Value == "label" {
			continue
		}
		cp := path + "." + k.Value
		kind, msg := classify(k.Value, v)
		if kind == concurrent || kind == unknown {
			w.warn(k.Line, cp, "%s", msg)
			continue
		}
		if kind == leaf {
			continue
		}
		switch k.Value {
		case "branch", "while", "retry":
			if err := w.inner(v, cp, parent, modeAll); err != nil {
				return err
			}
		case "try", "catch", "for_each":
			mode := modeAll
			if k.Value == "try" {
				mode = modeTry
			}
			st, err := w.list(v, cp, k.Line, parent, mode)
			if err != nil {
				return err
			}
			if st != nil {
				p.Content[i+1] = st
			}
		case "switch", "group_by":
			if isNull(v) {
				continue
			}
			if v.Kind != yaml.SequenceNode {
				return invalid(v.Line, cp, "%s is not a list", cp)
			}
			for c, cs := range v.Content {
				if err := w.inner(cs, child(cp, c), parent, modeAll); err != nil {
					return err
				}
			}
		}
	}
	return nil
}

// childLists returns the child processor lists of parent component name
// with config v.
func childLists(name string, v *yaml.Node) []*yaml.Node {
	switch name {
	case "branch", "while", "retry":
		return []*yaml.Node{value(v, "processors")}
	case "try", "catch", "for_each":
		return []*yaml.Node{v}
	case "switch", "group_by":
		var out []*yaml.Node
		if v.Kind == yaml.SequenceNode {
			for _, c := range v.Content {
				out = append(out, value(c, "processors"))
			}
		}
		return out
	}
	return nil
}

// resources wraps the child lists inside every processor_resources entry,
// except those used inside a concurrent parent.
func (w *walker) resources(list *yaml.Node) error {
	for i, entry := range list.Content {
		path := child("processor_resources", i)
		label := ""
		if l := value(entry, "label"); l != nil && l.Kind == yaml.ScalarNode && l.Value != "" {
			label = l.Value
			path = "processor_resources." + label
		}
		if label != "" && w.tainted[label] {
			w.warn(entry.Line, path, "this resource is used inside a parent that runs as one step (a concurrent or unknown parent), so it runs as one step and nothing inside it is stepped")
			continue
		}
		if err := w.processor(entry, path, -1); err != nil {
			return err
		}
	}
	return nil
}

func (w *walker) warn(line int, path, format string, args ...any) {
	w.warnings = append(w.warnings, fmt.Sprintf("%s (line %d): ", path, line)+fmt.Sprintf(format, args...))
}

// holdsProcessors reports whether config v holds a processors list at any
// depth: an unknown parent.
func holdsProcessors(v *yaml.Node) bool {
	if v == nil {
		return false
	}
	if v.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(v.Content); i += 2 {
			if v.Content[i].Value == "processors" && v.Content[i+1].Kind == yaml.SequenceNode {
				return true
			}
		}
	}
	for _, c := range v.Content {
		if holdsProcessors(c) {
			return true
		}
	}
	return false
}

// taintedResources returns the processor_resources labels used, directly or
// through other resources, inside an opaque parent (concurrent or unknown)
// in lists or in any processor resource.
func taintedResources(lists []*yaml.Node, resources *yaml.Node) map[string]bool {
	tainted := map[string]bool{}
	for _, l := range lists {
		scanList(l, tainted)
	}
	bodies := map[string]*yaml.Node{}
	if resources != nil && resources.Kind == yaml.SequenceNode {
		for _, e := range resources.Content {
			scanProcessor(e, tainted)
			if l := value(e, "label"); l != nil && l.Kind == yaml.ScalarNode {
				bodies[l.Value] = e
			}
		}
	}
	queue := make([]string, 0, len(tainted))
	for l := range tainted {
		queue = append(queue, l)
	}
	for len(queue) > 0 {
		l := queue[0]
		queue = queue[1:]
		found := map[string]bool{}
		refs(bodies[l], found)
		for r := range found {
			if !tainted[r] {
				tainted[r] = true
				queue = append(queue, r)
			}
		}
	}
	return tainted
}

// scanList adds to out the resources referenced inside the opaque parents
// of the processor list l, following the parents the walk wraps.
func scanList(l *yaml.Node, out map[string]bool) {
	if l == nil || l.Kind != yaml.SequenceNode {
		return
	}
	for _, p := range l.Content {
		scanProcessor(p, out)
	}
}

func scanProcessor(p *yaml.Node, out map[string]bool) {
	if p == nil || p.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(p.Content); i += 2 {
		name, v := p.Content[i].Value, p.Content[i+1]
		if name == "label" {
			continue
		}
		switch kind, _ := classify(name, v); kind {
		case concurrent, unknown:
			refs(v, out)
		case parent:
			for _, l := range childLists(name, v) {
				scanList(l, out)
			}
		}
	}
}

// refs adds to out every resource label referenced in n: resource: <label>
// and a workflow's branch_resources.
func refs(n *yaml.Node, out map[string]bool) {
	if n == nil {
		return
	}
	if n.Kind == yaml.MappingNode {
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i].Value, n.Content[i+1]
			switch {
			case k == "resource" && v.Kind == yaml.ScalarNode:
				out[v.Value] = true
			case k == "branch_resources" && v.Kind == yaml.SequenceNode:
				for _, l := range v.Content {
					if l.Kind == yaml.ScalarNode {
						out[l.Value] = true
					}
				}
			}
		}
	}
	for _, c := range n.Content {
		refs(c, out)
	}
}
