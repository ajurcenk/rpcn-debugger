package rewrite

import (
	"fmt"
	"iter"
	"strconv"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// The stepper modes (AD-7).
const (
	modeAll = "all"
	modeTry = "try"
)

// The repeat label kinds a stepper computes its label by (AD-10).
const (
	repeatMsg     = "msg"     // for_each, try: one entry per message
	repeatAttempt = "attempt" // retry: one entry per attempt
	repeatLoop    = "loop"    // while: one entry per loop
	repeatGroup   = "group"   // group_by: one entry per group
)

// kind is what the stepper of one child list is told about its parent.
type kind struct {
	mode string
	// repeat is the repeat label kind, or "" for none (branch, switch
	// cases, catch and top-level lists).
	repeat string
	// maxAttempts is a retry's max_retries as written, or nil.
	maxAttempts *yaml.Node
	// noStrict is set for catch: ExecuteCatchAll runs every child on the
	// caught messages, whatever error_handling.strict says.
	noStrict bool
}

// all is the kind of a plain ExecuteAll list.
var all = kind{mode: modeAll}

// shape is where a parent's child processor lists sit in its config.
type shape int

const (
	ownList shape = iota // the config is the list
	inner                // the config's processors field
	cases                // the processors field of each element of the config
)

// parentRow is one row of the AD-7 table: where a parent's child lists are
// and the kind of their steppers.
type parentRow struct {
	shape shape
	kind  kind
}

// parents is the AD-7 table, keyed by parent component name. Every walk of
// the processor tree (the wrap, the resource check and the taint scan)
// reads it. A parent with concurrent entries (workflow, parallel, retry
// with parallel: true) is opaque instead (classify).
var parents = map[string]parentRow{
	"branch":   {inner, kind{mode: modeAll}},
	"while":    {inner, kind{mode: modeAll, repeat: repeatLoop}},
	"retry":    {inner, kind{mode: modeAll, repeat: repeatAttempt}},
	"try":      {ownList, kind{mode: modeTry, repeat: repeatMsg}},
	"catch":    {ownList, kind{mode: modeAll, noStrict: true}},
	"for_each": {ownList, kind{mode: modeAll, repeat: repeatMsg}},
	"switch":   {cases, kind{mode: modeAll}},
	"group_by": {cases, kind{mode: modeAll, repeat: repeatGroup}},
}

// parentKind returns the kind of the child lists of parent component name
// with config v (AD-7, AD-10).
func parentKind(name string, v *yaml.Node) kind {
	row, ok := parents[name]
	if !ok {
		return all
	}
	k := row.kind
	if k.repeat == repeatAttempt {
		if m := value(v, "max_retries"); m != nil && m.Kind == yaml.ScalarNode && !isNull(m) {
			c := *m
			k.maxAttempts = &c
		}
	}
	return k
}

// childList is one child processor list of an AD-7 parent.
type childList struct {
	// list is the list node (nil when absent), and slot where it sits in
	// the parent's config.
	list *yaml.Node
	slot **yaml.Node
	// path and eng are the suffixes of the list's user path and engine
	// path after the parent's: "", ".processors" or "[i].processors"
	// (engine ".i.processors").
	path, eng string
	// keyLine is the line of the key the list is under: the end-of-entry
	// stop (AD-10).
	keyLine int
	// bad, when set, is the node at path that should hold the list but is
	// not want ("a list" or "a mapping"); list is then nil. The walker turns
	// it into PIPELINE_INVALID; the resource check and taint scan rely on
	// their list functions ignoring a nil list, so every caller must either
	// check bad or tolerate a nil list.
	bad  *yaml.Node
	want string
}

// childLists yields, per the AD-7 table, the child lists of the parent
// component at key index i of processor mapping p, in config order. It
// yields nothing for a component that isn't in the table.
func childLists(p *yaml.Node, i int) iter.Seq[childList] {
	return func(yield func(childList) bool) {
		k, v := p.Content[i], p.Content[i+1]
		row, ok := parents[k.Value]
		if !ok {
			return
		}
		switch row.shape {
		case ownList:
			yield(childList{list: v, slot: &p.Content[i+1], keyLine: k.Line})
		case inner:
			innerLists(v, "", "", yield)
		case cases:
			if isNull(v) {
				return
			}
			if v.Kind != yaml.SequenceNode {
				yield(childList{bad: v, want: "a list"})
				return
			}
			for c, cs := range v.Content {
				if !innerLists(cs, "["+strconv.Itoa(c)+"]", "."+strconv.Itoa(c), yield) {
					return
				}
			}
		}
	}
}

// innerLists yields the processors fields of mapping m, whose suffixes are
// path and eng. It returns false when yield asked to stop.
func innerLists(m *yaml.Node, path, eng string, yield func(childList) bool) bool {
	if isNull(m) {
		return true
	}
	if m.Kind != yaml.MappingNode {
		return yield(childList{path: path, eng: eng, bad: m, want: "a mapping"})
	}
	for j := 0; j+1 < len(m.Content); j += 2 {
		if m.Content[j].Value != "processors" {
			continue
		}
		cl := childList{
			list: m.Content[j+1], slot: &m.Content[j+1],
			path: path + ".processors", eng: eng + ".processors",
			keyLine: m.Content[j].Line,
		}
		if !yield(cl) {
			return false
		}
	}
	return true
}

// walker wraps child processor lists in steppers (AD-6), giving each a
// dense id in depth-first pre-order (AD-16) and recording its stops.
type walker struct {
	next int
	// strict is the value of every stepper's strict field: a bool, or the
	// interpolation as written.
	strict *yaml.Node
	stops  map[stop]core.Position
	// paths maps engine paths (AD-6) to user paths, and bases the engine
	// path of each mapping, mutation or bloblang processor to the file line
	// of its first Bloblang line.
	paths    map[string]string
	bases    map[string]int
	warnings []string
	// tainted holds the processor_resources labels used inside a
	// concurrent parent; they stay unwrapped.
	tainted map[string]bool
	// src holds the lines of the user's file, procs the user path of each
	// processor, child list and component node, and anchors the source
	// node each synthesized stepper node stands for: the position map of
	// every encoded part is built from them (FilePosition).
	src     []string
	procs   map[*yaml.Node]string
	anchors map[*yaml.Node]*yaml.Node
}

// wrap returns the stepper processor around list, the child list at path
// whose key is on keyLine, and wraps the lists inside its children. eng is
// the engine path the stepper is built at.
func (w *walker) wrap(list *yaml.Node, path, eng string, keyLine, parent int, k kind) (*yaml.Node, error) {
	id := w.next
	w.next++
	// The stepper, its config and its child list all stand for the list.
	w.paths[eng] = path
	w.paths[eng+"."+StepperName] = path
	w.paths[eng+"."+StepperName+".processors"] = path
	w.procs[list] = path
	lines := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Style: yaml.FlowStyle}
	for i, c := range list.Content {
		cp := child(path, i)
		ce := eng + "." + StepperName + ".processors." + strconv.Itoa(i)
		w.paths[ce] = cp
		w.procs[c] = cp
		w.stops[stop{id, i}] = core.Position{UserPath: cp, Line: c.Line}
		lines.Content = append(lines.Content, scalar("!!int", strconv.Itoa(c.Line)))
		if err := w.processor(c, cp, ce, id); err != nil {
			return nil, err
		}
	}
	// The end-of-entry stop (AD-10).
	w.stops[stop{id, len(list.Content)}] = core.Position{UserPath: path, Line: keyLine}

	strict := w.strict
	if k.noStrict {
		strict = scalar("!!bool", "false")
	}
	conf := []*yaml.Node{
		scalar("!!str", "id"), scalar("!!int", strconv.Itoa(id)),
		scalar("!!str", "parent"), scalar("!!int", strconv.Itoa(parent)),
		scalar("!!str", "mode"), scalar("!!str", k.mode),
		scalar("!!str", "strict"), strict,
		scalar("!!str", "concurrent"), scalar("!!bool", "false"),
	}
	if k.repeat != "" {
		conf = append(conf, scalar("!!str", "repeat"), scalar("!!str", k.repeat))
	}
	if k.repeat == repeatAttempt {
		// max_retries as written: an interpolation resolves in the engine.
		maxAttempts := k.maxAttempts
		if maxAttempts == nil {
			maxAttempts = scalar("!!int", "0")
		}
		conf = append(conf, scalar("!!str", "max_attempts"), maxAttempts)
	}
	conf = append(conf,
		scalar("!!str", "lines"), lines,
		scalar("!!str", "processors"), list,
	)
	// The wrapper's own keys belong to the list it wraps.
	confNode := &yaml.Node{Kind: yaml.MappingNode, Content: conf}
	st := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{scalar("!!str", StepperName), confNode}}
	w.anchors[st], w.anchors[confNode] = list, list
	return st, nil
}

// list wraps the child list v at path (key on keyLine) and returns its
// replacement, a one-element list holding the stepper, or nil when there is
// nothing to step. eng is the engine path of the list.
func (w *walker) list(v *yaml.Node, path, eng string, keyLine, parent int, k kind) (*yaml.Node, error) {
	if isNull(v) {
		return nil, nil
	}
	if v.Kind != yaml.SequenceNode {
		return nil, invalid(v.Line, path, "%s is not a list", path)
	}
	if len(v.Content) == 0 {
		return nil, nil
	}
	st, err := w.wrap(v, path, eng+".0", keyLine, parent, k)
	if err != nil {
		return nil, err
	}
	seq := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Content: []*yaml.Node{st}}
	w.anchors[seq] = v
	return seq, nil
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
	if _, ok := parents[name]; ok {
		return parent, ""
	}
	if (name == "processors" && v.Kind == yaml.SequenceNode) || holdsProcessors(v) {
		return unknown, name + " is not a parent processor the debugger knows" + opaque
	}
	return leaf, ""
}

// processor wraps the child lists of processor p at path, engine path eng,
// inside stepper parent, per the AD-7 table.
func (w *walker) processor(p *yaml.Node, path, eng string, parent int) error {
	if p.Kind != yaml.MappingNode {
		return nil // Not a processor config: the engine reports it.
	}
	for i := 0; i+1 < len(p.Content); i += 2 {
		k, v := p.Content[i], p.Content[i+1]
		if k.Value == "label" {
			continue
		}
		cp, ce := path+"."+k.Value, eng+"."+k.Value
		if base, ok := baseLine(k, v); ok {
			w.bases[eng] = base
		}
		class, msg := classify(k.Value, v)
		if class == concurrent || class == unknown {
			w.warn(k.Line, cp, "%s", msg)
			continue
		}
		if class == leaf {
			continue
		}
		pk := parentKind(k.Value, v)
		for cl := range childLists(p, i) {
			lp := cp + cl.path
			if cl.bad != nil {
				return invalid(cl.bad.Line, lp, "%s is not %s", lp, cl.want)
			}
			st, err := w.list(cl.list, lp, ce+cl.eng, cl.keyLine, parent, pk)
			if err != nil {
				return err
			}
			if st != nil {
				*cl.slot = st
			}
		}
	}
	return nil
}

// resources wraps the child lists inside every processor_resources entry,
// except those used inside a concurrent parent.
func (w *walker) resources(list *yaml.Node) error {
	for i, entry := range list.Content {
		path := child("processor_resources", i)
		eng := ResourcePath + "." + strconv.Itoa(i)
		label := ""
		if l := value(entry, "label"); l != nil && l.Kind == yaml.ScalarNode && l.Value != "" {
			label = l.Value
			path = "processor_resources." + label
			eng = QualifyResource(ResourcePath, label)
		}
		w.paths[eng] = path
		w.procs[entry] = path
		if label != "" && w.tainted[label] {
			// Unwrapped, so only the entry's own mapping has a base line.
			w.entryBases(entry, eng)
			w.warn(entry.Line, path, "this resource is used inside a parent that runs as one step (a concurrent or unknown parent), so it runs as one step and nothing inside it is stepped")
			continue
		}
		if err := w.processor(entry, path, eng, -1); err != nil {
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
			for cl := range childLists(p, i) {
				scanList(cl.list, out)
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
