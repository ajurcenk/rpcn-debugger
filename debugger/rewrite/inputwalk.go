package rewrite

import (
	"fmt"
	"slices"

	"gopkg.in/yaml.v3"
)

// inputWalker walks the capture tree (AD-5): the top-level input, the
// child inputs of composites, and the input_resources that resource:
// names. It classifies every input by the input table, records one finding
// per input, and forces auto_replay_nacks to true on the way.
type inputWalker struct {
	w *walker
	// resources maps each input_resources label to its entry and path.
	resources map[string]inputResource
	// classes holds the class and effect of each input resource, computed
	// once per Rewrite (resourceClasses); emitted marks the resources whose
	// findings the walk has recorded.
	classes map[string]resClass
	emitted map[string]bool
	// refs, when set, makes the walk local (resourceClasses): a resource:
	// child is reported to refs and adds nothing, no finding is kept and
	// nothing is forced.
	refs func(label string)
	// overridden holds the group nodes the capture replaced with the
	// debug group: they don't make the input side-effecting.
	overridden map[*yaml.Node]bool
	findings   []InputFinding
}

type inputResource struct {
	body *yaml.Node
	path string
}

type resClass struct {
	class  InputClass
	effect string
}

func newInputWalker(w *walker, inputResources *yaml.Node, overridden map[*yaml.Node]bool) *inputWalker {
	iw := &inputWalker{
		w: w, resources: map[string]inputResource{}, emitted: map[string]bool{}, overridden: overridden,
	}
	if inputResources != nil && inputResources.Kind == yaml.SequenceNode {
		for i, e := range inputResources.Content {
			if l := value(e, "label"); l != nil && l.Kind == yaml.ScalarNode && !isInterpolated(l) {
				iw.resources[l.Value] = inputResource{e, resourcePath("input_resources", i, e)}
			}
		}
	}
	iw.classes = iw.resourceClasses()
	return iw
}

// resourceClasses classifies each input resource once: its own subtree
// first, with resource: children as references only, then the worst class
// is propagated from each resource to its referrers to a fixed point, so a
// reference cycle ends and every member gets the class of the worst one.
func (iw *inputWalker) resourceClasses() map[string]resClass {
	labels := make([]string, 0, len(iw.resources))
	for l := range iw.resources {
		labels = append(labels, l)
	}
	slices.Sort(labels)
	classes := map[string]resClass{}
	referrers := map[string][]string{}
	for _, l := range labels {
		local := &inputWalker{w: iw.w, resources: iw.resources, overridden: iw.overridden, refs: func(ref string) {
			if !slices.Contains(referrers[ref], l) {
				referrers[ref] = append(referrers[ref], l)
			}
		}}
		c, e := local.block(iw.resources[l].body, iw.resources[l].path)
		classes[l] = resClass{c, e}
	}
	queue := slices.Clone(labels)
	for len(queue) > 0 {
		l := queue[0]
		queue = queue[1:]
		from := classes[l]
		for _, r := range referrers[l] {
			if from.class.rank() > classes[r].class.rank() {
				classes[r] = resClass{from.class, "through " + iw.resources[l].path + ": " + from.effect}
				queue = append(queue, r)
			}
		}
	}
	return classes
}

// component returns the key and config of the input block b's component:
// its first key other than label and processors.
func component(b *yaml.Node) (k, v *yaml.Node) {
	if b == nil || b.Kind != yaml.MappingNode {
		return nil, nil
	}
	for i := 0; i+1 < len(b.Content); i += 2 {
		if n := b.Content[i].Value; n != "label" && n != "processors" {
			return b.Content[i], b.Content[i+1]
		}
	}
	return nil, nil
}

// block classifies the input block b at path, its children first in
// pre-order (the block's finding comes before theirs), and returns its
// class and effect. A block without a component has no finding and is
// replay-safe here; the engine reports it.
func (iw *inputWalker) block(b *yaml.Node, path string) (InputClass, string) {
	k, conf := component(b)
	if k == nil {
		return ReplaySafe, ""
	}
	name := k.Value
	if iw.refs == nil {
		iw.forceReplayNacks(conf, path+"."+name)
	}
	idx := len(iw.findings)
	iw.findings = append(iw.findings, InputFinding{UserPath: path, Line: k.Line, Component: name})

	class, effect := ReplaySafe, ""
	row, known := inputTable[name]
	switch {
	case !known:
		class, effect = Unknown, "the debugger doesn't know what the "+name+" input does, so it is treated as side-effecting"
	case row.class != nil:
		class, effect = row.class(conf)
	}
	if class.rank() < SideEffecting.rank() {
		if g, ok := iw.keptGroup(conf, row.groups); ok {
			class, effect = SideEffecting, g
		}
	}
	worst := func(c InputClass, e, via string) {
		if c.rank() > class.rank() {
			class, effect = c, "through "+via+": "+e
		}
	}
	for _, cf := range row.children {
		cp := path + "." + name + "." + cf.name
		c := value(conf, cf.name)
		switch {
		case c == nil:
		case cf.shape == childOne:
			ce, ee := iw.block(c, cp)
			worst(ce, ee, cp)
		case cf.shape == childInputs && c.Kind == yaml.SequenceNode:
			for i, e := range c.Content {
				ce, ee := iw.block(e, child(cp, i))
				worst(ce, ee, child(cp, i))
			}
		case cf.shape == childMap && c.Kind == yaml.MappingNode:
			for i := 0; i+1 < len(c.Content); i += 2 {
				p := cp + "." + c.Content[i].Value
				ce, ee := iw.block(c.Content[i+1], p)
				worst(ce, ee, p)
			}
		}
	}
	if name == "resource" {
		rc, re, via := iw.resource(conf)
		worst(rc, re, via)
	}
	iw.findings[idx].Class, iw.findings[idx].Effect = class, effect
	return class, effect
}

// resource returns the class, effect and path of the input resource named
// by the scalar ref, from iw.classes. The first time a resource is named,
// the walk records its findings (and forces its auto_replay_nacks); the
// resource's own finding takes its class from iw.classes. In a local walk
// a defined resource adds nothing and is reported to iw.refs.
func (iw *inputWalker) resource(ref *yaml.Node) (InputClass, string, string) {
	if ref == nil || ref.Kind != yaml.ScalarNode || isInterpolated(ref) {
		return Unknown, "the input resource is only known at runtime, so it is treated as side-effecting", "input_resources"
	}
	r, ok := iw.resources[ref.Value]
	if !ok {
		return Unknown, fmt.Sprintf("no input resource is labelled %q", ref.Value), "input_resources"
	}
	if iw.refs != nil {
		iw.refs(ref.Value)
		return ReplaySafe, "", r.path
	}
	rc := iw.classes[ref.Value]
	if !iw.emitted[ref.Value] {
		iw.emitted[ref.Value] = true
		idx := len(iw.findings)
		iw.block(r.body, r.path)
		if idx < len(iw.findings) {
			iw.findings[idx].Class, iw.findings[idx].Effect = rc.class, rc.effect
		}
	}
	return rc.class, rc.effect, r.path
}

// keptGroup returns the effect of the first group field in conf that is set
// and that the capture didn't override (E3 entry 5 overrides the rest).
func (iw *inputWalker) keptGroup(conf *yaml.Node, groups []string) (string, bool) {
	for _, g := range groups {
		n := fieldAt(conf, g)
		if !isSet(conf, g) || iw.overridden[n] {
			continue
		}
		if n.Kind == yaml.ScalarNode && !isInterpolated(n) {
			return fmt.Sprintf("joins consumer group %q (%s), and its members rebalance", n.Value, g), true
		}
		return fmt.Sprintf("joins the consumer group in %s, which the debugger can't override, and its members rebalance", g), true
	}
	return "", false
}

// forceReplayNacks sets auto_replay_nacks to true in the input config conf
// at path when it is set to anything else, with a warning: with false, the
// capture's nack is treated as an ack (offsets are committed, checkpoints
// advance). An absent field is left absent: it defaults to true.
func (iw *inputWalker) forceReplayNacks(conf *yaml.Node, path string) {
	k, v := field(conf, "auto_replay_nacks")
	if k == nil || isNull(v) {
		return
	}
	var b bool
	if v.Kind == yaml.ScalarNode && v.ShortTag() == "!!bool" && v.Decode(&b) == nil && b {
		return
	}
	nv := *v
	nv.Kind, nv.Value, nv.Tag, nv.Style, nv.Content = yaml.ScalarNode, "true", "!!bool", 0, nil
	i := slices.Index(conf.Content, k)
	conf.Content[i+1] = &nv
	iw.w.warn(k.Line, path+".auto_replay_nacks", "auto_replay_nacks is set to true for the capture: otherwise the capture's nack would be treated as an ack, committing offsets or advancing checkpoints")
}
