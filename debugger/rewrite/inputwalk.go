package rewrite

import (
	"fmt"
	"slices"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// inputWalker walks the capture tree (AD-5): the top-level input, the
// child inputs of composites, and the input_resources that resource:
// names. It classifies every input by the input table, records one finding
// per input, and on the way overrides the consumer groups, forces
// auto_replay_nacks to true and warns about start offsets at latest.
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
	// base is the debug-group base (Options.DebugGroup), and keep the
	// launch's keepConsumerGroup: with an empty base or keep, no group is
	// overridden.
	base string
	keep bool
	// groups counts the inputs whose groups the walk overrode: the next
	// one gets <base>-<groups> (AD-5, D5).
	groups int
	// overrides is the launch's inputClasses (Options.InputClasses): the
	// class of each named component, applied before the rollup.
	overrides map[string]InputClass
	findings  []InputFinding
}

type inputResource struct {
	body *yaml.Node
	path string
}

type resClass struct {
	class  InputClass
	effect string
	// inherited: the class comes from a child or a named resource.
	inherited bool
}

func newInputWalker(w *walker, inputResources *yaml.Node, opts Options) *inputWalker {
	iw := &inputWalker{
		w: w, resources: map[string]inputResource{}, emitted: map[string]bool{},
		base: opts.DebugGroup, keep: opts.KeepConsumerGroup, overrides: opts.InputClasses,
	}
	if inputResources != nil && inputResources.Kind == yaml.SequenceNode {
		for i, e := range inputResources.Content {
			if l := resourceLabel(e); l != nil && !isInterpolated(l) {
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
		local := &inputWalker{w: iw.w, resources: iw.resources, base: iw.base, keep: iw.keep, overrides: iw.overrides, refs: func(ref string) {
			if !slices.Contains(referrers[ref], l) {
				referrers[ref] = append(referrers[ref], l)
			}
		}}
		c, e, inh := local.block(iw.resources[l].body, iw.resources[l].path)
		classes[l] = resClass{c, e, inh}
	}
	queue := slices.Clone(labels)
	for len(queue) > 0 {
		l := queue[0]
		queue = queue[1:]
		from := classes[l]
		for _, r := range referrers[l] {
			if rank(from.class) > rank(classes[r].class) {
				classes[r] = resClass{from.class, "through " + iw.resources[l].path + ": " + from.effect, true}
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
// class and effect, and whether the class is a child's or a named
// resource's. A block without a component has no finding and is
// replay-safe here; the engine reports it. An inputClasses override of the
// component replaces its own class before its children's are rolled up, so
// a composite follows its overridden children.
// hardRejected reports whether the input component name with config conf
// can't run in a debug session at all, so neither a group override nor
// inputClasses applies to it: the FixedInputs components, and a redpanda
// input without seed_brokers, which needs the shared client (E3 entry 6).
func hardRejected(name string, conf *yaml.Node) bool {
	if _, ok := core.FixedInputs[name]; ok {
		return true
	}
	return name == "redpanda" && !isSet(conf, "seed_brokers")
}

func (iw *inputWalker) block(b *yaml.Node, path string) (InputClass, string, bool) {
	k, conf := component(b)
	if k == nil {
		return ReplaySafe, "", false
	}
	name := k.Value
	row, known := inputTable[name]
	fixed := hardRejected(name, conf)
	if iw.refs == nil {
		if !fixed {
			iw.overrideGroups(row, conf, path+"."+name)
		}
		iw.forceReplayNacks(conf, path+"."+name)
		iw.warnLatest(row, conf, path+"."+name)
	}
	idx := len(iw.findings)
	iw.findings = append(iw.findings, InputFinding{UserPath: path, Line: k.Line, Component: name})

	class, effect := ReplaySafe, ""
	switch {
	case !known:
		class, effect = Unknown, "the debugger doesn't know what the "+name+" input does, so it is treated as side-effecting"
	case row.class != nil:
		class, effect = row.class(conf)
	}
	if rank(class) < rank(SideEffecting) {
		if g, ok := iw.keptGroup(row, conf); ok {
			class, effect = SideEffecting, g
		}
	}
	override, loosened := InputClass(""), false
	// A hard-rejected input stays rejected: the Session refuses an
	// override that loosens a FixedInputs component, and this keeps the
	// rewrite safe on its own, also for redpanda without seed_brokers.
	if !fixed {
		if o, ok := iw.overrides[name]; ok {
			override, loosened = o, rank(o) < rank(class)
			note := fmt.Sprintf("inputClasses marks %s %s", name, o)
			switch {
			case o == ReplaySafe:
				effect = ""
			case effect == "":
				effect = note
			case o != class:
				effect += " (" + note + ")"
			}
			class = o
		}
	}
	inherited := false
	worst := func(c InputClass, e, via string) {
		if rank(c) > rank(class) {
			class, effect, inherited = c, "through "+via+": "+e, true
		}
	}
	for _, cf := range row.children {
		cp := path + "." + name + "." + cf.name
		c := value(conf, cf.name)
		switch {
		case c == nil:
		case cf.shape == childOne:
			ce, ee, _ := iw.block(c, cp)
			worst(ce, ee, cp)
		case cf.shape == childInputs && c.Kind == yaml.SequenceNode:
			for i, e := range c.Content {
				ce, ee, _ := iw.block(e, child(cp, i))
				worst(ce, ee, child(cp, i))
			}
		case cf.shape == childMap && c.Kind == yaml.MappingNode:
			for i := 0; i+1 < len(c.Content); i += 2 {
				p := cp + "." + c.Content[i].Value
				ce, ee, _ := iw.block(c.Content[i+1], p)
				worst(ce, ee, p)
			}
		}
	}
	if name == "resource" {
		rc, re, via := iw.resource(conf)
		worst(rc, re, via)
	} else if len(row.children) == 0 {
		// The walker doesn't descend here (a leaf, or an unknown
		// composite): a stdin anywhere below would read the DAP stream
		// (AD-15), so it is a rejected finding of its own.
		iw.nestedStdin(conf, path+"."+name, worst)
	}
	f := &iw.findings[idx]
	f.Class, f.Effect, f.Inherited, f.Override, f.Loosened = class, effect, inherited, override, loosened
	return class, effect, inherited
}

// nestedStdin records a rejected finding for every stdin key in conf, the
// config at path of an input the walker doesn't descend into, and rolls
// each up into its input through worst.
func (iw *inputWalker) nestedStdin(conf *yaml.Node, path string, worst func(c InputClass, e, via string)) {
	walkKeys(conf, path, func(k *yaml.Node, p string) bool {
		if k.Value == "stdin" {
			c, e := inputTable["stdin"].class(nil)
			iw.findings = append(iw.findings, InputFinding{UserPath: p, Line: k.Line, Component: "stdin", Class: c, Effect: e})
			worst(c, e, p)
		}
		return false
	})
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
			f := &iw.findings[idx]
			f.Class, f.Effect, f.Inherited = rc.class, rc.effect, rc.inherited
		}
	}
	return rc.class, rc.effect, r.path
}

// overridable reports whether the capture replaces the group value n of an
// input of row with the debug group (AD-5): a literal, non-empty group of an
// overridable row, when the launch has a debug-group base and doesn't keep
// the groups. The walk replaces such a value with another one, so it holds
// for the replaced value too. In a local walk (resourceClasses) it says
// what the walk will do.
func (iw *inputWalker) overridable(row inputRow, n *yaml.Node) bool {
	return row.overridable && iw.base != "" && !iw.keep &&
		n != nil && n.Kind == yaml.ScalarNode && !isNull(n) && n.Value != "" && !isInterpolated(n)
}

// overrideGroups replaces each overridable group field of the input config
// conf at path with <base>-<k>, where k counts the inputs overridden so far
// in walker order, so distinct groups stay distinct (AD-5, D5), with a
// launch warning naming both groups. It replaces the value node, so lint
// errors at it keep the user's position.
func (iw *inputWalker) overrideGroups(row inputRow, conf *yaml.Node, path string) {
	group := ""
	for _, g := range row.groups {
		gk, gv := field(conf, g)
		if !iw.overridable(row, gv) {
			continue
		}
		if group == "" {
			group = fmt.Sprintf("%s-%d", iw.base, iw.groups)
			iw.groups++
		}
		ng := *gv
		ng.Value, ng.Tag, ng.Style = group, "!!str", 0
		conf.Content[slices.Index(conf.Content, gk)+1] = &ng
		iw.w.warn(gk.Line, path+"."+g, "%s %q is replaced by the debug group %q for the capture", groupNoun(row), gv.Value, group)
	}
}

// groupNoun names what the group fields of row hold.
func groupNoun(row inputRow) string {
	if row.queueGroup {
		return "queue group"
	}
	return "consumer group"
}

// queueShare is what a kept queue group does: core NATS queue members
// split the messages, and the capture's nack doesn't redeliver them.
const queueShare = "takes its share of the messages, which aren't redelivered after the nack"

// keptGroup returns the effect of the first group field of row in conf that
// is set and that the capture doesn't override: kept by keepConsumerGroup,
// interpolated, or a field the debugger leaves alone (AD-5, D4).
func (iw *inputWalker) keptGroup(row inputRow, conf *yaml.Node) (string, bool) {
	for _, g := range row.groups {
		n := fieldAt(conf, g)
		if !isSet(conf, g) || iw.overridable(row, n) {
			continue
		}
		literal := n.Kind == yaml.ScalarNode && !isInterpolated(n)
		switch {
		case row.queueGroup && literal:
			return fmt.Sprintf("joins queue group %q and %s", n.Value, queueShare), true
		case row.queueGroup:
			return fmt.Sprintf("joins the queue group in %s, which the debugger can't override, and %s", g, queueShare), true
		case literal:
			return fmt.Sprintf("joins consumer group %q (%s), and its members rebalance", n.Value, g), true
		}
		return fmt.Sprintf("joins the consumer group in %s, which the debugger can't override, and its members rebalance", g), true
	}
	return "", false
}

// warnLatest gives the input config conf at path one launch warning when a
// start-offset field of row resolves to latest (start_offset: latest, or
// start_from_oldest: false): the capture then waits for new records, or,
// under a kept group, starts at latest only when the group has no commits.
// In a row with start_offset, start_from_oldest doesn't decide the start
// (C/kafka/franz_reader.go:307-325), so only start_offset counts. The
// field itself stays as configured.
func (iw *inputWalker) warnLatest(row inputRow, conf *yaml.Node, path string) {
	hasOffset := slices.ContainsFunc(row.starts, func(s string) bool {
		return s == "start_offset" || strings.HasSuffix(s, ".start_offset")
	})
	for _, s := range row.starts {
		parent, last := conf, s
		if i := strings.LastIndexByte(s, '.'); i >= 0 {
			parent, last = fieldAt(conf, s[:i]), s[i+1:]
		}
		if hasOffset && last == "start_from_oldest" {
			continue
		}
		k, v := field(parent, last)
		if k == nil || isNull(v) || v.Kind != yaml.ScalarNode || isInterpolated(v) {
			continue
		}
		var oldest bool
		latest := (last == "start_offset" && strings.EqualFold(v.Value, "latest")) ||
			(last == "start_from_oldest" && v.ShortTag() == "!!bool" && v.Decode(&oldest) == nil && !oldest)
		if !latest {
			continue
		}
		if _, kept := iw.keptGroup(row, conf); kept {
			iw.w.warn(k.Line, path+"."+s, "%s: %s starts at the latest offset unless the kept group has committed offsets, so the capture may wait for new records and the \"Waiting for the input\" note may follow", last, v.Value)
		} else {
			iw.w.warn(k.Line, path+"."+s, "%s: %s starts at the latest offset, so the capture waits for new records and the \"Waiting for the input\" note may follow", last, v.Value)
		}
		return
	}
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
