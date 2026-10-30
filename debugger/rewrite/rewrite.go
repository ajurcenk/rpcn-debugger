package rewrite

import (
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// StepperName is the stepper plugin that wraps a child processor list.
const StepperName = "rpcn_debug_stepper"

// Plan is the result of one rewrite (AD-4): the capture config, the step
// config's parts, the launch warnings, and the LineMap of the steppers it
// wrote.
type Plan struct {
	// Input is the capture config: the user's input block without
	// input.processors, as YAML for StreamBuilder.AddInputYAML.
	Input string
	// Resources holds the top-level *_resources sections, as YAML for
	// StreamBuilder.AddResourcesYAML, with the child lists inside
	// processor_resources wrapped. It is empty when there are none.
	Resources string
	// Steppers are the top-level rpcn_debug_stepper configs, as YAML for
	// StreamBuilder.AddProcessorYAML, in order: input.processors,
	// pipeline.processors, output.processors. Empty lists have none.
	Steppers []string
	// Warnings are the launch warnings for the user, in rewrite order.
	Warnings []string
	// Inputs are the findings of the input-tree walk (AD-5), one per
	// input in walker order (pre-order, resources where first named). The
	// rewrite refuses nothing on them; the Session decides.
	Inputs []InputFinding
	// Strict is error_handling.strict, for the step stream (AD-4(b)). It is
	// false when strict is an interpolation: StrictExpr then holds it.
	Strict bool
	// StrictExpr is error_handling.strict verbatim (as a YAML scalar, with
	// its quoting) when it is a ${VAR} interpolation, and "" otherwise. The
	// rewrite has no env (AD-22), so the engine resolves it for the step
	// stream; the steppers get it verbatim and resolve it themselves.
	StrictExpr string
	// StrictLine is the file line of the StrictExpr scalar, or 0.
	StrictLine int
	// Output is the step stream's output config (AD-21), as YAML for
	// StreamBuilder.AddOutputYAML: rpcn_debug_sink with the mode of
	// Options.Output and, under output: real, the user's output block
	// without its top-level processors and with stdout and stderr replaced
	// by drop.
	Output string
	// Delivery is where rpcn_debug_sink's Delivered Snapshots are
	// positioned (AD-21): the user path output and the line of the
	// top-level output key, or 0 when there is none.
	Delivery core.Position
	// Options are the rewrite options this Plan was made with. They are
	// read-only.
	Options Options

	stops map[stop]core.Position
	// paths and bases back Translate (see walker).
	paths map[string]string
	bases map[string]int
	// positions backs FilePosition: one map per encoded part, and src
	// holds the lines of the user's file.
	positions map[string]posMap
	src       []string
}

// stop is one LineMap key (AD-16).
type stop struct{ stepperID, childIndex int }

// Position implements core.LineMap. childIndex n (after the last child) is
// the end-of-entry stop, at the line of the key that owns the list (AD-10).
func (p Plan) Position(stepperID, childIndex int) (core.Position, bool) {
	pos, ok := p.stops[stop{stepperID, childIndex}]
	return pos, ok
}

// Options are the rewrite's inputs besides the YAML. The Session supplies
// them, so the rewrite stays pure (AD-5).
type Options struct {
	// DebugGroup is the debug-group base rpcn-debug-<os-user> that
	// overridden consumer-group fields derive from (AD-5): the k-th
	// overridden input in walker order gets <DebugGroup>-<k>. Empty
	// overrides nothing.
	DebugGroup string
	// KeepConsumerGroup is the launch's keepConsumerGroup: no group is
	// overridden, so a set group field makes its input side-effecting.
	KeepConsumerGroup bool
	// InputClasses maps an input component name to the class it gets
	// instead of the input table's (the launch's inputClasses, AD-5). It
	// replaces the component's own class before a composite takes its
	// worst child's, and a finding records when it loosened the class. The
	// Session validates the values and the engine the names; a
	// core.FixedInputs component keeps its class whatever it says.
	InputClasses map[string]InputClass
	// Output is the launch's output mode (AD-21): where rpcn_debug_sink
	// sends what it receives. "" is dry-run.
	Output core.OutputMode
}

// Rewrite turns pipeline YAML into a Plan (AD-4, AD-6), with opts. It never
// writes anything to disk. Its errors are core.Errors with code
// PIPELINE_INVALID, with Line and UserPath set when known.
func Rewrite(src []byte, opts Options) (Plan, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(src, &doc); err != nil {
		return Plan{}, yamlError(err)
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return Plan{}, invalid(0, "", "the pipeline is not a YAML mapping")
	}
	// Aliases go first: a re-encoded subtree would lose the anchor they
	// point to, and wrapping must not reach one node through two paths.
	root := resolve(doc.Content[0])

	input := value(root, "input")
	if input == nil {
		// Without one the engine falls back to stdin.
		return Plan{}, invalid(0, "", "the pipeline has no top-level input")
	}
	if input.Kind != yaml.MappingNode {
		return Plan{}, invalid(input.Line, "input", "input is not a mapping")
	}

	w := &walker{
		stops: map[stop]core.Position{}, paths: map[string]string{}, bases: map[string]int{},
		src: strings.Split(string(src), "\n"), procs: map[*yaml.Node]string{}, anchors: map[*yaml.Node]*yaml.Node{},
	}
	plan := Plan{Options: opts, positions: map[string]posMap{}, src: w.src}
	plan.Strict, w.strict = strict(root)
	if isInterpolated(w.strict) {
		// Encoded, so a quoted scalar stays quoted, as in the steppers;
		// without comments, which would land in front of the value.
		c := *w.strict
		c.HeadComment, c.LineComment, c.FootComment = "", "", ""
		enc, err := encode(&c)
		if err != nil {
			return Plan{}, err
		}
		plan.StrictExpr, plan.StrictLine = strings.TrimSuffix(enc, "\n"), w.strict.Line
	}
	capture := without(input, "processors")
	iw := newInputWalker(w, value(root, "input_resources"), opts)
	iw.block(capture, "input")
	iw.unreached()
	plan.Inputs = iw.findings
	w.procs[capture] = "input"
	var err error
	if plan.Input, plan.positions[core.PartInput], err = w.encodeMapped(capture); err != nil {
		return Plan{}, err
	}

	// The top-level lists, in step order.
	sections := []struct {
		name string
		node *yaml.Node
	}{{"input", input}, {"pipeline", value(root, "pipeline")}, {"output", value(root, "output")}}
	lists := make([]*yaml.Node, len(sections))
	listKeys := make([]*yaml.Node, len(sections))
	for i, s := range sections {
		if isNull(s.node) {
			continue
		}
		if s.node.Kind != yaml.MappingNode {
			return Plan{}, invalid(s.node.Line, s.name, "%s is not a mapping", s.name)
		}
		k, v := field(s.node, "processors")
		if isNull(v) {
			continue
		}
		if v.Kind != yaml.SequenceNode {
			return Plan{}, invalid(v.Line, s.name+".processors", "%s.processors is not a list", s.name)
		}
		lists[i], listKeys[i] = v, k
	}
	resources := value(root, "processor_resources")
	if !isNull(resources) && resources.Kind != yaml.SequenceNode {
		return Plan{}, invalid(resources.Line, "processor_resources", "processor_resources is not a list")
	}

	// An undefined resource reference would hang the run (entry 2.13).
	paths := make([]string, len(sections))
	for i, s := range sections {
		paths[i] = s.name + ".processors"
	}
	if err := checkResourceRefs(lists, paths, resources, input, value(root, "input_resources")); err != nil {
		return Plan{}, err
	}

	// Resources used inside a concurrent parent stay unwrapped (epic
	// decision 2026-10-16): their steppers would run on several goroutines.
	bodies := resourceBodies(resources)
	w.tainted = taintedResources(lists, resources, bodies)
	w.statefulRes = statefulResources(bodies)
	// Before any list is wrapped; the warnings go after the others.
	effectWarn := &walker{}
	effectWarn.warnEffects(effectSections{
		input: input, lists: lists, paths: paths,
		inputRes: value(root, "input_resources"), procRes: resources, cacheRes: value(root, "cache_resources"),
		rateRes: value(root, "rate_limit_resources"), outputRes: value(root, "output_resources"),
		output: value(root, "output"), real: opts.Output.Effective() == core.OutputReal,
	})

	for i, s := range sections {
		if lists[i] == nil || len(lists[i].Content) == 0 {
			continue
		}
		// The step config adds the top-level steppers in this order with
		// AddProcessorYAML, so top-level stepper k is built at
		// root.pipeline.processors.<k>.
		eng := "root.pipeline.processors." + strconv.Itoa(len(plan.Steppers))
		stepper, err := w.wrap(lists[i], s.name+".processors", eng, listKeys[i].Line, -1, all)
		if err != nil {
			return Plan{}, err
		}
		enc, pm, err := w.encodeMapped(stepper)
		if err != nil {
			return Plan{}, err
		}
		plan.positions[core.StepperPart(len(plan.Steppers))] = pm
		plan.Steppers = append(plan.Steppers, enc)
	}
	if !isNull(resources) {
		if err := w.resources(resources); err != nil {
			return Plan{}, err
		}
	}
	w.warnNestedLists(input, "input", "runs during capture, not stepped")
	plan.Delivery = core.Position{UserPath: "output"}
	mode := opts.Output.Effective()
	outKey, out := field(root, "output")
	outRes := value(root, "output_resources")
	// Under real, user and userRes are the copies written to (realOutput).
	var user, userRes *yaml.Node
	if mode == core.OutputReal {
		if !isNull(out) && out.Kind == yaml.MappingNode {
			if user, err = w.realOutput(without(out, "processors"), "output", true); err != nil {
				return Plan{}, err
			}
		}
		// The real warning goes first; realOutput added its own after it.
		n := len(w.warnings)
		w.warnReal(outKey, out, user)
		w.warnings = append(append(w.warnings[:0:0], w.warnings[n:]...), w.warnings[:n]...)
		if outRes != nil {
			// A real output may write through a resource output.
			if err = sharedResources(out, outRes); err != nil {
				return Plan{}, err
			}
			if userRes, err = w.realOutput(outRes, "output_resources", false); err != nil {
				return Plan{}, err
			}
		}
	}
	if out != nil {
		plan.Delivery.Line = outKey.Line
		// Under real the user's output runs its per-output processors as
		// in production (AD-21).
		if mode != core.OutputReal {
			w.warnStdout(out, "output", mode)
			w.warnNestedLists(out, "output", "is not run in dry-run")
			w.warnBatching(out, "output", false)
		}
	}
	if outRes != nil && mode != core.OutputReal {
		w.warnStdout(outRes, "output_resources", mode)
		if outRes.Kind == yaml.SequenceNode {
			for i, e := range outRes.Content {
				w.warnBatching(e, resourcePath("output_resources", i, e), false)
			}
		}
	}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if k, v := root.Content[i], root.Content[i+1]; topLevel[k.Value].warn != nil {
			topLevel[k.Value].warn(w, k, v)
		}
	}

	// Of the top-level sections (topLevel), only the input, the steppers
	// and the *_resources sections reach the engine (AD-5).
	res := &yaml.Node{Kind: yaml.MappingNode}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if k, v := root.Content[i], root.Content[i+1]; strings.HasSuffix(k.Value, "_resources") {
			if k.Value == "output_resources" && userRes != nil {
				v = userRes
			}
			res.Content = append(res.Content, k, v)
			if v.Kind == yaml.SequenceNode {
				for j, e := range v.Content {
					if _, ok := w.procs[e]; !ok {
						w.procs[e] = resourcePath(k.Value, j, e)
					}
				}
			}
		}
	}
	if len(res.Content) > 0 {
		if plan.Resources, plan.positions[core.PartResources], err = w.encodeMapped(res); err != nil {
			return Plan{}, err
		}
	}
	if plan.Output, plan.positions[core.PartOutput], err = w.encodeMapped(w.sinkConfig(outKey, user, mode)); err != nil {
		return Plan{}, err
	}
	w.warnings = append(w.warnings, effectWarn.warnings...)
	plan.Warnings, plan.stops, plan.paths, plan.bases = w.warnings, w.stops, w.paths, w.bases
	return plan, nil
}

// section is what the rewrite does with a top-level section (AD-5).
type section int

const (
	// dropped sections never reach the engine: the engine gets no-op
	// metrics and tracer and the AD-15 logger, and the rest only matter to
	// rpk connect.
	dropped section = iota
	// used sections are read by the rewrite itself: input is the capture
	// config, pipeline and output become steppers and the delivery
	// position, and error_handling sets strict.
	used
)

// topLevelSection is one allowlist entry: what happens to the section,
// and the launch warning given when it is dropped and set, or nil.
type topLevelSection struct {
	use  section
	warn func(w *walker, k, v *yaml.Node)
}

// topLevel is the allowlist of top-level sections (AD-5, E3 entry 6).
// Every section of the stock config schema is here or ends in _resources;
// the *_resources sections are carried to the capture and step streams.
// TestTopLevelAllowlist checks both, and that a dropped section reaches
// neither stream. Any other key is left to the engine's lint.
var topLevel = map[string]topLevelSection{
	"input": {use: used}, "pipeline": {use: used}, "output": {use: used}, "error_handling": {use: used},
	"buffer":   {use: dropped, warn: func(w *walker, k, v *yaml.Node) { w.warnBuffer(k, v) }},
	"redpanda": {use: dropped, warn: warnRedpanda},
	"metrics":  {}, "tracer": {}, "http": {}, "logger": {},
	"shutdown_delay": {}, "shutdown_timeout": {}, "tests": {},
}

// warnRedpanda warns when the top-level redpanda block (key k, value v) is
// set: only rpk connect sets up its shared client and topics.
func warnRedpanda(w *walker, k, v *yaml.Node) {
	if !isNull(v) {
		w.warn(k.Line, "redpanda", "the top-level redpanda block isn't used in a debug session: its shared client, logs_topic and status_topic are only set up by rpk connect")
	}
}

// strict returns error_handling.strict: its value, and the node to write
// into each stepper's strict field. An interpolated value is written
// verbatim, for the engine to resolve, and reported as false (Rewrite puts
// it in Plan.StrictExpr).
func strict(root *yaml.Node) (bool, *yaml.Node) {
	off := scalar("!!bool", "false")
	eh := value(root, "error_handling")
	if eh == nil || eh.Kind != yaml.MappingNode {
		return false, off
	}
	s := value(eh, "strict")
	if isInterpolated(s) {
		c := *s
		return false, &c
	}
	var b bool
	if s == nil || s.Decode(&b) != nil || !b {
		return false, off
	}
	return true, scalar("!!bool", "true")
}

// invalid returns a PIPELINE_INVALID core.Error.
func invalid(line int, path, format string, args ...any) core.Error {
	e := core.NewError(core.CodePipelineInvalid, format, args...)
	e.Line, e.UserPath = line, path
	return e
}

var yamlLine = regexp.MustCompile(`^yaml: line (\d+): `)

// yamlError turns a yaml.v3 parse error into a core.Error. When the text
// starts with "yaml: line N: ", Line is N and that prefix is dropped, since
// Error adds the line back.
func yamlError(err error) core.Error {
	e := core.NewError(core.CodePipelineInvalid, "%v", err)
	if m := yamlLine.FindStringSubmatch(e.Message); m != nil {
		e.Line, _ = strconv.Atoi(m[1])
		e.Message = "invalid YAML: " + strings.TrimPrefix(e.Message, m[0])
	}
	return e
}

// resolve returns a deep copy of n with every alias replaced by a copy of
// the node it points to, merge keys (<<) expanded with YAML merge semantics
// (explicit keys win, then earlier merge sources), and no anchors.
func resolve(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	if n.Kind == yaml.AliasNode && n.Alias != nil {
		return resolve(n.Alias)
	}
	c := *n
	c.Anchor, c.Alias = "", nil
	if len(n.Content) > 0 {
		c.Content = make([]*yaml.Node, len(n.Content))
		for i, child := range n.Content {
			c.Content[i] = resolve(child)
		}
	}
	if c.Kind == yaml.MappingNode {
		merge(&c)
	}
	return &c
}

// merge expands the merge keys of the resolved mapping m in place.
func merge(m *yaml.Node) {
	var explicit, sources []*yaml.Node
	for i := 0; i+1 < len(m.Content); i += 2 {
		k, v := m.Content[i], m.Content[i+1]
		if k.Kind != yaml.ScalarNode || k.ShortTag() != "!!merge" {
			explicit = append(explicit, k, v)
			continue
		}
		switch v.Kind {
		case yaml.MappingNode:
			sources = append(sources, v)
		case yaml.SequenceNode:
			for _, s := range v.Content {
				if s.Kind == yaml.MappingNode {
					sources = append(sources, s)
				}
			}
		}
	}
	if sources == nil {
		return
	}
	seen := map[string]bool{}
	for i := 0; i < len(explicit); i += 2 {
		seen[explicit[i].Value] = true
	}
	out := explicit
	for _, src := range sources {
		for i := 0; i+1 < len(src.Content); i += 2 {
			if k := src.Content[i]; !seen[k.Value] {
				seen[k.Value] = true
				out = append(out, k, src.Content[i+1])
			}
		}
	}
	m.Content = out
}

// isInterpolated reports whether scalar n holds a ${VAR} interpolation,
// which the engine resolves at runtime (AD-6, AD-22).
func isInterpolated(n *yaml.Node) bool {
	return n != nil && n.Kind == yaml.ScalarNode && strings.Contains(n.Value, "${")
}

// field returns the key and value nodes of key in mapping m, or nils.
func field(m *yaml.Node, key string) (k, v *yaml.Node) {
	if m == nil || m.Kind != yaml.MappingNode {
		return nil, nil
	}
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i], m.Content[i+1]
		}
	}
	return nil, nil
}

// value returns the value of key in mapping m, or nil.
func value(m *yaml.Node, key string) *yaml.Node {
	_, v := field(m, key)
	return v
}

// without returns a shallow copy of mapping m without key.
func without(m *yaml.Node, key string) *yaml.Node {
	c := *m
	c.Content = nil
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value != key {
			c.Content = append(c.Content, m.Content[i], m.Content[i+1])
		}
	}
	return &c
}

// isNull reports whether n is missing or a YAML null (an empty value), which
// Connect treats like a missing key.
func isNull(n *yaml.Node) bool {
	return n == nil || (n.Kind == yaml.ScalarNode && n.Tag == "!!null")
}

func scalar(tag, v string) *yaml.Node {
	return &yaml.Node{Kind: yaml.ScalarNode, Tag: tag, Value: v}
}

func encode(n *yaml.Node) (string, error) {
	out, err := yaml.Marshal(n)
	if err != nil {
		return "", core.NewError(core.CodePipelineInvalid, "encode the rewritten config: %v", err)
	}
	return string(out), nil
}

// child returns the user path of element i of the list at path.
func child(path string, i int) string { return fmt.Sprintf("%s[%d]", path, i) }
