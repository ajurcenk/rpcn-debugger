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
	// Strict is error_handling.strict, for the step stream (AD-4(b)). It is
	// false when strict is an interpolation (then only steppers get it).
	Strict bool

	stops map[stop]core.Position
	// paths and bases back Translate (see walker).
	paths map[string]string
	bases map[string]int
}

// stop is one LineMap key (AD-16).
type stop struct{ stepperID, childIndex int }

// Position implements core.LineMap. childIndex n (after the last child) is
// the end-of-entry stop, at the line of the key that owns the list (AD-10).
func (p Plan) Position(stepperID, childIndex int) (core.Position, bool) {
	pos, ok := p.stops[stop{stepperID, childIndex}]
	return pos, ok
}

// Rewrite turns pipeline YAML into a Plan (AD-4, AD-6). It never writes
// anything to disk. Its errors are core.Errors with code PIPELINE_INVALID,
// with Line and UserPath set when known.
func Rewrite(src []byte) (Plan, error) {
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
	if err := rejectStdin(input, "input"); err != nil {
		return Plan{}, err
	}
	if res := value(root, "input_resources"); res != nil {
		if err := rejectStdin(res, "input_resources"); err != nil {
			return Plan{}, err
		}
	}

	w := &walker{stops: map[stop]core.Position{}, paths: map[string]string{}, bases: map[string]int{}}
	var plan Plan
	plan.Strict, w.strict = strict(root)
	if isInterpolated(w.strict) {
		w.warn(w.strict.Line, "error_handling.strict", "strict is an interpolation, so it is applied per stepper only, not to the whole step stream")
	}
	var err error
	if plan.Input, err = encode(without(input, "processors")); err != nil {
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
	w.tainted = taintedResources(lists, resources)

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
		enc, err := encode(stepper)
		if err != nil {
			return Plan{}, err
		}
		plan.Steppers = append(plan.Steppers, enc)
	}
	if !isNull(resources) {
		if err := w.resources(resources); err != nil {
			return Plan{}, err
		}
	}
	w.warnNestedLists(input, "input", "runs during capture, not stepped")
	if out := value(root, "output"); out != nil {
		w.warnStdout(out, "output")
		w.warnNestedLists(out, "output", "is not run in dry-run")
	}
	if res := value(root, "output_resources"); res != nil {
		w.warnStdout(res, "output_resources")
	}

	// metrics, tracer, http and logger are dropped (AD-5): only the input,
	// the steppers and the *_resources sections reach the engine.
	res := &yaml.Node{Kind: yaml.MappingNode}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if strings.HasSuffix(root.Content[i].Value, "_resources") {
			res.Content = append(res.Content, root.Content[i], root.Content[i+1])
		}
	}
	if len(res.Content) > 0 {
		if plan.Resources, err = encode(res); err != nil {
			return Plan{}, err
		}
	}
	plan.Warnings, plan.stops, plan.paths, plan.bases = w.warnings, w.stops, w.paths, w.bases
	return plan, nil
}

// strict returns error_handling.strict: its value, and the node to write
// into each stepper's strict field. An interpolated value is written
// verbatim, for the engine to resolve, and reported as false.
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
