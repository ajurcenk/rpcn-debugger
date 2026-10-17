package rewrite

import (
	"errors"
	"fmt"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// StepperName is the stepper plugin that wraps a child processor list.
const StepperName = "rpcn_debug_stepper"

// pipelineStepperID is the id of the stepper wrapping pipeline.processors.
// It is the only stepper in this entry.
const pipelineStepperID = 0

// Plan is the result of one rewrite: the parts the engine builds its capture
// and step streams from, plus the LineMap of the steppers it wrote.
type Plan struct {
	// Input is the user's input block, as YAML for StreamBuilder.AddInputYAML.
	Input string
	// Resources holds the top-level *_resources sections, as YAML for
	// StreamBuilder.AddResourcesYAML. It is empty when there are none.
	Resources string
	// Stepper is the rpcn_debug_stepper wrapping pipeline.processors, as YAML
	// for StreamBuilder.AddProcessorYAML.
	Stepper string
	// Children are the stepper's children, in order.
	Children []Child
}

// Child is one child processor of a stepper.
type Child struct {
	// Path is the child's user path, e.g. pipeline.processors[0].
	Path string
	// Line is the child's 1-based line in the user's file.
	Line int
}

// Position implements core.LineMap.
func (p Plan) Position(stepperID, childIndex int) (core.Position, bool) {
	if stepperID != pipelineStepperID || childIndex < 0 || childIndex >= len(p.Children) {
		return core.Position{}, false
	}
	c := p.Children[childIndex]
	return core.Position{UserPath: c.Path, Line: c.Line}, true
}

// Rewrite turns pipeline YAML into a Plan. It wraps only pipeline.processors
// (one stepper, id 0); the rest of the wrapping list (AD-6) comes later. The
// input block is kept as written. Rewrite never writes anything to disk.
func Rewrite(src []byte) (Plan, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(src, &doc); err != nil {
		return Plan{}, err // yaml.v3 errors carry "line N".
	}
	if doc.Kind != yaml.DocumentNode || len(doc.Content) != 1 || doc.Content[0].Kind != yaml.MappingNode {
		return Plan{}, errors.New("the pipeline is not a YAML mapping")
	}
	root := doc.Content[0]

	input := value(root, "input")
	if input == nil {
		// Without one the engine falls back to stdin.
		return Plan{}, errors.New("the pipeline has no top-level input")
	}
	if input.Kind != yaml.MappingNode {
		return Plan{}, fmt.Errorf("line %d: input is not a mapping", input.Line)
	}
	// stdin is the DAP stream in stdio mode (AD-15); it is a rejected input
	// (AD-5). The rest of the stdio rules come with the full rewrite.
	for i := 0; i+1 < len(input.Content); i += 2 {
		if k := input.Content[i]; k.Value == "stdin" {
			return Plan{}, fmt.Errorf("line %d: the stdin input can't be debugged: stdin carries the debugger's DAP stream", k.Line)
		}
	}

	var plan Plan
	var err error
	if plan.Input, err = encode(input); err != nil {
		return Plan{}, err
	}

	resources := &yaml.Node{Kind: yaml.MappingNode}
	for i := 0; i+1 < len(root.Content); i += 2 {
		if strings.HasSuffix(root.Content[i].Value, "_resources") {
			resources.Content = append(resources.Content, root.Content[i], root.Content[i+1])
		}
	}
	if len(resources.Content) > 0 {
		if plan.Resources, err = encode(resources); err != nil {
			return Plan{}, err
		}
	}

	procs := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq"}
	if pipeline := value(root, "pipeline"); !isNull(pipeline) {
		if pipeline.Kind != yaml.MappingNode {
			return Plan{}, fmt.Errorf("line %d: pipeline is not a mapping", pipeline.Line)
		}
		if p := value(pipeline, "processors"); !isNull(p) {
			if p.Kind != yaml.SequenceNode {
				return Plan{}, fmt.Errorf("line %d: pipeline.processors is not a list", p.Line)
			}
			procs = p
		}
	}

	lines := &yaml.Node{Kind: yaml.SequenceNode, Tag: "!!seq", Style: yaml.FlowStyle}
	for i, child := range procs.Content {
		plan.Children = append(plan.Children, Child{
			Path: fmt.Sprintf("pipeline.processors[%d]", i),
			Line: child.Line,
		})
		lines.Content = append(lines.Content, scalar("!!int", fmt.Sprint(child.Line)))
	}

	stepper := &yaml.Node{Kind: yaml.MappingNode, Content: []*yaml.Node{
		scalar("!!str", StepperName),
		{Kind: yaml.MappingNode, Content: []*yaml.Node{
			scalar("!!str", "id"), scalar("!!int", fmt.Sprint(pipelineStepperID)),
			scalar("!!str", "lines"), lines,
			scalar("!!str", "processors"), procs,
		}},
	}}
	if plan.Stepper, err = encode(stepper); err != nil {
		return Plan{}, err
	}
	return plan, nil
}

// value returns the value of key in mapping m, or nil.
func value(m *yaml.Node, key string) *yaml.Node {
	for i := 0; i+1 < len(m.Content); i += 2 {
		if m.Content[i].Value == key {
			return m.Content[i+1]
		}
	}
	return nil
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
		return "", fmt.Errorf("encode the rewritten config: %w", err)
	}
	return string(out), nil
}
