package rewrite

import (
	"strconv"

	"gopkg.in/yaml.v3"
)

// refChecker finds a processor resource reference with no matching
// processor_resources label. Benthos would otherwise retry the lookup for
// every message, and the session would hang.
type refChecker struct {
	labels map[string]bool
	err    error
}

// checkResourceRefs returns a PIPELINE_INVALID error for the first
// reference (in file order of the lists, then the resources) to a processor
// resource that isn't defined. lists are the top-level processor lists at
// the user paths in paths; input and inputResources are searched for the
// processor lists nested inside them. Interpolated labels or references are skipped:
// only the engine can resolve them.
func checkResourceRefs(lists []*yaml.Node, paths []string, resources, input, inputResources *yaml.Node) error {
	c := &refChecker{labels: map[string]bool{}}
	if !isNull(resources) {
		for _, e := range resources.Content {
			l := value(e, "label")
			if isInterpolated(l) {
				return nil // Some label is only known at runtime.
			}
			if l != nil && l.Kind == yaml.ScalarNode {
				c.labels[l.Value] = true
			}
		}
	}
	for i, l := range lists {
		c.list(l, paths[i])
	}
	if !isNull(resources) {
		for i, e := range resources.Content {
			c.processor(e, resourcePath("processor_resources", i, e))
		}
	}
	// Processor lists inside the input tree and input_resources run during
	// capture (the input's own top-level list is already in lists).
	c.search(input, "input", false)
	if !isNull(inputResources) && inputResources.Kind == yaml.SequenceNode {
		for i, e := range inputResources.Content {
			c.search(e, resourcePath("input_resources", i, e), false)
		}
	}
	return c.err
}

// resourcePath is the user path of entry i of the resource list section,
// as the walker names it: by its label, or by its index when it has none.
func resourcePath(section string, i int, e *yaml.Node) string {
	if l := value(e, "label"); l != nil && l.Kind == yaml.ScalarNode && l.Value != "" {
		return section + "." + l.Value
	}
	return child(section, i)
}

func (c *refChecker) list(l *yaml.Node, path string) {
	if l == nil || l.Kind != yaml.SequenceNode {
		return
	}
	for i, p := range l.Content {
		c.processor(p, child(path, i))
	}
}

// processor checks processor p at path: a resource processor's label, the
// child lists of a known parent, and anything inside an opaque one.
func (c *refChecker) processor(p *yaml.Node, path string) {
	if c.err != nil || p == nil || p.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(p.Content); i += 2 {
		k, v := p.Content[i], p.Content[i+1]
		if k.Value == "label" {
			continue
		}
		cp := path + "." + k.Value
		if k.Value == "resource" {
			c.ref(v, path)
			continue
		}
		switch kind, _ := classify(k.Value, v); kind {
		case parent:
			switch k.Value {
			case "branch", "while", "retry":
				c.list(value(v, "processors"), cp+".processors")
			case "try", "catch", "for_each":
				c.list(v, cp)
			case "switch", "group_by":
				if v.Kind == yaml.SequenceNode {
					for n, cs := range v.Content {
						c.list(value(cs, "processors"), child(cp, n)+".processors")
					}
				}
			}
		case concurrent, unknown:
			c.opaque(v, cp)
		}
	}
}

// opaque checks the inside of an opaque parent's config n at path: any
// processors list in it, and a workflow's branch_resources.
func (c *refChecker) opaque(n *yaml.Node, path string) { c.search(n, path, true) }

// search walks config n at path for processor lists. With procs set, a list
// of mappings is a processor list (inside an opaque processor parent); in
// the input tree it isn't: there only `processors:` lists hold processors,
// and a list item like `resource: x` names an input resource.
func (c *refChecker) search(n *yaml.Node, path string, procs bool) {
	if c.err != nil || n == nil {
		return
	}
	switch n.Kind {
	case yaml.MappingNode:
		for i := 0; i+1 < len(n.Content); i += 2 {
			k, v := n.Content[i], n.Content[i+1]
			cp := path + "." + k.Value
			switch {
			case k.Value == "processors" && v.Kind == yaml.SequenceNode:
				c.list(v, cp)
			case k.Value == "branch_resources" && v.Kind == yaml.SequenceNode:
				for j, r := range v.Content {
					c.ref(r, child(cp, j))
				}
			default:
				c.search(v, cp, procs)
			}
		}
	case yaml.SequenceNode:
		// A list inside an opaque config, such as the value of the
		// `processors` processor, holds processors: check each one.
		for i, e := range n.Content {
			if procs && e.Kind == yaml.MappingNode {
				c.processor(e, path+"["+strconv.Itoa(i)+"]")
			} else {
				c.search(e, path+"["+strconv.Itoa(i)+"]", procs)
			}
		}
	}
}

// ref records an error when v names an undefined processor resource.
func (c *refChecker) ref(v *yaml.Node, path string) {
	if c.err != nil || v == nil || v.Kind != yaml.ScalarNode || isInterpolated(v) {
		return
	}
	if !c.labels[v.Value] {
		c.err = invalid(v.Line, path, "resource %q is not defined in processor_resources", v.Value)
	}
}
