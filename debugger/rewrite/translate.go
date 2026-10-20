package rewrite

import (
	"strconv"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// ResourcePath is the engine path every processor_resources entry logs
// under. The engine tells entries apart only by the label attribute logged
// right after it, so log handlers qualify the path with QualifyResource
// before they translate it.
const ResourcePath = "root.processor_resources"

// QualifyResource returns enginePath, a path the engine logged for a
// processor_resources entry (ResourcePath or below it), with the entry's
// label inserted after ResourcePath: the form Translate expects. Any other
// path, or an empty label, comes back unchanged.
func QualifyResource(enginePath, label string) string {
	if label == "" {
		return enginePath
	}
	if enginePath == ResourcePath || strings.HasPrefix(enginePath, ResourcePath+".") {
		return ResourcePath + "." + label + enginePath[len(ResourcePath):]
	}
	return enginePath
}

var _ core.Translate = Plan{}

// Translate implements core.Translate (AD-6). The longest engine path
// prefix the rewrite recorded (a stepper, its child list or one of its
// children) becomes its user path, and every later numeric segment becomes
// an index: root.pipeline.processors.1.rpcn_debug_stepper.processors.2.
// branch.processors.0.rpcn_debug_stepper.processors.0 is
// pipeline.processors[2].branch.processors[0] when the pipeline has no
// input.processors. A resource path must be qualified first
// (QualifyResource).
func (p Plan) Translate(enginePath string) (string, bool) {
	for prefix := enginePath; prefix != ""; {
		if user, ok := p.paths[prefix]; ok {
			return user + indexSegments(enginePath[len(prefix):]), true
		}
		i := strings.LastIndexByte(prefix, '.')
		if i < 0 {
			break
		}
		prefix = prefix[:i]
	}
	return enginePath, false
}

// indexSegments turns the remainder of an engine path (".branch.processors.0")
// into the user style (".branch.processors[0]").
func indexSegments(rest string) string {
	if rest == "" {
		return ""
	}
	var b strings.Builder
	for seg := range strings.SplitSeq(strings.TrimPrefix(rest, "."), ".") {
		if _, err := strconv.Atoi(seg); err == nil && seg != "" {
			b.WriteString("[" + seg + "]")
			continue
		}
		b.WriteString("." + seg)
	}
	return b.String()
}

// TranslateLine implements core.Translate: line, 1-based inside the
// Bloblang of the processor at enginePath, becomes base + line - 1, where
// base is the AD-6 base line.
func (p Plan) TranslateLine(enginePath string, line int) (int, bool) {
	base, ok := p.bases[enginePath]
	if !ok || line < 1 {
		return line, false
	}
	return base + line - 1, true
}

// bloblangKeys are the processors whose value is a Bloblang mapping.
var bloblangKeys = map[string]bool{"mapping": true, "mutation": true, "bloblang": true}

// baseLine returns the file line of the first Bloblang line of the
// processor field k: v, when k is a Bloblang processor (AD-6): a literal
// block starts on the line after its key, and a plain scalar on the key's
// line starts there. Folded and quoted values have none.
func baseLine(k, v *yaml.Node) (int, bool) {
	if !bloblangKeys[k.Value] || v.Kind != yaml.ScalarNode {
		return 0, false
	}
	switch {
	case v.Style&yaml.LiteralStyle != 0:
		return v.Line + 1, true
	case v.Style == 0 && v.Line == k.Line:
		return v.Line, true
	}
	return 0, false
}

// entryBases records the base line of processor p's own Bloblang field, at
// engine path eng, without walking into it.
func (w *walker) entryBases(p *yaml.Node, eng string) {
	if p.Kind != yaml.MappingNode {
		return
	}
	for i := 0; i+1 < len(p.Content); i += 2 {
		if base, ok := baseLine(p.Content[i], p.Content[i+1]); ok {
			w.bases[eng] = base
		}
	}
}
