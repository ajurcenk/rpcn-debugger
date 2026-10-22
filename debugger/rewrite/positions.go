package rewrite

import (
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// posEntry is one node of an encoded part: where it starts in the encoded
// text, and where its source node is in the user's file.
type posEntry struct {
	encLine, encCol int
	line, col       int
	// encBlock is set when the encoded node is a literal or folded
	// scalar, block when the source node is one; indent is then the
	// 1-based column of its content in the file, and last the file line of
	// its last content line.
	encBlock, block bool
	indent, last    int
	// path is the user path of the innermost processor (or component)
	// holding the node, or "".
	path string
}

// posMap maps positions in one encoded part to the user's file. Its entries
// are in document order, which is position order.
type posMap struct {
	entries []posEntry
	lines   int // line count of the encoded text
	// requoted are the multi-line source values the encoder wrote on one
	// line (a block with trailing spaces becomes double-quoted): the engine
	// still counts the value's lines, so its positions for them land on
	// the lines after it, which belong to other nodes.
	requoted []requoted
}

// requoted is a multi-line source value encoded on line, with newlines
// line breaks in its value.
type requoted struct{ line, newlines int }

// FilePosition maps line and col, 1-based in the encoded text of part (as
// the engine reports them in lint errors), to the user's file: the file
// line and column, and the user path of the innermost processor or
// component there ("" when none). ok is false when the position is outside
// the part, or may belong to a multi-line value encoded on one line (see
// posMap.requoted), where no file position is exact (AD-6). fileCol is 0
// when the file line holds a ${VAR} interpolation: the engine lints the
// substituted value, so its columns don't match the file's.
//
// A position on a node's line maps by its offset from the node, the way the
// engine computes it from the node; inside a literal or folded block it
// maps to the block's content lines. A position before the first node of
// its line maps to that node.
func (p Plan) FilePosition(part string, line, col int) (fileLine, fileCol int, userPath string, ok bool) {
	m, found := p.positions[part]
	if !found || line < 1 || line > m.lines || len(m.entries) == 0 {
		return 0, 0, "", false
	}
	for _, r := range m.requoted {
		if line > r.line && line <= r.line+r.newlines {
			return 0, 0, "", false
		}
	}
	fileLine, fileCol, userPath, ok = m.lookup(line, col)
	if ok && fileLine >= 1 && fileLine <= len(p.src) && strings.Contains(p.src[fileLine-1], "${") {
		fileCol = 0
	}
	return fileLine, fileCol, userPath, ok
}

// lookup maps an encoded line and column of m to the file (FilePosition).
func (m posMap) lookup(line, col int) (fileLine, fileCol int, userPath string, ok bool) {
	// The last node starting on line at or before col.
	at, first, before := -1, -1, -1
	for i, e := range m.entries {
		switch {
		case e.encLine < line:
			before = i
		case e.encLine == line:
			if first < 0 {
				first = i
			}
			if e.encCol <= col {
				at = i
			}
		}
	}
	switch {
	case at >= 0:
		e := m.entries[at]
		l, c := e.offset(0, col-e.encCol)
		return l, c, e.path, true
	case first >= 0:
		e := m.entries[first]
		if e.block {
			l, c := e.offset(0, 1)
			return l, c, e.path, true
		}
		return e.line, e.col, e.path, true
	case before >= 0:
		e := m.entries[before]
		l, c := e.offset(line-e.encLine, col-e.encCol)
		return l, c, e.path, true
	}
	return 0, 0, "", false
}

// offset maps a position dl lines and dc columns from the node's start in
// the encoded text to the file. The engine reports an error at line l and
// column c (both 1-based) of a scalar's value at the node's line plus l (a
// block, whose content starts on the next line) or plus l-1 (a flow
// scalar), and at the node's column plus c. The same rule run backwards
// against the source node gives the file position: in a block, line l of
// its content, at column c of that line.
func (e posEntry) offset(dl, dc int) (int, int) {
	l := dl + 1
	if e.encBlock {
		l = dl
	}
	if e.block {
		return min(max(e.line+l, e.line+1), e.last), e.indent + max(dc, 1) - 1
	}
	return e.line + l - 1, max(e.col+dc, 1)
}

// encodeMapped encodes n like encode, and returns the position map of the
// result. It re-parses its own output and walks it in step with n: yaml.v3
// gives both trees the same shape, so each encoded node pairs with its
// source node. A synthesized node (Line 0) maps to its anchor in
// w.anchors, or else to the nearest enclosing source node.
func (w *walker) encodeMapped(n *yaml.Node) (string, posMap, error) {
	text, err := encode(n)
	if err != nil {
		return "", posMap{}, err
	}
	var doc yaml.Node
	if err := yaml.Unmarshal([]byte(text), &doc); err != nil || len(doc.Content) != 1 {
		return "", posMap{}, core.NewError(core.CodePipelineInvalid, "re-read the rewritten config: %v", err)
	}
	m := posMap{lines: strings.Count(strings.TrimSuffix(text, "\n"), "\n") + 1}
	w.mapNode(&m, doc.Content[0], n, firstSource(n), "")
	return text, m, nil
}

func (w *walker) mapNode(m *posMap, enc, src, anchor *yaml.Node, path string) {
	if src != nil {
		if src.Line > 0 {
			anchor = src
		} else if a, ok := w.anchors[src]; ok {
			// A stepper's wrapper belongs to the list it wraps.
			anchor = a
			if p, ok := w.procs[a]; ok {
				path = p
			}
		}
		if p, ok := w.procs[src]; ok {
			path = p
		}
	}
	e := posEntry{encLine: enc.Line, encCol: enc.Column, path: path}
	if anchor != nil {
		e.line, e.col = anchor.Line, w.valueColumn(anchor)
		if anchor == src && isBlock(src) {
			e.block, e.indent = true, w.contentIndent(src)
			e.last = src.Line + max(strings.Count(strings.TrimRight(src.Value, "\n"), "\n")+1, 1)
			e.encBlock = isBlock(enc)
		}
		if anchor == src && isBlock(src) && !isBlock(enc) && strings.Contains(src.Value, "\n") {
			m.requoted = append(m.requoted, requoted{line: enc.Line, newlines: strings.Count(src.Value, "\n")})
		}
	}
	if e.line > 0 {
		m.entries = append(m.entries, e)
	}
	same := src != nil && src.Kind == enc.Kind && len(src.Content) == len(enc.Content)
	for i, c := range enc.Content {
		var s *yaml.Node
		if same {
			s = src.Content[i]
		}
		// A subtree that lost step maps to the last source node.
		w.mapNode(m, c, s, anchor, path)
	}
}

func isBlock(n *yaml.Node) bool {
	return n.Kind == yaml.ScalarNode && n.Style&(yaml.LiteralStyle|yaml.FoldedStyle) != 0
}

// valueColumn returns the column where node n's value starts in the user's
// file: yaml.v3 puts a node's Column at its anchor or tag, when it has one.
func (w *walker) valueColumn(n *yaml.Node) int {
	if n.Line < 1 || n.Line > len(w.src) || n.Column < 1 {
		return n.Column
	}
	l, c := w.src[n.Line-1], n.Column-1
	for c < len(l) && (l[c] == '&' || l[c] == '!') {
		for c < len(l) && l[c] != ' ' && l[c] != '\t' {
			c++
		}
		for c < len(l) && (l[c] == ' ' || l[c] == '\t') {
			c++
		}
	}
	if c >= len(l) {
		return n.Column
	}
	return c + 1
}

// contentIndent returns the 1-based column of the first content line of the
// block scalar n in the user's file, or n's column when unknown.
func (w *walker) contentIndent(n *yaml.Node) int {
	for i := n.Line; i < len(w.src); i++ { // w.src[i] is file line i+1
		l := w.src[i]
		if t := strings.TrimLeft(l, " "); t != "" {
			return len(l) - len(t) + 1
		}
	}
	return n.Column
}

// firstSource returns the first node of n, in document order, that came
// from the user's file, or nil.
func firstSource(n *yaml.Node) *yaml.Node {
	if n == nil {
		return nil
	}
	if n.Line > 0 {
		return n
	}
	for _, c := range n.Content {
		if s := firstSource(c); s != nil {
			return s
		}
	}
	return nil
}
