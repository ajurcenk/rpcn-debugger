// Command gen-launch-schema writes the launch contract of dap.LaunchConfig
// into vscode/package.json (AD-12): contributes.debuggers[0]'s
// configurationAttributes (from dap.Schema) and initialConfigurations (from
// dap.InitialConfigurations). Every other part of the file, key order
// included, is kept.
//
//	go run ./cmd/gen-launch-schema           # rewrite the file (task gen:schema)
//	go run ./cmd/gen-launch-schema -check    # exit 1 if it is stale (task lint:schema)
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"slices"

	"github.com/ajurcenk/rpcn-debugger/debugger/dap"
)

func main() {
	file := flag.String("file", "vscode/package.json", "the extension manifest to update")
	check := flag.Bool("check", false, "don't write: exit 1 if the file is not up to date")
	flag.Parse()

	if err := run(*file, *check); err != nil {
		fmt.Fprintln(os.Stderr, "gen-launch-schema:", err)
		os.Exit(1)
	}
}

func run(file string, check bool) error {
	src, err := os.ReadFile(file)
	if err != nil {
		return err
	}
	out, err := generate(src)
	if err != nil {
		return fmt.Errorf("%s: %w", file, err)
	}
	if bytes.Equal(src, out) {
		return nil
	}
	if check {
		return fmt.Errorf("%s is stale: dap.LaunchConfig changed; run `task gen:schema` and commit the result", file)
	}
	info, err := os.Stat(file)
	if err != nil {
		return err
	}
	return os.WriteFile(file, out, info.Mode().Perm())
}

// generate returns the manifest src with the generated launch contract.
func generate(src []byte) ([]byte, error) {
	root, err := parse(src)
	if err != nil {
		return nil, err
	}
	debugger, err := root.path("contributes", "debuggers", 0)
	if err != nil {
		return nil, err
	}
	attrs, err := toNode(map[string]any{"launch": dap.Schema()})
	if err != nil {
		return nil, err
	}
	initial, err := toNode(dap.InitialConfigurations())
	if err != nil {
		return nil, err
	}
	// Entries read as VS Code writes them: type, request and name first.
	for _, entry := range initial.arr {
		entry.front("type", "request", "name")
	}
	debugger.set("configurationAttributes", attrs)
	debugger.set("initialConfigurations", initial)

	var buf bytes.Buffer
	if err := root.write(&buf, ""); err != nil {
		return nil, err
	}
	buf.WriteByte('\n')
	return buf.Bytes(), nil
}

// node is a JSON value that keeps its object keys in order.
type node struct {
	obj    []member // object members, when isObj
	isObj  bool
	arr    []*node // array items, when isArr
	isArr  bool
	scalar json.RawMessage // otherwise
}

type member struct {
	key string
	val *node
}

func parse(src []byte) (*node, error) {
	dec := json.NewDecoder(bytes.NewReader(src))
	dec.UseNumber()
	n, err := decode(dec)
	if err != nil {
		return nil, err
	}
	if _, err := dec.Token(); !errors.Is(err, io.EOF) {
		return nil, errors.New("trailing data after the JSON value")
	}
	return n, nil
}

func decode(dec *json.Decoder) (*node, error) {
	tok, err := dec.Token()
	if err != nil {
		return nil, err
	}
	switch t := tok.(type) {
	case json.Delim:
		switch t {
		case '{':
			n := &node{isObj: true, obj: []member{}}
			for dec.More() {
				kt, err := dec.Token()
				if err != nil {
					return nil, err
				}
				v, err := decode(dec)
				if err != nil {
					return nil, err
				}
				n.obj = append(n.obj, member{key: kt.(string), val: v})
			}
			_, err := dec.Token() // }
			return n, err
		case '[':
			n := &node{isArr: true, arr: []*node{}}
			for dec.More() {
				v, err := decode(dec)
				if err != nil {
					return nil, err
				}
				n.arr = append(n.arr, v)
			}
			_, err := dec.Token() // ]
			return n, err
		}
		return nil, fmt.Errorf("unexpected %v", t)
	default:
		raw, err := marshal(t)
		if err != nil {
			return nil, err
		}
		return &node{scalar: raw}, nil
	}
}

// toNode converts a generated value. Maps get sorted keys, as
// encoding/json writes them, so the output is stable.
func toNode(v any) (*node, error) {
	raw, err := marshal(v)
	if err != nil {
		return nil, err
	}
	return parse(raw)
}

// marshal encodes v without HTML escaping, so strings such as `<repo>`
// stay as written.
func marshal(v any) (json.RawMessage, error) {
	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(v); err != nil {
		return nil, err
	}
	return bytes.TrimRight(buf.Bytes(), "\n"), nil
}

// path walks object keys (string) and array indices (int) from n.
func (n *node) path(steps ...any) (*node, error) {
	cur := n
	for _, s := range steps {
		switch k := s.(type) {
		case string:
			next := cur.get(k)
			if next == nil {
				return nil, fmt.Errorf("no %q in %v", k, steps)
			}
			cur = next
		case int:
			if !cur.isArr || k >= len(cur.arr) {
				return nil, fmt.Errorf("no item %d in %v", k, steps)
			}
			cur = cur.arr[k]
		}
	}
	if !cur.isObj {
		return nil, fmt.Errorf("%v is not an object", steps)
	}
	return cur, nil
}

func (n *node) get(key string) *node {
	if !n.isObj {
		return nil
	}
	for _, m := range n.obj {
		if m.key == key {
			return m.val
		}
	}
	return nil
}

// front moves the given keys of an object, those it has, to its front, in
// that order. The other keys keep their order.
func (n *node) front(keys ...string) {
	var head, rest []member
	for _, k := range keys {
		for _, m := range n.obj {
			if m.key == k {
				head = append(head, m)
			}
		}
	}
	for _, m := range n.obj {
		if !slices.Contains(keys, m.key) {
			rest = append(rest, m)
		}
	}
	n.obj = append(head, rest...)
}

// set replaces key's value in place, or appends the key.
func (n *node) set(key string, v *node) {
	for i := range n.obj {
		if n.obj[i].key == key {
			n.obj[i].val = v
			return
		}
	}
	n.obj = append(n.obj, member{key: key, val: v})
}

// write writes n with two-space indentation, as npm writes package.json.
func (n *node) write(w *bytes.Buffer, indent string) error {
	inner := indent + "  "
	switch {
	case n.isObj:
		if len(n.obj) == 0 {
			w.WriteString("{}")
			return nil
		}
		w.WriteString("{\n")
		for i, m := range n.obj {
			key, err := marshal(m.key)
			if err != nil {
				return err
			}
			w.WriteString(inner)
			w.Write(key)
			w.WriteString(": ")
			if err := m.val.write(w, inner); err != nil {
				return err
			}
			if i < len(n.obj)-1 {
				w.WriteByte(',')
			}
			w.WriteByte('\n')
		}
		w.WriteString(indent + "}")
	case n.isArr:
		if len(n.arr) == 0 {
			w.WriteString("[]")
			return nil
		}
		w.WriteString("[\n")
		for i, v := range n.arr {
			w.WriteString(inner)
			if err := v.write(w, inner); err != nil {
				return err
			}
			if i < len(n.arr)-1 {
				w.WriteByte(',')
			}
			w.WriteByte('\n')
		}
		w.WriteString(indent + "]")
	default:
		w.Write(n.scalar)
	}
	return nil
}
