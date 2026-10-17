package dap

import (
	"encoding/base64"
	"fmt"
	"strings"

	"github.com/google/go-dap"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// The Variables tree of a stop renders its Snapshot (AD-11):
//
//	Batches › batch i › message j › content, metadata › key… and internal › __key…, error
//
// A variablesReference encodes (stop, kind, batch, message), so it needs no
// table. The stop is the frame id modulo maxRefStops: a reference from an
// earlier stop resolves to nothing, as AD-17 requires (only one handed out
// exactly maxRefStops stops earlier could collide).

// Node kinds of a variablesReference.
const (
	kindBatches  = 1 // the batches: the "Batches" scope
	kindBatch    = 2 // the messages of batch b
	kindMessage  = 3 // content, metadata and error of message m of batch b
	kindMetadata = 4 // the non-internal keys of message m, then "internal"
	kindInternal = 5 // the internal (__) keys of message m
)

// Reference layout:
// refBase + (((stop*maxRefBatches + b)*maxRefMessages + m)*refKinds + kind).
// It stays below 2^31.
const (
	refBase        = 8
	refKinds       = 8
	maxRefMessages = 1 << 12
	maxRefBatches  = 1 << 8
	maxRefStops    = 1 << 7
)

// summaryMax bounds the content preview in a message's value.
const summaryMax = 80

type ref struct{ stop, kind, batch, msg int }

// stopOf is the stop part of the references of frame frameID.
func stopOf(frameID int) int { return ((frameID % maxRefStops) + maxRefStops) % maxRefStops }

func encodeRef(frameID, kind, batch, msg int) int {
	if batch < 0 || batch >= maxRefBatches || msg < 0 || msg >= maxRefMessages {
		return 0 // not expandable
	}
	return refBase + ((stopOf(frameID)*maxRefBatches+batch)*maxRefMessages+msg)*refKinds + kind
}

func decodeRef(n int) (ref, bool) {
	if n < refBase {
		return ref{}, false
	}
	n -= refBase
	r := ref{kind: n % refKinds}
	n /= refKinds
	r.msg = n % maxRefMessages
	n /= maxRefMessages
	r.batch = n % maxRefBatches
	r.stop = n / maxRefBatches
	return r, r.kind >= kindBatches && r.kind <= kindInternal && r.stop < maxRefStops
}

// batchesRef is the variablesReference of the "Batches" scope of frameID.
func batchesRef(frameID int) int { return encodeRef(frameID, kindBatches, 0, 0) }

// variables returns the children of reference a.VariablesReference in the
// stop f; a reference from another stop has none. Indexed children
// (batches, messages) honour start and count; count 0 means all.
func variables(f core.Frame, a dap.VariablesArguments) []dap.Variable {
	out := []dap.Variable{}
	snap := f.Snapshot
	r, ok := decodeRef(a.VariablesReference)
	if !ok || r.stop != stopOf(f.ID) {
		return out // from an earlier stop, or no reference at all
	}
	if r.kind == kindBatches {
		if a.Filter == "named" {
			return out
		}
		lo, hi := page(len(snap.Batches), a.Start, a.Count)
		for b := lo; b < hi; b++ {
			out = append(out, batchVar(f.ID, b, snap.Batches[b]))
		}
		return out
	}
	if r.batch >= len(snap.Batches) {
		return out
	}
	batch := snap.Batches[r.batch]
	if r.kind == kindBatch {
		if a.Filter == "named" {
			return out
		}
		lo, hi := page(len(batch.Messages), a.Start, a.Count)
		for m := lo; m < hi; m++ {
			out = append(out, messageVar(f.ID, r.batch, m, batch.Messages[m]))
		}
		return out
	}
	if r.msg >= len(batch.Messages) || a.Filter == "indexed" {
		return out
	}
	msg := batch.Messages[r.msg]
	switch r.kind {
	case kindMessage:
		out = append(out, contentVar(msg.Content), metadataVar(f.ID, r.batch, r.msg, msg.Metadata))
		if msg.Error != "" {
			out = append(out, dap.Variable{Name: "error", Value: msg.Error, Type: "error"})
		}
	case kindMetadata:
		internal := 0
		for _, e := range msg.Metadata {
			if e.Internal {
				internal++
				continue
			}
			out = append(out, metaVar(e))
		}
		if internal > 0 {
			out = append(out, dap.Variable{
				Name:               "internal",
				Value:              countText(internal, "key"),
				VariablesReference: encodeRef(f.ID, kindInternal, r.batch, r.msg),
				NamedVariables:     internal,
				PresentationHint:   &dap.VariablePresentationHint{Kind: "virtual"},
			})
		}
	case kindInternal:
		for _, e := range msg.Metadata {
			if e.Internal {
				out = append(out, metaVar(e))
			}
		}
	}
	return out
}

// page clamps [start, start+count) to n items.
func page(n, start, count int) (lo, hi int) {
	lo = min(max(start, 0), n)
	hi = n
	if count > 0 {
		hi = min(lo+count, n)
	}
	return lo, hi
}

func batchVar(frameID, b int, batch core.Batch) dap.Variable {
	return dap.Variable{
		Name:               fmt.Sprintf("batch %d", b),
		Value:              countText(len(batch.Messages), "message"),
		VariablesReference: encodeRef(frameID, kindBatch, b, 0),
		IndexedVariables:   len(batch.Messages),
	}
}

func messageVar(frameID, b, i int, m core.Message) dap.Variable {
	value := contentText(m.Content)
	if r := []rune(value); len(r) > summaryMax {
		value = string(r[:summaryMax]) + "…"
	}
	if m.Error != "" {
		value = "error: " + m.Error + " · " + value
	}
	named := 2
	if m.Error != "" {
		named = 3
	}
	return dap.Variable{
		Name:               fmt.Sprintf("message %d", i),
		Value:              value,
		VariablesReference: encodeRef(frameID, kindMessage, b, i),
		NamedVariables:     named,
	}
}

func contentVar(c core.Content) dap.Variable {
	typ := "bytes"
	switch {
	case c.JSON != nil:
		typ = "json"
	case c.Text != nil:
		typ = "text"
	}
	return dap.Variable{Name: "content", Value: contentText(c), Type: typ}
}

func metadataVar(frameID, b, m int, meta []core.Meta) dap.Variable {
	named, internal := 0, false
	for _, e := range meta {
		if e.Internal {
			internal = true
		} else {
			named++
		}
	}
	if internal {
		named++
	}
	v := dap.Variable{Name: "metadata", Value: countText(len(meta), "key"), NamedVariables: named}
	if len(meta) > 0 {
		v.VariablesReference = encodeRef(frameID, kindMetadata, b, m)
	}
	return v
}

// metaVar shows a key's display text when it has one, else its lossless
// value (e.g. base64 for bytes that aren't UTF-8).
func metaVar(e core.Meta) dap.Variable {
	value := e.Value
	if e.Text != nil {
		value = *e.Text
	}
	return dap.Variable{Name: e.Key, Value: value, Type: string(e.Type)}
}

// contentText renders content for the Variables view: its text, or base64
// when it isn't valid UTF-8.
func contentText(c core.Content) string {
	if c.Text != nil {
		return *c.Text
	}
	return base64.StdEncoding.EncodeToString(c.Raw)
}

func countText(n int, noun string) string {
	if n == 1 {
		return "1 " + noun
	}
	return fmt.Sprintf("%d %ss", n, strings.TrimSuffix(noun, "s"))
}
