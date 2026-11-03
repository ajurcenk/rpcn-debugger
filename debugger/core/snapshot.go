package core

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// SchemaVersion is the Snapshot schema version this build writes. Readers
// reject a newer major version (AD-11).
const SchemaVersion = "1.0"

// CheckSchemaVersion is the major-version gate of a Snapshot reader
// (AD-11): v, a Snapshot's schemaVersion "<major>.<minor>", is readable
// when its major is at most SchemaVersion's. A newer major, and a missing
// or unparseable version, is an error naming it.
func CheckSchemaVersion(v string) error {
	if v == "" {
		return errors.New("the snapshot has no schemaVersion")
	}
	have, ok := majorVersion(v)
	if !ok {
		return fmt.Errorf("the snapshot's schemaVersion %q is not a <major>.<minor> version", v)
	}
	if want, _ := majorVersion(SchemaVersion); have > want {
		return fmt.Errorf("the snapshot's schemaVersion %s is newer than this rpcn-debug reads (%s): update rpcn-debug", v, SchemaVersion)
	}
	return nil
}

// majorVersion returns the major of a "<major>.<minor>" version.
func majorVersion(v string) (int, bool) {
	major, minor, ok := strings.Cut(v, ".")
	if !ok {
		return 0, false
	}
	m, err := strconv.Atoi(major)
	if err != nil || m < 0 || strings.HasPrefix(major, "+") {
		return 0, false
	}
	if n, err := strconv.Atoi(minor); err != nil || n < 0 || strings.HasPrefix(minor, "+") {
		return 0, false
	}
	return m, true
}

// Snapshot is the only representation of message state (AD-11): the DAP
// Variables, capture files and tests all use it. It never references engine
// objects; a stepper builds it from its deep copy of the batch (AD-19).
type Snapshot struct {
	SchemaVersion string    `json:"schemaVersion"`
	Position      Position  `json:"position"`
	Batches       []Batch   `json:"batches"`
	Skipped       []Skipped `json:"skipped,omitempty"`
	Bloblang      *Bloblang `json:"bloblang,omitempty"`
}

// Position is where a Snapshot was taken. LineMap fills UserPath and Line;
// the Session fills the rest.
type Position struct {
	ThreadID   int `json:"threadId"`
	FrameID    int `json:"frameId"`
	StepperID  int `json:"stepperId"`
	ChildIndex int `json:"childIndex"`
	// UserPath is the child's user path, e.g. pipeline.processors[0].
	UserPath string `json:"userPath"`
	// File is the pipeline file, as an absolute path.
	File string `json:"file"`
	// Line is the child's 1-based line in File, or 0 when unknown.
	Line int `json:"line"`
}

// Batch is one batch of messages. Steppers pass several, because group_by
// splits batches (AD-7).
type Batch struct {
	Messages []Message `json:"messages"`
}

// Message is one message of a batch.
type Message struct {
	// Index is the message's 0-based index in its batch.
	Index    int     `json:"index"`
	Content  Content `json:"content"`
	Metadata []Meta  `json:"metadata"`
	// Error is the processor error on the message, or "" when there is none.
	Error string `json:"error,omitempty"`
}

// Content holds the views of a message's content. Raw is always set; it is
// base64 in JSON.
type Content struct {
	Raw []byte `json:"raw"`
	// Text is the content as text, when it is valid UTF-8.
	Text *string `json:"text,omitempty"`
	// JSON is the content as JSON, when it is a valid JSON document.
	JSON json.RawMessage `json:"json,omitempty"`
}

// MetaType is the type of a metadata value: one of the closed set below.
type MetaType string

// The closed set of metadata types (AD-11).
const (
	MetaString    MetaType = "string"
	MetaBytes     MetaType = "bytes"
	MetaInt       MetaType = "int"
	MetaFloat     MetaType = "float"
	MetaBool      MetaType = "bool"
	MetaTimestamp MetaType = "timestamp"
	MetaObject    MetaType = "object"
	MetaArray     MetaType = "array"
	MetaNull      MetaType = "null"
)

// Meta is one metadata key. Value is lossless for Type: a string as is,
// bytes as base64, an int as a decimal string, a float in Go's shortest
// round-trip form, a bool as true/false, a timestamp as RFC 3339 with
// nanoseconds, an object or array as JSON, and null as "null". Text is for
// display only.
type Meta struct {
	Key   string   `json:"key"`
	Type  MetaType `json:"type"`
	Value string   `json:"value"`
	Text  *string  `json:"text,omitempty"`
	// Internal is true for keys that start with "__". The core never
	// filters them; frontends group them.
	Internal bool `json:"internal"`
}

// Skipped is a message left out of a batch, e.g. by maxMessages.
type Skipped struct {
	Index  int    `json:"index"`
	Reason string `json:"reason"`
}

// Bloblang is the statement-stepping state of a mapping (AD-13).
type Bloblang struct {
	// Root is the value of root, as JSON. It is absent while Unassigned.
	Root json.RawMessage `json:"root,omitempty"`
	// Unassigned is true for a mapping before its first root assignment.
	Unassigned bool `json:"unassigned,omitempty"`
	// Vars holds the mapping's variables, as JSON.
	Vars map[string]json.RawMessage `json:"vars,omitempty"`
	// Meta holds the mapping's metadata.
	Meta []Meta `json:"meta,omitempty"`
	// Recomputed names the values that replay recomputed (uuid_v4, now, …).
	Recomputed []string `json:"recomputed,omitempty"`
}
