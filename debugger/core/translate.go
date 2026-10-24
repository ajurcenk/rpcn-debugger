package core

import (
	"regexp"
	"strconv"
)

// enginePathRef finds engine paths (root.pipeline.processors.0…) in text.
var enginePathRef = regexp.MustCompile(`\broot(?:\.[A-Za-z0-9_]+)+`)

// TranslateText returns text with every engine path tr maps replaced by
// its user path (AD-6); the rest, and all of text when tr is nil, stays as
// it is. It never touches line or column positions.
func TranslateText(tr Translate, text string) string {
	if tr == nil {
		return text
	}
	return enginePathRef.ReplaceAllStringFunc(text, func(m string) string {
		if up, ok := tr.Translate(m); ok {
			return up
		}
		return m
	})
}

// The config parts the engine adds to its streams, as the rewrite names
// them for its position map (AD-6): the capture input, the resources, the
// step stream's output (rpcn_debug_sink, AD-21) and each top-level stepper
// (StepperPart).
const (
	PartInput     = "input"
	PartResources = "resources"
	PartOutput    = "output"
)

// StepperPart returns the part name of top-level stepper i.
func StepperPart(i int) string { return "stepper/" + strconv.Itoa(i) }
