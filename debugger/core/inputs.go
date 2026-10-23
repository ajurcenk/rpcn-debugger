package core

import (
	"fmt"
	"maps"
	"slices"
	"strings"
)

// InputClass is the AD-5 class of an input in the capture tree.
type InputClass string

// The input classes (AD-5), from best to worst. Unknown is treated as
// side-effecting.
const (
	ClassReplaySafe    InputClass = "replay-safe"
	ClassSideEffecting InputClass = "side-effecting"
	ClassUnknown       InputClass = "unknown"
	ClassRejected      InputClass = "rejected"
)

// OverrideClasses are the classes the launch key inputClasses may set, in
// the order the schema lists them.
var OverrideClasses = []InputClass{ClassReplaySafe, ClassSideEffecting, ClassRejected}

// InputFinding is the class of one input in the capture tree (AD-5): the
// rewrite reports it, and the Session decides what to refuse.
type InputFinding struct {
	// UserPath is the input block's user path: input,
	// input.broker.inputs[1], input_resources.<label>, …
	UserPath string
	// Line is the file line of the component key.
	Line int
	// Component is the input component name.
	Component string
	Class     InputClass
	// Effect says what capturing from the input does to the system it
	// reads from, or why it is rejected or unknown. It is empty for a
	// replay-safe input.
	Effect string
	// Inherited is set when Class comes from a child input or a named
	// input resource (a composite takes its worst child's class), whose own
	// finding is in the same list.
	Inherited bool
	// Override is the class the launch's inputClasses gave the component,
	// or "" when it has none. It replaces the component's own class before
	// a composite takes its worst child's.
	Override InputClass
	// Loosened is set when Override is better than the class the input
	// table gives the component: the Session warns that the debugger can't
	// prove it.
	Loosened bool
}

// FixedInputs are the input components inputClasses can't loosen: they
// stay rejected, whatever the launch says, with the reason.
var FixedInputs = map[string]string{
	"stdin":             "stdin carries the debugger's DAP stream",
	"dynamic":           "inputs can be added to it at run time through its HTTP API, so the debugger can't see them all",
	"redpanda_migrator": "it needs its paired redpanda_migrator output, which the capture doesn't have",
}

// checkInputClasses returns LAUNCH_INVALID for an inputClasses value
// outside OverrideClasses, or one that loosens a FixedInputs component,
// naming its key.
func checkInputClasses(classes map[string]InputClass) error {
	for _, name := range slices.Sorted(maps.Keys(classes)) {
		if !slices.Contains(OverrideClasses, classes[name]) {
			return NewError(CodeLaunchInvalid, `"inputClasses.%s" must be one of %s, not %q`, name, overrideList(), classes[name])
		}
		if why, fixed := FixedInputs[name]; fixed && classes[name] != ClassRejected {
			return NewError(CodeLaunchInvalid, `"inputClasses.%s" can only be %s: the %s input can't be debugged (%s)`, name, ClassRejected, name, why)
		}
	}
	return nil
}

func overrideList() string {
	s := make([]string, len(OverrideClasses))
	for i, c := range OverrideClasses {
		s[i] = string(c)
	}
	return strings.Join(s, ", ")
}

// replayHint ends every INPUT_REJECTED message.
const replayHint = "; replay a saved capture instead, with the `replay` launch key (coming with epic-edit-restart-replay)"

// inputDecision is the Session's AD-5 decision on a launch's findings: a
// refusal, or the warnings to emit.
func inputDecision(pipeline string, findings []InputFinding, allowSideEffects bool) ([]string, error) {
	var warnings []string
	for _, f := range findings {
		if f.Loosened {
			warnings = append(warnings, fmt.Sprintf("inputClasses marks %s at %s %s; the debugger can't prove it", f.Component, f.UserPath, f.Override))
		}
	}
	if rejected := sources(findings, func(c InputClass) bool { return c == ClassRejected }); len(rejected) > 0 {
		f := rejected[0]
		e := NewError(CodeInputRejected, "%s: the %s input at %s, line %d can't be debugged: %s%s",
			pipeline, f.Component, f.UserPath, f.Line, f.Effect, replayHint)
		e.UserPath, e.Line, e.Inline = f.UserPath, f.Line, true
		return nil, e
	}
	risky := sources(findings, func(c InputClass) bool { return c == ClassSideEffecting || c == ClassUnknown })
	if len(risky) == 0 {
		return warnings, nil
	}
	if allowSideEffects {
		for _, f := range risky {
			warnings = append(warnings, fmt.Sprintf("capturing from the %s input at %s (line %d) has side effects: %s", f.Component, f.UserPath, f.Line, f.Effect))
		}
		return warnings, nil
	}
	var b strings.Builder
	fmt.Fprintf(&b, "%s: capturing from the input has side effects, so the launch needs \"allowSideEffects\": true:", pipeline)
	for _, f := range risky {
		fmt.Fprintf(&b, "\n  - the %s input at %s, line %d: %s", f.Component, f.UserPath, f.Line, f.Effect)
	}
	e := NewError(CodeSideEffectsNotAllowed, "%s", b.String())
	e.UserPath, e.Line, e.Inline = risky[0].UserPath, risky[0].Line, true
	return nil, e
}

// sources returns the findings whose class matches, without the inherited
// ones (a composite's class is its child's, which is listed itself), or
// with them when every match is inherited.
func sources(findings []InputFinding, match func(InputClass) bool) []InputFinding {
	var own, all []InputFinding
	for _, f := range findings {
		if !match(f.Class) {
			continue
		}
		all = append(all, f)
		if !f.Inherited {
			own = append(own, f)
		}
	}
	if len(own) == 0 {
		return all
	}
	return own
}
