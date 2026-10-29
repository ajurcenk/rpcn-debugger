package core

import (
	"errors"
	"fmt"
	"regexp"
	"testing"
)

// The numbers are the DAP error ids: they are fixed and never reused.
func TestRegistryNumbersArePinned(t *testing.T) {
	want := map[Code]int{
		"LAUNCH_INVALID":           1001,
		"PIPELINE_INVALID":         1002,
		"NOTHING_CAPTURED":         1003,
		"SESSION_ENDED":            1004,
		"NOT_SUPPORTED":            1005,
		"NOT_STOPPED":              1006,
		"RUN_FAILED":               1007,
		"CTX_LOST":                 1008,
		"LICENSE_REQUIRED":         1009,
		"STRICT_REJECTED":          1010,
		"INPUT_REJECTED":           1011,
		"SIDE_EFFECTS_NOT_ALLOWED": 1012,
		"OUTPUT_FAILED":            1013,
	}
	if len(numbers) != len(want) {
		t.Errorf("registry has %d codes, want %d: a new code needs a pinned number here", len(numbers), len(want))
	}
	for code, n := range want {
		if got := Number(code); got != n {
			t.Errorf("Number(%s) = %d, want %d", code, got, n)
		}
	}
	seen := map[int]Code{}
	upper := regexp.MustCompile(`^[A-Z]+(_[A-Z]+)*$`)
	for _, c := range Codes() {
		if !upper.MatchString(string(c)) {
			t.Errorf("code %q is not UPPER_SNAKE", c)
		}
		if other, dup := seen[Number(c)]; dup {
			t.Errorf("codes %s and %s share number %d", c, other, Number(c))
		}
		seen[Number(c)] = c
	}
	if Number("NO_SUCH_CODE") != 0 {
		t.Error("an unknown code has a number")
	}
}

func TestErrorText(t *testing.T) {
	cases := []struct {
		err  Error
		want string
	}{
		{Error{Code: CodeRunFailed, Message: "boom"}, "boom"},
		{Error{Code: CodePipelineInvalid, Message: "bad", Line: 4}, "bad (at line 4)"},
		{Error{Code: CodePipelineInvalid, Message: "bad", UserPath: "pipeline.processors[0]", Line: 4}, "bad (at pipeline.processors[0], line 4)"},
	}
	for _, tc := range cases {
		if got := tc.err.Error(); got != tc.want {
			t.Errorf("Error() = %q, want %q", got, tc.want)
		}
	}
}

func TestErrorMatching(t *testing.T) {
	wrapped := fmt.Errorf("outer: %w", NewError(CodeNothingCaptured, "nothing"))
	if !HasCode(wrapped, CodeNothingCaptured) || HasCode(wrapped, CodeRunFailed) {
		t.Error("HasCode does not match by code")
	}
	var e Error
	if !errors.As(wrapped, &e) || e.Code != CodeNothingCaptured || e.Message != "nothing" {
		t.Errorf("errors.As = %+v", e)
	}
	if err := AsError(errors.New("plain"), CodeRunFailed); !HasCode(err, CodeRunFailed) || err.Error() != "plain" {
		t.Errorf("AsError(plain) = %v", err)
	}
	if err := AsError(wrapped, CodeRunFailed); !HasCode(err, CodeNothingCaptured) {
		t.Errorf("AsError(wrapped) = %v, want the wrapped code", err)
	}
	if AsError(nil, CodeRunFailed) != nil {
		t.Error("AsError(nil) is not nil")
	}
}

// Wrapping text around an Error survives AsError, with the wrapped code and
// location kept.
func TestAsErrorKeepsWrapText(t *testing.T) {
	inner := Error{Code: CodePipelineInvalid, Message: "bad processor", UserPath: "pipeline.processors[1]", Line: 9}
	err := AsError(fmt.Errorf("%s: %w", "/p/pipeline.yaml", inner), CodeRunFailed)
	var e Error
	if !errors.As(err, &e) {
		t.Fatalf("AsError = %T, want an Error", err)
	}
	if e.Code != CodePipelineInvalid || e.UserPath != inner.UserPath || e.Line != 9 {
		t.Errorf("AsError = %+v, want the wrapped code and location", e)
	}
	if want := "/p/pipeline.yaml: bad processor (at pipeline.processors[1], line 9)"; e.Error() != want {
		t.Errorf("Error() = %q, want %q", e.Error(), want)
	}
	if got := AsError(inner, CodeRunFailed); got != error(inner) {
		t.Errorf("AsError(Error) = %+v, want it unchanged", got)
	}
}
