package core

import (
	"strings"
	"testing"
)

func TestCheckOutputMode(t *testing.T) {
	for _, m := range []OutputMode{"", OutputDryRun, OutputReal, OutputNone} {
		if err := checkOutputMode(m); err != nil {
			t.Errorf("checkOutputMode(%q) = %v", m, err)
		}
	}
	err := checkOutputMode("maybe")
	if !HasCode(err, CodeLaunchInvalid) || !strings.Contains(err.Error(), `"output" must be one of dry-run, real, none`) {
		t.Errorf("checkOutputMode(maybe) = %v, want LAUNCH_INVALID naming output", err)
	}
	if OutputMode("").Effective() != OutputDryRun || OutputNone.Effective() != OutputNone {
		t.Error("Effective does not read \"\" as dry-run")
	}
}
