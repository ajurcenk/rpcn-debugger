package core

import (
	"context"
	"strings"
	"testing"
	"time"
)

func TestDebugGroupName(t *testing.T) {
	for _, tc := range []struct {
		name string
		env  map[string]string
		want string
	}{
		{"jane", nil, "rpcn-debug-jane"},
		{`DOMAIN\Jane Doe`, nil, "rpcn-debug-domain-jane-doe"},
		{"j.doe_1-x", nil, "rpcn-debug-j.doe_1-x"},
		{"", map[string]string{"USER": "Bob"}, "rpcn-debug-bob"},
		{"", map[string]string{"USERNAME": "CORP\\Ann"}, "rpcn-debug-corp-ann"},
		{"", map[string]string{"USER": "u", "USERNAME": "n"}, "rpcn-debug-u"},
		{"", nil, "rpcn-debug-user"},
	} {
		getenv := func(k string) string { return tc.env[k] }
		if got := debugGroupName(tc.name, getenv); got != tc.want {
			t.Errorf("debugGroupName(%q, %v) = %q, want %q", tc.name, tc.env, got, tc.want)
		}
	}
}

// The process user's group never comes from the launch's env.
func TestDebugGroupIgnoresLaunchEnv(t *testing.T) {
	e := &prepEngine{started: make(chan StartOptions, 1)}
	s := NewSession(e, newRecorder())
	if err := do(s, Launch{Pipeline: "p.yaml", Cwd: t.TempDir(), Env: map[string]string{"USER": "intruder", "USERNAME": "intruder"}}); err != nil {
		t.Fatal(err)
	}
	if e.checked.DebugGroup != debugGroup() || strings.Contains(e.checked.DebugGroup, "intruder") {
		t.Errorf("Check DebugGroup = %q, want %q", e.checked.DebugGroup, debugGroup())
	}
}

// Check's Prepared and the debug group reach Start unchanged (AD-6: one
// rewrite per launch; AD-5: the Session supplies the group).
func TestPreparedReachesStart(t *testing.T) {
	e := &prepEngine{started: make(chan StartOptions, 1)}
	s := NewSession(e, newRecorder())
	if err := do(s, Launch{Pipeline: "p.yaml", Cwd: t.TempDir()}); err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(e.checked.DebugGroup, "rpcn-debug-") {
		t.Errorf("Check DebugGroup = %q, want rpcn-debug-<os-user>", e.checked.DebugGroup)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	select {
	case o := <-e.started:
		if p, ok := o.Prepared.(*int); !ok || p != e.prepared {
			t.Errorf("Start Prepared = %v, want Check's %v", o.Prepared, e.prepared)
		}
		if o.DebugGroup != e.checked.DebugGroup {
			t.Errorf("Start DebugGroup = %q, want Check's %q", o.DebugGroup, e.checked.DebugGroup)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Start was not called")
	}
}

// prepEngine records the options of Check and Start and hands out a
// Prepared that Start must get back.
type prepEngine struct {
	checked  StartOptions
	prepared *int
	started  chan StartOptions
}

func (e *prepEngine) Check(o StartOptions) (CheckResult, error) {
	e.checked, e.prepared = o, new(int)
	return CheckResult{Lines: lines{}, Prepared: e.prepared}, nil
}

func (e *prepEngine) Start(_ context.Context, o StartOptions, _ Arriver) error {
	e.started <- o
	return nil
}
