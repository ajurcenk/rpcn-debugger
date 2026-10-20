package core

import (
	"maps"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestParseDotenv(t *testing.T) {
	src := "\uFEFF# a comment\n" +
		"\n" +
		"A=1\n" +
		"export B = two words \n" +
		"C='single # not a comment \\n'\n" +
		"D=\"double\\n\\\"q\\\"\" # trailing comment\n" +
		"E=plain # comment\n" +
		"F=\n" +
		"G=a=b\r\n" +
		"H= # nothing\n" +
		"I=\t#x\n" +
		"export\tJ=1\n" +
		"A=override\n"
	got, err := parseDotenv(src)
	if err != nil {
		t.Fatal(err)
	}
	want := map[string]string{
		"A": "override",
		"B": "two words",
		"C": `single # not a comment \n`,
		"D": "double\n\"q\"",
		"E": "plain",
		"F": "",
		"G": "a=b",
		"H": "",
		"I": "",
		"J": "1",
	}
	if !maps.Equal(got, want) {
		t.Errorf("parseDotenv = %q, want %q", got, want)
	}
}

func TestParseDotenvErrors(t *testing.T) {
	const secret = "s3cr3t"
	for name, tc := range map[string]struct {
		src  string
		line string
	}{
		"no name":        {"A=1\n# c\n=" + secret + "\n", "line 3:"},
		"no equals":      {"A=1\n\nKEY\n", "line 3:"},
		"bad name":       {"1A=" + secret + "\n", "line 1:"},
		"unterminated":   {"A=\"" + secret + "\n", "line 1 (A):"},
		"text after":     {"A='x' " + secret + "\n", "line 1 (A):"},
		"export no name": {"export =" + secret + "\n", "line 1:"},
	} {
		t.Run(name, func(t *testing.T) {
			_, err := parseDotenv(tc.src)
			if err == nil {
				t.Fatal("no error")
			}
			if !strings.HasPrefix(err.Error(), tc.line) {
				t.Errorf("error = %q, want it to start with %q", err, tc.line)
			}
			if strings.Contains(err.Error(), secret) {
				t.Errorf("error %q shows the value", err)
			}
		})
	}
}

func TestEnvLookup(t *testing.T) {
	t.Setenv("RPCN_DEBUG_TEST_A", "p")
	t.Setenv("RPCN_DEBUG_TEST_C", "p")
	l := EnvLookup(map[string]string{"RPCN_DEBUG_TEST_A": "f", "RPCN_DEBUG_TEST_B": "f"}, map[string]string{"RPCN_DEBUG_TEST_A": "e"})
	for name, want := range map[string]string{"RPCN_DEBUG_TEST_A": "e", "RPCN_DEBUG_TEST_B": "f", "RPCN_DEBUG_TEST_C": "p"} {
		if v, ok := l(name); !ok || v != want {
			t.Errorf("lookup(%s) = %q, %v; want %q", name, v, ok, want)
		}
	}
	if _, ok := l("RPCN_DEBUG_TEST_UNSET"); ok {
		t.Error("an unset name was found")
	}
}

// A malformed envFile fails the launch with LAUNCH_INVALID naming envFile
// and the line, never the value, and the engine never checks the pipeline.
func TestLaunchBadEnvFile(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("A=1\n# ok\nKEY_ONLY_s3cr3t\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	var got string
	s := NewSession(&pathEngine{got: &got}, newRecorder())
	err := do(s, Launch{Pipeline: "p.yaml", Cwd: dir, EnvFile: ".env"})
	if !HasCode(err, CodeLaunchInvalid) || !strings.Contains(err.Error(), "envFile line 3") {
		t.Errorf("launch = %v, want LAUNCH_INVALID naming envFile line 3", err)
	}
	if err != nil && strings.Contains(err.Error(), "s3cr3t") {
		t.Errorf("launch error %q shows the line", err)
	}
	if got != "" {
		t.Error("the engine checked the pipeline")
	}
}

// The launch's lookup reaches Check: env over envFile over the process.
func TestLaunchLookupReachesCheck(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, ".env"), []byte("RPCN_DEBUG_TEST_A=f\nRPCN_DEBUG_TEST_B=f\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv("RPCN_DEBUG_TEST_C", "p")
	e := &lookupEngine{}
	s := NewSession(e, newRecorder())
	if err := do(s, Launch{Pipeline: "p.yaml", Cwd: dir, EnvFile: ".env", Env: map[string]string{"RPCN_DEBUG_TEST_A": "e"}}); err != nil {
		t.Fatal(err)
	}
	if e.lookup == nil {
		t.Fatal("Check got no lookup")
	}
	for name, want := range map[string]string{"RPCN_DEBUG_TEST_A": "e", "RPCN_DEBUG_TEST_B": "f", "RPCN_DEBUG_TEST_C": "p"} {
		if v, _ := e.lookup(name); v != want {
			t.Errorf("lookup(%s) = %q, want %q", name, v, want)
		}
	}
}

type lookupEngine struct {
	pathEngine
	lookup func(string) (string, bool)
}

func (e *lookupEngine) Check(o StartOptions) (CheckResult, error) {
	e.lookup = o.Lookup
	return CheckResult{Lines: lines{}}, nil
}
