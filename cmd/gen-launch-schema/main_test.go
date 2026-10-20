package main

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/ajurcenk/rpcn-debugger/debugger/dap"
)

const manifest = `{
  "name": "x",
  "zeta": "<kept> & unescaped",
  "contributes": {
    "debuggers": [
      {
        "type": "redpanda-connect",
        "initialConfigurations": [],
        "label": "L"
      }
    ]
  },
  "empty": {},
  "list": [],
  "n": 1.50
}
`

func TestGenerateKeepsTheRest(t *testing.T) {
	out, err := generate([]byte(manifest))
	if err != nil {
		t.Fatal(err)
	}
	s := string(out)
	for _, want := range []string{
		`"zeta": "<kept> & unescaped"`, `"empty": {}`, `"list": []`, `"n": 1.50`,
		// initialConfigurations is replaced in place; configurationAttributes
		// is appended after label.
		`"initialConfigurations": [` + "\n" + `          {` + "\n" + `            "type": "redpanda-connect",`,
		`"label": "L",` + "\n" + `        "configurationAttributes": {`,
	} {
		if !strings.Contains(s, want) {
			t.Errorf("output lacks %q:\n%s", want, s)
		}
	}
	if strings.Index(s, `"name": "x"`) > strings.Index(s, `"zeta"`) {
		t.Error("top-level key order changed")
	}

	var got struct {
		Contributes struct {
			Debuggers []struct {
				ConfigurationAttributes map[string]any `json:"configurationAttributes"`
				InitialConfigurations   []any          `json:"initialConfigurations"`
			} `json:"debuggers"`
		} `json:"contributes"`
	}
	if err := json.Unmarshal(out, &got); err != nil {
		t.Fatal(err)
	}
	d := got.Contributes.Debuggers[0]
	want := roundTrip(t, map[string]any{"launch": dap.Schema()})
	if !reflect.DeepEqual(d.ConfigurationAttributes, want) {
		t.Errorf("configurationAttributes = %v, want %v", d.ConfigurationAttributes, want)
	}
	if !reflect.DeepEqual(d.InitialConfigurations, roundTrip(t, dap.InitialConfigurations())) {
		t.Errorf("initialConfigurations = %v", d.InitialConfigurations)
	}

	again, err := generate(out)
	if err != nil {
		t.Fatal(err)
	}
	if string(again) != s {
		t.Error("generate is not idempotent")
	}
}

func roundTrip[T any](t *testing.T, v T) T {
	t.Helper()
	raw, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	var out T
	if err := json.Unmarshal(raw, &out); err != nil {
		t.Fatal(err)
	}
	return out
}

func TestCheck(t *testing.T) {
	file := filepath.Join(t.TempDir(), "package.json")
	if err := os.WriteFile(file, []byte(manifest), 0o644); err != nil {
		t.Fatal(err)
	}
	err := run(file, true)
	if err == nil || !strings.Contains(err.Error(), file) || !strings.Contains(err.Error(), "stale") {
		t.Fatalf("check on a stale file = %v, want an error naming it", err)
	}
	if b, _ := os.ReadFile(file); string(b) != manifest {
		t.Error("check modified the file")
	}
	if err := run(file, false); err != nil {
		t.Fatal(err)
	}
	if err := run(file, true); err != nil {
		t.Errorf("check after generating = %v, want nil", err)
	}
}

func TestGenerateNeedsADebugger(t *testing.T) {
	if _, err := generate([]byte(`{"contributes":{"debuggers":[]}}`)); err == nil {
		t.Error("generate without a debugger succeeded")
	}
}
