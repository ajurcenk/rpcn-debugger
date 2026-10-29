package debugger

import (
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// The E3 entry 7 launch rows through debugger.Serve: the output key's
// value and the mode-dependent warnings.
func TestServeOutputLaunch(t *testing.T) {
	const composite = genInput + `output:
  switch:
    cases:
      - check: 'true'
        output:
          drop: {}
          processors:
            - mapping: 'root = "case"'
`
	const brokerStdout = genInput + `output:
  broker:
    outputs:
      - drop: {}
      - stdout: {}
`
	cases := map[string]struct {
		src  string
		args map[string]any
		code core.Code
		// want must all be in the error or the warnings, and not must all
		// be missing from the warnings.
		want, not []string
	}{
		"bad value": {
			src: genInput, args: map[string]any{"output": "maybe"}, code: core.CodeLaunchInvalid,
			want: []string{`"output"`, "one of dry-run, real, none"},
		},
		"default": {src: genInput + "output:\n  drop: {}\n"},
		"real": {
			src: genInput + "output:\n  drop: {}\n", args: map[string]any{"output": "real"},
			want: []string{"output: real writes each delivered batch to the drop output at output (line 3)"},
		},
		"stdout under real": {
			src: brokerStdout, args: map[string]any{"output": "real"},
			want: []string{
				"output: real writes each delivered batch to the broker output at output (line 3)",
				"output.broker.outputs[1].stdout (line 7): the stdout output is replaced by drop under output: real",
			},
		},
		"stdout under none": {
			src: brokerStdout, args: map[string]any{"output": "none"},
			want: []string{"output.broker.outputs[1].stdout (line 7): the stdout output is redirected to dry-run"},
			not:  []string{"output: real"},
		},
		"composite under real": {
			src: composite, args: map[string]any{"output": "real"},
			want: []string{"output: real writes each delivered batch to the switch output"},
			not:  []string{"not run in dry-run"},
		},
		"composite under dry-run": {
			src:  composite,
			want: []string{"output.switch.cases[0].output.processors (line 9): this processor list is not run in dry-run"},
		},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			resp, out, _ := launchOnly(t, tc.src, tc.args)
			if tc.code != "" {
				if resp.Success || errorID(resp) != core.Number(tc.code) {
					t.Fatalf("launch = %+v, want %s", resp, tc.code)
				}
				for _, w := range tc.want {
					if !strings.Contains(resp.Message, w) {
						t.Errorf("error %q does not name %q", resp.Message, w)
					}
				}
				return
			}
			if !resp.Success {
				t.Fatalf("launch = %+v, want success", resp)
			}
			all := strings.Join(out, "")
			if len(out) != len(tc.want) {
				t.Errorf("warnings = %q, want %d", out, len(tc.want))
			}
			for _, w := range tc.want {
				if !strings.Contains(all, w) {
					t.Errorf("warnings %q do not name %q", out, w)
				}
			}
			for _, w := range tc.not {
				if strings.Contains(all, w) {
					t.Errorf("warnings %q name %q", out, w)
				}
			}
		})
	}
}

// runToEnd launches path with args, continues at every stop and returns
// the delivered batches' contents once the session terminates.
func runToEnd(t *testing.T, path string, args map[string]any) []string {
	t.Helper()
	delivered, _ := runToEndOutput(t, path, args)
	return delivered
}

// runToEndOutput is runToEnd, also returning the text of every output
// event.
func runToEndOutput(t *testing.T, path string, args map[string]any) (delivered, out []string) {
	t.Helper()
	c := serveEnv(t, inputsEnv(t))
	c.send("initialize", map[string]any{})
	c.next("response", "initialize")
	launch := map[string]any{"pipeline": path}
	for k, v := range args {
		launch[k] = v
	}
	c.send("launch", launch)
	for {
		m := c.nextAny()
		switch {
		case m.Type == "response" && m.Command == "launch":
			if !m.Success {
				t.Fatalf("launch = %+v", m)
			}
		case m.Type == "event" && m.Event == "initialized":
			c.send("configurationDone", map[string]any{})
		case m.Type == "event" && m.Event == "stopped":
			c.send("continue", map[string]any{"threadId": 1})
		case m.Type == "event" && m.Event == "output":
			var b struct {
				Output string `json:"output"`
			}
			_ = json.Unmarshal(m.Body, &b)
			out = append(out, b.Output)
		case m.Type == "event" && m.Event == "rpcnDelivered":
			var ev struct {
				Snapshot core.Snapshot `json:"snapshot"`
			}
			_ = json.Unmarshal(m.Body, &ev)
			for _, b := range ev.Snapshot.Batches {
				for _, msg := range b.Messages {
					delivered = append(delivered, string(msg.Content.Raw))
				}
			}
		case m.Type == "event" && m.Event == "terminated":
			c.send("disconnect", map[string]any{})
			c.next("response", "disconnect")
			return delivered, out
		}
	}
}

// output: real writes the shown batch to the user's output once, with
// output.processors applied once; none and dry-run write nothing. The
// Debug Console's header says what became of the batch, per mode.
func TestServeOutputRealWritesOnce(t *testing.T) {
	headers := map[string]string{
		"real":    "Delivered batch 1 (2 messages), written to the output\n",
		"none":    "Delivered batch 1 (2 messages), not written (output: none)\n",
		"dry-run": "Delivered batch 1 (2 messages) to dry-run output\n",
	}
	for mode, header := range headers {
		t.Run(mode, func(t *testing.T) {
			dir := t.TempDir()
			outFile := filepath.Join(dir, "out.txt")
			src := `input:
  generate: {count: 2, batch_size: 2, interval: "", mapping: 'root.n = counter()'}
output:
  processors:
    - mapping: 'root = this.merge({"seen": this.seen.or(0) + 1})'
  file: {path: ` + outFile + `, codec: lines}
`
			path := filepath.Join(dir, "pipeline.yaml")
			if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
				t.Fatal(err)
			}
			delivered, out := runToEndOutput(t, path, map[string]any{"output": mode})
			want := `{"n":1,"seen":1}` + "\n" + `{"n":2,"seen":1}`
			if got := strings.Join(delivered, "\n"); got != want {
				t.Errorf("delivered = %s, want %s", got, want)
			}
			if n := countPrefix(out, "Delivered batch"); n != 1 || countPrefix(out, header) != 1 {
				t.Errorf("output = %q, want one delivered batch headed %q", out, header)
			}
			raw, err := os.ReadFile(outFile)
			if mode != "real" {
				if err == nil {
					t.Errorf("output: %s wrote %q", mode, raw)
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if got := strings.TrimSuffix(string(raw), "\n"); got != want {
				t.Errorf("written = %s, want exactly the delivered batch %s", got, want)
			}
		})
	}
}

// countPrefix counts the texts that start with prefix.
func countPrefix(texts []string, prefix string) int {
	n := 0
	for _, s := range texts {
		if strings.HasPrefix(s, prefix) {
			n++
		}
	}
	return n
}

// A run that delivers nothing says so in the words of its output mode.
func TestServeOutputNothingDelivered(t *testing.T) {
	src := genInput + `pipeline:
  processors:
    - mapping: 'root = deleted()'
output:
  drop: {}
`
	for mode, note := range map[string]string{
		"real":    "Nothing was delivered, so nothing was written to the output\n",
		"none":    "Nothing was delivered (output: none)\n",
		"dry-run": "Nothing was delivered to dry-run output\n",
	} {
		t.Run(mode, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "pipeline.yaml")
			if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
				t.Fatal(err)
			}
			delivered, out := runToEndOutput(t, path, map[string]any{"output": mode})
			if len(delivered) != 0 || countPrefix(out, note) != 1 {
				t.Errorf("delivered = %q, output = %q; want nothing and %q", delivered, out, note)
			}
		})
	}
}

// A write the user's output refuses under output: real is one delivered
// batch whose header says it failed, with the error, and the run fails with
// OUTPUT_FAILED at the output key. The file output's path is a directory,
// so every write fails at once. (An unreachable broker fails the same way
// once the write deadline passes.)
func TestServeOutputRealWriteFails(t *testing.T) {
	dir := t.TempDir()
	src := genInput + "output:\n  file: {path: " + dir + ", codec: lines}\n"
	path := filepath.Join(dir, "pipeline.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	delivered, out := runToEndOutput(t, path, map[string]any{"output": "real"})
	if len(delivered) != 1 {
		t.Errorf("delivered = %q, want the one failed batch", delivered)
	}
	if countPrefix(out, "Delivered batch 1 (1 messages), failed to write: ") != 1 || !strings.Contains(strings.Join(out, ""), "is a directory") {
		t.Errorf("output = %q, want the failed batch with its write error", out)
	}
	if countPrefix(out, "The debug run failed: the output failed to write a delivered batch: ") != 1 ||
		!strings.Contains(strings.Join(out, ""), "(at output, line 3)") {
		t.Errorf("output = %q, want the OUTPUT_FAILED run failure at the output key", out)
	}
}

// writeRun writes the pipeline src (with %s for the output file) to a temp
// dir, runs it to the end under output: real and returns the delivered
// contents and the written file.
func writeRun(t *testing.T, src string) (delivered []string, written string) {
	t.Helper()
	dir := t.TempDir()
	outFile := filepath.Join(dir, "out.txt")
	path := filepath.Join(dir, "pipeline.yaml")
	if err := os.WriteFile(path, []byte(strings.ReplaceAll(src, "%s", outFile)), 0o600); err != nil {
		t.Fatal(err)
	}
	delivered = runToEnd(t, path, map[string]any{"output": "real"})
	raw, err := os.ReadFile(outFile)
	if err != nil {
		t.Fatal(err)
	}
	return delivered, string(raw)
}

// Output batching is removed under real: a count the one captured batch
// never reaches would otherwise hold it for ever. The run completes and
// writes the batch once.
func TestServeOutputRealBatchingRemoved(t *testing.T) {
	delivered, written := writeRun(t, `input:
  generate: {count: 2, batch_size: 2, interval: "", mapping: 'root.n = counter()'}
output:
  broker:
    outputs:
      - file: {path: %s, codec: lines}
    batching: {count: 5}
`)
	want := `{"n":1}` + "\n" + `{"n":2}`
	if got := strings.Join(delivered, "\n"); got != want {
		t.Errorf("delivered = %s, want %s", got, want)
	}
	if got := strings.TrimSuffix(written, "\n"); got != want {
		t.Errorf("written = %q, want exactly %s", got, want)
	}
}

// Under real a composite output runs its per-case processors, as in
// production: the file gets what they made, while the delivered batch is
// what reached the output.
func TestServeOutputRealCompositeRunsCaseProcessors(t *testing.T) {
	delivered, written := writeRun(t, `input:
  generate: {count: 1, interval: "", mapping: 'root.n = 1'}
output:
  switch:
    cases:
      - check: 'true'
        output:
          file: {path: %s, codec: lines}
          processors:
            - mapping: 'root = "case " + this.n.string()'
      - output:
          drop: {}
`)
	if got := strings.Join(delivered, "\n"); got != `{"n":1}` {
		t.Errorf("delivered = %s, want {\"n\":1}", got)
	}
	if got := strings.TrimSuffix(written, "\n"); got != "case 1" {
		t.Errorf("written = %q, want the case processor's result \"case 1\"", got)
	}
}

// A user's output that fails to init under real fails the run instead of
// hanging it.
func TestServeOutputRealInitFails(t *testing.T) {
	dir := t.TempDir()
	src := genInput + "output:\n  switch:\n    cases:\n      - output: {drop: {}}\n"
	path := filepath.Join(dir, "pipeline.yaml")
	if err := os.WriteFile(path, []byte(src), 0o600); err != nil {
		t.Fatal(err)
	}
	_, out := runToEndOutput(t, path, map[string]any{"output": "real"})
	if all := strings.Join(out, ""); !strings.Contains(all, "The debug run failed: run the step stream:") || !strings.Contains(all, "fewer than 2 cases") {
		t.Errorf("output = %q, want the init failure", out)
	}
}
