package core

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// replayFile writes a placeholder capture file under dir: restartEngine's
// LoadCapture decides what it holds.
func replayFile(t *testing.T, dir, name string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, []byte("capture"), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

// A replay steps the file's batch: Check lints only what Step runs, the
// replay note names the absolute path, and the run stops at the entry stop
// on the file's batch. Nothing is captured, no waiting note goes out, and
// the run ends like a captured one; Restart re-steps the same batch as
// batch 1.
func TestReplay(t *testing.T) {
	dir := t.TempDir()
	path := replayFile(t, dir, ".rpcn-debug/c.json")
	e := &restartEngine{mapping: "v1"}
	rec := newRecorder()
	rec.threads, rec.delivery = true, true
	s := NewSession(e, rec)
	s.SetCaptureWait(time.Millisecond)
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml", Cwd: dir, Replay: ".rpcn-debug/c.json"}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	expect(t, rec, "output:w v1", "thread:started 1",
		"output:Replaying "+path+": 1 messages; the input is not read.\n",
		"delivered:1 recorded", "stopped:entry")
	st, _ := s.Stopped()
	if got := string(st.Frames[0].Snapshot.Batches[0].Messages[0].Content.Raw); got != "f0" {
		t.Errorf("stop content = %q, want the file's batch", got)
	}
	rec.none(t)
	checks, steps, captures := e.counts()
	if captures != 0 {
		t.Errorf("captures = %d, want none", captures)
	}
	if len(checks) != 1 || !checks[0].StepOnly || checks[0].Output != OutputDryRun {
		t.Errorf("checks = %+v, want one StepOnly dry-run Check", checks)
	}
	if len(steps) != 1 || steps[0].Prepared != "v1" {
		t.Errorf("steps = %+v", steps)
	}
	if len(e.loads) != 1 || e.loads[0] != (loadCall{path, 0}) {
		t.Errorf("loads = %+v, want %s", e.loads, path)
	}

	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "output:"+RunFinishedNote, "thread:exited 1")
	rec.none(t)
	e.set("v2", nil)
	if err := do(s, Restart{}); err != nil {
		t.Fatalf("Restart after the replay = %v", err)
	}
	expect(t, rec, "output:"+restartNote, "output:w v2", "thread:started 1", "delivered:: f0/v2", "stopped:entry")
	rec.none(t)
	if _, _, captures := e.counts(); captures != 0 || len(e.loads) != 1 {
		t.Errorf("captures %d, loads %d after Restart; want 0 and 1", captures, len(e.loads))
	}
}

// A replay skips the input-class decision: a rejected input replays, with
// no refusal or input warning; the other launch warnings still show.
func TestReplayRejectedInput(t *testing.T) {
	dir := t.TempDir()
	replayFile(t, dir, "c.json")
	rejected := []InputFinding{{UserPath: "input", Line: 2, Component: "http_server", Class: ClassRejected, Effect: "it is a server"}}

	e := &restartEngine{mapping: "v1", inputs: rejected}
	if err := do(NewSession(e, newRecorder()), Launch{Pipeline: "/p/pipeline.yaml", Cwd: dir}); !HasCode(err, CodeInputRejected) {
		t.Fatalf("capture launch = %v, want INPUT_REJECTED", err)
	}

	e = &restartEngine{mapping: "v1", inputs: rejected}
	_, rec := launchRestart(t, e, Launch{Cwd: dir, Replay: "c.json"})
	expect(t, rec, "output:w v1", "thread:started 1", "output:Replaying", "delivered:1 recorded", "stopped:entry")
	rec.none(t)
}

// replay with saveCapture fails the launch before the pipeline is checked.
func TestReplayWithSaveCapture(t *testing.T) {
	dir := t.TempDir()
	replayFile(t, dir, "c.json")
	e := &restartEngine{}
	err := do(NewSession(e, newRecorder()), Launch{Pipeline: "/p/pipeline.yaml", Cwd: dir, Replay: "c.json", SaveCapture: "d.json"})
	if !HasCode(err, CodeLaunchInvalid) || !strings.Contains(err.Error(), `"replay"`) || !strings.Contains(err.Error(), `"saveCapture"`) {
		t.Errorf("launch = %v, want LAUNCH_INVALID naming replay and saveCapture", err)
	}
	if checks, _, _ := e.counts(); len(checks) != 0 {
		t.Error("the engine checked the pipeline")
	}
}

// A missing file, a directory and a file the engine can't load fail the
// launch, naming replay, the absolute path and the reason.
func TestReplayBadFile(t *testing.T) {
	dir := t.TempDir()
	replayFile(t, dir, "bad.json")
	if err := os.Mkdir(filepath.Join(dir, "sub"), 0o700); err != nil {
		t.Fatal(err)
	}
	for name, tc := range map[string]struct {
		file, reason string
		loadErr      error
	}{
		"missing":   {"none.json", "does not exist", nil},
		"directory": {"sub", "is a directory", nil},
		// The engine's errors name the file, as ReadCapture's do.
		"bad JSON": {"bad.json", "invalid character", fmt.Errorf("capture file %s: invalid character 'c'", filepath.Join(dir, "bad.json"))},
		"newer":    {"bad.json", "schemaVersion", fmt.Errorf(`capture file %s: schemaVersion "2.0" is newer`, filepath.Join(dir, "bad.json"))},
	} {
		t.Run(name, func(t *testing.T) {
			e := &restartEngine{loadErr: tc.loadErr}
			err := do(NewSession(e, newRecorder()), Launch{Pipeline: "/p/pipeline.yaml", Cwd: dir, Replay: tc.file})
			if !HasCode(err, CodeLaunchInvalid) {
				t.Fatalf("launch = %v, want LAUNCH_INVALID", err)
			}
			for _, want := range []string{`"replay"`, filepath.Join(dir, tc.file), tc.reason} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error %q doesn't contain %q", err, want)
				}
			}
			if n := strings.Count(err.Error(), filepath.Join(dir, tc.file)); n != 1 {
				t.Errorf("error %q names the path %d times, want once", err, n)
			}
		})
	}
}

// maxMessages applies to the file's batch: the engine keeps the first n,
// and the usual note counts against the file's total.
func TestReplayMaxMessages(t *testing.T) {
	dir := t.TempDir()
	path := replayFile(t, dir, "c.json")
	e := &restartEngine{mapping: "v1", fileMsgs: 10}
	_, rec := launchRestart(t, e, Launch{Cwd: dir, Replay: "c.json", MaxMessages: 3})
	expect(t, rec, "output:w v1", "thread:started 1",
		"output:Replaying "+path+": 3 messages; the input is not read.\n",
		"output:captured 3 of 10 messages (maxMessages)", "delivered:1 recorded", "stopped:entry")
	if len(e.loads) != 1 || e.loads[0].maxMessages != 3 {
		t.Errorf("loads = %+v, want maxMessages 3", e.loads)
	}
}

// Under output: real a replay runs as dry-run, with a note; a Restart of it
// sends no second dry-run note.
func TestReplayOutputReal(t *testing.T) {
	dir := t.TempDir()
	replayFile(t, dir, "c.json")
	e := &restartEngine{mapping: "v1"}
	s, rec := launchRestart(t, e, Launch{Cwd: dir, Replay: "c.json", Output: OutputReal})
	expect(t, rec, "output:w v1", "thread:started 1", "output:Replaying", "output:"+replayDryRunNote,
		"delivered:1 recorded", "stopped:entry")
	checks, steps, _ := e.counts()
	if checks[0].Output != OutputDryRun || steps[0].Output != OutputDryRun {
		t.Errorf("check output %q, step output %q; want dry-run", checks[0].Output, steps[0].Output)
	}
	if err := do(s, Restart{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "thread:exited 1", "output:"+restartNote, "output:w v1", "thread:started 1", "delivered:1 recorded", "stopped:entry")
	rec.none(t)
}
