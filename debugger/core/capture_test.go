package core

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// launchSave launches e from cwd dir with saveCapture name and runs it.
func launchSave(t *testing.T, e *fakeEngine, dir, name string) (*Session, *recorder) {
	t.Helper()
	rec := newRecorder()
	s := NewSession(e, rec)
	if err := do(s, Launch{Pipeline: "/p/pipeline.yaml", Cwd: dir, SaveCapture: name}); err != nil {
		t.Fatal(err)
	}
	if err := do(s, ConfigurationDone{}); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = do(s, Terminate{}) })
	return s, rec
}

func readFile(t *testing.T, path string) string {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return string(raw)
}

// saveCapture saves the kept batch right after the capture and before the
// entry stop: into <cwd>/.rpcn-debug/ (0700) with a .gitignore holding *,
// then the console note names the absolute path.
func TestSaveCaptureAfterCapture(t *testing.T) {
	dir := t.TempDir()
	e := newEngine()
	e.captured = [2]int{10, 12}
	s, rec := launchSave(t, e, dir, "orders.json")
	path := filepath.Join(dir, ".rpcn-debug", "orders.json")
	expect(t, rec, "output:captured 10 of 12", "output:Saved the capture to "+path+".\n", "stopped:entry")

	saves := e.savedCaptures()
	if len(saves) != 1 || saves[0].path != path || saves[0].pipeline != "/p/pipeline.yaml" ||
		saves[0].b.Kept != 10 || saves[0].b.Total != 12 {
		t.Fatalf("saves = %+v", saves)
	}
	if got := readFile(t, filepath.Join(dir, ".rpcn-debug", ".gitignore")); got != "*\n" {
		t.Errorf(".gitignore = %q, want *", got)
	}
	fi, err := os.Stat(filepath.Join(dir, ".rpcn-debug"))
	if err != nil {
		t.Fatal(err)
	}
	if fi.Mode().Perm() != 0o700 {
		t.Errorf(".rpcn-debug mode = %v, want 0700", fi.Mode().Perm())
	}
	if err := do(s, Continue{}); err != nil {
		t.Fatal(err)
	}
	<-e.delivered
}

// A name in a subfolder creates it.
func TestSaveCaptureSubfolder(t *testing.T) {
	dir := t.TempDir()
	e := newEngine()
	_, rec := launchSave(t, e, dir, "sub/x.json")
	path := filepath.Join(dir, ".rpcn-debug", "sub", "x.json")
	expect(t, rec, "output:Saved the capture to "+path, "stopped:entry")
	if got := readFile(t, path); got != "capture" {
		t.Errorf("capture = %q", got)
	}
}

// A name that leaves .rpcn-debug/ fails the launch, naming saveCapture,
// before the pipeline is checked.
func TestSaveCaptureEscapingName(t *testing.T) {
	for _, name := range []string{"../x.json", "/tmp/x.json", "a/../../x.json", ".", ".gitignore", "a/.."} {
		t.Run(name, func(t *testing.T) {
			var got string
			s := NewSession(&pathEngine{got: &got}, newRecorder())
			err := do(s, Launch{Pipeline: "/p/pipeline.yaml", Cwd: t.TempDir(), SaveCapture: name})
			if !HasCode(err, CodeLaunchInvalid) || !strings.Contains(err.Error(), `"saveCapture"`) {
				t.Errorf("launch = %v, want LAUNCH_INVALID naming saveCapture", err)
			}
			if got != "" {
				t.Error("the engine checked the pipeline after an invalid launch")
			}
		})
	}
}

// No saveCapture, no save.
func TestNoSaveCapture(t *testing.T) {
	dir := t.TempDir()
	e := newEngine()
	_, rec := launchSave(t, e, dir, "")
	expect(t, rec, "stopped:entry")
	if saves := e.savedCaptures(); len(saves) != 0 {
		t.Errorf("saves = %+v", saves)
	}
	if _, err := os.Stat(filepath.Join(dir, ".rpcn-debug")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(".rpcn-debug: %v, want none", err)
	}
}

// An existing .gitignore is never changed.
func TestSaveCaptureKeepsGitignore(t *testing.T) {
	dir := t.TempDir()
	gi := filepath.Join(dir, ".rpcn-debug", ".gitignore")
	if err := os.MkdirAll(filepath.Dir(gi), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(gi, []byte("foo"), 0o600); err != nil {
		t.Fatal(err)
	}
	_, rec := launchSave(t, newEngine(), dir, "orders.json")
	expect(t, rec, "output:Saved the capture", "stopped:entry")
	if got := readFile(t, gi); got != "foo" {
		t.Errorf(".gitignore = %q, want foo", got)
	}
}

// A failed save is a stderr note naming the path and the error, and the
// run still steps and delivers the batch.
func TestSaveCaptureFails(t *testing.T) {
	t.Run("folder is a file", func(t *testing.T) {
		dir := t.TempDir()
		if err := os.WriteFile(filepath.Join(dir, ".rpcn-debug"), nil, 0o600); err != nil {
			t.Fatal(err)
		}
		e := newEngine()
		s, rec := launchSave(t, e, dir, "orders.json")
		ev := rec.next(t)
		if ev.kind != "output" || !strings.HasPrefix(ev.detail, "Could not save the capture to "+filepath.Join(dir, ".rpcn-debug", "orders.json")+": ") {
			t.Fatalf("event = %+v, want the failed-save note", ev)
		}
		expect(t, rec, "stopped:entry")
		if err := do(s, Continue{}); err != nil {
			t.Fatal(err)
		}
		<-e.delivered
	})
	t.Run("engine fails", func(t *testing.T) {
		e := newEngine()
		e.saveErr = errors.New("disk full")
		s, rec := launchSave(t, e, t.TempDir(), "orders.json")
		expect(t, rec, "output:: disk full\n", "stopped:entry")
		if err := do(s, Continue{}); err != nil {
			t.Fatal(err)
		}
		<-e.delivered
	})
}

// Restart re-steps the saved batch without saving it again.
func TestRestartDoesNotSave(t *testing.T) {
	dir := t.TempDir()
	e := &restartEngine{mapping: "v1"}
	s, rec := launchRestart(t, e, Launch{Cwd: dir, SaveCapture: "r.json"})
	path := filepath.Join(dir, ".rpcn-debug", "r.json")
	expect(t, rec, "output:w v1", "thread:started 1", "output:Saved the capture to "+path, "delivered:1 recorded", "stopped:entry")
	if err := do(s, Restart{}); err != nil {
		t.Fatal(err)
	}
	expect(t, rec, "thread:exited 1", "output:"+restartNote, "output:w v1", "thread:started 1", "delivered:1 recorded", "stopped:entry")
	rec.none(t)
	e.mu.Lock()
	saves := append([]string(nil), e.saves...)
	e.mu.Unlock()
	if len(saves) != 1 || saves[0] != path {
		t.Errorf("saves = %q, want one to %s", saves, path)
	}
}

// Do(SaveCapture) saves the kept batch by the same rules: the folder, the
// .gitignore and the note. It is LAUNCH_INVALID for an escaping name,
// NOT_SUPPORTED before the capture, RUN_FAILED when the save fails and
// SESSION_ENDED after Terminate.
func TestSaveCaptureCommand(t *testing.T) {
	dir := t.TempDir()
	e := newEngine()
	hold := make(chan struct{})
	e.hold = hold
	s, rec := launchSave(t, e, dir, "")
	if err := do(s, SaveCapture{Path: "x.json"}); !HasCode(err, CodeNotSupported) {
		t.Errorf("SaveCapture before the capture = %v, want NOT_SUPPORTED", err)
	}
	close(hold)
	expect(t, rec, "stopped:entry")

	if err := do(s, SaveCapture{Path: "../x.json"}); !HasCode(err, CodeLaunchInvalid) {
		t.Errorf("SaveCapture of ../x.json = %v, want LAUNCH_INVALID", err)
	}
	if err := do(s, SaveCapture{Path: "x.json"}); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dir, ".rpcn-debug", "x.json")
	expect(t, rec, "output:Saved the capture to "+path+".\n")
	if saves := e.savedCaptures(); len(saves) != 1 || saves[0].path != path {
		t.Errorf("saves = %+v", saves)
	}
	if got := readFile(t, filepath.Join(dir, ".rpcn-debug", ".gitignore")); got != "*\n" {
		t.Errorf(".gitignore = %q", got)
	}

	e.mu.Lock()
	e.saveErr = errors.New("disk full")
	e.mu.Unlock()
	if err := do(s, SaveCapture{Path: "y.json"}); !HasCode(err, CodeRunFailed) || !strings.Contains(err.Error(), "disk full") {
		t.Errorf("failed SaveCapture = %v, want RUN_FAILED", err)
	}
	_ = do(s, Terminate{})
	if err := do(s, SaveCapture{Path: "x.json"}); !HasCode(err, CodeSessionEnded) {
		t.Errorf("SaveCapture after Terminate = %v, want SESSION_ENDED", err)
	}
}
