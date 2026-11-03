package core

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
)

// CaptureDir is the folder under cwd that capture files are written to
// (AD-22). It is gitignored by its own .gitignore.
const CaptureDir = ".rpcn-debug"

// captureGitignore is the .gitignore a save creates in CaptureDir when it
// has none: it ignores everything there, itself included.
const captureGitignore = "*\n"

// checkCaptureName checks a capture file name: a local relative path
// (filepath.IsLocal) under CaptureDir that names a file other than its
// .gitignore. key names the setting in the error.
func checkCaptureName(key, name string) error {
	clean := filepath.Clean(name)
	if !filepath.IsLocal(name) || clean == "." || clean == ".gitignore" {
		return NewError(CodeLaunchInvalid, "%s %q must be a file name inside %s/ under cwd: a relative path that doesn't leave that folder (no .. or absolute path)",
			key, name, CaptureDir)
	}
	return nil
}

// saveCapture writes b to the capture file <cwd>/.rpcn-debug/<name> through
// the engine, and returns its absolute path. It creates the folder (mode
// 0700) and, when missing, its .gitignore holding *; an existing .gitignore
// is never changed. name was checked by checkCaptureName.
func (s *Session) saveCapture(b CapturedBatch, name string) (string, error) {
	s.mu.Lock()
	cwd, pipeline := s.cwd, s.pipeline
	s.mu.Unlock()
	dir := filepath.Join(cwd, CaptureDir)
	path := filepath.Join(dir, name)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return path, err
	}
	if err := writeGitignore(filepath.Join(dir, ".gitignore")); err != nil {
		return path, err
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		return path, err
	}
	return path, s.engine.SaveCapture(b, pipeline, path)
}

// writeGitignore creates the .gitignore at path holding captureGitignore,
// unless a file is already there.
func writeGitignore(path string) error {
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, fs.ErrExist) {
		return nil
	}
	if err != nil {
		return err
	}
	_, err = f.WriteString(captureGitignore)
	return errors.Join(err, f.Close())
}

// savedNote is the console note after a capture file is written.
func savedNote(path string) string {
	return fmt.Sprintf("Saved the capture to %s.\n", path)
}

// autoSave saves b, the batch the run just captured, when the launch sets
// saveCapture (AD-11, AD-22), and tells the user where. A failed save is a
// stderr note: the run still steps the batch. It does nothing once the
// session ended. A Restart that replaced the run meanwhile doesn't stop
// it: the file holds the captured batch, which the Restart re-steps.
func (s *Session) autoSave(b CapturedBatch) {
	s.mu.Lock()
	name, ended := s.saveName, s.ended
	s.mu.Unlock()
	if name == "" || ended {
		return
	}
	path, err := s.saveCapture(b, name)
	if err != nil {
		s.fe.Emit(Output{Category: "stderr", Text: fmt.Sprintf("Could not save the capture to %s: %v\n", path, err)})
		return
	}
	s.fe.Emit(Output{Category: "console", Text: savedNote(path)})
}

// saveCaptureCmd runs SaveCapture: it saves the kept batch to name by the
// rules of saveCapture. It is NOT_SUPPORTED before the batch is captured,
// and a failed save is RUN_FAILED.
func (s *Session) saveCaptureCmd(name string) error {
	if err := checkCaptureName("the capture file", name); err != nil {
		return err
	}
	s.mu.Lock()
	ended, kept := s.ended, s.kept
	s.mu.Unlock()
	switch {
	case ended:
		return NewError(CodeSessionEnded, "the debug session ended")
	case kept == nil:
		return NewError(CodeNotSupported, "Saving a capture is not supported before the batch is captured")
	}
	path, err := s.saveCapture(*kept, name)
	if err != nil {
		return NewError(CodeRunFailed, "save the capture to %s: %v", path, err)
	}
	s.fe.Emit(Output{Category: "console", Text: savedNote(path)})
	return nil
}
