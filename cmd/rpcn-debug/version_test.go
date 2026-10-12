package main

import (
	"bytes"
	"strings"
	"testing"
)

func TestDepVersions(t *testing.T) {
	connect, benthos := depVersions()
	if connect != "v4.112.0" {
		t.Errorf("connect version = %q, want v4.112.0", connect)
	}
	if benthos != "v4.81.0" {
		t.Errorf("benthos version = %q, want v4.81.0", benthos)
	}
}

func TestRunVersion(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--version"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
	want := "rpcn-debug " + version + " (connect v4.112.0, benthos v4.81.0)\n"
	if stdout.String() != want {
		t.Errorf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Errorf("stderr = %q, want empty", stderr.String())
	}
}

func TestRunNoArgs(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run(nil, &stdout, &stderr); code != 1 {
		t.Fatalf("exit code = %d, want 1", code)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "not implemented") {
		t.Errorf("stderr = %q, want a not-implemented message", stderr.String())
	}
}

func TestRunUnknownFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--nope"}, &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "Usage") {
		t.Errorf("stderr = %q, want usage", stderr.String())
	}
}
