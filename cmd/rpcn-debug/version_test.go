package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"strconv"
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
	if code := run([]string{"--version"}, strings.NewReader(""), &stdout, &stderr); code != 0 {
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

func dapFrame(body string) string {
	return fmt.Sprintf("Content-Length: %d\r\n\r\n%s", len(body), body)
}

// readFrame reads one Content-Length framed DAP message body from r.
func readFrame(r *bufio.Reader) ([]byte, error) {
	header, err := r.ReadString('\n')
	if err != nil {
		return nil, err
	}
	n, err := strconv.Atoi(strings.TrimSpace(strings.TrimPrefix(header, "Content-Length:")))
	if err != nil {
		return nil, fmt.Errorf("bad header %q: %w", header, err)
	}
	if blank, err := r.ReadString('\n'); err != nil || blank != "\r\n" {
		return nil, fmt.Errorf("bad header delimiter %q: %v", blank, err)
	}
	body := make([]byte, n)
	if _, err := io.ReadFull(r, body); err != nil {
		return nil, err
	}
	return body, nil
}

func TestRunNoArgs(t *testing.T) {
	stdin := strings.NewReader(
		dapFrame(`{"seq":1,"type":"request","command":"initialize","arguments":{"adapterID":"redpanda-connect"}}`) +
			dapFrame(`{"seq":2,"type":"request","command":"disconnect","arguments":{}}`))
	var stdout, stderr bytes.Buffer
	if code := run(nil, stdin, &stdout, &stderr); code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", code, stderr.String())
	}

	r := bufio.NewReader(&stdout)
	for i, command := range []string{"initialize", "disconnect"} {
		raw, err := readFrame(r)
		if err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		var resp struct {
			Seq        int    `json:"seq"`
			Type       string `json:"type"`
			RequestSeq int    `json:"request_seq"`
			Command    string `json:"command"`
			Success    bool   `json:"success"`
		}
		if err := json.Unmarshal(raw, &resp); err != nil {
			t.Fatalf("response %d: %v", i, err)
		}
		if resp.Seq != i+1 || resp.Type != "response" || resp.RequestSeq != i+1 ||
			resp.Command != command || !resp.Success {
			t.Errorf("response %d = %s, want a successful %s response to seq %d", i, raw, command, i+1)
		}
	}
	if rest, _ := io.ReadAll(r); len(rest) != 0 {
		t.Errorf("unexpected extra stdout: %q", rest)
	}
}

func TestRunUnknownFlag(t *testing.T) {
	var stdout, stderr bytes.Buffer
	if code := run([]string{"--nope"}, strings.NewReader(""), &stdout, &stderr); code != 2 {
		t.Fatalf("exit code = %d, want 2", code)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if !strings.Contains(stderr.String(), "Usage") {
		t.Errorf("stderr = %q, want usage", stderr.String())
	}
}
