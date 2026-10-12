package main

import (
	"fmt"
	"runtime/debug"
)

// Set at build time with -ldflags "-X main.version=... -X main.date=...".
var (
	version = "dev"
	date    = ""
)

const (
	connectModule = "github.com/redpanda-data/connect/v4"
	benthosModule = "github.com/redpanda-data/benthos/v4"
)

// depVersions returns the versions of connect and benthos linked into this
// binary, read from its build info. Unknown versions are reported as
// "unknown".
func depVersions() (connect, benthos string) {
	connect, benthos = "unknown", "unknown"
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return connect, benthos
	}
	for _, dep := range info.Deps {
		m := dep
		if m.Replace != nil {
			m = m.Replace
		}
		switch dep.Path {
		case connectModule:
			connect = m.Version
		case benthosModule:
			benthos = m.Version
		}
	}
	return connect, benthos
}

// versionLine is the single line printed by --version.
func versionLine() string {
	connect, benthos := depVersions()
	return fmt.Sprintf("rpcn-debug %s (connect %s, benthos %s)", version, connect, benthos)
}
