package core

import (
	"os"
	"os/user"
	"strings"
)

// debugGroupPrefix starts every debug-group base (AD-5).
const debugGroupPrefix = "rpcn-debug-"

// debugGroup returns the debug-group base rpcn-debug-<os-user> (AD-5). The
// user is the process user, never the pipeline's env or envFile.
func debugGroup() string {
	name := ""
	if u, err := user.Current(); err == nil {
		name = u.Username
	}
	return debugGroupName(name, os.Getenv)
}

// debugGroupName builds the debug-group base from the process user name:
// with an empty name it falls back to $USER, then $USERNAME, then "user".
// The name is lowercased and every rune outside [a-z0-9._-] becomes '-', so
// DOMAIN\Jane Doe becomes domain-jane-doe.
func debugGroupName(name string, getenv func(string) string) string {
	if name == "" {
		name = getenv("USER")
	}
	if name == "" {
		name = getenv("USERNAME")
	}
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	if b.Len() == 0 {
		return debugGroupPrefix + "user"
	}
	return debugGroupPrefix + b.String()
}
