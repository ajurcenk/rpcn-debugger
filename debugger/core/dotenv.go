package core

import (
	"fmt"
	"os"
	"strings"
)

// parseDotenv parses the dotenv file src into a map (AD-22): one KEY=VALUE
// per line, with blank lines and lines starting with # skipped, an optional
// "export " prefix, and a value that is unquoted (trailing " # comment"
// dropped), single-quoted (literal) or double-quoted (with \n, \r, \t, \",
// \\ escapes). A later line overrides an earlier one. A malformed line is
// an error naming its 1-based line number; errors never quote a value,
// since values may be secrets.
func parseDotenv(src string) (map[string]string, error) {
	out := map[string]string{}
	for i, line := range strings.Split(src, "\n") {
		n := i + 1
		line = strings.TrimSpace(strings.TrimSuffix(line, "\r"))
		if i == 0 {
			line = strings.TrimPrefix(line, "\uFEFF")
		}
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		if rest, ok := strings.CutPrefix(line, "export"); ok && rest != "" && (rest[0] == ' ' || rest[0] == '\t') {
			line = strings.TrimSpace(rest)
		}
		key, raw, ok := strings.Cut(line, "=")
		if !ok {
			return nil, fmt.Errorf("line %d: expected NAME=value", n)
		}
		key = strings.TrimSpace(key)
		if !validEnvName(key) {
			return nil, fmt.Errorf("line %d: the name before = must be letters, digits and _, not starting with a digit", n)
		}
		val, err := dotenvValue(strings.TrimSpace(raw))
		if err != nil {
			return nil, fmt.Errorf("line %d (%s): %s", n, key, err)
		}
		out[key] = val
	}
	return out, nil
}

// dotenvValue decodes one trimmed value. Its errors never quote the value.
func dotenvValue(raw string) (string, error) {
	if raw == "" {
		return "", nil
	}
	switch q := raw[0]; q {
	case '\'', '"':
		var b strings.Builder
		i := 1
		for ; i < len(raw); i++ {
			c := raw[i]
			if c == q {
				break
			}
			if q == '"' && c == '\\' && i+1 < len(raw) {
				i++
				switch e := raw[i]; e {
				case 'n':
					b.WriteByte('\n')
				case 'r':
					b.WriteByte('\r')
				case 't':
					b.WriteByte('\t')
				case '"', '\\', '$':
					b.WriteByte(e)
				default:
					b.WriteByte('\\')
					b.WriteByte(e)
				}
				continue
			}
			b.WriteByte(c)
		}
		if i >= len(raw) {
			return "", fmt.Errorf("the quoted value has no closing %c", q)
		}
		if rest := strings.TrimSpace(raw[i+1:]); rest != "" && !strings.HasPrefix(rest, "#") {
			return "", fmt.Errorf("unexpected text after the closing %c", q)
		}
		return b.String(), nil
	}
	// An unquoted value ends at a # that follows whitespace or starts it
	// (raw is trimmed, so "A= # c" starts with the #).
	if raw[0] == '#' {
		return "", nil
	}
	for i := 1; i < len(raw); i++ {
		if raw[i] == '#' && (raw[i-1] == ' ' || raw[i-1] == '\t') {
			return strings.TrimSpace(raw[:i]), nil
		}
	}
	return raw, nil
}

func validEnvName(s string) bool {
	if s == "" {
		return false
	}
	for i, c := range s {
		switch {
		case c == '_', c >= 'A' && c <= 'Z', c >= 'a' && c <= 'z':
		case c >= '0' && c <= '9' && i > 0:
		default:
			return false
		}
	}
	return true
}

// EnvLookup returns the lookup the engine resolves ${VAR} with (AD-22):
// env wins over file, which wins over the process environment.
func EnvLookup(file, env map[string]string) func(string) (string, bool) {
	return func(name string) (string, bool) {
		if v, ok := env[name]; ok {
			return v, true
		}
		if v, ok := file[name]; ok {
			return v, true
		}
		return os.LookupEnv(name)
	}
}
