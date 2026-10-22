package engine

import (
	"errors"
	"fmt"
	"regexp"
	"strconv"
	"strings"

	"github.com/redpanda-data/benthos/v4/public/service"

	"github.com/ajurcenk/rpcn-debugger/debugger/core"
)

// yamlPos is the "yaml: line N: " that starts a YAML parse error.
var yamlPos = regexp.MustCompile(`^yaml: line (\d+): `)

// unmappedNote follows an error whose positions could not all be mapped
// back to the user's file.
const unmappedNote = " (line:col positions refer to the rewritten config, not the pipeline file)"

// configError returns err, the error of the Add*YAML call for config part,
// with its positions mapped to the user's file through Configs.Positions
// (AD-6). A lint error is rebuilt from its items as the engine formats it
// ("lint errors: " then one item per line), each item as
// "<user path>: (line X, column Y) <what>", so nothing in the items' text
// is rewritten; a YAML parse error gets its leading "yaml: line N: "
// mapped. The result is a PIPELINE_INVALID core.Error, Inline, whose Line
// and UserPath come from the first mapped position. When a position can't
// be mapped it stays as written, and the message keeps label (what the
// part is to the user) and says so. Without Positions, or without
// positions, it is err wrapped in label.
func (r *run) configError(part, label string, err error) error {
	pos := r.conf.Positions
	if pos == nil {
		return fmt.Errorf("%s: %w", label, err)
	}
	e := core.Error{Code: core.CodePipelineInvalid, Inline: true}
	unmapped := false
	// at returns the mapped "<path>: (line X, column Y)" for (line, col).
	at := func(line, col int, written string) string {
		fl, fc, up, ok := pos(part, line, col)
		if !ok {
			unmapped = true
			return written
		}
		if up == "" {
			up = label
		}
		if e.Line == 0 {
			e.Line, e.UserPath = fl, up
		}
		if fc <= 0 || col <= 0 {
			return fmt.Sprintf("%s: (line %d)", up, fl)
		}
		return fmt.Sprintf("%s: (line %d, column %d)", up, fl, fc)
	}

	var text string
	var lints service.LintError
	switch {
	case errors.As(err, &lints) && len(lints) > 0:
		items := make([]string, len(lints))
		for i, l := range lints {
			items[i] = at(l.Line, l.Column, fmt.Sprintf("(%d,%d)", l.Line, l.Column)) + " " + l.What
		}
		text = "lint errors: " + strings.Join(items, "\n")
	default:
		text = err.Error()
		m := yamlPos.FindStringSubmatch(text)
		if m == nil {
			return fmt.Errorf("%s: %w", label, err)
		}
		line, _ := strconv.Atoi(m[1])
		mapped := at(line, 0, "")
		if mapped == "" {
			e.Message = label + ": " + text + unmappedNote
			return e
		}
		// The mapped position names the line: "<path>: (line X) yaml: …".
		text = mapped + " yaml: " + strings.TrimPrefix(text, m[0])
	}
	if unmapped {
		e.Message = label + ": " + text + unmappedNote
		return e
	}
	e.Message = text
	return e
}
