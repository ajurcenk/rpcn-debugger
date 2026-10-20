package core

import "regexp"

// enginePathRef finds engine paths (root.pipeline.processors.0…) in text.
var enginePathRef = regexp.MustCompile(`\broot(?:\.[A-Za-z0-9_]+)+`)

// TranslateText returns text with every engine path tr maps replaced by
// its user path (AD-6); the rest, and all of text when tr is nil, stays as
// it is. It never touches line or column positions.
func TranslateText(tr Translate, text string) string {
	if tr == nil {
		return text
	}
	return enginePathRef.ReplaceAllStringFunc(text, func(m string) string {
		if up, ok := tr.Translate(m); ok {
			return up
		}
		return m
	})
}
