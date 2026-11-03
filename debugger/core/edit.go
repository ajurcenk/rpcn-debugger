package core

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"slices"
	"strconv"
	"time"
	"unicode/utf8"
)

// The parsing of SetMessage values (AD-19). Each parse takes the value as
// the user entered it and the message (or key) as the stop shows it, and
// returns the new lossless form, or an error saying why the text is
// refused.

// editContent returns the new raw content for entered: its bytes when cur
// is shown as text, or its base64 decoding when cur is shown as base64.
func editContent(cur Content, entered []byte) ([]byte, error) {
	if cur.Text != nil {
		return slices.Clone(entered), nil
	}
	raw, err := base64.StdEncoding.DecodeString(string(entered))
	if err != nil {
		return nil, fmt.Errorf("the content is shown as base64 (it isn't UTF-8), and %q is not base64: %v", clip(string(entered)), err)
	}
	return raw, nil
}

// contentOf returns the views of raw, as a stepper's Snapshot has them:
// the text view when raw is valid UTF-8, and the JSON view when it is
// also a valid JSON document.
func contentOf(raw []byte) Content {
	c := Content{Raw: raw}
	if utf8.Valid(raw) {
		text := string(raw)
		c.Text = &text
		if json.Valid(raw) {
			c.JSON = json.RawMessage(slices.Clone(raw))
		}
	}
	return c
}

// editMeta returns key cur with the value entered, parsed by cur's type
// (see SetMessage.Metadata), with its lossless Value and display Text.
func editMeta(cur Meta, entered string) (Meta, error) {
	out := Meta{Key: cur.Key, Type: cur.Type, Internal: cur.Internal}
	fail := func(want string, err error) (Meta, error) {
		msg := fmt.Sprintf("metadata %q is %s, and %q is not %s", cur.Key, cur.Type, clip(entered), want)
		if err != nil {
			msg += ": " + err.Error()
		}
		return Meta{}, errors.New(msg)
	}
	switch cur.Type {
	case MetaString:
		out.Value = entered
	case MetaBytes:
		raw := []byte(entered)
		if cur.Text == nil {
			var err error
			if raw, err = base64.StdEncoding.DecodeString(entered); err != nil {
				return fail("base64 (the value isn't shown as text)", err)
			}
		}
		out.Value = base64.StdEncoding.EncodeToString(raw)
		if utf8.Valid(raw) {
			text := string(raw)
			out.Text = &text
		}
	case MetaInt:
		if n, err := strconv.ParseInt(entered, 10, 64); err == nil {
			out.Value = strconv.FormatInt(n, 10)
		} else if u, uerr := strconv.ParseUint(entered, 10, 64); uerr == nil {
			out.Value = strconv.FormatUint(u, 10)
		} else {
			return fail("a decimal integer", nil)
		}
	case MetaFloat:
		f, err := strconv.ParseFloat(entered, 64)
		if err != nil {
			return fail("a number", nil)
		}
		out.Value = strconv.FormatFloat(f, 'g', -1, 64)
	case MetaBool:
		if entered != "true" && entered != "false" {
			return fail("true or false", nil)
		}
		out.Value = entered
	case MetaTimestamp:
		t, err := time.Parse(time.RFC3339Nano, entered)
		if err != nil {
			return fail("an RFC 3339 timestamp", nil)
		}
		out.Value = t.Format(time.RFC3339Nano)
	case MetaObject, MetaArray:
		v, err := decodeJSON(entered)
		if err != nil {
			return fail("JSON", err)
		}
		_, isObject := v.(map[string]any)
		_, isArray := v.([]any)
		switch {
		case cur.Type == MetaObject && !isObject:
			return fail("a JSON object", nil)
		case cur.Type == MetaArray && !isArray:
			return fail("a JSON array", nil)
		}
		// The canonical form, as a stepper's Snapshot shows the value: keys
		// sorted, numbers as written.
		raw, err := json.Marshal(v)
		if err != nil {
			return fail("JSON", err)
		}
		out.Value = string(raw)
	case MetaNull:
		if entered != "null" {
			return fail("null (a null value can only stay null)", nil)
		}
		out.Value = "null"
	default:
		return Meta{}, fmt.Errorf("metadata %q has the unknown type %q", cur.Key, cur.Type)
	}
	return out, nil
}

// decodeJSON decodes exactly one JSON value from text.
func decodeJSON(text string) (any, error) {
	dec := json.NewDecoder(bytes.NewReader([]byte(text)))
	dec.UseNumber()
	var v any
	if err := dec.Decode(&v); err != nil {
		return nil, err
	}
	if dec.More() {
		return nil, errors.New("trailing data after the JSON value")
	}
	return v, nil
}

// clipMax bounds the entered text an EDIT_INVALID message quotes.
const clipMax = 60

func clip(s string) string {
	if r := []rune(s); len(r) > clipMax {
		return string(r[:clipMax]) + "…"
	}
	return s
}
