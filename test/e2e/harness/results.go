package harness

import (
	"encoding/json"
	"errors"
	"strings"
	"testing"

	"github.com/wangzw/peers/test/e2e/node"
)

// Result is the outcome of one MCP tool call on a harness Session.
type Result struct {
	// ToolResult carries the tool call's text, error flag and structured content.
	node.ToolResult
}

// Decode unmarshals the structured content of a successful tool result into T.
// The caller passes string(r.Structured) after r.MustOK(t); T is normally an
// output structure of the mcpserver package.
func Decode[T any](text string) (T, error) {
	var value T
	if text == "" {
		return value, errors.New("harness: no structured content")
	}
	if err := json.Unmarshal([]byte(text), &value); err != nil {
		return value, err
	}
	return value, nil
}

// toolCodeMarker and toolOriginMarker frame the suffix mcpserver appends to an
// error result's text; the origin part is present only for an error from another
// Broker.
const (
	toolCodeMarker   = " (code: "
	toolOriginMarker = "; reported by Broker "
)

// ToolError extracts the error code name and the originating Broker
// fingerprint from a tool error's text. It looks at the last " (code: " so a
// forged one in the body does not win. ok is false when the text carries no
// such suffix.
func ToolError(text string) (code, originFingerprint string, ok bool) {
	index := strings.LastIndex(text, toolCodeMarker)
	if index < 0 {
		return "", "", false
	}
	rest := text[index+len(toolCodeMarker):]
	end := strings.IndexByte(rest, ')')
	if end < 0 {
		return "", "", false
	}
	code = rest[:end]
	if split := strings.Index(code, toolOriginMarker); split >= 0 {
		origin := code[split+len(toolOriginMarker):]
		code = code[:split]
		if isFingerprint(origin) {
			originFingerprint = origin
		}
	}
	if !validToolCode(code) {
		return "", "", false
	}
	return code, originFingerprint, true
}

func validToolCode(code string) bool {
	if code == "" {
		return false
	}
	for _, r := range code {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '_':
		default:
			return false
		}
	}
	return true
}

func isFingerprint(s string) bool {
	if len(s) != 64 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9', r >= 'a' && r <= 'f':
		default:
			return false
		}
	}
	return true
}

// Code returns the error code of a failed tool result, or "" when the result
// is not an error.
func (r Result) Code() string {
	if !r.IsError {
		return ""
	}
	code, _, _ := ToolError(r.Text)
	return code
}

// Origin returns the originating Broker fingerprint of a failed tool result,
// or "" when the text carries none.
func (r Result) Origin() string {
	_, origin, _ := ToolError(r.Text)
	return origin
}

// MustOK fails the test when the tool result reports an error.
func (r Result) MustOK(t testing.TB) {
	t.Helper()
	if r.IsError {
		t.Fatalf("harness: tool call failed: %s", r.Text)
	}
}

// MustCode fails the test unless the tool result is an error with the given
// error code name.
func (r Result) MustCode(t testing.TB, code string) {
	t.Helper()
	if got := r.Code(); got != code {
		t.Fatalf("harness: tool error code is %q, want %q; result: %s", got, code, r.Text)
	}
}

// HasPrefix reports whether the tool result text starts with prefix.
func (r Result) HasPrefix(prefix string) bool {
	return strings.HasPrefix(r.Text, prefix)
}
