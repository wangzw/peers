//go:build e2e

package e2e

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/syscontainer"
)

func sysPollDecode[T any](t *testing.T, deadline time.Time, session *syscontainer.Session, tool string, args map[string]any) (T, string) {
	t.Helper()
	var zero T
	text, isError, err := session.PollCall(t, deadline, tool, args)
	if err != nil {
		return zero, err.Error()
	}
	if isError {
		return zero, text
	}
	var out T
	if err := json.Unmarshal([]byte(text), &out); err != nil {
		return zero, "decoding " + tool + ": " + err.Error()
	}
	return out, ""
}
