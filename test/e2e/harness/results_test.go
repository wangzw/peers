package harness

import (
	"strings"
	"testing"

	"github.com/wangzw/peers/test/e2e/node"
)

type agentShape struct {
	Address string `json:"address"`
	Status  string `json:"status"`
}

type listAgentsShape struct {
	Self   string       `json:"self_address"`
	Agents []agentShape `json:"agents"`
}

// TestDecode checks that Decode unmarshals structured content and rejects empty and malformed text.
func TestDecode(t *testing.T) {
	text := `{"self_address":"a@h","agents":[{"address":"b@h","status":"busy"}],"unknown":true}`

	out, err := Decode[listAgentsShape](text)
	if err != nil {
		t.Fatalf("Decode: %v", err)
	}
	if out.Self != "a@h" {
		t.Errorf("Self = %q, want %q", out.Self, "a@h")
	}
	if len(out.Agents) != 1 {
		t.Fatalf("len(Agents) = %d, want 1", len(out.Agents))
	}
	if out.Agents[0].Status != "busy" {
		t.Errorf("Agents[0].Status = %q, want %q", out.Agents[0].Status, "busy")
	}

	if _, err := Decode[listAgentsShape](""); err == nil {
		t.Error("Decode of empty text returned no error")
	} else if !strings.Contains(err.Error(), "no structured content") {
		t.Errorf("Decode of empty text returned %v, want an error mentioning no structured content", err)
	}
	if _, err := Decode[listAgentsShape]("{"); err == nil {
		t.Error("Decode of malformed JSON returned no error")
	}
}

// TestToolError checks that a tool error's code and originating Broker fingerprint are read from the last code marker in the text.
func TestToolError(t *testing.T) {
	fingerprint := strings.Repeat("a", 64)

	tests := []struct {
		name       string
		text       string
		wantCode   string
		wantOrigin string
		wantOK     bool
	}{
		{
			name:     "code only",
			text:     "Not sent: no such agent (code: not_found)",
			wantCode: "not_found",
			wantOK:   true,
		},
		{
			name:       "code and origin",
			text:       "Delivery unknown: link lost (code: delivery_unknown; reported by Broker " + fingerprint + ")",
			wantCode:   "delivery_unknown",
			wantOrigin: fingerprint,
			wantOK:     true,
		},
		{
			name:     "hint after the code",
			text:     "Not connected: rejected (code: pairing_rejected). Ask the peer for a new key",
			wantCode: "pairing_rejected",
			wantOK:   true,
		},
		{
			name:     "the last code wins",
			text:     "Not sent: text mentions (code: fake) but ends with (code: not_visible)",
			wantCode: "not_visible",
			wantOK:   true,
		},
		{
			name:   "no code",
			text:   "plain text",
			wantOK: false,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			code, origin, ok := ToolError(test.text)
			if ok != test.wantOK {
				t.Fatalf("ok = %v, want %v", ok, test.wantOK)
			}
			if code != test.wantCode {
				t.Errorf("code = %q, want %q", code, test.wantCode)
			}
			if origin != test.wantOrigin {
				t.Errorf("origin = %q, want %q", origin, test.wantOrigin)
			}
		})
	}
}

// TestResultCodeAndOrigin checks Code, Origin and HasPrefix on successful and failed tool results.
func TestResultCodeAndOrigin(t *testing.T) {
	ok := Result{ToolResult: node.ToolResult{Text: "fine"}}
	if got := ok.Code(); got != "" {
		t.Errorf("Code of a successful result = %q, want empty", got)
	}

	failed := Result{ToolResult: node.ToolResult{
		IsError: true,
		Text:    "Delivery unknown: link lost (code: delivery_unknown; reported by Broker " + strings.Repeat("b", 64) + ")",
	}}
	if got := failed.Code(); got != "delivery_unknown" {
		t.Errorf("Code = %q, want delivery_unknown", got)
	}
	if got := failed.Origin(); got != strings.Repeat("b", 64) {
		t.Errorf("Origin = %q, want the 64-character fingerprint", got)
	}
	if !failed.HasPrefix("Delivery unknown:") {
		t.Errorf("HasPrefix did not match the result text %q", failed.Text)
	}
}
