package broker

import (
	"fmt"
	"os"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

// TestSetConfigAlias checks that a permitted change reaches Set with the
// initiator and returns the old and new values.
func TestSetConfigAlias(t *testing.T) {
	tb := newTestBroker(t)
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")
	id, address := auditRegister(t, tb, "w")
	if address != "w@hostA" {
		t.Fatalf("registration address = %q, want %q", address, "w@hostA")
	}

	resp, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:     "broker.alias",
		Value:   "build-box",
		AgentId: id,
	})
	if err != nil {
		t.Fatalf("SetConfig: %v", err)
	}

	if len(tb.cfg.setCalls) != 1 {
		t.Fatalf("Set calls = %d, want 1", len(tb.cfg.setCalls))
	}
	call := tb.cfg.setCalls[0]
	wantActor := config.Actor{UIDKnown: true, UID: uint32(os.Geteuid()), Account: "alice", Agent: address}
	if call.Actor != wantActor {
		t.Errorf("Set actor = %+v, want %+v", call.Actor, wantActor)
	}
	if call.Key != "broker.alias" {
		t.Errorf("Set key = %q, want %q", call.Key, "broker.alias")
	}
	if call.Value.Kind != config.KindString || call.Value.String != "build-box" {
		t.Errorf("Set value = %+v, want KindString %q", call.Value, "build-box")
	}
	if got := resp.GetOldValue().GetStringValue(); got != "hostA" {
		t.Errorf("old_value = %q, want %q", got, "hostA")
	}
	if got := resp.GetNewValue().GetStringValue(); got != "build-box" {
		t.Errorf("new_value = %q, want %q", got, "build-box")
	}
}

// TestSetConfigSecretRejectedBeforeParse checks that a secret key is refused
// before parsing, with a message that never contains the value.
func TestSetConfigSecretRejectedBeforeParse(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.secretKeys["pairing.static_key"] = true
	tb.cfg.values["pairing.static_key"] = config.Value{Kind: config.KindSecret, String: "K1"}
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

	for _, value := range []string{"abcdefghijklmnopq", "", "[unterminated"} {
		_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
			Key:   "pairing.static_key",
			Value: value,
		})
		detail := errorDetailOf(t, err)
		if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
			t.Errorf("value %q: code = %v, want INVALID_ARGUMENT", value, detail.GetCode())
		}
		msg := detail.GetMessage()
		if !strings.Contains(msg, "can only be set in the configuration file") {
			t.Errorf("value %q: message %q does not say where the key may be set", value, msg)
		}
		if !strings.Contains(msg, "/fake/config.yaml") {
			t.Errorf("value %q: message %q does not name the configuration file", value, msg)
		}
		if value != "" && strings.Contains(msg, value) {
			t.Errorf("value %q: message %q contains the value", value, msg)
		}
	}

	if tb.cfg.parseCalls != 0 {
		t.Errorf("ParseValue calls = %d, want 0", tb.cfg.parseCalls)
	}
	if len(tb.cfg.setCalls) != 0 {
		t.Errorf("Set calls = %d, want 0", len(tb.cfg.setCalls))
	}
}

// TestSetConfigParseError checks that an unparsable value is invalid-argument
// and never reaches Set.
func TestSetConfigParseError(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.parseErr = fmt.Errorf("%w: broker.alias: invalid alias", config.ErrInvalidValue)
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

	_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:   "broker.alias",
		Value: "bad",
	})
	detail := errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Errorf("code = %v, want INVALID_ARGUMENT", detail.GetCode())
	}
	if tb.cfg.parseCalls != 1 {
		t.Errorf("ParseValue calls = %d, want 1", tb.cfg.parseCalls)
	}
	if len(tb.cfg.setCalls) != 0 {
		t.Errorf("Set calls = %d, want 0", len(tb.cfg.setCalls))
	}
}

// TestSetConfigListenOverridden checks that broker.listen is refused before
// parsing while --listen fixes the address, and succeeds without --listen.
func TestSetConfigListenOverridden(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) { d.ListenOverride = ":17444" })
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

	for _, value := range []string{":17445", "[x"} {
		_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
			Key:   "broker.listen",
			Value: value,
		})
		detail := errorDetailOf(t, err)
		if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
			t.Errorf("value %q: code = %v, want INVALID_ARGUMENT", value, detail.GetCode())
		}
		msg := detail.GetMessage()
		if !strings.Contains(msg, "--listen") {
			t.Errorf("value %q: message %q does not mention --listen", value, msg)
		}
		if !strings.Contains(msg, ":17444") {
			t.Errorf("value %q: message %q does not name the fixed address", value, msg)
		}
	}
	if tb.cfg.parseCalls != 0 {
		t.Errorf("ParseValue calls = %d, want 0", tb.cfg.parseCalls)
	}
	if len(tb.cfg.setCalls) != 0 {
		t.Errorf("Set calls = %d, want 0", len(tb.cfg.setCalls))
	}

	free := newTestBroker(t)
	_, err := free.b.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:   "broker.listen",
		Value: "127.0.0.1:17445",
	})
	if err != nil {
		t.Fatalf("SetConfig without --listen: %v", err)
	}
	if len(free.cfg.setCalls) != 1 {
		t.Errorf("Set calls without --listen = %d, want 1", len(free.cfg.setCalls))
	}
}

// TestSetConfigListenUnavailable checks that a value the validator refuses as
// unavailable is invalid-argument.
func TestSetConfigListenUnavailable(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.setErr = fmt.Errorf("%w: broker.listen: address %q is unavailable: x",
		config.ErrInvalidValue, ":17445")
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

	_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:   "broker.listen",
		Value: "127.0.0.1:17445",
	})
	detail := errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Errorf("code = %v, want INVALID_ARGUMENT", detail.GetCode())
	}
	if !strings.Contains(detail.GetMessage(), "is unavailable") {
		t.Errorf("message %q does not say the address is unavailable", detail.GetMessage())
	}
}

// TestSetConfigCombination checks that an invalid combination and too many
// advertised addresses are both invalid-argument.
func TestSetConfigCombination(t *testing.T) {
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")
	cases := []struct {
		name string
		err  error
	}{
		{"invalid combination", config.ErrInvalidCombination},
		{"too many addresses", config.ErrTooManyAddresses},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tb := newTestBroker(t)
			tb.cfg.setErr = tc.err

			_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
				Key:   "broker.listen",
				Value: "127.0.0.1:17445",
			})
			detail := errorDetailOf(t, err)
			if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
				t.Errorf("code = %v, want INVALID_ARGUMENT", detail.GetCode())
			}
		})
	}
}

// TestSetConfigFileWriteIsInternal checks that a file write failure is an
// internal error.
func TestSetConfigFileWriteIsInternal(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.setErr = config.ErrFileWrite
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

	_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:   "broker.alias",
		Value: "build-box",
	})
	detail := errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INTERNAL {
		t.Errorf("code = %v, want INTERNAL", detail.GetCode())
	}
}

// TestSetConfigBadAgent checks that a bad agent_id is not-registered and
// reaches neither ParseValue nor Set.
func TestSetConfigBadAgent(t *testing.T) {
	tb := newTestBroker(t)
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

	_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:     "broker.alias",
		Value:   "build-box",
		AgentId: "nope",
	})
	detail := errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED {
		t.Errorf("code = %v, want NOT_REGISTERED", detail.GetCode())
	}
	if tb.cfg.parseCalls != 0 {
		t.Errorf("ParseValue calls = %d, want 0", tb.cfg.parseCalls)
	}
	if len(tb.cfg.setCalls) != 0 {
		t.Errorf("Set calls = %d, want 0", len(tb.cfg.setCalls))
	}
}

// TestSetConfigUnknownKey checks that an empty or unknown key is
// invalid-argument before parsing.
func TestSetConfigUnknownKey(t *testing.T) {
	t.Run("empty key", func(t *testing.T) {
		tb := newTestBroker(t)
		ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

		_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{Value: "x"})
		detail := errorDetailOf(t, err)
		if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
			t.Errorf("code = %v, want INVALID_ARGUMENT", detail.GetCode())
		}
		if !strings.Contains(detail.GetMessage(), "key is required") {
			t.Errorf("message %q does not say the key is required", detail.GetMessage())
		}
		if tb.cfg.parseCalls != 0 {
			t.Errorf("ParseValue calls = %d, want 0", tb.cfg.parseCalls)
		}
		if len(tb.cfg.setCalls) != 0 {
			t.Errorf("Set calls = %d, want 0", len(tb.cfg.setCalls))
		}
	})

	t.Run("unknown key", func(t *testing.T) {
		tb := newTestBroker(t)
		ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

		_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
			Key:   "bogus.key",
			Value: "x",
		})
		detail := errorDetailOf(t, err)
		if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
			t.Errorf("code = %v, want INVALID_ARGUMENT", detail.GetCode())
		}
		if !strings.Contains(detail.GetMessage(), "bogus.key") {
			t.Errorf("message %q does not name the key", detail.GetMessage())
		}
		if tb.cfg.parseCalls != 0 {
			t.Errorf("ParseValue calls = %d, want 0", tb.cfg.parseCalls)
		}
		if len(tb.cfg.setCalls) != 0 {
			t.Errorf("Set calls = %d, want 0", len(tb.cfg.setCalls))
		}
	})
}

// TestSetConfigLowPortDenied checks that the privileged-port rule is reported
// as permission_denied with the explanation intact.
func TestSetConfigLowPortDenied(t *testing.T) {
	tb := newTestBroker(t)
	tb.cfg.setErr = fmt.Errorf("%w: changing broker.listen to a port below 1024 requires root", config.ErrLowPortDenied)
	ctx := auditCallerContext(1000, "ana")

	_, err := tb.b.SetConfig(ctx, &localpb.SetConfigRequest{
		Key:   "broker.listen",
		Value: ":80",
	})
	detail := errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Errorf("code = %v, want PERMISSION_DENIED", detail.GetCode())
	}
	if !strings.Contains(detail.GetMessage(), "port below 1024") {
		t.Errorf("message %q does not explain the privileged port", detail.GetMessage())
	}
	if len(tb.cfg.setCalls) != 1 {
		t.Errorf("Set calls = %d, want 1", len(tb.cfg.setCalls))
	}
}
