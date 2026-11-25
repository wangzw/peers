package broker

import (
	"context"
	"strings"
	"testing"

	"google.golang.org/protobuf/encoding/prototext"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

// TestGetConfigAll checks that an empty key returns every entry in key order
// with the current values.
func TestGetConfigAll(t *testing.T) {
	tb := newTestBroker(t)
	delete(tb.cfg.values, "log.level")
	tb.cfg.values["broker.alias"] = config.Value{Kind: config.KindString, String: "build-box"}
	tb.cfg.values["broker.listen"] = config.Value{Kind: config.KindString, String: "127.0.0.1:7000"}
	tb.cfg.values["broker.offline"] = config.Value{Kind: config.KindBool, Bool: false}
	tb.cfg.values["broker.public"] = config.Value{Kind: config.KindBool, Bool: true}
	tb.cfg.values["topics.auto_join"] = config.Value{Kind: config.KindBool, Bool: true}

	resp, err := tb.b.GetConfig(context.Background(), &localpb.GetConfigRequest{})
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}

	want := []*localpb.ConfigEntry{
		{Key: "broker.alias", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "build-box"}}},
		{Key: "broker.listen", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_StringValue{StringValue: "127.0.0.1:7000"}}},
		{Key: "broker.offline", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: false}}},
		{Key: "broker.public", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}}},
		{Key: "topics.auto_join", Value: &localpb.ConfigValue{Value: &localpb.ConfigValue_BoolValue{BoolValue: true}}},
	}
	got := resp.GetEntries()
	if len(got) != len(want) {
		t.Fatalf("entries = %d, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].GetKey() != want[i].GetKey() {
			t.Errorf("entry %d key = %q, want %q", i, got[i].GetKey(), want[i].GetKey())
			continue
		}
		if !proto.Equal(got[i].GetValue(), want[i].GetValue()) {
			t.Errorf("entry %q value = %v, want %v", got[i].GetKey(), got[i].GetValue(), want[i].GetValue())
		}
	}
}

// TestGetConfigSecretOnlySaysSet checks that a secret key is reported only as
// set, never with its value.
func TestGetConfigSecretOnlySaysSet(t *testing.T) {
	tb := newTestBroker(t)
	const secret = "S3CR3T-do-not-leak"
	tb.cfg.secretKeys["pairing.static_key"] = true
	tb.cfg.values["pairing.static_key"] = config.Value{Kind: config.KindSecret, String: secret}

	resp, err := tb.b.GetConfig(context.Background(), &localpb.GetConfigRequest{Key: "pairing.static_key"})
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	entries := resp.GetEntries()
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	isSet, ok := entries[0].GetValue().GetValue().(*localpb.ConfigValue_SecretIsSet)
	if !ok {
		t.Fatalf("value oneof = %T, want secret_is_set", entries[0].GetValue().GetValue())
	}
	if !isSet.SecretIsSet {
		t.Error("secret_is_set = false, want true")
	}
	if text := prototext.Format(resp); strings.Contains(text, secret) {
		t.Errorf("response text contains the secret value %q: %s", secret, text)
	}
}

// TestGetConfigUnknownKey checks that an unknown key is an invalid-argument
// error naming the key.
func TestGetConfigUnknownKey(t *testing.T) {
	tb := newTestBroker(t)

	_, err := tb.b.GetConfig(context.Background(), &localpb.GetConfigRequest{Key: "bogus.key"})
	detail := errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Errorf("code = %v, want INVALID_ARGUMENT", detail.GetCode())
	}
	if !strings.Contains(detail.GetMessage(), "bogus.key") {
		t.Errorf("message %q does not name the key", detail.GetMessage())
	}
}

// TestGetConfigOneKey checks that a non-empty key returns exactly that entry.
func TestGetConfigOneKey(t *testing.T) {
	tb := newTestBroker(t)

	resp, err := tb.b.GetConfig(context.Background(), &localpb.GetConfigRequest{Key: "broker.alias"})
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	entries := resp.GetEntries()
	if len(entries) != 1 {
		t.Fatalf("entries = %d, want 1", len(entries))
	}
	if entries[0].GetKey() != "broker.alias" {
		t.Errorf("key = %q, want %q", entries[0].GetKey(), "broker.alias")
	}
	s, ok := entries[0].GetValue().GetValue().(*localpb.ConfigValue_StringValue)
	if !ok {
		t.Fatalf("value oneof = %T, want string_value", entries[0].GetValue().GetValue())
	}
	if s.StringValue != "hostA" {
		t.Errorf("string_value = %q, want %q", s.StringValue, "hostA")
	}
}
