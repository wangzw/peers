package broker

import (
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

// TestToConfigValueEachKind checks that every value kind converts to the
// matching wire oneof.
func TestToConfigValueEachKind(t *testing.T) {
	tests := []struct {
		name  string
		value config.Value
		check func(t *testing.T, got *localpb.ConfigValue)
	}{
		{
			name:  "string",
			value: config.Value{Kind: config.KindString, String: "build-box"},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				s, ok := got.Value.(*localpb.ConfigValue_StringValue)
				if !ok {
					t.Fatalf("oneof is %T, want string_value", got.Value)
				}
				if s.StringValue != "build-box" {
					t.Errorf("string_value = %q, want %q", s.StringValue, "build-box")
				}
			},
		},
		{
			name:  "bool",
			value: config.Value{Kind: config.KindBool, Bool: true},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				b, ok := got.Value.(*localpb.ConfigValue_BoolValue)
				if !ok {
					t.Fatalf("oneof is %T, want bool_value", got.Value)
				}
				if !b.BoolValue {
					t.Error("bool_value = false, want true")
				}
			},
		},
		{
			name:  "bool false",
			value: config.Value{Kind: config.KindBool, Bool: false},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				b, ok := got.Value.(*localpb.ConfigValue_BoolValue)
				if !ok {
					t.Fatalf("oneof is %T, want bool_value", got.Value)
				}
				if b.BoolValue {
					t.Error("bool_value = true, want false")
				}
			},
		},
		{
			name:  "int",
			value: config.Value{Kind: config.KindInt, Int: -42},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				i, ok := got.Value.(*localpb.ConfigValue_IntValue)
				if !ok {
					t.Fatalf("oneof is %T, want int_value", got.Value)
				}
				if i.IntValue != -42 {
					t.Errorf("int_value = %d, want -42", i.IntValue)
				}
			},
		},
		{
			name:  "int beyond int32",
			value: config.Value{Kind: config.KindInt, Int: 1 << 40},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				i, ok := got.Value.(*localpb.ConfigValue_IntValue)
				if !ok {
					t.Fatalf("oneof is %T, want int_value", got.Value)
				}
				if i.IntValue != 1<<40 {
					t.Errorf("int_value = %d, want %d", i.IntValue, int64(1<<40))
				}
			},
		},
		{
			name:  "duration",
			value: config.Value{Kind: config.KindDuration, Duration: 1500 * time.Millisecond},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				d, ok := got.Value.(*localpb.ConfigValue_DurationValue)
				if !ok {
					t.Fatalf("oneof is %T, want duration_value", got.Value)
				}
				if d.DurationValue.AsDuration() != 1500*time.Millisecond {
					t.Errorf("duration_value = %v, want %v", d.DurationValue.AsDuration(), 1500*time.Millisecond)
				}
			},
		},
		{
			name:  "string list",
			value: config.Value{Kind: config.KindStringList, Strings: []string{"alpha", "beta"}},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				l, ok := got.Value.(*localpb.ConfigValue_StringListValue)
				if !ok {
					t.Fatalf("oneof is %T, want string_list_value", got.Value)
				}
				want := []string{"alpha", "beta"}
				if len(l.StringListValue.GetValues()) != len(want) {
					t.Fatalf("values = %v, want %v", l.StringListValue.GetValues(), want)
				}
				for i := range want {
					if l.StringListValue.GetValues()[i] != want[i] {
						t.Errorf("values[%d] = %q, want %q", i, l.StringListValue.GetValues()[i], want[i])
					}
				}
			},
		},
		{
			name:  "secret not set",
			value: config.Value{Kind: config.KindSecret, SecretIsSet: false},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				s, ok := got.Value.(*localpb.ConfigValue_SecretIsSet)
				if !ok {
					t.Fatalf("oneof is %T, want secret_is_set", got.Value)
				}
				if s.SecretIsSet {
					t.Error("secret_is_set = true, want false")
				}
			},
		},
		{
			name:  "secret is set",
			value: config.Value{Kind: config.KindSecret, SecretIsSet: true},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				s, ok := got.Value.(*localpb.ConfigValue_SecretIsSet)
				if !ok {
					t.Fatalf("oneof is %T, want secret_is_set", got.Value)
				}
				if !s.SecretIsSet {
					t.Error("secret_is_set = false, want true")
				}
			},
		},
		{
			name:  "unset",
			value: config.Value{Kind: config.KindUnset},
			check: func(t *testing.T, got *localpb.ConfigValue) {
				t.Helper()
				if got != nil {
					t.Fatalf("got %v, want nil", got)
				}
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toConfigValue(tt.value)
			if tt.value.Kind != config.KindUnset && got == nil {
				t.Fatal("got nil, want a value")
			}
			tt.check(t, got)
		})
	}
}

// TestToConfigValueEmptyStringList checks that an empty string list converts
// to an empty string_list_value.
func TestToConfigValueEmptyStringList(t *testing.T) {
	got := toConfigValue(config.Value{Kind: config.KindStringList})
	if got == nil {
		t.Fatal("got nil, want a string_list_value")
	}
	l, ok := got.Value.(*localpb.ConfigValue_StringListValue)
	if !ok {
		t.Fatalf("oneof is %T, want string_list_value", got.Value)
	}
	if len(l.StringListValue.GetValues()) != 0 {
		t.Errorf("values = %v, want empty", l.StringListValue.GetValues())
	}
}

// TestToConfigValueCopiesStrings checks that the converted value owns a copy
// of the string list.
func TestToConfigValueCopiesStrings(t *testing.T) {
	in := []string{"alpha", "beta"}
	got := toConfigValue(config.Value{Kind: config.KindStringList, Strings: in})
	l := got.GetStringListValue()
	in[0] = "changed"
	_ = append(in, "gamma")
	if values := l.GetValues(); len(values) != 2 || values[0] != "alpha" || values[1] != "beta" {
		t.Errorf("values = %v after mutating the input, want [alpha beta]", values)
	}
}

// TestAuditEventKindMappingIsComplete checks that every audit event kind maps
// to a distinct wire kind and an unknown kind to UNSPECIFIED.
func TestAuditEventKindMappingIsComplete(t *testing.T) {
	kinds := []config.EventKind{
		config.EventConfigSet,
		config.EventConfigFileChanged,
		config.EventPairingKeyCreated,
		config.EventPairingSucceeded,
		config.EventPairingFailed,
		config.EventLinkUp,
		config.EventLinkDown,
		config.EventUnpaired,
		config.EventConnectionRejected,
		config.EventRejectedByPeer,
		config.EventLocalAliasAssigned,
		config.EventConfigRejected,
		config.EventUpdateApplied,
		config.EventUpdateFailed,
	}
	seen := make(map[localpb.AuditEventKind]config.EventKind, len(kinds))
	for _, kind := range kinds {
		got := toAuditEventKind(kind)
		if got == localpb.AuditEventKind_AUDIT_EVENT_KIND_UNSPECIFIED {
			t.Errorf("%s maps to UNSPECIFIED", kind)
			continue
		}
		if other, ok := seen[got]; ok {
			t.Errorf("%s and %s both map to %s", other, kind, got)
			continue
		}
		seen[got] = kind
	}
	if got := toAuditEventKind(config.EventKind("bogus")); got != localpb.AuditEventKind_AUDIT_EVENT_KIND_UNSPECIFIED {
		t.Errorf("bogus maps to %s, want UNSPECIFIED", got)
	}
}

// TestUpdateAuditEventNumbersMatchTheProtocol checks that the update audit
// events keep their assigned local-protocol numbers.
func TestUpdateAuditEventNumbersMatchTheProtocol(t *testing.T) {
	cases := []struct {
		kind   config.EventKind
		wire   localpb.AuditEventKind
		number int32
	}{
		{config.EventUpdateApplied, localpb.AuditEventKind_AUDIT_EVENT_KIND_UPDATE_APPLIED, 13},
		{config.EventUpdateFailed, localpb.AuditEventKind_AUDIT_EVENT_KIND_UPDATE_FAILED, 14},
	}
	for _, tc := range cases {
		if got := toAuditEventKind(tc.kind); got != tc.wire {
			t.Errorf("%s maps to %s, want %s", tc.kind, got, tc.wire)
		}
		if got := int32(tc.wire); got != tc.number {
			t.Errorf("%s has protocol number %d, want %d", tc.wire, got, tc.number)
		}
	}
}

// TestToAuditEventSecretChange checks that a secret key change is audited
// without either value.
func TestToAuditEventSecretChange(t *testing.T) {
	got := toAuditEvent(config.Event{
		Event: config.EventConfigFileChanged,
		Key:   "pairing.static_key",
	})
	if got.Event != localpb.AuditEventKind_AUDIT_EVENT_KIND_CONFIG_FILE_CHANGED {
		t.Errorf("event = %s, want CONFIG_FILE_CHANGED", got.Event)
	}
	if got.Key != "pairing.static_key" {
		t.Errorf("key = %q, want %q", got.Key, "pairing.static_key")
	}
	if got.OldValue != nil {
		t.Errorf("old_value = %v, want nil", got.OldValue)
	}
	if got.NewValue != nil {
		t.Errorf("new_value = %v, want nil", got.NewValue)
	}
}

// TestToAuditEventPeerAndCount checks that the peer reference, count and time
// survive the conversion.
func TestToAuditEventPeerAndCount(t *testing.T) {
	var fingerprint [32]byte
	for i := range fingerprint {
		fingerprint[i] = byte(i)
	}
	got := toAuditEvent(config.Event{
		Time:     time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC),
		Event:    config.EventPairingSucceeded,
		Peer:     &config.PeerRef{Fingerprint: fingerprint, Alias: "peer-a"},
		Count:    3,
		MergeKey: "fp:x",
	})
	if got.Peer == nil {
		t.Fatal("peer is nil, want a peer reference")
	}
	if len(got.Peer.GetFingerprint()) != 32 {
		t.Errorf("fingerprint length = %d, want 32", len(got.Peer.GetFingerprint()))
	}
	for i, b := range got.Peer.GetFingerprint() {
		if b != fingerprint[i] {
			t.Errorf("fingerprint[%d] = %d, want %d", i, b, fingerprint[i])
		}
	}
	if got.Peer.GetAlias() != "peer-a" {
		t.Errorf("alias = %q, want %q", got.Peer.GetAlias(), "peer-a")
	}
	if got.Count != 3 {
		t.Errorf("count = %d, want 3", got.Count)
	}
	if !got.Time.AsTime().Equal(time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)) {
		t.Errorf("time = %v, want 2026-11-18T12:00:00Z", got.Time.AsTime())
	}
}

// TestToAuditEventCopiesOldAndNewValues checks that the old and new values are
// converted into separate wire values.
func TestToAuditEventCopiesOldAndNewValues(t *testing.T) {
	old := config.Value{Kind: config.KindString, String: "old.example:17443"}
	newValue := config.Value{Kind: config.KindInt, Int: 17444}
	got := toAuditEvent(config.Event{
		Event: config.EventConfigSet,
		Key:   "broker.listen",
		Old:   &old,
		New:   &newValue,
	})

	oldOut := got.GetOldValue()
	if oldOut == nil {
		t.Fatal("old_value is nil, want the old value")
	}
	if gotOld := oldOut.GetStringValue(); gotOld != old.String {
		t.Errorf("old_value = %q, want %q", gotOld, old.String)
	}

	newOut := got.GetNewValue()
	if newOut == nil {
		t.Fatal("new_value is nil, want the new value")
	}
	if gotNew := newOut.GetIntValue(); gotNew != newValue.Int {
		t.Errorf("new_value = %d, want %d", gotNew, newValue.Int)
	}
}

// TestToAuditEventCopiesActorAccountDetail checks that actor, account and
// detail survive the conversion.
func TestToAuditEventCopiesActorAccountDetail(t *testing.T) {
	got := toAuditEvent(config.Event{
		Event:   config.EventConfigSet,
		Actor:   "user:alice",
		Account: "account-alice",
		Detail:  "changed broker.listen",
	})
	if got.Actor != "user:alice" {
		t.Errorf("actor = %q, want %q", got.Actor, "user:alice")
	}
	if got.Account != "account-alice" {
		t.Errorf("account = %q, want %q", got.Account, "account-alice")
	}
	if got.Detail != "changed broker.listen" {
		t.Errorf("detail = %q, want %q", got.Detail, "changed broker.listen")
	}
}

// TestConfigErrorCode checks that each configuration sentinel maps to the
// expected wire error code, wrapped or not.
func TestConfigErrorCode(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want localpb.ErrorCode
	}{
		{"unknown key", config.ErrUnknownKey, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"secret key", config.ErrSecretKey, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"listen overridden", config.ErrListenOverridden, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"invalid value", config.ErrInvalidValue, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"invalid combination", config.ErrInvalidCombination, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"too many addresses", config.ErrTooManyAddresses, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"file write", config.ErrFileWrite, localpb.ErrorCode_ERROR_CODE_INTERNAL},
		{"not loaded", config.ErrNotLoaded, localpb.ErrorCode_ERROR_CODE_INTERNAL},
		{"already loaded", config.ErrAlreadyLoaded, localpb.ErrorCode_ERROR_CODE_INTERNAL},
		{"wrapped unknown key", fmt.Errorf("lookup: %w", config.ErrUnknownKey), localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"wrapped secret key", fmt.Errorf("lookup: %w", config.ErrSecretKey), localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"wrapped listen overridden", fmt.Errorf("lookup: %w", config.ErrListenOverridden), localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"wrapped invalid value", fmt.Errorf("lookup: %w", config.ErrInvalidValue), localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"wrapped invalid combination", fmt.Errorf("lookup: %w", config.ErrInvalidCombination), localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"wrapped too many addresses", fmt.Errorf("lookup: %w", config.ErrTooManyAddresses), localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT},
		{"wrapped file write", fmt.Errorf("lookup: %w", config.ErrFileWrite), localpb.ErrorCode_ERROR_CODE_INTERNAL},
		{"wrapped not loaded", fmt.Errorf("lookup: %w", config.ErrNotLoaded), localpb.ErrorCode_ERROR_CODE_INTERNAL},
		{"wrapped already loaded", fmt.Errorf("lookup: %w", config.ErrAlreadyLoaded), localpb.ErrorCode_ERROR_CODE_INTERNAL},
		{"other", errors.New("boom"), localpb.ErrorCode_ERROR_CODE_INTERNAL},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := configErrorCode(tt.err); got != tt.want {
				t.Errorf("configErrorCode(%v) = %s, want %s", tt.err, got, tt.want)
			}
		})
	}
}
