package broker

import (
	"errors"
	"reflect"
	"testing"
	"time"

	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

func errorDetailOf(t *testing.T, err error) *localpb.ErrorDetail {
	t.Helper()
	if err == nil {
		t.Fatal("errorDetailOf: nil error")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("errorDetailOf: %v is not a gRPC status error", err)
	}
	var found []*localpb.ErrorDetail
	for _, detail := range st.Details() {
		if d, ok := detail.(*localpb.ErrorDetail); ok {
			found = append(found, d)
		}
	}
	if len(found) != 1 {
		t.Fatalf("errorDetailOf: %v carries %d *localpb.ErrorDetail values, want exactly 1", err, len(found))
	}
	return found[0]
}

// TestFakeConfigSecretKey checks that the fake reports a secret key as set
// without returning its value.
func TestFakeConfigSecretKey(t *testing.T) {
	f := newFakeConfig(t.TempDir())
	f.secretKeys["pairing.static_key"] = true
	f.values["pairing.static_key"] = config.Value{Kind: config.KindSecret, String: "K1"}

	v, err := f.Get("pairing.static_key")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if v.Kind != config.KindSecret {
		t.Errorf("Kind = %v, want KindSecret", v.Kind)
	}
	if !v.SecretIsSet {
		t.Error("SecretIsSet = false, want true")
	}
	if v.String != "" {
		t.Errorf("String = %q, want the secret value hidden", v.String)
	}
}

// TestFakeConfigSetReturnsOld checks that the fake records the Set call and
// returns the previous value.
func TestFakeConfigSetReturnsOld(t *testing.T) {
	f := newFakeConfig(t.TempDir())
	actor := config.Actor{Account: "alice", Agent: "w@hostA"}
	newValue := config.Value{Kind: config.KindString, String: "hostB"}

	old, err := f.Set(actor, "broker.alias", newValue)
	if err != nil {
		t.Fatalf("Set: %v", err)
	}
	if old.String != "hostA" {
		t.Errorf("old value = %q, want %q", old.String, "hostA")
	}
	if len(f.setCalls) == 0 {
		t.Fatal("Set recorded no call")
	}
	last := f.setCalls[len(f.setCalls)-1]
	if last.Actor != actor {
		t.Errorf("recorded actor = %+v, want %+v", last.Actor, actor)
	}
	if last.Key != "broker.alias" {
		t.Errorf("recorded key = %q, want %q", last.Key, "broker.alias")
	}
	if !reflect.DeepEqual(last.Value, newValue) {
		t.Errorf("recorded value = %+v, want %+v", last.Value, newValue)
	}

	stored, err := f.Get("broker.alias")
	if err != nil {
		t.Fatalf("Get after Set: %v", err)
	}
	if stored.String != "hostB" {
		t.Errorf("stored value = %q, want %q", stored.String, "hostB")
	}
}

// TestFakeConfigSetErrShortCircuits checks that a failed Set is returned
// without running subscribers or changing the value.
func TestFakeConfigSetErrShortCircuits(t *testing.T) {
	f := newFakeConfig(t.TempDir())
	wantErr := errors.New("injected set failure")
	f.setErr = wantErr
	called := 0
	f.Subscribe("broker.alias", func(old, new config.Value) { called++ })

	_, err := f.Set(config.Actor{Account: "alice"}, "broker.alias",
		config.Value{Kind: config.KindString, String: "hostB"})
	if !errors.Is(err, wantErr) {
		t.Fatalf("Set error = %v, want %v", err, wantErr)
	}
	if called != 0 {
		t.Errorf("subscriber ran %d times, want 0", called)
	}
	stored, err := f.Get("broker.alias")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.String != "hostA" {
		t.Errorf("value after failed Set = %q, want %q", stored.String, "hostA")
	}
}

// TestFakeConfigReadAuditSince checks that ReadAudit filters by time and
// returns an injected read error.
func TestFakeConfigReadAuditSince(t *testing.T) {
	f := newFakeConfig(t.TempDir())
	base := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	for i := 0; i < 3; i++ {
		f.Audit(config.Event{Time: base.Add(time.Duration(i) * time.Minute), Event: config.EventConfigSet})
	}

	events, err := f.ReadAudit(base.Add(time.Minute))
	if err != nil {
		t.Fatalf("ReadAudit: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("ReadAudit returned %d events, want 2", len(events))
	}
	if !events[0].Time.Equal(base.Add(time.Minute)) || !events[1].Time.Equal(base.Add(2*time.Minute)) {
		t.Errorf("ReadAudit times = %v, %v, want the second and third events", events[0].Time, events[1].Time)
	}

	wantErr := errors.New("injected read failure")
	f.readAuditErr = wantErr
	events, err = f.ReadAudit(base)
	if !errors.Is(err, wantErr) {
		t.Fatalf("ReadAudit error = %v, want %v", err, wantErr)
	}
	if events != nil {
		t.Errorf("ReadAudit returned %d events with an error, want none", len(events))
	}
}

// TestFakeConfigFireHelpers checks that the fire helpers deliver old and new
// values to subscribers without changing stored values.
func TestFakeConfigFireHelpers(t *testing.T) {
	f := newFakeConfig(t.TempDir())

	var keyCalls [][2]config.Value
	f.Subscribe("broker.alias", func(old, new config.Value) {
		keyCalls = append(keyCalls, [2]config.Value{old, new})
	})
	var advertisedCalls [][2][]config.AdvertisedAddr
	f.SubscribeAdvertised(func(old, new []config.AdvertisedAddr) {
		advertisedCalls = append(advertisedCalls, [2][]config.AdvertisedAddr{old, new})
	})

	oldValue := config.Value{Kind: config.KindString, String: "old"}
	newValue := config.Value{Kind: config.KindString, String: "new"}
	f.fireKey("broker.alias", oldValue, newValue)

	oldAddrs := []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.1:7000"}}
	newAddrs := []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "10.0.0.2:7000"}}
	f.fireAdvertised(oldAddrs, newAddrs)

	if len(keyCalls) != 1 {
		t.Fatalf("key callback ran %d times, want 1", len(keyCalls))
	}
	if !reflect.DeepEqual(keyCalls[0][0], oldValue) || !reflect.DeepEqual(keyCalls[0][1], newValue) {
		t.Errorf("key callback saw %+v, want %+v then %+v", keyCalls[0], oldValue, newValue)
	}
	if len(advertisedCalls) != 1 {
		t.Fatalf("advertised callback ran %d times, want 1", len(advertisedCalls))
	}
	if !reflect.DeepEqual(advertisedCalls[0][0], oldAddrs) || !reflect.DeepEqual(advertisedCalls[0][1], newAddrs) {
		t.Errorf("advertised callback saw %+v, want %+v then %+v", advertisedCalls[0], oldAddrs, newAddrs)
	}
	if !reflect.DeepEqual(f.subscribedKeys, []string{"broker.alias"}) {
		t.Errorf("subscribedKeys = %v, want [broker.alias]", f.subscribedKeys)
	}

	stored, err := f.Get("broker.alias")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if stored.String != "hostA" {
		t.Errorf("stored value after fireKey = %q, want %q", stored.String, "hostA")
	}
}
