package broker

import (
	"context"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

func newKeyPairing(key string, expires time.Time, shown []string, err error) *fakePairing {
	return &fakePairing{
		keyFn: func() (string, time.Time, []string, error) {
			return key, expires, shown, err
		},
	}
}

func pairkeyCalls(p *fakePairing) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func callCreatePairingKey(t *testing.T, tb *testBroker, req *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	return tb.client.CreatePairingKey(ctx, req)
}

// TestCreatePairingKeyReturnsKeyAndShown checks that the key, the expiry and
// every shown address come back in pairing's order.
func TestCreatePairingKeyReturnsKeyAndShown(t *testing.T) {
	t0 := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	shown := []string{"box(192.0.2.10:17443)", "box(10.0.0.5:17443)"}
	p := newKeyPairing("K7QF-2M9X-HT4C-PWZ3", t0.Add(10*time.Minute), shown, nil)
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	resp, err := callCreatePairingKey(t, tb, &localpb.CreatePairingKeyRequest{})
	if err != nil {
		t.Fatalf("CreatePairingKey: %v", err)
	}
	if got, want := resp.GetKey(), "K7QF-2M9X-HT4C-PWZ3"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
	if got, want := resp.GetExpiresAt().AsTime(), t0.Add(10*time.Minute); !got.Equal(want) {
		t.Fatalf("ExpiresAt = %v, want %v", got, want)
	}
	if got := resp.GetShownAddresses(); len(got) != len(shown) {
		t.Fatalf("ShownAddresses = %v, want %v", got, shown)
	} else {
		for i := range shown {
			if got[i] != shown[i] {
				t.Fatalf("ShownAddresses[%d] = %q, want %q (order must not change)", i, got[i], shown[i])
			}
		}
	}
}

// TestCreatePairingKeyEmptyShownStillSucceeds checks that a Broker with no
// address to show still returns a key and an expiry.
func TestCreatePairingKeyEmptyShownStillSucceeds(t *testing.T) {
	p := newKeyPairing("KEY", time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC), nil, nil)
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	resp, err := callCreatePairingKey(t, tb, &localpb.CreatePairingKeyRequest{})
	if err != nil {
		t.Fatalf("CreatePairingKey: %v", err)
	}
	if got := resp.GetShownAddresses(); len(got) != 0 {
		t.Fatalf("ShownAddresses = %v, want empty", got)
	}
	if resp.GetExpiresAt() == nil {
		t.Fatal("ExpiresAt is nil, want the expiry")
	}
}

// TestCreatePairingKeyNotListening checks that a Broker that does not listen
// returns invalid_argument and writes no audit.
func TestCreatePairingKeyNotListening(t *testing.T) {
	p := newKeyPairing("", time.Time{}, nil, pairing.ErrNotListening)
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	_, err := callCreatePairingKey(t, tb, &localpb.CreatePairingKeyRequest{})
	detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT)
	if !strings.Contains(detail.GetMessage(), "is not listening") {
		t.Fatalf("message = %q, want it to say the Broker does not listen", detail.GetMessage())
	}
	if strings.Contains(detail.GetMessage(), "connect_broker") {
		t.Fatalf("message = %q, want no MCP tool name", detail.GetMessage())
	}
	if evs := auditsOf(tb.cfg, config.EventPairingKeyCreated); len(evs) != 0 {
		t.Fatalf("audit events = %d, want none for a failed key creation", len(evs))
	}
}

// TestCreatePairingKeyOnlyOneTimeKey checks that the handler asks pairing for
// exactly one one-time key.
func TestCreatePairingKeyOnlyOneTimeKey(t *testing.T) {
	p := newKeyPairing("K7QF-2M9X-HT4C-PWZ3", time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC), nil, nil)
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	resp, err := callCreatePairingKey(t, tb, &localpb.CreatePairingKeyRequest{})
	if err != nil {
		t.Fatalf("CreatePairingKey: %v", err)
	}
	if got, want := resp.GetKey(), "K7QF-2M9X-HT4C-PWZ3"; got != want {
		t.Fatalf("Key = %q, want %q", got, want)
	}
	calls := pairkeyCalls(p)
	if len(calls) != 1 || calls[0] != "CreateOneTimeKey" {
		t.Fatalf("pairing calls = %v, want exactly [CreateOneTimeKey]", calls)
	}
}

// TestCreatePairingKeyAudited checks that one pairing_key_created record names
// the agent as actor and does not carry the key.
func TestCreatePairingKeyAudited(t *testing.T) {
	const key = "K7QF-2M9X-HT4C-PWZ3"
	expires := time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC)
	p := newKeyPairing(key, expires, nil, nil)
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))
	id, address := auditRegister(t, tb, "api-worker")

	if _, err := callCreatePairingKey(t, tb, &localpb.CreatePairingKeyRequest{AgentId: id}); err != nil {
		t.Fatalf("CreatePairingKey: %v", err)
	}

	evs := auditsOf(tb.cfg, config.EventPairingKeyCreated)
	if len(evs) != 1 {
		t.Fatalf("pairing_key_created events = %d, want 1", len(evs))
	}
	ev := evs[0]
	if ev.Actor != address {
		t.Fatalf("Actor = %q, want the agent address %q", ev.Actor, address)
	}
	if want := usernameFor(uint32(os.Geteuid())); ev.Account != want {
		t.Fatalf("Account = %q, want %q", ev.Account, want)
	}
	if strings.Contains(ev.Detail, key) {
		t.Fatalf("Detail = %q carries the pairing key", ev.Detail)
	}
	if want := "expires 2026-11-18T10:00:00Z"; ev.Detail != want {
		t.Fatalf("Detail = %q, want %q", ev.Detail, want)
	}
	if !ev.Time.IsZero() {
		t.Fatalf("Time = %v, want the zero time (config fills it)", ev.Time)
	}
}

// TestCreatePairingKeyBadAgentNoKey checks that an unknown agent id is refused
// before any key is created.
func TestCreatePairingKeyBadAgentNoKey(t *testing.T) {
	p := newKeyPairing("KEY", time.Date(2026, 11, 18, 10, 0, 0, 0, time.UTC), nil, nil)
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	_, err := callCreatePairingKey(t, tb, &localpb.CreatePairingKeyRequest{AgentId: "nope"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED)
	if calls := pairkeyCalls(p); len(calls) != 0 {
		t.Fatalf("pairing calls = %v, want none before the actor is resolved", calls)
	}
}

// TestCreatePairingKeyWithoutPairing checks that a missing pairing returns
// internal instead of panicking.
func TestCreatePairingKeyWithoutPairing(t *testing.T) {
	tb := newTestBroker(t)

	_, err := callCreatePairingKey(t, tb, &localpb.CreatePairingKeyRequest{})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INTERNAL)
}
