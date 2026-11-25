package directory

import (
	"testing"
	"time"

	"github.com/wangzw/peers/internal/pairing"
)

var _ PeerTable = (*pairing.Pairing)(nil)

// TestBrokerStatusString checks the String form of each BrokerStatus, including
// unknown values.
func TestBrokerStatusString(t *testing.T) {
	cases := []struct {
		status BrokerStatus
		want   string
	}{
		{StatusConnected, "connected"},
		{StatusConnectable, "connectable"},
		{StatusUnreachable, "not_directly_reachable"},
		{BrokerStatus(0), "unknown"},
		{BrokerStatus(200), "unknown"},
	}
	for _, tc := range cases {
		if got := tc.status.String(); got != tc.want {
			t.Errorf("BrokerStatus(%d).String() = %q, want %q", uint8(tc.status), got, tc.want)
		}
	}
}

// TestPairingSatisfiesPeerTable checks that *pairing.Pairing satisfies
// PeerTable and that its peer fields are usable.
func TestPairingSatisfiesPeerTable(t *testing.T) {
	var table PeerTable = (*pairing.Pairing)(nil)
	_ = table

	fp := Fingerprint{1}
	peers := []pairing.Peer{{
		Fingerprint:   fp,
		PublicKey:     []byte{7},
		Alias:         "peer-alias",
		LocalAlias:    "local-alias",
		Role:          pairing.RoleDialer,
		PairedAt:      time.Unix(0, 0),
		Candidates:    []pairing.Candidate{{Source: pairing.SourceAcceptor, Addr: "host:1"}},
		LastConnected: "2026-11-18",
	}}
	if peers[0].Fingerprint != fp {
		t.Fatalf("pairing.Peer.Fingerprint = %v, want %v", peers[0].Fingerprint, fp)
	}
	if peers[0].Role != pairing.RoleDialer {
		t.Fatalf("pairing.Peer.Role = %v, want RoleDialer", peers[0].Role)
	}
	if got := peers[0].Candidates[0].Source; got != pairing.SourceAcceptor {
		t.Fatalf("pairing.Candidate.Source = %v, want SourceAcceptor", got)
	}
}

// TestRealClockUsesNow checks that realClock uses its now function when set and
// the wall clock otherwise, and that NewTimer fires.
func TestRealClockUsesNow(t *testing.T) {
	fixed := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
	c := realClock{now: func() time.Time { return fixed }}
	if got := c.Now(); !got.Equal(fixed) {
		t.Fatalf("realClock{now: ...}.Now() = %v, want %v", got, fixed)
	}

	before := time.Now()
	now := realClock{}.Now()
	after := time.Now()
	if now.Before(before) || now.After(after) {
		t.Fatalf("realClock{}.Now() = %v is outside the window [%v, %v] measured around the call", now, before, after)
	}

	ch := realClock{}.NewTimer(time.Millisecond).C()
	select {
	case <-ch:
	case <-time.After(eventTimeout):
		t.Fatal("timer did not fire before the event timeout")
	}
}
