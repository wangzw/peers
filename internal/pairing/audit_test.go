package pairing

import (
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// auditTestSettings records the events pairing hands to the audit log.
type auditTestSettings struct {
	events []config.Event
}

func (s *auditTestSettings) DataDir() (string, error) { return "/tmp/peers-test", nil }
func (s *auditTestSettings) Alias() (string, error)   { return "hostA", nil }
func (s *auditTestSettings) Hostname() (string, error) {
	return "hostA", nil
}
func (s *auditTestSettings) Advertised() ([]config.AdvertisedAddr, error) { return nil, nil }
func (s *auditTestSettings) ListenAddress() (string, error)               { return ":17443", nil }
func (s *auditTestSettings) StaticKey() string                            { return "" }
func (s *auditTestSettings) MaxFailures() (int, error)                    { return 5, nil }
func (s *auditTestSettings) MaxAttemptsPerMinute() (int, error)           { return 10, nil }
func (s *auditTestSettings) Audit(e config.Event)                         { s.events = append(s.events, e) }

func auditTestFingerprint(fill byte) Fingerprint {
	var fp Fingerprint
	for i := range fp {
		fp[i] = fill
	}
	return fp
}

// TestAuditPairingSucceeded covers the acceptor's pairing_succeeded event:
// pairing leaves Actor and Account empty, stamps the injected time, and names
// the peer by fingerprint and effective alias.
func TestAuditPairingSucceeded(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 0, 0, 123456789, time.UTC)
	fp := auditTestFingerprint(0xab)
	s := &auditTestSettings{}

	auditPairingSucceeded(s, now, fp, "hostB-1")

	if len(s.events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(s.events))
	}
	e := s.events[0]
	if e.Event != config.EventPairingSucceeded {
		t.Errorf("Event = %q, want %q", e.Event, config.EventPairingSucceeded)
	}
	if !e.Time.Equal(now) {
		t.Errorf("Time = %v, want the injected %v", e.Time, now)
	}
	if e.Actor != "" || e.Account != "" {
		t.Errorf("Actor/Account = %q/%q, want empty", e.Actor, e.Account)
	}
	if e.Detail != "" || e.MergeKey != "" {
		t.Errorf("Detail/MergeKey = %q/%q, want empty", e.Detail, e.MergeKey)
	}
	if e.Peer == nil {
		t.Fatal("Peer = nil, want the peer reference")
	}
	if e.Peer.Fingerprint != [32]byte(fp) {
		t.Errorf("Peer.Fingerprint = %x, want %x", e.Peer.Fingerprint, fp)
	}
	if e.Peer.Alias != "hostB-1" {
		t.Errorf("Peer.Alias = %q, want hostB-1", e.Peer.Alias)
	}
}

// TestAuditPairingFailedCarriesReasonAndMergeKey covers a rate-limited failure:
// the reason goes in Detail and the source-based key in MergeKey so config can
// merge repeated attempts from one host.
func TestAuditPairingFailedCarriesReasonAndMergeKey(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 1, 0, 0, time.UTC)
	fp := auditTestFingerprint(0x01)
	s := &auditTestSettings{}

	auditPairingFailed(s, now, fp, "hostB",
		auditReasonTryLater, auditSourceMergeKey("192.0.2.10:17443"))

	if len(s.events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(s.events))
	}
	e := s.events[0]
	if e.Event != config.EventPairingFailed {
		t.Errorf("Event = %q, want %q", e.Event, config.EventPairingFailed)
	}
	if e.Detail != auditReasonTryLater {
		t.Errorf("Detail = %q, want %q", e.Detail, auditReasonTryLater)
	}
	if e.MergeKey != "source:192.0.2.10/32" {
		t.Errorf("MergeKey = %q, want source:192.0.2.10/32", e.MergeKey)
	}
	if e.Peer == nil || e.Peer.Alias != "hostB" {
		t.Errorf("Peer = %+v, want fingerprint with alias hostB", e.Peer)
	}
}

// TestAuditPairingFailedWithoutMergeKey covers rejections that must not merge.
func TestAuditPairingFailedWithoutMergeKey(t *testing.T) {
	s := &auditTestSettings{}
	fp := auditTestFingerprint(0x02)

	auditPairingFailed(s, time.Now(), fp, "", auditReasonKeyInvalid, "")

	if len(s.events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(s.events))
	}
	if s.events[0].MergeKey != "" {
		t.Errorf("MergeKey = %q, want empty", s.events[0].MergeKey)
	}
	if s.events[0].Peer == nil {
		t.Error("Peer = nil, want the fingerprint even when the alias is unknown")
	}
}

// TestAuditLocalAliasAssigned covers the event written when pairing resolves an
// alias collision by generating a local alias.
func TestAuditLocalAliasAssigned(t *testing.T) {
	now := time.Date(2026, 11, 18, 12, 2, 0, 0, time.UTC)
	s := &auditTestSettings{}
	fp := auditTestFingerprint(0x03)

	auditLocalAliasAssigned(s, now, fp, "hostB-03ab")

	if len(s.events) != 1 {
		t.Fatalf("recorded %d events, want 1", len(s.events))
	}
	e := s.events[0]
	if e.Event != config.EventLocalAliasAssigned {
		t.Errorf("Event = %q, want %q", e.Event, config.EventLocalAliasAssigned)
	}
	if e.Peer == nil || e.Peer.Alias != "hostB-03ab" {
		t.Errorf("Peer = %+v, want alias hostB-03ab", e.Peer)
	}
	if e.Actor != "" || e.Account != "" {
		t.Errorf("Actor/Account = %q/%q, want empty", e.Actor, e.Account)
	}
}

// TestAuditEventZeroFingerprintOmitsPeer keeps a zero fingerprint out of the
// record rather than emitting an all-zero peer.
func TestAuditEventZeroFingerprintOmitsPeer(t *testing.T) {
	e := newPairingAuditEvent(config.EventPairingFailed, time.Now(), Fingerprint{}, "", "", "")
	if e.Peer != nil {
		t.Errorf("Peer = %+v, want nil for a zero fingerprint", e.Peer)
	}
}

// TestAuditReasonVocabulary pins the Detail strings. The audit log records the
// reason only, never a key or address, so these values are part of the log
// contract.
func TestAuditReasonVocabulary(t *testing.T) {
	cases := []struct {
		got  string
		want string
	}{
		{auditReasonKeyInvalid, "key invalid"},
		{auditReasonVerificationFailed, "verification failed"},
		{auditReasonTryLater, "try later"},
		{auditReasonTimeout, "handshake timeout"},
		{auditReasonNotConnected, "not connected"},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("audit reason = %q, want %q", c.got, c.want)
		}
	}
}

// TestAuditSourceMergeKey covers the source-based merge key: the IPv4 /32,
// the IPv6 /64 without brackets, and no key for an address with no IP.
func TestAuditSourceMergeKey(t *testing.T) {
	tests := []struct {
		addr string
		want string
	}{
		{"192.0.2.10:17443", "source:192.0.2.10/32"},
		{"[2001:db8::1]:17443", "source:2001:db8::/64"},
		{"[2001:db8:1:2::abcd]:17443", "source:2001:db8:1:2::/64"},
		{"[::ffff:203.0.113.7]:443", "source:203.0.113.7/32"},
		{"peer.example:17443", ""},
		{"not-an-address", ""},
		{"", ""},
	}
	for _, tt := range tests {
		if got := auditSourceMergeKey(tt.addr); got != tt.want {
			t.Errorf("auditSourceMergeKey(%q) = %q, want %q", tt.addr, got, tt.want)
		}
	}
}

// TestAuditSourceMergeKeyIgnoresPort makes the merge group stable across the
// ephemeral ports one source uses.
func TestAuditSourceMergeKeyIgnoresPort(t *testing.T) {
	a := auditSourceMergeKey("192.0.2.10:1000")
	b := auditSourceMergeKey("192.0.2.10:2000")
	if a == "" || a != b {
		t.Fatalf("merge keys %q and %q, want the same non-empty key", a, b)
	}
	if c := auditSourceMergeKey("192.0.2.11:1000"); c == a {
		t.Fatalf("different hosts share merge key %q", c)
	}
}
