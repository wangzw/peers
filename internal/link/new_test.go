package link

import (
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

func TestNewRejectsMissingCollaborators(t *testing.T) {
	peers := linktest.NewFakePeers(t)
	settings := linktest.NewFakeSettings("b")

	if l, err := New(Options{Settings: settings}); err == nil || l != nil {
		t.Fatal("New without Peers succeeded, want error and nil Link")
	}
	if l, err := New(Options{Peers: peers}); err == nil || l != nil {
		t.Fatal("New without Settings succeeded, want error and nil Link")
	}

	l, err := New(Options{Peers: peers, Settings: settings})
	if err != nil {
		t.Fatalf("New with both collaborators: %v", err)
	}
	if l == nil {
		t.Fatal("New returned a nil *Link without an error")
	}
}

func TestNewDefaults(t *testing.T) {
	peers := linktest.NewFakePeers(t)
	settings := linktest.NewFakeSettings("b")

	l, err := New(Options{Peers: peers, Settings: settings})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if l.protocolVersion != linkpb.ProtocolVersion {
		t.Errorf("protocolVersion = %d, want %d", l.protocolVersion, linkpb.ProtocolVersion)
	}
	if l.sendQueueSize != 1024 {
		t.Errorf("sendQueueSize = %d, want 1024", l.sendQueueSize)
	}
	if l.firstFrameTimeout != 10*time.Second {
		t.Errorf("firstFrameTimeout = %v, want 10s", l.firstFrameTimeout)
	}
	if l.helloTimeout != 10*time.Second {
		t.Errorf("helloTimeout = %v, want 10s", l.helloTimeout)
	}
	if l.rejectLinger != time.Second {
		t.Errorf("rejectLinger = %v, want 1s", l.rejectLinger)
	}
	if l.maxUnauthenticated != 64 {
		t.Errorf("maxUnauthenticated = %d, want 64", l.maxUnauthenticated)
	}
	if l.maxUnauthenticatedPerSource != 8 {
		t.Errorf("maxUnauthenticatedPerSource = %d, want 8", l.maxUnauthenticatedPerSource)
	}
	if l.enforcementMinTime != 5*time.Second {
		t.Errorf("enforcementMinTime = %v, want 5s", l.enforcementMinTime)
	}
	if l.localNets != nil {
		t.Error("localNets is not nil, want nil in production")
	}
	if l.onDial != nil {
		t.Error("onDial is not nil, want nil in production")
	}
	if l.dialContext != nil {
		t.Error("dialContext is not nil, want nil in production")
	}
	if l.randRead == nil {
		t.Error("randRead is nil, want crypto/rand.Read")
	}
	if l.clock == nil {
		t.Error("clock is nil, want RealClock")
	}
	if l.logger == nil {
		t.Error("logger is nil, want slog.Default")
	}
	if l.rootCtx == nil || l.cancelRoot == nil {
		t.Error("root context or cancel function is nil")
	}
	if l.closed {
		t.Error("closed is true on a fresh Link")
	}
}

func TestNewProtocolVersionFromOptions(t *testing.T) {
	peers := linktest.NewFakePeers(t)
	settings := linktest.NewFakeSettings("b")

	l, err := New(Options{Peers: peers, Settings: settings, ProtocolVersion: 7})
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if l.protocolVersion != 7 {
		t.Fatalf("protocolVersion = %d, want 7", l.protocolVersion)
	}

	SetTestKnobs(l, TestKnobs{SendQueueSize: 8, HelloTimeout: time.Second})
	if l.protocolVersion != 7 {
		t.Errorf("protocolVersion = %d after SetTestKnobs, want 7", l.protocolVersion)
	}
	if l.sendQueueSize != 8 || l.helloTimeout != time.Second {
		t.Errorf("SetTestKnobs did not apply: sendQueueSize = %d, helloTimeout = %v", l.sendQueueSize, l.helloTimeout)
	}
}

func TestPhaseAndFailReasonStrings(t *testing.T) {
	phases := map[Phase]string{
		PhaseNone:        "none",
		PhaseDialing:     "dialing",
		PhaseWaiting:     "waiting",
		PhaseHandshaking: "handshaking",
		PhaseEstablished: "established",
		PhaseBackoff:     "backoff",
		PhaseRejected:    "rejected",
		Phase(200):       "unknown",
	}
	for p, want := range phases {
		if got := p.String(); got != want {
			t.Errorf("Phase(%d).String() = %q, want %q", uint8(p), got, want)
		}
	}

	reasons := map[FailReason]string{
		FailNone:                "none",
		FailUnreachable:         "unreachable",
		FailRoleMismatch:        "role_mismatch",
		FailVersionIncompatible: "version_incompatible",
		FailNotAPeer:            "not_a_peer",
		FailQueueFull:           "queue_full",
		FailConnectionLost:      "connection_lost",
		FailProtocol:            "protocol",
		FailInboundLimit:        "inbound_limit",
		FailReason(200):         "unknown",
	}
	for r, want := range reasons {
		if got := r.String(); got != want {
			t.Errorf("FailReason(%d).String() = %q, want %q", uint8(r), got, want)
		}
	}
}
