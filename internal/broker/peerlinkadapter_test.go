package broker

import (
	"testing"

	"github.com/wangzw/peers/internal/link"
)

// TestMapLinkPhase checks the mapping from every link phase, including an
// unknown value.
func TestMapLinkPhase(t *testing.T) {
	tests := []struct {
		name string
		in   link.Phase
		want LinkPhase
	}{
		{"none", link.PhaseNone, LinkNone},
		{"dialing", link.PhaseDialing, LinkDialing},
		{"waiting", link.PhaseWaiting, LinkWaiting},
		{"handshaking", link.PhaseHandshaking, LinkHandshaking},
		{"established", link.PhaseEstablished, LinkEstablished},
		{"backoff", link.PhaseBackoff, LinkBackoff},
		{"rejected", link.PhaseRejected, LinkRejected},
		{"unknown", link.Phase(200), LinkNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapLinkPhase(tt.in); got != tt.want {
				t.Fatalf("mapLinkPhase(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestMapLinkFailure checks the mapping from every link failure, with unknown and
// unhandled reasons mapping to none.
func TestMapLinkFailure(t *testing.T) {
	tests := []struct {
		name string
		in   link.FailReason
		want LinkFailure
	}{
		{"none", link.FailNone, FailureNone},
		{"unreachable", link.FailUnreachable, FailureNone},
		{"role mismatch", link.FailRoleMismatch, FailureRoleMismatch},
		{"version incompatible", link.FailVersionIncompatible, FailureVersionIncompatible},
		{"not a peer", link.FailNotAPeer, FailureNotAPeer},
		{"queue full", link.FailQueueFull, FailureNone},
		{"connection lost", link.FailConnectionLost, FailureNone},
		{"protocol", link.FailProtocol, FailureNone},
		{"inbound limit", link.FailInboundLimit, FailureInboundLimit},
		{"unknown", link.FailReason(200), FailureNone},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mapLinkFailure(tt.in); got != tt.want {
				t.Fatalf("mapLinkFailure(%d) = %d, want %d", tt.in, got, tt.want)
			}
		})
	}
}

// TestPeerLinkStatusVersionDetail checks that the detail names the peer's version
// only for an incompatible version.
func TestPeerLinkStatusVersionDetail(t *testing.T) {
	tests := []struct {
		name string
		in   link.LinkState
		want PeerLinkStatus
	}{
		{
			name: "known peer version",
			in:   link.LinkState{Phase: link.PhaseBackoff, LastFailure: link.FailVersionIncompatible, PeerVersion: 2},
			want: PeerLinkStatus{
				Phase:   LinkBackoff,
				Failure: FailureVersionIncompatible,
				Detail:  "peer speaks link protocol version 2",
			},
		},
		{
			name: "unknown peer version",
			in:   link.LinkState{Phase: link.PhaseBackoff, LastFailure: link.FailVersionIncompatible, PeerVersion: 0},
			want: PeerLinkStatus{
				Phase:   LinkBackoff,
				Failure: FailureVersionIncompatible,
			},
		},
		{
			name: "other failure with a peer version",
			in:   link.LinkState{Phase: link.PhaseRejected, LastFailure: link.FailNotAPeer, PeerVersion: 5},
			want: PeerLinkStatus{
				Phase:   LinkRejected,
				Failure: FailureNotAPeer,
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := peerLinkStatus(tt.in); got != tt.want {
				t.Fatalf("peerLinkStatus(%+v) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}
