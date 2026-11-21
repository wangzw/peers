package broker

import (
	"context"
	"fmt"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// peerLinkAdapter adapts the real link to the interfaces the broker's peer
// management uses: PeerLinks for the local RPCs and transportTarget for the
// transport that pairing reaches link through. The dial, drop and kick calls
// are forwarded as they are, so pairing and Disconnect see exactly the link's
// own behaviour.
type peerLinkAdapter struct{ l *link.Link }

// PairDial forwards to link; pairing starts the pairing handshake over the same
// connection when the callback returns.
func (a peerLinkAdapter) PairDial(ctx context.Context, address string, run func(pairing.PairConn) error) error {
	return a.l.PairDial(ctx, address, run)
}

// Drop forwards to link, which leaves the peer in the peer table untouched.
func (a peerLinkAdapter) Drop(fp pairing.Fingerprint) {
	a.l.Drop(fp)
}

// Kick forwards to link, which redials the peer at once when it is in backoff.
func (a peerLinkAdapter) Kick(fp pairing.Fingerprint) {
	a.l.Kick(fp)
}

// Status reports the link state of fp in the broker's own vocabulary.
func (a peerLinkAdapter) Status(fp pairing.Fingerprint) PeerLinkStatus {
	return peerLinkStatus(a.l.State(fp))
}

// mapLinkPhase translates a link phase. A value the broker does not know means
// there is no record to report.
func mapLinkPhase(ph link.Phase) LinkPhase {
	switch ph {
	case link.PhaseDialing:
		return LinkDialing
	case link.PhaseWaiting:
		return LinkWaiting
	case link.PhaseHandshaking:
		return LinkHandshaking
	case link.PhaseEstablished:
		return LinkEstablished
	case link.PhaseBackoff:
		return LinkBackoff
	case link.PhaseRejected:
		return LinkRejected
	default:
		return LinkNone
	}
}

// mapLinkFailure translates a link failure into the ones the status detail
// explains. Every other failure, and an unknown value, needs no explanation.
func mapLinkFailure(r link.FailReason) LinkFailure {
	switch r {
	case link.FailNotAPeer:
		return FailureNotAPeer
	case link.FailRoleMismatch:
		return FailureRoleMismatch
	case link.FailVersionIncompatible:
		return FailureVersionIncompatible
	case link.FailInboundLimit:
		return FailureInboundLimit
	default:
		return FailureNone
	}
}

// peerLinkStatus converts a link state. The detail carries the peer's protocol
// version only when the version is what made the link fail and the peer's
// version is known; reporting "version 0" would name a version the peer never
// spoke.
func peerLinkStatus(st link.LinkState) PeerLinkStatus {
	out := PeerLinkStatus{
		Phase:   mapLinkPhase(st.Phase),
		Failure: mapLinkFailure(st.LastFailure),
	}
	if out.Failure == FailureVersionIncompatible && st.PeerVersion != 0 {
		out.Detail = fmt.Sprintf("peer speaks link protocol version %d", st.PeerVersion)
	}
	return out
}

var (
	_ PeerLinks         = peerLinkAdapter{}
	_ transportTarget   = peerLinkAdapter{}
	_ LinkRunner        = (*link.Link)(nil)
	_ pairing.Transport = (*link.Link)(nil)
)
