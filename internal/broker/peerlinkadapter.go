package broker

import (
	"context"
	"fmt"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

type peerLinkAdapter struct{ l *link.Link }

func (a peerLinkAdapter) PairDial(ctx context.Context, address string, run func(pairing.PairConn) error) error {
	return a.l.PairDial(ctx, address, run)
}

func (a peerLinkAdapter) Drop(fp pairing.Fingerprint) {
	a.l.Drop(fp)
}

func (a peerLinkAdapter) Kick(fp pairing.Fingerprint) {
	a.l.Kick(fp)
}

func (a peerLinkAdapter) Status(fp pairing.Fingerprint) PeerLinkStatus {
	return peerLinkStatus(a.l.State(fp))
}

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

// peerLinkStatus converts a link state. The detail carries the peer's version
// only when an incompatible version failed the link and that version is known:
// reporting version 0 would name a version the peer never spoke.
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
