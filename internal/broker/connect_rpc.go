package broker

import (
	"context"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// Runtime log event names for the peer management calls.
const (
	EvPeerPaired    = "peer_paired"
	EvPairingFailed = "pairing_failed"
	EvPeerUnpaired  = "peer_unpaired"
)

// Connect pairs with the Broker at req.Address using a one-time pairing key.
// The response carries the peer's effective alias, fingerprint, the host name
// from the address and any directory fingerprint warnings. A failure returns
// the local interface error and, once dialing has started, writes one audit
// record.
func (b *Broker) Connect(ctx context.Context, req *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
	actor, err := b.auditActor(ctx, req.GetAgentId())
	if err != nil {
		return nil, toStatus(err)
	}
	if b.deps.Pairing == nil || b.deps.PeerLinks == nil {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"peer management is not available in this Broker"))
	}

	address := req.GetAddress()
	peer, shownHost, err := b.deps.Pairing.Connect(ctx, address, req.GetKey(), req.GetLocalAlias())
	if ev, ok := connectEvent(actor, address, peer, err); ok {
		b.deps.Config.Audit(ev)
	}
	if err != nil {
		e := connectError(address, req.GetLocalAlias(), err)
		code := "canceled"
		if e != nil {
			code = errorCodeName(e.Code)
		}
		b.log.Warn(EvPairingFailed, "code", code, "address", address)
		if e != nil {
			return nil, toStatus(e)
		}
		return nil, toStatus(err)
	}

	// Kick forces an immediate redial if the handshake connection dropped; it
	// does nothing on an established link or on the acceptor.
	b.deps.PeerLinks.Kick(peer.Fingerprint)

	alias := effectivePeerAlias(peer)
	b.log.Info(EvPeerPaired, "fingerprint", peer.Fingerprint.String(), "alias", alias, "role", "dialer")

	resp := &localpb.ConnectResponse{
		Alias:       alias,
		Fingerprint: append([]byte(nil), peer.Fingerprint[:]...),
		ShownHost:   shownHost,
	}
	if b.deps.Warner != nil {
		resp.Warnings = b.deps.Warner.ConnectWarnings(peer)
	}
	return resp, nil
}
