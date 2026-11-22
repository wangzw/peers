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
// The address and the local alias go to pairing unchanged: it accepts both
// address forms and decides whether the alias is usable. On success the
// response carries the effective alias, the fingerprint, the host name taken
// from the address and the warnings the directory hook adds. Failures map to
// the local interface codes and, once dialing has started, leave one audit
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
		// The pairing key is part of the request, never of the address or the
		// error code, so it cannot reach this record.
		b.log.Warn(EvPairingFailed, "code", code, "address", address)
		if e != nil {
			return nil, toStatus(e)
		}
		return nil, toStatus(err)
	}

	// The handshake connection normally becomes the link, but if it dropped
	// right after the handshake the dialer would wait out a backoff; Kick makes
	// it redial at once. On an established link or for the acceptor it does
	// nothing.
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
