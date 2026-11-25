package broker

import (
	"context"
	"encoding/hex"
	"strings"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

func resolvePeerRef(p PairingOps, ref string) (pairing.Fingerprint, *Error) {
	if ref == "" {
		return pairing.Fingerprint{}, Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
			"broker is required: give the peer's alias or its 64-hex-digit fingerprint")
	}
	if fp, ok := p.ResolveAlias(ref); ok {
		return fp, nil
	}
	if len(ref) == 64 {
		raw, err := hex.DecodeString(strings.ToLower(ref))
		if err == nil {
			var fp pairing.Fingerprint
			copy(fp[:], raw)
			if p.IsPeer(fp) {
				return fp, nil
			}
		}
	}
	return pairing.Fingerprint{}, Errorf(localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
		"no peer Broker has the alias or fingerprint %q", ref)
}

// Disconnect removes one peer from the pairing table. The link is closed by
// pairing through the injected transport, so the handler must not call
// PeerLinks.Drop.
func (b *Broker) Disconnect(ctx context.Context, req *localpb.DisconnectRequest) (*localpb.DisconnectResponse, error) {
	actor, err := b.auditActor(ctx, req.GetAgentId())
	if err != nil {
		return nil, toStatus(err)
	}
	if b.deps.Pairing == nil || b.deps.PeerLinks == nil {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"peer management is not available in this Broker"))
	}

	fingerprint, refErr := resolvePeerRef(b.deps.Pairing, req.GetBroker())
	if refErr != nil {
		return nil, toStatus(refErr)
	}

	// Read the alias before Unpair removes the entry: the audit record and the
	// log line name the peer by its alias before removal.
	alias := b.deps.Pairing.EffectiveAlias(fingerprint)
	if err := b.deps.Pairing.Unpair(fingerprint); err != nil {
		return nil, toStatus(unpairError(err))
	}
	// Unpairing is the only event that forgets a peer's inbound buckets; a
	// dropped link keeps them so a reconnect cannot refresh the quota.
	if b.deps.Delivery != nil {
		b.deps.Delivery.inbound.dropPeer(fingerprint)
	}

	b.deps.Config.Audit(unpairedEvent(actor, fingerprint, alias))
	b.log.Info(EvPeerUnpaired, "fingerprint", fingerprint.String(), "alias", alias)
	return &localpb.DisconnectResponse{}, nil
}
