package broker

import (
	"context"

	"google.golang.org/protobuf/types/known/timestamppb"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// CreatePairingKey generates a one-time pairing key and returns it with the
// addresses to show and its expiry. It never reports the key in an audit record
// or a log line. The optional agent_id only fills the audit actor, and a bad
// one is refused before any key is created. A Broker that does not listen
// returns invalid_argument; a local-only degraded Broker returns
// broker_degraded, and a Broker without pairing returns internal.
func (b *Broker) CreatePairingKey(ctx context.Context, req *localpb.CreatePairingKeyRequest) (*localpb.CreatePairingKeyResponse, error) {
	if degraded := b.degradedState(); degraded != nil &&
		degraded.Level == localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY {
		msg := degraded.Reason
		if degraded.Fix != "" {
			msg += "; " + degraded.Fix
		}
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED, "%s", msg))
	}

	actor, err := b.auditActor(ctx, req.GetAgentId())
	if err != nil {
		return nil, toStatus(err)
	}
	if b.deps.Pairing == nil {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL, "peer management is not available in this Broker"))
	}

	key, expires, shown, err := b.deps.Pairing.CreateOneTimeKey()
	if err != nil {
		return nil, toStatus(pairingKeyError(err))
	}

	b.deps.Config.Audit(pairingKeyCreatedEvent(actor, expires))

	if shown == nil {
		shown = []string{}
	}
	return &localpb.CreatePairingKeyResponse{
		Key:            key,
		ExpiresAt:      timestamppb.New(expires),
		ShownAddresses: shown,
	}, nil
}
