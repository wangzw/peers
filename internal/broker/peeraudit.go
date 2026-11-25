package broker

import (
	"context"
	"errors"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

func (b *Broker) auditActor(ctx context.Context, agentID string) (config.Actor, error) {
	caller, ok := CallerFrom(ctx)
	if !ok {
		return config.Actor{}, Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL, "caller identity is missing")
	}
	if agentID == "" {
		return config.Actor{UIDKnown: true, UID: caller.UID, Account: caller.Username}, nil
	}
	a, err := b.agentFor(ctx, agentID)
	if err != nil {
		return config.Actor{}, err
	}
	b.mu.Lock()
	address := b.addressOf(a)
	b.mu.Unlock()
	return config.Actor{UIDKnown: true, UID: caller.UID, Account: caller.Username, Agent: address}, nil
}

func pairingKeyCreatedEvent(actor config.Actor, expires time.Time) config.Event {
	return config.Event{
		Event:   config.EventPairingKeyCreated,
		Actor:   actor.Agent,
		Account: actor.Account,
		Detail:  "expires " + expires.UTC().Format(time.RFC3339),
	}
}

func connectEvent(actor config.Actor, address string, peer pairing.Peer, err error) (config.Event, bool) {
	if err == nil {
		return config.Event{
			Event:   config.EventPairingSucceeded,
			Actor:   actor.Agent,
			Account: actor.Account,
			Peer:    &config.PeerRef{Fingerprint: peer.Fingerprint, Alias: effectivePeerAlias(peer)},
		}, true
	}

	// A *PairingError unwraps to ErrPairingRejected, so check it first to keep
	// its Reason; that Reason is the accept-side vocabulary.
	var pe *pairing.PairingError
	if errors.As(err, &pe) {
		return pairingFailedEvent(actor, address, pairReasonText(pe.Reason)), true
	}
	switch {
	case errors.Is(err, pairing.ErrPairingRejected):
		return pairingFailedEvent(actor, address, "rejected"), true
	case errors.Is(err, pairing.ErrProtocol):
		return pairingFailedEvent(actor, address, "protocol error"), true
	case errors.Is(err, pairing.ErrPeerUnreachable):
		return pairingFailedEvent(actor, address, "peer unreachable"), true
	default:
		return config.Event{}, false
	}
}

func pairReasonText(reason pairing.RejectReason) string {
	switch reason {
	case pairing.ReasonKeyInvalid:
		return "key invalid"
	case pairing.ReasonVerificationFailed:
		return "verification failed"
	case pairing.ReasonTryLater:
		return "try later"
	case pairing.ReasonTimeout:
		return "handshake timeout"
	case pairing.ReasonNotConnected:
		return "not connected"
	case pairing.ReasonKeyExpired:
		return "key expired"
	case pairing.ReasonVersionIncompatible:
		return "version incompatible"
	default:
		return "rejected"
	}
}

func pairingFailedEvent(actor config.Actor, address, reason string) config.Event {
	return config.Event{
		Event:   config.EventPairingFailed,
		Actor:   actor.Agent,
		Account: actor.Account,
		Detail:  reason + "; address " + address,
	}
}

func unpairedEvent(actor config.Actor, fp pairing.Fingerprint, alias string) config.Event {
	return config.Event{
		Event:   config.EventUnpaired,
		Actor:   actor.Agent,
		Account: actor.Account,
		Peer:    &config.PeerRef{Fingerprint: fp, Alias: alias},
	}
}
