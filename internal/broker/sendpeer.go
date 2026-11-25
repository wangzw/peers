package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

const maxLinkBackoff = 8 * time.Second

func (d *delivery) sendToPeer(ctx context.Context, r peerSend) error {
	ctx, cancel := context.WithDeadline(ctx, r.Deadline)
	defer cancel()

	role := d.deps.Peers.Role(r.Peer)
	if role == pairing.RoleUnknown {
		return notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "no Broker is known as %q", r.Addr.Alias)
	}

	alias := d.deps.Peers.EffectiveAlias(r.Peer)
	cfg := d.deps.Settings.Send()
	retriesLeft := cfg.LinkRetries
	backoff := cfg.LinkBackoff
	everSent := false
	start := time.Now()

	// Send the caller's name and ref verbatim, never the roster's canonical ref:
	// only the receiving Broker can decide whether a short ref selects the
	// owner's Agent.
	var target *remoteMember

	for ctx.Err() == nil {
		// The generation is taken at the start of every attempt so an up event
		// landing between the check and the wait still satisfies it.
		gen := d.waiter.generation(r.Peer)

		if !d.deps.Link.Established(r.Peer) {
			if role == pairing.RoleDialer {
				if retriesLeft == 0 {
					break
				}
				retriesLeft--
				d.deps.Link.Kick(r.Peer)
			}
			d.waiter.wait(ctx, r.Peer, waitForLink(role == pairing.RoleDialer, backoff, r.Deadline), gen)
			if role == pairing.RoleDialer {
				backoff = min(2*backoff, maxLinkBackoff)
			}
			continue
		}

		if target == nil {
			members := d.deps.Topics.RemoteMembers(r.Peer)
			cands := remoteCandidates(members,
				func(m remoteMember) bool {
					return d.deps.Topics.VisibleRemote(r.Caller.ID, r.Peer, m.Name, m.Ref)
				},
				func(name, ref string, unique bool) string {
					sig, _ := signatureFor(name, ref, unique, alias)
					return sig
				})
			_, perr := pickTarget(r.Addr, cands, remoteRefMatch, "on "+alias)
			if perr != nil {
				return perr
			}
			target = &remoteMember{
				Name: r.Addr.Name,
				Ref:  r.Addr.Ref,
			}
		}

		f, err := encodeDeliverRequest(deliverRequest{
			MessageID:  r.MessageID,
			SenderName: r.Caller.Name,
			SenderRef:  r.Caller.Ref,
			TargetName: target.Name,
			TargetRef:  target.Ref,
			Remaining:  time.Until(r.Deadline),
			Text:       r.Text,
		})
		if err != nil {
			return notSent(localpb.ErrorCode_ERROR_CODE_INTERNAL, "cannot encode the delivery request: %v", err)
		}

		reply, sent, reqErr := d.deps.Link.Request(ctx, r.Peer, f)

		var unsupported *link.UnsupportedError
		if errors.As(reqErr, &unsupported) {
			return &Error{
				Code:    localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
				Message: fmt.Sprintf("Not sent: Broker %s does not support message delivery", alias),
				Origin:  peerFingerprintBytes(r.Peer),
			}
		}
		if reqErr == nil {
			detail, decodeErr := decodeDeliverResult(reply)
			if decodeErr != nil {
				// An unreadable reply cannot prove the message was not written,
				// so the result is unknown.
				return unknownResult("unreadable reply from %s", alias)
			}
			if detail == nil {
				return nil
			}
			return d.peerError(r.Peer, alias, detail)
		}
		if sent {
			if !errors.Is(reqErr, link.ErrLinkDown) {
				return unknownResult("unreadable reply from %s", alias)
			}
			everSent = true
			continue
		}

		// Nothing left the transport; wait by the same rule as an unestablished
		// link so a just-dropped link does not spin the loop.
		if role == pairing.RoleDialer {
			if retriesLeft == 0 {
				break
			}
			retriesLeft--
			d.deps.Link.Kick(r.Peer)
		}
		d.waiter.wait(ctx, r.Peer, waitForLink(role == pairing.RoleDialer, backoff, r.Deadline), gen)
		if role == pairing.RoleDialer {
			backoff = min(2*backoff, maxLinkBackoff)
		}
	}

	if everSent {
		return unknownResult("no reply from %s within %s; the message may have been delivered",
			alias, time.Since(start).Round(time.Millisecond))
	}
	return notSent(localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE, "Broker %s is unreachable", alias)
}

func waitForLink(dialer bool, backoff time.Duration, deadline time.Time) time.Duration {
	remaining := time.Until(deadline)
	if dialer {
		return min(backoff, remaining)
	}
	return remaining
}

// peerFingerprintBytes returns a copy of p so a later change to p cannot alter an error already returned.
func peerFingerprintBytes(p peerID) []byte {
	return append([]byte(nil), p[:]...)
}
