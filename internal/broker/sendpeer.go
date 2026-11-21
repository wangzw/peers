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

// maxLinkBackoff caps a single wait between redials (design-config.md:101).
// The sender keeps one retry budget for the whole send, so an unreachable peer
// is reported within send.request_timeout no matter how the retries are spent.
const maxLinkBackoff = 8 * time.Second

// sendToPeer delivers one message to a remote peer. It waits for the link,
// resolves the target in the peer's roster, sends a kind-400 request, waits for
// the kind-401 result and retries with the same message id after a link drop.
//
// Only the dialer kicks a redial; the acceptor waits for the peer to reconnect.
// A peer error and an unsupported kind end the send at once: the peer has
// decided, so retrying cannot change the result (design-broker.md:244).
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

	// target is resolved once and reused: the message must stay pinned to the
	// address the caller wrote, even if the roster changes during a retry. The
	// request carries that name and ref verbatim, never the roster's canonical
	// ref: a short ref may be one the owner's callers hold, and only the
	// receiving Broker may decide whether it selects the owner's Agent.
	var target *remoteMember

	for ctx.Err() == nil {
		// The generation is taken at the start of every attempt, before the
		// established check and the request. An up event that lands between
		// those steps still follows this value, so the wait below returns at
		// once instead of sitting out a backoff or the rest of the budget.
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
				// A resolution failure is decided by the sender; retrying the
				// link cannot change it.
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
			// The peer answered that it cannot handle the kind, so it certainly
			// did not write anything. This is a version mismatch, not a drop.
			return &Error{
				Code:    localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
				Message: fmt.Sprintf("Not sent: Broker %s does not support message delivery", alias),
				Origin:  peerFingerprintBytes(r.Peer),
			}
		}
		if reqErr == nil {
			detail, decodeErr := decodeDeliverResult(reply)
			if decodeErr != nil {
				// The request may have been written; an unreadable reply cannot
				// prove otherwise.
				return unknownResult("unreadable reply from %s", alias)
			}
			if detail == nil {
				return nil
			}
			// The peer speaks for itself only: its own fingerprint is the
			// source, and any fingerprint it claimed internally is ignored. Its
			// error message is quoted as the peer's words, never trusted as this
			// Broker's own text.
			return d.peerError(r.Peer, alias, detail)
		}
		if sent {
			// Only a dropped link is waited out and resent with the same id; the
			// receiver's dedupe table makes that resend safe. Any other error
			// after the frame left means its reply could not be read, and a
			// resend would only repeat that reply; the message may have been
			// written already.
			if !errors.Is(reqErr, link.ErrLinkDown) {
				return unknownResult("unreadable reply from %s", alias)
			}
			everSent = true
			continue
		}

		// Nothing left the transport. Wait once under the same rules as an
		// unestablished link so a just-dropped link does not spin the loop.
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

// waitForLink returns how long one wait may last. A dialer waits one backoff
// step so it can kick again; an acceptor waits the rest of the budget, because
// only the peer's reconnect can end its wait.
func waitForLink(dialer bool, backoff time.Duration, deadline time.Time) time.Duration {
	remaining := time.Until(deadline)
	if dialer {
		return min(backoff, remaining)
	}
	return remaining
}

// peerFingerprintBytes returns a copy of p for an Error.Origin, so a later
// change to p cannot alter an error already returned to the caller.
func peerFingerprintBytes(p peerID) []byte {
	return append([]byte(nil), p[:]...)
}
