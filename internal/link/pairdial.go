package link

import (
	"context"
	"fmt"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

var _ pairing.Transport = (*Link)(nil)

// PairDial dials address for the pairing handshake, hands the connection to
// run, and turns it into a link once pairing committed the peer. A run error is
// returned unchanged; the caller's context only bounds dialing and run, never
// the link that starts afterwards.
func (l *Link) PairDial(ctx context.Context, address string, run func(pairing.PairConn) error) error {
	l.mu.Lock()
	closed := l.closed
	l.mu.Unlock()
	if closed {
		return ErrClosed
	}

	tc, id, err := l.dialTLS(ctx, address)
	if err != nil {
		return fmt.Errorf("%w: %v", pairing.ErrDialFailed, err)
	}
	c, err := l.openClientStream(tc, id, address)
	if err != nil {
		return fmt.Errorf("%w: %v", pairing.ErrDialFailed, err)
	}

	if runErr := run(newPairConn(c, l.protocolVersion, l.lingerAfterSend)); runErr != nil {
		c.closeAsync(FailProtocol)
		return runErr
	}

	// run may have dropped this peer while re-pairing; taking the generation
	// after it keeps the Drop from suppressing the new connection.
	fp := id.fp
	gen := l.dropGeneration(fp)
	if role := l.peers.Role(fp); role != pairing.RoleDialer {
		l.logger.Error("link: paired peer is not a dialing-side peer",
			"peer", fp.String(), "role", role.String())
		c.closeAsync(FailProtocol)
		return nil
	}

	c.role = pairing.RoleDialer
	salt, failure, peerVersion, handshakeErr := l.dialerHandshake(c)
	if handshakeErr != nil || failure != FailNone {
		reason := failure
		if reason == FailNone {
			reason = FailConnectionLost
		}
		c.closeAfterFailedHandshake(reason)

		l.mu.Lock()
		if !l.closed && l.dropGen[fp] == gen {
			rec := l.ensureRecord(fp, pairing.RoleDialer, PhaseDialing)
			// An established link already covers this peer; do not replace its
			// state with the result of this extra pairing connection.
			if rec.conn == nil {
				if failure == FailNotAPeer {
					l.setPhase(rec, PhaseRejected, FailNotAPeer)
				} else {
					rec.lastFailure = failure
					rec.peerVersion = peerVersion
				}
			}
		}
		l.mu.Unlock()

		if failure == FailNotAPeer {
			// pairing committed the peer, so PairDial still succeeds; the link
			// stops redialing until the user pairs again.
			l.settings.Audit(config.Event{
				Time:  l.clock.Now(),
				Event: config.EventRejectedByPeer,
				Peer: &config.PeerRef{
					Fingerprint: fp,
					Alias:       l.peers.EffectiveAlias(fp),
				},
			})
			l.logger.Warn("link: peer no longer treats this Broker as a peer",
				"peer", fp.String())
			return nil
		}

		l.startDialLoop(fp, nil, nil, true)
		return nil
	}

	delivered, ok := l.install(fp, pairing.RoleDialer, c, salt, gen)
	if !ok {
		c.closeAsync(FailConnectionLost)
		return nil
	}
	l.peers.ObserveConnected(fp, address)
	// The connection is installed, so it must run even if Drop removed the
	// record before the loop starts; startDialLoop handles that case.
	l.startDialLoop(fp, c, delivered, false)
	return nil
}
