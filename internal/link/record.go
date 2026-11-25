package link

import (
	"context"
	"time"

	"github.com/wangzw/peers/internal/pairing"
)

// record is the link state for one peer; every field is guarded by Link.mu.
type record struct {
	fp          Fingerprint
	role        pairing.Role
	phase       Phase
	lastFailure FailReason
	peerVersion uint32
	nextDial    time.Time

	conn *conn
	salt []byte

	// kick signals the dial loop to end a backoff early; its capacity one keeps
	// Kick from blocking.
	kick       chan struct{}
	cancelLoop context.CancelFunc
}

// Subscribe registers a pair of link up and down callbacks that run in
// subscription order on the notifier goroutine; neither may be nil.
func (l *Link) Subscribe(onUp func(peer Fingerprint, salt []byte), onDown func(peer Fingerprint)) {
	if onUp == nil || onDown == nil {
		panic("link: Subscribe requires non-nil callbacks")
	}
	l.subMu.Lock()
	l.subs = append(l.subs, subscription{onUp: onUp, onDown: onDown})
	l.subMu.Unlock()
}

// State returns a copy of the link state of peer, or the zero state with
// PhaseNone when there is no record.
func (l *Link) State(peer Fingerprint) LinkState {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := l.records[peer]
	if rec == nil {
		return LinkState{Phase: PhaseNone}
	}
	return LinkState{
		Phase:       rec.phase,
		Role:        rec.role,
		LastFailure: rec.lastFailure,
		PeerVersion: rec.peerVersion,
		NextDial:    rec.nextDial,
	}
}

// Drop forgets the peer and closes its connection without waiting; the down
// notification is posted once the connection's state handler exited. It always
// invalidates the drop generation an earlier install for that peer obtained, so
// an install holding an old generation fails afterwards, and a drop may be
// repeated.
func (l *Link) Drop(peer Fingerprint) {
	l.mu.Lock()
	rec := l.records[peer]
	delete(l.records, peer)
	l.dropGen[peer]++
	l.forgetSlotsLocked(peer)
	if rec == nil {
		l.mu.Unlock()
		return
	}
	if rec.cancelLoop != nil {
		rec.cancelLoop()
	}
	c := rec.conn
	if c == nil {
		l.mu.Unlock()
		return
	}
	ch := make(chan struct{})
	l.dropping[peer] = ch
	l.mu.Unlock()

	go func() {
		c.closeAsync(FailConnectionLost)
		<-c.stateDone
		l.mu.Lock()
		l.notifier.post(event{peer: peer, reason: c.reason()})
		if l.dropping[peer] == ch {
			delete(l.dropping, peer)
		}
		l.mu.Unlock()
		close(ch)
	}()
}

// Kick ends the backoff of a dialer peer early; it does nothing for accept-side
// records, other phases, or while a kick is already queued.
func (l *Link) Kick(peer Fingerprint) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := l.records[peer]
	if rec == nil || rec.role != pairing.RoleDialer || rec.phase != PhaseBackoff {
		return
	}
	select {
	case rec.kick <- struct{}{}:
	default:
	}
}

// dropGeneration returns the current drop generation of peer; a handshake takes
// it before installing and passes it to install, so a Drop in between rejects
// the stale connection.
func (l *Link) dropGeneration(peer Fingerprint) uint64 {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dropGen[peer]
}

// install makes c the current connection of peer, waiting for a replaced
// connection and for an in-flight Drop of the same peer to finish; it returns
// false when the Link closed or peer was dropped since gen was taken. The
// returned channel is closed after every onUp callback returned, which holds
// back state frames until subscribers know the link is up.
func (l *Link) install(peer Fingerprint, role pairing.Role, c *conn, salt []byte, gen uint64) (<-chan struct{}, bool) {
	var old *conn
	for {
		l.mu.Lock()
		if l.closed || l.dropGen[peer] != gen {
			l.mu.Unlock()
			return nil, false
		}
		if ch := l.dropping[peer]; ch != nil {
			l.mu.Unlock()
			<-ch
			continue
		}

		rec := l.records[peer]
		if rec != nil && rec.conn != nil && rec.conn != c && rec.conn != old {
			old = rec.conn
			l.mu.Unlock()
			old.closeAsync(FailConnectionLost)
			<-old.stateDone
			continue
		}

		// The replaced connection is closed, but its teardown may not have run
		// yet. Post the down event here in that case; otherwise teardown
		// already posted it.
		replaced := old != nil && rec != nil && rec.conn == old

		if rec == nil || rec.role != role {
			if rec != nil && rec.cancelLoop != nil {
				rec.cancelLoop()
			}
			rec = &record{fp: peer, role: role, kick: make(chan struct{}, 1)}
			l.records[peer] = rec
		}
		if replaced {
			l.notifier.post(event{peer: peer, reason: old.reason()})
		}

		rec.conn = c
		rec.salt = append([]byte(nil), salt...)
		rec.phase = PhaseEstablished
		rec.lastFailure = FailNone
		rec.peerVersion = 0
		// A next dial left over from an earlier backoff would outlive the timer
		// that was armed for it. Only waitBackoff publishes a non-zero NextDial,
		// and it does so together with the timer.
		rec.nextDial = time.Time{}

		delivered := make(chan struct{})
		l.notifier.post(event{up: true, peer: peer, salt: append([]byte(nil), salt...), delivered: delivered})
		l.mu.Unlock()
		return delivered, true
	}
}

// teardown records that c finished; the caller must have closed c.done() and
// c.stateDone, and a connection since replaced or dropped is ignored.
func (l *Link) teardown(c *conn) {
	l.mu.Lock()
	defer l.mu.Unlock()
	rec := l.records[c.id.fp]
	if rec == nil || rec.conn != c {
		return
	}
	rec.conn = nil
	rec.salt = nil
	rec.lastFailure = c.reason()
	if rec.role == pairing.RoleDialer {
		rec.phase = PhaseBackoff
	} else {
		rec.phase = PhaseWaiting
	}
	l.notifier.post(event{peer: c.id.fp, reason: c.reason()})
}

// ensureRecord returns the record of peer, creating it with role and phase when
// absent; the caller must hold l.mu.
func (l *Link) ensureRecord(peer Fingerprint, role pairing.Role, phase Phase) *record {
	if rec := l.records[peer]; rec != nil {
		return rec
	}
	rec := &record{fp: peer, role: role, phase: phase, kick: make(chan struct{}, 1)}
	l.records[peer] = rec
	return rec
}

// setPhase changes the phase and last failure of rec, and does nothing when rec
// was replaced; the caller must hold l.mu.
func (l *Link) setPhase(rec *record, phase Phase, failure FailReason) {
	if l.records[rec.fp] != rec {
		return
	}
	rec.phase = phase
	rec.lastFailure = failure
}
