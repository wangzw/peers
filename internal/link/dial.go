package link

import (
	"context"
	"net"
	"sync"

	"github.com/wangzw/peers/internal/pairing"
)

// Start starts the dial loop of every peer this Broker is the dialing side of.
// It creates the link record of every peer in the table, returns without
// waiting for any dial, and does nothing when called again or after Close.
// Records that were already established by an inbound connection are left as
// they are.
func (l *Link) Start() {
	l.mu.Lock()
	if l.started || l.closed {
		l.mu.Unlock()
		return
	}
	l.started = true
	l.mu.Unlock()

	// The notifier delivers the up event of a link installed later, including
	// one established by an inbound connection before Start.
	l.notifier.start()

	for _, peer := range l.peers.Peers() {
		switch peer.Role {
		case pairing.RoleAcceptor:
			l.mu.Lock()
			l.ensureRecord(peer.Fingerprint, pairing.RoleAcceptor, PhaseWaiting)
			l.mu.Unlock()
		case pairing.RoleDialer:
			l.mu.Lock()
			rec := l.ensureRecord(peer.Fingerprint, pairing.RoleDialer, PhaseDialing)
			start := rec.cancelLoop == nil && rec.conn == nil && rec.phase != PhaseEstablished
			l.mu.Unlock()
			if start {
				l.startDialLoop(peer.Fingerprint, nil, nil, false)
			}
		}
	}
}

// localNetworks returns the local interface networks used to rank dial
// candidates. A failing enumeration only loses the same-network preference; it
// is not a reason to stop dialing.
func (l *Link) localNetworks() []*net.IPNet {
	f := l.localNets
	if f == nil {
		f = defaultLocalNets
	}
	nets, err := f()
	if err != nil {
		l.logger.Debug("link: cannot enumerate local networks", "error", err)
		return nil
	}
	return nets
}

// startDialLoop starts the dial loop of one record. initial, when not nil, is a
// connection that was already paired and must run before the first backoff,
// even when Drop removed the record or canceled the loop context between
// install and this call; startWithBackoff makes the loop wait one reconnect
// interval before dialing, which is what a PairDial whose Hello exchange failed
// uses. A loop that is already running for the same peer is canceled first.
func (l *Link) startDialLoop(fp Fingerprint, initial *conn, delivered <-chan struct{}, startWithBackoff bool) {
	l.mu.Lock()
	rec := l.records[fp]
	if rec == nil || l.closed {
		closed := l.closed
		if initial == nil {
			l.mu.Unlock()
			return
		}
		if closed {
			// Close collected the connections in the records it could see; this
			// one lost its record to Drop, so Close did not close it. Close it
			// here before running it, or a buffered state frame would reach its
			// handler after Close returned. It must not join wg either: Close
			// may already be waiting on it.
			l.mu.Unlock()
			initial.closeAsync(FailConnectionLost)
			l.runConn(initial, delivered)
			return
		}
		// Drop removed the record after install but is still waiting for the
		// state handler of the installed connection to exit. The connection
		// must run regardless of the record: without it stateDone never
		// closes, Drop blocks forever, onDown never fires and every later
		// install of the peer waits on that Drop. No dial loop follows, since
		// the peer is gone.
		l.wg.Add(1)
		l.mu.Unlock()
		go func() {
			defer l.wg.Done()
			l.runConn(initial, delivered)
		}()
		return
	}
	if rec.cancelLoop != nil {
		rec.cancelLoop()
	}
	ctx, cancel := context.WithCancel(l.rootCtx)
	rec.cancelLoop = cancel
	l.wg.Add(1)
	l.mu.Unlock()

	go func() {
		defer l.wg.Done()
		if l.beforeDialLoop != nil {
			l.beforeDialLoop()
		}
		// install accepted the connection, so it must run whatever happened to
		// the record or this loop's context since: Drop waits for its state
		// handler before posting onDown, and every later install of the peer
		// waits for that Drop. Running it only inside the loop, after the
		// context check, left the peer stuck when Drop canceled the context
		// before this goroutine was scheduled.
		if initial != nil {
			l.runConn(initial, delivered)
			// The connection that just ended was established, so the next dial
			// waits one reconnect interval.
			startWithBackoff = true
		}
		l.dialLoop(ctx, rec, startWithBackoff)
	}()
}

// dialOutcome is the result of one dial round that the loop acts on.
type dialOutcome int

const (
	roundEstablished dialOutcome = iota // the connection ran and ended
	roundFailed                         // try again after a backoff
	roundRejected                       // the peer does not know us; stop dialing
)

// dialLoop redials fp until the record disappears, the Link closes or the peer
// rejects the Broker. The reconnect interval starts at the configured minimum
// after a successful link ends or a failed round, and doubles with every
// consecutive failure up to the configured maximum.
func (l *Link) dialLoop(ctx context.Context, rec *record, startWithBackoff bool) {
	failures := 0
	for {
		if ctx.Err() != nil {
			return
		}

		if startWithBackoff {
			startWithBackoff = false
		} else {
			switch l.dialRound(ctx, rec) {
			case roundRejected:
				return
			case roundEstablished:
				failures = 0
			}
		}

		if !l.waitBackoff(ctx, rec, failures) {
			return
		}
		failures++
	}
}

// staleLocked reports whether this dial loop no longer owns rec: the peer was
// dropped, another connection was installed, or the Link closed. The caller
// must hold l.mu.
func (l *Link) staleLocked(rec *record) bool {
	return l.records[rec.fp] != rec || l.closed || rec.conn != nil
}

// recordLiveLocked reports whether rec still covers the peer with no connection
// installed, so a dial result may update its state. The caller must hold l.mu.
func (l *Link) recordLiveLocked(rec *record) bool {
	return l.records[rec.fp] == rec && rec.conn == nil
}

// dialRound tries the candidates of fp once, in order, until one address
// reaches a TLS connection whose certificate fingerprint is fp and the Hello
// exchange succeeds. On success it runs the connection and returns only after
// the link ended.
func (l *Link) dialRound(ctx context.Context, rec *record) dialOutcome {
	fp := rec.fp

	l.mu.Lock()
	if l.staleLocked(rec) {
		// A connection was installed by someone else, the peer was dropped, or
		// the Link closed. This loop is stale.
		l.mu.Unlock()
		return roundRejected
	}
	rec.phase = PhaseDialing
	l.mu.Unlock()

	addrs, last := l.peers.Candidates(fp)
	candidates := orderCandidates(addrs, last, l.localNetworks())

	for _, addr := range candidates {
		if ctx.Err() != nil {
			return roundFailed
		}

		gen := l.dropGeneration(fp)
		tc, id, err := l.dialTLS(ctx, addr)
		if err != nil {
			continue
		}
		if id.fp != fp {
			// A different Broker answers on this address. Nothing was sent over
			// the stream, and the TLS peer already saw our certificate.
			_ = tc.Close()
			continue
		}

		l.mu.Lock()
		if l.staleLocked(rec) {
			l.mu.Unlock()
			_ = tc.Close()
			return roundRejected
		}
		rec.phase = PhaseHandshaking
		l.mu.Unlock()

		c, err := l.openClientStream(tc, id, addr)
		if err != nil {
			// openClientStream closed the transport on failure; try the next
			// address.
			continue
		}
		c.role = pairing.RoleDialer

		salt, failure, peerVersion, handshakeErr := l.dialerHandshake(c)
		if handshakeErr != nil || failure != FailNone {
			c.closeAfterFailedHandshake(failedHandshakeReason(failure))

			if failure == FailNotAPeer {
				l.mu.Lock()
				if l.recordLiveLocked(rec) {
					rec.phase = PhaseRejected
					rec.lastFailure = FailNotAPeer
				}
				l.mu.Unlock()
				l.auditPeerRejected(fp)
				return roundRejected
			}

			l.mu.Lock()
			if l.recordLiveLocked(rec) {
				rec.lastFailure = failure
				rec.peerVersion = peerVersion
			}
			l.mu.Unlock()
			return roundFailed
		}

		delivered, ok := l.install(fp, pairing.RoleDialer, c, salt, gen)
		if !ok {
			c.closeAsync(FailConnectionLost)
			return roundRejected
		}
		l.peers.ObserveConnected(fp, addr)
		l.runConn(c, delivered)
		return roundEstablished
	}

	l.mu.Lock()
	if l.recordLiveLocked(rec) {
		rec.lastFailure = FailUnreachable
		rec.peerVersion = 0
	}
	l.mu.Unlock()
	return roundFailed
}

// waitBackoff records the next dial time and waits for it, for Kick, or for the
// loop context to end. It reports false when the loop must exit.
func (l *Link) waitBackoff(ctx context.Context, rec *record, failures int) bool {
	// The interval bounds are read for every round, so a configuration change
	// applies to the next one.
	delay := backoffDelay(failures, l.settings.ReconnectMin(), l.settings.ReconnectMax())

	l.mu.Lock()
	if l.staleLocked(rec) {
		// A new connection took over while this loop was finishing; it must not
		// overwrite the state of an established link with a backoff.
		l.mu.Unlock()
		return false
	}

	// The timer is armed before the backoff is published. A state observer that
	// sees PhaseBackoff with this NextDial may advance the clock to it at once;
	// arming the timer after the unlock would instead schedule it at the already
	// advanced now+delay, with no later advance to fire it, and the loop would
	// wait forever.
	signal := make(chan struct{})
	var once sync.Once
	stop := l.clock.AfterFunc(delay, func() {
		once.Do(func() { close(signal) })
	})
	rec.phase = PhaseBackoff
	rec.nextDial = l.clock.Now().Add(delay)
	l.mu.Unlock()

	select {
	case <-signal:
	case <-rec.kick:
	case <-ctx.Done():
		if stop != nil {
			stop()
		}
		return false
	}
	if stop != nil {
		stop()
	}
	return ctx.Err() == nil
}
