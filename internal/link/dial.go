package link

import (
	"context"
	"net"
	"sync"

	"github.com/wangzw/peers/internal/pairing"
)

// Start makes the peers on which this side is the dialer begin dialing. It
// creates the link record of every peer in the table, returns without waiting
// for a dial, does nothing when called again or after Close, and leaves an
// already established record unchanged.
func (l *Link) Start() {
	l.mu.Lock()
	if l.started || l.closed {
		l.mu.Unlock()
		return
	}
	l.started = true
	l.mu.Unlock()

	// Starting the notifier here delivers up events posted before Start,
	// including one from a connection an inbound handshake established.
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

// A failing enumeration only loses the same-network preference, not a reason to
// stop dialing.
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
// install and this call. startWithBackoff makes the loop wait one reconnect
// interval before dialing. A loop already running for the same peer is
// canceled first.
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
			// Close did not collect this connection: its record was lost to Drop,
			// so close it before running it, or a buffered state frame would
			// reach its handler after Close returned. It must not join wg either:
			// Close may already be waiting on it.
			l.mu.Unlock()
			initial.closeAsync(FailConnectionLost)
			l.runConn(initial, delivered)
			return
		}
		// The connection must run even though Drop removed the record: Drop
		// waits for its state handler, so without it Drop blocks forever and
		// every later install of the peer waits on that Drop. No dial loop
		// follows, since the peer is gone.
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
		// handler before posting onDown.
		if initial != nil {
			l.runConn(initial, delivered)
			startWithBackoff = true
		}
		l.dialLoop(ctx, rec, startWithBackoff)
	}()
}

type dialOutcome int

const (
	roundEstablished dialOutcome = iota
	roundFailed
	roundRejected
)

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
			// A different Broker answers: send nothing; the TLS peer already saw
			// our certificate.
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
			// openClientStream closed the transport; try the next address.
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

// It reports false when the loop must exit.
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

	// Arm the timer before publishing the backoff: an observer that sees
	// PhaseBackoff may advance the clock to NextDial at once, and a timer armed
	// after the unlock would wait for an advance that never comes.
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
