package broker

const sessionPeerProtocol = 1

// checkSessionProtocolLocked logs an unrecognized session registry frame
// protocol version once per agent and value. Callers must hold Broker.mu.
func (b *Broker) checkSessionProtocolLocked(a *agent) {
	p := a.session.PeerProtocol
	if p == 0 || p == sessionPeerProtocol || p == a.warnedPeerProtocol {
		return
	}
	b.log.Warn(EvSessionProtocolUnrecognized,
		"agent", shortID(a.id),
		"pid", a.session.PID,
		"peer_protocol", p,
		"supported", sessionPeerProtocol,
	)
	a.warnedPeerProtocol = p
}
