package pairing

// commitHandshakeForTest drives the handshake commit path through the
// production signature. It is a seam for the regression tests that are also
// copied onto the pre-fix revision, where commitHandshake took the own-alias
// callback instead of a cached value.
func commitHandshakeForTest(p *Pairing, e peerEntry, token uint64) (replaced bool, err error) {
	replaced, _, _, err = p.peers.commitHandshake(e, token)
	return replaced, err
}
