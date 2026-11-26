package pairing

func commitHandshakeForTest(p *Pairing, e peerEntry, token uint64) (replaced bool, err error) {
	replaced, _, _, err = p.peers.commitHandshake(e, token)
	return replaced, err
}
