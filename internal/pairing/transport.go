package pairing

import "context"

// Transport is injected by the broker during assembly; link implements it, so
// pairing never imports link.
type Transport interface {
	// PairDial dials address and hands the connection to run. A dial failure
	// must wrap ErrDialFailed and run's error is returned unchanged. When run
	// returns nil the connection becomes a link and PairDial sends the link's
	// Hello; Connect does not.
	PairDial(ctx context.Context, address string, run func(PairConn) error) error

	// Drop closes the link to fp. It is safe with no link and may be called more
	// than once; pairing calls it outside the peer-table mutex.
	Drop(fp Fingerprint)
}
