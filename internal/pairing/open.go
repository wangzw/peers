package pairing

import (
	"fmt"
	"path/filepath"
)

const peersFileName = "peers.json"

// Open loads the identity and the peer table and installs the transport. It
// must be called once per data directory, after config.Load. Each call returns
// an independent instance; a data directory should have only one. It returns
// ErrNoTransport for a nil transport, ErrNotOpen when the data directory or the
// alias cannot be read, ErrIdentity when the identity is rejected, and ErrStore
// when the peer table cannot be read or parsed.
func Open(t Transport) (*Pairing, error) {
	return newPairing(newConfigSettings(), t, realClock{}, realFileOps{}, nil)
}

func newPairing(s settings, t Transport, c clock, ops fileOps, randRead func([]byte) (int, error)) (*Pairing, error) {
	if t == nil {
		return nil, ErrNoTransport
	}
	if s == nil {
		return nil, fmt.Errorf("%w: no settings", ErrNotOpen)
	}
	if c == nil {
		c = realClock{}
	}
	if ops == nil {
		ops = realFileOps{}
	}

	dir, err := s.DataDir()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotOpen, err)
	}
	id, err := loadIdentity(dir, c.Now)
	if err != nil {
		return nil, err
	}
	table, err := loadPeerTable(filepath.Join(dir, peersFileName), ops)
	if err != nil {
		return nil, err
	}
	ownAlias, err := s.Alias()
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrNotOpen, err)
	}
	table.setOwnAlias(ownAlias)
	return &Pairing{
		settings:  s,
		transport: t,
		clock:     c,
		identity:  id,
		peers:     table,
		keys:      &keyTable{},
		storeOps:  ops,
		randRead:  randRead,
	}, nil
}
