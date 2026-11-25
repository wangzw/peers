package broker

import (
	"context"
	"errors"
	"fmt"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// peerAssembler builds pairing and link and connects them to the Broker's peer
// surfaces. It only constructs instances; Run binds the listener and starts link
// after the local socket is ready.
type peerAssembler struct {
	openPairing func(t pairing.Transport) (PeerStore, *pairing.Pairing, error)
	newLink     func(opts link.Options) (*link.Link, error)
	adapt       func(l *link.Link) (PeerLinks, LinkRunner)
}

var _ Assembler = (*peerAssembler)(nil)

func newPeerAssembler() *peerAssembler {
	return &peerAssembler{
		openPairing: func(t pairing.Transport) (PeerStore, *pairing.Pairing, error) {
			p, err := pairing.Open(t)
			if err != nil {
				return nil, nil, err
			}
			return p, p, nil
		},
		newLink: link.New,
		adapt: func(l *link.Link) (PeerLinks, LinkRunner) {
			return peerLinkAdapter{l}, l
		},
	}
}

// Name is the assembler's name in startup errors.
func (*peerAssembler) Name() string { return "pairing and link" }

// Assemble opens pairing, wires broker.alias into it, builds link over the same
// pairing instance, binds the late transport, and fills Deps and Assembly. The
// alias is wired before link exists so no link can handshake with an empty alias
// cache.
func (a *peerAssembler) Assemble(ctx context.Context, cfg ConfigSource, deps *Deps, asm *Assembly) ([]Component, error) {
	lt := &lateTransport{}
	store, concrete, err := a.openPairing(lt)
	if err != nil {
		// Only the identity and the peer table live in the data directory; a
		// configuration failure here stays fatal.
		switch {
		case errors.Is(err, pairing.ErrIdentity):
			err = wrapDataUnavailable(dataKindIdentity, err)
		case errors.Is(err, pairing.ErrStore):
			err = wrapDataUnavailable(dataKindPeers, err)
		}
		return nil, fmt.Errorf("loading identity and peer table: %w", err)
	}

	if err := wireOwnAlias(cfg, store); err != nil {
		return nil, err
	}

	lk, err := a.newLink(link.Options{
		Peers:           concrete,
		Settings:        link.ConfigSettings(),
		Clock:           link.RealClock(),
		Logger:          deps.Logger,
		ProtocolVersion: asm.LinkProtocolVersion,
	})
	if err != nil {
		return nil, fmt.Errorf("creating link: %w", err)
	}
	links, runner := a.adapt(lk)

	lt.set(links)

	deps.Pairing = store
	deps.PeerLinks = links
	deps.Peers = newPeerViewFunc(store, links, func() BrokerCatalog { return deps.Catalog })

	asm.Pairing = concrete
	asm.Link = lk
	asm.Runner = runner

	return nil, nil
}
