package linktest

import (
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
)

const kindHello uint32 = 1

// Observations records what link told the peer table about one peer.
type Observations struct {
	// Aliases holds the aliases link learned from the peer, in call order.
	Aliases []string
	// AcceptorAddresses holds each advertised address list link learned, in
	// call order.
	AcceptorAddresses [][]config.AdvertisedAddr
	// Connected holds the addresses link used to reach the peer, in call order.
	Connected []string
}

// FakePeers is an in-memory peer table with its own identity. It implements the
// method set link requires, so a test can start several identities in one
// process.
type FakePeers struct {
	mu            sync.Mutex
	identity      Identity
	peers         map[pairing.Fingerprint]*fakePeer
	observed      map[pairing.Fingerprint]*Observations
	accept        func(pairing.PairConn) error
	roleHook      func(fp pairing.Fingerprint)
	connectedHook func(fp pairing.Fingerprint, address string)
	changed       chan struct{}
}

type fakePeer struct {
	role       pairing.Role
	alias      string
	candidates []string
	last       string
}

// NewFakePeers returns a peer table whose identity is a freshly generated
// Identity.
func NewFakePeers(t testing.TB) *FakePeers {
	t.Helper()
	return &FakePeers{
		identity: NewIdentity(t),
		peers:    make(map[pairing.Fingerprint]*fakePeer),
		observed: make(map[pairing.Fingerprint]*Observations),
		changed:  make(chan struct{}),
	}
}

// Identity returns the identity this fake presents on its TLS connections.
func (p *FakePeers) Identity() Identity { return p.identity }

// Add records fp as a peer with the given role and alias. candidates are the
// static dial candidates, used only when fp is a dialer peer. An existing entry
// for fp is replaced.
func (p *FakePeers) Add(fp pairing.Fingerprint, role pairing.Role, alias string, candidates ...string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.peers[fp] = &fakePeer{
		role:       role,
		alias:      alias,
		candidates: append([]string(nil), candidates...),
	}
	p.notifyLocked()
}

// Remove deletes fp from the table. Observations are kept.
func (p *FakePeers) Remove(fp pairing.Fingerprint) {
	p.mu.Lock()
	defer p.mu.Unlock()
	delete(p.peers, fp)
	p.notifyLocked()
}

// SetCandidates replaces the static dial candidates of fp and its last
// connected address. An absent fp is created with RoleUnknown.
func (p *FakePeers) SetCandidates(fp pairing.Fingerprint, addrs []string, last string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peer, ok := p.peers[fp]
	if !ok {
		peer = &fakePeer{}
		p.peers[fp] = peer
	}
	peer.candidates = append([]string(nil), addrs...)
	peer.last = last
	p.notifyLocked()
}

// SetAccept installs the function called by Accept. Passing nil restores the
// default, which closes the connection and returns an error.
func (p *FakePeers) SetAccept(f func(pairing.PairConn) error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.accept = f
	p.notifyLocked()
}

// SetRoleHook installs a function called by Role just before it returns,
// letting a test change the table between link's role query and its install of
// the connection.
func (p *FakePeers) SetRoleHook(f func(fp pairing.Fingerprint)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.roleHook = f
	p.notifyLocked()
}

// SetConnectedHook installs a function called by ObserveConnected after the
// address is recorded, letting a test change the table between link's install
// of a paired connection and the start of its dial loop.
func (p *FakePeers) SetConnectedHook(f func(fp pairing.Fingerprint, address string)) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.connectedHook = f
	p.notifyLocked()
}

// Observed returns a copy of everything recorded for fp.
func (p *FakePeers) Observed(fp pairing.Fingerprint) Observations {
	p.mu.Lock()
	defer p.mu.Unlock()
	obs, ok := p.observed[fp]
	if !ok {
		return Observations{}
	}
	return cloneObservations(obs)
}

// Wait blocks until cond is true, re-evaluating it after every change to the
// table, and fails the test once eventTimeout passes.
func (p *FakePeers) Wait(t testing.TB, cond func() bool) {
	t.Helper()
	waitUntil(t, "fake peers state", p.changedCh, cond)
}

func cloneObservations(obs *Observations) Observations {
	out := Observations{
		Aliases:   append([]string(nil), obs.Aliases...),
		Connected: append([]string(nil), obs.Connected...),
	}
	if obs.AcceptorAddresses != nil {
		out.AcceptorAddresses = make([][]config.AdvertisedAddr, len(obs.AcceptorAddresses))
		for i, addrs := range obs.AcceptorAddresses {
			out.AcceptorAddresses[i] = append([]config.AdvertisedAddr(nil), addrs...)
		}
	}
	return out
}

func (p *FakePeers) changedCh() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.changed
}

// notifyLocked wakes every waiter. The caller must hold p.mu.
func (p *FakePeers) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

// Fingerprint implements the identity half of the Peers interface.
func (p *FakePeers) Fingerprint() pairing.Fingerprint { return p.identity.Fingerprint }

// TLSConfig implements the identity half of the Peers interface.
func (p *FakePeers) TLSConfig() *tls.Config { return p.identity.TLSConfig() }

// Peers returns every entry, sorted by fingerprint. Each entry is a copy.
func (p *FakePeers) Peers() []pairing.Peer {
	p.mu.Lock()
	defer p.mu.Unlock()
	fps := make([]pairing.Fingerprint, 0, len(p.peers))
	for fp := range p.peers {
		fps = append(fps, fp)
	}
	sort.Slice(fps, func(i, j int) bool {
		return bytes.Compare(fps[i][:], fps[j][:]) < 0
	})
	out := make([]pairing.Peer, 0, len(fps))
	for _, fp := range fps {
		peer := p.peers[fp]
		entry := pairing.Peer{
			Fingerprint:   fp,
			Alias:         peer.alias,
			Role:          peer.role,
			LastConnected: peer.last,
		}
		for _, addr := range peer.candidates {
			entry.Candidates = append(entry.Candidates, pairing.Candidate{Source: pairing.SourcePairing, Addr: addr})
		}
		out = append(out, entry)
	}
	return out
}

// Role returns the role recorded for fp, or RoleUnknown when fp is absent. The
// role hook, when set, runs after the value is read but before it is returned.
func (p *FakePeers) Role(fp pairing.Fingerprint) pairing.Role {
	p.mu.Lock()
	role := pairing.RoleUnknown
	if peer, ok := p.peers[fp]; ok {
		role = peer.role
	}
	hook := p.roleHook
	p.mu.Unlock()
	if hook != nil {
		hook(fp)
	}
	return role
}

// EffectiveAlias returns the alias recorded by Add, or the empty string.
func (p *FakePeers) EffectiveAlias(fp pairing.Fingerprint) string {
	p.mu.Lock()
	defer p.mu.Unlock()
	if peer, ok := p.peers[fp]; ok {
		return peer.alias
	}
	return ""
}

// Candidates returns the dial addresses for a dialer peer: the direct
// addresses from the most recent ObserveAcceptorAddresses call first, then the
// static candidates, de-duplicated and order-preserving. Non-dialer peers have
// no candidates. The second result is the last connected address when one was
// observed, otherwise the last address given to SetCandidates.
func (p *FakePeers) Candidates(fp pairing.Fingerprint) (addresses []string, last string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	peer, ok := p.peers[fp]
	if !ok || peer.role != pairing.RoleDialer {
		return nil, ""
	}
	seen := make(map[string]bool)
	add := func(addr string) {
		if !seen[addr] {
			seen[addr] = true
			addresses = append(addresses, addr)
		}
	}
	obs := p.observed[fp]
	if obs != nil && len(obs.AcceptorAddresses) > 0 {
		for _, addr := range obs.AcceptorAddresses[len(obs.AcceptorAddresses)-1] {
			if addr.Kind == config.AdvertisedKindDirect && addr.Addr != "" {
				add(addr.Addr)
			}
		}
	}
	for _, addr := range peer.candidates {
		add(addr)
	}
	last = peer.last
	if obs != nil && len(obs.Connected) > 0 {
		last = obs.Connected[len(obs.Connected)-1]
	}
	return addresses, last
}

// ObserveAlias records the alias link learned from fp.
func (p *FakePeers) ObserveAlias(fp pairing.Fingerprint, alias string) {
	p.mu.Lock()
	defer p.mu.Unlock()
	obs := p.observationsLocked(fp)
	obs.Aliases = append(obs.Aliases, alias)
	p.notifyLocked()
}

// ObserveAcceptorAddresses records the advertised addresses link learned from
// fp. Unknown kinds and empty addresses are kept as given.
func (p *FakePeers) ObserveAcceptorAddresses(fp pairing.Fingerprint, addresses []config.AdvertisedAddr) {
	p.mu.Lock()
	defer p.mu.Unlock()
	obs := p.observationsLocked(fp)
	obs.AcceptorAddresses = append(obs.AcceptorAddresses, append([]config.AdvertisedAddr(nil), addresses...))
	p.notifyLocked()
}

// ObserveConnected records the address link used to reach fp. The connected
// hook, when set, runs after the address is recorded.
func (p *FakePeers) ObserveConnected(fp pairing.Fingerprint, address string) {
	p.mu.Lock()
	obs := p.observationsLocked(fp)
	obs.Connected = append(obs.Connected, address)
	hook := p.connectedHook
	p.notifyLocked()
	p.mu.Unlock()
	if hook != nil {
		hook(fp, address)
	}
}

func (p *FakePeers) observationsLocked(fp pairing.Fingerprint) *Observations {
	obs, ok := p.observed[fp]
	if !ok {
		obs = &Observations{}
		p.observed[fp] = obs
	}
	return obs
}

// Accept hands conn to the installed accept function. Without one it closes
// conn and returns an error.
func (p *FakePeers) Accept(conn pairing.PairConn) error {
	p.mu.Lock()
	accept := p.accept
	p.mu.Unlock()
	if accept == nil {
		_ = conn.Close()
		return errors.New("linktest: no accept function")
	}
	return accept(conn)
}

// Introduce registers each fake as a peer of the other: dialer dials acceptor,
// and acceptor waits. acceptorAddrs are the dialer's dial candidates for the
// acceptor.
func Introduce(dialer, acceptor *FakePeers, dialerAlias, acceptorAlias string, acceptorAddrs ...string) {
	dialer.Add(acceptor.Fingerprint(), pairing.RoleDialer, acceptorAlias, acceptorAddrs...)
	acceptor.Add(dialer.Fingerprint(), pairing.RoleAcceptor, dialerAlias)
}

// FakePairAccept returns a minimal acceptor-side pairing flow: it expects the
// request, challenge, confirm and link Hello frames in order, unreads the
// Hello, records the dialer in p and returns nil. Any unexpected frame or
// transport error closes conn and is returned.
func FakePairAccept(p *FakePeers, alias string) func(pairing.PairConn) error {
	return func(conn pairing.PairConn) error {
		fail := func(err error) error {
			_ = conn.Close()
			return err
		}
		ctx := context.Background()
		if err := pairReceive(ctx, conn, pairingpb.KindPairRequest); err != nil {
			return fail(err)
		}
		if err := conn.Send(ctx, pairingpb.KindPairChallenge, nil); err != nil {
			return fail(err)
		}
		if err := pairReceive(ctx, conn, pairingpb.KindPairConfirm); err != nil {
			return fail(err)
		}
		if err := conn.Send(ctx, pairingpb.KindPairDone, nil); err != nil {
			return fail(err)
		}
		kind, payload, err := conn.Receive(ctx)
		if err != nil {
			return fail(err)
		}
		if kind != kindHello {
			return fail(fmt.Errorf("linktest: pairing hand-off frame kind %d, want %d", kind, kindHello))
		}
		conn.Unread(kind, payload)
		p.Add(conn.PeerFingerprint(), pairing.RoleAcceptor, alias)
		return nil
	}
}

// FakePairRun returns the dialer-side counterpart of FakePairAccept. It sends
// the request and confirm frames, checks the challenge and done replies, and
// records the acceptor in p with RoleDialer.
func FakePairRun(p *FakePeers, alias string) func(pairing.PairConn) error {
	return func(conn pairing.PairConn) error {
		fail := func(err error) error {
			_ = conn.Close()
			return err
		}
		ctx := context.Background()
		if err := conn.Send(ctx, pairingpb.KindPairRequest, nil); err != nil {
			return fail(err)
		}
		if err := pairReceive(ctx, conn, pairingpb.KindPairChallenge); err != nil {
			return fail(err)
		}
		if err := conn.Send(ctx, pairingpb.KindPairConfirm, nil); err != nil {
			return fail(err)
		}
		if err := pairReceive(ctx, conn, pairingpb.KindPairDone); err != nil {
			return fail(err)
		}
		p.Add(conn.PeerFingerprint(), pairing.RoleDialer, alias)
		return nil
	}
}

func pairReceive(ctx context.Context, conn pairing.PairConn, want uint32) error {
	kind, _, err := conn.Receive(ctx)
	if err != nil {
		return err
	}
	if kind != want {
		return fmt.Errorf("linktest: pairing frame kind %d, want %d", kind, want)
	}
	return nil
}
