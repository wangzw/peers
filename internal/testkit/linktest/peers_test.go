package linktest

import (
	"context"
	"crypto/ed25519"
	"net"
	"reflect"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

func TestIdentityFingerprintMatchesPairing(t *testing.T) {
	id := NewIdentity(t)
	if len(id.PublicKey) != ed25519.PublicKeySize {
		t.Fatalf("public key length = %d, want %d", len(id.PublicKey), ed25519.PublicKeySize)
	}
	if got := pairing.FingerprintOf(id.PublicKey); got != id.Fingerprint {
		t.Fatalf("FingerprintOf(PublicKey) = %s, want %s", got, id.Fingerprint)
	}
	if got := pairing.FingerprintOf(id.Leaf.RawSubjectPublicKeyInfo); got != id.Fingerprint {
		t.Fatalf("FingerprintOf(subject public key info) = %s, want %s", got, id.Fingerprint)
	}

	other := NewIdentity(t)
	if other.Fingerprint == id.Fingerprint {
		t.Fatal("two identities share a fingerprint")
	}
}

func TestFakePeersCandidatesOrder(t *testing.T) {
	peers := NewFakePeers(t)
	dialPeer := NewIdentity(t).Fingerprint
	acceptorPeer := NewIdentity(t).Fingerprint

	peers.Add(dialPeer, pairing.RoleDialer, "acceptor", "a", "b")
	peers.ObserveAcceptorAddresses(dialPeer, []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "c"},
		{Kind: "relay", Addr: "x"},
		{Kind: config.AdvertisedKindDirect, Addr: "a"},
	})

	got, last := peers.Candidates(dialPeer)
	want := []string{"c", "a", "b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("Candidates() = %v, want %v", got, want)
	}
	if last != "" {
		t.Fatalf("Candidates() last = %q, want empty", last)
	}

	peers.ObserveConnected(dialPeer, "192.0.2.7:17443")
	if _, last := peers.Candidates(dialPeer); last != "192.0.2.7:17443" {
		t.Fatalf("Candidates() last = %q, want the observed address", last)
	}

	peers.Add(acceptorPeer, pairing.RoleAcceptor, "dialer")
	got, last = peers.Candidates(acceptorPeer)
	if len(got) != 0 || last != "" {
		t.Fatalf("acceptor peer Candidates() = %v, %q; want empty", got, last)
	}
}

// memFrame is one frame on a memPairConn.
type memFrame struct {
	kind    uint32
	payload []byte
}

// memPairConn is an in-memory pairing.PairConn used to run the two fake
// handshake halves against each other.
type memPairConn struct {
	fingerprint pairing.Fingerprint
	remote      string

	in  <-chan memFrame
	out chan<- memFrame

	mu     sync.Mutex
	unread []memFrame
	closed chan struct{}
	once   sync.Once
}

func (c *memPairConn) PeerFingerprint() pairing.Fingerprint { return c.fingerprint }

func (c *memPairConn) PeerPublicKey() []byte { return nil }

func (c *memPairConn) RemoteAddr() string { return c.remote }

// memProtocolVersion stands in for the link protocol version the fake
// connections speak. The fake pairing flows do not compare versions.
const memProtocolVersion uint32 = 1

func (c *memPairConn) ProtocolVersion() uint32 { return memProtocolVersion }

func (c *memPairConn) Send(ctx context.Context, kind uint32, payload []byte) error {
	frame := memFrame{kind: kind, payload: append([]byte(nil), payload...)}
	select {
	case c.out <- frame:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return net.ErrClosed
	}
}

func (c *memPairConn) Receive(ctx context.Context) (uint32, []byte, error) {
	c.mu.Lock()
	if n := len(c.unread); n > 0 {
		frame := c.unread[n-1]
		c.unread = c.unread[:n-1]
		c.mu.Unlock()
		return frame.kind, frame.payload, nil
	}
	c.mu.Unlock()
	select {
	case frame := <-c.in:
		return frame.kind, frame.payload, nil
	case <-ctx.Done():
		return 0, nil, ctx.Err()
	case <-c.closed:
		return 0, nil, net.ErrClosed
	}
}

func (c *memPairConn) Unread(kind uint32, payload []byte) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.unread = append(c.unread, memFrame{kind: kind, payload: append([]byte(nil), payload...)})
}

func (c *memPairConn) Close() error {
	c.once.Do(func() { close(c.closed) })
	return nil
}

func TestFakePairAcceptAndRunInterlock(t *testing.T) {
	dialer := NewFakePeers(t)
	acceptor := NewFakePeers(t)

	dialToAcceptor := make(chan memFrame, 8)
	acceptorToDial := make(chan memFrame, 8)
	dialConn := &memPairConn{
		fingerprint: acceptor.Fingerprint(),
		remote:      "127.0.0.1:1",
		in:          acceptorToDial,
		out:         dialToAcceptor,
		closed:      make(chan struct{}),
	}
	acceptConn := &memPairConn{
		fingerprint: dialer.Fingerprint(),
		remote:      "127.0.0.1:2",
		in:          dialToAcceptor,
		out:         acceptorToDial,
		closed:      make(chan struct{}),
	}

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()

	errs := make(chan error, 2)
	go func() {
		err := FakePairRun(dialer, "acceptor")(dialConn)
		if err == nil {
			err = dialConn.Send(ctx, kindHello, nil)
		}
		errs <- err
	}()
	go func() {
		errs <- FakePairAccept(acceptor, "dialer")(acceptConn)
	}()
	for i := 0; i < 2; i++ {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("fake pairing flow: %v", err)
			}
		case <-ctx.Done():
			t.Fatal("fake pairing flow timed out")
		}
	}

	if got := dialer.Role(acceptor.Fingerprint()); got != pairing.RoleDialer {
		t.Fatalf("dialer peer role = %v, want %v", got, pairing.RoleDialer)
	}
	if got := acceptor.Role(dialer.Fingerprint()); got != pairing.RoleAcceptor {
		t.Fatalf("acceptor peer role = %v, want %v", got, pairing.RoleAcceptor)
	}
	if got := dialer.EffectiveAlias(acceptor.Fingerprint()); got != "acceptor" {
		t.Fatalf("dialer-side alias = %q, want %q", got, "acceptor")
	}
	if got := acceptor.EffectiveAlias(dialer.Fingerprint()); got != "dialer" {
		t.Fatalf("acceptor-side alias = %q, want %q", got, "dialer")
	}

	kind, _, err := acceptConn.Receive(ctx)
	if err != nil {
		t.Fatalf("reading the unread Hello: %v", err)
	}
	if kind != kindHello {
		t.Fatalf("unread frame kind = %d, want %d", kind, kindHello)
	}
}
