package pairing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/protobuf/proto"
)

func seedLocalPeer(t *testing.T, p *Pairing, alias string, clk *hsClock) Fingerprint {
	t.Helper()
	fp, pub := hsKey(t)
	if _, err := p.peers.put(peerEntry{
		fingerprint: fp,
		publicKey:   pub,
		alias:       alias,
		role:        RoleDialer,
		pairedAt:    clk.Now(),
	}); err != nil {
		t.Fatalf("seed peer %q: %v", alias, err)
	}
	return fp
}

func acceptManual(t *testing.T, p *Pairing, alias, key string) {
	t.Helper()
	fp, pub := hsKey(t)
	aConn, bConn := hsConnPairFor(fp, pub, p.identity.fingerprint(), p.identity.publicKey())
	if err := hsCompleteAsInitiator(t, p, aConn, bConn, fp, alias, key); err != nil {
		t.Fatalf("manual handshake: %v", err)
	}
}

type addrRecordingTransport struct {
	*hsTransport
	mu      sync.Mutex
	address string
}

func (t *addrRecordingTransport) PairDial(ctx context.Context, address string, run func(PairConn) error) error {
	t.mu.Lock()
	t.address = address
	t.mu.Unlock()
	return t.hsTransport.PairDial(ctx, address, run)
}

func (t *addrRecordingTransport) dialedAddress() string {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.address
}

type dropFrameConn struct {
	PairConn
	kind uint32
}

func (c *dropFrameConn) Receive(ctx context.Context) (uint32, []byte, error) {
	kind, payload, err := c.PairConn.Receive(ctx)
	if err != nil {
		return 0, nil, err
	}
	if kind == c.kind {
		return 0, nil, io.EOF
	}
	return kind, payload, nil
}

// TestREQBrokerPairing004_ConnectNeedsLocalAlias checks that an alias conflict
// with no local alias returns ErrNeedsLocalAlias and commits nothing.
func TestREQBrokerPairing004_ConnectNeedsLocalAlias(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	seedLocalPeer(t, initP, "beta", clk)

	_, _, err, acceptErr := hsPair(t, initP, accP, initTr, "STATIC-KEY", "", nil)
	if !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("Connect error = %v, want ErrNeedsLocalAlias", err)
	}
	if acceptErr == nil {
		t.Fatal("Accept reported success after the initiator abandoned the handshake")
	}
	if got := len(initP.Peers()); got != 1 {
		t.Fatalf("initiator peers = %d, want only the seeded peer", got)
	}
	if got := len(initP.peers.reserved); got != 0 {
		t.Fatalf("%d aliases left reserved after the failure", got)
	}
}

// TestREQBrokerPairing004_ProvidedLocalAliasCompletes checks that a provided
// local alias completes the handshake without renaming the conflicting peer.
func TestREQBrokerPairing004_ProvidedLocalAliasCompletes(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	conflicting := seedLocalPeer(t, initP, "beta", clk)

	peer, _, connectErr, acceptErr := hsPair(t, initP, accP, initTr, "STATIC-KEY", "picked", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	if connectErr != nil {
		t.Fatalf("Connect with a local alias: %v", connectErr)
	}
	if acceptErr != nil {
		t.Fatalf("Accept with a local alias: %v", acceptErr)
	}
	if peer.LocalAlias != "picked" {
		t.Fatalf("LocalAlias = %q, want picked", peer.LocalAlias)
	}
	if got := initP.EffectiveAlias(conflicting); got != "beta" {
		t.Fatalf("the existing peer alias changed to %q", got)
	}
}

func setHSAlias(s *hsSettings, alias string) {
	s.mu.Lock()
	s.alias = alias
	s.mu.Unlock()
}

// TestREQBrokerPairing002_SelfAliasFromSettingsInHandshake checks that each
// handshake reports the current configured alias, not a cached one.
func TestREQBrokerPairing002_SelfAliasFromSettingsInHandshake(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	peer, connectErr, acceptErr := pairWithHello(t, initP, accP, initTr, "STATIC-KEY")
	if connectErr != nil || acceptErr != nil {
		t.Fatalf("first pairing: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	if peer.Alias != "beta" {
		t.Fatalf("peer alias = %q, want the acceptor's configured alias beta", peer.Alias)
	}

	setHSAlias(accS, "gamma")
	peer, connectErr, acceptErr = pairWithHello(t, initP, accP, initTr, "STATIC-KEY")
	if connectErr != nil || acceptErr != nil {
		t.Fatalf("second pairing: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	if peer.Alias != "gamma" {
		t.Fatalf("peer alias after the change = %q, want gamma from settings", peer.Alias)
	}
	if got := initP.EffectiveAlias(accP.Fingerprint()); got != "gamma" {
		t.Fatalf("effective alias after re-pairing = %q, want gamma", got)
	}
}

// TestREQBrokerPairing003_AliasChangeObservedByLink checks that ObserveAlias
// changes the effective alias and resolution.
func TestREQBrokerPairing003_AliasChangeObservedByLink(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if _, connectErr, acceptErr := pairWithHello(t, initP, accP, initTr, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
		t.Fatalf("pairing: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	fp := accP.Fingerprint()
	if got := initP.EffectiveAlias(fp); got != "beta" {
		t.Fatalf("effective alias before the change = %q, want beta", got)
	}

	initP.ObserveAlias(fp, "renamed")
	if got := initP.EffectiveAlias(fp); got != "renamed" {
		t.Fatalf("effective alias after ObserveAlias = %q, want renamed", got)
	}
	if got, ok := initP.ResolveAlias("renamed"); !ok || got != fp {
		t.Fatalf("ResolveAlias(renamed) = %s, %v; want the peer fingerprint", got, ok)
	}
	if _, ok := initP.ResolveAlias("beta"); ok {
		t.Fatal("the old alias still resolves after the change")
	}
}

// TestREQBrokerPairing010_ConnectReturnsPeerAliasAndFingerprint checks the peer
// fields Connect returns for a plain host:port address.
func TestREQBrokerPairing010_ConnectReturnsPeerAliasAndFingerprint(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	peer, shown, connectErr, acceptErr := hsPair(t, initP, accP, initTr, "STATIC-KEY", "", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	if connectErr != nil {
		t.Fatalf("Connect: %v", connectErr)
	}
	if acceptErr != nil {
		t.Fatalf("Accept: %v", acceptErr)
	}
	if shown != "" {
		t.Fatalf("shown = %q, want empty for a plain host:port address", shown)
	}
	if peer.Alias != "beta" {
		t.Fatalf("peer alias = %q, want the acceptor's self-reported alias beta", peer.Alias)
	}
	if peer.Fingerprint != accP.identity.fingerprint() {
		t.Fatalf("peer fingerprint = %s, want the acceptor fingerprint", peer.Fingerprint)
	}
	if !bytes.Equal(peer.PublicKey, accP.identity.publicKey()) {
		t.Fatalf("peer public key = %x, want the acceptor public key", peer.PublicKey)
	}
	if peer.Role != RoleDialer {
		t.Fatalf("peer role = %v, want dialer", peer.Role)
	}
	if !peer.PairedAt.Equal(clk.Now()) {
		t.Fatalf("peer PairedAt = %v, want %v", peer.PairedAt, clk.Now())
	}
}

// TestREQBrokerPairing010_ShownAddressFormDialsInnerAddress checks that the
// shown-address form dials only the address inside the parentheses.
func TestREQBrokerPairing010_ShownAddressFormDialsInnerAddress(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initP := hsNewPairing(t, initS, &hsTransport{}, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	tr := &addrRecordingTransport{hsTransport: &hsTransport{
		raw:      aConn,
		afterRun: func(ctx context.Context, raw PairConn) error { return hsSendHello(raw) },
	}}
	initP.transport = tr

	recB := &hsRecordingConn{PairConn: bConn}
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(recB) }()

	_, shown, err := initP.Connect(context.Background(), "shownName(127.0.0.1:17443)", "STATIC-KEY", "")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := awaitAccept(acceptErr); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if got := tr.dialedAddress(); got != "127.0.0.1:17443" {
		t.Fatalf("PairDial address = %q, want the inner address only", got)
	}
	if shown != "shownName" {
		t.Fatalf("shownHost = %q, want shownName", shown)
	}
}

// TestREQBrokerPairing010_InvalidAddressRejected checks that malformed
// addresses are rejected before any dial.
func TestREQBrokerPairing010_InvalidAddressRejected(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	tr := &hsTransport{}
	p := hsNewPairing(t, s, tr, clk)

	for _, address := range []string{
		"",
		"no-port",
		"host:",
		":17443",
		"host:0",
		"host:70000",
		"host(abc",
	} {
		if _, _, err := p.Connect(context.Background(), address, "K", ""); !errors.Is(err, ErrInvalidAddress) {
			t.Errorf("Connect(%q) error = %v, want ErrInvalidAddress", address, err)
		}
	}
	if tr.dials != 0 {
		t.Fatalf("PairDial was called %d times for invalid addresses", tr.dials)
	}
}

// TestREQBrokerPairing011_WrongKeyLeavesBothTablesEmpty checks that a wrong key
// rejects the handshake and leaves both peer tables empty.
func TestREQBrokerPairing011_WrongKeyLeavesBothTablesEmpty(t *testing.T) {
	clk := newHSClock()
	initP, tr := newInitiatorPairing(t, clk)
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "RIGHT-KEY"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	_, _, connectErr, acceptErr := hsPair(t, initP, accP, tr, "WRONG-KEY", "", nil)
	assertRejectedReason(t, connectErr, ReasonKeyInvalid)
	if acceptErr == nil {
		t.Fatal("Accept reported success for a wrong key")
	}
	if len(initP.Peers()) != 0 {
		t.Fatalf("initiator table = %+v, want empty", initP.Peers())
	}
	if len(accP.Peers()) != 0 {
		t.Fatalf("acceptor table = %+v, want empty", accP.Peers())
	}
}

// TestREQBrokerPairing011_ExpiredAndUsedKeysRejected checks that an expired or
// already used one-time key is rejected and changes no peer table.
func TestREQBrokerPairing011_ExpiredAndUsedKeysRejected(t *testing.T) {
	t.Run("expired", func(t *testing.T) {
		clk := newHSClock()
		initP, tr := newInitiatorPairing(t, clk)
		accS := newHSSettings(t)
		accS.alias = "beta"
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)

		key, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey: %v", err)
		}
		clk.Advance(11 * time.Minute)

		_, _, connectErr, acceptErr := hsPair(t, initP, accP, tr, key, "", nil)
		assertRejectedReason(t, connectErr, ReasonKeyExpired)
		if acceptErr == nil {
			t.Fatal("Accept succeeded with an expired key")
		}
		if len(initP.Peers()) != 0 || len(accP.Peers()) != 0 {
			t.Fatal("an expired key changed a peer table")
		}
	})

	t.Run("used", func(t *testing.T) {
		clk := newHSClock()
		accS := newHSSettings(t)
		accS.alias = "beta"
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)

		key, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey: %v", err)
		}
		first, firstTr := newInitiatorPairing(t, clk)
		if _, connectErr, acceptErr := pairWithHello(t, first, accP, firstTr, key); connectErr != nil || acceptErr != nil {
			t.Fatalf("first use: Connect=%v Accept=%v", connectErr, acceptErr)
		}

		second, secondTr := newInitiatorPairing(t, clk)
		// Send the link Hello so a wrongly accepted reuse fails an assertion
		// instead of blocking Accept until the test timeout.
		_, _, connectErr, acceptErr := hsPair(t, second, accP, secondTr, key, "", func(ctx context.Context, raw PairConn) error {
			return hsSendHello(raw)
		})
		assertRejectedReason(t, connectErr, ReasonKeyInvalid)
		if acceptErr == nil {
			t.Fatal("Accept succeeded for a second use of a one-time key")
		}
		if accP.IsPeer(second.identity.fingerprint()) {
			t.Fatal("the second use of a one-time key was committed")
		}
	})
}

// TestREQBrokerPairing012_MismatchedFingerprintViewsFailLookup checks that a
// different initiator fingerprint matches no key and commits nothing.
func TestREQBrokerPairing012_MismatchedFingerprintViewsFailLookup(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	otherFP, otherPub := hsKey(t)
	bConn.fp = otherFP
	bConn.pub = otherPub
	initTr.raw = aConn
	initTr.afterRun = func(ctx context.Context, raw PairConn) error { return hsSendHello(raw) }

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()
	_, _, connectErr := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", "")
	assertRejectedReason(t, connectErr, ReasonKeyInvalid)
	if err := awaitAccept(acceptErr); err == nil {
		t.Fatal("Accept succeeded despite different fingerprint views")
	}
	if len(initP.Peers()) != 0 || len(accP.Peers()) != 0 {
		t.Fatalf("peer tables changed after a mismatched fingerprint view: initiator=%+v acceptor=%+v", initP.Peers(), accP.Peers())
	}
}

// TestREQBrokerPairing014_OnlyConnectCallsPairDial checks that only Connect
// dials: creating a key and accepting do not.
func TestREQBrokerPairing014_OnlyConnectCallsPairDial(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	s.alias = "beta"
	s.staticKey = "STATIC-KEY"
	tr := &hsTransport{}
	p := hsNewPairing(t, s, tr, clk)

	if _, _, _, err := p.CreateOneTimeKey(); err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	if tr.dials != 0 {
		t.Fatalf("CreateOneTimeKey dialed %d times", tr.dials)
	}

	acceptManual(t, p, "caller", "STATIC-KEY")
	if tr.dials != 0 {
		t.Fatalf("Accept dialed %d times", tr.dials)
	}

	accS := newHSSettings(t)
	accS.alias = "gamma"
	accS.staticKey = "STATIC-KEY"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)
	aConn, bConn := hsConnPairFor(p.identity.fingerprint(), p.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	tr.raw = aConn
	tr.afterRun = func(ctx context.Context, raw PairConn) error { return hsSendHello(raw) }
	recB := &hsRecordingConn{PairConn: bConn}
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(recB) }()
	if _, _, err := p.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", ""); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := awaitAccept(acceptErr); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if tr.dials != 1 {
		t.Fatalf("PairDial calls = %d after Connect, want 1", tr.dials)
	}
}

// TestREQBrokerPairing027_AcceptorNeverCallsPairDial checks that the acceptor
// never dials and commits each accepted peer.
func TestREQBrokerPairing027_AcceptorNeverCallsPairDial(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	s.alias = "beta"
	s.staticKey = "STATIC-KEY"
	tr := &hsTransport{}
	p := hsNewPairing(t, s, tr, clk)

	acceptManual(t, p, "caller-one", "STATIC-KEY")
	acceptManual(t, p, "caller-two", "STATIC-KEY")

	if tr.dials != 0 {
		t.Fatalf("the acceptor called PairDial %d times", tr.dials)
	}
	if got := len(p.Peers()); got != 2 {
		t.Fatalf("acceptor peers = %d, want 2", got)
	}
}

// TestAcceptDiscardsPendingWhenConnectionCloses checks that a connection closed
// before Hello leaves no peer, no reservation and no Drop.
func TestAcceptDiscardsPendingWhenConnectionCloses(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accTr := &hsTransport{}
	accP := hsNewPairing(t, accS, accTr, clk)

	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	initTr.raw = aConn
	initTr.afterRun = func(ctx context.Context, raw PairConn) error {
		return raw.Close()
	}

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()
	if _, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", ""); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := awaitAccept(acceptErr); err == nil {
		t.Fatal("Accept succeeded although the connection closed before the Hello")
	}
	if got := len(accP.Peers()); got != 0 {
		t.Fatalf("acceptor committed a pending entry: %+v", accP.Peers())
	}
	if got := len(accP.peers.reserved); got != 0 {
		t.Fatalf("%d aliases left reserved", got)
	}
	if len(accTr.drops) != 0 {
		t.Fatalf("the acceptor dropped %v, want no Drop on a discarded pending entry", accTr.drops)
	}
}

// TestPairDoneLostLeavesBothUnpaired checks that losing PairDone leaves both
// sides unpaired and consumes the one-time key.
func TestPairDoneLostLeavesBothUnpaired(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}

	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	initTr.raw = aConn
	initTr.runConn = &dropFrameConn{PairConn: aConn, kind: kindPairDone}

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()
	if _, _, connectErr := initP.Connect(context.Background(), "127.0.0.1:17443", key, ""); connectErr == nil {
		t.Fatal("Connect succeeded although PairDone was lost")
	}
	if err := awaitAccept(acceptErr); err == nil {
		t.Fatal("Accept succeeded although PairDone was lost")
	}
	if len(initP.Peers()) != 0 || len(accP.Peers()) != 0 {
		t.Fatalf("peer tables changed after losing PairDone: initiator=%+v acceptor=%+v", initP.Peers(), accP.Peers())
	}
	if got := len(accP.peers.reserved); got != 0 {
		t.Fatalf("%d aliases left reserved", got)
	}
	if got := len(accP.keys.keys); got != 0 {
		t.Fatalf("the one-time key was not consumed: %d entries", got)
	}

	retry, retryTr := newInitiatorPairing(t, clk)
	_, _, retryErr, _ := hsPair(t, retry, accP, retryTr, key, "", nil)
	assertRejectedReason(t, retryErr, ReasonKeyInvalid)
}

// TestConcurrentHandshakesSameKeyOnlyOneSucceeds checks that two handshakes
// holding one key let only the first commit.
func TestConcurrentHandshakesSameKeyOnlyOneSucceeds(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)
	accP.SetOwnAlias("beta")

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}

	type attempt struct {
		fp        Fingerprint
		conn      *hsConn
		acceptErr chan error
		ni, nr    []byte
	}
	start := func(seed int) *attempt {
		fp, pub := hsKey(t)
		aConn, bConn := hsConnPairFor(fp, pub, accP.identity.fingerprint(), accP.identity.publicKey())
		acceptErr := make(chan error, 1)
		go func() { acceptErr <- accP.Accept(bConn) }()
		ni := makeSeq(seed, nonceLength)
		nr := hsManualChallenge(t, aConn, fp, ni, key)
		return &attempt{fp: fp, conn: aConn, acceptErr: acceptErr, ni: ni, nr: nr}
	}

	// Drive both requests to PairChallenge before either confirm, so both
	// lookups pick the same one-time key without relying on goroutine timing.
	first := start(0)
	second := start(64)

	kind, _ := hsManualConfirm(t, first.conn, key, first.ni, first.nr, first.fp, accP.identity.fingerprint(), "dup")
	if kind != kindPairDone {
		t.Fatalf("first reply = %d, want PairDone", kind)
	}
	if err := first.conn.Send(context.Background(), kindHello, nil); err != nil {
		t.Fatalf("first Hello: %v", err)
	}
	if err := awaitAccept(first.acceptErr); err != nil {
		t.Fatalf("first handshake: %v", err)
	}

	kind, payload := hsManualConfirm(t, second.conn, key, second.ni, second.nr, second.fp, accP.identity.fingerprint(), "dup")
	if kind != kindPairReject {
		t.Fatalf("second reply = %d, want PairReject", kind)
	}
	if err := rejectError(payload); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("second reject = %v, want a rejection", err)
	}
	if err := awaitAccept(second.acceptErr); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("second Accept = %v, want a rejection", err)
	}

	if got := len(accP.Peers()); got != 1 {
		t.Fatalf("acceptor peers = %d, want 1", got)
	}
	if got := len(accP.keys.keys); got != 0 {
		t.Fatalf("%d one-time keys survived", got)
	}
}

// TestSameFingerprintHandshakesKeepIndependentReservations checks that
// abandoning one handshake does not release another handshake's reservation for
// the same alias.
func TestSameFingerprintHandshakesKeepIndependentReservations(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)
	accP.SetOwnAlias("beta")

	initiatorFP, initiatorPub := hsKey(t)
	const key = "STATIC-KEY"

	type attempt struct {
		conn      *hsConn
		acceptErr chan error
		ni, nr    []byte
	}
	start := func(seed int) *attempt {
		aConn, bConn := hsConnPairFor(initiatorFP, initiatorPub, accP.identity.fingerprint(), accP.identity.publicKey())
		acceptErr := make(chan error, 1)
		go func() { acceptErr <- accP.Accept(bConn) }()
		ni := makeSeq(seed, nonceLength)
		nr := hsManualChallenge(t, aConn, initiatorFP, ni, key)
		return &attempt{conn: aConn, acceptErr: acceptErr, ni: ni, nr: nr}
	}
	first := start(0)
	second := start(64)

	for _, hs := range []*attempt{first, second} {
		kind, _ := hsManualConfirm(t, hs.conn, key, hs.ni, hs.nr, initiatorFP, accP.identity.fingerprint(), "dup")
		if kind != kindPairDone {
			t.Fatalf("reply = %d, want PairDone", kind)
		}
	}

	_ = first.conn.Close()
	if err := awaitAccept(first.acceptErr); err == nil {
		t.Fatal("the abandoned handshake succeeded")
	}

	if err := second.conn.Send(context.Background(), kindHello, nil); err != nil {
		t.Fatalf("second Hello: %v", err)
	}
	if err := awaitAccept(second.acceptErr); err != nil {
		t.Fatalf("second handshake after the first released: %v", err)
	}
	if !accP.IsPeer(initiatorFP) {
		t.Fatal("the second handshake did not commit")
	}
	accP.peers.mu.Lock()
	left := len(accP.peers.reserved)
	accP.peers.mu.Unlock()
	if left != 0 {
		t.Fatalf("%d reservations left behind", left)
	}
}

// TestInitiatorReReservesAfterEffectiveAliasChanges checks that a commit moves
// the reservation to a new effective alias rather than failing.
func TestInitiatorReReservesAfterEffectiveAliasChanges(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	initP.SetOwnAlias("alpha")
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	accFP := accP.identity.fingerprint()
	accPub := accP.identity.publicKey()
	if _, err := initP.peers.put(peerEntry{
		fingerprint: accFP,
		publicKey:   accPub,
		alias:       "old",
		role:        RoleDialer,
		pairedAt:    clk.Now(),
	}); err != nil {
		t.Fatalf("seed peer: %v", err)
	}

	const key = "STATIC-KEY"
	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accFP, accPub)
	initTr.raw = aConn
	initTr.afterRun = nil

	responder := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
		defer cancel()

		kind, payload, err := bConn.Receive(ctx)
		if err != nil || kind != kindPairRequest {
			responder <- fmt.Errorf("receive PairRequest: kind=%d err=%v", kind, err)
			return
		}
		var request pairingpb.PairRequest
		if err := proto.Unmarshal(payload, &request); err != nil {
			responder <- err
			return
		}
		nr := makeSeq(96, nonceLength)
		challenge := &pairingpb.PairChallenge{
			ResponderNonce: nr,
			ResponderAlias: "beta",
			Proof:          responderProof([]byte(key), request.GetInitiatorNonce(), nr, accFP, initP.identity.fingerprint()),
		}
		payload, err = proto.Marshal(challenge)
		if err != nil {
			responder <- err
			return
		}
		if err := bConn.Send(ctx, kindPairChallenge, payload); err != nil {
			responder <- err
			return
		}
		if kind, _, err = bConn.Receive(ctx); err != nil || kind != kindPairConfirm {
			responder <- fmt.Errorf("receive PairConfirm: kind=%d err=%v", kind, err)
			return
		}

		initP.ObserveAlias(accFP, "alpha")

		payload, err = proto.Marshal(&pairingpb.PairDone{})
		if err != nil {
			responder <- err
			return
		}
		responder <- bConn.Send(ctx, kindPairDone, payload)
	}()

	peer, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", key, "")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := <-responder; err != nil {
		t.Fatalf("responder: %v", err)
	}

	generated := initP.EffectiveAlias(accFP)
	if generated == "" || generated == "beta" || generated == "old" {
		t.Fatalf("effective alias after re-reserving = %q", generated)
	}
	if peer.LocalAlias != generated {
		t.Fatalf("peer local alias = %q, want %q", peer.LocalAlias, generated)
	}
	if peer.Alias != "beta" {
		t.Fatalf("peer self-reported alias = %q, want beta", peer.Alias)
	}
}

// TestRejectAuditUsesVerificationTime checks that an acceptor rejection audit
// carries the time of the failed verification, not the time the PairRequest was
// read.
func TestRejectAuditUsesVerificationTime(t *testing.T) {
	// The advance stays under the 10-second handshake timeout, so the acceptor
	// still reaches the rejection and can send its PairReject frame.
	const advance = 2 * time.Second

	run := func(t *testing.T, badProof, removeKey bool, wantReason RejectReason) {
		clk := newHSClock()
		accS := newHSSettings(t)
		accS.alias = "beta"
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)
		accP.SetOwnAlias("beta")

		pairingKey, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey: %v", err)
		}

		initiatorFP, initiatorPub := hsKey(t)
		aConn, bConn := hsConnPairFor(initiatorFP, initiatorPub, accP.identity.fingerprint(), accP.identity.publicKey())
		acceptErr := make(chan error, 1)
		go func() { acceptErr <- accP.Accept(bConn) }()

		ni := makeSeq(0, nonceLength)
		nr := hsManualChallenge(t, aConn, initiatorFP, ni, pairingKey)
		requestedAt := clk.Now()
		if removeKey {
			accP.keys.mu.Lock()
			accP.keys.keys = nil
			accP.keys.mu.Unlock()
		}
		clk.Advance(advance)

		proof := initiatorProof([]byte(pairingKey), ni, nr, initiatorFP, accP.identity.fingerprint())
		if badProof {
			proof = make([]byte, proofLength)
		}
		if err := hsSendProto(aConn, kindPairConfirm, &pairingpb.PairConfirm{Proof: proof}); err != nil {
			t.Fatalf("send PairConfirm: %v", err)
		}
		if _, _, err := receiveWithTimeout(aConn); err != nil {
			t.Fatalf("receive the reply to PairConfirm: %v", err)
		}
		err = awaitAccept(acceptErr)
		var pairErr *PairingError
		if !errors.As(err, &pairErr) || pairErr.Reason != wantReason {
			t.Fatalf("Accept error = %v, want reason %v", err, wantReason)
		}

		wantTime := requestedAt.Add(advance)
		var failed *config.Event
		for _, e := range accS.recorded() {
			if e.Event == config.EventPairingFailed {
				ev := e
				failed = &ev
			}
		}
		if failed == nil {
			t.Fatal("no pairing_failed audit event")
		}
		if !failed.Time.Equal(wantTime) {
			t.Fatalf("audit time = %v, want the verification time %v", failed.Time, wantTime)
		}
	}

	t.Run("verification failure", func(t *testing.T) {
		run(t, true, false, ReasonVerificationFailed)
	})
	t.Run("key removed before confirm", func(t *testing.T) {
		run(t, false, true, ReasonKeyInvalid)
	})
}

// TestUnverifiedAliasNotAudited checks that a self-reported alias outside the
// alias character set never reaches the audit log on the unverified rejection
// paths.
func TestUnverifiedAliasNotAudited(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	for i, want := range []RejectReason{ReasonKeyInvalid, ReasonTryLater} {
		err := hsRequestWithAlias(t, accP, "bad alias!", "not-a-key")
		var pairErr *PairingError
		if !errors.As(err, &pairErr) || pairErr.Reason != want {
			t.Fatalf("attempt %d error = %v, want reason %v", i, err, want)
		}
	}

	failed := 0
	for _, e := range accS.recorded() {
		if e.Event != config.EventPairingFailed {
			continue
		}
		failed++
		if e.Peer == nil {
			t.Fatal("a failure audit has no peer reference")
		}
		if e.Peer.Alias != "" {
			t.Fatalf("unverified alias %q reached the audit log", e.Peer.Alias)
		}
	}
	if failed != 2 {
		t.Fatalf("failure audits = %d, want 2", failed)
	}
}

func hsManualChallenge(t *testing.T, conn *hsConn, initiatorFP Fingerprint, ni []byte, key string) []byte {
	t.Helper()
	request := &pairingpb.PairRequest{
		InitiatorNonce:  ni,
		LookupTag:       computeHint([]byte(key), ni, initiatorFP),
		ProtocolVersion: conn.version,
	}
	if err := hsSendProto(conn, kindPairRequest, request); err != nil {
		t.Fatalf("send PairRequest: %v", err)
	}
	kind, payload, err := receiveWithTimeout(conn)
	if err != nil {
		t.Fatalf("receive PairChallenge: %v", err)
	}
	if kind != kindPairChallenge {
		t.Fatalf("frame after PairRequest = %d, want PairChallenge", kind)
	}
	var challenge pairingpb.PairChallenge
	if err := proto.Unmarshal(payload, &challenge); err != nil {
		t.Fatalf("decode PairChallenge: %v", err)
	}
	return challenge.GetResponderNonce()
}

func hsManualConfirm(t *testing.T, conn *hsConn, key string, ni, nr []byte, initiatorFP, acceptorFP Fingerprint, alias string) (uint32, []byte) {
	t.Helper()
	proof := initiatorProof([]byte(key), ni, nr, initiatorFP, acceptorFP)
	if err := hsSendProto(conn, kindPairConfirm, &pairingpb.PairConfirm{Proof: proof, InitiatorAlias: alias}); err != nil {
		t.Fatalf("send PairConfirm: %v", err)
	}
	kind, payload, err := receiveWithTimeout(conn)
	if err != nil {
		t.Fatalf("receive the reply to PairConfirm: %v", err)
	}
	return kind, payload
}

// Encoding the alias by field number makes an acceptor that reads it from the request accept the reserved field.
func hsRequestWithAlias(t *testing.T, p *Pairing, alias, key string) error {
	t.Helper()
	fp, pub := hsKey(t)
	aConn, bConn := hsConnPairFor(fp, pub, p.identity.fingerprint(), p.identity.publicKey())
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- p.Accept(bConn) }()

	ni := makeSeq(1, nonceLength)
	request, err := proto.Marshal(&pairingpb.PairRequest{
		InitiatorNonce:  ni,
		LookupTag:       computeHint([]byte(key), ni, fp),
		ProtocolVersion: aConn.version,
	})
	if err != nil {
		t.Fatalf("marshal PairRequest: %v", err)
	}
	if err := hsSendRaw(aConn, kindPairRequest, hsAppendStringField(request, 2, alias)); err != nil {
		t.Fatalf("send PairRequest: %v", err)
	}
	if _, _, err := receiveWithTimeout(aConn); err != nil {
		t.Fatalf("receive reject: %v", err)
	}
	return awaitAccept(acceptErr)
}

// TestAcceptorOverwriteDropsOldLinkAndCandidates checks that re-pairing as
// acceptor clears the candidates and drops the old link.
func TestAcceptorOverwriteDropsOldLinkAndCandidates(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	accTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, accTr, clk)

	if _, connectErr, acceptErr := pairWithHello(t, initP, accP, initTr, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
		t.Fatalf("first handshake: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	fp := initP.identity.fingerprint()

	accP.peers.mu.Lock()
	entry := accP.peers.peers[fp]
	entry.acceptor = append(entry.acceptor, direct("acceptor:1"))
	entry.card = append(entry.card, direct("card:2"))
	entry.pairingAddr = "pairing:3"
	entry.lastConnected = "pairing:3"
	accP.peers.mu.Unlock()

	if _, connectErr, acceptErr := pairWithHello(t, initP, accP, initTr, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
		t.Fatalf("second handshake: Connect=%v Accept=%v", connectErr, acceptErr)
	}

	if got := accP.Role(fp); got != RoleAcceptor {
		t.Fatalf("role after re-pairing = %v, want acceptor", got)
	}
	stored, ok := accP.peers.get(fp)
	if !ok {
		t.Fatal("the re-paired peer is missing")
	}
	if len(stored.acceptor) != 0 || len(stored.card) != 0 || stored.pairingAddr != "" || stored.lastConnected != "" {
		t.Fatalf("acceptor re-pairing kept candidates: %+v", stored)
	}
	if len(accTr.drops) != 1 || accTr.drops[0] != fp {
		t.Fatalf("Drop calls = %v, want exactly one for %s", accTr.drops, fp)
	}
}

// TestAcceptTimeoutClosesWithoutPairRejectFrame checks that a timed-out acceptor
// closes without sending PairReject and commits nothing.
func TestAcceptTimeoutClosesWithoutPairRejectFrame(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	initTr.raw = aConn

	recB := &hsRecordingConn{PairConn: bConn}
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(recB) }()
	if _, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", ""); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	clk.Advance(11 * time.Second)

	err := awaitAccept(acceptErr)
	var pairErr *PairingError
	if !errors.As(err, &pairErr) || pairErr.Reason != ReasonTimeout {
		t.Fatalf("Accept error = %v, want a timeout PairingError", err)
	}
	for _, kind := range recB.sentKinds() {
		if kind == kindPairReject {
			t.Fatal("the acceptor sent a PairReject frame on timeout")
		}
	}
	if got := len(accP.Peers()); got != 0 {
		t.Fatalf("acceptor committed despite the timeout: %+v", accP.Peers())
	}
	if got := len(accP.peers.reserved); got != 0 {
		t.Fatalf("%d aliases left reserved after timeout", got)
	}
	bConn.mu.Lock()
	closed := bConn.closed
	bConn.mu.Unlock()
	if !closed {
		t.Fatal("the acceptor did not close the connection on timeout")
	}
}

type orderClock struct {
	*hsClock
	mu        sync.Mutex
	seq       int
	dialSeen  bool
	dialSeq   int
	timerSeen bool
	timerSeq  int
}

func (c *orderClock) next() int {
	c.seq++
	return c.seq
}

func (c *orderClock) markDial() {
	c.mu.Lock()
	c.dialSeq = c.next()
	c.dialSeen = true
	c.mu.Unlock()
}

func (c *orderClock) NewTimer(d time.Duration) timer {
	c.mu.Lock()
	c.timerSeq = c.next()
	c.timerSeen = true
	c.mu.Unlock()
	return c.hsClock.NewTimer(d)
}

func (c *orderClock) timerStartedAfterDial() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.timerSeen && c.dialSeen && c.timerSeq > c.dialSeq
}

type dialOrderTransport struct {
	conn     PairConn
	clock    *orderClock
	afterRun func(context.Context, PairConn) error
	dials    int
}

func (t *dialOrderTransport) PairDial(ctx context.Context, address string, run func(PairConn) error) error {
	t.dials++
	t.clock.markDial()
	t.clock.Advance(30 * time.Second)
	if err := run(t.conn); err != nil {
		return err
	}
	if t.afterRun != nil {
		return t.afterRun(ctx, t.conn)
	}
	return nil
}

func (t *dialOrderTransport) Drop(Fingerprint) {}

// TestHandshakeTimeoutStartsAfterDial checks that the handshake deadline starts
// only after the dial completes.
func TestHandshakeTimeoutStartsAfterDial(t *testing.T) {
	clk := &orderClock{hsClock: newHSClock()}
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"

	tr := &dialOrderTransport{clock: clk}
	initP := hsNewPairing(t, initS, tr, newHSClock())
	initP.clock = clk
	accP := hsNewPairing(t, accS, &hsTransport{}, newHSClock())

	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	tr.conn = aConn
	tr.afterRun = func(ctx context.Context, raw PairConn) error { return hsSendHello(raw) }

	recB := &hsRecordingConn{PairConn: bConn}
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(recB) }()

	if _, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", ""); err != nil {
		t.Fatalf("Connect after a thirty-second dial: %v", err)
	}
	if err := awaitAccept(acceptErr); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if !clk.timerStartedAfterDial() {
		t.Fatal("the ten-second handshake limit started before the dial completed")
	}
}

// TestConnectRejectsInvalidHostAddress checks that invalid hosts are rejected
// before dialing and valid ones reach PairDial.
func TestConnectRejectsInvalidHostAddress(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	s.alias = "alpha"
	tr := &hsTransport{dialErr: fmt.Errorf("%w: refused", ErrDialFailed)}
	p := hsNewPairing(t, s, tr, clk)

	invalid := []string{
		"bad host:17443",
		"ho st:443",
		"-badhost.example:443",
		"host..example:443",
		"host/name:17443",
	}
	for _, address := range invalid {
		if _, _, err := p.Connect(context.Background(), address, "K", ""); !errors.Is(err, ErrInvalidAddress) {
			t.Errorf("Connect(%q) error = %v, want ErrInvalidAddress", address, err)
		}
	}
	if tr.dials != 0 {
		t.Fatalf("PairDial was called %d times for invalid hosts", tr.dials)
	}

	valid := []string{
		"127.0.0.1:17443",
		"peer.example:17443",
		"[2001:db8::1]:17443",
	}
	for _, address := range valid {
		if _, _, err := p.Connect(context.Background(), address, "K", ""); !errors.Is(err, ErrPeerUnreachable) {
			t.Errorf("Connect(%q) error = %v, want the dial to be attempted", address, err)
		}
	}
	if tr.dials != len(valid) {
		t.Fatalf("PairDial calls = %d for valid hosts, want %d", tr.dials, len(valid))
	}
}
