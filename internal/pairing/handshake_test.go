package pairing

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/hmac"
	"crypto/rand"
	"errors"
	"fmt"
	"io"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/protobuf/encoding/protowire"
	"google.golang.org/protobuf/proto"
)

// testWaitBound is the package-wide fallback that only decides how long a wait that never completes takes to fail; do not shrink it, or load and -race overhead cause flakes.
const testWaitBound = 30 * time.Second

type hsSettings struct {
	mu             sync.Mutex
	dataDir        string
	alias          string
	hostname       string
	advertised     []config.AdvertisedAddr
	listen         string
	staticKey      string
	staticKeyCalls int
	maxFailures    int
	maxAttempts    int
	audits         []config.Event
	aliasFunc      func() (string, error)
	aliasCalls     int
}

func newHSSettings(t *testing.T) *hsSettings {
	t.Helper()
	return &hsSettings{
		dataDir:     t.TempDir(),
		alias:       "self",
		hostname:    "testhost",
		listen:      "127.0.0.1:17443",
		maxFailures: 5,
		maxAttempts: 10,
	}
}

func (s *hsSettings) DataDir() (string, error)  { return s.dataDir, nil }
func (s *hsSettings) Hostname() (string, error) { return s.hostname, nil }
func (s *hsSettings) ListenAddress() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.listen, nil
}
func (s *hsSettings) StaticKey() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.staticKeyCalls++
	return s.staticKey
}

func (s *hsSettings) staticKeyCallCount() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.staticKeyCalls
}
func (s *hsSettings) MaxFailures() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxFailures, nil
}
func (s *hsSettings) MaxAttemptsPerMinute() (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.maxAttempts, nil
}
func (s *hsSettings) Advertised() ([]config.AdvertisedAddr, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]config.AdvertisedAddr(nil), s.advertised...), nil
}
func (s *hsSettings) Alias() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.aliasCalls++
	if s.aliasFunc != nil {
		return s.aliasFunc()
	}
	return s.alias, nil
}
func (s *hsSettings) Audit(e config.Event) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.audits = append(s.audits, e)
}
func (s *hsSettings) recorded() []config.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]config.Event(nil), s.audits...)
}

type hsTimer struct {
	c        chan time.Time
	deadline time.Time
	stopped  bool
	fired    bool
	clock    *hsClock
}

func (t *hsTimer) C() <-chan time.Time { return t.c }
func (t *hsTimer) Stop() bool {
	t.clock.mu.Lock()
	defer t.clock.mu.Unlock()
	if t.fired || t.stopped {
		return false
	}
	t.stopped = true
	return true
}

type hsClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*hsTimer
}

func newHSClock() *hsClock {
	return &hsClock{now: time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)}
}

func (c *hsClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *hsClock) NewTimer(d time.Duration) timer {
	c.mu.Lock()
	defer c.mu.Unlock()
	t := &hsTimer{c: make(chan time.Time, 1), deadline: c.now.Add(d), clock: c}
	c.timers = append(c.timers, t)
	return t
}

func (c *hsClock) Advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	now := c.now
	var due []*hsTimer
	for _, t := range c.timers {
		if !t.stopped && !t.fired && !t.deadline.After(now) {
			t.fired = true
			due = append(due, t)
		}
	}
	c.mu.Unlock()
	for _, t := range due {
		t.c <- now
	}
}

type hsConn struct {
	mu         sync.Mutex
	queue      []hsFrame
	closed     bool
	wake       chan struct{}
	peer       *hsConn
	fp         Fingerprint
	pub        []byte
	remote     string
	version    uint32
	sent       []uint32
	sentCh     chan uint32
	closeCount int
}

type hsFrame struct {
	kind    uint32
	payload []byte
}

func newHSConn() *hsConn {
	return &hsConn{wake: make(chan struct{}, 1), sentCh: make(chan uint32, 32), version: hsProtocolVersion}
}

func hsConnPairFor(aFP Fingerprint, aPub []byte, bFP Fingerprint, bPub []byte) (*hsConn, *hsConn) {
	a := newHSConn()
	b := newHSConn()
	a.peer = b
	b.peer = a
	a.fp = bFP
	a.pub = append([]byte(nil), bPub...)
	b.fp = aFP
	b.pub = append([]byte(nil), aPub...)
	a.remote = "192.0.2.1:40001"
	b.remote = "192.0.2.2:40002"
	return a, b
}

func (c *hsConn) notify() {
	select {
	case c.wake <- struct{}{}:
	default:
	}
}

func (c *hsConn) PeerFingerprint() Fingerprint { return c.fp }
func (c *hsConn) PeerPublicKey() []byte        { return append([]byte(nil), c.pub...) }
func (c *hsConn) RemoteAddr() string           { return c.remote }
func (c *hsConn) ProtocolVersion() uint32      { return c.version }

func (c *hsConn) Send(ctx context.Context, kind uint32, payload []byte) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}
	c.mu.Lock()
	c.sent = append(c.sent, kind)
	c.mu.Unlock()
	select {
	case c.sentCh <- kind:
	default:
	}

	peer := c.peer
	if peer == nil {
		return io.ErrClosedPipe
	}
	peer.mu.Lock()
	if peer.closed {
		peer.mu.Unlock()
		return io.ErrClosedPipe
	}
	peer.queue = append(peer.queue, hsFrame{kind: kind, payload: append([]byte(nil), payload...)})
	peer.mu.Unlock()
	peer.notify()
	return nil
}

func (c *hsConn) Receive(ctx context.Context) (uint32, []byte, error) {
	for {
		c.mu.Lock()
		if len(c.queue) > 0 {
			f := c.queue[0]
			c.queue = c.queue[1:]
			c.mu.Unlock()
			return f.kind, f.payload, nil
		}
		closed := c.closed
		c.mu.Unlock()
		if closed {
			return 0, nil, io.EOF
		}
		select {
		case <-ctx.Done():
			return 0, nil, ctx.Err()
		case <-c.wake:
		}
	}
}

func (c *hsConn) Unread(kind uint32, payload []byte) {
	c.mu.Lock()
	c.queue = append([]hsFrame{{kind: kind, payload: append([]byte(nil), payload...)}}, c.queue...)
	c.mu.Unlock()
	c.notify()
}

func (c *hsConn) Close() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	c.closeCount++
	peer := c.peer
	c.mu.Unlock()
	c.notify()
	if peer != nil {
		peer.mu.Lock()
		peer.closed = true
		peer.mu.Unlock()
		peer.notify()
	}
	return nil
}

type hsRecordingConn struct {
	PairConn
	mu    sync.Mutex
	kinds []uint32
}

func (r *hsRecordingConn) Send(ctx context.Context, kind uint32, payload []byte) error {
	r.mu.Lock()
	r.kinds = append(r.kinds, kind)
	r.mu.Unlock()
	return r.PairConn.Send(ctx, kind, payload)
}

type hsOnSendConn struct {
	PairConn
	kind uint32
	f    func()
	once sync.Once
}

func (c *hsOnSendConn) Send(ctx context.Context, kind uint32, payload []byte) error {
	err := c.PairConn.Send(ctx, kind, payload)
	if err == nil && kind == c.kind {
		c.once.Do(c.f)
	}
	return err
}

func (r *hsRecordingConn) sentKinds() []uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]uint32(nil), r.kinds...)
}

type hsTransport struct {
	mu        sync.Mutex
	dialErr   error
	raw       PairConn
	runConn   PairConn
	beforeRun func()
	afterRun  func(ctx context.Context, raw PairConn) error
	dials     int
	drops     []Fingerprint
}

func (t *hsTransport) PairDial(ctx context.Context, address string, run func(PairConn) error) error {
	t.mu.Lock()
	t.dials++
	dialErr := t.dialErr
	raw := t.raw
	runConn := t.runConn
	beforeRun := t.beforeRun
	afterRun := t.afterRun
	t.mu.Unlock()

	if dialErr != nil {
		return dialErr
	}
	if beforeRun != nil {
		beforeRun()
	}
	conn := runConn
	if conn == nil {
		conn = raw
	}
	if err := run(conn); err != nil {
		return err
	}
	if afterRun != nil {
		return afterRun(ctx, raw)
	}
	return nil
}

func (t *hsTransport) Drop(fp Fingerprint) {
	t.mu.Lock()
	t.drops = append(t.drops, fp)
	t.mu.Unlock()
}

func hsSendHello(raw PairConn) error {
	return raw.Send(context.Background(), kindHello, nil)
}

func hsNewPairing(t *testing.T, s *hsSettings, tr Transport, clk *hsClock) *Pairing {
	t.Helper()
	id, err := generateIdentity(s.dataDir, clk.Now)
	if err != nil {
		t.Fatalf("generateIdentity: %v", err)
	}
	table, err := loadPeerTable(filepath.Join(s.dataDir, "peers.json"), realFileOps{})
	if err != nil {
		t.Fatalf("loadPeerTable: %v", err)
	}
	s.mu.Lock()
	ownAlias := s.alias
	s.mu.Unlock()
	table.setOwnAlias(ownAlias)
	return &Pairing{
		settings:  s,
		transport: tr,
		clock:     clk,
		identity:  id,
		peers:     table,
		keys:      &keyTable{},
		storeOps:  realFileOps{},
		randRead:  rand.Read,
	}
}

func hsKey(t *testing.T) (Fingerprint, []byte) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("GenerateKey: %v", err)
	}
	return FingerprintOf(pub), append([]byte(nil), pub...)
}

func hsPair(t *testing.T, initP, accP *Pairing, initTr *hsTransport, key, localAlias string, afterRun func(context.Context, PairConn) error) (Peer, string, error, error) {
	t.Helper()
	return hsPairFrom(t, initP, accP, initTr, key, localAlias, "192.0.2.2:40002", afterRun)
}

func hsPairFrom(t *testing.T, initP, accP *Pairing, initTr *hsTransport, key, localAlias, acceptorRemote string, afterRun func(context.Context, PairConn) error) (Peer, string, error, error) {
	t.Helper()
	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	bConn.remote = acceptorRemote
	initTr.raw = aConn
	initTr.runConn = nil
	initTr.beforeRun = nil
	initTr.afterRun = afterRun

	recB := &hsRecordingConn{PairConn: bConn}
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(recB) }()

	peer, shown, connectErr := initP.Connect(context.Background(), "127.0.0.1:17443", key, localAlias)
	return peer, shown, connectErr, awaitAccept(acceptErr)
}

// TestHandshakeHMACFormulaVectors checks computeHint, responderProof and
// initiatorProof against formula vectors transcribed independently of them.
func TestHandshakeHMACFormulaVectors(t *testing.T) {
	t.Parallel()

	key := []byte("K7QF-2M9X-HT4C-PWZ3")
	ni := makeSeq(0, 32)
	nr := makeSeq(32, 32)

	var acceptor, initiatorSeen, initiator, acceptorSeen Fingerprint
	for i := range acceptor {
		acceptor[i] = 0x11
		initiatorSeen[i] = 0x22
		initiator[i] = 0x33
		acceptorSeen[i] = 0x44
	}

	// The expected values are computed independently of the implementation, so
	// a formula change on either side fails this test.
	wantHint := designPairHint(key, ni, initiator)
	if got := computeHint(key, ni, initiator); !hmac.Equal(got, wantHint) {
		t.Fatalf("hint = %x, want %x", got, wantHint)
	}
	wantMR := designResponderProof(key, ni, nr, acceptor, initiatorSeen)
	if got := responderProof(key, ni, nr, acceptor, initiatorSeen); !hmac.Equal(got, wantMR) {
		t.Fatalf("responder proof = %x, want %x", got, wantMR)
	}
	wantMI := designInitiatorProof(key, ni, nr, initiator, acceptorSeen)
	if got := initiatorProof(key, ni, nr, initiator, acceptorSeen); !hmac.Equal(got, wantMI) {
		t.Fatalf("initiator proof = %x, want %x", got, wantMI)
	}
}

// TestPairingFrameOrderAndKinds checks the frame order and kinds of a
// successful handshake and that both sides commit.
func TestPairingFrameOrderAndKinds(t *testing.T) {
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

	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	initTr.raw = aConn
	initTr.afterRun = func(ctx context.Context, raw PairConn) error { return hsSendHello(raw) }
	recA := &hsRecordingConn{PairConn: aConn}
	initTr.runConn = recA
	recB := &hsRecordingConn{PairConn: bConn}

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(recB) }()

	peer, shown, err := initP.Connect(context.Background(), "hostA(127.0.0.1:17443)", "STATIC-KEY", "")
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if shown != "hostA" {
		t.Fatalf("shownHost = %q, want hostA", shown)
	}
	if err := awaitAccept(acceptErr); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if got, want := recA.sentKinds(), []uint32{kindPairRequest, kindPairConfirm}; !equalKinds(got, want) {
		t.Fatalf("initiator frames = %v, want %v", got, want)
	}
	if got, want := recB.sentKinds(), []uint32{kindPairChallenge, kindPairDone}; !equalKinds(got, want) {
		t.Fatalf("acceptor frames = %v, want %v", got, want)
	}
	if peer.Role != RoleDialer || peer.Fingerprint != accP.identity.fingerprint() {
		t.Fatalf("peer = %+v, want dialer with the acceptor fingerprint", peer)
	}
	if !accP.IsPeer(initP.identity.fingerprint()) {
		t.Fatal("acceptor did not commit the initiator")
	}
	if !initP.IsPeer(accP.identity.fingerprint()) {
		t.Fatal("initiator did not commit the acceptor")
	}
}

// TestAcceptCommitsOnlyAfterHandoffFrame checks that the acceptor commits only
// after the link Hello and puts that frame back for link.
func TestAcceptCommitsOnlyAfterHandoffFrame(t *testing.T) {
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
	runDone := make(chan struct{})
	releaseHello := make(chan struct{})
	initTr.afterRun = func(ctx context.Context, raw PairConn) error {
		close(runDone)
		<-releaseHello
		return hsSendHello(raw)
	}

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()
	connectErr := make(chan error, 1)
	go func() {
		_, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", "")
		connectErr <- err
	}()

	<-runDone
	if got := accP.Peers(); len(got) != 0 {
		t.Fatalf("acceptor committed before the link Hello: %+v", got)
	}
	close(releaseHello)
	if err := <-connectErr; err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if err := awaitAccept(acceptErr); err != nil {
		t.Fatalf("Accept: %v", err)
	}
	if !accP.IsPeer(initP.identity.fingerprint()) {
		t.Fatal("acceptor did not commit after the Hello")
	}
	kind, _, err := receiveWithTimeout(bConn)
	if err != nil || kind != kindHello {
		t.Fatalf("handoff frame was not unread: kind=%d err=%v", kind, err)
	}
}

// TestAcceptRequiresHelloKind checks that a handoff frame other than the link
// Hello is a protocol error leaving no peer and no reservation.
func TestAcceptRequiresHelloKind(t *testing.T) {
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
	initTr.afterRun = func(ctx context.Context, raw PairConn) error {
		return raw.Send(context.Background(), 99, nil)
	}

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()
	if _, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", ""); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	err := awaitAccept(acceptErr)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Accept error = %v, want ErrProtocol", err)
	}
	if got := accP.Peers(); len(got) != 0 {
		t.Fatalf("acceptor committed on a bad handoff frame: %+v", got)
	}
	if got := len(accP.peers.reserved); got != 0 {
		t.Fatalf("%d aliases left reserved", got)
	}
}

// TestHandshakeTimeoutInitiator checks that the initiator maps its own deadline
// to a timeout PairingError.
func TestHandshakeTimeoutInitiator(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	tr := &hsTransport{}
	p := hsNewPairing(t, s, tr, clk)
	peerFP, peerPub := hsKey(t)
	aConn, _ := hsConnPairFor(p.identity.fingerprint(), p.identity.publicKey(), peerFP, peerPub)
	tr.raw = aConn
	tr.beforeRun = func() { clk.Advance(30 * time.Second) }
	go func() {
		<-aConn.sentCh
		clk.Advance(11 * time.Second)
	}()

	_, _, err := p.Connect(context.Background(), "127.0.0.1:17443", "K", "")
	var pairErr *PairingError
	if !errors.As(err, &pairErr) || pairErr.Reason != ReasonTimeout {
		t.Fatalf("Connect error = %v, want a timeout PairingError", err)
	}
}

// TestHandshakeTimeoutAcceptor checks that the acceptor returns a timeout
// PairingError and commits nothing.
func TestHandshakeTimeoutAcceptor(t *testing.T) {
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

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()
	if _, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", ""); err != nil {
		t.Fatalf("Connect: %v", err)
	}
	clk.Advance(11 * time.Second)
	err := awaitAccept(acceptErr)
	var pairErr *PairingError
	if !errors.As(err, &pairErr) || pairErr.Reason != ReasonTimeout {
		t.Fatalf("Accept error = %v, want a timeout PairingError", err)
	}
	if got := accP.Peers(); len(got) != 0 {
		t.Fatalf("acceptor committed despite the timeout: %+v", got)
	}
	if got := len(accP.peers.reserved); got != 0 {
		t.Fatalf("%d aliases left reserved after timeout", got)
	}
}

// TestFailureLimitInvalidatesKey checks that a one-time key is discarded after
// the failure limit and its next request is rejected as key invalid.
func TestFailureLimitInvalidatesKey(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.maxFailures = 2
	accS.staticKey = ""
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}

	for attempt := 0; attempt < 2; attempt++ {
		if got := hsBadConfirmAttempt(t, accP, key); !errors.Is(got, ErrPairingRejected) {
			t.Fatalf("attempt %d error = %v, want a rejection", attempt, got)
		}
	}

	err = hsRequestAttempt(t, accP, key, ReasonKeyInvalid)
	var pairErr *PairingError
	if !errors.As(err, &pairErr) || pairErr.Reason != ReasonKeyInvalid {
		t.Fatalf("request after failure limit = %v, want key invalid", err)
	}
	if got := accP.Peers(); len(got) != 0 {
		t.Fatalf("peer table changed on failed handshakes: %+v", got)
	}
}

// TestConcurrentAcceptorHandshakesReserveDistinctAliases checks that concurrent
// handshakes reporting the same alias get distinct effective aliases.
func TestConcurrentAcceptorHandshakesReserveDistinctAliases(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	const peers = 2
	type result struct {
		fp  Fingerprint
		err error
	}
	results := make(chan result, peers)
	start := make(chan struct{})
	for i := 0; i < peers; i++ {
		fp, pub := hsKey(t)
		aConn, bConn := hsConnPairFor(fp, pub, accP.identity.fingerprint(), accP.identity.publicKey())
		go func(initiatorFP Fingerprint) {
			<-start
			results <- result{fp: initiatorFP, err: hsCompleteAsInitiator(t, accP, aConn, bConn, initiatorFP, "dup", "STATIC-KEY")}
		}(fp)
	}
	close(start)

	seen := make(map[string]bool)
	for i := 0; i < peers; i++ {
		res := <-results
		if res.err != nil {
			t.Fatalf("handshake %d: %v", i, res.err)
		}
		alias := accP.EffectiveAlias(res.fp)
		if alias == "" {
			t.Fatalf("handshake %d has no effective alias", i)
		}
		if seen[alias] {
			t.Fatalf("two concurrent peers got the same effective alias %q", alias)
		}
		seen[alias] = true
	}
}

// TestConnectNeedsLocalAliasThenProvidedAliasCompletes checks that an alias
// conflict needs a local alias and that a provided alias completes without
// renaming the existing peer.
func TestConnectNeedsLocalAliasThenProvidedAliasCompletes(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	dupFP, dupPub := hsKey(t)
	if _, err := initP.peers.put(peerEntry{
		fingerprint: dupFP, publicKey: dupPub, alias: "beta", role: RoleDialer, pairedAt: clk.Now(),
	}); err != nil {
		t.Fatalf("seed peer table: %v", err)
	}

	_, _, err, _ := hsPair(t, initP, accP, initTr, "STATIC-KEY", "", nil)
	if !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("Connect without a local alias = %v, want ErrNeedsLocalAlias", err)
	}

	peer, _, err, acceptErr := hsPair(t, initP, accP, initTr, "STATIC-KEY", "picked", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	if err != nil {
		t.Fatalf("Connect with a local alias: %v", err)
	}
	if acceptErr != nil {
		t.Fatalf("Accept with a local alias: %v", acceptErr)
	}
	if peer.LocalAlias != "picked" {
		t.Fatalf("LocalAlias = %q, want picked", peer.LocalAlias)
	}
	if got := initP.EffectiveAlias(dupFP); got != "beta" {
		t.Fatalf("existing peer alias changed to %q", got)
	}
}

// TestConnectDialErrorMapping checks how Connect maps dial failures to sentinel
// and pairing errors.
func TestConnectDialErrorMapping(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	tr := &hsTransport{}
	p := hsNewPairing(t, s, tr, clk)
	ctx := context.Background()

	cases := []struct {
		name string
		dial error
		want error
		time bool
	}{
		{name: "dial failed", dial: fmt.Errorf("%w: refused", ErrDialFailed), want: ErrPeerUnreachable},
		{name: "deadline", dial: context.DeadlineExceeded, time: true},
		{name: "store", dial: fmt.Errorf("%w: disk", ErrStore), want: ErrStore},
		{name: "canceled", dial: context.Canceled, want: context.Canceled},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			tr.dialErr = tc.dial
			_, _, err := p.Connect(ctx, "127.0.0.1:17443", "K", "")
			if tc.time {
				var pairErr *PairingError
				if !errors.As(err, &pairErr) || pairErr.Reason != ReasonTimeout {
					t.Fatalf("error = %v, want timeout PairingError", err)
				}
				return
			}
			if !errors.Is(err, tc.want) {
				t.Fatalf("error = %v, want %v", err, tc.want)
			}
		})
	}
}

// TestConnectCanceledDialIsCanceled checks that a canceled dial returns
// context.Canceled, not ErrPeerUnreachable.
func TestConnectCanceledDialIsCanceled(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	tr := &hsTransport{dialErr: fmt.Errorf("dial refused: %w: %w", ErrDialFailed, context.Canceled)}
	p := hsNewPairing(t, s, tr, clk)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, _, err := p.Connect(ctx, "127.0.0.1:17443", "K", "")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("error = %v, want context.Canceled", err)
	}
	if errors.Is(err, ErrPeerUnreachable) {
		t.Fatalf("error = %v, must not be ErrPeerUnreachable", err)
	}
}

// TestMismatchedFingerprintViewsFailLookup checks that a different initiator
// fingerprint on the acceptor side matches no key and commits nothing.
func TestMismatchedFingerprintViewsFailLookup(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)
	otherFP, otherPub := hsKey(t)

	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	bConn.fp = otherFP
	bConn.pub = otherPub
	initTr.raw = aConn
	initTr.afterRun = func(ctx context.Context, raw PairConn) error { return hsSendHello(raw) }

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()
	_, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", "")
	var pairErr *PairingError
	if !errors.As(err, &pairErr) || pairErr.Reason != ReasonKeyInvalid {
		t.Fatalf("Connect error = %v, want a key-invalid rejection", err)
	}
	if err := awaitAccept(acceptErr); err == nil {
		t.Fatal("acceptor reported success after a mismatched fingerprint view")
	}
	if got := initP.Peers(); len(got) != 0 {
		t.Fatalf("initiator committed after a failed proof: %+v", got)
	}
	if got := accP.Peers(); len(got) != 0 {
		t.Fatalf("acceptor committed after a failed proof: %+v", got)
	}
}

// TestLookupTagReplayDoesNotConsumeOneTimeKey checks that a forwarded lookup tag
// matches no key and cannot exhaust the key's failure count.
func TestLookupTagReplayDoesNotConsumeOneTimeKey(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.maxFailures = 3
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}

	initP, tr := newInitiatorPairing(t, clk)

	ni := makeSeq(9, nonceLength)
	hint := computeHint([]byte(key), ni, initP.identity.fingerprint())

	for i := 0; i < accS.maxFailures; i++ {
		mFP, mPub := hsKey(t)
		aConn, bConn := hsConnPairFor(mFP, mPub, accP.identity.fingerprint(), accP.identity.publicKey())
		acceptErr := make(chan error, 1)
		go func() { acceptErr <- accP.Accept(bConn) }()

		request := &pairingpb.PairRequest{InitiatorNonce: ni, LookupTag: hint, ProtocolVersion: aConn.version}
		if err := hsSendProto(aConn, kindPairRequest, request); err != nil {
			t.Fatalf("replay %d: send PairRequest: %v", i, err)
		}
		kind, payload, recvErr := receiveWithTimeout(aConn)
		if recvErr != nil {
			t.Fatalf("replay %d: receive reply: %v", i, recvErr)
		}
		switch kind {
		case kindPairChallenge:
			t.Errorf("replay %d received a PairChallenge for the forwarded lookup tag", i)
			if err := hsSendProto(aConn, kindPairConfirm, &pairingpb.PairConfirm{Proof: make([]byte, proofLength)}); err != nil {
				t.Fatalf("replay %d: send bad PairConfirm: %v", i, err)
			}
			if _, _, err := receiveWithTimeout(aConn); err != nil {
				t.Fatalf("replay %d: receive reply to PairConfirm: %v", i, err)
			}
		case kindPairReject:
			var reject pairingpb.PairReject
			if err := proto.Unmarshal(payload, &reject); err != nil {
				t.Fatalf("replay %d: decode PairReject: %v", i, err)
			}
			if reason, ok := rejectReasonFromPB(reject.GetReason()); !ok || reason != ReasonKeyInvalid {
				t.Fatalf("replay %d: reject reason = %v, want key invalid", i, reject.GetReason())
			}
		default:
			t.Fatalf("replay %d: frame kind = %d, want PairReject", i, kind)
		}
		_ = awaitAccept(acceptErr)
	}

	peer, _, connectErr, acceptErr := hsPairFrom(t, initP, accP, tr, key, "", "198.51.100.9:9999", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	if connectErr != nil {
		t.Fatalf("the key's owner could not pair after the replays: %v", connectErr)
	}
	if acceptErr != nil {
		t.Fatalf("Accept for the key's owner = %v, want success", acceptErr)
	}
	if peer.Fingerprint != accP.identity.fingerprint() {
		t.Fatalf("paired peer fingerprint = %x, want the acceptor", peer.Fingerprint)
	}
}

// TestLookupTagReplayAgainstExpiredRecordIsKeyInvalid checks that a forwarded lookup tag that points at an expired key but is bound to another fingerprint matches no record and is rejected as key invalid, not key expired.
func TestLookupTagReplayAgainstExpiredRecordIsKeyInvalid(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	ownerFP, _ := hsKey(t)

	ni := makeSeq(11, nonceLength)
	hint := computeHint([]byte(key), ni, ownerFP)
	clk.Advance(11 * time.Minute)

	mFP, mPub := hsKey(t)
	aConn, bConn := hsConnPairFor(mFP, mPub, accP.identity.fingerprint(), accP.identity.publicKey())
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()

	if err := hsSendProto(aConn, kindPairRequest, &pairingpb.PairRequest{InitiatorNonce: ni, LookupTag: hint, ProtocolVersion: aConn.version}); err != nil {
		t.Fatalf("send forwarded PairRequest: %v", err)
	}
	kind, payload, recvErr := receiveWithTimeout(aConn)
	if recvErr != nil {
		t.Fatalf("receive reply: %v", recvErr)
	}
	if kind == kindPairChallenge {
		t.Fatal("a forwarded lookup tag received a PairChallenge")
	}
	if kind != kindPairReject {
		t.Fatalf("frame kind = %d, want PairReject", kind)
	}
	var reject pairingpb.PairReject
	if err := proto.Unmarshal(payload, &reject); err != nil {
		t.Fatalf("decode PairReject: %v", err)
	}
	if reason, ok := rejectReasonFromPB(reject.GetReason()); !ok || reason != ReasonKeyInvalid {
		t.Fatalf("reject reason = %v, want key invalid", reject.GetReason())
	}
	_ = awaitAccept(acceptErr)
}

// TestRateLimitedFailureUsesSourceMergeKey checks that a rate-limited rejection
// audits with the source prefix as its merge key.
func TestRateLimitedFailureUsesSourceMergeKey(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if err := hsRequestAttempt(t, accP, "not-a-key", ReasonKeyInvalid); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("first request error = %v", err)
	}
	err := hsRequestAttempt(t, accP, "not-a-key", ReasonTryLater)
	var pairErr *PairingError
	if !errors.As(err, &pairErr) || pairErr.Reason != ReasonTryLater {
		t.Fatalf("second request error = %v, want try later", err)
	}

	var mergeKey string
	for _, e := range accS.recorded() {
		if e.Event == config.EventPairingFailed && e.Detail == auditReasonTryLater {
			mergeKey = e.MergeKey
		}
	}
	if mergeKey != "source:192.0.2.2/32" {
		t.Fatalf("merge key = %q, want source:192.0.2.2/32", mergeKey)
	}
}

// TestAttemptCounterResetsEachMinute checks that the attempt counter starts over
// when the clock crosses a minute boundary.
func TestAttemptCounterResetsEachMinute(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if err := hsRequestAttempt(t, accP, "bad-one", ReasonKeyInvalid); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("first request error = %v", err)
	}
	if err := hsRequestAttempt(t, accP, "bad-two", ReasonTryLater); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("second request error = %v", err)
	}

	clk.Advance(time.Minute)
	if err := hsRequestAttempt(t, accP, "bad-three", ReasonKeyInvalid); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("request after the minute boundary error = %v, want a key-invalid rejection", err)
	}
}

// TestAttemptLimitIsPerSource checks that one source exhausting its allowance
// does not rate-limit another source.
func TestAttemptLimitIsPerSource(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if err := hsRequestAttemptFrom(t, accP, "not-the-static-key", ReasonKeyInvalid, "198.51.100.1:1000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("source X first request error = %v", err)
	}
	if err := hsRequestAttemptFrom(t, accP, "not-the-static-key", ReasonTryLater, "198.51.100.1:1001"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("source X second request error = %v", err)
	}

	initP, tr := newInitiatorPairing(t, clk)
	peer, _, connectErr, acceptErr := hsPairFrom(t, initP, accP, tr, "STATIC-KEY", "", "198.51.100.2:2000", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	if connectErr != nil {
		t.Fatalf("source Y Connect with the static key = %v, want success", connectErr)
	}
	if acceptErr != nil {
		t.Fatalf("source Y Accept = %v, want success", acceptErr)
	}
	if peer.Fingerprint != accP.identity.fingerprint() {
		t.Fatalf("paired peer fingerprint = %x, want the acceptor", peer.Fingerprint)
	}
}

// TestAttemptLimitGroupsIPv6ByPrefix checks that two addresses in one /64 share
// an allowance and a different /64 has its own.
func TestAttemptLimitGroupsIPv6ByPrefix(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonKeyInvalid, "[2001:db8:1:2::1]:1000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("first address in the /64 error = %v", err)
	}
	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonTryLater, "[2001:db8:1:2::abcd]:2000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("second address in the same /64 error = %v", err)
	}
	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonKeyInvalid, "[2001:db8:1:3::1]:3000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("address in another /64 error = %v", err)
	}
}

// TestAttemptLimitSharesOneAllowanceAmongNonIPSources checks that sources with
// no parsable IP share one unknown-source allowance.
func TestAttemptLimitSharesOneAllowanceAmongNonIPSources(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonKeyInvalid, "not-an-ip:1000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("first unknown source error = %v", err)
	}
	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonTryLater, "another-name:2000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("second unknown source error = %v, want the shared allowance to rate-limit it", err)
	}
}

// TestAttemptLimitTreatsMappedIPv4AsIPv4 checks that an IPv4-mapped IPv6
// address shares the plain IPv4 address's allowance.
func TestAttemptLimitTreatsMappedIPv4AsIPv4(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonKeyInvalid, "198.51.100.7:1000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("plain IPv4 error = %v", err)
	}
	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonTryLater, "[::ffff:198.51.100.7]:2000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("mapped IPv4 error = %v, want it to share the plain address's allowance", err)
	}
}

// TestAttemptLimitHasGlobalCap checks the second limit, ten times the
// per-source allowance across all sources in one minute.
func TestAttemptLimitHasGlobalCap(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	for i := 0; i < 10; i++ {
		remote := fmt.Sprintf("198.51.100.%d:1000", i+1)
		if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonKeyInvalid, remote); !errors.Is(err, ErrPairingRejected) {
			t.Fatalf("source %d below the global cap error = %v", i+1, err)
		}
	}
	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonTryLater, "198.51.100.200:1000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("request above the global cap error = %v", err)
	}

	clk.Advance(time.Minute)
	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonKeyInvalid, "198.51.100.200:1001"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("request after the minute boundary error = %v", err)
	}
}

// TestRateLimitedAuditMergesSeparateIPv6Prefixes checks that the audit merge key
// is the /64 prefix, so two /64s get distinct keys.
func TestRateLimitedAuditMergesSeparateIPv6Prefixes(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	for _, remote := range []string{"[2001:db8:a::1]:1000", "[2001:db8:b::1]:1000"} {
		if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonKeyInvalid, remote); !errors.Is(err, ErrPairingRejected) {
			t.Fatalf("first request from %s error = %v", remote, err)
		}
		if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonTryLater, remote); !errors.Is(err, ErrPairingRejected) {
			t.Fatalf("rate-limited request from %s error = %v", remote, err)
		}
	}

	got := make(map[string]bool)
	for _, e := range accS.recorded() {
		if e.Event == config.EventPairingFailed && e.Detail == auditReasonTryLater {
			got[e.MergeKey] = true
		}
	}
	for _, want := range []string{"source:2001:db8:a::/64", "source:2001:db8:b::/64"} {
		if !got[want] {
			t.Fatalf("merge keys = %v, want %q", got, want)
		}
	}
	if len(got) != 2 {
		t.Fatalf("merge keys = %v, want exactly two", got)
	}
}

// TestStaticKeyUnreadBeforeRateChecks checks that a one-time key hit, an expired record hit and a rate-limited request leave the static key unread, while a counted request reads it once.
func TestStaticKeyUnreadBeforeRateChecks(t *testing.T) {
	t.Run("one-time key match", func(t *testing.T) {
		clk := newHSClock()
		accS := newHSSettings(t)
		accS.alias = "beta"
		accS.staticKey = "STATIC-KEY"
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)

		key, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey: %v", err)
		}
		fp, pub := hsKey(t)
		aConn, bConn := hsConnPairFor(fp, pub, accP.identity.fingerprint(), accP.identity.publicKey())
		bConn.remote = "198.51.100.10:1000"
		acceptErr := make(chan error, 1)
		go func() { acceptErr <- accP.Accept(bConn) }()

		ni := make([]byte, nonceLength)
		if _, err := rand.Read(ni); err != nil {
			t.Fatalf("random nonce: %v", err)
		}
		request := &pairingpb.PairRequest{
			InitiatorNonce:  ni,
			LookupTag:       computeHint([]byte(key), ni, fp),
			ProtocolVersion: aConn.version,
		}
		if err := hsSendProto(aConn, kindPairRequest, request); err != nil {
			t.Fatalf("send PairRequest: %v", err)
		}
		if kind, _, err := receiveWithTimeout(aConn); err != nil || kind != kindPairChallenge {
			t.Fatalf("receive challenge: kind=%d err=%v", kind, err)
		}
		_ = aConn.Close()
		_ = awaitAccept(acceptErr)

		if got := accS.staticKeyCallCount(); got != 0 {
			t.Fatalf("a one-time key match read the static key %d times, want 0", got)
		}
	})

	t.Run("expired record hit", func(t *testing.T) {
		clk := newHSClock()
		accS := newHSSettings(t)
		accS.alias = "beta"
		accS.staticKey = "STATIC-KEY"
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)

		key, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey: %v", err)
		}
		clk.Advance(11 * time.Minute)
		if err := hsRequestAttemptFrom(t, accP, key, ReasonKeyExpired, "198.51.100.11:1000"); !errors.Is(err, ErrPairingRejected) {
			t.Fatalf("expired record request error = %v", err)
		}
		if got := accS.staticKeyCallCount(); got != 0 {
			t.Fatalf("an expired record hit read the static key %d times, want 0", got)
		}
	})

	t.Run("rate-limited request", func(t *testing.T) {
		clk := newHSClock()
		accS := newHSSettings(t)
		accS.alias = "beta"
		accS.staticKey = "STATIC-KEY"
		accS.maxAttempts = 1
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)

		if err := hsRequestAttemptFrom(t, accP, "not-the-static-key", ReasonKeyInvalid, "198.51.100.12:1000"); !errors.Is(err, ErrPairingRejected) {
			t.Fatalf("counted request error = %v", err)
		}
		before := accS.staticKeyCallCount()
		if before != 1 {
			t.Fatalf("the counted request read the static key %d times, want 1", before)
		}
		if err := hsRequestAttemptFrom(t, accP, "not-the-static-key", ReasonTryLater, "198.51.100.12:1001"); !errors.Is(err, ErrPairingRejected) {
			t.Fatalf("rate-limited request error = %v", err)
		}
		if got := accS.staticKeyCallCount(); got != before {
			t.Fatalf("a rate-limited request read the static key %d times, want %d", got, before)
		}
	})
}

// TestUnknownRejectReasonIsProtocolError checks that an unassigned PairReject
// reason is a protocol error.
func TestUnknownRejectReasonIsProtocolError(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	tr := &hsTransport{}
	p := hsNewPairing(t, s, tr, clk)
	aConn, bConn := hsConnPairFor(p.identity.fingerprint(), p.identity.publicKey(), p.identity.fingerprint(), p.identity.publicKey())
	tr.raw = aConn
	go func() {
		<-aConn.sentCh
		payload, _ := proto.Marshal(&pairingpb.PairReject{Reason: pairingpb.PairingRejectReason(99)})
		_ = bConn.Send(context.Background(), kindPairReject, payload)
	}()

	_, _, err := p.Connect(context.Background(), "127.0.0.1:17443", "K", "")
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("Connect error = %v, want ErrProtocol", err)
	}
}

// TestAliasChangedAtCommitLeavesNoPeer checks that a rename between reservation
// and commit refuses the peer and leaves no reservation.
func TestAliasChangedAtCommitLeavesNoPeer(t *testing.T) {
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
	initTr.runConn = &hsOnSendConn{
		PairConn: aConn,
		kind:     kindPairConfirm,
		f:        func() { initP.SetOwnAlias("beta") },
	}
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(&hsRecordingConn{PairConn: bConn}) }()

	_, _, err := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", "")
	if !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("Connect error = %v, want ErrNeedsLocalAlias", err)
	}
	if got := initP.Peers(); len(got) != 0 {
		t.Fatalf("initiator committed with a conflicting own alias: %+v", got)
	}
	if got := len(initP.peers.reserved); got != 0 {
		t.Fatalf("%d aliases left reserved", got)
	}
	if acceptErrVal := awaitAccept(acceptErr); acceptErrVal == nil {
		t.Fatal("acceptor completed a handshake the initiator refused to commit")
	}
}

// TestStaticKeyReReadAtConfirm checks that deleting or rotating the static key
// before PairConfirm verification rejects the stale proof.
func TestStaticKeyReReadAtConfirm(t *testing.T) {
	const oldKey = "OLD-STATIC"

	cases := []struct {
		name string
		swap func(*hsSettings)
	}{
		{name: "deleted", swap: func(s *hsSettings) { setStaticKey(s, "") }},
		{name: "rotated", swap: func(s *hsSettings) { setStaticKey(s, "NEW-STATIC") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			clk := newHSClock()
			accS := newHSSettings(t)
			accS.alias = "beta"
			accS.staticKey = oldKey
			accP := hsNewPairing(t, accS, &hsTransport{}, clk)

			initiatorFP, initiatorPub := hsKey(t)
			aConn, bConn := hsConnPairFor(initiatorFP, initiatorPub, accP.identity.fingerprint(), accP.identity.publicKey())
			// Unblocks Accept if a wrong implementation stops waiting for a
			// frame this test never sends.
			defer func() { _ = aConn.Close() }()

			acceptErr := make(chan error, 1)
			go func() { acceptErr <- accP.Accept(bConn) }()

			ni := make([]byte, nonceLength)
			if _, err := rand.Read(ni); err != nil {
				t.Fatalf("random nonce: %v", err)
			}
			request := &pairingpb.PairRequest{
				InitiatorNonce:  ni,
				LookupTag:       computeHint([]byte(oldKey), ni, initiatorFP),
				ProtocolVersion: aConn.version,
			}
			if err := hsSendProto(aConn, kindPairRequest, request); err != nil {
				t.Fatalf("send PairRequest: %v", err)
			}
			kind, payload, err := receiveWithTimeout(aConn)
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

			tc.swap(accS)
			proof := initiatorProof([]byte(oldKey), ni, challenge.GetResponderNonce(), initiatorFP, accP.identity.fingerprint())
			if err := hsSendProto(aConn, kindPairConfirm, &pairingpb.PairConfirm{Proof: proof, InitiatorAlias: "intruder"}); err != nil {
				t.Fatalf("send PairConfirm: %v", err)
			}

			kind, payload, err = receiveWithTimeout(aConn)
			if err != nil {
				t.Fatalf("receive the reply to PairConfirm: %v", err)
			}
			if kind != kindPairReject {
				t.Fatalf("acceptor replied with frame %d, want PairReject: a stale static key completed the handshake", kind)
			}
			var reject pairingpb.PairReject
			if err := proto.Unmarshal(payload, &reject); err != nil {
				t.Fatalf("decode PairReject: %v", err)
			}
			if reason, ok := rejectReasonFromPB(reject.GetReason()); !ok || reason != ReasonVerificationFailed {
				t.Fatalf("reject reason = %v, want verification failed", reject.GetReason())
			}

			var pairErr *PairingError
			if err := awaitAccept(acceptErr); !errors.As(err, &pairErr) || pairErr.Reason != ReasonVerificationFailed {
				t.Fatalf("Accept error = %v, want a verification-failure PairingError", err)
			}
			if got := accP.Peers(); len(got) != 0 {
				t.Fatalf("acceptor committed after the key changed: %+v", got)
			}
			if got := len(accP.peers.reserved); got != 0 {
				t.Fatalf("%d aliases left reserved after the key changed", got)
			}
		})
	}
}

// TestInitiatorWithholdsAliasUntilResponderVerified checks that a failed
// responder proof stops the initiator before PairConfirm and its alias.
func TestInitiatorWithholdsAliasUntilResponderVerified(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "initiator-secret"
	tr := &hsTransport{}
	p := hsNewPairing(t, initS, tr, clk)

	acceptorFP, acceptorPub := hsKey(t)
	aConn, bConn := hsConnPairFor(p.identity.fingerprint(), p.identity.publicKey(), acceptorFP, acceptorPub)
	tr.raw = aConn

	connectErr := make(chan error, 1)
	go func() {
		_, _, err := p.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", "")
		connectErr <- err
	}()

	kind, payload, err := receiveWithTimeout(bConn)
	if err != nil || kind != kindPairRequest {
		t.Fatalf("fake responder received kind=%d err=%v, want PairRequest", kind, err)
	}
	if hsHasField(payload, 2) {
		t.Fatalf("PairRequest carries field 2, so the alias is revealed before the responder is verified: %x", payload)
	}
	if bytes.Contains(payload, []byte(initS.alias)) {
		t.Fatalf("PairRequest contains the initiator alias %q: %x", initS.alias, payload)
	}

	nr := make([]byte, nonceLength)
	if _, err := rand.Read(nr); err != nil {
		t.Fatalf("random nonce: %v", err)
	}
	challenge := &pairingpb.PairChallenge{
		ResponderNonce: nr,
		ResponderAlias: "acceptor",
		Proof:          make([]byte, proofLength),
	}
	if err := hsSendProto(bConn, kindPairChallenge, challenge); err != nil {
		t.Fatalf("send PairChallenge: %v", err)
	}

	select {
	case err := <-connectErr:
		var pairErr *PairingError
		if !errors.As(err, &pairErr) || pairErr.Reason != ReasonVerificationFailed {
			t.Fatalf("Connect error = %v, want a verification-failure PairingError", err)
		}
	case <-time.After(testWaitBound):
		t.Fatal("Connect did not return within the test wait bound")
	}
	for _, sent := range hsSentKinds(aConn) {
		if sent == kindPairConfirm {
			t.Fatal("the initiator sent PairConfirm, and with it its alias, before verifying the responder")
		}
	}
}

// TestAcceptorReadsAliasFromConfirmedPairConfirm checks that the acceptor
// records the alias from PairConfirm and audits it.
func TestAcceptorReadsAliasFromConfirmedPairConfirm(t *testing.T) {
	const key = "STATIC-KEY"
	const alias = "gamma"

	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = key
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	initiatorFP, initiatorPub := hsKey(t)
	aConn, bConn := hsConnPairFor(initiatorFP, initiatorPub, accP.identity.fingerprint(), accP.identity.publicKey())
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()

	ni := makeSeq(0, nonceLength)
	request, err := proto.Marshal(&pairingpb.PairRequest{
		InitiatorNonce:  ni,
		LookupTag:       computeHint([]byte(key), ni, initiatorFP),
		ProtocolVersion: aConn.version,
	})
	if err != nil {
		t.Fatalf("marshal PairRequest: %v", err)
	}
	if err := hsSendRaw(aConn, kindPairRequest, request); err != nil {
		t.Fatalf("send PairRequest: %v", err)
	}
	kind, payload, err := receiveWithTimeout(aConn)
	if err != nil || kind != kindPairChallenge {
		t.Fatalf("frame after PairRequest: kind=%d err=%v, want PairChallenge", kind, err)
	}
	var challenge pairingpb.PairChallenge
	if err := proto.Unmarshal(payload, &challenge); err != nil {
		t.Fatalf("decode PairChallenge: %v", err)
	}

	proof := initiatorProof([]byte(key), ni, challenge.GetResponderNonce(), initiatorFP, accP.identity.fingerprint())
	confirm, err := proto.Marshal(&pairingpb.PairConfirm{Proof: proof})
	if err != nil {
		t.Fatalf("marshal PairConfirm: %v", err)
	}
	if err := hsSendRaw(aConn, kindPairConfirm, hsAppendStringField(confirm, 2, alias)); err != nil {
		t.Fatalf("send PairConfirm: %v", err)
	}
	kind, _, err = receiveWithTimeout(aConn)
	if err != nil || kind != kindPairDone {
		t.Fatalf("frame after PairConfirm: kind=%d err=%v, want PairDone", kind, err)
	}
	if err := aConn.Send(context.Background(), kindHello, nil); err != nil {
		t.Fatalf("send Hello: %v", err)
	}
	if err := awaitAccept(acceptErr); err != nil {
		t.Fatalf("Accept: %v", err)
	}

	peers := accP.Peers()
	if len(peers) != 1 {
		t.Fatalf("acceptor peers = %d, want 1", len(peers))
	}
	if peers[0].Alias != alias {
		t.Fatalf("the acceptor recorded the self-reported alias %q, want %q from PairConfirm", peers[0].Alias, alias)
	}

	var succeeded bool
	for _, e := range accS.recorded() {
		if e.Event == config.EventPairingSucceeded {
			succeeded = true
			if e.Peer == nil || e.Peer.Alias != alias {
				t.Fatalf("pairing_succeeded peer = %+v, want alias %q", e.Peer, alias)
			}
		}
	}
	if !succeeded {
		t.Fatal("no pairing_succeeded audit event")
	}
}

// TestAcceptorFailureAuditsOmitInitiatorAlias checks that failures before the
// initiator proof verifies audit the fingerprint alone.
func TestAcceptorFailureAuditsOmitInitiatorAlias(t *testing.T) {
	const key = "STATIC-KEY"
	const alias = "initiator-secret"

	run := func(t *testing.T, fail func(*hsClock, *hsConn, *hsConn)) (config.Event, RejectReason) {
		t.Helper()
		clk := newHSClock()
		accS := newHSSettings(t)
		accS.alias = "beta"
		accS.staticKey = key
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)

		initiatorFP, initiatorPub := hsKey(t)
		aConn, bConn := hsConnPairFor(initiatorFP, initiatorPub, accP.identity.fingerprint(), accP.identity.publicKey())
		acceptErr := make(chan error, 1)
		go func() { acceptErr <- accP.Accept(bConn) }()

		ni := makeSeq(0, nonceLength)
		request, err := proto.Marshal(&pairingpb.PairRequest{
			InitiatorNonce:  ni,
			LookupTag:       computeHint([]byte(key), ni, initiatorFP),
			ProtocolVersion: aConn.version,
		})
		if err != nil {
			t.Fatalf("marshal PairRequest: %v", err)
		}
		if err := hsSendRaw(aConn, kindPairRequest, hsAppendStringField(request, 2, alias)); err != nil {
			t.Fatalf("send PairRequest: %v", err)
		}
		kind, payload, err := receiveWithTimeout(aConn)
		if err != nil || kind != kindPairChallenge {
			t.Fatalf("frame after PairRequest: kind=%d err=%v, want PairChallenge", kind, err)
		}
		var challenge pairingpb.PairChallenge
		if err := proto.Unmarshal(payload, &challenge); err != nil {
			t.Fatalf("decode PairChallenge: %v", err)
		}

		fail(clk, aConn, bConn)

		err = awaitAccept(acceptErr)
		var pairErr *PairingError
		if !errors.As(err, &pairErr) {
			t.Fatalf("Accept error = %v (%T), want *PairingError", err, err)
		}

		var failed []config.Event
		for _, e := range accS.recorded() {
			if e.Event == config.EventPairingFailed {
				failed = append(failed, e)
			}
		}
		if len(failed) != 1 {
			t.Fatalf("pairing_failed events = %d, want 1", len(failed))
		}
		return failed[0], pairErr.Reason
	}

	t.Run("verification failed", func(t *testing.T) {
		failed, reason := run(t, func(_ *hsClock, aConn, _ *hsConn) {
			confirm, err := proto.Marshal(&pairingpb.PairConfirm{Proof: make([]byte, proofLength)})
			if err != nil {
				t.Fatalf("marshal PairConfirm: %v", err)
			}
			if err := hsSendRaw(aConn, kindPairConfirm, hsAppendStringField(confirm, 2, alias)); err != nil {
				t.Fatalf("send PairConfirm: %v", err)
			}
			if _, _, err := receiveWithTimeout(aConn); err != nil {
				t.Fatalf("receive the reply to PairConfirm: %v", err)
			}
		})
		if reason != ReasonVerificationFailed {
			t.Fatalf("rejection reason = %v, want %v", reason, ReasonVerificationFailed)
		}
		if failed.Detail != auditReasonVerificationFailed {
			t.Fatalf("audit detail = %q, want %q", failed.Detail, auditReasonVerificationFailed)
		}
		assertNoAuditedAlias(t, failed)
	})

	t.Run("connection closed before confirm", func(t *testing.T) {
		failed, reason := run(t, func(_ *hsClock, aConn, _ *hsConn) {
			_ = aConn.Close()
		})
		if reason != ReasonNotConnected {
			t.Fatalf("rejection reason = %v, want %v", reason, ReasonNotConnected)
		}
		if failed.Detail != auditReasonNotConnected {
			t.Fatalf("audit detail = %q, want %q", failed.Detail, auditReasonNotConnected)
		}
		assertNoAuditedAlias(t, failed)
	})

	t.Run("handshake timeout", func(t *testing.T) {
		failed, reason := run(t, func(clk *hsClock, _, _ *hsConn) {
			clk.Advance(handshakeTimeout + time.Second)
		})
		if reason != ReasonTimeout {
			t.Fatalf("rejection reason = %v, want %v", reason, ReasonTimeout)
		}
		if failed.Detail != auditReasonTimeout {
			t.Fatalf("audit detail = %q, want %q", failed.Detail, auditReasonTimeout)
		}
		assertNoAuditedAlias(t, failed)
	})
}

func assertNoAuditedAlias(t *testing.T, e config.Event) {
	t.Helper()
	if e.Peer == nil {
		t.Fatal("pairing_failed has no peer reference")
	}
	if e.Peer.Alias != "" {
		t.Fatalf("pairing_failed audited the unverified alias %q", e.Peer.Alias)
	}
}

func receiveWithTimeout(conn PairConn) (uint32, []byte, error) {
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	return conn.Receive(ctx)
}

func awaitAccept(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(testWaitBound):
		return errors.New("Accept did not return within the test wait bound")
	}
}

func hsBadConfirmAttempt(t *testing.T, p *Pairing, key string) error {
	t.Helper()
	fp, pub := hsKey(t)
	aConn, bConn := hsConnPairFor(fp, pub, p.identity.fingerprint(), p.identity.publicKey())
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- p.Accept(bConn) }()

	ni := make([]byte, nonceLength)
	if _, err := rand.Read(ni); err != nil {
		t.Fatalf("random nonce: %v", err)
	}
	request := &pairingpb.PairRequest{
		InitiatorNonce:  ni,
		LookupTag:       computeHint([]byte(key), ni, fp),
		ProtocolVersion: aConn.version,
	}
	if err := hsSendProto(aConn, kindPairRequest, request); err != nil {
		t.Fatalf("send PairRequest: %v", err)
	}
	kind, _, err := receiveWithTimeout(aConn)
	if err != nil || kind != kindPairChallenge {
		t.Fatalf("receive challenge: kind=%d err=%v", kind, err)
	}
	if err := hsSendProto(aConn, kindPairConfirm, &pairingpb.PairConfirm{Proof: make([]byte, proofLength), InitiatorAlias: "intruder"}); err != nil {
		t.Fatalf("send bad PairConfirm: %v", err)
	}
	return awaitAccept(acceptErr)
}

func hsRequestAttempt(t *testing.T, p *Pairing, key string, want RejectReason) error {
	t.Helper()
	return hsRequestAttemptFrom(t, p, key, want, "192.0.2.2:40002")
}

func hsRequestAttemptFrom(t *testing.T, p *Pairing, key string, want RejectReason, acceptorRemote string) error {
	t.Helper()
	fp, pub := hsKey(t)
	aConn, bConn := hsConnPairFor(fp, pub, p.identity.fingerprint(), p.identity.publicKey())
	bConn.remote = acceptorRemote
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- p.Accept(bConn) }()

	ni := make([]byte, nonceLength)
	if _, err := rand.Read(ni); err != nil {
		t.Fatalf("random nonce: %v", err)
	}
	request := &pairingpb.PairRequest{
		InitiatorNonce:  ni,
		LookupTag:       computeHint([]byte(key), ni, fp),
		ProtocolVersion: aConn.version,
	}
	if err := hsSendProto(aConn, kindPairRequest, request); err != nil {
		t.Fatalf("send PairRequest: %v", err)
	}
	kind, payload, err := receiveWithTimeout(aConn)
	if err != nil {
		t.Fatalf("receive reject: %v", err)
	}
	if kind != kindPairReject {
		t.Fatalf("frame kind = %d, want PairReject", kind)
	}
	var reject pairingpb.PairReject
	if err := proto.Unmarshal(payload, &reject); err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	gotReason, ok := rejectReasonFromPB(reject.GetReason())
	if !ok || gotReason != want {
		t.Fatalf("reject reason = %v, want %v", reject.GetReason(), want)
	}
	return awaitAccept(acceptErr)
}

func hsCompleteAsInitiator(t *testing.T, p *Pairing, aConn, bConn *hsConn, initiatorFP Fingerprint, alias, key string) error {
	t.Helper()
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- p.Accept(bConn) }()

	ni := make([]byte, nonceLength)
	if _, err := rand.Read(ni); err != nil {
		return err
	}
	request := &pairingpb.PairRequest{
		InitiatorNonce:  ni,
		LookupTag:       computeHint([]byte(key), ni, initiatorFP),
		ProtocolVersion: aConn.version,
	}
	if err := hsSendProto(aConn, kindPairRequest, request); err != nil {
		return err
	}
	kind, payload, err := receiveWithTimeout(aConn)
	if err != nil || kind != kindPairChallenge {
		return fmt.Errorf("challenge: kind=%d err=%v", kind, err)
	}
	var challenge pairingpb.PairChallenge
	if err := proto.Unmarshal(payload, &challenge); err != nil {
		return err
	}
	proof := initiatorProof([]byte(key), ni, challenge.GetResponderNonce(), initiatorFP, p.identity.fingerprint())
	if err := hsSendProto(aConn, kindPairConfirm, &pairingpb.PairConfirm{Proof: proof, InitiatorAlias: alias}); err != nil {
		return err
	}
	kind, _, err = receiveWithTimeout(aConn)
	if err != nil || kind != kindPairDone {
		return fmt.Errorf("done: kind=%d err=%v", kind, err)
	}
	if err := aConn.Send(context.Background(), kindHello, nil); err != nil {
		return err
	}
	return awaitAccept(acceptErr)
}

func hsSendProto(conn PairConn, kind uint32, msg proto.Message) error {
	payload, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	return conn.Send(context.Background(), kind, payload)
}

func hsSendRaw(conn PairConn, kind uint32, payload []byte) error {
	return conn.Send(context.Background(), kind, payload)
}

// hsAppendStringField appends one string field to an encoded protobuf message,
// so a test can send a field the message type no longer declares.
func hsAppendStringField(payload []byte, num protowire.Number, value string) []byte {
	payload = protowire.AppendTag(payload, num, protowire.BytesType)
	return protowire.AppendString(payload, value)
}

func hsHasField(payload []byte, num protowire.Number) bool {
	for len(payload) > 0 {
		n, typ, tagLen := protowire.ConsumeTag(payload)
		if tagLen < 0 {
			return false
		}
		payload = payload[tagLen:]
		valueLen := protowire.ConsumeFieldValue(n, typ, payload)
		if valueLen < 0 {
			return false
		}
		if n == num {
			return true
		}
		payload = payload[valueLen:]
	}
	return false
}

func hsSentKinds(conn *hsConn) []uint32 {
	conn.mu.Lock()
	defer conn.mu.Unlock()
	return append([]uint32(nil), conn.sent...)
}

func makeSeq(start, n int) []byte {
	out := make([]byte, n)
	for i := range out {
		out[i] = byte(start + i)
	}
	return out
}

func equalKinds(a, b []uint32) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
