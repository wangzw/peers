package broker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/accountdb"
	"github.com/wangzw/peers/internal/broker/helper"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/broker/lookupgate"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

func resetUsernameCache(t *testing.T) {
	t.Helper()
	oldLookup, oldNow := lookupUsername, usernameNow
	oldCache, oldUnknown := usernameCache, unknownAccounts
	oldInflight := usernameInflight
	usernameCache = make(map[uint32]usernameCacheEntry)
	unknownAccounts = newUnknownAccountCache(usernameUnknownCacheMax)
	usernameInflight = make(map[uint32]chan struct{})
	t.Cleanup(func() {
		// A background refresh may still hold the lock and the maps.
		usernameMu.Lock()
		lookupUsername, usernameNow = oldLookup, oldNow
		usernameCache, unknownAccounts = oldCache, oldUnknown
		usernameInflight = oldInflight
		usernameMu.Unlock()
	})
}

// testClock is a manually advanced clock; the background refresh reads it from
// its own goroutine, so the time is guarded.
type testClock struct {
	mu sync.Mutex
	t  time.Time
}

func newTestClock(t time.Time) *testClock { return &testClock{t: t} }

func (c *testClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *testClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.t = c.t.Add(d)
}

// TestUsernameForCachesUntilTTL checks that a resolved name is cached until its
// TTL and refreshed in the background after it expires.
func TestUsernameForCachesUntilTTL(t *testing.T) {
	resetUsernameCache(t)

	var calls atomic.Int32
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		calls.Add(1)
		return accountdb.User{Name: "alice", UID: uid}, nil
	}
	clock := newTestClock(time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC))
	usernameNow = clock.now

	if got := usernameFor(7); got != "alice" {
		t.Fatalf("usernameFor(7) = %q, want alice", got)
	}
	if got := usernameFor(7); got != "alice" {
		t.Fatalf("usernameFor(7) again = %q, want alice", got)
	}
	if got := calls.Load(); got != 1 {
		t.Errorf("lookup ran %d times inside the TTL, want 1", got)
	}

	clock.advance(usernameCacheTTL + time.Second)
	if got := usernameFor(7); got != "alice" {
		t.Fatalf("usernameFor(7) after the TTL = %q, want the cached name", got)
	}
	waitForRefresh(t, 7)
	if got := calls.Load(); got != 2 {
		t.Errorf("lookup ran %d times after the refresh, want 2", got)
	}
	if got := usernameFor(7); got != "alice" {
		t.Fatalf("usernameFor(7) after the refresh = %q, want alice", got)
	}
	if got := calls.Load(); got != 2 {
		t.Errorf("lookup ran %d times for a fresh entry, want 2", got)
	}
}

func waitForRefresh(t *testing.T, uid uint32) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		usernameMu.Lock()
		_, running := usernameInflight[uid]
		usernameMu.Unlock()
		if !running {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("a background refresh for uid %d did not finish", uid)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestUsernameForDoesNotCacheLookupFailure checks that a lookup failure answers
// only the request that ran it.
func TestUsernameForDoesNotCacheLookupFailure(t *testing.T) {
	resetUsernameCache(t)

	calls := 0
	lookupUsername = func(uint32) (accountdb.User, error) {
		calls++
		return accountdb.User{}, errors.New("account database unavailable")
	}
	usernameNow = func() time.Time { return time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC) }

	if got := usernameFor(7); got != "7" {
		t.Fatalf("usernameFor(7) = %q, want the decimal uid", got)
	}
	if got := usernameFor(7); got != "7" {
		t.Fatalf("usernameFor(7) again = %q, want the decimal uid", got)
	}
	if calls != 2 {
		t.Errorf("failed lookup ran %d times for two calls, want 2: a failure must not be cached", calls)
	}
	usernameMu.Lock()
	_, cached := usernameCache[7]
	usernameMu.Unlock()
	if cached {
		t.Error("a lookup failure was written into the cache")
	}
}

// TestUsernameForCachesEmptyName checks that an account with an empty name is
// cached as the decimal uid.
func TestUsernameForCachesEmptyName(t *testing.T) {
	resetUsernameCache(t)

	calls := 0
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		calls++
		return accountdb.User{UID: uid}, nil
	}
	usernameNow = func() time.Time { return time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC) }

	if got := usernameFor(7); got != "7" {
		t.Fatalf("usernameFor(7) = %q, want the decimal uid", got)
	}
	if got := usernameFor(7); got != "7" {
		t.Fatalf("usernameFor(7) again = %q, want the decimal uid", got)
	}
	if calls != 1 {
		t.Errorf("empty-name lookup ran %d times, want 1", calls)
	}
}

// TestUsernameForCoalescesConcurrentLookups checks that concurrent cache misses
// for one uid share a single lookup.
func TestUsernameForCoalescesConcurrentLookups(t *testing.T) {
	resetUsernameCache(t)

	var calls atomic.Int32
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		calls.Add(1)
		once.Do(func() { close(started) })
		<-release
		return accountdb.User{Name: "alice", UID: uid}, nil
	}
	usernameNow = func() time.Time { return time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC) }

	const workers = 10
	ready := make(chan struct{})
	gate := make(chan struct{})
	waiting := make(chan struct{}, workers)
	usernameWaiterHook = func(uint32) { waiting <- struct{}{} }
	t.Cleanup(func() { usernameWaiterHook = nil })
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			ready <- struct{}{}
			<-gate
			if got := usernameFor(7); got != "alice" {
				t.Errorf("usernameFor(7) = %q, want alice", got)
			}
		}()
	}
	for i := 0; i < workers; i++ {
		<-ready
	}
	close(gate)
	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("the lookup never started")
	}
	for i := 1; i < workers; i++ {
		select {
		case <-waiting:
		case <-time.After(eventTimeout):
			t.Fatalf("only %d callers reached the coalescing wait, want %d", i, workers-1)
		}
	}
	close(release)
	wg.Wait()
	if got := calls.Load(); got != 1 {
		t.Fatalf("lookup ran %d times for concurrent callers, want 1", got)
	}
}

type helloCall struct {
	caller Caller
	ok     bool
}

type credTestServer struct {
	localpb.UnimplementedLocalServer
	calls chan helloCall
	auths chan *callerAuthInfo
}

func newCredTestServer() *credTestServer {
	return &credTestServer{
		calls: make(chan helloCall, 16),
		auths: make(chan *callerAuthInfo, 16),
	}
}

func (s *credTestServer) Hello(ctx context.Context, req *localpb.HelloRequest) (*localpb.HelloResponse, error) {
	if info, ok := authInfoFrom(ctx); ok {
		select {
		case s.auths <- info:
		default:
		}
	}
	c, ok := CallerFrom(ctx)
	select {
	case s.calls <- helloCall{caller: c, ok: ok}:
	default:
	}
	return &localpb.HelloResponse{ProtocolVersion: req.GetProtocolVersion()}, nil
}

type credTestBroker struct {
	path string
}

func startCredBroker(t *testing.T, creds credentials.TransportCredentials, impl localpb.LocalServer) *credTestBroker {
	t.Helper()
	path := socktest.Path(t, "broker.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listen on %s: %v", path, err)
	}
	srv := grpc.NewServer(grpc.Creds(creds))
	localpb.RegisterLocalServer(srv, impl)
	go func() { _ = srv.Serve(ln) }()
	t.Cleanup(srv.Stop)
	return &credTestBroker{path: path}
}

func newCredClient(t *testing.T, path string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

func callHello(t *testing.T, cc *grpc.ClientConn) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	_, err := localpb.NewLocalClient(cc).Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion})
	return err
}

// TestCallerFromPeerCred checks that CallerFrom returns the connection's uid and
// pid read from peer credentials.
func TestCallerFromPeerCred(t *testing.T) {
	srv := newCredTestServer()
	b := startCredBroker(t, newPeerCredCreds(ReadPeerCred, func(Caller) bool { return true }, nil, nil), srv)

	if err := callHello(t, newCredClient(t, b.path)); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	select {
	case call := <-srv.calls:
		if !call.ok {
			t.Fatal("CallerFrom returned false")
		}
		if got, want := call.caller.UID, uint32(os.Geteuid()); got != want {
			t.Errorf("UID = %d, want %d", got, want)
		}
		if got, want := call.caller.PID, int32(os.Getpid()); got != want {
			t.Errorf("PID = %d, want %d", got, want)
		}
		if call.caller.Username == "" {
			t.Error("Username is empty")
		}
	case <-time.After(eventTimeout):
		t.Fatal("Hello was not called")
	}
}

// TestUserBrokerRejectsOtherAccount checks that a user Broker rejects another
// account's connection during the handshake before any handler runs.
func TestUserBrokerRejectsOtherAccount(t *testing.T) {
	if os.Geteuid() == 4242 {
		t.Skip("test process runs as uid 4242")
	}
	rejected := make(chan Caller, 8)
	creds := newPeerCredCreds(ReadPeerCred, func(c Caller) bool { return c.UID == 4242 }, func(c Caller) {
		select {
		case rejected <- c:
		default:
		}
	}, nil)
	srv := newCredTestServer()
	b := startCredBroker(t, creds, srv)

	err := callHello(t, newCredClient(t, b.path))
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Hello error = %v (code %v), want code %v", err, status.Code(err), codes.Unavailable)
	}
	// The window is the assertion: a handler that started would be caught under
	// normal load, and a correct run cannot fail here.
	select {
	case call := <-srv.calls:
		t.Fatalf("Hello was called with caller %+v, want the connection rejected during the handshake", call.caller)
	case <-time.After(200 * time.Millisecond):
	}
	select {
	case c := <-rejected:
		if got, want := c.UID, uint32(os.Geteuid()); got != want {
			t.Errorf("rejected UID = %d, want %d", got, want)
		}
	case <-time.After(eventTimeout):
		t.Fatal("onReject was not called")
	}
}

// TestReadPeerCredErrorClosesConnection checks that a peer-credential read
// failure fails the connection before any handler runs.
func TestReadPeerCredErrorClosesConnection(t *testing.T) {
	readErr := errors.New("cannot read peer credentials")
	srv := newCredTestServer()
	creds := newPeerCredCreds(func(net.Conn) (Caller, error) { return Caller{}, readErr }, func(Caller) bool { return true }, nil, nil)
	b := startCredBroker(t, creds, srv)

	err := callHello(t, newCredClient(t, b.path))
	if err == nil {
		t.Fatal("Hello succeeded, want the connection to fail")
	}
	if status.Code(err) != codes.Unavailable {
		t.Fatalf("Hello error = %v (code %v), want code %v", err, status.Code(err), codes.Unavailable)
	}
	// The window is the assertion: a handler that started would be caught under
	// normal load, and a correct run cannot fail here.
	select {
	case call := <-srv.calls:
		t.Fatalf("Hello was called with caller %+v, want the connection closed during the handshake", call.caller)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestCallerReadOncePerConnection checks that peer credentials are read once per
// connection.
func TestCallerReadOncePerConnection(t *testing.T) {
	var reads atomic.Int32
	read := func(net.Conn) (Caller, error) {
		reads.Add(1)
		return Caller{UID: 7, PID: 9, Username: "tester"}, nil
	}
	srv := newCredTestServer()
	b := startCredBroker(t, newPeerCredCreds(read, func(Caller) bool { return true }, nil, nil), srv)
	cc := newCredClient(t, b.path)

	for i := 0; i < 5; i++ {
		if err := callHello(t, cc); err != nil {
			t.Fatalf("Hello %d: %v", i, err)
		}
	}
	if got := reads.Load(); got != 1 {
		t.Fatalf("ReadPeerCred called %d times on one connection, want 1", got)
	}
}

// TestAuthInfoSharedPerConnection checks that calls on one connection share an
// AuthInfo and another connection gets its own.
func TestAuthInfoSharedPerConnection(t *testing.T) {
	srv := newCredTestServer()
	b := startCredBroker(t, newPeerCredCreds(ReadPeerCred, func(Caller) bool { return true }, nil, nil), srv)

	nextAuth := func() *callerAuthInfo {
		t.Helper()
		select {
		case info := <-srv.auths:
			return info
		case <-time.After(eventTimeout):
			t.Fatal("Hello did not report AuthInfo")
			return nil
		}
	}

	cc1 := newCredClient(t, b.path)
	if err := callHello(t, cc1); err != nil {
		t.Fatalf("Hello 1: %v", err)
	}
	first := nextAuth()
	if first.rejected.Load() {
		t.Fatal("a connection is marked rejected before any version mismatch")
	}
	if err := callHello(t, cc1); err != nil {
		t.Fatalf("Hello 2: %v", err)
	}
	second := nextAuth()
	if first != second {
		t.Fatal("two calls on one connection got different *callerAuthInfo pointers")
	}

	cc2 := newCredClient(t, b.path)
	if err := callHello(t, cc2); err != nil {
		t.Fatalf("Hello 3: %v", err)
	}
	third := nextAuth()
	if third == first {
		t.Fatal("a second connection got the first connection's *callerAuthInfo pointer")
	}
}

// TestAccountKeyForUnknownUIDsShareBucket checks that uids with no account entry
// share one account key.
func TestAccountKeyForUnknownUIDsShareBucket(t *testing.T) {
	resetUsernameCache(t)
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		if uid == 7 {
			return accountdb.User{Name: "alice", UID: uid}, nil
		}
		return accountdb.User{}, accountdb.ErrNotFound
	}
	usernameNow = func() time.Time { return time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC) }

	if got, err := accountKeyFor(7); err != nil || got != 7 {
		t.Fatalf("accountKeyFor(known uid) = (%d, %v), want the uid itself", got, err)
	}
	for _, uid := range []uint32{8, 9, 200000} {
		if got, err := accountKeyFor(uid); err != nil || got != unknownAccountKey {
			t.Fatalf("accountKeyFor(uid %d with no entry) = (%d, %v), want the unknown bucket %d", uid, got, err, unknownAccountKey)
		}
	}
}

// TestUnknownAccountCacheIsBounded checks that rotating uids with no account
// entry cannot grow the negative cache past its bound or enter the real-account
// cache.
func TestUnknownAccountCacheIsBounded(t *testing.T) {
	resetUsernameCache(t)
	lookupUsername = func(uint32) (accountdb.User, error) { return accountdb.User{}, accountdb.ErrNotFound }
	usernameNow = func() time.Time { return time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC) }

	// The bound is a literal, not usernameUnknownCacheMax, so raising the
	// constant cannot also raise the assertion.
	const wantMax = 1024
	const uids = 5000
	for i := 0; i < uids; i++ {
		usernameFor(uint32(100000 + i))
	}
	usernameMu.Lock()
	unknown, real := unknownAccounts.len(), len(usernameCache)
	usernameMu.Unlock()
	if unknown > wantMax {
		t.Fatalf("negative cache holds %d entries after %d uids, want at most %d", unknown, uids, wantMax)
	}
	if real != 0 {
		t.Fatalf("real-account cache holds %d entries after %d uids with no account, want 0", real, uids)
	}
}

// TestUnknownAccountCacheEvictsLeastRecentlyUsed checks that a hit keeps a
// negative-cache entry while an older one is evicted.
func TestUnknownAccountCacheEvictsLeastRecentlyUsed(t *testing.T) {
	resetUsernameCache(t)
	var mu sync.Mutex
	looked := make(map[uint32]int)
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		mu.Lock()
		looked[uid]++
		mu.Unlock()
		return accountdb.User{}, accountdb.ErrNotFound
	}
	usernameNow = func() time.Time { return time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC) }
	lookups := func(uid uint32) int {
		mu.Lock()
		defer mu.Unlock()
		return looked[uid]
	}

	const wantMax = 1024
	for uid := uint32(1); uid <= wantMax; uid++ {
		usernameFor(uid)
	}
	usernameFor(1)
	for uid := uint32(10001); uid < 10001+50; uid++ {
		usernameFor(uid)
	}

	usernameFor(1)
	if got := lookups(1); got != 1 {
		t.Fatalf("uid 1 was looked up %d times, want 1: a recently used entry was evicted", got)
	}
	usernameFor(2)
	if got := lookups(2); got != 2 {
		t.Fatalf("uid 2 was looked up %d times, want 2: the least recently used entry stayed cached", got)
	}
}

// TestUnknownAccountCacheExpiryForcesLookup checks that a negative entry past
// its TTL is a miss.
func TestUnknownAccountCacheExpiryForcesLookup(t *testing.T) {
	resetUsernameCache(t)
	var calls int
	lookupUsername = func(uint32) (accountdb.User, error) {
		calls++
		return accountdb.User{}, accountdb.ErrNotFound
	}
	now := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	usernameNow = func() time.Time { return now }

	usernameFor(7)
	if calls != 1 {
		t.Fatalf("first lookup ran %d times, want 1", calls)
	}
	now = now.Add(usernameFailureTTL + time.Second)
	usernameFor(7)
	if calls != 2 {
		t.Fatalf("lookup ran %d times after the negative entry expired, want 2", calls)
	}
	usernameMu.Lock()
	size := unknownAccounts.len()
	usernameMu.Unlock()
	if size != 1 {
		t.Fatalf("negative cache holds %d entries, want 1", size)
	}
}

// TestUsernameLookupConcurrencyIsBounded checks that the account database is
// queried by at most lookupgate.Concurrency calls at once, so a burst of
// unknown uids cannot start one getent process per connection.
func TestUsernameLookupConcurrencyIsBounded(t *testing.T) {
	resetUsernameCache(t)

	var running, peak atomic.Int32
	started := make(chan struct{}, 64)
	release := make(chan struct{})
	lookupUsername = func(uint32) (accountdb.User, error) {
		n := running.Add(1)
		for {
			p := peak.Load()
			if n <= p || peak.CompareAndSwap(p, n) {
				break
			}
		}
		started <- struct{}{}
		<-release
		running.Add(-1)
		return accountdb.User{}, accountdb.ErrNotFound
	}
	usernameNow = func() time.Time { return time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC) }

	const workers = 20
	var wg sync.WaitGroup
	wg.Add(workers)
	for i := 0; i < workers; i++ {
		go func(uid uint32) {
			defer wg.Done()
			usernameFor(uid)
		}(uint32(200000 + i))
	}

	for i := 0; i < lookupgate.Concurrency; i++ {
		select {
		case <-started:
		case <-time.After(eventTimeout):
			close(release)
			wg.Wait()
			t.Fatalf("only %d lookups started, want %d", i, lookupgate.Concurrency)
		}
	}
	// The window is the assertion: a violating run would be caught, and a
	// correct run cannot fail here; peak is the supplementary check.
	select {
	case <-started:
		close(release)
		wg.Wait()
		t.Fatalf("more than %d lookups ran at once", lookupgate.Concurrency)
	case <-time.After(100 * time.Millisecond):
	}

	close(release)
	wg.Wait()
	if got := peak.Load(); got > lookupgate.Concurrency {
		t.Fatalf("peak lookups = %d, want at most %d", got, lookupgate.Concurrency)
	}
}

// TestRealAccountCacheSurvivesUnknownRotation checks that rotating many uids
// with no account entry cannot evict a cached real account.
func TestRealAccountCacheSurvivesUnknownRotation(t *testing.T) {
	withHandshakeWaiterLimit(t, 0)
	resetUsernameCache(t)

	const realUID = 4242
	now := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	usernameNow = func() time.Time { return now }

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseLookups := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseLookups)
	var blockLookup atomic.Bool
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		if uid == realUID {
			return accountdb.User{Name: "alice", UID: uid}, nil
		}
		if blockLookup.Load() {
			<-release
		}
		return accountdb.User{}, accountdb.ErrNotFound
	}

	if got := usernameFor(realUID); got != "alice" {
		t.Fatalf("usernameFor(%d) = %q, want alice", realUID, got)
	}
	const unknownUIDs = 2000
	for i := 0; i < unknownUIDs; i++ {
		usernameFor(uint32(100000 + i))
	}

	usernameMu.Lock()
	_, kept := usernameCache[realUID]
	realSize, unknownSize := len(usernameCache), unknownAccounts.len()
	usernameMu.Unlock()
	if !kept {
		t.Fatal("rotating uids with no account entry evicted the real account")
	}
	if realSize != 1 {
		t.Fatalf("real-account cache holds %d entries, want 1", realSize)
	}
	if unknownSize > usernameUnknownCacheMax {
		t.Fatalf("negative cache holds %d entries, want at most %d", unknownSize, usernameUnknownCacheMax)
	}

	blockLookup.Store(true)
	tb := newTestBroker(t, func(d *Deps) {
		d.AllowCaller = allowEveryCaller
		d.ReadPeerCred = func(net.Conn) (Caller, error) { return Caller{UID: realUID}, nil }
	})
	if err := helloOnce(tb.sock, eventTimeout); err != nil {
		t.Fatalf("cached real account: %v, want acceptance on the fast path", err)
	}
}

func helloOnce(sock string, timeout time.Duration) error {
	cc, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return err
	}
	defer func() { _ = cc.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_, err = localpb.NewLocalClient(cc).Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion})
	return err
}

// TestGlobalSlotTakenBeforeAccountLookup checks that a connection takes a global
// slot before looking up the caller's account.
func TestGlobalSlotTakenBeforeAccountLookup(t *testing.T) {
	withLocalTotalLimit(t, maxLocalConnections, maxLocalStreams, maxLocalRegistrations, 2)
	resetUsernameCache(t)

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseLookups := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseLookups)
	lookupStarted := make(chan uint32, 8)
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		lookupStarted <- uid
		<-release
		return accountdb.User{}, accountdb.ErrNotFound
	}

	var mu sync.Mutex
	next := uint32(1000)
	read := func(net.Conn) (Caller, error) {
		mu.Lock()
		uid := next
		next++
		mu.Unlock()
		return Caller{UID: uid}, nil
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.AllowCaller = allowEveryCaller
		d.ReadPeerCred = read
	})

	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- helloOnce(tb.sock, eventTimeout) }()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-lookupStarted:
		case <-time.After(eventTimeout):
			releaseLookups()
			t.Fatalf("only %d lookups started, want 2", i)
		}
	}

	// The third connection finds the global slots taken before it looks up its
	// account, so it is refused promptly while the lookup queue stays blocked.
	if err := helloOnce(tb.sock, eventTimeout); status.Code(err) != codes.Unavailable {
		releaseLookups()
		t.Fatalf("third connection: err = %v (code %v), want the handshake refused while the lookup queue is blocked", err, status.Code(err))
	}
	// The window is the assertion: a lookup that started despite the taken slots
	// would send to lookupStarted, and a correct run cannot fail here.
	select {
	case uid := <-lookupStarted:
		releaseLookups()
		t.Fatalf("uid %d started a lookup with the global slots already taken", uid)
	case <-time.After(100 * time.Millisecond):
	}

	releaseLookups()
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Errorf("blocked connection: %v", err)
		}
	}
}

// withHandshakeWaiterLimit shrinks the lookup wait queue for one test; run it
// before the Broker is built.
func withHandshakeWaiterLimit(t *testing.T, limit int) {
	t.Helper()
	old := handshakeLookupWaiters
	handshakeLookupWaiters = make(chan struct{}, limit)
	t.Cleanup(func() { handshakeLookupWaiters = old })
}

// TestHandshakeCachedRealAccountSkipsLookupWait checks that an expired cached
// real account connects while every lookup wait slot is taken, and is refreshed
// in the background.
func TestHandshakeCachedRealAccountSkipsLookupWait(t *testing.T) {
	withHandshakeWaiterLimit(t, 2)
	resetUsernameCache(t)

	const realUID = 4242
	now := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	usernameNow = func() time.Time { return now }
	usernameMu.Lock()
	usernameCache[realUID] = usernameCacheEntry{name: "alice", expires: now.Add(-time.Second)}
	usernameMu.Unlock()

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseLookups := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseLookups)
	started := make(chan uint32, 8)
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		if uid == realUID {
			return accountdb.User{Name: "alice", UID: uid}, nil
		}
		started <- uid
		<-release
		return accountdb.User{}, accountdb.ErrNotFound
	}

	var useReal atomic.Bool
	var mu sync.Mutex
	next := uint32(300000)
	read := func(net.Conn) (Caller, error) {
		if useReal.Load() {
			return Caller{UID: realUID}, nil
		}
		mu.Lock()
		uid := next
		next++
		mu.Unlock()
		return Caller{UID: uid}, nil
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.AllowCaller = allowEveryCaller
		d.ReadPeerCred = read
	})

	results := make(chan error, 2)
	for i := 0; i < 2; i++ {
		go func() { results <- helloOnce(tb.sock, eventTimeout) }()
	}
	for i := 0; i < 2; i++ {
		select {
		case <-started:
		case <-time.After(eventTimeout):
			releaseLookups()
			t.Fatalf("only %d lookups started, want 2", i)
		}
	}
	if got := len(handshakeLookupWaiters); got != 2 {
		releaseLookups()
		t.Fatalf("waiters = %d, want the queue saturated at 2", got)
	}

	useReal.Store(true)
	if err := helloOnce(tb.sock, eventTimeout); err != nil {
		releaseLookups()
		t.Fatalf("cached real account: %v, want it accepted while the lookup queue is full", err)
	}

	waitForRefresh(t, realUID)
	usernameMu.Lock()
	entry := usernameCache[realUID]
	usernameMu.Unlock()
	if entry.name != "alice" || !entry.expires.Equal(now.Add(usernameCacheTTL)) {
		releaseLookups()
		t.Fatalf("the cached account was not refreshed in the background: entry = %+v", entry)
	}

	releaseLookups()
	for i := 0; i < 2; i++ {
		if err := <-results; err != nil {
			t.Errorf("blocked unknown connection: %v", err)
		}
	}
}

// TestHandshakeLookupWaitersAreBounded checks that at most 64 connections wait
// for an account lookup and the rest are refused at once.
func TestHandshakeLookupWaitersAreBounded(t *testing.T) {
	resetUsernameCache(t)

	const connections = 80
	const wantWaiters = 64
	const realUID = 4242

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseLookups := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseLookups)
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		<-release
		if uid == realUID {
			return accountdb.User{Name: "alice", UID: uid}, nil
		}
		return accountdb.User{}, accountdb.ErrNotFound
	}

	now := time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC)
	usernameNow = func() time.Time { return now }
	usernameMu.Lock()
	usernameCache[realUID] = usernameCacheEntry{name: "alice", expires: now.Add(usernameCacheTTL)}
	usernameMu.Unlock()

	var useReal atomic.Bool
	var mu sync.Mutex
	next := uint32(400000)
	read := func(net.Conn) (Caller, error) {
		if useReal.Load() {
			return Caller{UID: realUID}, nil
		}
		mu.Lock()
		uid := next
		next++
		mu.Unlock()
		return Caller{UID: uid}, nil
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.AllowCaller = allowEveryCaller
		d.ReadPeerCred = read
	})

	results := make(chan error, connections)
	for i := 0; i < connections; i++ {
		go func() { results <- helloOnce(tb.sock, eventTimeout) }()
	}

	deadline := time.Now().Add(eventTimeout)
	peak := 0
	for len(handshakeLookupWaiters) < wantWaiters {
		if n := len(handshakeLookupWaiters); n > peak {
			peak = n
		}
		if time.Now().After(deadline) {
			releaseLookups()
			t.Fatalf("waiters reached %d, want %d", len(handshakeLookupWaiters), wantWaiters)
		}
		time.Sleep(time.Millisecond)
	}
	if n := len(handshakeLookupWaiters); n > peak {
		peak = n
	}
	if peak > wantWaiters {
		releaseLookups()
		t.Fatalf("waiters peaked at %d, want at most %d", peak, wantWaiters)
	}

	refused := 0
	for refused < connections-wantWaiters {
		select {
		case err := <-results:
			if status.Code(err) != codes.Unavailable {
				releaseLookups()
				t.Fatalf("refused connection error = %v (code %v), want code %v", err, status.Code(err), codes.Unavailable)
			}
			refused++
		case <-time.After(eventTimeout):
			releaseLookups()
			t.Fatalf("only %d of %d connections past the bound were refused", refused, connections-wantWaiters)
		}
	}

	useReal.Store(true)
	if err := helloOnce(tb.sock, eventTimeout); err != nil {
		releaseLookups()
		t.Fatalf("cached real account: %v, want it accepted while the lookup wait is full", err)
	}

	releaseLookups()
	accepted := 0
	for i := 0; i < connections-refused; i++ {
		select {
		case err := <-results:
			if err != nil {
				t.Errorf("admitted connection: %v", err)
			} else {
				accepted++
			}
		case <-time.After(eventTimeout):
			t.Fatalf("only %d of %d admitted connections finished", accepted, connections-refused)
		}
	}
	if accepted != wantWaiters {
		t.Errorf("accepted connections = %d, want %d", accepted, wantWaiters)
	}
}

// TestHandshakeLookupTimeoutReleasesGlobalSlot checks that a lookup that cannot
// take a gate slot is refused within the handshake deadline and returns its
// global slot.
func TestHandshakeLookupTimeoutReleasesGlobalSlot(t *testing.T) {
	withLocalTotalLimit(t, maxLocalConnections, maxLocalStreams, maxLocalRegistrations, 1)
	resetUsernameCache(t)
	oldTimeout := handshakeLookupTimeout
	handshakeLookupTimeout = 100 * time.Millisecond
	t.Cleanup(func() { handshakeLookupTimeout = oldTimeout })

	var held []func()
	t.Cleanup(func() {
		for _, release := range held {
			release()
		}
	})
	for i := 0; i < lookupgate.Concurrency; i++ {
		release, err := lookupgate.Acquire(context.Background())
		if err != nil {
			t.Fatalf("taking getent slot %d: %v", i, err)
		}
		held = append(held, release)
	}

	var lookups atomic.Int32
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		lookups.Add(1)
		return accountdb.User{Name: "alice", UID: uid}, nil
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.AllowCaller = allowEveryCaller
		d.ReadPeerCred = func(net.Conn) (Caller, error) { return Caller{UID: 1000}, nil }
	})

	if err := helloOnce(tb.sock, eventTimeout); status.Code(err) != codes.Unavailable {
		t.Fatalf("connection whose lookup timed out: err = %v (code %v), want the handshake refused", err, status.Code(err))
	}
	if got := lookups.Load(); got != 0 {
		t.Fatalf("a lookup ran without a getent slot: %d lookups", got)
	}

	for _, release := range held {
		release()
	}
	held = nil
	if err := helloOnce(tb.sock, eventTimeout); err != nil {
		t.Fatalf("connection after a timed-out lookup: %v, want it accepted", err)
	}
}

// TestUsernameLookupFailureIsNotUnknown checks that only a missing account entry
// enters the unknown bucket; a lookup failure is not cached and fails closed.
func TestUsernameLookupFailureIsNotUnknown(t *testing.T) {
	resetUsernameCache(t)

	boom := errors.New("account database unavailable")
	failing := true
	calls := 0
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		calls++
		if failing {
			return accountdb.User{}, boom
		}
		return accountdb.User{Name: "alice", UID: uid}, nil
	}
	usernameNow = func() time.Time { return time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC) }

	if key, err := accountKeyFor(7); !errors.Is(err, boom) {
		t.Fatalf("accountKeyFor(7) = (%d, %v), want the lookup failure", key, err)
	}
	if key, err := accountKeyFor(7); !errors.Is(err, boom) {
		t.Fatalf("accountKeyFor(7) again = (%d, %v), want the failure to run again", key, err)
	}
	if calls != 2 {
		t.Errorf("failed lookup ran %d times for two requests, want 2: a failure must not be cached", calls)
	}
	if got := usernameFor(7); got != "7" {
		t.Errorf("usernameFor(7) = %q, want the decimal uid for display", got)
	}
	if calls != 3 {
		t.Errorf("display lookup ran %d times, want 3: a failure must not be cached", calls)
	}

	failing = false
	if key, err := accountKeyFor(7); err != nil || key != 7 {
		t.Fatalf("accountKeyFor(7) after the failure = (%d, %v), want the real account", key, err)
	}
	if calls != 4 {
		t.Errorf("lookup ran %d times, want 4", calls)
	}
	if key, err := accountKeyFor(7); err != nil || key != 7 {
		t.Fatalf("accountKeyFor(7) after the success = (%d, %v), want the real account", key, err)
	}
	if calls != 4 {
		t.Errorf("lookup ran %d times after a successful one, want 4: the account is cached", calls)
	}
}

// TestBrokerAndHelperShareLookupGate checks that the broker's caller-name lookup
// and the helper's account lookup share one process-wide gate.
func TestBrokerAndHelperShareLookupGate(t *testing.T) {
	resetUsernameCache(t)

	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAll := func() { releaseOnce.Do(func() { close(release) }) }
	t.Cleanup(releaseAll)

	started := make(chan struct{}, lookupgate.Concurrency)
	checker := &helper.AccessChecker{
		Lookup: func(uint32) (helper.Account, error) {
			started <- struct{}{}
			<-release
			return helper.Account{}, helper.ErrNoAccount
		},
		Timeout:       10 * time.Second,
		Logger:        slog.New(slog.DiscardHandler),
		MaxConcurrent: 8,
		MaxPerAccount: 8,
	}
	checkResults := make(chan error, lookupgate.Concurrency)
	for i := 0; i < lookupgate.Concurrency; i++ {
		go func(uid uint32) {
			allowed, err := checker.Check(context.Background(), uid, "/config.yaml", helper.AccessRead)
			if err == nil && allowed {
				err = errors.New("unknown account was allowed")
			}
			checkResults <- err
		}(uint32(5000 + i))
	}
	for i := 0; i < lookupgate.Concurrency; i++ {
		select {
		case <-started:
		case <-time.After(eventTimeout):
			releaseAll()
			t.Fatalf("only %d helper lookups started, want %d", i, lookupgate.Concurrency)
		}
	}

	done := make(chan struct{})
	go func() {
		usernameFor(7000)
		close(done)
	}()
	select {
	case <-done:
		releaseAll()
		t.Fatal("the broker lookup ran while the helper held every process-wide lookup slot")
	case <-time.After(200 * time.Millisecond):
		// The window is the assertion: the gate's slots are unexported, so
		// staying out is observed by giving a violation time to complete, and a
		// blocked lookup cannot false-fail here.
	}

	releaseAll()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("the broker lookup did not finish after the helper released the gate")
	}
	for i := 0; i < lookupgate.Concurrency; i++ {
		if err := <-checkResults; err != nil {
			t.Errorf("helper check: %v", err)
		}
	}
}
