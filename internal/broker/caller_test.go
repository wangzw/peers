package broker

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
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
	"github.com/wangzw/peers/internal/broker/localpb"
)

// resetUsernameCache replaces the cache and its seams for one test.
func resetUsernameCache(t *testing.T) {
	t.Helper()
	oldLookup, oldNow := lookupUsername, usernameNow
	oldCache := usernameCache
	oldInflight := usernameInflight
	usernameCache = make(map[uint32]usernameCacheEntry)
	usernameInflight = make(map[uint32]chan struct{})
	t.Cleanup(func() {
		lookupUsername, usernameNow = oldLookup, oldNow
		usernameCache = oldCache
		usernameInflight = oldInflight
	})
}

func TestUsernameForCachesUntilTTL(t *testing.T) {
	resetUsernameCache(t)

	calls := 0
	lookupUsername = func(uid uint32) (accountdb.User, error) {
		calls++
		return accountdb.User{Name: "alice", UID: uid}, nil
	}
	now := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	usernameNow = func() time.Time { return now }

	if got := usernameFor(7); got != "alice" {
		t.Fatalf("usernameFor(7) = %q, want alice", got)
	}
	if got := usernameFor(7); got != "alice" {
		t.Fatalf("usernameFor(7) again = %q, want alice", got)
	}
	if calls != 1 {
		t.Errorf("lookup ran %d times inside the TTL, want 1", calls)
	}

	now = now.Add(usernameCacheTTL + time.Second)
	if got := usernameFor(7); got != "alice" {
		t.Fatalf("usernameFor(7) after the TTL = %q, want alice", got)
	}
	if calls != 2 {
		t.Errorf("lookup ran %d times after the TTL, want 2", calls)
	}
}

func TestUsernameForCachesFailureUntilTTL(t *testing.T) {
	resetUsernameCache(t)

	calls := 0
	lookupUsername = func(uint32) (accountdb.User, error) {
		calls++
		return accountdb.User{}, errors.New("account database unavailable")
	}
	now := time.Date(2026, 11, 19, 0, 0, 0, 0, time.UTC)
	usernameNow = func() time.Time { return now }

	if got := usernameFor(7); got != "7" {
		t.Fatalf("usernameFor(7) = %q, want the decimal uid", got)
	}
	if got := usernameFor(7); got != "7" {
		t.Fatalf("usernameFor(7) again = %q, want the decimal uid", got)
	}
	if calls != 1 {
		t.Errorf("failed lookup ran %d times inside the failure TTL, want 1", calls)
	}

	now = now.Add(30 * time.Second)
	if got := usernameFor(7); got != "7" {
		t.Fatalf("usernameFor(7) after the failure TTL = %q, want the decimal uid", got)
	}
	if calls != 2 {
		t.Errorf("failed lookup ran %d times after the failure TTL, want 2", calls)
	}
}

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

// TestUsernameForCoalescesConcurrentLookups: callers that miss the cache for
// one uid at the same moment wait for a single lookup instead of each running
// its own.
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
	case <-time.After(3 * time.Second):
		t.Fatal("the lookup never started")
	}
	// Give every worker time to reach the cache while the first lookup is held.
	time.Sleep(100 * time.Millisecond)
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

// credTestServer records what Hello sees on the caller's connection.
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

// startCredBroker starts a real gRPC server on a Unix socket. The socket path
// is kept short because Unix socket paths are limited to about 100 bytes.
func startCredBroker(t *testing.T, creds credentials.TransportCredentials, impl localpb.LocalServer) *credTestBroker {
	t.Helper()
	dir, err := os.MkdirTemp("", "apcred")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	path := filepath.Join(dir, "broker.sock")
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
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	_, err := localpb.NewLocalClient(cc).Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion})
	return err
}

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
	case <-time.After(3 * time.Second):
		t.Fatal("Hello was not called")
	}
}

// REQ-broker-config-019: a user Broker rejects connections from other
// accounts during the handshake, before any RPC handler runs.
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
	case <-time.After(3 * time.Second):
		t.Fatal("onReject was not called")
	}
}

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
	select {
	case call := <-srv.calls:
		t.Fatalf("Hello was called with caller %+v, want the connection closed during the handshake", call.caller)
	case <-time.After(200 * time.Millisecond):
	}
}

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

func TestAuthInfoSharedPerConnection(t *testing.T) {
	srv := newCredTestServer()
	b := startCredBroker(t, newPeerCredCreds(ReadPeerCred, func(Caller) bool { return true }, nil, nil), srv)

	nextAuth := func() *callerAuthInfo {
		t.Helper()
		select {
		case info := <-srv.auths:
			return info
		case <-time.After(3 * time.Second):
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
