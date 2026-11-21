package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helper"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// fakeHelperPool is the systemRouter's view of a helper pool.
type fakeHelperPool struct {
	mu       sync.Mutex
	backend  *helper.Backend
	err      error
	acquired []uint32
	releases int
	closes   int
}

var _ helperPool = (*fakeHelperPool)(nil)

func (p *fakeHelperPool) Acquire(_ context.Context, uid uint32) (*helper.Backend, func(), error) {
	p.mu.Lock()
	p.acquired = append(p.acquired, uid)
	b, err := p.backend, p.err
	p.mu.Unlock()
	if err != nil {
		return nil, nil, err
	}
	return b, func() {
		p.mu.Lock()
		p.releases++
		p.mu.Unlock()
	}, nil
}

func (p *fakeHelperPool) Close() error {
	p.mu.Lock()
	p.closes++
	p.mu.Unlock()
	return nil
}

// helperAccessCall is one recorded Check call.
type helperAccessCall struct {
	uid  uint32
	path string
	need helper.Access
}

// fakeHelperAccess returns the configured results in order.
type fakeHelperAccess struct {
	mu      sync.Mutex
	results []fakeAccessResult
	next    int
	calls   []helperAccessCall
}

type fakeAccessResult struct {
	allowed bool
	err     error
}

func (f *fakeHelperAccess) Check(_ context.Context, uid uint32, path string, need helper.Access) (bool, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.calls = append(f.calls, helperAccessCall{uid: uid, path: path, need: need})
	if f.next >= len(f.results) {
		return true, nil
	}
	r := f.results[f.next]
	f.next++
	return r.allowed, r.err
}

func (f *fakeHelperAccess) snapshot() []helperAccessCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	return append([]helperAccessCall(nil), f.calls...)
}

func TestSystemCheckerMapsResults(t *testing.T) {
	checkErr := errors.New("uid mismatch")
	access := &fakeHelperAccess{results: []fakeAccessResult{
		{allowed: true},
		{allowed: false},
		{allowed: false, err: checkErr},
	}}
	checker := systemChecker{c: access}

	ctx := context.Background()
	granted, err := checker.Check(ctx, Caller{UID: 1001}, "/etc/peers/config.yaml", AccessRead)
	if !granted || err != nil {
		t.Fatalf("first Check = (%v, %v), want (true, nil)", granted, err)
	}
	granted, err = checker.Check(ctx, Caller{UID: 1001}, "/etc/peers/config.yaml", AccessWrite)
	if granted || err != nil {
		t.Fatalf("second Check = (%v, %v), want (false, nil)", granted, err)
	}
	granted, err = checker.Check(ctx, Caller{UID: 1001}, "/etc/peers/config.yaml", AccessWrite)
	if granted || !errors.Is(err, checkErr) {
		t.Fatalf("third Check = (%v, %v), want (false, %v)", granted, err, checkErr)
	}
}

func TestSystemCheckerPassesPathAndAccess(t *testing.T) {
	access := &fakeHelperAccess{}
	checker := systemChecker{c: access}
	ctx := context.Background()

	const path = "/etc/peers/config.yaml"
	if _, err := checker.Check(ctx, Caller{UID: 1001}, path, AccessWrite); err != nil {
		t.Fatalf("Check(write): %v", err)
	}
	if _, err := checker.Check(ctx, Caller{UID: 1001}, path, AccessRead); err != nil {
		t.Fatalf("Check(read): %v", err)
	}
	granted, err := checker.Check(ctx, Caller{UID: 1001}, path, AccessNone)
	if !granted || err != nil {
		t.Fatalf("Check(none) = (%v, %v), want (true, nil)", granted, err)
	}
	calls := access.snapshot()
	if len(calls) != 2 {
		t.Fatalf("checker called %d times, want 2", len(calls))
	}
	if calls[0].uid != 1001 || calls[0].path != path || calls[0].need != helper.AccessWrite {
		t.Errorf("first call = %+v, want uid 1001, path %q, helper.AccessWrite", calls[0], path)
	}
	if calls[1].uid != 1001 || calls[1].path != path || calls[1].need != helper.AccessRead {
		t.Errorf("second call = %+v, want uid 1001, path %q, helper.AccessRead", calls[1], path)
	}
}

func TestSystemCheckerRejectsUnknownAccess(t *testing.T) {
	access := &fakeHelperAccess{}
	checker := systemChecker{c: access}

	granted, err := checker.Check(context.Background(), Caller{UID: 1001}, "/etc/peers/config.yaml", Access(99))
	if err == nil {
		t.Fatal("Check accepted an unknown access level")
	}
	if granted {
		t.Error("Check returned true with the error")
	}
	if calls := access.snapshot(); len(calls) != 0 {
		t.Errorf("helper checker called %d times for an unknown level, want 0", len(calls))
	}
}

func TestSystemRouterForwards(t *testing.T) {
	backend := &helper.Backend{}
	pool := &fakeHelperPool{backend: backend}
	router := systemRouter{pool: pool}

	got, release, err := router.Acquire(context.Background(), 4242)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	owned, ok := got.(ownedBackend)
	if !ok {
		t.Fatalf("Acquire returned %T, want ownedBackend", got)
	}
	if owned.SessionBackend != SessionBackend(backend) {
		t.Fatalf("Acquire returned %p, want the pool's backend %p", owned.SessionBackend, backend)
	}
	if owned.uid != 4242 {
		t.Errorf("ownedBackend uid = %d, want 4242", owned.uid)
	}
	if len(pool.acquired) != 1 || pool.acquired[0] != 4242 {
		t.Errorf("pool acquired uids = %v, want [4242]", pool.acquired)
	}
	if release == nil {
		t.Fatal("Acquire returned a nil release")
	}
	release()
	if pool.releases != 1 {
		t.Errorf("release called %d times, want 1", pool.releases)
	}

	pool.err = errors.New("no helper")
	got, release, err = router.Acquire(context.Background(), 4242)
	if err == nil {
		t.Fatal("Acquire returned a nil error after the pool failed")
	}
	if got != nil {
		t.Errorf("Acquire returned a non-nil SessionBackend on error: %v", got)
	}
	if release != nil {
		t.Error("Acquire returned a non-nil release on error")
	}
}

func TestSystemAssemblerFillsDeps(t *testing.T) {
	router := &fakeSessions{}
	checker := &wiringChecker{granted: true}
	closes := 0
	var gotExe string
	called := false
	a := systemAssembler{
		executable: func() (string, error) { return "/bin/true", nil },
		newDeps: func(exe string, _ *slog.Logger) (SessionRouter, accessChecker, func() error) {
			called = true
			gotExe = exe
			return router, checker, func() error {
				closes++
				return nil
			}
		},
	}
	deps := &Deps{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))}
	asm := &Assembly{}

	comps, err := a.Assemble(context.Background(), nil, deps, asm)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if !called {
		t.Fatal("newDeps was not called")
	}
	if gotExe != "/bin/true" {
		t.Errorf("newDeps exe = %q, want /bin/true", gotExe)
	}
	if deps.Sessions != SessionRouter(router) {
		t.Error("Assemble did not put the router in Deps.Sessions")
	}
	if deps.AllowCaller == nil || !deps.AllowCaller(Caller{UID: 4242}) {
		t.Error("Deps.AllowCaller does not allow account 4242")
	}
	if asm.Access != accessChecker(checker) {
		t.Error("Assemble did not put the checker in Assembly.Access")
	}
	if len(comps) != 1 {
		t.Fatalf("Assemble returned %d components, want 1", len(comps))
	}
	if comps[0].Name() != "system" {
		t.Errorf("component name = %q, want system", comps[0].Name())
	}
	if err := comps[0].Start(context.Background(), nil); err != nil {
		t.Errorf("component Start: %v", err)
	}
	comps[0].Stop()
	comps[0].Stop()
	if closes != 1 {
		t.Errorf("close called %d times, want 1", closes)
	}

	boom := errors.New("executable is gone")
	failing := systemAssembler{
		executable: func() (string, error) { return "", boom },
		newDeps: func(string, *slog.Logger) (SessionRouter, accessChecker, func() error) {
			t.Error("newDeps was called after locating the executable failed")
			return nil, nil, nil
		},
	}
	if _, err := failing.Assemble(context.Background(), nil, &Deps{}, &Assembly{}); !errors.Is(err, boom) {
		t.Fatalf("Assemble error = %v, want %v", err, boom)
	}
}

// TestNewSystemDepsFillsAccessCheckLimits checks that the system Broker's
// access checker carries the check limits rather than relying on the helper
// defaults.
func TestNewSystemDepsFillsAccessCheckLimits(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, checker, closeFn := newSystemDeps(ctx, "/bin/true", slog.New(slog.NewTextHandler(io.Discard, nil)))
	defer func() {
		if err := closeFn(); err != nil {
			t.Errorf("closing the helper pool: %v", err)
		}
	}()

	sc, ok := checker.(systemChecker)
	if !ok {
		t.Fatalf("checker is %T, want systemChecker", checker)
	}
	ac, ok := sc.c.(*helper.AccessChecker)
	if !ok {
		t.Fatalf("wrapped checker is %T, want *helper.AccessChecker", sc.c)
	}
	if ac.MaxConcurrent != accessChecksConcurrent {
		t.Errorf("MaxConcurrent = %d, want %d", ac.MaxConcurrent, accessChecksConcurrent)
	}
	if ac.MaxPerAccount != accessChecksPerAccount {
		t.Errorf("MaxPerAccount = %d, want %d", ac.MaxPerAccount, accessChecksPerAccount)
	}
}

// inProcessHelperStart serves the helper protocol over pipes instead of
// spawning a child. exits receives one function per helper that ends it, which
// stands in for the signal a service manager sends when it stops the Broker.
func inProcessHelperStart(t *testing.T, exits chan<- func()) func(context.Context, helper.Account) (*helper.Process, error) {
	t.Helper()
	return func(_ context.Context, acct helper.Account) (*helper.Process, error) {
		toServerR, toServerW := io.Pipe()
		fromServerR, fromServerW := io.Pipe()
		srvCtx, srvCancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() {
			err := helper.Serve(srvCtx, toServerR, fromServerW, helper.InboxOps{}, helper.Identity{
				UID:    acct.UID,
				GID:    acct.GID,
				Groups: acct.Groups,
			})
			_ = fromServerW.Close()
			_ = toServerR.Close()
			done <- err
		}()
		exits <- srvCancel
		return &helper.Process{
			Stdin:  toServerW,
			Stdout: fromServerR,
			Wait: func() error {
				srvCancel()
				return <-done
			},
			Kill: func() error {
				srvCancel()
				return nil
			},
		}, nil
	}
}

// waitForAccountHelperRecord waits until the logger wrote a record for an
// account helper.
func waitForAccountHelperRecord(t *testing.T, logs *syncBuffer) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if strings.Contains(logs.String(), "account_helper_") {
			return
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatalf("no account_helper_ record within 3s: %s", logs.String())
}

// TestSystemStopDoesNotLogHelperExitAsError runs the production assembler and
// its pool wiring around an in-process helper. Cancelling the context passed
// to Assemble, as a signal does, and then ending the helper, as the service
// manager does, must not produce an ERROR or WARN account_helper_ record.
func TestSystemStopDoesNotLogHelperExitAsError(t *testing.T) {
	uid := uint32(os.Getuid())
	if _, err := helper.LookupAccount(uid); err != nil {
		t.Skipf("the current account is not in the account database: %v", err)
	}

	var logs syncBuffer
	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)
	logger := NewLogger(&logs, level)

	exits := make(chan func(), 4)
	old := newHelperPool
	newHelperPool = func(opts helper.PoolOptions) *helper.Pool {
		opts.Start = inProcessHelperStart(t, exits)
		return helper.NewPool(opts)
	}
	t.Cleanup(func() { newHelperPool = old })

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	a := systemAssembler{executable: func() (string, error) { return "/bin/true", nil }}
	deps := &Deps{Logger: logger}
	comps, err := a.Assemble(ctx, nil, deps, &Assembly{})
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if len(comps) != 1 {
		t.Fatalf("Assemble returned %d components, want 1", len(comps))
	}
	stopped := false
	defer func() {
		if !stopped {
			comps[0].Stop()
		}
	}()

	backend, release, err := deps.Sessions.Acquire(ctx, uid)
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()
	lookupCtx, cancelLookup := context.WithTimeout(ctx, 3*time.Second)
	_, lookupErr := backend.Lookup(lookupCtx, "/no/such/dir", "/no/such/dir/inbox.sock")
	cancelLookup()
	if lookupErr != nil && !errors.Is(lookupErr, inbox.ErrNoRecord) {
		t.Fatalf("Lookup: %v", lookupErr)
	}

	var exit func()
	select {
	case exit = <-exits:
	case <-time.After(3 * time.Second):
		t.Fatal("the in-process helper did not start")
	}

	// A service stop signals the Broker and the helper at the same time.
	cancel()
	exit()

	// Read after watchLive wrote its record, before pool Close can hide it.
	waitForAccountHelperRecord(t, &logs)
	records := decodeLogLines(t, logs.String())
	found := false
	for _, rec := range records {
		event, _ := rec["event"].(string)
		if !strings.HasPrefix(event, "account_helper_") {
			continue
		}
		found = true
		if lvl := rec["level"]; lvl == "ERROR" || lvl == "WARN" {
			t.Fatalf("%s was logged at %v during a stop (log: %s)", event, lvl, logs.String())
		}
	}
	if !found {
		t.Fatalf("the helper exit was not logged: %s", logs.String())
	}
	stopped = true
	comps[0].Stop()
}

func TestAssemblersForPrependsSystem(t *testing.T) {
	x := &configWiring{}

	got := assemblersFor(RunOptions{System: true, Assemblers: []Assembler{x}})
	if len(got) != 2 {
		t.Fatalf("assemblersFor returned %d assemblers, want 2", len(got))
	}
	if _, ok := got[0].(systemAssembler); !ok {
		t.Errorf("first assembler is %T, want systemAssembler", got[0])
	}
	if got[1] != Assembler(x) {
		t.Error("the caller's assembler is not second")
	}

	got = assemblersFor(RunOptions{System: false, Assemblers: []Assembler{x}})
	if len(got) != 1 || got[0] != Assembler(x) {
		t.Fatalf("user-mode assemblersFor = %v, want only the caller's assembler", got)
	}

	got = assemblersFor(RunOptions{System: true, Assemblers: []Assembler{}})
	if len(got) != 1 {
		t.Fatalf("system-mode empty assemblers = %d, want 1", len(got))
	}
	if _, ok := got[0].(systemAssembler); !ok {
		t.Errorf("empty list's only assembler is %T, want systemAssembler", got[0])
	}
}

func TestConfigWiringUsesAssemblyAccess(t *testing.T) {
	t.Run("denied", func(t *testing.T) {
		denied := &wiringChecker{granted: false}
		tb := newTestBroker(t, func(d *Deps) {
			newWiringComponents(t, d.Config, d, &Assembly{Access: denied}, nil)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_, err := tb.client.SetConfig(ctx, &localpb.SetConfigRequest{Key: "broker.alias", Value: "build-box"})
		wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED)
	})

	t.Run("nil allows", func(t *testing.T) {
		tb := newTestBroker(t, func(d *Deps) {
			newWiringComponents(t, d.Config, d, &Assembly{}, nil)
		})
		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		resp, err := tb.client.GetConfig(ctx, &localpb.GetConfigRequest{Key: "broker.alias"})
		if err != nil {
			t.Fatalf("GetConfig with a nil Assembly.Access: %v", err)
		}
		if entries := resp.GetEntries(); len(entries) != 1 {
			t.Fatalf("GetConfig returned %d entries, want 1", len(entries))
		}
	})
}

func TestAllowCallerOverridesSameAccount(t *testing.T) {
	readOther := func(net.Conn) (Caller, error) { return Caller{UID: 4242}, nil }

	t.Run("default rejects another account", func(t *testing.T) {
		tb := newTestBroker(t, func(d *Deps) {
			d.Geteuid = func() int { return 1000 }
			d.ReadPeerCred = readOther
		})
		if _, err := callTestHello(t, tb.client, localpb.ProtocolVersion); err == nil {
			t.Fatal("Hello succeeded for an account the Broker does not serve")
		}
	})

	t.Run("AllowCaller accepts every account", func(t *testing.T) {
		tb := newTestBroker(t, func(d *Deps) {
			d.Geteuid = func() int { return 1000 }
			d.ReadPeerCred = readOther
			d.AllowCaller = func(Caller) bool { return true }
		})
		if _, err := callTestHello(t, tb.client, localpb.ProtocolVersion); err != nil {
			t.Fatalf("Hello with AllowCaller: %v", err)
		}
	})
}

func TestSystemRegisterAcquiresCallerAccount(t *testing.T) {
	sessions := &fakeSessions{}
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = sessions
		d.Geteuid = func() int { return 0 }
		d.ReadPeerCred = func(net.Conn) (Caller, error) { return Caller{UID: 4242}, nil }
		d.AllowCaller = func(Caller) bool { return true }
	})

	fake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	rs := startRegistration(t, tb, fake)

	waitForReg(t, "the connection to wait", func() bool {
		return tb.lockedWaitingCount() == 1
	})
	if got := sessions.AcquiredUIDs(); !reflect.DeepEqual(got, []uint32{4242}) {
		t.Fatalf("Acquire uids = %v, want [4242]", got)
	}

	tb.clock.Advance(10 * time.Second)
	if reason := rs.problem(3 * time.Second); reason != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SOCKET_NOT_OWNED {
		t.Fatalf("reason = %v, want socket_not_owned", reason)
	}
}

// recordedDelivery is one Deliver call seen by a recordingBackend.
type recordedDelivery struct {
	session inbox.Session
	sig     string
	text    string
	msgID   string
}

// recordingBackend forwards to one account backend and records Deliver calls.
type recordingBackend struct {
	SessionBackend
	mu         sync.Mutex
	deliveries []recordedDelivery
}

func newRecordingBackend() *recordingBackend {
	base, _, _ := UserRouter().Acquire(context.Background(), 0)
	return &recordingBackend{SessionBackend: base}
}

func (b *recordingBackend) Deliver(ctx context.Context, s inbox.Session, sig, text, msgID string, timeout time.Duration) error {
	b.mu.Lock()
	b.deliveries = append(b.deliveries, recordedDelivery{session: s, sig: sig, text: text, msgID: msgID})
	b.mu.Unlock()
	return b.SessionBackend.Deliver(ctx, s, sig, text, msgID, timeout)
}

func (b *recordingBackend) snapshot() []recordedDelivery {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]recordedDelivery(nil), b.deliveries...)
}

// sequenceSessions hands out its backends in Acquire order.
type sequenceSessions struct {
	mu       sync.Mutex
	backends []SessionBackend
	uids     []uint32
	next     int
}

var _ SessionRouter = (*sequenceSessions)(nil)

func (s *sequenceSessions) Acquire(_ context.Context, uid uint32) (SessionBackend, func(), error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.uids = append(s.uids, uid)
	if s.next >= len(s.backends) {
		return nil, nil, errors.New("no more backends")
	}
	b := s.backends[s.next]
	s.next++
	return b, func() {}, nil
}

func TestSystemDeliveryUsesTargetAgentBackend(t *testing.T) {
	senderBackend := newRecordingBackend()
	targetBackend := newRecordingBackend()
	router := &sequenceSessions{backends: []SessionBackend{senderBackend, targetBackend}}

	var adapter *agentTableAdapter
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = router
		d.Geteuid = func() int { return 0 }
		d.ReadPeerCred = func(net.Conn) (Caller, error) {
			// The fake session's socket belongs to the test process, so the
			// caller must be that account for the socket owner check to pass.
			return Caller{UID: uint32(os.Geteuid())}, nil
		}
		d.AllowCaller = func(Caller) bool { return true }
		adapter = &agentTableAdapter{}
		d.Delivery = newRPCDelivery(adapter, topicVisibility{d.Topics}, emptyDeliveryPeerTable(),
			newFakeSettings(defaultSendSettings(), "hostA"))
	})
	adapter.setBroker(tb.b)

	senderFake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	targetFake := fakesession.New(t, fakesession.Options{Name: "frontend"})
	sender := registerListAgent(t, tb, senderFake)
	rs := startRegistrationAt(t, tb.newClient(t), targetFake.ConfigDir(), targetFake.SocketPath())
	target := AgentID(rs.complete(3 * time.Second).GetAgentId())
	joinListTopics(t, tb, sender, "t1")
	joinListTopics(t, tb, target, "t1")

	targetView, ok := tb.b.lookupAgent(target)
	if !ok {
		t.Fatal("target agent disappeared from the table")
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if _, err := tb.client.Send(ctx, &localpb.SendRequest{
		AgentId:   string(sender),
		ToAddress: targetView.Address,
		Message:   "hello from the system Broker",
	}); err != nil {
		t.Fatalf("Send: %v", err)
	}

	if got := len(senderBackend.snapshot()); got != 0 {
		t.Errorf("the sender's backend recorded %d deliveries, want 0", got)
	}
	deliveries := targetBackend.snapshot()
	if len(deliveries) != 1 {
		t.Fatalf("the target's backend recorded %d deliveries, want 1", len(deliveries))
	}
	if deliveries[0].sig != "api-worker@hostA" {
		t.Errorf("Deliver signature = %q, want api-worker@hostA", deliveries[0].sig)
	}
}

// statBlockingRouter replaces one account's backend with an ownedBackend whose
// directory stat blocks.
type statBlockingRouter struct {
	SessionRouter
	stuckUID uint32
	stat     func(string) (fs.FileInfo, error)
}

func (r statBlockingRouter) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	base, release, err := r.SessionRouter.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	if uid != r.stuckUID {
		return base, release, nil
	}
	return ownedBackend{SessionBackend: base, uid: uid, stat: r.stat}, release, nil
}

// sequencePeerCreds identifies successive connections with the given uids.
func sequencePeerCreds(uids ...uint32) func(net.Conn) (Caller, error) {
	var mu sync.Mutex
	next := 0
	return func(net.Conn) (Caller, error) {
		mu.Lock()
		defer mu.Unlock()
		uid := uids[next]
		if next < len(uids)-1 {
			next++
		}
		return Caller{UID: uid}, nil
	}
}

// TestVerifyConfigDirBoundedTimesOut checks that a blocked ownership check is
// abandoned after the named timeout and reported as a failed check.
func TestVerifyConfigDirBoundedTimesOut(t *testing.T) {
	release := make(chan struct{})
	defer close(release)
	started := make(chan struct{})
	var once sync.Once

	backend := ownedBackend{
		uid: uint32(os.Geteuid()),
		stat: func(string) (fs.FileInfo, error) {
			once.Do(func() { close(started) })
			<-release
			return nil, errors.New("stat was released")
		},
	}

	begin := time.Now()
	done := make(chan error, 1)
	var checks configDirCheckSet
	go func() { done <- verifyConfigDirBounded(context.Background(), backend.uid, &checks, backend, "") }()

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("the ownership check did not reach stat")
	}
	var err error
	select {
	case err = <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("the bounded ownership check did not return")
	}
	if err == nil {
		t.Fatal("the bounded ownership check reported success while stat was blocked")
	}
	if elapsed := time.Since(begin); elapsed < configDirVerifyTimeout {
		t.Fatalf("the bounded ownership check returned after %s, want at least %s", elapsed, configDirVerifyTimeout)
	}
}

// TestBlockedConfigDirCheckDoesNotHoldBrokerLock checks that the ownership
// check runs without Broker.mu: one account stuck in stat must not prevent
// another account from registering.
func TestBlockedConfigDirCheckDoesNotHoldBrokerLock(t *testing.T) {
	realUID := uint32(os.Geteuid())
	statStarted := make(chan struct{})
	releaseStat := make(chan struct{})
	var once sync.Once

	stat := func(string) (fs.FileInfo, error) {
		once.Do(func() { close(statStarted) })
		<-releaseStat
		return nil, errors.New("stat was released")
	}

	const stuckUID = 9001
	router := statBlockingRouter{SessionRouter: &fakeSessions{}, stuckUID: stuckUID, stat: stat}
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = router
		d.AllowCaller = func(Caller) bool { return true }
		d.ReadPeerCred = sequencePeerCreds(stuckUID, realUID)
	})
	t.Cleanup(func() { close(releaseStat) })

	dir := shortTempDir(t)
	// This registration stops in the ownership check, whose stat does not
	// return until the test releases it.
	startRegistrationAt(t, tb.client, dir, filepath.Join(dir, "stuck.sock"))
	select {
	case <-statStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("the stuck account's ownership check did not reach stat")
	}

	// Another account's registration must complete while the first check is
	// still blocked.
	fake := fakesession.New(t, fakesession.Options{Name: "other"})
	other := tb.newClient(t)
	rs := startRegistrationAt(t, other, fake.ConfigDir(), fake.SocketPath())
	rs.complete(3 * time.Second)

	// The blocked check itself is abandoned after the named timeout and
	// reported as a failure.
	waitForReg(t, "the blocked ownership check to time out", func() bool {
		for _, line := range tb.logLines(t) {
			if line["event"] != EvRegistryWatchFailed {
				continue
			}
			uid, ok := line["uid"].(float64)
			if ok && uint32(uid) == stuckUID {
				return true
			}
		}
		return false
	})
}

// TestBlockedConfigDirCheckKeepsOneGoroutinePerAccount checks that repeated
// registrations of one account cannot pile up ownership checks behind a stat
// that never returns. The first check holds the account's slot; later
// registrations wait for it in their own goroutines and fail when their budget
// expires, so only one check goroutine is alive.
func TestBlockedConfigDirCheckKeepsOneGoroutinePerAccount(t *testing.T) {
	const (
		stuckUID      = 9004
		registrations = 10
	)

	release := make(chan struct{})
	t.Cleanup(func() { close(release) })
	started := make(chan struct{})
	var startedOnce sync.Once
	var active, peak atomic.Int32
	stat := func(string) (fs.FileInfo, error) {
		n := active.Add(1)
		defer active.Add(-1)
		for {
			seen := peak.Load()
			if n <= seen || peak.CompareAndSwap(seen, n) {
				break
			}
		}
		startedOnce.Do(func() { close(started) })
		<-release
		return nil, errors.New("stat was released")
	}

	router := statBlockingRouter{SessionRouter: &fakeSessions{}, stuckUID: stuckUID, stat: stat}
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = router
		d.AllowCaller = func(Caller) bool { return true }
		d.ReadPeerCred = sequencePeerCreds(stuckUID)
	})

	dir := shortTempDir(t)
	for i := 0; i < registrations; i++ {
		startRegistrationAt(t, tb.client, dir, filepath.Join(dir, fmt.Sprintf("stuck-%d.sock", i)))
	}

	select {
	case <-started:
	case <-time.After(3 * time.Second):
		t.Fatal("the first ownership check did not reach stat")
	}

	// The other registrations wait for the account's slot within the same
	// budget as the check itself: none may fail before the first check reaches
	// its timeout, and each fails once its own budget expires. Without the
	// per-account limit each of them would start a goroutine behind the stuck
	// stat.
	expiredChecks := func() int {
		n := 0
		for _, line := range tb.logLines(t) {
			if line["event"] != EvRegistryWatchFailed {
				continue
			}
			uid, ok := line["uid"].(float64)
			if !ok || uint32(uid) != stuckUID {
				continue
			}
			if msg, _ := line["error"].(string); strings.Contains(msg, "context deadline exceeded") {
				n++
			}
		}
		return n
	}
	waitUntil := time.Now().Add(configDirVerifyTimeout / 2)
	for time.Now().Before(waitUntil) {
		time.Sleep(5 * time.Millisecond)
	}
	if got := expiredChecks(); got != 0 {
		t.Fatalf("registrations failed %d times before the budget expired, want 0", got)
	}
	deadline := time.Now().Add(2 * configDirVerifyTimeout)
	for time.Now().Before(deadline) {
		if expiredChecks() >= registrations {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	if got := expiredChecks(); got != registrations {
		t.Fatalf("registrations that timed out waiting for the check slot = %d, want %d", got, registrations)
	}
	if got := peak.Load(); got != 1 {
		t.Fatalf("ownership check goroutines alive at once = %d, want 1", got)
	}
}

// TestConcurrentConfigDirChecksAllTakeTheWatch checks that registrations of one
// account that arrive while an ownership check is running wait for the slot and
// then take their watch. A Broker restart reconnects every session of an
// account at once, so a later connection may not be left registered without a
// watch on its configuration directory.
func TestConcurrentConfigDirChecksAllTakeTheWatch(t *testing.T) {
	uid := uint32(os.Geteuid())
	const registrations = 5

	var checks atomic.Int32
	stat := func(path string) (fs.FileInfo, error) {
		checks.Add(1)
		time.Sleep(50 * time.Millisecond)
		return os.Stat(path)
	}

	router := statBlockingRouter{SessionRouter: &fakeSessions{}, stuckUID: uid, stat: stat}
	tb := newTestBroker(t, func(d *Deps) {
		d.Sessions = router
		d.AllowCaller = func(Caller) bool { return true }
		d.ReadPeerCred = sequencePeerCreds(uid)
	})

	dir := shortTempDir(t)
	for i := 0; i < registrations; i++ {
		startRegistrationAt(t, tb.client, dir, filepath.Join(dir, fmt.Sprintf("queued-%d.sock", i)))
	}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && checks.Load() < registrations {
		time.Sleep(5 * time.Millisecond)
	}
	if got := checks.Load(); got != registrations {
		t.Fatalf("ownership checks run = %d, want %d: a later registration was rejected instead of waiting for the slot", got, registrations)
	}

	watchRefs := func() int {
		tb.b.mu.Lock()
		defer tb.b.mu.Unlock()
		if w := tb.b.watches[watchKeyFor(uid, dir)]; w != nil {
			return w.refs
		}
		return 0
	}
	deadline = time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) && watchRefs() < registrations {
		time.Sleep(5 * time.Millisecond)
	}
	if got := watchRefs(); got != registrations {
		t.Fatalf("watch references = %d, want %d", got, registrations)
	}
}
