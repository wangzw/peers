package broker

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wangzw/peers/internal/accountdb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// orderLog records events from the startup and shutdown sequence. Every
// assertion reads this one log, so the order does not depend on which goroutine
// observed a moment.
type orderLog struct {
	mu      sync.Mutex
	entries []string
}

func (o *orderLog) add(s string) {
	o.mu.Lock()
	o.entries = append(o.entries, s)
	o.mu.Unlock()
}

func (o *orderLog) list() []string {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]string(nil), o.entries...)
}

// fakeRunner is the LinkRunner the assemblers install. It records the
// lifecycle calls Run drives.
type fakeRunner struct {
	order     *orderLog
	listenErr error
	started   chan struct{} // closed by Start; tests wait on it when they need the call to have happened
}

var _ LinkRunner = (*fakeRunner)(nil)

func newFakeRunner(order *orderLog) *fakeRunner { return &fakeRunner{order: order} }

func (r *fakeRunner) Listen(address string) error {
	r.order.add("Listen " + address)
	return r.listenErr
}

func (r *fakeRunner) Start() {
	r.order.add("Start")
	if r.started != nil {
		close(r.started)
	}
}

func (r *fakeRunner) Close() { r.order.add("Close") }

// runWaitBound bounds every wait for Run to reach its next lifecycle step or
// to return. It is a liveness bound, not a latency budget: it fires only when
// Run is stuck, so a slow but correct startup must not trip it.
//
// The system form resolves broker.local_socket_group before it binds anything,
// and the account database bounds one lookup at accountdb.LookupBound, so that
// single step may legitimately take that long on a host whose NSS source is
// slow. Ten seconds is that bound plus three for the rest of startup and for
// scheduling delay on a loaded host.
const runWaitBound = 10 * time.Second

// TestRunWaitBoundCoversAccountLookup checks the invariant behind runWaitBound:
// a Run test must not give up on startup while the account database is still
// inside its own bound for one lookup. accountdb.LookupBound is the product
// bound (seven seconds on Linux: getentTimeout plus getentWaitDelay), and
// runWaitBound adds a startup margin to it. Lowering runWaitBound below the
// bound would reintroduce the "Run did not return" failure on a slow NSS
// source.
func TestRunWaitBoundCoversAccountLookup(t *testing.T) {
	const startupMargin = 3 * time.Second
	if want := accountdb.LookupBound + startupMargin; runWaitBound < want {
		t.Fatalf("runWaitBound = %v, want at least %v (accountdb.LookupBound %v plus %v startup margin)", runWaitBound, want, accountdb.LookupBound, startupMargin)
	}
}

// waitStarted blocks until Start ran, with the run liveness bound.
func (r *fakeRunner) waitStarted(t *testing.T) {
	t.Helper()
	select {
	case <-r.started:
	case <-time.After(runWaitBound):
		t.Fatal("link Start was not called")
	}
}

// orderedReady records each readiness write before passing it to the pipe.
type orderedReady struct {
	w     io.WriteCloser
	order *orderLog
}

func (r *orderedReady) Write(p []byte) (int, error) {
	r.order.add("ready " + strings.TrimSpace(string(p)))
	return r.w.Write(p)
}

func (r *orderedReady) Close() error { return r.w.Close() }

// orderComponent records Start and Stop. It always implements
// PostReadyComponent, but AfterReady records only when afterReady is set, so a
// plain component stays out of the recorded sequence.
type orderComponent struct {
	name           string
	order          *orderLog
	startErr       error
	afterReady     bool
	afterReadyErr  error
	afterReadyDone chan struct{}
	captured       chan *Broker
}

var _ PostReadyComponent = (*orderComponent)(nil)

func (c *orderComponent) Name() string { return c.name }

func (c *orderComponent) Start(_ context.Context, b *Broker) error {
	c.order.add(c.name + ".Start")
	if c.captured != nil {
		c.captured <- b
	}
	return c.startErr
}

func (c *orderComponent) Stop() { c.order.add(c.name + ".Stop") }

func (c *orderComponent) AfterReady(context.Context) error {
	if c.afterReady {
		c.order.add(c.name + ".AfterReady")
	}
	if c.afterReadyDone != nil {
		close(c.afterReadyDone)
	}
	return c.afterReadyErr
}

// fakeAssembler is an Assembler backed by a function.
type fakeAssembler struct {
	name string
	fn   func(ctx context.Context, cfg ConfigSource, deps *Deps, asm *Assembly) ([]Component, error)
}

var _ Assembler = (*fakeAssembler)(nil)

func (a *fakeAssembler) Name() string { return a.name }

func (a *fakeAssembler) Assemble(ctx context.Context, cfg ConfigSource, deps *Deps, asm *Assembly) ([]Component, error) {
	return a.fn(ctx, cfg, deps, asm)
}

// checkErrConfig makes CheckRuntimeDir fail while delegating everything else.
type checkErrConfig struct {
	ConfigSource
	err error
}

func (c checkErrConfig) CheckRuntimeDir() error { return c.err }

// runTest drives one Run in a goroutine with a readiness pipe, a fake clockless
// configuration and a controllable periodic-check channel.
type runTest struct {
	t      *testing.T
	ctx    context.Context
	cancel context.CancelFunc
	dir    string
	cfg    *fakeConfig
	topics *fakeTopics
	logs   *syncBuffer
	opts   RunOptions

	readyR *os.File
	readyW *os.File
	reader *bufio.Reader
	ticks  chan time.Time
	checks chan struct{}

	done    chan error
	started bool
	waited  bool
}

// newRunTest creates a fresh runtime directory for the test.
func newRunTest(t *testing.T, mutate ...func(*runTest)) *runTest {
	t.Helper()
	dir, err := os.MkdirTemp("", "ap-")
	if err != nil {
		t.Fatalf("MkdirTemp: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	return newRunTestAt(t, dir, mutate...)
}

// newRunTestAt uses an existing directory, so a test can share the account lock
// path between two Runs.
func newRunTestAt(t *testing.T, dir string, mutate ...func(*runTest)) *runTest {
	t.Helper()
	cfg := newFakeConfig(dir)
	topics := newFakeTopics()
	logs := &syncBuffer{}
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("os.Pipe: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	rt := &runTest{
		t:      t,
		ctx:    ctx,
		cancel: cancel,
		dir:    dir,
		cfg:    cfg,
		topics: topics,
		logs:   logs,
		readyR: r,
		readyW: w,
		reader: bufio.NewReader(r),
		ticks:  make(chan time.Time, 4),
		checks: make(chan struct{}, 4),
	}
	rt.opts = RunOptions{
		LogOutput:  logs,
		Ready:      w,
		Assemblers: []Assembler{},
		Deps: Deps{
			Config:    cfg,
			Topics:    topics,
			InSession: func() bool { return false },
		},
		checkTick:  rt.ticks,
		afterCheck: func() { rt.checks <- struct{}{} },
	}
	for _, m := range mutate {
		m(rt)
	}
	t.Cleanup(rt.stop)
	return rt
}

func (rt *runTest) start() {
	rt.t.Helper()
	rt.started = true
	rt.done = make(chan error, 1)
	go func() { rt.done <- Run(rt.ctx, rt.opts) }()
}

func (rt *runTest) socketPath() string { return filepath.Join(rt.dir, "broker.sock") }

// readyLine reads one readiness line, bounded by the run liveness bound.
func (rt *runTest) readyLine() string {
	rt.t.Helper()
	if err := rt.readyR.SetReadDeadline(time.Now().Add(runWaitBound)); err != nil {
		rt.t.Fatalf("SetReadDeadline: %v", err)
	}
	line, err := rt.reader.ReadString('\n')
	if err != nil {
		rt.t.Fatalf("read readiness line: %v", err)
	}
	return line
}

func (rt *runTest) waitErr() error {
	rt.t.Helper()
	select {
	case err := <-rt.done:
		rt.waited = true
		return err
	case <-time.After(runWaitBound):
		rt.t.Fatal("Run did not return")
		return nil
	}
}

func (rt *runTest) stop() {
	rt.cancel()
	if rt.started && !rt.waited {
		select {
		case err := <-rt.done:
			_ = err
			rt.waited = true
		case <-time.After(runWaitBound):
			rt.t.Errorf("Run did not return after cancel")
		}
	}
	_ = rt.readyR.Close()
	_ = rt.readyW.Close()
}

func helloAt(t *testing.T, sock string, version uint32) (*localpb.HelloResponse, error) {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	defer func() { _ = cc.Close() }()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	return localpb.NewLocalClient(cc).Hello(ctx, &localpb.HelloRequest{ProtocolVersion: version})
}

func equalStrings(a, b []string) bool {
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

func indexOf(list []string, want string) int {
	for i, s := range list {
		if s == want {
			return i
		}
	}
	return -1
}

func countString(list []string, want string) int {
	n := 0
	for _, s := range list {
		if s == want {
			n++
		}
	}
	return n
}

func logsHaveEvent(t *testing.T, raw string, event string) bool {
	t.Helper()
	for _, rec := range decodeLogLines(t, raw) {
		if rec["event"] == event {
			return true
		}
	}
	return false
}

func TestRunWritesOKAfterSocketBound(t *testing.T) {
	rt := newRunTest(t)
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want %q", got, "ok\n")
	}
	fi, err := os.Stat(rt.socketPath())
	if err != nil {
		t.Fatalf("stat broker.sock: %v", err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Errorf("socket mode = %o, want 600", got)
	}
	if _, err := helloAt(t, rt.socketPath(), localpb.ProtocolVersion); err != nil {
		t.Fatalf("Hello after ok: %v", err)
	}

	if err := rt.readyR.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("SetReadDeadline: %v", err)
	}
	if _, err := rt.reader.ReadString('\n'); !errors.Is(err, io.EOF) {
		t.Fatalf("read after ok = %v, want EOF (the pipe must be closed)", err)
	}
}

func TestRunSecondInstanceFails(t *testing.T) {
	// REQ-broker-config-013: one instance per account.
	first := newRunTest(t)
	first.start()
	if got := first.readyLine(); got != "ok\n" {
		t.Fatalf("first readiness line = %q", got)
	}
	before, err := os.Stat(first.socketPath())
	if err != nil {
		t.Fatalf("stat broker.sock: %v", err)
	}

	second := newRunTestAt(t, first.dir, func(rt *runTest) {
		rt.opts.Listen = "127.0.0.1:17444"
	})
	second.start()
	err = second.waitErr()
	if !errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("second Run error = %v, want ErrAlreadyRunning", err)
	}
	if got := second.readyLine(); got != err.Error()+"\n" {
		t.Errorf("second readiness line = %q, want %q", got, err.Error()+"\n")
	}

	if _, err := helloAt(t, first.socketPath(), localpb.ProtocolVersion); err != nil {
		t.Fatalf("first Broker stopped serving: %v", err)
	}
	after, err := os.Stat(first.socketPath())
	if err != nil {
		t.Fatalf("stat broker.sock after second Run: %v", err)
	}
	if !os.SameFile(before, after) {
		t.Error("the second Run replaced the first instance's socket")
	}
}

func TestRunRefusesInsideSession(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Deps.InSession = func() bool { return true }
	})
	rt.start()

	err := rt.waitErr()
	if !errors.Is(err, ErrInSession) {
		t.Fatalf("Run error = %v, want ErrInSession", err)
	}
	if got := rt.readyLine(); !strings.Contains(got, "parent processes") {
		t.Errorf("readiness line = %q, want it to mention the parent processes", got)
	} else if strings.Contains(got, "CLAUDE_CODE_MESSAGING_SOCKET") {
		t.Errorf("readiness line = %q, want it to avoid naming the environment variable", got)
	}
	lock, err := AcquireAccountLock(rt.cfg.AccountLock())
	if err != nil {
		t.Fatalf("the lock was not released: %v", err)
	}
	lock.Release()
	if _, err := os.Stat(rt.socketPath()); !os.IsNotExist(err) {
		t.Errorf("broker.sock exists, want no socket (stat error %v)", err)
	}
}

func TestRunSentinels(t *testing.T) {
	t.Run("runtime dir", func(t *testing.T) {
		rt := newRunTest(t)
		rt.opts.Deps.Config = checkErrConfig{ConfigSource: rt.cfg, err: errors.New("permission denied")}
		rt.start()

		err := rt.waitErr()
		if !errors.Is(err, ErrRuntimeDir) {
			t.Fatalf("Run error = %v, want ErrRuntimeDir", err)
		}
		if got := rt.readyLine(); got != err.Error()+"\n" {
			t.Errorf("readiness line = %q, want %q", got, err.Error()+"\n")
		}
	})

	loadErr := func() error { return fmt.Errorf("%w: broker.listen: bad", config.ErrInvalidValue) }
	cases := []struct {
		name    string
		listen  string
		loadErr error
		want    error
	}{
		{name: "invalid listen", listen: "x", loadErr: loadErr(), want: ErrInvalidListen},
		{name: "listen error without override", listen: "", loadErr: loadErr(), want: ErrInvalidConfig},
		{name: "other load error", listen: "", loadErr: errors.New("yaml"), want: ErrInvalidConfig},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			rt := newRunTest(t, func(rt *runTest) { rt.opts.Listen = tc.listen })
			rt.cfg.loadErr = tc.loadErr
			rt.start()

			err := rt.waitErr()
			if !errors.Is(err, tc.want) {
				t.Fatalf("Run error = %v, want %v", err, tc.want)
			}
			if got := rt.readyLine(); got != err.Error()+"\n" {
				t.Errorf("readiness line = %q, want %q", got, err.Error()+"\n")
			}
		})
	}
}

func TestRunListensBeforeOKAndStartsAfter(t *testing.T) {
	// REQ-broker-config-024: bind the link address before reporting ready and
	// dial only after the local socket is ready.
	order := &orderLog{}
	runner := newFakeRunner(order)
	runner.started = make(chan struct{})
	rt := newRunTest(t, func(rt *runTest) {
		rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:17443"})
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
			asm.Runner = runner
			return nil, nil
		}}}
		rt.opts.Components = []Component{&orderComponent{name: "C", order: order}}
		rt.opts.Ready = &orderedReady{w: rt.readyW, order: order}
	})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}
	runner.waitStarted(t)
	want := []string{"C.Start", "Listen 127.0.0.1:17443", "ready ok", "Start"}
	if got := order.list(); !equalStrings(got, want) {
		t.Fatalf("startup order = %v, want %v", got, want)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	got := order.list()
	closeIdx, stopIdx := indexOf(got, "Close"), indexOf(got, "C.Stop")
	if closeIdx < 0 || stopIdx < 0 {
		t.Fatalf("shutdown order = %v, want both Close and C.Stop", got)
	}
	if closeIdx > stopIdx {
		t.Errorf("shutdown order = %v, want link Close before C.Stop", got)
	}
}

func TestRunEmptyListenSkipsListen(t *testing.T) {
	order := &orderLog{}
	runner := newFakeRunner(order)
	runner.started = make(chan struct{})
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
			asm.Runner = runner
			return nil, nil
		}}}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}
	runner.waitStarted(t)

	got := order.list()
	for _, e := range got {
		if strings.HasPrefix(e, "Listen ") {
			t.Errorf("order = %v, want no Listen for an empty broker.listen", got)
		}
	}
	if indexOf(got, "Start") < 0 {
		t.Errorf("order = %v, want link Start", got)
	}
}

func TestRunListenFailure(t *testing.T) {
	// REQ-broker-config-024: a bind failure aborts startup.
	order := &orderLog{}
	runner := newFakeRunner(order)
	runner.listenErr = errors.New("address already in use")
	rt := newRunTest(t, func(rt *runTest) {
		rt.cfg.setValue("broker.listen", config.Value{Kind: config.KindString, String: "127.0.0.1:17443"})
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
			asm.Runner = runner
			return nil, nil
		}}}
		rt.opts.Components = []Component{&orderComponent{name: "C", order: order}}
	})
	rt.start()

	err := rt.waitErr()
	if !errors.Is(err, ErrListenUnavailable) {
		t.Fatalf("Run error = %v, want ErrListenUnavailable", err)
	}
	for _, want := range []string{"127.0.0.1:17443", "address already in use"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to contain %q", err, want)
		}
	}
	if got := rt.readyLine(); got != err.Error()+"\n" {
		t.Errorf("readiness line = %q, want %q", got, err.Error()+"\n")
	}
	if _, statErr := os.Stat(rt.socketPath()); !os.IsNotExist(statErr) {
		t.Errorf("broker.sock exists after a listen failure")
	}
	if n := countString(order.list(), "C.Stop"); n != 1 {
		t.Errorf("C.Stop called %d times, want 1", n)
	}
	if indexOf(order.list(), "Close") < 0 {
		t.Errorf("order = %v, want link Close", order.list())
	}
	lock, lockErr := AcquireAccountLock(rt.cfg.AccountLock())
	if lockErr != nil {
		t.Fatalf("the lock was not released: %v", lockErr)
	}
	lock.Release()
}

func TestRunAssemblersRunAfterLoadBeforeNew(t *testing.T) {
	replacement := newFakeTopics()
	captured := make(chan *Broker, 1)
	sawLoad := false
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "swap", fn: func(_ context.Context, _ ConfigSource, deps *Deps, _ *Assembly) ([]Component, error) {
			sawLoad = len(rt.cfg.LoadCalls()) > 0
			deps.Topics = replacement
			return nil, nil
		}}}
		rt.opts.Components = []Component{&orderComponent{name: "C", order: &orderLog{}, captured: captured}}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	var b *Broker
	select {
	case b = <-captured:
	case <-time.After(3 * time.Second):
		t.Fatal("the component Start was not called")
	}
	if !sawLoad {
		t.Error("the assembler ran before Config.Load")
	}
	if b.deps.Topics != replacement {
		t.Error("the assembler's Deps change did not reach New")
	}
}

func TestRunAssemblerOrderAndAssembly(t *testing.T) {
	order := &orderLog{}
	p := &pairing.Pairing{}
	secondSawSame := false
	asm1 := &fakeAssembler{name: "a1", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
		asm.Pairing = p
		order.add("a1")
		return []Component{&orderComponent{name: "X1", order: order}}, nil
	}}
	asm2 := &fakeAssembler{name: "a2", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
		secondSawSame = asm.Pairing == p
		order.add("a2")
		return []Component{&orderComponent{name: "X2", order: order}}, nil
	}}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Assemblers = []Assembler{asm1, asm2}
		rt.opts.Components = []Component{&orderComponent{name: "Y", order: order}}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}
	if !secondSawSame {
		t.Error("the second assembler did not see the first assembler's Assembly")
	}
	wantStart := []string{"a1", "a2", "X1.Start", "X2.Start", "Y.Start"}
	if got := order.list(); !equalStrings(got, wantStart) {
		t.Fatalf("startup order = %v, want %v", got, wantStart)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	got := order.list()
	wantStop := []string{"Y.Stop", "X2.Stop", "X1.Stop"}
	if len(got) < len(wantStop) || !equalStrings(got[len(got)-len(wantStop):], wantStop) {
		t.Errorf("shutdown order = %v, want it to end with %v", got, wantStop)
	}
}

func TestRunAssemblerFailureStopsEarlier(t *testing.T) {
	order := &orderLog{}
	asm1 := &fakeAssembler{name: "first", fn: func(context.Context, ConfigSource, *Deps, *Assembly) ([]Component, error) {
		return []Component{&orderComponent{name: "X", order: order}}, nil
	}}
	asm2 := &fakeAssembler{name: "second", fn: func(context.Context, ConfigSource, *Deps, *Assembly) ([]Component, error) {
		return nil, errors.New("open identity: permission denied")
	}}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Assemblers = []Assembler{asm1, asm2}
	})
	rt.start()

	err := rt.waitErr()
	if err == nil {
		t.Fatal("Run succeeded, want the assembler error")
	}
	for _, want := range []string{"second", "open identity: permission denied"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to contain %q", err, want)
		}
	}
	if got := rt.readyLine(); got != err.Error()+"\n" {
		t.Errorf("readiness line = %q, want %q", got, err.Error()+"\n")
	}
	list := order.list()
	if n := countString(list, "X.Stop"); n != 1 {
		t.Errorf("X.Stop called %d times, want 1", n)
	}
	if indexOf(list, "X.Start") >= 0 {
		t.Errorf("order = %v, want X never started", list)
	}
	if _, statErr := os.Stat(rt.socketPath()); !os.IsNotExist(statErr) {
		t.Errorf("broker.sock exists after an assembler failure")
	}
	lock, lockErr := AcquireAccountLock(rt.cfg.AccountLock())
	if lockErr != nil {
		t.Fatalf("the lock was not released: %v", lockErr)
	}
	lock.Release()
}

func TestRunNilAssemblersUsesDefaults(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) { rt.opts.Assemblers = nil })
	rt.start()
	// A nil list selects DefaultAssemblers. Its first entry is the peer
	// assembler, which opens the real pairing identity; that needs the real
	// config singleton, which the fake ConfigSource does not provide. The
	// readiness line naming the assembler proves Run used the defaults rather
	// than starting with none.
	got := rt.readyLine()
	if !strings.HasPrefix(got, "pairing and link: ") {
		t.Fatalf("readiness line = %q, want the default assembler's failure", got)
	}
	if !strings.Contains(got, "loading identity and peer table") {
		t.Fatalf("readiness line = %q, want it to name loading identity and peer table", got)
	}
	rt.cancel()
	if err := rt.waitErr(); err == nil {
		t.Fatal("Run error = nil, want the default assembler's failure")
	}
}

func TestRunPassesLinkProtocolVersion(t *testing.T) {
	for _, version := range []uint32{9, 0} {
		t.Run(fmt.Sprintf("version %d", version), func(t *testing.T) {
			seen := make(chan uint32, 2)
			mk := func(name string) Assembler {
				return &fakeAssembler{name: name, fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
					seen <- asm.LinkProtocolVersion
					return nil, nil
				}}
			}
			rt := newRunTest(t, func(rt *runTest) {
				rt.opts.LinkProtocolVersion = version
				rt.opts.Assemblers = []Assembler{mk("a1"), mk("a2")}
			})
			rt.start()
			if got := rt.readyLine(); got != "ok\n" {
				t.Fatalf("readiness line = %q", got)
			}
			for i := 0; i < 2; i++ {
				select {
				case got := <-seen:
					if got != version {
						t.Errorf("assembler %d saw LinkProtocolVersion %d, want %d", i, got, version)
					}
				case <-time.After(3 * time.Second):
					t.Fatal("an assembler did not run")
				}
			}
		})
	}
}

func TestRunComponentFailureReportsReason(t *testing.T) {
	order := &orderLog{}
	c1 := &orderComponent{name: "C1", order: order}
	c2 := &orderComponent{name: "C2", order: order, startErr: errors.New("boom")}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Components = []Component{c1, c2}
	})
	rt.start()

	err := rt.waitErr()
	if err == nil {
		t.Fatal("Run succeeded, want the component error")
	}
	for _, want := range []string{"C2", "boom"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to contain %q", err, want)
		}
	}
	if got := rt.readyLine(); got != err.Error()+"\n" {
		t.Errorf("readiness line = %q, want %q", got, err.Error()+"\n")
	}
	list := order.list()
	if n := countString(list, "C1.Stop"); n != 1 {
		t.Errorf("C1.Stop called %d times, want 1", n)
	}
	// The failing component is stopped too, so nothing it built survives a
	// failed startup.
	if n := countString(list, "C2.Stop"); n != 1 {
		t.Errorf("C2.Stop called %d times, want 1", n)
	}
	if _, statErr := os.Stat(rt.socketPath()); !os.IsNotExist(statErr) {
		t.Errorf("broker.sock exists after a component failure")
	}
	lock, lockErr := AcquireAccountLock(rt.cfg.AccountLock())
	if lockErr != nil {
		t.Fatalf("the lock was not released: %v", lockErr)
	}
	lock.Release()
}

func TestRunPassesListenOverride(t *testing.T) {
	rt := newRunTest(t, func(rt *runTest) { rt.opts.Listen = "127.0.0.1:0" })
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	calls := rt.cfg.LoadCalls()
	if len(calls) != 1 {
		t.Fatalf("Load called %d times, want 1", len(calls))
	}
	if calls[0].Mode != config.ModeUser {
		t.Errorf("Load mode = %v, want config.ModeUser", calls[0].Mode)
	}
	if calls[0].ListenOverride != "127.0.0.1:0" {
		t.Errorf("ListenOverride = %q, want %q", calls[0].ListenOverride, "127.0.0.1:0")
	}
}

func TestRunPostReadyAfterOK(t *testing.T) {
	order := &orderLog{}
	runner := newFakeRunner(order)
	done := make(chan struct{})
	p := &orderComponent{name: "P", order: order, afterReady: true, afterReadyDone: done}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Assemblers = []Assembler{&fakeAssembler{name: "link", fn: func(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
			asm.Runner = runner
			return nil, nil
		}}}
		rt.opts.Components = []Component{p}
		rt.opts.Ready = &orderedReady{w: rt.readyW, order: order}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Fatal("AfterReady was not called")
	}
	list := order.list()
	readyIdx := indexOf(list, "ready ok")
	startIdx := indexOf(list, "Start")
	afterIdx := indexOf(list, "P.AfterReady")
	if readyIdx < 0 || startIdx < 0 || afterIdx < 0 {
		t.Fatalf("order = %v, want ready ok, Start and P.AfterReady", list)
	}
	if readyIdx > afterIdx {
		t.Errorf("order = %v, want the readiness write before AfterReady", list)
	}
	if startIdx > afterIdx {
		t.Errorf("order = %v, want link Start before AfterReady", list)
	}
}

func TestRunRebindsMissingSocket(t *testing.T) {
	rt := newRunTest(t)
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	if err := os.Remove(rt.socketPath()); err != nil {
		t.Fatalf("remove broker.sock: %v", err)
	}
	rt.ticks <- time.Now()
	select {
	case <-rt.checks:
	case <-time.After(3 * time.Second):
		t.Fatal("the periodic check did not run")
	}

	if _, err := helloAt(t, rt.socketPath(), localpb.ProtocolVersion); err != nil {
		t.Fatalf("Hello after rebind: %v", err)
	}
	if !logsHaveEvent(t, rt.logs.String(), EvLocalSocketRebound) {
		t.Errorf("the log has no %s event", EvLocalSocketRebound)
	}
}

func TestRunExitsWhenLockReplaced(t *testing.T) {
	rt := newRunTest(t)
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	lockPath := rt.cfg.AccountLock()
	if err := os.Remove(lockPath); err != nil {
		t.Fatalf("remove lock: %v", err)
	}
	if err := os.WriteFile(lockPath, []byte("replacement"), 0o600); err != nil {
		t.Fatalf("replace lock: %v", err)
	}
	rt.ticks <- time.Now()

	err := rt.waitErr()
	if !errors.Is(err, ErrLockLost) {
		t.Fatalf("Run error = %v, want ErrLockLost", err)
	}
	if _, statErr := os.Stat(rt.socketPath()); !os.IsNotExist(statErr) {
		t.Errorf("broker.sock exists after the lock was lost")
	}
}

func TestRunReplacesStaleSocket(t *testing.T) {
	rt := newRunTest(t)
	ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: rt.socketPath(), Net: "unix"})
	if err != nil {
		t.Fatalf("create stale socket: %v", err)
	}
	ln.SetUnlinkOnClose(false)
	if err := ln.Close(); err != nil {
		t.Fatalf("close stale socket: %v", err)
	}

	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}
	if _, err := helloAt(t, rt.socketPath(), localpb.ProtocolVersion); err != nil {
		t.Fatalf("Hello after replacing the stale socket: %v", err)
	}
}

func TestRunShutdownOnCancel(t *testing.T) {
	rt := newRunTest(t)
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if _, err := os.Stat(rt.socketPath()); !os.IsNotExist(err) {
		t.Errorf("broker.sock still exists after shutdown")
	}
	if _, err := os.Stat(rt.cfg.AccountLock()); err != nil {
		t.Errorf("the account lock file was removed: %v", err)
	}
	lock, err := AcquireAccountLock(rt.cfg.AccountLock())
	if err != nil {
		t.Fatalf("the lock is still held after shutdown: %v", err)
	}
	lock.Release()
	if got := rt.cfg.CloseCount(); got != 1 {
		t.Errorf("Config.Close called %d times, want 1", got)
	}
}

func TestRunLogIsJSON(t *testing.T) {
	// REQ-broker-config-011: one JSON object per log line.
	rt := newRunTest(t, func(rt *runTest) { rt.opts.Version = "v9.9.9" })
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}
	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}

	records := decodeLogLines(t, rt.logs.String())
	var sawStarted, sawStopping bool
	for _, rec := range records {
		switch rec["event"] {
		case EvBrokerStarted:
			sawStarted = true
			if rec["version"] != "v9.9.9" {
				t.Errorf("broker_started version = %v, want v9.9.9", rec["version"])
			}
			if _, ok := rec["time"]; !ok {
				t.Errorf("broker_started has no time field: %v", rec)
			}
		case EvBrokerStopping:
			sawStopping = true
		}
	}
	if !sawStarted {
		t.Error("the log has no broker_started event")
	}
	if !sawStopping {
		t.Error("the log has no broker_stopping event")
	}
}

func TestRunLogLevelFollowsConfig(t *testing.T) {
	captured := make(chan *Broker, 1)
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.Components = []Component{&orderComponent{name: "C", order: &orderLog{}, captured: captured}}
	})
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	var b *Broker
	select {
	case b = <-captured:
	case <-time.After(3 * time.Second):
		t.Fatal("the component Start was not called")
	}
	rt.cfg.setValue("log.level", config.Value{Kind: config.KindString, String: "debug"})
	if !b.Logger().Enabled(context.Background(), slog.LevelDebug) {
		t.Error("a log.level change to debug did not take effect")
	}
}

func TestRunSetsDefaultLogger(t *testing.T) {
	// REQ-broker-config-011: with no injected logger, Run installs the JSON
	// logger as the process default, so config and inbox log through it too.
	previous := slog.Default()
	t.Cleanup(func() { slog.SetDefault(previous) })

	rt := newRunTest(t)
	rt.start()
	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q", got)
	}

	slog.Info("probe")
	rt.cancel()
	if err := rt.waitErr(); err != nil {
		t.Fatalf("Run error = %v, want nil", err)
	}
	if !logsHaveEvent(t, rt.logs.String(), "probe") {
		t.Fatalf("the process default logger did not write probe to LogOutput: %s", rt.logs.String())
	}
}
