package link

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// manualTimers is an afterFunc seam for tests. It records callbacks and runs
// them only when the test calls fireAll.
type manualTimers struct {
	mu     sync.Mutex
	timers []*manualTimer
}

type manualTimer struct {
	f       func()
	fired   bool
	stopped bool
}

func (m *manualTimers) afterFunc(d time.Duration, f func()) func() bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	t := &manualTimer{f: f}
	m.timers = append(m.timers, t)
	return func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		if t.fired || t.stopped {
			return false
		}
		t.stopped = true
		return true
	}
}

func (m *manualTimers) fireAll() {
	m.mu.Lock()
	var fns []func()
	for _, t := range m.timers {
		if t.fired || t.stopped {
			continue
		}
		t.fired = true
		fns = append(fns, t.f)
	}
	m.mu.Unlock()

	for _, f := range fns {
		f()
	}
}

func (m *manualTimers) pending() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	n := 0
	for _, t := range m.timers {
		if !t.fired && !t.stopped {
			n++
		}
	}
	return n
}

// newTrackedPair returns an accepted tracked server connection and the client
// side of the same local TCP connection.
func newTrackedPair(t *testing.T, reg *unauthRegistry) (*trackedConn, net.Conn) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	tl := &trackingListener{Listener: ln, reg: reg}

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := tl.Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- c
	}()

	client, err := net.Dial("tcp", ln.Addr().String())
	if err != nil {
		_ = ln.Close()
		t.Fatalf("dial: %v", err)
	}

	var tc *trackedConn
	select {
	case c := <-accepted:
		tc = c.(*trackedConn)
	case err := <-acceptErr:
		_ = ln.Close()
		_ = client.Close()
		t.Fatalf("accept: %v", err)
	case <-time.After(3 * time.Second):
		_ = ln.Close()
		_ = client.Close()
		t.Fatal("timed out waiting for accept")
	}
	_ = ln.Close()

	t.Cleanup(func() {
		_ = tc.Close()
		_ = client.Close()
	})
	return tc, client
}

// expectEOF asserts that the client side observes the server connection close.
func expectEOF(t *testing.T, c net.Conn) {
	t.Helper()
	if err := c.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, 1)
	_, err := c.Read(buf)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("client read after server close = %v, want EOF", err)
	}
}

// roundTrip writes from the client and reads the same bytes on the tracked
// connection, proving both sides are still usable.
func roundTrip(t *testing.T, tc *trackedConn, client net.Conn) {
	t.Helper()
	msg := []byte("ping")
	if _, err := client.Write(msg); err != nil {
		t.Fatalf("client write: %v", err)
	}
	if err := tc.SetReadDeadline(time.Now().Add(3 * time.Second)); err != nil {
		t.Fatalf("set read deadline: %v", err)
	}
	buf := make([]byte, len(msg))
	if _, err := io.ReadFull(tc, buf); err != nil {
		t.Fatalf("server read: %v", err)
	}
	if string(buf) != string(msg) {
		t.Fatalf("server read %q, want %q", buf, msg)
	}
}

// newTestRegistry builds a registry with a discard logger and manual timers.
func newTestRegistry(max, maxPerSource int, timers *manualTimers) *unauthRegistry {
	return newUnauthRegistry(max, maxPerSource, 10*time.Second, slog.New(slog.NewTextHandler(io.Discard, nil)), timers.afterFunc)
}

func TestFirstFrameDeadlineClosesSilentConn(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(64, 8, timers)

	_, client := newTrackedPair(t, reg)
	if got := reg.count(); got != 1 {
		t.Fatalf("count after accept = %d, want 1", got)
	}

	timers.fireAll()

	expectEOF(t, client)
	if got := reg.count(); got != 0 {
		t.Fatalf("count after deadline = %d, want 0", got)
	}
}

func TestFirstFrameSeenStopsDeadline(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(64, 8, timers)

	tc, client := newTrackedPair(t, reg)
	tc.firstFrameSeen()

	timers.fireAll()

	roundTrip(t, tc, client)
	if got := reg.count(); got != 1 {
		t.Fatalf("count after first frame = %d, want 1", got)
	}
}

// TestCapEvictsOldest checks the global cap. It is rewritten for per-source
// limits: the old version opened three connections over real localhost sockets,
// which now share one source and would hit the per-source limit first, so it no
// longer exercised the global limit. Distinct sources keep the per-source limit
// out of the way; with the global cap at two, the third connection closes the
// oldest connection (the earliest of the tied busiest sources) and the two
// survivors stay open.
func TestCapEvictsOldest(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(2, 8, timers)

	c1 := &fakeSourceConn{remote: "198.51.100.1:4000"}
	c2 := &fakeSourceConn{remote: "198.51.100.2:4000"}
	c3 := &fakeSourceConn{remote: "198.51.100.3:4000"}
	reg.track(c1)
	reg.track(c2)
	reg.track(c3)

	if !c1.isClosed() {
		t.Fatal("oldest connection is open at the cap, want closed")
	}
	if c2.isClosed() || c3.isClosed() {
		t.Fatalf("survivors closed: c2=%v c3=%v, want both open", c2.isClosed(), c3.isClosed())
	}
	if got := reg.count(); got != 2 {
		t.Fatalf("count at cap = %d, want 2", got)
	}
}

func TestAuthenticatedLeavesRegistry(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(2, 8, timers)

	tc1, c1 := newTrackedPair(t, reg)
	tc1.authenticated()
	if got := reg.count(); got != 0 {
		t.Fatalf("count after authentication = %d, want 0", got)
	}
	if got := timers.pending(); got != 0 {
		t.Fatalf("pending deadlines after authentication = %d, want 0", got)
	}

	newTrackedPair(t, reg)
	newTrackedPair(t, reg)
	if got := reg.count(); got != 2 {
		t.Fatalf("count with two unauthenticated conns = %d, want 2", got)
	}

	roundTrip(t, tc1, c1)
}

func TestClaimStreamOnce(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(64, 8, timers)

	tc, _ := newTrackedPair(t, reg)
	if !tc.claimStream() {
		t.Fatal("first claimStream = false, want true")
	}
	if tc.claimStream() {
		t.Fatal("second claimStream = true, want false")
	}

	concurrent, _ := newTrackedPair(t, reg)
	var wins atomic.Int32
	var wg sync.WaitGroup
	start := make(chan struct{})
	for i := 0; i < 50; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			if concurrent.claimStream() {
				wins.Add(1)
			}
		}()
	}
	close(start)
	wg.Wait()
	if got := wins.Load(); got != 1 {
		t.Fatalf("concurrent claimStream winners = %d, want 1", got)
	}
}

// fakeSourceConn is a net.Conn with a fixed remote address and a recorded
// Close, so registry tests can place connections at chosen sources without
// opening sockets.
type fakeSourceConn struct {
	remote string
	mu     sync.Mutex
	closed bool
}

func (c *fakeSourceConn) Read([]byte) (int, error)         { return 0, io.EOF }
func (c *fakeSourceConn) Write(b []byte) (int, error)      { return len(b), nil }
func (c *fakeSourceConn) LocalAddr() net.Addr              { return testSourceAddr("127.0.0.1:1") }
func (c *fakeSourceConn) RemoteAddr() net.Addr             { return testSourceAddr(c.remote) }
func (c *fakeSourceConn) SetDeadline(time.Time) error      { return nil }
func (c *fakeSourceConn) SetReadDeadline(time.Time) error  { return nil }
func (c *fakeSourceConn) SetWriteDeadline(time.Time) error { return nil }

func (c *fakeSourceConn) Close() error {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *fakeSourceConn) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.closed
}

type testSourceAddr string

func (a testSourceAddr) Network() string { return "tcp" }
func (a testSourceAddr) String() string  { return string(a) }

// TestPerSourceLimitKeepsOtherSources reproduces the denial of service where a
// single source filling the shared unauthenticated pool evicted a connection
// from a different source. One connection from source B arrives first; source A
// then registers more connections than the global cap. B must stay open,
// because A's own connections are the ones over the per-source limit.
func TestPerSourceLimitKeepsOtherSources(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(64, 8, timers)

	b := &fakeSourceConn{remote: "198.51.100.7:4000"}
	reg.track(b)

	for i := 0; i < 64; i++ {
		reg.track(&fakeSourceConn{remote: fmt.Sprintf("203.0.113.7:%d", 5000+i)})
	}

	if b.isClosed() {
		t.Fatal("connection from source B was closed while source A filled the pool")
	}
}

// TestPerSourceLimitClosesOldestOfSource checks that a source over its own
// limit loses its oldest connection and nothing from another source.
func TestPerSourceLimitClosesOldestOfSource(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(64, 8, timers)

	other := &fakeSourceConn{remote: "198.51.100.7:4000"}
	reg.track(other)

	var same []*fakeSourceConn
	for i := 0; i < 9; i++ {
		c := &fakeSourceConn{remote: fmt.Sprintf("203.0.113.7:%d", 5000+i)}
		reg.track(c)
		same = append(same, c)
	}

	if !same[0].isClosed() {
		t.Fatal("oldest connection of the flooding source is open, want closed")
	}
	for i := 1; i < len(same); i++ {
		if same[i].isClosed() {
			t.Fatalf("connection %d of the flooding source closed, want open", i)
		}
	}
	if other.isClosed() {
		t.Fatal("connection from another source closed, want open")
	}
	if got := reg.count(); got != 9 {
		t.Fatalf("count = %d, want 9 (8 from the flooding source plus 1 other)", got)
	}
}

// TestGlobalLimitClosesOldestOfBusiestSource fills the global cap from eight
// sources, then adds a ninth source. The global limit closes the earliest
// connection of the earliest busiest source, and the ninth source survives.
func TestGlobalLimitClosesOldestOfBusiestSource(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(64, 8, timers)

	var first []*fakeSourceConn // the earliest connection of each source, in order
	for s := 0; s < 8; s++ {
		for i := 0; i < 8; i++ {
			c := &fakeSourceConn{remote: fmt.Sprintf("203.0.113.%d:%d", s+1, 5000+i)}
			reg.track(c)
			if i == 0 {
				first = append(first, c)
			}
		}
	}
	if got := reg.count(); got != 64 {
		t.Fatalf("count after filling the global cap = %d, want 64", got)
	}

	b := &fakeSourceConn{remote: "198.51.100.7:4000"}
	reg.track(b)

	if b.isClosed() {
		t.Fatal("new connection from source B closed, want open")
	}
	if !first[0].isClosed() {
		t.Fatal("earliest connection of the earliest busiest source is open, want closed")
	}
	for _, c := range first[1:] {
		if c.isClosed() {
			t.Fatal("connection of a later source closed, want only the earliest source affected")
		}
	}
	if got := reg.count(); got != 64 {
		t.Fatalf("count after global eviction = %d, want 64", got)
	}
}

// TestGlobalLimitSparesEarlierLighterSource checks that the global limit picks
// its victim by source load, not by age. Source B registers first but holds one
// connection; eight sources then fill the global cap with eight connections
// each. The connection closed must be the earliest connection of the earliest
// busiest source, not B, which is the oldest connection in the pool.
func TestGlobalLimitSparesEarlierLighterSource(t *testing.T) {
	timers := &manualTimers{}
	reg := newTestRegistry(64, 8, timers)

	b := &fakeSourceConn{remote: "198.51.100.7:4000"}
	reg.track(b)

	var first []*fakeSourceConn // the earliest connection of each source, in order
	for s := 0; s < 8; s++ {
		for i := 0; i < 8; i++ {
			c := &fakeSourceConn{remote: fmt.Sprintf("203.0.113.%d:%d", s+1, 5000+i)}
			reg.track(c)
			if i == 0 {
				first = append(first, c)
			}
		}
	}
	if got := reg.count(); got != 64 {
		t.Fatalf("count after filling the global cap = %d, want 64", got)
	}

	if b.isClosed() {
		t.Fatal("earliest connection overall, from the lighter source, was closed, want open")
	}
	if !first[0].isClosed() {
		t.Fatal("earliest connection of the earliest busiest source is open, want closed")
	}
	for _, c := range first[1:] {
		if c.isClosed() {
			t.Fatal("connection of a later busiest source closed, want only the earliest busiest source affected")
		}
	}
}

// TestEvictionLogsSourceAndCause checks the Debug record written for each
// eviction: the event name plus the source prefix and the cause.
func TestEvictionLogsSourceAndCause(t *testing.T) {
	tests := []struct {
		name         string
		max          int
		maxPerSource int
		remote       []string
		wantSource   string
		wantCause    string
	}{
		{
			name:         "source limit",
			max:          64,
			maxPerSource: 2,
			remote:       []string{"203.0.113.7:5000", "203.0.113.7:5001", "203.0.113.7:5002"},
			wantSource:   "203.0.113.7/32",
			wantCause:    "source_limit",
		},
		{
			name:         "global limit",
			max:          2,
			maxPerSource: 8,
			remote:       []string{"198.51.100.1:5000", "198.51.100.2:5000", "198.51.100.3:5000"},
			wantSource:   "198.51.100.1/32",
			wantCause:    "global_limit",
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var buf bytes.Buffer
			logger := slog.New(slog.NewJSONHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
			timers := &manualTimers{}
			reg := newUnauthRegistry(tt.max, tt.maxPerSource, 10*time.Second, logger, timers.afterFunc)

			for _, remote := range tt.remote {
				reg.track(&fakeSourceConn{remote: remote})
			}

			var found map[string]any
			for _, line := range strings.Split(strings.TrimSpace(buf.String()), "\n") {
				var entry map[string]any
				if err := json.Unmarshal([]byte(line), &entry); err != nil {
					t.Fatalf("decode log line %q: %v", line, err)
				}
				if entry["msg"] == "link: closed an unauthenticated connection" {
					found = entry
				}
			}
			if found == nil {
				t.Fatalf("no \"link: closed an unauthenticated connection\" record in %q", buf.String())
			}
			if got := found["source"]; got != tt.wantSource {
				t.Fatalf("log source = %v, want %q", got, tt.wantSource)
			}
			if got := found["cause"]; got != tt.wantCause {
				t.Fatalf("log cause = %v, want %q", got, tt.wantCause)
			}
		})
	}
}
