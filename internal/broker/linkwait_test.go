package broker

import (
	"context"
	"fmt"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// waitLink is a minimal deliveryLink for the waiter tests. Subscribe and
// Established have real behaviour; the other three methods are no-ops.
type waitLink struct {
	mu    sync.Mutex
	up    map[peerID]bool
	onUps []func(p peerID, salt []byte)
}

func newWaitLink() *waitLink {
	return &waitLink{up: make(map[peerID]bool)}
}

func (l *waitLink) Request(context.Context, peerID, link.Frame) (link.Frame, bool, error) {
	return link.Frame{}, false, nil
}

func (l *waitLink) HandleRequest(link.FrameKind, func(context.Context, peerID, link.Frame) link.Frame) {
}

func (l *waitLink) Subscribe(onUp func(p peerID, salt []byte), _ func(p peerID)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onUps = append(l.onUps, onUp)
}

func (l *waitLink) Established(p peerID) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.up[p]
}

func (l *waitLink) Kick(peerID) {}

// fireUp marks p established and runs the subscription callbacks, as the link
// does when a handshake completes.
func (l *waitLink) fireUp(p peerID) {
	l.mu.Lock()
	l.up[p] = true
	subs := append([]func(peerID, []byte){}, l.onUps...)
	l.mu.Unlock()
	for _, f := range subs {
		f(p, nil)
	}
}

var _ deliveryLink = (*waitLink)(nil)

// TestLinkWaiterAlreadyUp checks that an up event recorded before the wait
// makes it return true immediately instead of waiting for the timer.
func TestLinkWaiterAlreadyUp(t *testing.T) {
	l := newWaitLink()
	p := peerID{1}
	w := newLinkWaiter(l, nil)
	l.fireUp(p)

	start := time.Now()
	if !w.wait(context.Background(), p, time.Second, 0) {
		t.Fatal("wait returned false, want true")
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("wait took %v, want it to return without waiting", elapsed)
	}
}

// TestLinkWaiterWakesOnUp checks that an up event wakes a waiting caller well
// before the maximum, so a send does not burn its budget on the timer.
func TestLinkWaiterWakesOnUp(t *testing.T) {
	l := newWaitLink()
	p := peerID{2}
	w := newLinkWaiter(l, nil)

	done := make(chan bool, 1)
	go func() { done <- w.wait(context.Background(), p, time.Second) }()
	waitForRegistrations(t, w, p, 1)

	l.fireUp(p)

	select {
	case ok := <-done:
		if !ok {
			t.Fatal("wait returned false, want true")
		}
	case <-time.After(eventTimeout):
		t.Fatal("wait did not return in time")
	}
}

// TestLinkWaiterUpBetweenObserveAndWait checks the generation path: a caller
// records the generation it saw at a failure, an up event lands before wait
// registers, and the wait must still return at once instead of sleeping until
// the timer.
func TestLinkWaiterUpBetweenObserveAndWait(t *testing.T) {
	l := newWaitLink()
	p := peerID{3}
	w := newLinkWaiter(l, nil)

	since := w.generation(p)
	l.fireUp(p)

	start := time.Now()
	if !w.wait(context.Background(), p, 2*time.Second, since) {
		t.Fatal("wait returned false, want true")
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("wait took %v, want it woken by the up event", elapsed)
	}
}

// TestLinkWaiterIgnoresUpBeforeSince checks the other half of the rule: a wait
// that starts from a generation observed after a failure is not satisfied by an
// earlier up event, even though the link now reports itself as established.
func TestLinkWaiterIgnoresUpBeforeSince(t *testing.T) {
	l := newWaitLink()
	p := peerID{7}
	w := newLinkWaiter(l, nil)
	l.fireUp(p)
	since := w.generation(p)

	if w.wait(context.Background(), p, 20*time.Millisecond, since) {
		t.Fatal("wait returned true on an up event older than the observed generation")
	}
}

// TestLinkWaiterTimeout checks that wait gives up once the maximum elapses, so
// the dialer's backoff still bounds the send.
func TestLinkWaiterTimeout(t *testing.T) {
	l := newWaitLink()
	w := newLinkWaiter(l, nil)

	start := time.Now()
	if w.wait(context.Background(), peerID{4}, 20*time.Millisecond) {
		t.Fatal("wait returned true, want false")
	}
	if elapsed := time.Since(start); elapsed >= time.Second {
		t.Fatalf("wait took %v, want it bounded by 20ms", elapsed)
	}
}

// TestLinkWaiterContextCancel checks that a cancelled context returns false
// instead of blocking the caller.
func TestLinkWaiterContextCancel(t *testing.T) {
	l := newWaitLink()
	w := newLinkWaiter(l, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool, 1)
	go func() { done <- w.wait(ctx, peerID{5}, 2*time.Second) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("wait returned true, want false")
		}
	case <-time.After(eventTimeout):
		t.Fatal("wait did not return after the context ended")
	}
}

// staleLink models the disconnect window a real link has: the state handler
// has not run yet, so Established keeps reporting true while the transport
// already refuses frames. Until released, every Request fails without leaving
// the Broker; after release, the link delivers.
type staleLink struct {
	mu           sync.Mutex
	released     bool
	kicks        int
	onUps        []func(p peerID, salt []byte)
	firstFailure chan struct{}
	once         sync.Once
}

func newStaleLink() *staleLink {
	return &staleLink{firstFailure: make(chan struct{})}
}

func (l *staleLink) Request(context.Context, peerID, link.Frame) (link.Frame, bool, error) {
	l.mu.Lock()
	released := l.released
	l.mu.Unlock()
	if !released {
		l.once.Do(func() { close(l.firstFailure) })
		return link.Frame{}, false, fmt.Errorf("%w: the state handler has not run yet", link.ErrNotEstablished)
	}
	return encodeDeliverResult(nil), true, nil
}

func (l *staleLink) HandleRequest(link.FrameKind, func(context.Context, peerID, link.Frame) link.Frame) {
}

func (l *staleLink) Subscribe(onUp func(peerID, []byte), _ func(peerID)) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.onUps = append(l.onUps, onUp)
}

// Established stays true for the whole window, exactly like the real link.
func (l *staleLink) Established(peerID) bool { return true }

func (l *staleLink) Kick(peerID) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.kicks++
}

func (l *staleLink) kickCount() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.kicks
}

// release lets the transport deliver and announces the up event, as the link
// does once the redial finishes.
func (l *staleLink) release(p peerID) {
	l.mu.Lock()
	l.released = true
	subs := append([]func(peerID, []byte){}, l.onUps...)
	l.mu.Unlock()
	for _, f := range subs {
		f(p, nil)
	}
}

var _ deliveryLink = (*staleLink)(nil)

// TestSendWaitsForUpWhenEstablishedIsStale reproduces the disconnect window: the
// transport refuses the frame (sent=false) while Established still reads true.
// The sender must wait for the next up event instead of trusting that reading,
// or it burns every retry before the link can come back.
func TestSendWaitsForUpWhenEstablishedIsStale(t *testing.T) {
	l := newStaleLink()
	peer := fakenetPeer(20)
	callerID := "1a2b3c" + strings.Repeat("0", 58)
	caller := agentView{ID: callerID, Name: "api-worker", Ref: "1a2b3c"}

	topics := &deliveryTopics{
		local: map[string][]string{callerID: {"t1"}},
		remote: map[peerID][]deliveryRemoteEntry{peer: {{
			Member: remoteMember{Name: "frontend", Ref: "abc123"},
			Topics: []string{"t1"},
		}}},
	}
	peers := &deliveryPeers{
		aliases:   map[string]peerID{"hostB": peer},
		roles:     map[peerID]pairing.Role{peer: pairing.RoleDialer},
		effective: map[peerID]string{peer: "hostB"},
	}
	settings := newFakeSettings(sendSettings{
		RequestTimeout: 2 * time.Second,
		LinkBackoff:    20 * time.Millisecond,
		LinkRetries:    3,
		DedupeWindow:   time.Minute,
	}, "hostA")
	// The retry spacing is the behaviour under test, so the test advances it
	// itself. A wall-clock timer would race a sleep-based check under load.
	clock := newFakeClock(time.Now())
	d := newDelivery(deliveryDeps{
		Topics:      topics,
		Peers:       peers,
		Link:        l,
		Settings:    settings,
		WaiterClock: clock,
	})

	addr, ok := parseTarget("frontend@hostB")
	if !ok {
		t.Fatal("parseTarget rejected frontend@hostB")
	}
	done := make(chan error, 1)
	go func() {
		done <- d.sendToPeer(context.Background(), peerSend{
			Caller:    caller,
			Peer:      peer,
			Addr:      addr,
			MessageID: "3f2a1c4e-5b6d-4e8f-9a0b-1c2d3e4f5a6b",
			Text:      "hello",
			Deadline:  time.Now().Add(time.Hour),
		})
	}()

	select {
	case <-l.firstFailure:
	case <-time.After(eventTimeout):
		t.Fatal("the send never reached the transport")
	}

	// The sender must be blocked waiting for an up event now. One that reads
	// the stale Established value never registers a wait and gives up after
	// burning every retry, so the helper reports the send's own error.
	waitForWaitOrDone(t, d.waiter, peer, done)
	if got := l.kickCount(); got != 1 {
		t.Fatalf("kicks after the first failed request = %d, want 1", got)
	}

	// One backoff later the sender may try once more. It must kick once more
	// and wait again rather than spend the remaining retries at once.
	clock.Advance(20 * time.Millisecond)
	waitForKicks(t, l, 2)
	waitForWaitOrDone(t, d.waiter, peer, done)
	if got := l.kickCount(); got != 2 {
		t.Fatalf("kicks after one backoff = %d, want 2", got)
	}

	select {
	case err := <-done:
		t.Fatalf("sendToPeer gave up before the link came up: %v (kicks %d)", err, l.kickCount())
	default:
	}

	l.release(peer)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sendToPeer = %v, want nil after the link came up", err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("sendToPeer did not finish after the link came up")
	}
}

// TestLinkWaiterManyWaiters checks that one up event wakes every waiter of the
// same peer, not just one.
func TestLinkWaiterManyWaiters(t *testing.T) {
	l := newWaitLink()
	p := peerID{6}
	w := newLinkWaiter(l, nil)

	const n = 32
	results := make(chan bool, n)
	for i := 0; i < n; i++ {
		go func() {
			results <- w.wait(context.Background(), p, time.Second)
		}()
	}
	waitForRegistrations(t, w, p, n)
	l.fireUp(p)

	for i := 0; i < n; i++ {
		select {
		case ok := <-results:
			if !ok {
				t.Fatal("a waiter returned false, want true")
			}
		case <-time.After(eventTimeout):
			t.Fatal("a waiter did not return in time")
		}
	}
}

// waitForWaitOrDone waits until one waiter is blocked on p. It fails with the
// send's own error when the send gives up first, which is what a sender that
// trusts the stale Established value does instead of waiting for an up event.
func waitForWaitOrDone(t *testing.T, w *linkWaiter, p peerID, done <-chan error) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		w.mu.Lock()
		got := len(w.ch[p])
		w.mu.Unlock()
		if got > 0 {
			return
		}
		select {
		case err := <-done:
			t.Fatalf("sendToPeer gave up before the link came up: %v", err)
		default:
		}
		if !time.Now().Before(deadline) {
			t.Fatal("sendToPeer never waited for the link to come up")
		}
		runtime.Gosched()
	}
}

// waitForKicks waits until the link has been kicked n times.
func waitForKicks(t *testing.T, l *staleLink, n int) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for l.kickCount() < n {
		if !time.Now().Before(deadline) {
			t.Fatalf("kicks = %d, want %d", l.kickCount(), n)
		}
		runtime.Gosched()
	}
}

// waitForRegistrations waits until n waiters are blocked on p. It reads the
// waiter's own bookkeeping under its mutex, so firing an up event afterwards is
// certain to hit registered waiters and cannot be lost.
func waitForRegistrations(t *testing.T, w *linkWaiter, p peerID, n int) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		w.mu.Lock()
		got := len(w.ch[p])
		w.mu.Unlock()
		if got >= n {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("only %d of %d waiters registered for the peer", got, n)
		}
		runtime.Gosched()
	}
}
