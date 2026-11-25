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

// TestLinkWaiterAlreadyUp checks that an up event recorded before the wait makes
// it return true without arming a timer.
func TestLinkWaiterAlreadyUp(t *testing.T) {
	l := newWaitLink()
	p := peerID{1}
	clock := newFakeClock(time.Now())
	w := newLinkWaiter(l, clock)
	l.fireUp(p)

	if !w.wait(context.Background(), p, time.Second, 0) {
		t.Fatal("wait returned false, want true")
	}
	if got := clock.createdTimers(); got != 0 {
		t.Fatalf("wait armed %d timer(s), want it to return on the recorded up event", got)
	}
}

// TestLinkWaiterWakesOnUp checks that an up event wakes a waiting caller before its maximum elapses.
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

// TestLinkWaiterUpBetweenObserveAndWait checks that an up event arriving after the observed generation but before the wait returns it true.
func TestLinkWaiterUpBetweenObserveAndWait(t *testing.T) {
	l := newWaitLink()
	p := peerID{3}
	clock := newFakeClock(time.Now())
	w := newLinkWaiter(l, clock)

	since := w.generation(p)
	l.fireUp(p)

	if !w.wait(context.Background(), p, 2*time.Second, since) {
		t.Fatal("wait returned false, want true")
	}
	if got := clock.createdTimers(); got != 0 {
		t.Fatalf("wait armed %d timer(s), want it to return on the up event that followed the observed generation", got)
	}
}

// TestLinkWaiterIgnoresUpBeforeSince checks that an up event older than the observed generation does not satisfy the wait.
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

// TestLinkWaiterTimeout checks that wait returns false once its maximum elapses.
func TestLinkWaiterTimeout(t *testing.T) {
	l := newWaitLink()
	p := peerID{4}
	clock := newFakeClock(time.Now())
	w := newLinkWaiter(l, clock)

	done := make(chan bool, 1)
	go func() { done <- w.wait(context.Background(), p, 20*time.Millisecond) }()
	waitForRegistrations(t, w, p, 1)
	clock.Advance(20 * time.Millisecond)
	select {
	case ok := <-done:
		if ok {
			t.Fatal("wait returned true, want false once the maximum elapsed")
		}
	case <-time.After(eventTimeout):
		t.Fatal("wait did not return after its maximum elapsed")
	}
}

// TestLinkWaiterContextCancel checks that a cancelled context returns false.
func TestLinkWaiterContextCancel(t *testing.T) {
	l := newWaitLink()
	w := newLinkWaiter(l, nil)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	done := make(chan bool, 1)
	go func() { done <- w.wait(ctx, peerID{5}, time.Hour) }()
	select {
	case ok := <-done:
		if ok {
			t.Fatal("wait returned true, want false")
		}
	case <-time.After(eventTimeout):
		t.Fatal("wait did not return after the context ended")
	}
}

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

// TestSendWaitsForUpWhenEstablishedIsStale checks that a sender waits for the next up event while the transport refuses frames even though Established still reads true.
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

	waitForWaitOrDone(t, d.waiter, peer, done)
	if got := l.kickCount(); got != 1 {
		t.Fatalf("kicks after the first failed request = %d, want 1", got)
	}

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

// TestLinkWaiterManyWaiters checks that one up event wakes every waiter of the peer.
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

func waitForRegistrations(t *testing.T, w *linkWaiter, p peerID, n int) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
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
