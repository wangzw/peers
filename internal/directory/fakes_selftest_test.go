package directory

import (
	"bytes"
	"context"
	"errors"
	"sync"
	"testing"
	"time"
)

// TestFakeNetOrderedDelivery checks that frames arrive in the order they were sent.
func TestFakeNetOrderedDelivery(t *testing.T) {
	n := newFakeNet()
	a, b := Fingerprint{1}, Fingerprint{2}
	ea := n.Endpoint(a)
	eb := n.Endpoint(b)
	n.Connect(a, b)

	var (
		mu  sync.Mutex
		got [][]byte
	)
	eb.HandleState(7, func(peer Fingerprint, payload []byte) {
		if peer != a {
			t.Errorf("state handler got peer %v, want %v", peer, a)
		}
		mu.Lock()
		got = append(got, append([]byte(nil), payload...))
		mu.Unlock()
	})

	for i := 0; i < 100; i++ {
		if err := ea.Send(b, 7, []byte{byte(i)}); err != nil {
			t.Fatalf("Send frame %d: %v", i, err)
		}
	}
	n.Settle(t)

	mu.Lock()
	defer mu.Unlock()
	if len(got) != 100 {
		t.Fatalf("received %d frames, want 100", len(got))
	}
	for i, payload := range got {
		if !bytes.Equal(payload, []byte{byte(i)}) {
			t.Fatalf("frame %d = %v, want %v", i, payload, []byte{byte(i)})
		}
	}
}

// TestFakeNetSendWhenDown checks that Send and Request on a down link report the failure.
func TestFakeNetSendWhenDown(t *testing.T) {
	n := newFakeNet()
	a, b := Fingerprint{1}, Fingerprint{2}
	ea := n.Endpoint(a)
	n.Endpoint(b)

	if err := ea.Send(b, 1, []byte("x")); !errors.Is(err, errFakeNotEstablished) {
		t.Fatalf("Send on a down link = %v, want errFakeNotEstablished", err)
	}

	_, _, sent, err := ea.Request(context.Background(), b, 1, []byte("x"))
	if sent {
		t.Fatal("Request on a down link reported sent = true")
	}
	if err != nil {
		t.Fatalf("Request on a down link = %v, want nil error", err)
	}
}

// TestFakeClockFiresTimers checks that a timer fires only after the fake clock passes it.
func TestFakeClockFiresTimers(t *testing.T) {
	c := newFakeClock(time.Date(2026, 11, 18, 0, 0, 0, 0, time.UTC))
	tm := c.NewTimer(10 * time.Minute)

	c.Advance(9 * time.Minute)
	select {
	case <-tm.C():
		t.Fatal("timer fired after 9m, want it pending")
	default:
	}

	c.Advance(time.Minute)
	select {
	case <-tm.C():
	case <-time.After(eventTimeout):
		t.Fatal("timer did not fire after 10m")
	}
}

// TestSettleWaitsForChainedFrames checks that Settle waits for a frame sent by a handler.
func TestSettleWaitsForChainedFrames(t *testing.T) {
	n := newFakeNet()
	s, a, b := Fingerprint{1}, Fingerprint{2}, Fingerprint{3}
	es := n.Endpoint(s)
	ea := n.Endpoint(a)
	eb := n.Endpoint(b)
	n.Connect(s, a)
	n.Connect(a, b)

	var (
		mu  sync.Mutex
		got []byte
	)
	ea.HandleState(1, func(peer Fingerprint, payload []byte) {
		if err := ea.Send(b, 2, payload); err != nil {
			t.Errorf("forward to %v: %v", b, err)
		}
	})
	eb.HandleState(2, func(peer Fingerprint, payload []byte) {
		mu.Lock()
		got = append([]byte(nil), payload...)
		mu.Unlock()
	})

	if err := es.Send(a, 1, []byte("hello")); err != nil {
		t.Fatalf("Send: %v", err)
	}
	n.Settle(t)

	mu.Lock()
	defer mu.Unlock()
	if !bytes.Equal(got, []byte("hello")) {
		t.Fatalf("chained delivery = %q, want %q", got, "hello")
	}
}
