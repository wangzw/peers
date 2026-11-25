package link

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
)

// TestRecvDeliversBufferedFrameWhenClosed checks that recv still delivers a
// frame buffered after the connection closed.
func TestRecvDeliversBufferedFrameWhenClosed(t *testing.T) {
	// Pin one processor, park recv in its select, land the close, then deliver
	// the frame through an unbuffered in; reorder it and the drain is no longer
	// the only path that can surface the frame.
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)

	c := newConn(nil, nil, peerIdentity{}, 4)
	c.in = make(chan *linkpb.Frame)

	want := &linkpb.Frame{Kind: 300, Payload: []byte("buffered")}
	got := make(chan *linkpb.Frame, 1)
	errCh := make(chan error, 1)
	go func() {
		f, err := c.recv(context.Background())
		if err != nil {
			errCh <- err
			return
		}
		got <- f
	}()

	runtime.Gosched()

	close(c.closed)

	sendTimer := time.NewTimer(2 * time.Second)
	defer sendTimer.Stop()
	select {
	case c.in <- want:
	case <-sendTimer.C:
	}

	recvTimer := time.NewTimer(2 * time.Second)
	defer recvTimer.Stop()
	select {
	case f := <-got:
		if f != want {
			t.Fatalf("recv returned %v, want the buffered frame", f)
		}
	case err := <-errCh:
		t.Fatalf("recv reported %v after close with a buffered frame", err)
	case <-recvTimer.C:
		t.Fatal("recv did not return")
	}
}
