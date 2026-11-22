package link

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/link/linkpb"
)

// TestRecvDeliversBufferedFrameWhenClosed checks that a recv already waiting on
// the receive select still delivers a frame that is buffered after the
// connection closed. Without the drain in the closed case, recv reports the
// close and the waiting frame is lost to that caller.
func TestRecvDeliversBufferedFrameWhenClosed(t *testing.T) {
	// Pin the scheduler to one processor so the ordering below is fixed: recv
	// parks in its select, the close wakes it on the closed case, and only then
	// does the test hand it the buffered frame.
	old := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(old)

	c := newConn(nil, nil, peerIdentity{}, 4)
	// An unbuffered in queue makes the frame hand-off rendezvous with the
	// closed case's drain, so the test controls exactly when the frame arrives.
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

	// Let recv reach its blocking select and park there before the close.
	runtime.Gosched()

	close(c.closed)

	// Buffer the frame only after the close, so recv observes the closed case
	// with an empty in queue and only the drain can surface the frame. On the
	// unfixed code nothing receives this send and it times out.
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
