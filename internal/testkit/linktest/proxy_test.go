package linktest

import (
	"bytes"
	"io"
	"net"
	"testing"
	"time"
)

// eventTimeout is the package-wide fallback for the wait on the proxy
// forwarding a paused byte after Resume. The byte crosses two real TCP
// connections (client to proxy, proxy to the echo server) and the echo server
// has to read and write it back. It only decides how long a wait that will
// never complete takes to fail: the success path selects on the forwarded byte
// and never spends this time, so a larger value costs nothing. Thirty seconds
// is far above the worst normal latency under load; it is not a claim that an
// event should take that long.
const eventTimeout = 30 * time.Second

// startEchoServer starts a TCP server that echoes every byte back to the
// sender, and returns its address.
func startEchoServer(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("linktest: echo listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go func() {
				defer func() { _ = conn.Close() }()
				_, _ = io.Copy(conn, conn)
			}()
		}
	}()
	return ln.Addr().String()
}

func TestProxyForwardsPausesAndCaptures(t *testing.T) {
	proxy := NewProxy(t, startEchoServer(t))
	conn, err := net.Dial("tcp", proxy.Addr())
	if err != nil {
		t.Fatalf("dial proxy: %v", err)
	}
	defer func() { _ = conn.Close() }()
	proxy.WaitAccepted(t, 1)

	if _, err := conn.Write([]byte("hello")); err != nil {
		t.Fatalf("write hello: %v", err)
	}
	got := make([]byte, len("hello"))
	if _, err := io.ReadFull(conn, got); err != nil {
		t.Fatalf("read hello: %v", err)
	}
	if string(got) != "hello" {
		t.Fatalf("echoed %q, want %q", got, "hello")
	}
	waitUntil(t, "captured hello", proxy.changedCh, func() bool {
		return bytes.Contains(proxy.Captured(), []byte("hello"))
	})

	// Wait until nothing is left in flight, then pause forwarding.
	proxy.waitBuffered(t, func(n int) bool { return n == 0 })
	proxy.Pause()

	if _, err := conn.Write([]byte("x")); err != nil {
		t.Fatalf("write x: %v", err)
	}
	proxy.waitBuffered(t, func(n int) bool { return n > 0 })

	readc := make(chan []byte, 1)
	go func() {
		b := make([]byte, 1)
		n, err := conn.Read(b)
		if err != nil {
			return
		}
		readc <- b[:n]
	}()
	select {
	case b := <-readc:
		t.Fatalf("proxy forwarded %q before Resume", b)
	default:
	}

	proxy.Resume()
	select {
	case b := <-readc:
		if string(b) != "x" {
			t.Fatalf("forwarded %q after Resume, want %q", b, "x")
		}
	case <-time.After(eventTimeout):
		t.Fatal("timed out waiting for the paused byte")
	}
	waitUntil(t, "captured x", proxy.changedCh, func() bool {
		return bytes.Contains(proxy.Captured(), []byte("x"))
	})
}
