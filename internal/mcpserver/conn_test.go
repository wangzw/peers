package mcpserver

import (
	"context"
	"errors"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

// dialFakeConn starts a fake Local service and returns a client connection to it.
func dialFakeConn(t *testing.T, h fakelocal.Handlers) (*fakelocal.Server, *connPair) {
	t.Helper()
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, h)
	cc, c, err := dialLocal(path)
	if err != nil {
		t.Fatalf("dialLocal: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return srv, &connPair{cc: cc, c: c}
}

// connPair pairs the client connection with the typed Local client.
type connPair struct {
	cc *grpc.ClientConn
	c  localpb.LocalClient
}

func TestProbeSocket(t *testing.T) {
	t.Run("answers when a server listens", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		fakelocal.Start(t, path, fakelocal.Handlers{})
		if got := probeSocket(context.Background(), path); got != probeOK {
			t.Fatalf("probeSocket = %v, want probeOK", got)
		}
	})

	t.Run("missing path", func(t *testing.T) {
		if got := probeSocket(context.Background(), socktest.Path(t, "absent.sock")); got != probeMissing {
			t.Fatalf("probeSocket = %v, want probeMissing", got)
		}
	})

	t.Run("regular file at the path", func(t *testing.T) {
		// Linux reports ECONNREFUSED for a regular file, macOS ENOTSOCK; both
		// mean nothing listens there, so the probe must say refused.
		path := fakelocal.SocketPath(t)
		if err := os.WriteFile(path, []byte("not a socket"), 0o644); err != nil {
			t.Fatalf("write placeholder: %v", err)
		}
		if got := probeSocket(context.Background(), path); got != probeRefused {
			t.Fatalf("probeSocket = %v, want probeRefused", got)
		}
	})

	t.Run("stale socket file", func(t *testing.T) {
		path := fakelocal.SocketPath(t)
		lis, err := net.Listen("unix", path)
		if err != nil {
			t.Fatalf("listen: %v", err)
		}
		// net.UnixListener removes the socket file on Close by default; keep it
		// so that the path outlives the listener, as it does after a crash.
		lis.(*net.UnixListener).SetUnlinkOnClose(false)
		if err := lis.Close(); err != nil {
			t.Fatalf("close listener: %v", err)
		}
		t.Cleanup(func() { _ = os.Remove(path) })
		// The socket file stays behind after the listener closes; connecting to
		// it yields ECONNREFUSED.
		if got := probeSocket(context.Background(), path); got != probeRefused {
			t.Fatalf("probeSocket = %v, want probeRefused", got)
		}
	})

	t.Run("permission denied", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can traverse a directory regardless of its mode")
		}
		dir := socktest.Dir(t)
		if err := os.Chmod(dir, 0o000); err != nil {
			t.Fatalf("chmod: %v", err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })

		got := probeSocket(context.Background(), filepath.Join(dir, "broker.sock"))
		if got != probeDenied {
			t.Fatalf("probeSocket = %v, want probeDenied", got)
		}
	})
}

// The raw probe must report the account that answered so that selection can
// reject a socket another account laid down.
func TestInspectSocketReportsForeignPeer(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{})

	foreign := uint32(os.Geteuid()) + 1
	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return foreign, nil }
	t.Cleanup(func() { peerUID = old })

	got := inspectSocket(context.Background(), path)
	if got.Outcome != probeWrongPeer {
		t.Fatalf("inspectSocket outcome = %v, want probeWrongPeer", got.Outcome)
	}
	if !got.PeerKnown || got.PeerUID != foreign {
		t.Fatalf("inspectSocket = %+v, want the peer uid %d", got, foreign)
	}
}

// Even if selection probed a socket owned by this account, the connection that
// actually carries the protocol must check the peer again, so a socket taken
// over between the probe and the dial is never used.
func TestDialLocalRefusesForeignPeer(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{})

	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return uint32(os.Geteuid()) + 1, nil }
	t.Cleanup(func() { peerUID = old })

	cc, c, err := dialLocal(path)
	if err != nil {
		t.Fatalf("dialLocal: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	if _, err := hello(ctx, c, localpb.ProtocolVersion); err == nil {
		t.Fatal("hello succeeded on a connection served by another account")
	} else if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("hello timed out instead of refusing the foreign peer: %v", err)
	}
}

func TestDialLocal(t *testing.T) {
	srv, conn := dialFakeConn(t, fakelocal.Handlers{})
	if got := srv.Path(); got == "" {
		t.Fatal("fake server has no path")
	}
	res, err := hello(context.Background(), conn.c, localpb.ProtocolVersion)
	if err != nil {
		t.Fatalf("hello over the dialed connection: %v", err)
	}
	if !res.Compatible {
		t.Fatalf("hello over the dialed connection is incompatible: %+v", res)
	}
}

func TestHello(t *testing.T) {
	t.Run("compatible version", func(t *testing.T) {
		_, conn := dialFakeConn(t, fakelocal.Handlers{})
		res, err := hello(context.Background(), conn.c, localpb.ProtocolVersion)
		if err != nil {
			t.Fatalf("hello: %v", err)
		}
		if !res.Compatible || res.BrokerVersion != localpb.ProtocolVersion {
			t.Fatalf("hello = %+v, want compatible version %d", res, localpb.ProtocolVersion)
		}
	})

	t.Run("different version in the response", func(t *testing.T) {
		// REQ-broker-config-016
		_, conn := dialFakeConn(t, fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return &localpb.HelloResponse{ProtocolVersion: 2}, nil
			},
		})
		res, err := hello(context.Background(), conn.c, localpb.ProtocolVersion)
		if err != nil {
			t.Fatalf("hello: %v", err)
		}
		if res.Compatible {
			t.Fatalf("hello = %+v, want incompatible", res)
		}
		if res.BrokerVersion != 2 {
			t.Fatalf("BrokerVersion = %d, want 2", res.BrokerVersion)
		}
	})

	t.Run("sends the injected version", func(t *testing.T) {
		srv, conn := dialFakeConn(t, fakelocal.Handlers{
			Hello: func(_ context.Context, req *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return &localpb.HelloResponse{ProtocolVersion: req.GetProtocolVersion()}, nil
			},
		})
		res, err := hello(context.Background(), conn.c, 7)
		if err != nil {
			t.Fatalf("hello: %v", err)
		}
		if !res.Compatible || res.BrokerVersion != 7 {
			t.Fatalf("hello = %+v, want compatible version 7", res)
		}
		calls := srv.Calls()
		if len(calls) != 1 || calls[0].Method != "Hello" {
			t.Fatalf("calls = %+v, want one Hello", calls)
		}
		req, ok := calls[0].Request.(*localpb.HelloRequest)
		if !ok {
			t.Fatalf("Hello request has type %T", calls[0].Request)
		}
		if req.GetProtocolVersion() != 7 {
			t.Fatalf("HelloRequest.protocol_version = %d, want 7", req.GetProtocolVersion())
		}
	})

	t.Run("version mismatch as an error detail", func(t *testing.T) {
		_, conn := dialFakeConn(t, fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
					Message: "protocol version mismatch",
				})
			},
		})
		res, err := hello(context.Background(), conn.c, localpb.ProtocolVersion)
		if err != nil {
			t.Fatalf("hello returned %v, want nil for a reported mismatch", err)
		}
		if res.Compatible {
			t.Fatalf("hello = %+v, want incompatible", res)
		}
		if res.BrokerVersion != 0 {
			t.Fatalf("BrokerVersion = %d, want 0", res.BrokerVersion)
		}
	})

	t.Run("other error is a connection failure", func(t *testing.T) {
		_, conn := dialFakeConn(t, fakelocal.Handlers{
			Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				return nil, errors.New("unavailable")
			},
		})
		if _, err := hello(context.Background(), conn.c, localpb.ProtocolVersion); err == nil {
			t.Fatal("hello returned nil, want an error")
		}
	})

	t.Run("respects the caller context", func(t *testing.T) {
		// The handler reports when it has started and then blocks until its
		// context ends. Cancelling the caller's context after that point proves
		// hello used it: a hello that kept only its own deadline would return
		// DeadlineExceeded instead of Canceled, whatever the scheduling delay.
		started := make(chan struct{})
		_, conn := dialFakeConn(t, fakelocal.Handlers{
			Hello: func(ctx context.Context, _ *localpb.HelloRequest) (*localpb.HelloResponse, error) {
				close(started)
				<-ctx.Done()
				return nil, ctx.Err()
			},
		})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()
		done := make(chan error, 1)
		go func() {
			_, err := hello(ctx, conn.c, localpb.ProtocolVersion)
			done <- err
		}()
		select {
		case <-started:
		case <-time.After(eventTimeout):
			t.Fatalf("hello never reached the fake Broker within %s", eventTimeout)
		}
		cancel()
		select {
		case err := <-done:
			if err == nil {
				t.Fatal("hello returned nil, want an error")
			}
			if code := status.Code(err); code != codes.Canceled {
				t.Fatalf("hello returned %v (code %s), want Canceled from the caller context", err, code)
			}
		case <-time.After(eventTimeout):
			t.Fatalf("hello did not return within %s of the caller context being cancelled", eventTimeout)
		}
	})
}

func TestWaitNotReady(t *testing.T) {
	t.Run("waits while the connection is ready", func(t *testing.T) {
		_, conn := dialFakeConn(t, fakelocal.Handlers{})
		if _, err := hello(context.Background(), conn.c, localpb.ProtocolVersion); err != nil {
			t.Fatalf("hello: %v", err)
		}

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
		defer cancel()
		start := time.Now()
		waitNotReady(ctx, conn.cc)
		elapsed := time.Since(start)

		if !errors.Is(ctx.Err(), context.DeadlineExceeded) {
			t.Fatalf("ctx.Err() = %v, want DeadlineExceeded", ctx.Err())
		}
		if elapsed < 200*time.Millisecond {
			t.Fatalf("waitNotReady returned after %v, before the context deadline", elapsed)
		}
	})

	t.Run("returns when the Broker goes away", func(t *testing.T) {
		srv, conn := dialFakeConn(t, fakelocal.Handlers{})
		if _, err := hello(context.Background(), conn.c, localpb.ProtocolVersion); err != nil {
			t.Fatalf("hello: %v", err)
		}

		// The context outlives the fallback below by far, so a timely return
		// can only come from the Broker stopping, not from the context ending.
		ctx, cancel := context.WithTimeout(context.Background(), time.Hour)
		defer cancel()

		srv.Stop()
		done := make(chan struct{})
		go func() {
			waitNotReady(ctx, conn.cc)
			close(done)
		}()
		select {
		case <-done:
			if ctx.Err() != nil {
				t.Fatalf("waitNotReady returned on ctx ending (%v), want the Broker stopping", ctx.Err())
			}
		case <-time.After(eventTimeout):
			t.Fatalf("waitNotReady did not return within %s of the Broker stopping", eventTimeout)
		}
	})
}
