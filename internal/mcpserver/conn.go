package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wangzw/peers/internal/broker/localpb"
)

const (
	// probeTimeout bounds one raw socket probe.
	probeTimeout = time.Second
	// helloTimeout bounds the Hello call.
	helloTimeout = 3 * time.Second
)

// probeResult classifies the outcome of a raw Unix socket probe.
type probeResult int

const (
	// probeOK means a process of an account that may serve a Broker accepted
	// the connection.
	probeOK probeResult = iota
	// probeMissing means the socket path does not exist, or a path component
	// is missing or not a directory.
	probeMissing
	// probeRefused means the path exists but no process accepts connections.
	probeRefused
	// probeDenied means the account may not connect to the socket.
	probeDenied
	// probeOther means the probe failed for any other reason.
	probeOther
	// probeWrongPeer means a process answered, but its account may not serve a
	// Broker at this path. The probe result carries that account in PeerUID.
	probeWrongPeer
)

// probeInfo is the result of a raw Unix socket probe together with the account
// of the process that answered. PeerKnown is false when no peer credentials were
// read, which happens only for a probe that did not connect.
type probeInfo struct {
	Outcome   probeResult
	PeerUID   uint32
	PeerKnown bool
}

// peerUID reads the effective uid of the process on the other end of conn. It is
// a package variable so that tests can pretend a socket is served by another
// account.
var peerUID = systemPeerUID

// peerMayServe reports whether uid may serve a Broker at all: the account of
// this process, or root. The per-step rules (the system Broker must be
// root, the user one must be this account) are applied by the selector.
func peerMayServe(uid uint32) bool {
	return uid == uint32(os.Geteuid()) || uid == 0
}

// dialUnixSocket opens a raw Unix socket connection bounded by probeTimeout.
func dialUnixSocket(ctx context.Context, path string) (net.Conn, error) {
	d := net.Dialer{Timeout: probeTimeout}
	return d.DialContext(ctx, "unix", path)
}

// inspectSocket connects to the Unix socket at path, reads the account of the
// process on the other end before sending anything, and closes the connection at
// once. A connection is usable only when its peer may serve a Broker at all.
// The probe uses a raw dial rather than gRPC because gRPC's lazy connection
// hides the difference between a missing socket, a refused one and one the
// account may not use.
func inspectSocket(ctx context.Context, path string) probeInfo {
	conn, err := dialUnixSocket(ctx, path)
	if err != nil {
		return probeInfo{Outcome: classifyDialError(err)}
	}
	defer func() { _ = conn.Close() }()

	uid, err := peerUID(conn)
	if err != nil {
		return probeInfo{Outcome: probeOther}
	}
	info := probeInfo{Outcome: probeOK, PeerUID: uid, PeerKnown: true}
	if !peerMayServe(uid) {
		info.Outcome = probeWrongPeer
	}
	return info
}

// probeSocket reports only the outcome of inspectSocket. The background
// connection loop uses it to decide whether a known socket is still there.
func probeSocket(ctx context.Context, path string) probeResult {
	return inspectSocket(ctx, path).Outcome
}

// classifyDialError maps a failed raw dial onto a probe result.
func classifyDialError(err error) probeResult {
	switch {
	case errors.Is(err, syscall.ENOENT), errors.Is(err, syscall.ENOTDIR):
		return probeMissing
	// Linux reports ECONNREFUSED for a path that is not a socket, macOS
	// reports ENOTSOCK. Both mean nothing accepts connections there.
	case errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.ENOTSOCK):
		return probeRefused
	case errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return probeDenied
	default:
		return probeOther
	}
}

// dialLocal opens a gRPC client connection to the Broker listening on the Unix
// socket at path. The idle timeout is disabled so that a connection leaves
// READY only when the Broker goes away, which lets waitNotReady treat leaving
// READY as a reliable sign of a disconnect. Every underlying connection is
// checked with peerUID before gRPC sends a byte, so a socket taken over by
// another account is never used even after selection probed it.
func dialLocal(path string) (*grpc.ClientConn, localpb.LocalClient, error) {
	abs, err := filepath.Abs(path)
	if err != nil {
		return nil, nil, err
	}
	cc, err := grpc.NewClient("unix://"+abs,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithIdleTimeout(0),
		grpc.WithContextDialer(dialUnixChecked),
	)
	if err != nil {
		return nil, nil, err
	}
	return cc, localpb.NewLocalClient(cc), nil
}

// dialUnixChecked dials a Unix socket and rejects a peer that may not serve a
// Broker before the connection is handed to gRPC.
func dialUnixChecked(ctx context.Context, address string) (net.Conn, error) {
	path := unixPathFromAddress(address)
	conn, err := dialUnixSocket(ctx, path)
	if err != nil {
		return nil, err
	}
	uid, err := peerUID(conn)
	if err != nil {
		_ = conn.Close()
		return nil, fmt.Errorf("cannot read the peer credentials of %s: %w", path, err)
	}
	if !peerMayServe(uid) {
		_ = conn.Close()
		return nil, fmt.Errorf("the Broker socket %s is served by uid %d, not this account (%d) or root",
			path, uid, os.Geteuid())
	}
	return conn, nil
}

// unixPathFromAddress turns the target gRPC hands to a context dialer back into
// a socket path. Depending on the resolver it is either the path itself or the
// full "unix://" target.
func unixPathFromAddress(address string) string {
	if path, ok := strings.CutPrefix(address, "unix://"); ok {
		return path
	}
	if path, ok := strings.CutPrefix(address, "unix:"); ok {
		return path
	}
	return address
}

// helloResult is the outcome of the version handshake.
type helloResult struct {
	// Compatible reports whether the Broker speaks the same local protocol
	// version as the caller.
	Compatible bool
	// BrokerVersion is the version the Broker reported, or 0 when the Broker
	// reported the mismatch as an error.
	BrokerVersion uint32
}

// hello performs the Hello handshake with the Broker. A Broker that answers a
// different version, either in the response or through a VERSION_MISMATCH
// error detail, is incompatible and returns no error: the caller must keep the
// connection and wait for the Broker to go away rather than retry. Any other
// error is a connection failure.
func hello(ctx context.Context, c localpb.LocalClient, version uint32) (helloResult, error) {
	ctx, cancel := context.WithTimeout(ctx, helloTimeout)
	defer cancel()

	resp, err := c.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: version})
	if err != nil {
		if d := errorDetail(err); d != nil && d.GetCode() == localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH {
			return helloResult{}, nil
		}
		return helloResult{}, err
	}
	v := resp.GetProtocolVersion()
	return helloResult{Compatible: v == version, BrokerVersion: v}, nil
}

// waitNotReady returns when cc leaves connectivity.Ready or ctx is done. It
// calls Connect so that the connection state is meaningful even when no RPC
// has been made yet.
func waitNotReady(ctx context.Context, cc *grpc.ClientConn) {
	cc.Connect()
	s := cc.GetState()
	for s == connectivity.Ready {
		if !cc.WaitForStateChange(ctx, s) {
			return
		}
		s = cc.GetState()
	}
}
