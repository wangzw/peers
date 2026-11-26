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
	probeTimeout = time.Second
	helloTimeout = 3 * time.Second
)

type probeResult int

// Outcomes of a raw Unix socket probe.
const (
	probeOK probeResult = iota
	probeMissing
	probeRefused
	probeDenied
	probeOther
	probeWrongPeer
)

type probeInfo struct {
	Outcome   probeResult
	PeerUID   uint32
	PeerKnown bool
}

// peerUID reads the effective uid of the process at the other end of conn;
// tests replace it to simulate a socket served by another account.
var peerUID = systemPeerUID

func peerMayServe(uid uint32) bool {
	return uid == uint32(os.Geteuid()) || uid == 0
}

func dialUnixSocket(ctx context.Context, path string) (net.Conn, error) {
	d := net.Dialer{Timeout: probeTimeout}
	return d.DialContext(ctx, "unix", path)
}

// The probe uses a raw dial rather than gRPC, whose lazy connection would hide
// the difference between a missing socket, a refused one and one the account may
// not use.
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

func probeSocket(ctx context.Context, path string) probeResult {
	return inspectSocket(ctx, path).Outcome
}

func classifyDialError(err error) probeResult {
	switch {
	case errors.Is(err, syscall.ENOENT), errors.Is(err, syscall.ENOTDIR):
		return probeMissing
	// Linux reports ECONNREFUSED for a path that is not a socket, macOS
	// reports ENOTSOCK; both mean nothing accepts connections there.
	case errors.Is(err, syscall.ECONNREFUSED), errors.Is(err, syscall.ENOTSOCK):
		return probeRefused
	case errors.Is(err, syscall.EACCES), errors.Is(err, syscall.EPERM):
		return probeDenied
	default:
		return probeOther
	}
}

// The idle timeout is disabled so that a connection leaves READY only when the
// Broker goes away, which lets waitNotReady treat leaving READY as a reliable
// sign of a disconnect.
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

// The peer is checked again here although selection probed the socket: it can
// change hands between the probe and the dial.
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

func unixPathFromAddress(address string) string {
	if path, ok := strings.CutPrefix(address, "unix://"); ok {
		return path
	}
	if path, ok := strings.CutPrefix(address, "unix:"); ok {
		return path
	}
	return address
}

type helloResult struct {
	Compatible    bool
	BrokerVersion uint32
}

// On a version mismatch, whether in the response or a VERSION_MISMATCH error
// detail, hello returns no error: the caller keeps the connection and waits for
// the Broker to go away rather than retry.
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
