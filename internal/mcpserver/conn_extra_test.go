package mcpserver

import (
	"context"
	"errors"
	"net"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestClassifyDialError maps each dial failure onto its probe outcome.
func TestClassifyDialError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want probeResult
	}{
		{"missing", syscall.ENOENT, probeMissing},
		{"not a directory", syscall.ENOTDIR, probeMissing},
		{"refused", syscall.ECONNREFUSED, probeRefused},
		{"not a socket", syscall.ENOTSOCK, probeRefused},
		{"denied", syscall.EACCES, probeDenied},
		{"operation not permitted", syscall.EPERM, probeDenied},
		{"other", errors.New("something else"), probeOther},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := classifyDialError(tt.err); got != tt.want {
				t.Fatalf("classifyDialError(%v) = %v, want %v", tt.err, got, tt.want)
			}
		})
	}
}

// TestUnixPathFromAddress strips the unix scheme from a dial address.
func TestUnixPathFromAddress(t *testing.T) {
	tests := []struct {
		in   string
		want string
	}{
		{"unix:///tmp/broker.sock", "/tmp/broker.sock"},
		{"unix:/tmp/broker.sock", "/tmp/broker.sock"},
		{"/tmp/broker.sock", "/tmp/broker.sock"},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			if got := unixPathFromAddress(tt.in); got != tt.want {
				t.Fatalf("unixPathFromAddress(%q) = %q, want %q", tt.in, got, tt.want)
			}
		})
	}
}

// listenUnixProbe starts a Unix listener that answers nothing, because the
// kernel completes the connection a credential probe needs.
func listenUnixProbe(t *testing.T) string {
	t.Helper()
	path := filepath.Join(shortTempDir(t), "probe.sock")
	ln, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("listening on a Unix socket: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return path
}

// TestInspectSocketPeerUIDError reports a socket whose credentials cannot be read
// as an unusable one.
func TestInspectSocketPeerUIDError(t *testing.T) {
	path := listenUnixProbe(t)
	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return 0, errors.New("no credentials") }
	t.Cleanup(func() { peerUID = old })

	if got := inspectSocket(context.Background(), path); got.Outcome != probeOther {
		t.Fatalf("inspectSocket = %+v, want outcome probeOther", got)
	}
}

// TestDialUnixCheckedPeerUIDError refuses a connection whose peer credentials
// cannot be read.
func TestDialUnixCheckedPeerUIDError(t *testing.T) {
	path := listenUnixProbe(t)
	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return 0, errors.New("no credentials") }
	t.Cleanup(func() { peerUID = old })

	conn, err := dialUnixChecked(context.Background(), "unix://"+path)
	if err == nil {
		_ = conn.Close()
		t.Fatal("dialUnixChecked accepted a peer whose credentials could not be read")
	}
	if !strings.Contains(err.Error(), "cannot read the peer credentials") {
		t.Fatalf("error = %q, want it to name the failed credential read", err)
	}
}

// TestProbePathUsesInspectSocketByDefault reports a missing socket through the
// real inspection when no probe seam is set.
func TestProbePathUsesInspectSocketByDefault(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "missing.sock")
	s := &selector{}
	got := s.probePath(context.Background(), path, kindUser)
	if got.Outcome != probeMissing {
		t.Fatalf("probePath = %+v, want probeMissing through inspectSocket", got)
	}
}
