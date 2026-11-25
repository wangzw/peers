package service

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// failingStatusLocal answers Hello but fails Status, standing in for a Broker
// that is up enough to greet the probe but cannot serve the status call.
type failingStatusLocal struct {
	localpb.UnimplementedLocalServer
}

func (failingStatusLocal) Hello(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
	return &localpb.HelloResponse{ProtocolVersion: localpb.ProtocolVersion}, nil
}

func (failingStatusLocal) Status(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
	return nil, errors.New("status unavailable")
}

// startLocalServer serves srv on a temporary local socket and returns its path.
func startLocalServer(t *testing.T, srv localpb.LocalServer) string {
	t.Helper()
	sock := shortProbeSocket(t)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen on %s: %v", sock, err)
	}
	server := grpc.NewServer()
	localpb.RegisterLocalServer(server, srv)
	go func() { _ = server.Serve(lis) }()
	t.Cleanup(server.Stop)
	return sock
}

// TestStatusProbeReportsAStatusFailure checks that a Broker that answers Hello
// but fails Status is an error, not a successful healthy reading.
func TestStatusProbeReportsAStatusFailure(t *testing.T) {
	sock := startLocalServer(t, failingStatusLocal{})

	state, degraded, err := defaultStatusProbe(sock)
	if err == nil || !strings.Contains(err.Error(), "status unavailable") {
		t.Fatalf("defaultStatusProbe error = %v, want the Status failure", err)
	}
	if degraded {
		t.Error("degraded = true although Status failed")
	}
	if state != (DegradedState{}) {
		t.Errorf("state = %+v, want the zero value when Status fails", state)
	}
}

// TestDialStatusSocketReportsUnreadablePeerCredentials checks that a socket
// whose peer account cannot be read is closed and reported, not handed to gRPC.
func TestDialStatusSocketReportsUnreadablePeerCredentials(t *testing.T) {
	sock := shortProbeSocket(t)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen on %s: %v", sock, err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	credErr := errors.New("no peer credentials")
	statusPeerUID = func(net.Conn) (uint32, error) { return 0, credErr }
	t.Cleanup(func() { statusPeerUID = systemPeerUID })

	conn, err := dialStatusSocket(context.Background(), "unix://"+sock)
	if conn != nil {
		_ = conn.Close()
		t.Error("dialStatusSocket returned a connection whose peer account could not be read")
	}
	if !errors.Is(err, credErr) {
		t.Fatalf("dialStatusSocket error = %v, want %v", err, credErr)
	}
	if !strings.Contains(err.Error(), "cannot read the peer credentials of "+sock) {
		t.Errorf("dialStatusSocket error = %q, want it to name the socket", err)
	}
}

// TestSocketPathFromAddress covers every shape gRPC may hand the dialer: the
// two Unix prefixes are stripped, and anything else is already a path.
func TestSocketPathFromAddress(t *testing.T) {
	cases := []struct{ in, want string }{
		{"unix:///tmp/peers/broker.sock", "/tmp/peers/broker.sock"},
		{"unix:/tmp/peers/broker.sock", "/tmp/peers/broker.sock"},
		{"/tmp/peers/broker.sock", "/tmp/peers/broker.sock"},
		{"dns:///broker:17443", "dns:///broker:17443"},
	}
	for _, tc := range cases {
		if got := socketPathFromAddress(tc.in); got != tc.want {
			t.Errorf("socketPathFromAddress(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}

// TestStatusPeerWantNamesTheExpectedAccount pins the account named in the
// rejection error: root for a system socket, the effective account otherwise.
func TestStatusPeerWantNamesTheExpectedAccount(t *testing.T) {
	if got := statusPeerWant("/run/peers/broker.sock"); got != "root" {
		t.Errorf("statusPeerWant(system socket) = %q, want root", got)
	}
	userPath := filepath.Join("/tmp", "peers-"+strconv.Itoa(os.Geteuid()), "broker.sock")
	if got, want := statusPeerWant(userPath), fmt.Sprintf("this account (%d)", os.Geteuid()); got != want {
		t.Errorf("statusPeerWant(user socket) = %q, want %q", got, want)
	}
}
