//go:build peers_testenv

package mcpserver

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// PEERS_TEST_SOCKET names the socket to use, ahead of account selection.
func TestSelectTestSocketTakesPrecedence(t *testing.T) {
	envPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, envPath, fakelocal.Handlers{})
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, fakelocal.Handlers{})
	systemPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, systemPath, fakelocal.Handlers{})
	t.Setenv("PEERS_TEST_SOCKET", envPath)

	s := &selector{
		userSocket:   userPath,
		systemSocket: systemPath,
		probe:        inspectSocket,
	}

	got, err := s.Find(context.Background())
	if err != nil {
		t.Fatalf("Find: %v", err)
	}
	if got.Kind != kindTest || got.Path != envPath {
		t.Fatalf("Find = %+v, want {%s kindTest}", got, envPath)
	}
}

// A dead PEERS_TEST_SOCKET must not silently fall through to another
// Broker.
func TestSelectTestSocketMissingDoesNotFallThrough(t *testing.T) {
	missing := filepath.Join(t.TempDir(), "absent.sock")
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, fakelocal.Handlers{})
	t.Setenv("PEERS_TEST_SOCKET", missing)

	s := &selector{
		userSocket:   userPath,
		systemSocket: filepath.Join(t.TempDir(), "system.sock"),
		probe:        inspectSocket,
	}

	_, err := s.Find(context.Background())
	if err == nil {
		t.Fatal("Find returned nil error for a dead PEERS_TEST_SOCKET")
	}
	if !strings.Contains(err.Error(), missing) || !strings.Contains(err.Error(), "PEERS_TEST_SOCKET") {
		t.Fatalf("error = %q, want it to name %s and PEERS_TEST_SOCKET", err, missing)
	}
}

// PEERS_TEST_SOCKET names a socket that must be served by this account or
// by root. Another account makes the socket unusable, and the error names the
// path and the account that answered.
func TestSelectRejectsForeignTestSocket(t *testing.T) {
	envPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, envPath, fakelocal.Handlers{})
	t.Setenv("PEERS_TEST_SOCKET", envPath)

	foreign := uint32(os.Geteuid()) + 1
	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return foreign, nil }
	t.Cleanup(func() { peerUID = old })

	s := &selector{
		userSocket:   filepath.Join(t.TempDir(), "user.sock"),
		systemSocket: filepath.Join(t.TempDir(), "system.sock"),
		probe:        inspectSocket,
	}

	_, err := s.Find(context.Background())
	if err == nil {
		t.Fatalf("Find accepted a socket served by uid %d", foreign)
	}
	wantUID := strconv.FormatUint(uint64(foreign), 10)
	for _, want := range []string{envPath, wantUID, "PEERS_TEST_SOCKET"} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to contain %q", err, want)
		}
	}
}

// FindBrokerWithDial returns a dialer that checks the peer of every connection
// it opens, so a socket taken over by another account after selection is
// refused before Hello is sent.
func TestFindBrokerWithDialRefusesForeignPeer(t *testing.T) {
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{})
	t.Setenv("PEERS_TEST_SOCKET", path)

	gotPath, dial, err := FindBrokerWithDial(context.Background())
	if err != nil {
		t.Fatalf("FindBrokerWithDial: %v", err)
	}
	if gotPath != path {
		t.Fatalf("FindBrokerWithDial path = %q, want %q", gotPath, path)
	}

	// The socket now answers as another account, as if another process had
	// taken it over between selection and the connection.
	foreign := uint32(os.Geteuid()) + 1
	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return foreign, nil }
	t.Cleanup(func() { peerUID = old })

	cc, client, err := dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	defer func() { _ = cc.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	_, err = client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion})
	if err == nil {
		t.Fatal("the dialer connected to a Broker socket served by another account")
	}
	if !strings.Contains(err.Error(), "served by uid") {
		t.Fatalf("Hello error = %q, want it to name the peer uid", err)
	}
	for _, c := range srv.Calls() {
		if c.Method == "Hello" {
			t.Fatal("the fake Broker received Hello over a connection served by another account")
		}
	}
}
