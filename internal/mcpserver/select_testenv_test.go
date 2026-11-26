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
	"github.com/wangzw/peers/internal/testkit/socktest"
)

// TestSelectTestSocketTakesPrecedence selects the socket PEERS_TEST_SOCKET names
// ahead of account selection.
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

// TestSelectTestSocketMissingDoesNotFallThrough reports a dead PEERS_TEST_SOCKET
// instead of selecting another Broker.
func TestSelectTestSocketMissingDoesNotFallThrough(t *testing.T) {
	missing := socktest.Path(t, "absent.sock")
	userPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, userPath, fakelocal.Handlers{})
	t.Setenv("PEERS_TEST_SOCKET", missing)

	s := &selector{
		userSocket:   userPath,
		systemSocket: socktest.Path(t, "system.sock"),
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

// TestSelectRejectsForeignTestSocket checks that a PEERS_TEST_SOCKET served by
// another account is rejected.
func TestSelectRejectsForeignTestSocket(t *testing.T) {
	envPath := fakelocal.SocketPath(t)
	fakelocal.Start(t, envPath, fakelocal.Handlers{})
	t.Setenv("PEERS_TEST_SOCKET", envPath)

	foreign := uint32(os.Geteuid()) + 1
	old := peerUID
	peerUID = func(net.Conn) (uint32, error) { return foreign, nil }
	t.Cleanup(func() { peerUID = old })

	s := &selector{
		userSocket:   socktest.Path(t, "user.sock"),
		systemSocket: socktest.Path(t, "system.sock"),
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

// TestFindBrokerWithDialRefusesForeignPeer refuses, before Hello, a socket taken
// over by another account after selection.
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

// TestFindNamedSocket skips the named-socket step when PEERS_TEST_SOCKET is
// empty and selects a serving socket when it is set.
func TestFindNamedSocket(t *testing.T) {
	s := &selector{}

	t.Setenv("PEERS_TEST_SOCKET", "")
	got, found, err := s.findNamedSocket(context.Background())
	if err != nil || found || got.Path != "" {
		t.Fatalf("findNamedSocket = (%+v, %v, %v), want no target and no error", got, found, err)
	}

	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{})
	t.Setenv("PEERS_TEST_SOCKET", path)
	got, found, err = s.findNamedSocket(context.Background())
	if err != nil || !found || got.Path != path || got.Kind != kindTest {
		t.Fatalf("findNamedSocket = (%+v, %v, %v), want {%s kindTest}", got, found, err, path)
	}
}

// TestSelectorEnvironmentDefaultsToProcess reads the process environment when the
// selector has no environment seam.
func TestSelectorEnvironmentDefaultsToProcess(t *testing.T) {
	t.Setenv("PEERS_TEST_SOCKET", "/tmp/named.sock")
	s := &selector{}
	if got := s.environment("PEERS_TEST_SOCKET"); got != "/tmp/named.sock" {
		t.Fatalf("environment = %q, want the process value", got)
	}
}

// TestProbeDescriptionNamesEveryFailure describes each probe outcome in words.
func TestProbeDescriptionNamesEveryFailure(t *testing.T) {
	tests := []struct {
		in   probeResult
		want string
	}{
		{probeMissing, "the path does not exist"},
		{probeRefused, "no process accepts connections"},
		{probeDenied, "this account may not connect to it"},
		{probeOther, "the connection failed"},
		{probeOK, "unknown failure"},
	}
	for _, tt := range tests {
		if got := probeDescription(tt.in); got != tt.want {
			t.Fatalf("probeDescription(%v) = %q, want %q", tt.in, got, tt.want)
		}
	}
}

// TestFindAtLevelReportsDeadTestSocket reports a dead test socket as an error at
// the user level.
func TestFindAtLevelReportsDeadTestSocket(t *testing.T) {
	missing := filepath.Join(shortTempDir(t), "absent.sock")
	t.Setenv("PEERS_TEST_SOCKET", missing)
	s := &selector{}
	_, _, err := s.findAtLevel(context.Background(), false)
	if err == nil || !strings.Contains(err.Error(), "PEERS_TEST_SOCKET") {
		t.Fatalf("findAtLevel error = %v, want it to name PEERS_TEST_SOCKET", err)
	}
}

// TestFindBrokerFunctionsReportDeadTestSocket reports a dead PEERS_TEST_SOCKET
// with the path from every finder.
func TestFindBrokerFunctionsReportDeadTestSocket(t *testing.T) {
	missing := filepath.Join(shortTempDir(t), "absent.sock")
	t.Setenv("PEERS_TEST_SOCKET", missing)
	ctx := context.Background()

	errs := map[string]error{}
	if _, err := FindBroker(ctx); err != nil {
		errs["FindBroker"] = err
	} else {
		t.Fatal("FindBroker returned nil error for a dead test socket")
	}
	if _, err := FindBrokerForLevel(ctx, false, true); err != nil {
		errs["FindBrokerForLevel"] = err
	} else {
		t.Fatal("FindBrokerForLevel returned nil error for a dead test socket")
	}
	if _, _, err := FindBrokerWithDial(ctx); err != nil {
		errs["FindBrokerWithDial"] = err
	} else {
		t.Fatal("FindBrokerWithDial returned nil error for a dead test socket")
	}
	if _, _, err := FindBrokerWithDialForLevel(ctx, false, true); err != nil {
		errs["FindBrokerWithDialForLevel"] = err
	} else {
		t.Fatal("FindBrokerWithDialForLevel returned nil error for a dead test socket")
	}
	for name, err := range errs {
		if !strings.Contains(err.Error(), missing) || !strings.Contains(err.Error(), "PEERS_TEST_SOCKET") {
			t.Fatalf("%s error = %q, want it to name %s and PEERS_TEST_SOCKET", name, err, missing)
		}
	}
}

// TestFindBrokerWithDialForLevelReturnsDialer selects a live test socket and
// returns a dialer that opens a connection to it.
func TestFindBrokerWithDialForLevelReturnsDialer(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{})
	t.Setenv("PEERS_TEST_SOCKET", path)

	got, dial, err := FindBrokerWithDialForLevel(context.Background(), false, true)
	if err != nil {
		t.Fatalf("FindBrokerWithDialForLevel: %v", err)
	}
	if got != path {
		t.Fatalf("path = %q, want %q", got, path)
	}
	cc, client, err := dial()
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	if client == nil {
		t.Fatal("dial returned no client")
	}
	_ = cc.Close()
}
