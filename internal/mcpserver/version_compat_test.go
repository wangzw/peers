package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
	"google.golang.org/grpc/codes"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// versionRejectText is what a Broker speaking local protocol version 2 answers
// a client speaking version 1 with.
const versionRejectText = "local protocol version 1 is not supported by this Broker (version 2); restart the Broker"

// versionAfter records every requested wait and resolves it at once. A loop
// that polls the Broker after a version mismatch therefore runs as fast as the
// fake Broker answers, which turns the polling into extra Hello calls the test
// can count. The correct loop sends nothing and waits for the connection to
// leave READY.
type versionAfter struct {
	mu    sync.Mutex
	waits []time.Duration
}

func (a *versionAfter) after(d time.Duration) <-chan time.Time {
	a.mu.Lock()
	a.waits = append(a.waits, d)
	a.mu.Unlock()
	ch := make(chan time.Time, 1)
	ch <- time.Now()
	return ch
}

func (a *versionAfter) recorded() []time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]time.Duration(nil), a.waits...)
}

// versionOptions builds options whose Broker-facing behaviour is fully under
// test control: no environment overrides, an empty working directory, the given
// sockets, waits that resolve at once and no git origin.
func versionOptions(t *testing.T, userSocket, systemSocket string) (Options, *versionAfter) {
	t.Helper()
	after := &versionAfter{}
	dir := t.TempDir()
	return Options{
		Logger:       loopTestLogger(),
		getenv:       func(string) string { return "" },
		getwd:        func() (string, error) { return dir, nil },
		userSocket:   userSocket,
		systemSocket: systemSocket,
		readOrigin:   func(context.Context, string) (string, error) { return "", errors.New("no origin") },
		after:        after.after,
	}, after
}

// versionMismatchRejecter answers Hello the way a version 2 Broker rejects a
// version 1 client, counting every call.
func versionMismatchRejecter(hellos *atomic.Int64) fakelocal.Handlers {
	return fakelocal.Handlers{
		Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
			hellos.Add(1)
			return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
				Message: versionRejectText,
			})
		},
	}
}

// versionWaitMismatch calls list_agents until the background loop reports the
// version mismatch and returns that text. Until the first selection finishes,
// list_agents answers that the Broker is still being set up.
func versionWaitMismatch(t *testing.T, cs *mcp.ClientSession) string {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		res := callTool(t, cs, "list_agents", nil)
		text := textContent(t, res)
		if res.IsError && strings.Contains(text, "(code: version_mismatch)") {
			return text
		}
		time.Sleep(2 * time.Millisecond)
	}
	t.Fatal("list_agents never reported a version mismatch")
	return ""
}

// versionAssertNoRegister fails if srv receives a Register stream within the
// window. Only a compatible Hello leads to Register, so a stream here means the
// MCP server registered despite the mismatch.
func versionAssertNoRegister(t *testing.T, srv *fakelocal.Server, what string) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()
	if conn, err := srv.NextRegister(ctx); err == nil {
		_ = conn
		t.Fatalf("%s received a Register stream after a version mismatch", what)
	}
}

// REQ-broker-config-016
func TestMismatchDoesNotRegisterOrSwitch(t *testing.T) {
	userSock := fakelocal.SocketPath(t)
	systemSock := fakelocal.SocketPath(t)

	var userHellos atomic.Int64
	userSrv := fakelocal.Start(t, userSock, versionMismatchRejecter(&userHellos))

	var systemHellos atomic.Int64
	systemSrv := fakelocal.Start(t, systemSock, fakelocal.Handlers{
		Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
			systemHellos.Add(1)
			return &localpb.HelloResponse{ProtocolVersion: localpb.ProtocolVersion}, nil
		},
	})

	opts, _ := versionOptions(t, userSock, systemSock)
	h := startRunClient(t, opts)
	versionWaitMismatch(t, h.cs)

	for i := 0; i < 3; i++ {
		res := callTool(t, h.cs, "list_agents", nil)
		if !res.IsError {
			t.Fatalf("list_agents %d succeeded against a mismatched Broker", i+1)
		}
		text := textContent(t, res)
		if !strings.Contains(text, "restart the Broker") {
			t.Errorf("list_agents %d error = %q, want a \"restart the Broker\" hint", i+1, text)
		}
		if !strings.HasSuffix(text, " (code: version_mismatch)") {
			t.Errorf("list_agents %d error = %q, want the version_mismatch code suffix", i+1, text)
		}
	}

	versionAssertNoRegister(t, userSrv, "the user Broker")
	versionAssertNoRegister(t, systemSrv, "the system Broker")
	if got := systemHellos.Load(); got != 0 {
		t.Errorf("the system Broker received %d Hello calls; a mismatch must not switch to it", got)
	}
	if h.finished() {
		t.Fatalf("Run returned after a version mismatch: %v", h.errValue())
	}
}

// REQ-broker-config-016
func TestMismatchBrokerOlder(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	// The default Hello handler answers localpb.ProtocolVersion (1).
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{})

	opts, _ := versionOptions(t, sock, fakelocal.SocketPath(t))
	opts.LocalProtocolVersion = 2
	h := startRunClient(t, opts)

	text := versionWaitMismatch(t, h.cs)
	if !strings.Contains(text, "speaks local protocol version 1") {
		t.Errorf("list_agents error = %q, want the Broker's reported version 1", text)
	}
	if !strings.Contains(text, "restart the Broker") {
		t.Errorf("list_agents error = %q, want a \"restart the Broker\" hint", text)
	}
	if !strings.HasSuffix(text, " (code: version_mismatch)") {
		t.Errorf("list_agents error = %q, want the version_mismatch code suffix", text)
	}
	versionAssertNoRegister(t, srv, "the user Broker")
}

// REQ-broker-config-016
func TestMismatchNoHelloPolling(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	var hellos atomic.Int64
	fakelocal.Start(t, sock, versionMismatchRejecter(&hellos))

	opts, after := versionOptions(t, sock, fakelocal.SocketPath(t))
	h := startRunClient(t, opts)
	versionWaitMismatch(t, h.cs)

	for i := 0; i < 20; i++ {
		res := callTool(t, h.cs, "list_agents", nil)
		if !res.IsError {
			t.Fatalf("list_agents %d succeeded against a mismatched Broker", i+1)
		}
		if text := textContent(t, res); !strings.HasSuffix(text, " (code: version_mismatch)") {
			t.Fatalf("list_agents %d error = %q, want the version_mismatch code suffix", i+1, text)
		}
	}

	// The loop waits for the connection to leave READY, so neither the tool
	// calls nor the immediate waits cause another Hello.
	if got := hellos.Load(); got != 1 {
		t.Errorf("the Broker received %d Hello calls after the mismatch; the loop must not poll", got)
	}
	if waits := after.recorded(); len(waits) != 0 {
		t.Errorf("the loop backed off %v while the mismatched connection was still up; it must wait for READY to change", waits)
	}
}

// startMismatchBroker serves a fake Broker on path that rejects the handshake
// the way a real Broker at a different local protocol version does: a
// version_mismatch status carrying the code and the message, and no field for
// the Broker's protocol version.
func startMismatchBroker(t *testing.T, path string) {
	t.Helper()
	fakelocal.Start(t, path, versionMismatchRejecter(new(atomic.Int64)))
}

// REQ-broker-config-016
func TestMismatchRejectionNamesNoBrokerVersion(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	startMismatchBroker(t, sock)

	opts, _ := versionOptions(t, sock, fakelocal.SocketPath(t))
	h := startRunClient(t, opts)

	text := versionWaitMismatch(t, h.cs)
	if strings.Contains(text, "version 0") {
		t.Errorf("list_agents error = %q, must not report a broker version the Broker never sent", text)
	}
	want := fmt.Sprintf("the running Broker speaks a different local protocol version; this MCP server speaks version %d",
		localpb.ProtocolVersion)
	if !strings.Contains(text, want) {
		t.Errorf("list_agents error = %q, want %q", text, want)
	}
	if !strings.Contains(text, "restart the Broker") {
		t.Errorf("list_agents error = %q, want a \"restart the Broker\" hint", text)
	}
	if !strings.HasSuffix(text, " (code: version_mismatch)") {
		t.Errorf("list_agents error = %q, want the version_mismatch code suffix", text)
	}
}

// REQ-broker-config-016
func TestMismatchRecoversAfterBrokerRestart(t *testing.T) {
	sock := fakelocal.SocketPath(t)
	var version atomic.Uint32
	version.Store(2)
	srv := fakelocal.Start(t, sock, fakelocal.Handlers{
		Hello: func(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
			v := version.Load()
			if v == localpb.ProtocolVersion {
				return &localpb.HelloResponse{ProtocolVersion: v}, nil
			}
			return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
				Code:    localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH,
				Message: versionRejectText,
			})
		},
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{SelfAddress: "me@b"}, nil
		},
	})

	opts, _ := versionOptions(t, sock, fakelocal.SocketPath(t))
	h := startRunClient(t, opts)
	versionWaitMismatch(t, h.cs)

	// The user restarts the Broker with a version this MCP server speaks.
	version.Store(localpb.ProtocolVersion)
	srv.Restart()

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	conn, err := srv.NextRegister(ctx)
	if err != nil {
		t.Fatalf("the MCP server did not register after the Broker restarted: %v", err)
	}
	if err := conn.Complete("agent-1", "me@b", false); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	runWaitRegistered(t, h.cs)
}
