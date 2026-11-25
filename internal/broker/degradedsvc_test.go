package broker

import (
	"context"
	"io"
	"log/slog"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/connectivity"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// startTestDegradedService binds a local socket, starts the diagnostics-only
// service on it and returns the client path and the socket.
func startTestDegradedService(t *testing.T, state *localpb.DegradedState) (string, *LocalSocket, *degradedService) {
	t.Helper()
	path := shortSocketDir(t, "broker.sock")
	sock, err := BindLocalSocket(path, 0o600, nil)
	if err != nil {
		t.Fatalf("BindLocalSocket: %v", err)
	}
	t.Cleanup(func() { _ = sock.Close(true) })

	level := new(slog.LevelVar)
	svc, err := startDegradedService(
		sock.Listener(),
		func() *localpb.DegradedState { return cloneDegraded(state) },
		"9.9.9", localpb.ProtocolVersion,
		nil,
		func(Caller) bool { return true },
		NewLogger(io.Discard, level),
	)
	if err != nil {
		t.Fatalf("startDegradedService: %v", err)
	}
	t.Cleanup(svc.Stop)
	return path, sock, svc
}

func degradedClient(t *testing.T, path string) (localpb.LocalClient, *grpc.ClientConn) {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return localpb.NewLocalClient(cc), cc
}

// TestDegradedServiceHelloAndStatus checks that Hello and Status answer while
// diagnostics only, with Status carrying only the version and the fault.
func TestDegradedServiceHelloAndStatus(t *testing.T) {
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Step:   "identity",
		Reason: "cannot read identity.key",
		Fix:    "check the data directory",
	}
	path, _, _ := startTestDegradedService(t, state)
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()

	hello, err := client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion})
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if hello.GetProtocolVersion() != localpb.ProtocolVersion {
		t.Fatalf("Hello protocol = %d, want %d", hello.GetProtocolVersion(), localpb.ProtocolVersion)
	}

	got, err := client.Status(ctx, &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if got.GetCurrentVersion() != "9.9.9" {
		t.Errorf("CurrentVersion = %q, want 9.9.9", got.GetCurrentVersion())
	}
	if len(got.GetAgents()) != 0 || len(got.GetPeers()) != 0 {
		t.Errorf("Status returned agents or peers while diagnostics only: %+v", got)
	}
	if got.GetDegraded() == nil || got.GetDegraded().GetReason() != state.Reason {
		t.Errorf("Degraded = %+v, want the current state", got.GetDegraded())
	}
}

// TestDegradedServiceRefusesOtherCalls checks that calls outside Hello and
// Status return broker_degraded with the fault reason.
func TestDegradedServiceRefusesOtherCalls(t *testing.T) {
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: "cannot open audit.log",
	}
	path, _, _ := startTestDegradedService(t, state)
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()

	_, err := client.ListAgents(ctx, &localpb.ListAgentsRequest{})
	detail := errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED {
		t.Errorf("error code = %v, want broker_degraded", detail.GetCode())
	}
	if detail.GetMessage() != state.Reason {
		t.Errorf("error message = %q, want %q", detail.GetMessage(), state.Reason)
	}

	_, err = client.GetConfig(ctx, &localpb.GetConfigRequest{})
	detail = errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED {
		t.Errorf("GetConfig code = %v, want broker_degraded", detail.GetCode())
	}
}

// TestDegradedServiceRegisterKeepsStreamOpen checks that a registration gets
// not_registered/broker_degraded and the stream stays open.
func TestDegradedServiceRegisterKeepsStreamOpen(t *testing.T) {
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: "cannot read peers.json",
	}
	path, _, _ := startTestDegradedService(t, state)
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()

	stream, err := client.Register(ctx)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{InboxSocketPath: "/tmp/inbox.sock"}); err != nil {
		t.Fatalf("Register send: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("Register recv: %v", err)
	}
	problem := resp.GetNotRegistered()
	if problem == nil {
		t.Fatalf("Register response = %+v, want not_registered", resp)
	}
	if problem.GetReason() != localpb.NotRegisteredReason_NOT_REGISTERED_REASON_BROKER_DEGRADED {
		t.Errorf("reason = %v, want broker_degraded", problem.GetReason())
	}
	if problem.GetDetail() != state.Reason {
		t.Errorf("detail = %q, want %q", problem.GetDetail(), state.Reason)
	}

	// A second frame must not fail and the next receive keeps waiting.
	if err := stream.Send(&localpb.RegisterRequest{}); err != nil {
		t.Fatalf("Register second send: %v", err)
	}
	recvDone := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvDone <- err
	}()
	// The window is the assertion: a premature return fails, a slow run does
	// not.
	select {
	case err := <-recvDone:
		t.Fatalf("Register second recv returned %v, want the stream to stay open", err)
	case <-time.After(300 * time.Millisecond):
	}
	_ = stream.CloseSend()
	select {
	case err := <-recvDone:
		_ = err
	case <-time.After(eventTimeout):
		t.Fatal("Register stream did not end after CloseSend")
	}
}

// TestDegradedServiceStopEndsConnections checks that Stop closes the
// connections established before it.
func TestDegradedServiceStopEndsConnections(t *testing.T) {
	state := &localpb.DegradedState{Level: localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY, Reason: "degraded"}
	path, _, svc := startTestDegradedService(t, state)
	client, cc := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()

	if _, err := client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion}); err != nil {
		t.Fatalf("Hello before Stop: %v", err)
	}
	stream, err := client.Register(ctx)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{InboxSocketPath: "/tmp/inbox.sock"}); err != nil {
		t.Fatalf("Register send: %v", err)
	}
	if _, err := stream.Recv(); err != nil {
		t.Fatalf("Register recv: %v", err)
	}

	svc.Stop()

	recvDone := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvDone <- err
	}()
	select {
	case err := <-recvDone:
		if err == nil {
			t.Fatal("the Register stream continued after Stop")
		}
	case <-time.After(eventTimeout):
		t.Fatal("the Register stream did not end after Stop")
	}

	deadline := time.Now().Add(eventTimeout)
	for time.Now().Before(deadline) {
		if cc.GetState() != connectivity.Ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the client connection stayed ready after Stop: %v", cc.GetState())
}

// TestDegradedServiceStopLeavesListenerOpen checks that the original listener
// still accepts connections after the degraded service stops.
func TestDegradedServiceStopLeavesListenerOpen(t *testing.T) {
	state := &localpb.DegradedState{Level: localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY, Reason: "degraded"}
	path, sock, svc := startTestDegradedService(t, state)

	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	if _, err := client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion}); err != nil {
		t.Fatalf("Hello before Stop: %v", err)
	}

	svc.Stop()

	accepted := make(chan net.Conn, 1)
	acceptErr := make(chan error, 1)
	go func() {
		c, err := sock.Listener().Accept()
		if err != nil {
			acceptErr <- err
			return
		}
		accepted <- c
	}()
	conn, err := net.DialTimeout("unix", path, eventTimeout)
	if err != nil {
		t.Fatalf("dial after Stop: %v", err)
	}
	defer func() { _ = conn.Close() }()
	select {
	case c := <-accepted:
		_ = c.Close()
	case err := <-acceptErr:
		t.Fatalf("accept after Stop: %v", err)
	case <-time.After(eventTimeout):
		t.Fatal("the original listener did not accept after Stop")
	}
}

// TestDegradedServiceEnforcesLimits checks that the diagnostics-only service
// carries the same per-account registration limit as the full service.
func TestDegradedServiceEnforcesLimits(t *testing.T) {
	withLocalLimits(t, maxLocalConnections, maxLocalStreams, 2)
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: "degraded",
	}
	path, _, _ := startTestDegradedService(t, state)
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()

	for i := 0; i < 2; i++ {
		stream, err := client.Register(ctx)
		if err != nil {
			t.Fatalf("registration %d: %v", i+1, err)
		}
		if err := stream.Send(&localpb.RegisterRequest{InboxSocketPath: "/tmp/inbox.sock"}); err != nil {
			t.Fatalf("registration %d send: %v", i+1, err)
		}
		if _, err := stream.Recv(); err != nil {
			t.Fatalf("registration %d recv: %v", i+1, err)
		}
	}

	stream, err := client.Register(ctx)
	if err != nil {
		t.Fatalf("third registration: %v", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{InboxSocketPath: "/tmp/inbox.sock"}); err != nil {
		t.Fatalf("third registration send: %v", err)
	}
	_, err = stream.Recv()
	if status.Code(err) != codes.ResourceExhausted {
		t.Fatalf("third registration: err = %v (code %v), want ResourceExhausted", err, status.Code(err))
	}
}
