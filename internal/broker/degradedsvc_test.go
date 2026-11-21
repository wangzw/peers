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
	sock, err := BindLocalSocket(path, 0o600)
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

func TestDegradedServiceHelloAndStatus(t *testing.T) {
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Step:   "identity",
		Reason: "cannot read identity.key",
		Fix:    "check the data directory",
	}
	path, _, _ := startTestDegradedService(t, state)
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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

func TestDegradedServiceRefusesOtherCalls(t *testing.T) {
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: "cannot open audit.log",
	}
	path, _, _ := startTestDegradedService(t, state)
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
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

func TestDegradedServiceRegisterKeepsStreamOpen(t *testing.T) {
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: "cannot read peers.json",
	}
	path, _, _ := startTestDegradedService(t, state)
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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

	// A second frame must not fail and the next receive must keep waiting
	// rather than see EOF: the stream stays open until the client goes away.
	if err := stream.Send(&localpb.RegisterRequest{}); err != nil {
		t.Fatalf("Register second send: %v", err)
	}
	recvDone := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		recvDone <- err
	}()
	select {
	case err := <-recvDone:
		t.Fatalf("Register second recv returned %v, want the stream to stay open", err)
	case <-time.After(300 * time.Millisecond):
	}
	_ = stream.CloseSend()
	select {
	case err := <-recvDone:
		_ = err
	case <-time.After(2 * time.Second):
		t.Fatal("Register stream did not end after CloseSend")
	}
}

// TestDegradedServiceStopEndsConnections checks the other half of leaving
// diagnostics only: Stop closes the connections established before it, so the
// full service starts on a socket nobody is still talking to the degraded one.
func TestDegradedServiceStopEndsConnections(t *testing.T) {
	state := &localpb.DegradedState{Level: localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY, Reason: "degraded"}
	path, _, svc := startTestDegradedService(t, state)
	client, cc := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
	case <-time.After(2 * time.Second):
		t.Fatal("the Register stream did not end after Stop")
	}

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		if cc.GetState() != connectivity.Ready {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("the client connection stayed ready after Stop: %v", cc.GetState())
}

func TestDegradedServiceStopLeavesListenerOpen(t *testing.T) {
	state := &localpb.DegradedState{Level: localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY, Reason: "degraded"}
	path, sock, svc := startTestDegradedService(t, state)

	// A connection is accepted while the degraded service runs.
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: localpb.ProtocolVersion}); err != nil {
		t.Fatalf("Hello before Stop: %v", err)
	}

	svc.Stop()

	// The original listener still accepts new connections after the degraded
	// service stopped and closed its duplicate.
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
	conn, err := net.DialTimeout("unix", path, 2*time.Second)
	if err != nil {
		t.Fatalf("dial after Stop: %v", err)
	}
	defer func() { _ = conn.Close() }()
	select {
	case c := <-accepted:
		_ = c.Close()
	case err := <-acceptErr:
		t.Fatalf("accept after Stop: %v", err)
	case <-time.After(2 * time.Second):
		t.Fatal("the original listener did not accept after Stop")
	}
}

// TestDegradedServiceEnforcesLimits checks that the diagnostics-only service
// carries the same local interface limits as the full service: with two
// registrations allowed per account, a third is refused with quota_exceeded.
func TestDegradedServiceEnforcesLimits(t *testing.T) {
	withLocalLimits(t, maxLocalConnections, maxLocalStreams, 2)
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: "degraded",
	}
	path, _, _ := startTestDegradedService(t, state)
	client, _ := degradedClient(t, path)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
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
