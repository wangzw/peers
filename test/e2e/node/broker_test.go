package node

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

func init() {
	registerStub("broker-ok", runBrokerOKStub)
	registerStub("broker-fail", runBrokerFailStub)
}

type oracleTestServer struct {
	localpb.UnimplementedLocalServer

	mu         sync.Mutex
	auditLimit uint32
}

func (s *oracleTestServer) Hello(context.Context, *localpb.HelloRequest) (*localpb.HelloResponse, error) {
	return &localpb.HelloResponse{ProtocolVersion: localpb.ProtocolVersion}, nil
}

func (s *oracleTestServer) GetConfig(_ context.Context, req *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
	return &localpb.GetConfigResponse{Entries: []*localpb.ConfigEntry{{Key: req.GetKey()}}}, nil
}

func (s *oracleTestServer) Audit(req *localpb.AuditRequest, stream grpc.ServerStreamingServer[localpb.AuditEvent]) error {
	s.mu.Lock()
	s.auditLimit = req.GetLimit()
	s.mu.Unlock()
	for i := 0; i < 3; i++ {
		if err := stream.Send(&localpb.AuditEvent{Detail: fmt.Sprintf("event-%d", i)}); err != nil {
			return err
		}
	}
	return nil
}

func runBrokerOKStub() int {
	watchStubSignals()
	socket := os.Getenv("STUB_SOCKET")
	_ = os.Remove(socket)
	listener, err := net.Listen("unix", socket)
	if err != nil {
		fmt.Fprintf(os.Stderr, "broker stub: listening on %s: %v\n", socket, err)
		return 1
	}
	server := grpc.NewServer()
	localpb.RegisterLocalServer(server, &oracleTestServer{})
	if err := server.Serve(listener); err != nil {
		fmt.Fprintf(os.Stderr, "broker stub: serving: %v\n", err)
		return 1
	}
	return 0
}

// The JSON log line comes after the prefixed line, so a reader taking the last
// line gets the wrong one.
func runBrokerFailStub() int {
	fmt.Fprintln(os.Stderr, brokerStderrPrefix+"listen address :1 is not available")
	fmt.Fprintln(os.Stderr, `{"time":"2026-11-18T00:00:00Z","level":"error","event":"listen_failed"}`)
	return 4
}

func brokerTestEnv(t *testing.T, more map[string]string) []string {
	t.Helper()
	path := os.Getenv("PATH")
	if path == "" {
		path = "/usr/bin:/bin"
	}
	return BaseEnv{Home: t.TempDir(), Path: path}.Build(more)
}

// TestStartBrokerReady checks that StartBroker reports a ready Broker in its
// own session and that the handle stops it with SIGTERM.
func TestStartBrokerReady(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("ListProcs is only implemented on Linux")
	}
	dir := socktest.Dir(t)
	socket := filepath.Join(dir, "broker.sock")
	logPath := filepath.Join(t.TempDir(), "broker.log")

	proc, result, err := StartBroker(context.Background(), BrokerSpec{
		Path:    os.Args[0],
		Env:     brokerTestEnv(t, map[string]string{"E2E_NODE_STUB": "broker-ok", "STUB_SOCKET": socket}),
		LogPath: logPath,
		Socket:  socket,
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("StartBroker: %v", err)
	}
	if !result.Running {
		t.Fatalf("Running = false, want true (exit code %d, stderr %q)", result.ExitCode, result.StderrLast)
	}
	if result.PID <= 0 {
		t.Fatalf("PID = %d, want > 0", result.PID)
	}
	if got := proc.PID(); got != result.PID {
		t.Fatalf("BrokerProc.PID() = %d, want %d", got, result.PID)
	}

	procs, err := ListProcs(func(p Proc) bool { return p.PID == result.PID })
	if err != nil {
		t.Fatalf("ListProcs: %v", err)
	}
	if len(procs) != 1 {
		t.Fatalf("ListProcs found %d processes for PID %d, want 1", len(procs), result.PID)
	}
	if procs[0].SID != result.PID {
		t.Fatalf("SID = %d, want %d: the Broker did not start in its own session", procs[0].SID, result.PID)
	}

	if err := proc.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("Signal(SIGTERM): %v", err)
	}
	code, err := proc.Wait(5 * time.Second)
	if err != nil {
		t.Fatalf("Wait: %v", err)
	}
	if code != 0 {
		t.Fatalf("exit code = %d, want 0", code)
	}
}

// TestBrokerProcRefusesSignalAfterReap checks that a signal sent after the
// process was waited for is refused and leaves the sent count unchanged.
func TestBrokerProcRefusesSignalAfterReap(t *testing.T) {
	dir := socktest.Dir(t)
	socket := filepath.Join(dir, "broker.sock")
	logPath := filepath.Join(t.TempDir(), "broker.log")

	proc, result, err := StartBroker(context.Background(), BrokerSpec{
		Path:    os.Args[0],
		Env:     brokerTestEnv(t, map[string]string{"E2E_NODE_STUB": "broker-ok", "STUB_SOCKET": socket}),
		LogPath: logPath,
		Socket:  socket,
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("StartBroker: %v", err)
	}
	if !result.Running {
		t.Fatalf("Running = false, want true (exit code %d, stderr %q)", result.ExitCode, result.StderrLast)
	}
	if err := proc.SendSignal(syscall.SIGTERM); err != nil {
		t.Fatalf("SendSignal(SIGTERM): %v", err)
	}
	if _, err := proc.Wait(5 * time.Second); err != nil {
		t.Fatalf("Wait: %v", err)
	}
	before := proc.SignalsSent()

	err = proc.SendSignal(syscall.SIGQUIT)
	if !errors.Is(err, ErrBrokerReaped) {
		t.Fatalf("SendSignal after reap = %v, want ErrBrokerReaped", err)
	}
	if got := proc.SignalsSent(); got != before {
		t.Fatalf("SignalsSent = %d after a refused signal, want %d", got, before)
	}
	if err := proc.Signal(syscall.SIGKILL); !errors.Is(err, ErrBrokerReaped) {
		t.Fatalf("Signal after reap = %v, want ErrBrokerReaped", err)
	}
}

// TestBrokerProcSignalsRaceTheReap races a signal loop against the reap and
// checks that the loop neither deadlocks nor hands procsig a signal after the
// reap, and that every later signal is refused and not counted.
func TestBrokerProcSignalsRaceTheReap(t *testing.T) {
	const iterations = 40
	for i := 0; i < iterations; i++ {
		dir := socktest.Dir(t)
		socket := filepath.Join(dir, "broker.sock")
		logPath := filepath.Join(t.TempDir(), "broker.log")

		proc, result, err := StartBroker(context.Background(), BrokerSpec{
			Path:    os.Args[0],
			Env:     brokerTestEnv(t, map[string]string{"E2E_NODE_STUB": "broker-ok", "STUB_SOCKET": socket}),
			LogPath: logPath,
			Socket:  socket,
		}, 5*time.Second)
		if err != nil {
			t.Fatalf("iteration %d: StartBroker: %v", i, err)
		}
		if !result.Running {
			t.Fatalf("iteration %d: Running = false (exit %d): %s", i, result.ExitCode, result.StderrLast)
		}
		proc.TraceSends()

		assertNoSendAfterReap := func() {
			t.Helper()
			for _, send := range proc.SignalSends() {
				if send.Reaped {
					t.Fatalf("iteration %d: %v was handed to procsig after the Broker was reaped", i, send.Sig)
				}
			}
		}

		stop := make(chan struct{})
		var (
			sender  sync.WaitGroup
			sendErr error
		)
		sender.Add(1)
		go func() {
			defer sender.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				if err := proc.SendSignal(syscall.SIGQUIT); err != nil {
					if !errors.Is(err, ErrBrokerReaped) {
						sendErr = err
					}
					return
				}
			}
		}()

		// A refusal means the reap already won the race, which is the safe
		// outcome.
		stopErr := proc.SendSignal(syscall.SIGTERM)
		if stopErr != nil && !errors.Is(stopErr, ErrBrokerReaped) {
			close(stop)
			sender.Wait()
			t.Fatalf("iteration %d: SendSignal(SIGTERM): %v", i, stopErr)
		}
		_, waitErr := proc.Wait(5 * time.Second)
		close(stop)

		waited := make(chan struct{})
		go func() { sender.Wait(); close(waited) }()
		select {
		case <-waited:
		case <-time.After(5 * time.Second):
			t.Fatalf("iteration %d: the signal loop deadlocked with the reap", i)
		}
		assertNoSendAfterReap()
		if sendErr != nil {
			t.Fatalf("iteration %d: SendSignal(SIGQUIT): %v", i, sendErr)
		}
		if waitErr != nil {
			t.Fatalf("iteration %d: Wait: %v", i, waitErr)
		}

		before := proc.SignalsSent()
		reapErr := proc.SendSignal(syscall.SIGTERM)
		assertNoSendAfterReap()
		if !errors.Is(reapErr, ErrBrokerReaped) {
			t.Fatalf("iteration %d: SendSignal after reap = %v, want ErrBrokerReaped", i, reapErr)
		}
		if got := proc.SignalsSent(); got != before {
			t.Fatalf("iteration %d: SignalsSent changed on a refused signal: %d -> %d", i, before, got)
		}
	}
}

// TestStartBrokerFailure checks that a Broker which exits before becoming ready
// is reported with its exit code and last failure line instead of an error.
func TestStartBrokerFailure(t *testing.T) {
	dir := socktest.Dir(t)
	socket := filepath.Join(dir, "broker.sock")
	logPath := filepath.Join(dir, "broker.log")

	_, result, err := StartBroker(context.Background(), BrokerSpec{
		Path:    os.Args[0],
		Env:     brokerTestEnv(t, map[string]string{"E2E_NODE_STUB": "broker-fail"}),
		LogPath: logPath,
		Socket:  socket,
	}, 5*time.Second)
	if err != nil {
		t.Fatalf("StartBroker returned an error for a Broker that exited: %v", err)
	}
	if result.Running {
		t.Fatalf("Running = true, want false")
	}
	if result.ExitCode != 4 {
		t.Fatalf("ExitCode = %d, want 4", result.ExitCode)
	}
	const want = "listen address :1 is not available"
	if result.StderrLast != want {
		t.Fatalf("StderrLast = %q, want %q", result.StderrLast, want)
	}
}

func startOracleTestServer(t *testing.T, srv localpb.LocalServer) string {
	t.Helper()
	socket := filepath.Join(socktest.Dir(t), "local.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("node: listening on %s: %v", socket, err)
	}
	server := grpc.NewServer()
	localpb.RegisterLocalServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)
	return socket
}

// TestOracleGetConfigAndAudit checks GetConfig and Audit on the oracle,
// including the audit stream's order and the limit it sends.
func TestOracleGetConfigAndAudit(t *testing.T) {
	srv := &oracleTestServer{}
	oracle, err := DialOracle(startOracleTestServer(t, srv))
	if err != nil {
		t.Fatalf("DialOracle: %v", err)
	}
	defer func() { _ = oracle.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	config, err := oracle.GetConfig(ctx, "broker.alias")
	if err != nil {
		t.Fatalf("GetConfig: %v", err)
	}
	if len(config.GetEntries()) != 1 || config.GetEntries()[0].GetKey() != "broker.alias" {
		t.Fatalf("GetConfig returned %v, want one entry for broker.alias", config.GetEntries())
	}

	events, err := oracle.Audit(ctx, &localpb.AuditRequest{Limit: 2})
	if err != nil {
		t.Fatalf("Audit: %v", err)
	}
	if len(events) != 3 {
		t.Fatalf("Audit returned %d events, want all 3", len(events))
	}
	for i, event := range events {
		if want := fmt.Sprintf("event-%d", i); event.GetDetail() != want {
			t.Fatalf("event %d = %q, want %q: the stream order was not kept", i, event.GetDetail(), want)
		}
	}

	srv.mu.Lock()
	limit := srv.auditLimit
	srv.mu.Unlock()
	if limit != 2 {
		t.Fatalf("server received limit = %d, want 2", limit)
	}
}

// TestOracleErrorDetail checks that ErrorDetailOf finds an ErrorDetail in a gRPC
// status and reports none for other errors.
func TestOracleErrorDetail(t *testing.T) {
	oracle, err := DialOracle(startOracleTestServer(t, &oracleTestServer{}))
	if err != nil {
		t.Fatalf("DialOracle: %v", err)
	}
	defer func() { _ = oracle.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err = oracle.ListBrokers(ctx)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("ListBrokers error = %v, want Unimplemented", err)
	}
	if detail := ErrorDetailOf(err); detail != nil {
		t.Fatalf("ErrorDetailOf(Unimplemented) = %v, want nil", detail)
	}

	detail := &localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
		Message: "missing agent",
	}
	withDetails, err := status.New(codes.NotFound, "missing agent").WithDetails(detail)
	if err != nil {
		t.Fatalf("WithDetails: %v", err)
	}
	got := ErrorDetailOf(withDetails.Err())
	if got == nil {
		t.Fatalf("ErrorDetailOf returned nil for a status with an ErrorDetail")
	}
	if got.GetCode() != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want NOT_FOUND", got.GetCode())
	}
}

// TestAncestors checks that the ancestors of a child process include the test
// process.
func TestAncestors(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("Ancestors is only implemented on Linux")
	}
	child := exec.Command("sleep", "30")
	if err := child.Start(); err != nil {
		t.Fatalf("starting sleep: %v", err)
	}
	t.Cleanup(func() {
		_ = child.Process.Kill()
		_ = child.Wait()
	})

	ancestors, err := Ancestors(child.Process.Pid)
	if err != nil {
		t.Fatalf("Ancestors: %v", err)
	}
	self := os.Getpid()
	for _, pid := range ancestors {
		if pid == self {
			return
		}
	}
	t.Fatalf("Ancestors(%d) = %v, want it to contain the test process PID %d", child.Process.Pid, ancestors, self)
}

// TestInterpretWait4 checks the reaper's reading of a running child, an
// interrupted poll and a reaped child, including a signal ending it.
func TestInterpretWait4(t *testing.T) {
	const pid = 1234
	tests := []struct {
		name       string
		wpid       int
		err        error
		status     syscall.WaitStatus
		wantReaped bool
		wantExit   int
	}{
		{name: "still running", wpid: 0, status: 0, wantReaped: false, wantExit: 0},
		{name: "interrupted", wpid: 0, err: syscall.EINTR, status: 0, wantReaped: false, wantExit: 0},
		{name: "reaped with a code", wpid: pid, status: syscall.WaitStatus(7 << 8), wantReaped: true, wantExit: 7},
		{name: "reaped by a signal", wpid: pid, status: syscall.WaitStatus(syscall.SIGKILL), wantReaped: true, wantExit: -1},
		{name: "no child", wpid: 0, err: syscall.ECHILD, status: 0, wantReaped: true, wantExit: -1},
		{name: "unexpected error", wpid: 0, err: syscall.EINVAL, status: 0, wantReaped: true, wantExit: -1},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			reaped, exit := interpretWait4(pid, tc.wpid, tc.err, tc.status)
			if reaped != tc.wantReaped || exit != tc.wantExit {
				t.Fatalf("interpretWait4 = (%v, %d), want (%v, %d)", reaped, exit, tc.wantReaped, tc.wantExit)
			}
		})
	}
}

// TestInterpretWait4ReapsRealChild checks that the WNOHANG poll, as the only
// waiter, reaps a real child and reports its exit code.
func TestInterpretWait4ReapsRealChild(t *testing.T) {
	child := exec.Command("sh", "-c", "exit 7")
	if err := child.Start(); err != nil {
		t.Fatalf("starting the child: %v", err)
	}
	// Do not call child.Wait: the poll below must be the only waiter, as on a
	// platform without waitid(WNOWAIT).
	deadline := time.Now().Add(10 * time.Second)
	for {
		var status syscall.WaitStatus
		wpid, err := syscall.Wait4(child.Process.Pid, &status, syscall.WNOHANG, nil)
		reaped, exit := interpretWait4(child.Process.Pid, wpid, err, status)
		if reaped {
			if exit != 7 {
				t.Fatalf("exit code = %d, want 7", exit)
			}
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("child %d was not reaped within the deadline", child.Process.Pid)
		}
		time.Sleep(5 * time.Millisecond)
	}
}
