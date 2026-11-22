package fakelocal

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// eventTimeout is the package-wide fallback for a wait on an event produced by
// the fake Local server. The waits are driven by socket callbacks and by
// Recv/Send progress on a real Unix socket. It only decides how long a wait
// that will never complete takes to fail: the success path selects on the
// event itself and never spends this time, so a larger value costs nothing.
// Thirty seconds is far above the worst normal latency under load; it is not a
// claim that an event should take that long.
const eventTimeout = 30 * time.Second

func dial(t *testing.T, path string) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing %s: %v", path, err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

func waitContext(t *testing.T) (context.Context, context.CancelFunc) {
	t.Helper()
	return context.WithTimeout(context.Background(), eventTimeout)
}

func waitDone(t *testing.T, done <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatalf("timed out waiting for %s", what)
	}
}

func waitStreamError(t *testing.T, stream localpb.Local_RegisterClient) error {
	t.Helper()
	errCh := make(chan error, 1)
	go func() {
		_, err := stream.Recv()
		errCh <- err
	}()
	select {
	case err := <-errCh:
		if err == nil {
			t.Fatal("expected the register stream to end with an error")
		}
		return err
	case <-time.After(eventTimeout):
		t.Fatal("timed out waiting for the register stream to end")
		return nil
	}
}

func TestHelloAnswersProtocolVersion(t *testing.T) {
	t.Parallel()
	path := SocketPath(t)
	Start(t, path, Handlers{})
	client := localpb.NewLocalClient(dial(t, path))

	ctx, cancel := waitContext(t)
	defer cancel()
	resp, err := client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: 99})
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if resp.GetProtocolVersion() != localpb.ProtocolVersion {
		t.Fatalf("Hello protocol version = %d, want %d", resp.GetProtocolVersion(), localpb.ProtocolVersion)
	}
}

func TestRegisterStream(t *testing.T) {
	t.Parallel()
	path := SocketPath(t)
	s := Start(t, path, Handlers{})
	client := localpb.NewLocalClient(dial(t, path))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Register(ctx)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	first := &localpb.RegisterRequest{InboxSocketPath: "/tmp/inbox.sock", ClaudeConfigDir: "/cfg"}
	if err := stream.Send(first); err != nil {
		t.Fatalf("sending the first frame: %v", err)
	}

	waitCtx, waitCancel := waitContext(t)
	defer waitCancel()
	conn, err := s.NextRegister(waitCtx)
	if err != nil {
		t.Fatalf("NextRegister: %v", err)
	}
	if !proto.Equal(conn.First, first) {
		t.Fatalf("First = %v, want %v", conn.First, first)
	}

	if err := conn.Complete("agent-1", "alice@hostB", true); err != nil {
		t.Fatalf("Complete: %v", err)
	}
	resp, err := stream.Recv()
	if err != nil {
		t.Fatalf("receiving registration_complete: %v", err)
	}
	got := resp.GetRegistrationComplete()
	if got == nil {
		t.Fatalf("response = %v, want registration_complete", resp)
	}
	if got.GetAgentId() != "agent-1" || got.GetAgentAddress() != "alice@hostB" || !got.GetAutoJoin() {
		t.Fatalf("registration_complete = %v", got)
	}

	cancel()
	waitDone(t, conn.Done(), "Done to close after the client cancels")
}

func TestStopRemovesSocketAndRestartWorks(t *testing.T) {
	t.Parallel()
	path := SocketPath(t)
	s := Start(t, path, Handlers{})
	client := localpb.NewLocalClient(dial(t, path))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream, err := client.Register(ctx)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{InboxSocketPath: "/tmp/inbox.sock"}); err != nil {
		t.Fatalf("sending the first frame: %v", err)
	}
	waitCtx, waitCancel := waitContext(t)
	defer waitCancel()
	if _, err := s.NextRegister(waitCtx); err != nil {
		t.Fatalf("NextRegister: %v", err)
	}

	s.Stop()
	if _, err := os.Stat(path); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("socket file after Stop: stat error = %v, want a not-exist error", err)
	}
	_ = waitStreamError(t, stream)

	Start(t, path, Handlers{})
	reconnected := localpb.NewLocalClient(dial(t, path))
	helloCtx, helloCancel := waitContext(t)
	defer helloCancel()
	if _, err := reconnected.Hello(helloCtx, &localpb.HelloRequest{}); err != nil {
		t.Fatalf("Hello after restarting on the same path: %v", err)
	}
}

func TestErrorStatus(t *testing.T) {
	t.Parallel()
	detail := &localpb.ErrorDetail{
		Code:                         localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
		Message:                      "not visible",
		OriginatingBrokerFingerprint: []byte{0xde, 0xad, 0xbe, 0xef},
	}
	err := ErrorStatus(codes.PermissionDenied, detail)

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("status.FromError(%v) did not recognize a status error", err)
	}
	if st.Code() != codes.PermissionDenied {
		t.Fatalf("code = %v, want %v", st.Code(), codes.PermissionDenied)
	}
	if st.Message() != "not visible" {
		t.Fatalf("message = %q, want %q", st.Message(), "not visible")
	}
	details := st.Details()
	if len(details) != 1 {
		t.Fatalf("details = %v, want exactly one ErrorDetail", details)
	}
	got, ok := details[0].(*localpb.ErrorDetail)
	if !ok {
		t.Fatalf("detail type = %T, want *localpb.ErrorDetail", details[0])
	}
	if !proto.Equal(got, detail) {
		t.Fatalf("detail = %v, want %v", got, detail)
	}
}

func TestRestart(t *testing.T) {
	t.Parallel()
	path := SocketPath(t)
	s := Start(t, path, Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{}, nil
		},
	})
	client := localpb.NewLocalClient(dial(t, path))

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	if _, err := client.ListAgents(ctx, &localpb.ListAgentsRequest{Topic: "pre"}); err != nil {
		t.Fatalf("ListAgents: %v", err)
	}

	streamCtx, streamCancel := context.WithCancel(context.Background())
	defer streamCancel()
	stream, err := client.Register(streamCtx)
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{InboxSocketPath: "/tmp/inbox.sock"}); err != nil {
		t.Fatalf("sending the first frame: %v", err)
	}
	waitCtx, waitCancel := waitContext(t)
	defer waitCancel()
	if _, err := s.NextRegister(waitCtx); err != nil {
		t.Fatalf("NextRegister: %v", err)
	}

	s.Restart()
	_ = waitStreamError(t, stream)

	helloCtx, helloCancel := waitContext(t)
	defer helloCancel()
	fresh := localpb.NewLocalClient(dial(t, path))
	if _, err := fresh.Hello(helloCtx, &localpb.HelloRequest{}); err != nil {
		t.Fatalf("Hello after Restart: %v", err)
	}

	var found bool
	for _, call := range s.Calls() {
		if call.Method != "ListAgents" {
			continue
		}
		req, ok := call.Request.(*localpb.ListAgentsRequest)
		if ok && req.GetTopic() == "pre" {
			found = true
		}
	}
	if !found {
		t.Fatalf("Calls after Restart = %v, want the pre-restart ListAgents request", s.Calls())
	}
}

func TestCallsRecordUnaryRequestsInOrder(t *testing.T) {
	t.Parallel()
	path := SocketPath(t)
	s := Start(t, path, Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{}, nil
		},
		Send: func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error) {
			return &localpb.SendResponse{}, nil
		},
	})
	client := localpb.NewLocalClient(dial(t, path))

	ctx, cancel := waitContext(t)
	defer cancel()
	listReq := &localpb.ListAgentsRequest{Topic: "t"}
	if _, err := client.ListAgents(ctx, listReq); err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	sendReq := &localpb.SendRequest{ToAddress: "x@hostB", Message: "hi"}
	if _, err := client.Send(ctx, sendReq); err != nil {
		t.Fatalf("Send: %v", err)
	}

	calls := s.Calls()
	if len(calls) != 2 {
		t.Fatalf("Calls = %v, want two entries", calls)
	}
	if calls[0].Method != "ListAgents" || !proto.Equal(calls[0].Request, listReq) {
		t.Fatalf("Calls[0] = %v, want the ListAgents request", calls[0])
	}
	if calls[1].Method != "Send" || !proto.Equal(calls[1].Request, sendReq) {
		t.Fatalf("Calls[1] = %v, want the Send request", calls[1])
	}
}

func TestSocketPath(t *testing.T) {
	t.Parallel()
	path := SocketPath(t)
	if len(path) >= 100 {
		t.Fatalf("socket path %q is %d bytes, want fewer than 100", path, len(path))
	}
	base := filepath.Base(filepath.Dir(path))
	if !strings.HasPrefix(base, "ap-") {
		t.Fatalf("socket directory %q does not start with ap-", base)
	}
}

// blockingRegisterStream is a Local_RegisterServer whose Send blocks until the
// test releases it. It lets a test hold one Send in flight while the Register
// handler tries to return.
type blockingRegisterStream struct {
	grpc.ServerStream

	ctx         context.Context
	first       *localpb.RegisterRequest
	sendEntered chan struct{}
	sendRelease chan struct{}
}

func (s *blockingRegisterStream) Context() context.Context { return s.ctx }

func (s *blockingRegisterStream) Recv() (*localpb.RegisterRequest, error) {
	if s.first != nil {
		req := s.first
		s.first = nil
		return req, nil
	}
	<-s.ctx.Done()
	return nil, s.ctx.Err()
}

func (s *blockingRegisterStream) Send(*localpb.RegisterResponse) error {
	close(s.sendEntered)
	<-s.sendRelease
	return nil
}

// TestRegisterWaitsForInFlightSend checks that the Register handler does not
// return (and close Done) while a Send is still writing on the stream: grpc-go
// forbids using a server stream after its handler has returned.
func TestRegisterWaitsForInFlightSend(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	stream := &blockingRegisterStream{
		ctx:         ctx,
		first:       &localpb.RegisterRequest{InboxSocketPath: "/s"},
		sendEntered: make(chan struct{}),
		sendRelease: make(chan struct{}),
	}
	s := &Server{
		t:       t,
		streams: make(map[*RegisterConn]struct{}),
		notify:  make(chan struct{}, 1),
		closed:  make(chan struct{}),
	}
	handlerDone := make(chan struct{})
	go func() {
		_ = (&service{s: s}).Register(stream)
		close(handlerDone)
	}()

	regCtx, regCancel := context.WithTimeout(context.Background(), eventTimeout)
	defer regCancel()
	conn, err := s.NextRegister(regCtx)
	if err != nil {
		t.Fatalf("waiting for the Register stream: %v", err)
	}

	sendDone := make(chan error, 1)
	go func() { sendDone <- conn.Send(&localpb.RegisterResponse{}) }()
	select {
	case <-stream.sendEntered:
	case <-time.After(eventTimeout):
		t.Fatal("Send did not reach the stream")
	}

	conn.End()
	// Supplement: the handler's cleanup deletes the stream from s.streams before
	// it blocks on conn.sendMu, which the in-flight Send holds until
	// sendRelease. Wait for that deletion, then check Done directly: Done can be
	// closed only after the Send finishes.
	deadline := time.Now().Add(eventTimeout)
	for {
		s.mu.Lock()
		left := len(s.streams)
		s.mu.Unlock()
		if left == 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("the Register handler did not reach its cleanup")
		}
		time.Sleep(time.Millisecond)
	}
	select {
	case <-conn.Done():
		t.Fatal("the Register stream closed Done while a Send was still in flight")
	default:
	}
	// Negative window: a handler that returned while the Send was still in
	// flight shows up here.
	select {
	case <-handlerDone:
		t.Fatal("the Register handler returned while a Send was still in flight")
	case <-time.After(200 * time.Millisecond):
	}

	close(stream.sendRelease)
	if err := <-sendDone; err != nil {
		t.Fatalf("Send: %v", err)
	}
	select {
	case <-handlerDone:
	case <-time.After(eventTimeout):
		t.Fatal("the Register handler did not return after the Send finished")
	}
}
