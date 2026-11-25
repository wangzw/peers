package broker

import (
	"context"
	"errors"
	"io"
	"slices"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/protobuf/reflect/protoreflect"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

func callTestListAgents(t *testing.T, client localpb.LocalClient, agentID ...string) (*localpb.ListAgentsResponse, error) {
	t.Helper()
	var id string
	if len(agentID) > 0 {
		id = agentID[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	return client.ListAgents(ctx, &localpb.ListAgentsRequest{AgentId: id})
}

func callTestListBrokers(t *testing.T, client localpb.LocalClient) (*localpb.ListBrokersResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	return client.ListBrokers(ctx, &localpb.ListBrokersRequest{})
}

// rejectedRegister opens a Register stream and returns the error it receives. It
// waits for Header before the first Send: a rejected connection answers with a
// trailers-only version_mismatch, and without the wait the refusal races the
// Send on some runners.
func rejectedRegister(t *testing.T, client localpb.LocalClient, f *fakesession.Fake) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	stream, err := client.Register(ctx)
	if err != nil {
		return err
	}
	_, _ = stream.Header()
	if err := stream.Send(&localpb.RegisterRequest{
		InboxSocketPath: f.SocketPath(),
		ClaudeConfigDir: f.ConfigDir(),
	}); err != nil && !errors.Is(err, io.EOF) {
		return err
	}
	_, err = stream.Recv()
	return err
}

// TestRejectedConnStaysRejected checks that after Hello rejects a version every
// other call on that connection fails with version_mismatch.
func TestRejectedConnStaysRejected(t *testing.T) {
	tb := newTestBroker(t)
	f := fakesession.New(t, fakesession.Options{Name: "a"})

	if _, err := callTestHello(t, tb.client, 2); err == nil {
		t.Fatal("Hello{2} succeeded, want a version mismatch")
	}

	_, err := callTestListAgents(t, tb.client)
	detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)
	for _, want := range []string{"restart the Broker", "version 2"} {
		if !strings.Contains(detail.GetMessage(), want) {
			t.Errorf("ListAgents message %q does not contain %q", detail.GetMessage(), want)
		}
	}

	_, err = callTestListBrokers(t, tb.client)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)

	err = rejectedRegister(t, tb.client, f)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)

	w := tb.newClient(t)
	if _, err := callTestHello(t, w, localpb.ProtocolVersion); err != nil {
		t.Fatalf("Hello on another connection: %v", err)
	}
	if got := tb.lockedAgentCount(); got != 0 {
		t.Fatalf("the agent table lists %d agents, want none: the rejected Register must not have registered anything", got)
	}
	if uids := tb.sessions.AcquiredUIDs(); len(uids) != 0 {
		t.Fatalf("the session backend was acquired %d times, want none: the rejected Register must not have reached the handler", len(uids))
	}
}

// TestRejectionCarriesNoBrokerVersionField checks that the version_mismatch
// detail carries only the code and the message.
func TestRejectionCarriesNoBrokerVersionField(t *testing.T) {
	tb := newTestBroker(t)

	_, err := callTestHello(t, tb.client, 2)
	detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)

	var fields []string
	detail.ProtoReflect().Range(func(fd protoreflect.FieldDescriptor, _ protoreflect.Value) bool {
		fields = append(fields, string(fd.Name()))
		return true
	})
	slices.Sort(fields)
	if want := []string{"code", "message"}; !slices.Equal(fields, want) {
		t.Fatalf("version_mismatch detail carries fields %v, want exactly %v: the rejection must not carry the Broker's protocol version", fields, want)
	}
}

// TestOtherConnectionsUnaffected checks that a rejected connection leaves other
// connections and registered agents unaffected.
func TestOtherConnectionsUnaffected(t *testing.T) {
	tb := newTestBroker(t)
	f := fakesession.New(t, fakesession.Options{Name: "a"})

	y := tb.newClient(t)
	stream := startRegistrationAt(t, y, f.ConfigDir(), f.SocketPath())
	stream.complete(eventTimeout)
	id := mustAgentID(t, f.SocketPath())
	before, ok := tb.b.lookupAgent(id)
	if !ok {
		t.Fatal("the registered agent is missing before the rejection")
	}
	beforeList, err := callTestListAgents(t, y, string(id))
	if err != nil {
		t.Fatalf("ListAgents on the registered connection before the rejection: %v", err)
	}
	if beforeList.GetSelfAddress() == "" {
		t.Fatal("ListAgents returned an empty self_address for the registered connection")
	}

	if _, err := callTestHello(t, tb.client, 2); err == nil {
		t.Fatal("Hello{2} succeeded, want a version mismatch")
	}

	afterList, err := callTestListAgents(t, y, string(id))
	if err != nil {
		t.Fatalf("ListAgents on the registered connection after the rejection: %v", err)
	}
	if got, want := afterList.GetSelfAddress(), beforeList.GetSelfAddress(); got != want {
		t.Errorf("self_address after the rejection = %q, want unchanged %q", got, want)
	}

	after, ok := tb.b.lookupAgent(id)
	if !ok {
		t.Fatal("the registered agent disappeared after the rejection")
	}
	if after.Address != before.Address {
		t.Errorf("agent address = %q, want unchanged %q", after.Address, before.Address)
	}
	if got := tb.lockedConnCount(id); got != 1 {
		t.Errorf("connections of the agent = %d, want 1", got)
	}
	select {
	case err := <-stream.errs:
		t.Errorf("the registration stream of the other connection ended: %v", err)
	default:
	}

	w := tb.newClient(t)
	resp, err := callTestHello(t, w, localpb.ProtocolVersion)
	if err != nil {
		t.Fatalf("Hello{1} on a new connection: %v", err)
	}
	if got := resp.GetProtocolVersion(); got != localpb.ProtocolVersion {
		t.Errorf("Hello{1} returned version %d", got)
	}
}

// TestRejectedConnHelloStillAnswers checks that a matching Hello answers on a
// rejected connection but does not clear the mark.
func TestRejectedConnHelloStillAnswers(t *testing.T) {
	tb := newTestBroker(t)

	if _, err := callTestHello(t, tb.client, 2); err == nil {
		t.Fatal("Hello{2} succeeded, want a version mismatch")
	}

	resp, err := callTestHello(t, tb.client, localpb.ProtocolVersion)
	if err != nil {
		t.Fatalf("Hello{1} after a rejected Hello: %v", err)
	}
	if got := resp.GetProtocolVersion(); got != localpb.ProtocolVersion {
		t.Fatalf("ProtocolVersion = %d, want %d", got, localpb.ProtocolVersion)
	}

	_, err = callTestListAgents(t, tb.client)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)
}

// TestRejectionLoggedOnce checks that a rejection is logged once, not on every
// rejected call.
func TestRejectionLoggedOnce(t *testing.T) {
	tb := newTestBroker(t)

	if _, err := callTestHello(t, tb.client, 2); err == nil {
		t.Fatal("Hello{2} succeeded, want a version mismatch")
	}
	if _, err := callTestHello(t, tb.client, localpb.ProtocolVersion); err != nil {
		t.Fatalf("Hello{1} after a rejected Hello: %v", err)
	}
	_, err := callTestListAgents(t, tb.client)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)

	var count int
	for _, rec := range tb.logLines(t) {
		if rec["event"] != EvHelloVersionMismatch {
			continue
		}
		count++
		if got := rec["client_version"]; got != float64(2) {
			t.Errorf("client_version = %v, want 2", got)
		}
	}
	if count != 1 {
		t.Fatalf("%s records = %d, want 1: %v", EvHelloVersionMismatch, count, tb.logLines(t))
	}
}

type methodRecorder struct {
	mu      sync.Mutex
	methods []string
}

func (r *methodRecorder) record(method string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.methods = append(r.methods, method)
}

func (r *methodRecorder) list() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string(nil), r.methods...)
}

// TestVersionInterceptorRunsBeforeAccessCheck checks that the version
// interceptor rejects a call before the access interceptor sees it.
func TestVersionInterceptorRunsBeforeAccessCheck(t *testing.T) {
	rec := new(methodRecorder)
	accessUnary := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		rec.record(info.FullMethod)
		return handler(ctx, req)
	}
	accessStream := func(srv any, ss grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		rec.record(info.FullMethod)
		return handler(srv, ss)
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.ServerOptions = []grpc.ServerOption{
			grpc.ChainUnaryInterceptor(accessUnary),
			grpc.ChainStreamInterceptor(accessStream),
		}
	})
	f := fakesession.New(t, fakesession.Options{Name: "a"})

	if _, err := callTestHello(t, tb.client, 2); err == nil {
		t.Fatal("Hello{2} succeeded, want a version mismatch")
	}
	_, err := callTestListAgents(t, tb.client)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)
	err = rejectedRegister(t, tb.client, f)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)

	got := rec.list()
	want := []string{localpb.Local_Hello_FullMethodName}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("access interceptor saw %v, want only %v", got, want)
	}
}

// TestVersionServerOptionsAreChained checks that versionServerOptions returns
// both a unary and a stream interceptor.
func TestVersionServerOptionsAreChained(t *testing.T) {
	if got, want := len(versionServerOptions()), 2; got != want {
		t.Fatalf("versionServerOptions() returned %d options, want %d", got, want)
	}
}
