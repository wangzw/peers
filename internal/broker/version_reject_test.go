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

// callTestListAgents performs one ListAgents on client. When agentID is given,
// the request names that agent; a rejected connection needs no ID because the
// interceptor refuses the call before the handler reads it.
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

// callTestListBrokers performs one ListBrokers on client.
func callTestListBrokers(t *testing.T, client localpb.LocalClient) (*localpb.ListBrokersResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	return client.ListBrokers(ctx, &localpb.ListBrokersRequest{})
}

// rejectedRegister opens a Register stream, sends the first frame for f and
// returns the error the stream receives.
//
// A rejected connection is answered with a trailers-only version_mismatch, so
// Header returns once the Broker has refused the stream. Waiting for that
// answer before the first Send makes the refusal win the race on every runner:
// Send then reports io.EOF because the stream already ended, and the caller
// must read the real status from Recv. Without the wait the interleaving
// depends on scheduling, and macOS runners hit it intermittently.
func rejectedRegister(t *testing.T, client localpb.LocalClient, f *fakesession.Fake) error {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	stream, err := client.Register(ctx)
	if err != nil {
		return err
	}
	// Header reports no error for a trailers-only response; the status is left
	// for Recv.
	_, _ = stream.Header()
	if err := stream.Send(&localpb.RegisterRequest{
		InboxSocketPath: f.SocketPath(),
		ClaudeConfigDir: f.ConfigDir(),
	}); err != nil && !errors.Is(err, io.EOF) {
		// A bidi Send reports io.EOF when the Broker had already ended the
		// stream, and carries no status of its own. That is the normal outcome
		// for a rejected connection once the refusal has arrived, so fall
		// through to Recv, which holds the real error.
		return err
	}
	_, err = stream.Recv()
	return err
}

func TestRejectedConnStaysRejected(t *testing.T) {
	// REQ-broker-config-016: after Hello rejects a version, every other call on
	// that connection fails with version_mismatch.
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

func TestRejectionCarriesNoBrokerVersionField(t *testing.T) {
	// REQ-broker-config-016: the version_mismatch detail carries the code and
	// the message only. It has no field for the Broker's protocol version, so
	// a client that reads the structured detail alone learns no version. The
	// fakelocal fake in mcpserver models exactly these two fields.
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

func TestOtherConnectionsUnaffected(t *testing.T) {
	// REQ-broker-config-016: a rejected connection does not affect other
	// connections or registered agents.
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

	// Another call on the registered connection still succeeds and reports the
	// same self address.
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

func TestRejectedConnHelloStillAnswers(t *testing.T) {
	// REQ-broker-config-016: a matching Hello later answers on the rejected
	// connection but does not clear the mark.
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

func TestRejectionLoggedOnce(t *testing.T) {
	// REQ-broker-config-016: the rejection is logged once, not on every
	// rejected call.
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

// methodRecorder records the full method names that reach an interceptor.
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

func TestVersionInterceptorRunsBeforeAccessCheck(t *testing.T) {
	// REQ-broker-config-016: the version interceptors run before the access
	// interceptor, so a rejected connection never reaches a permission check.
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

func TestVersionServerOptionsAreChained(t *testing.T) {
	if got, want := len(versionServerOptions()), 2; got != want {
		t.Fatalf("versionServerOptions() returned %d options, want %d", got, want)
	}
}
