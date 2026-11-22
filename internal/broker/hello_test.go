package broker

import (
	"context"
	"strings"
	"sync/atomic"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// callTestHello performs one Hello on client with version v.
func callTestHello(t *testing.T, client localpb.LocalClient, v uint32) (*localpb.HelloResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	return client.Hello(ctx, &localpb.HelloRequest{ProtocolVersion: v})
}

// wantErrorDetail asserts that err carries one ErrorDetail with code want and
// returns it.
func wantErrorDetail(t *testing.T, err error, want localpb.ErrorCode) *localpb.ErrorDetail {
	t.Helper()
	if err == nil {
		t.Fatal("error is nil, want an error with a detail")
	}
	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("error %v is not a gRPC status", err)
	}
	for _, d := range st.Details() {
		if detail, ok := d.(*localpb.ErrorDetail); ok {
			if detail.GetCode() != want {
				t.Fatalf("detail code = %v, want %v (message %q)", detail.GetCode(), want, detail.GetMessage())
			}
			return detail
		}
	}
	t.Fatalf("error %v carries no ErrorDetail", err)
	return nil
}

func TestHelloSameVersion(t *testing.T) {
	// REQ-broker-config-016: a matching version completes the handshake.
	tb := newTestBroker(t)

	resp, err := callTestHello(t, tb.client, localpb.ProtocolVersion)
	if err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if got, want := resp.GetProtocolVersion(), localpb.ProtocolVersion; got != want {
		t.Fatalf("ProtocolVersion = %d, want %d", got, want)
	}
}

func TestHelloVersionMismatchKeepsServing(t *testing.T) {
	// REQ-broker-config-016: a mismatch fails the call without stopping the
	// Broker or closing the connection.
	tb := newTestBroker(t)

	_, err := callTestHello(t, tb.client, localpb.ProtocolVersion+1)
	detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)
	if !strings.Contains(detail.GetMessage(), "restart the Broker") {
		t.Errorf("message %q does not tell the user to restart the Broker", detail.GetMessage())
	}
	if got, want := status.Code(err), codes.FailedPrecondition; got != want {
		t.Errorf("status code = %v, want %v", got, want)
	}

	resp, err := callTestHello(t, tb.newClient(t), localpb.ProtocolVersion)
	if err != nil {
		t.Fatalf("Hello on a second connection after a mismatch: %v", err)
	}
	if got, want := resp.GetProtocolVersion(), localpb.ProtocolVersion; got != want {
		t.Fatalf("ProtocolVersion = %d, want %d", got, want)
	}

	var found bool
	for _, rec := range tb.logLines(t) {
		if rec["event"] == EvHelloVersionMismatch {
			found = true
		}
	}
	if !found {
		t.Fatalf("no %s log record: %v", EvHelloVersionMismatch, tb.logLines(t))
	}
}

func TestHelloUsesInjectedVersion(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) { d.LocalProtocolVersion = 7 })

	resp, err := callTestHello(t, tb.client, 7)
	if err != nil {
		t.Fatalf("Hello{7}: %v", err)
	}
	if got := resp.GetProtocolVersion(); got != 7 {
		t.Fatalf("ProtocolVersion = %d, want 7", got)
	}

	_, err = callTestHello(t, tb.client, localpb.ProtocolVersion)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)
}

func TestNewRequiresTopicsAndConfig(t *testing.T) {
	cfg := newFakeConfig(t.TempDir())
	if _, err := New(Deps{Config: cfg}); err == nil {
		t.Fatal("New without Topics returned no error")
	}

	topics := newFakeTopics()
	if _, err := New(Deps{Topics: topics}); err == nil {
		t.Fatal("New without Config returned no error")
	}
}

func TestServerOptionsApplied(t *testing.T) {
	var calls atomic.Int32
	counting := func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		calls.Add(1)
		return handler(ctx, req)
	}
	tb := newTestBroker(t, func(d *Deps) {
		d.ServerOptions = append(d.ServerOptions, grpc.ChainUnaryInterceptor(counting))
	})

	if _, err := callTestHello(t, tb.client, localpb.ProtocolVersion); err != nil {
		t.Fatalf("Hello: %v", err)
	}
	if got := calls.Load(); got != 1 {
		t.Fatalf("interceptor ran %d times, want 1", got)
	}
}
