package mcpserver

import (
	"context"
	"net"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"

	"github.com/wangzw/peers/internal/broker/localpb"
)

// updateStatusServer answers UpdateStatus with a canned response or error.
type updateStatusServer struct {
	localpb.UnimplementedLocalServer
	respond func(ctx context.Context) (*localpb.UpdateStatusResponse, error)
}

func (s *updateStatusServer) UpdateStatus(ctx context.Context, _ *localpb.UpdateStatusRequest) (*localpb.UpdateStatusResponse, error) {
	return s.respond(ctx)
}

// startUpdateStatusServer serves the Local service over a buffer connection.
// A nil implementation registers no service at all, which makes every call
// return Unimplemented.
func startUpdateStatusServer(t *testing.T, impl localpb.LocalServer) *bufconn.Listener {
	t.Helper()
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	if impl != nil {
		localpb.RegisterLocalServer(srv, impl)
	}
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return lis
}

func dialUpdateStatusServer(t *testing.T, lis *bufconn.Listener) *grpc.ClientConn {
	t.Helper()
	cc, err := grpc.NewClient("passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) {
			return lis.DialContext(ctx)
		}),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return cc
}

func TestQueryUpdateStatus(t *testing.T) {
	lis := startUpdateStatusServer(t, &updateStatusServer{respond: func(context.Context) (*localpb.UpdateStatusResponse, error) {
		return &localpb.UpdateStatusResponse{
			CurrentVersion:   "0.1.0",
			AvailableVersion: "0.1.1",
			ReleaseUrl:       "https://example.test/release",
		}, nil
	}})
	cc := dialUpdateStatusServer(t, lis)

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	got, err := queryUpdateStatus(ctx, cc)
	if err != nil {
		t.Fatalf("queryUpdateStatus: %v", err)
	}
	want := updateStatus{
		CurrentVersion:   "0.1.0",
		AvailableVersion: "0.1.1",
		ReleaseURL:       "https://example.test/release",
	}
	if got != want {
		t.Errorf("updateStatus = %+v, want %+v", got, want)
	}
}

func TestQueryUpdateStatusNoAvailableVersion(t *testing.T) {
	lis := startUpdateStatusServer(t, &updateStatusServer{respond: func(context.Context) (*localpb.UpdateStatusResponse, error) {
		return &localpb.UpdateStatusResponse{CurrentVersion: "0.1.0"}, nil
	}})
	cc := dialUpdateStatusServer(t, lis)

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	got, err := queryUpdateStatus(ctx, cc)
	if err != nil {
		t.Fatalf("queryUpdateStatus: %v", err)
	}
	if got.CurrentVersion != "0.1.0" || got.AvailableVersion != "" {
		t.Errorf("updateStatus = %+v, want current 0.1.0 and no available version", got)
	}
}

func TestQueryUpdateStatusUnimplemented(t *testing.T) {
	// An older Broker has no UpdateStatus method.
	lis := startUpdateStatusServer(t, nil)
	cc := dialUpdateStatusServer(t, lis)

	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	_, err := queryUpdateStatus(ctx, cc)
	if status.Code(err) != codes.Unimplemented {
		t.Fatalf("error = %v, want Unimplemented", err)
	}
}

func TestQueryUpdateStatusDeadline(t *testing.T) {
	lis := startUpdateStatusServer(t, &updateStatusServer{respond: func(ctx context.Context) (*localpb.UpdateStatusResponse, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}})
	cc := dialUpdateStatusServer(t, lis)

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err := queryUpdateStatus(ctx, cc)
	if status.Code(err) != codes.DeadlineExceeded {
		t.Fatalf("error = %v, want DeadlineExceeded", err)
	}
}

func TestBrokerUpdateStatusWithoutConnection(t *testing.T) {
	source := brokerUpdateStatus(&statusStore{})
	_, err := source(context.Background())
	if err == nil {
		t.Fatal("query without a connection returned no error")
	}
}
