package harness

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/test/e2e/node"
)

type fakeLocalServer struct {
	localpb.UnimplementedLocalServer

	mu                    sync.Mutex
	getConfigReq          *localpb.GetConfigRequest
	auditReq              *localpb.AuditRequest
	getConfigErr          error
	events                []*localpb.AuditEvent
	listBrokersCalls      int
	blockListBrokersAfter int
}

// ListBrokers answers normally until blockListBrokersAfter calls, then hangs
// until ctx is done, modelling a Broker that stops answering partway through a
// consistency window.
func (f *fakeLocalServer) ListBrokers(ctx context.Context, _ *localpb.ListBrokersRequest) (*localpb.ListBrokersResponse, error) {
	f.mu.Lock()
	f.listBrokersCalls++
	n := f.listBrokersCalls
	blockAfter := f.blockListBrokersAfter
	f.mu.Unlock()
	if blockAfter > 0 && n > blockAfter {
		<-ctx.Done()
		return nil, status.FromContextError(ctx.Err()).Err()
	}
	return &localpb.ListBrokersResponse{}, nil
}

func (f *fakeLocalServer) GetConfig(_ context.Context, req *localpb.GetConfigRequest) (*localpb.GetConfigResponse, error) {
	f.mu.Lock()
	f.getConfigReq = req
	err := f.getConfigErr
	f.mu.Unlock()
	if err != nil {
		return nil, err
	}
	return &localpb.GetConfigResponse{Entries: []*localpb.ConfigEntry{{Key: "broker.alias"}}}, nil
}

func (f *fakeLocalServer) Audit(req *localpb.AuditRequest, stream localpb.Local_AuditServer) error {
	f.mu.Lock()
	f.auditReq = req
	events := f.events
	f.mu.Unlock()
	for _, event := range events {
		if err := stream.Send(event); err != nil {
			return err
		}
	}
	return nil
}

func (f *fakeLocalServer) requests() (*localpb.GetConfigRequest, *localpb.AuditRequest) {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.getConfigReq, f.auditReq
}

func newFakeOracleClient(t *testing.T, srv localpb.LocalServer) *OracleClient {
	t.Helper()
	socket := filepath.Join(newSocketTempDir(t, "broker.sock"), "broker.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listening on %s: %v", socket, err)
	}
	server := grpc.NewServer()
	localpb.RegisterLocalServer(server, srv)
	go func() { _ = server.Serve(listener) }()
	t.Cleanup(server.Stop)

	oracle, err := node.DialOracle(socket)
	if err != nil {
		t.Fatalf("dialing %s: %v", socket, err)
	}
	t.Cleanup(func() { _ = oracle.Close() })
	return &OracleClient{oracle: oracle}
}

type countingListener struct {
	net.Listener

	mu    sync.Mutex
	conns int
}

func (l *countingListener) Accept() (net.Conn, error) {
	conn, err := l.Listener.Accept()
	if err == nil {
		l.mu.Lock()
		l.conns++
		l.mu.Unlock()
	}
	return conn, err
}

func (l *countingListener) count() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.conns
}

// TestLocalOracleReusesOneConnection checks that a local node's Oracle reuses
// one connection across calls instead of opening a new one each time.
func TestLocalOracleReusesOneConnection(t *testing.T) {
	root := newSocketTempDir(t, "broker.sock")
	socket := filepath.Join(root, strconv.Itoa(os.Geteuid()), "broker.sock")
	if err := os.MkdirAll(filepath.Dir(socket), 0o700); err != nil {
		t.Fatalf("creating the socket directory: %v", err)
	}

	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listening on %s: %v", socket, err)
	}
	counted := &countingListener{Listener: listener}
	server := grpc.NewServer()
	localpb.RegisterLocalServer(server, &fakeLocalServer{})
	go func() { _ = server.Serve(counted) }()
	t.Cleanup(server.Stop)

	local := &localNode{t: t, name: "reuse", root: root}
	t.Cleanup(func() {
		local.mu.Lock()
		oracles := append([]*OracleClient(nil), local.oracles...)
		local.mu.Unlock()
		for _, o := range oracles {
			o.close()
		}
	})
	n := &Node{impl: local}

	for i := 0; i < 3; i++ {
		if _, detail := n.Oracle().GetConfig(t, ""); detail != nil {
			t.Fatalf("call %d: GetConfig returned error detail %v", i+1, detail)
		}
	}
	if got := counted.count(); got != 1 {
		t.Fatalf("three Oracle calls opened %d connections, want 1", got)
	}
}

// TestOracleGetConfigAndAudit checks that OracleClient forwards GetConfig and
// Audit arguments and returns product errors as an ErrorDetail.
func TestOracleGetConfigAndAudit(t *testing.T) {
	since := time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)

	t.Run("GetConfig", func(t *testing.T) {
		fake := &fakeLocalServer{}
		oracle := newFakeOracleClient(t, fake)

		resp, detail := oracle.GetConfig(t, "broker.alias")
		if detail != nil {
			t.Fatalf("GetConfig returned error detail %v", detail)
		}
		if len(resp.GetEntries()) != 1 || resp.GetEntries()[0].GetKey() != "broker.alias" {
			t.Fatalf("GetConfig returned %v, want one broker.alias entry", resp.GetEntries())
		}
		req, _ := fake.requests()
		if req.GetKey() != "broker.alias" {
			t.Errorf("the Broker received key %q, want %q", req.GetKey(), "broker.alias")
		}
	})

	t.Run("Audit carries since and limit", func(t *testing.T) {
		fake := &fakeLocalServer{events: []*localpb.AuditEvent{
			{Detail: "first"},
			{Detail: "second"},
		}}
		oracle := newFakeOracleClient(t, fake)

		events, detail := oracle.Audit(t, since, 7)
		if detail != nil {
			t.Fatalf("Audit returned error detail %v", detail)
		}
		if len(events) != 2 || events[0].GetDetail() != "first" || events[1].GetDetail() != "second" {
			t.Fatalf("Audit returned %v, want first then second", events)
		}
		_, req := fake.requests()
		if req.GetSince() == nil || !req.GetSince().AsTime().Equal(since) {
			t.Errorf("AuditRequest.since = %v, want %v", req.GetSince(), since)
		}
		if req.GetLimit() != 7 {
			t.Errorf("AuditRequest.limit = %d, want 7", req.GetLimit())
		}
	})

	t.Run("Audit leaves zero values unset", func(t *testing.T) {
		fake := &fakeLocalServer{}
		oracle := newFakeOracleClient(t, fake)

		if _, detail := oracle.Audit(t, time.Time{}, 0); detail != nil {
			t.Fatalf("Audit returned error detail %v", detail)
		}
		_, req := fake.requests()
		if req.GetSince() != nil {
			t.Errorf("AuditRequest.since = %v, want unset", req.GetSince())
		}
		if req.GetLimit() != 0 {
			t.Errorf("AuditRequest.limit = %d, want 0", req.GetLimit())
		}
	})

	t.Run("a product error is returned as an ErrorDetail", func(t *testing.T) {
		st := status.New(codes.NotFound, "not found")
		st, err := st.WithDetails(&localpb.ErrorDetail{
			Code:    localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
			Message: "no such key",
		})
		if err != nil {
			t.Fatalf("attaching an ErrorDetail: %v", err)
		}

		fake := &fakeLocalServer{getConfigErr: st.Err()}
		oracle := newFakeOracleClient(t, fake)

		resp, detail := oracle.GetConfig(t, "missing")
		if resp != nil {
			t.Errorf("GetConfig returned response %v with an error", resp)
		}
		if detail == nil {
			t.Fatal("GetConfig returned no ErrorDetail")
		}
		if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
			t.Errorf("ErrorDetail.Code = %v, want %v", detail.GetCode(), localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
		}
	})
}

// TestListBrokersWithinStallsAtTheWindowEnd checks that a consistency window
// whose Oracle call hangs late reports a stall instead of passing on an earlier
// conclusive sample.
func TestListBrokersWithinStallsAtTheWindowEnd(t *testing.T) {
	const (
		window    = 100 * time.Millisecond
		callBound = 400 * time.Millisecond
	)
	// Answer the first call so the window holds a conclusive sample before the
	// hang.
	fake := &fakeLocalServer{blockListBrokersAfter: 1}
	oracle := newFakeOracleClient(t, fake)
	tb := &recordTB{}

	start := time.Now()
	msg := captureFatal(t, tb, func() {
		ConsistentlyPoll(tb, window, callBound, "the Oracle window", nil, func(deadline time.Time) Observation {
			if _, _, err := oracle.ListBrokersWithin(tb, deadline, callBound); err != nil {
				return Unobservable(err.Error())
			}
			return Observed(true, "")
		})
	})
	elapsed := time.Since(start)

	if msg == "" {
		t.Fatal("the window passed without reporting the Oracle stall")
	}
	for _, want := range []string{"could not be observed", "1 sample stalled", "reached its 400ms poll bound"} {
		if !strings.Contains(msg, want) {
			t.Errorf("failure %q does not contain %q", msg, want)
		}
	}
	if elapsed < callBound {
		t.Fatalf("the window returned after %s, want at least the %s call bound: the call was cut short by the window",
			elapsed, callBound)
	}
	if logged := strings.Join(tb.logs, "\n"); !strings.Contains(logged, "=== harness STALL: the Oracle window") {
		t.Errorf("the STALL marker was not logged:\n%s", logged)
	}
}
