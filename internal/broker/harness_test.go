package broker

import (
	"log/slog"
	"net"
	"path/filepath"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

type testBroker struct {
	b        *Broker
	sock     string
	conn     *grpc.ClientConn
	client   localpb.LocalClient
	clock    *fakeClock
	cfg      *fakeConfig
	sessions *fakeSessions
	topics   *fakeTopics
	dir      *fakeDirectory
	peers    *fakePeers
	logs     *syncBuffer
}

func newTestBroker(t *testing.T, mutate ...func(*Deps)) *testBroker {
	t.Helper()

	dir := socktest.Dir(t)

	clock := newFakeClock(time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
	cfg := newFakeConfig(dir)
	sessions := &fakeSessions{}
	topics := newFakeTopics()
	directory := newFakeDirectory()
	peers := newFakePeers()
	logs := &syncBuffer{}
	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)

	deps := Deps{
		Config:    cfg,
		Sessions:  sessions,
		Topics:    topics,
		Directory: directory,
		Peers:     peers,
		Clock:     clock,
		Logger:    NewLogger(logs, level),
		InSession: func() bool { return false },
	}
	for _, m := range mutate {
		m(&deps)
	}

	b, err := New(deps)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	sock := filepath.Join(dir, "b.sock")
	ln, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen on %s: %v", sock, err)
	}
	go func() { _ = b.Serve(ln) }()

	cc, err := grpc.NewClient("unix://"+sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	// Cleanups run last-in-first-out: the connection closes before the server
	// stops, and the temporary directory goes away last.
	t.Cleanup(b.Stop)
	t.Cleanup(func() { _ = cc.Close() })

	return &testBroker{
		b:        b,
		sock:     sock,
		conn:     cc,
		client:   localpb.NewLocalClient(cc),
		clock:    clock,
		cfg:      cfg,
		sessions: sessions,
		topics:   topics,
		dir:      directory,
		peers:    peers,
		logs:     logs,
	}
}

func (tb *testBroker) newClient(t *testing.T) localpb.LocalClient {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+tb.sock, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return localpb.NewLocalClient(cc)
}

func (tb *testBroker) logLines(t *testing.T) []map[string]any {
	t.Helper()
	return decodeLogLines(t, tb.logs.String())
}
