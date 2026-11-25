package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/socktest"
)

type fakeStatusServer struct {
	localpb.UnimplementedLocalServer

	mu           sync.Mutex
	helloVersion uint32
	statusCalled bool
	agents       []*localpb.AgentStatusEntry
	peers        []*localpb.BrokerInfo
}

func (f *fakeStatusServer) Hello(_ context.Context, req *localpb.HelloRequest) (*localpb.HelloResponse, error) {
	f.mu.Lock()
	f.helloVersion = req.GetProtocolVersion()
	f.mu.Unlock()
	return &localpb.HelloResponse{ProtocolVersion: localpb.ProtocolVersion}, nil
}

func (f *fakeStatusServer) Status(context.Context, *localpb.StatusRequest) (*localpb.StatusResponse, error) {
	f.mu.Lock()
	f.statusCalled = true
	f.mu.Unlock()
	return &localpb.StatusResponse{Agents: f.agents, Peers: f.peers}, nil
}

func startFakeStatusServer(t *testing.T, fake *fakeStatusServer) string {
	t.Helper()
	socket := socktest.Path(t, "broker.sock")
	lis, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	srv := grpc.NewServer()
	localpb.RegisterLocalServer(srv, fake)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	return socket
}

// TestStatusAgainstFakeServer checks the status output, that Hello sends the
// protocol version and that Status is called.
func TestStatusAgainstFakeServer(t *testing.T) {
	id1 := testAgentID("abcdef0123")
	id2 := testAgentID("abcdef0199")
	fake := &fakeStatusServer{
		agents: []*localpb.AgentStatusEntry{
			{
				AgentId: id1,
				Address: "a@hostB",
				Topics:  []string{"rs-probe"},
				Status:  localpb.AgentStatus_AGENT_STATUS_IDLE,
			},
			{
				AgentId:      id2,
				Address:      "b@hostB",
				Topics:       []string{"rs-probe"},
				Status:       localpb.AgentStatus_AGENT_STATUS_BUSY,
				PreviousName: "b-old",
			},
		},
		peers: []*localpb.BrokerInfo{
			{Alias: "hostA", Status: localpb.BrokerStatus_BROKER_STATUS_CONNECTED},
		},
	}
	socket := startFakeStatusServer(t, fake)

	var out bytes.Buffer
	if err := runStatus([]string{"--socket", socket}, &out); err != nil {
		t.Fatalf("runStatus: %v", err)
	}

	lines := strings.Split(strings.TrimSuffix(out.String(), "\n"), "\n")
	want := []string{
		"abcdef012 a@hostB status=idle topics=rs-probe prev=",
		"abcdef019 b@hostB status=busy topics=rs-probe prev=b-old",
		"peer hostA connected",
	}
	if len(lines) != len(want) {
		t.Fatalf("output lines = %q, want %q", lines, want)
	}
	for i := range want {
		if lines[i] != want[i] {
			t.Errorf("line %d = %q, want %q", i, lines[i], want[i])
		}
	}

	fake.mu.Lock()
	helloVersion := fake.helloVersion
	statusCalled := fake.statusCalled
	fake.mu.Unlock()
	if helloVersion != localpb.ProtocolVersion {
		t.Errorf("Hello protocol_version = %d, want %d", helloVersion, localpb.ProtocolVersion)
	}
	if !statusCalled {
		t.Error("Status was not called")
	}
}
