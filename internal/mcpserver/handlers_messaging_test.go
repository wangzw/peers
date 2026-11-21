package mcpserver

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/protobuf/encoding/protojson"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// messagingTestClient dials the fake Local service listening on path.
func messagingTestClient(t *testing.T, path string) localpb.LocalClient {
	t.Helper()
	cc, err := grpc.NewClient("unix://"+path, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("dialing the fake Local service: %v", err)
	}
	t.Cleanup(func() { _ = cc.Close() })
	return localpb.NewLocalClient(cc)
}

// messagingTestHandlers returns handlers in the connected state, registered as
// agentID.
func messagingTestHandlers(client localpb.LocalClient, agentID string) *handlers {
	st := &statusStore{}
	st.Update(func(s *Status) {
		s.Phase = PhaseConnected
		s.Client = client
		s.AgentID = agentID
		s.Address = "me@host"
	})
	return &handlers{st: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}
}

// messagingCallsOf returns the requests of one method the fake service recorded.
func messagingCallsOf(srv *fakelocal.Server, method string) []any {
	var out []any
	for _, c := range srv.Calls() {
		if c.Method == method {
			out = append(out, c.Request)
		}
	}
	return out
}

func TestListAgentsForwardsFilters(t *testing.T) {
	// REQ-agent-messaging-008: the filters, including the caller's identity,
	// reach the Broker; filtering is the Broker's job.
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{SelfAddress: "me@host"}, nil
		},
	})
	h := messagingTestHandlers(messagingTestClient(t, path), "agent-7")

	if _, err := h.ListAgents(context.Background(), ListAgentsIn{Topic: "topic-a", Broker: "host-b", Name: "worker"}); err != nil {
		t.Fatalf("ListAgents: %v", err)
	}

	reqs := messagingCallsOf(srv, "ListAgents")
	if len(reqs) != 1 {
		t.Fatalf("ListAgents requests = %d, want 1", len(reqs))
	}
	req, ok := reqs[0].(*localpb.ListAgentsRequest)
	if !ok {
		t.Fatalf("request is %T", reqs[0])
	}
	if got := req.GetAgentId(); got != "agent-7" {
		t.Errorf("agent_id = %q, want agent-7", got)
	}
	if got := req.GetTopic(); got != "topic-a" {
		t.Errorf("topic = %q, want topic-a", got)
	}
	if got := req.GetBroker(); got != "host-b" {
		t.Errorf("broker = %q, want host-b", got)
	}
	if got := req.GetName(); got != "worker" {
		t.Errorf("name = %q, want worker", got)
	}
}

func TestListAgentsOutput(t *testing.T) {
	// REQ-agent-messaging-008, REQ-agent-messaging-009, REQ-agent-messaging-010,
	// REQ-agent-messaging-011, REQ-agent-messaging-032: status words, the
	// omitted unknown status, the empty topic list, and self_address.
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{
				SelfAddress: "me@host",
				Agents: []*localpb.AgentEntry{
					{
						Address:      "other@host",
						SharedTopics: []string{"topic-a"},
						Status:       localpb.AgentStatus_AGENT_STATUS_WAITING,
						PreviousName: "old",
					},
					{
						Address:      "quiet@host",
						SharedTopics: nil,
						Status:       localpb.AgentStatus_AGENT_STATUS_UNSPECIFIED,
					},
				},
			}, nil
		},
	})
	h := messagingTestHandlers(messagingTestClient(t, path), "agent-7")

	out, err := h.ListAgents(context.Background(), ListAgentsIn{})
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshaling the result: %v", err)
	}
	var decoded struct {
		Self   string           `json:"self_address"`
		Agents []map[string]any `json:"agents"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decoding the result: %v", err)
	}
	if decoded.Self != "me@host" {
		t.Errorf("self_address = %q, want me@host", decoded.Self)
	}
	if len(decoded.Agents) != 2 {
		t.Fatalf("agents = %d, want 2", len(decoded.Agents))
	}

	first := decoded.Agents[0]
	if first["status"] != "waiting" {
		t.Errorf("first status = %v, want waiting", first["status"])
	}
	if first["previous_name"] != "old" {
		t.Errorf("first previous_name = %v, want old", first["previous_name"])
	}
	if first["address"] != "other@host" {
		t.Errorf("first address = %v", first["address"])
	}

	second := decoded.Agents[1]
	if _, ok := second["status"]; ok {
		t.Errorf("unknown status appears in JSON: %v", second)
	}
	topics, ok := second["shared_topics"].([]any)
	if !ok {
		t.Fatalf("shared_topics = %v, want an array", second["shared_topics"])
	}
	if len(topics) != 0 {
		t.Errorf("shared_topics = %v, want empty", topics)
	}
}

func TestListAgentsOutputAccount(t *testing.T) {
	// REQ-agent-messaging-008: the result carries the account that registered
	// each agent, so the model can tell apart agents with the same name. The
	// entry is decoded from JSON with unknown fields discarded, so the test
	// builds and fails on the missing output field against code that predates
	// it instead of failing to compile.
	var entry localpb.AgentEntry
	build := protojson.UnmarshalOptions{DiscardUnknown: true}
	if err := build.Unmarshal([]byte(`{"address":"other@host","sharedTopics":["topic-a"],"account":"alice"}`), &entry); err != nil {
		t.Fatalf("building the roster entry: %v", err)
	}

	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		ListAgents: func(context.Context, *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
			return &localpb.ListAgentsResponse{
				SelfAddress: "me@host",
				Agents:      []*localpb.AgentEntry{&entry},
			}, nil
		},
	})
	h := messagingTestHandlers(messagingTestClient(t, path), "agent-7")

	out, err := h.ListAgents(context.Background(), ListAgentsIn{})
	if err != nil {
		t.Fatalf("ListAgents: %v", err)
	}
	raw, err := json.Marshal(out)
	if err != nil {
		t.Fatalf("marshaling the result: %v", err)
	}
	var decoded struct {
		Agents []map[string]any `json:"agents"`
	}
	if err := json.Unmarshal(raw, &decoded); err != nil {
		t.Fatalf("decoding the result: %v", err)
	}
	if len(decoded.Agents) != 1 {
		t.Fatalf("agents = %v, want one entry", decoded.Agents)
	}
	if got := decoded.Agents[0]["account"]; got != "alice" {
		t.Errorf("account = %v, want alice (result %s)", got, raw)
	}
}

func TestListAgentsNotRegistered(t *testing.T) {
	// REQ-agent-messaging-038: an unregistered session gets an explanation and
	// no request reaches the Broker.
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{})
	st := &statusStore{}
	st.Update(func(s *Status) {
		s.Phase = PhaseConnected
		s.Client = messagingTestClient(t, path)
		s.NotRegistered = localpb.NotRegisteredReason_NOT_REGISTERED_REASON_SESSION_RECORD_NOT_FOUND
	})
	h := &handlers{st: st, log: slog.New(slog.NewTextHandler(io.Discard, nil))}

	_, err := h.ListAgents(context.Background(), ListAgentsIn{})
	if err == nil {
		t.Fatal("ListAgents succeeded without a registered agent")
	}
	if text := err.Error(); !strings.Contains(text, "not_registered") {
		t.Errorf("error = %q, want code not_registered", text)
	}
	if text := err.Error(); !strings.Contains(text, "not registered as an agent") {
		t.Errorf("error = %q, want the not-registered explanation", text)
	}
	if reqs := messagingCallsOf(srv, "ListAgents"); len(reqs) != 0 {
		t.Errorf("ListAgents reached the Broker while unregistered: %v", reqs)
	}
}

func TestSendMessageForwards(t *testing.T) {
	// REQ-agent-messaging-013: to, message and the caller's identity reach the
	// Broker unchanged.
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Send: func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error) {
			return &localpb.SendResponse{}, nil
		},
	})
	h := messagingTestHandlers(messagingTestClient(t, path), "agent-7")

	const message = "  hello, world\n"
	out, err := h.SendMessage(context.Background(), SendMessageIn{To: "other@host", Message: message})
	if err != nil {
		t.Fatalf("SendMessage: %v", err)
	}
	if want := "Delivered to the recipient's session (not necessarily read)."; out.Result != want {
		t.Errorf("result = %q, want %q", out.Result, want)
	}
	if out.To != "other@host" {
		t.Errorf("to = %q, want other@host", out.To)
	}

	reqs := messagingCallsOf(srv, "Send")
	if len(reqs) != 1 {
		t.Fatalf("Send requests = %d, want 1", len(reqs))
	}
	req, ok := reqs[0].(*localpb.SendRequest)
	if !ok {
		t.Fatalf("request is %T", reqs[0])
	}
	if got := req.GetAgentId(); got != "agent-7" {
		t.Errorf("agent_id = %q, want agent-7", got)
	}
	if got := req.GetToAddress(); got != "other@host" {
		t.Errorf("to_address = %q, want other@host", got)
	}
	if got := req.GetMessage(); got != message {
		t.Errorf("message = %q, want %q", got, message)
	}
}

func TestSendMessageErrors(t *testing.T) {
	// REQ-agent-messaging-027: DELIVERY_UNKNOWN is "Delivery unknown", an
	// ordinary failure is "Not sent", and a connection lost mid-RPC is
	// "Delivery unknown".
	tests := []struct {
		name   string
		stop   bool // stop the fake service inside the Send handler
		send   func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error)
		prefix string
	}{
		{
			name: "delivery unknown",
			send: func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error) {
				return nil, fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN,
					Message: "no answer",
				})
			},
			prefix: "Delivery unknown:",
		},
		{
			name: "not visible",
			send: func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error) {
				return nil, fakelocal.ErrorStatus(codes.NotFound, &localpb.ErrorDetail{
					Code:    localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE,
					Message: "no such agent",
				})
			},
			prefix: "Not sent:",
		},
		{
			name:   "connection lost",
			stop:   true,
			prefix: "Delivery unknown:",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			path := fakelocal.SocketPath(t)
			var srv *fakelocal.Server
			send := tc.send
			if tc.stop {
				send = func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error) {
					srv.Stop()
					return nil, nil
				}
			}
			srv = fakelocal.Start(t, path, fakelocal.Handlers{Send: send})
			h := messagingTestHandlers(messagingTestClient(t, path), "agent-7")
			_, err := h.SendMessage(context.Background(), SendMessageIn{To: "other@host", Message: "hi"})
			if err == nil {
				t.Fatal("SendMessage succeeded")
			}
			if !strings.HasPrefix(err.Error(), tc.prefix) {
				t.Errorf("error = %q, want prefix %q", err, tc.prefix)
			}
		})
	}
}

func TestSendMessageConcurrentAgentID(t *testing.T) {
	// REQ-agent-messaging-035: concurrent calls from one session carry the same
	// agent identity.
	path := fakelocal.SocketPath(t)
	srv := fakelocal.Start(t, path, fakelocal.Handlers{
		Send: func(context.Context, *localpb.SendRequest) (*localpb.SendResponse, error) {
			return &localpb.SendResponse{}, nil
		},
	})
	h := messagingTestHandlers(messagingTestClient(t, path), "agent-7")

	var wg sync.WaitGroup
	errs := make([]error, 2)
	for i := range errs {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			_, errs[i] = h.SendMessage(context.Background(), SendMessageIn{To: "other@host", Message: "hi"})
		}(i)
	}
	wg.Wait()
	for i, err := range errs {
		if err != nil {
			t.Fatalf("SendMessage %d: %v", i, err)
		}
	}

	reqs := messagingCallsOf(srv, "Send")
	if len(reqs) != 2 {
		t.Fatalf("Send requests = %d, want 2", len(reqs))
	}
	for _, raw := range reqs {
		req, ok := raw.(*localpb.SendRequest)
		if !ok {
			t.Fatalf("request is %T", raw)
		}
		if got := req.GetAgentId(); got != "agent-7" {
			t.Errorf("agent_id = %q, want agent-7", got)
		}
	}
}
