package broker

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"testing"

	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// rpcTestMsgID is the message id the RPC delivery fixture injects.
const rpcTestMsgID = "8d7c6b5a-4f3e-4d2c-9b1a-0f9e8d7c6b5a"

// newRPCDelivery wires a delivery to fakes for the Send RPC tests. The link is
// not exercised here; the remote path is driven through toPeer.
func newRPCDelivery(agents localAgents, topics visibility, peers peerTable, settings settingsSource) *delivery {
	return newDelivery(deliveryDeps{
		Agents:   agents,
		Topics:   topics,
		Peers:    peers,
		Link:     newWaitLink(),
		Settings: settings,
		Log:      slog.New(slog.NewTextHandler(io.Discard, nil)),
		NewID:    func() string { return rpcTestMsgID },
	})
}

// emptyDeliveryPeerTable is a peer table with no aliases and no roles.
func emptyDeliveryPeerTable() *deliveryPeers {
	return &deliveryPeers{
		aliases:   make(map[string]peerID),
		roles:     make(map[peerID]pairing.Role),
		effective: make(map[peerID]string),
	}
}

// emptyDeliveryTopics is a visibility fake with no topics and no peers.
func emptyDeliveryTopics() *deliveryTopics {
	return &deliveryTopics{
		local:  make(map[string][]string),
		remote: make(map[peerID][]deliveryRemoteEntry),
	}
}

func TestSendRPCErrorDetails(t *testing.T) {
	// REQ-agent-messaging-004: an unknown alias is reported as not_found through
	// exactly one ErrorDetail, so the MCP server can read the code.
	tb := newTestBroker(t, func(d *Deps) {
		d.Delivery = newRPCDelivery(newFakeAgents(), emptyDeliveryTopics(), emptyDeliveryPeerTable(),
			newFakeSettings(defaultSendSettings(), "hostA"))
	})
	caller := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	id := registerListAgent(t, tb, caller)

	_, err := tb.client.Send(context.Background(), &localpb.SendRequest{
		AgentId:   string(id),
		ToAddress: "nobody@unknown",
		Message:   "hello",
	})
	detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
	if got := len(status.Convert(err).Details()); got != 1 {
		t.Fatalf("status carries %d details, want exactly 1", got)
	}
	if len(detail.GetOriginatingBrokerFingerprint()) != 0 {
		t.Fatalf("originating fingerprint = %x, want empty for a local error", detail.GetOriginatingBrokerFingerprint())
	}
}

func TestSendRPCUnregistered(t *testing.T) {
	// REQ-agent-messaging-001: only a registered Agent may send.
	tb := newTestBroker(t)

	_, err := tb.client.Send(context.Background(), &localpb.SendRequest{
		AgentId:   "not-a-registered-agent",
		ToAddress: "someone@hostA",
		Message:   "hello",
	})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED)
}

func TestSendRPCWithoutDelivery(t *testing.T) {
	// A Broker without the delivery assembled reports internal instead of
	// panicking on a nil pointer.
	tb := newTestBroker(t)
	caller := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	id := registerListAgent(t, tb, caller)

	_, err := tb.client.Send(context.Background(), &localpb.SendRequest{
		AgentId:   string(id),
		ToAddress: "someone@hostA",
		Message:   "hello",
	})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INTERNAL)
}

func TestSendRPCLocalEndToEnd(t *testing.T) {
	// REQ-agent-messaging-013, REQ-agent-messaging-028: the RPC reaches the
	// target session through the real agent table and topic adapters.
	var adapter *agentTableAdapter
	tb := newTestBroker(t, func(d *Deps) {
		adapter = &agentTableAdapter{}
		d.Delivery = newRPCDelivery(adapter, topicVisibility{d.Topics}, emptyDeliveryPeerTable(),
			newFakeSettings(defaultSendSettings(), "hostA"))
	})
	adapter.setBroker(tb.b)

	callerFake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	targetFake := fakesession.New(t, fakesession.Options{Name: "frontend"})
	caller := registerListAgent(t, tb, callerFake)
	target := registerListAgent(t, tb, targetFake)
	joinListTopics(t, tb, caller, "t1")
	joinListTopics(t, tb, target, "t1")

	targetView, ok := tb.b.lookupAgent(target)
	if !ok {
		t.Fatal("target agent disappeared from the table")
	}

	resp, err := tb.client.Send(context.Background(), &localpb.SendRequest{
		AgentId:   string(caller),
		ToAddress: targetView.Address,
		Message:   "hello from the rpc",
	})
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if resp == nil {
		t.Fatal("Send returned a nil response")
	}

	frame, ok := targetFake.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("target session received no frame")
	}
	if frame.MsgID != rpcTestMsgID {
		t.Fatalf("frame msg_id = %q, want %q", frame.MsgID, rpcTestMsgID)
	}
	if frame.FromName != "api-worker@hostA" {
		t.Fatalf("frame from-name = %q, want %q", frame.FromName, "api-worker@hostA")
	}
	if !bytes.Contains([]byte(frame.Content), []byte("hello from the rpc")) {
		t.Fatalf("frame content %q does not carry the message", frame.Content)
	}
}

func TestSendRPCRemotePeerError(t *testing.T) {
	// REQ-agent-messaging-018: the peer's origin fingerprint is carried back to
	// the caller unchanged.
	peer := fp(9)
	var d *delivery
	tb := newTestBroker(t, func(deps *Deps) {
		peers := emptyDeliveryPeerTable()
		peers.aliases["hostB"] = peer
		peers.roles[peer] = pairing.RoleDialer
		peers.effective[peer] = "hostB"
		d = newRPCDelivery(newFakeAgents(), emptyDeliveryTopics(), peers,
			newFakeSettings(defaultSendSettings(), "hostA"))
		deps.Delivery = d
	})
	caller := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	id := registerListAgent(t, tb, caller)

	d.toPeer = func(context.Context, peerSend) error {
		return &Error{
			Code:    localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE,
			Message: "Not sent: the target session did not accept the message",
			Origin:  append([]byte(nil), peer[:]...),
		}
	}

	_, err := tb.client.Send(context.Background(), &localpb.SendRequest{
		AgentId:   string(id),
		ToAddress: "frontend@hostB",
		Message:   "hello",
	})
	detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE)
	if !bytes.Equal(detail.GetOriginatingBrokerFingerprint(), peer[:]) {
		t.Fatalf("originating fingerprint = %x, want %x", detail.GetOriginatingBrokerFingerprint(), peer[:])
	}
}
