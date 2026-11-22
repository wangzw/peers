package broker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/peer"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// auditCallerContext builds the context a local connection carries, without a
// real gRPC server.
func auditCallerContext(uid uint32, username string) context.Context {
	p := &peer.Peer{AuthInfo: &callerAuthInfo{caller: Caller{UID: uid, Username: username}}}
	return peer.NewContext(context.Background(), p)
}

// auditRegister registers a fake session and returns its agent ID and address.
// The registration stream stays open until the test ends, because closing it
// unregisters the agent.
func auditRegister(t *testing.T, tb *testBroker, name string) (id, address string) {
	t.Helper()

	fake := fakesession.New(t, fakesession.Options{Name: name})
	stream, err := tb.client.Register(context.Background())
	if err != nil {
		t.Fatalf("Register: %v", err)
	}
	t.Cleanup(func() { _ = stream.CloseSend() })

	if err := stream.Send(&localpb.RegisterRequest{
		InboxSocketPath: fake.SocketPath(),
		ClaudeConfigDir: fake.ConfigDir(),
	}); err != nil {
		t.Fatalf("send registration: %v", err)
	}

	type recvResult struct {
		resp *localpb.RegisterResponse
		err  error
	}
	done := make(chan recvResult, 1)
	go func() {
		resp, err := stream.Recv()
		done <- recvResult{resp, err}
	}()
	select {
	case r := <-done:
		if r.err != nil {
			t.Fatalf("recv registration: %v", r.err)
		}
		complete := r.resp.GetRegistrationComplete()
		if complete == nil {
			t.Fatalf("registration did not complete: %v", r.resp)
		}
		return complete.GetAgentId(), complete.GetAgentAddress()
	case <-time.After(eventTimeout):
		t.Fatal("timed out waiting for registration_complete")
		return "", ""
	}
}

func TestAuditActorEmptyAgent(t *testing.T) {
	tb := newTestBroker(t)
	// A fixed non-root uid keeps the assertion meaningful when the test process
	// runs as root: the actor must carry the caller's identity, not the
	// process's uid.
	const callerUID = 1234
	ctx := auditCallerContext(callerUID, "alice")

	actor, err := tb.b.auditActor(ctx, "")
	if err != nil {
		t.Fatalf("auditActor: %v", err)
	}
	want := config.Actor{UIDKnown: true, UID: callerUID, Account: "alice"}
	if actor != want {
		t.Fatalf("auditActor = %+v, want %+v", actor, want)
	}
}

func TestAuditActorRegisteredAgent(t *testing.T) {
	tb := newTestBroker(t)
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")
	id, address := auditRegister(t, tb, "api-worker")
	if address != "api-worker@hostA" {
		t.Fatalf("registration address = %q, want %q", address, "api-worker@hostA")
	}

	actor, err := tb.b.auditActor(ctx, id)
	if err != nil {
		t.Fatalf("auditActor: %v", err)
	}
	if !actor.UIDKnown || actor.UID != uint32(os.Geteuid()) {
		t.Fatalf("AuditActor identity = (%v, %d), want the caller uid %d", actor.UIDKnown, actor.UID, os.Geteuid())
	}
	if actor.Account != "alice" {
		t.Fatalf("AuditActor.Account = %q, want %q", actor.Account, "alice")
	}
	if actor.Agent != address {
		t.Fatalf("AuditActor.Agent = %q, want the registered address %q", actor.Agent, address)
	}
}

func TestAuditActorUnknownAgent(t *testing.T) {
	tb := newTestBroker(t)
	ctx := auditCallerContext(uint32(os.Geteuid()), "alice")

	_, err := tb.b.auditActor(ctx, "nope")
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("auditActor error = %v, want *Error", err)
	}
	if e.Code != localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED {
		t.Fatalf("error code = %v, want NOT_REGISTERED", e.Code)
	}
}

func TestConnectEventSucceeded(t *testing.T) {
	actor := config.Actor{Account: "alice", Agent: "api-worker@hostA"}
	peer := pairing.Peer{Alias: "hostB", LocalAlias: "b2"}

	e, ok := connectEvent(actor, "198.51.100.4:17443", peer, nil)
	if !ok {
		t.Fatal("connectEvent(success) = not auditable, want auditable")
	}
	if e.Event != config.EventPairingSucceeded {
		t.Fatalf("Event = %v, want pairing_succeeded", e.Event)
	}
	if e.Actor != actor.Agent || e.Account != actor.Account {
		t.Fatalf("actor = (%q, %q), want (%q, %q)", e.Actor, e.Account, actor.Agent, actor.Account)
	}
	if e.Peer == nil {
		t.Fatal("Peer = nil, want the peer reference")
	}
	if e.Peer.Alias != "b2" {
		t.Fatalf("Peer.Alias = %q, want the effective alias %q", e.Peer.Alias, "b2")
	}
	if e.Detail != "" {
		t.Fatalf("Detail = %q, want empty", e.Detail)
	}
}

func TestConnectEventFailures(t *testing.T) {
	const address = "198.51.100.4:17443"
	actor := config.Actor{Account: "alice", Agent: "api-worker@hostA"}

	tests := []struct {
		name       string
		err        error
		wantDetail string
	}{
		{
			name:       "key invalid",
			err:        &pairing.PairingError{Reason: pairing.ReasonKeyInvalid, Detail: "the key does not match"},
			wantDetail: "key invalid; address " + address,
		},
		{
			name:       "verification failed",
			err:        &pairing.PairingError{Reason: pairing.ReasonVerificationFailed, Detail: "the identity proof did not verify"},
			wantDetail: "verification failed; address " + address,
		},
		{
			name:       "try later",
			err:        &pairing.PairingError{Reason: pairing.ReasonTryLater, Detail: "the other side is busy"},
			wantDetail: "try later; address " + address,
		},
		{
			name:       "handshake timeout",
			err:        &pairing.PairingError{Reason: pairing.ReasonTimeout, Detail: "the other side did not answer"},
			wantDetail: "handshake timeout; address " + address,
		},
		{
			name:       "not connected",
			err:        &pairing.PairingError{Reason: pairing.ReasonNotConnected, Detail: "the other side closed the connection"},
			wantDetail: "not connected; address " + address,
		},
		{
			name:       "key expired",
			err:        &pairing.PairingError{Reason: pairing.ReasonKeyExpired, Detail: "key expired; ask the other side to generate a new pairing key"},
			wantDetail: "key expired; address " + address,
		},
		{
			name:       "unknown reason",
			err:        &pairing.PairingError{Reason: pairing.RejectReason(250), Detail: "something else"},
			wantDetail: "rejected; address " + address,
		},
		{
			name:       "rejected without a reason",
			err:        fmt.Errorf("handshake: %w", pairing.ErrPairingRejected),
			wantDetail: "rejected; address " + address,
		},
		{
			name:       "peer unreachable",
			err:        fmt.Errorf("dial: %w", pairing.ErrPeerUnreachable),
			wantDetail: "peer unreachable; address " + address,
		},
		{
			name:       "protocol error",
			err:        pairing.ErrProtocol,
			wantDetail: "protocol error; address " + address,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			e, ok := connectEvent(actor, address, pairing.Peer{}, tt.err)
			if !ok {
				t.Fatalf("connectEvent(%v) = not auditable, want auditable", tt.err)
			}
			if e.Event != config.EventPairingFailed {
				t.Fatalf("Event = %v, want pairing_failed", e.Event)
			}
			if e.Peer != nil {
				t.Fatalf("Peer = %+v, want nil on failure", e.Peer)
			}
			if e.Actor != actor.Agent || e.Account != actor.Account {
				t.Fatalf("actor = (%q, %q), want (%q, %q)", e.Actor, e.Account, actor.Agent, actor.Account)
			}
			if e.Detail != tt.wantDetail {
				t.Fatalf("Detail = %q, want %q", e.Detail, tt.wantDetail)
			}
		})
	}
}

func TestConnectEventArgumentErrorsNotAudited(t *testing.T) {
	actor := config.Actor{Account: "alice"}
	tests := []error{
		pairing.ErrInvalidAddress,
		pairing.ErrInvalidAlias,
		pairing.ErrNeedsLocalAlias,
		pairing.ErrStore,
		context.Canceled,
	}
	for _, err := range tests {
		if _, ok := connectEvent(actor, "198.51.100.4:17443", pairing.Peer{}, err); ok {
			t.Errorf("connectEvent(%v) = auditable, want not auditable", err)
		}
	}
}

func TestAuditEventsNeverCarryKey(t *testing.T) {
	const key = "K7QF-2M9X-HT4C-PWZ3"
	actor := config.Actor{Account: "alice", Agent: "api-worker@hostA"}
	const address = "1.2.3.4:5"

	success, ok := connectEvent(actor, address, pairing.Peer{Alias: "hostB", LocalAlias: "b2"}, nil)
	if !ok {
		t.Fatal("connectEvent(success) = not auditable, want auditable")
	}
	failure, ok := connectEvent(actor, address, pairing.Peer{},
		&pairing.PairingError{Reason: pairing.ReasonKeyInvalid, Detail: "key " + key + " is wrong"})
	if !ok {
		t.Fatal("connectEvent(failure) = not auditable, want auditable")
	}

	for _, e := range []config.Event{success, failure} {
		fields := []string{e.Detail, e.Actor, e.Account}
		if e.Peer != nil {
			fields = append(fields, e.Peer.Alias)
		}
		for _, f := range fields {
			if strings.Contains(f, key) {
				t.Fatalf("audit event %v carries the pairing key in %q", e.Event, f)
			}
		}
	}
}

func TestPairingKeyCreatedEvent(t *testing.T) {
	actor := config.Actor{Account: "alice", Agent: "api-worker@hostA"}
	expires := time.Date(2026, 11, 18, 10, 0, 0, 0, time.FixedZone("CST", 8*60*60))

	e := pairingKeyCreatedEvent(actor, expires)
	if e.Event != config.EventPairingKeyCreated {
		t.Fatalf("Event = %v, want pairing_key_created", e.Event)
	}
	if e.Actor != actor.Agent || e.Account != actor.Account {
		t.Fatalf("actor = (%q, %q), want (%q, %q)", e.Actor, e.Account, actor.Agent, actor.Account)
	}
	if e.Detail != "expires 2026-11-18T02:00:00Z" {
		t.Fatalf("Detail = %q, want %q", e.Detail, "expires 2026-11-18T02:00:00Z")
	}
	if !e.Time.IsZero() {
		t.Fatalf("Time = %v, want the zero time", e.Time)
	}
	if e.Peer != nil {
		t.Fatalf("Peer = %+v, want nil", e.Peer)
	}
}
