package broker

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
	"unicode/utf8"

	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/deliverpb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
)

// sendPeerMsgID is a fixed message id so a resend can be checked to reuse it.
const sendPeerMsgID = "3f2a1c4e-5b6d-4e8f-9a0b-1c2d3e4f5a6b"

// sendPeerFixture wires a sending delivery to a fake network. The peer end
// answers with whatever handler a test registers; no real receiving delivery is
// involved here.
type sendPeerFixture struct {
	t        *testing.T
	net      *fakeNet
	self     peerID
	peer     peerID
	delivery *delivery
	topics   *deliveryTopics
	peers    *deliveryPeers
	settings *fakeSettings
}

func newSendPeerFixture(t *testing.T, role pairing.Role) *sendPeerFixture {
	t.Helper()
	net := newFakeNet(t)
	self, peer := fakenetPeer(10), fakenetPeer(20)

	topics := &deliveryTopics{
		local:  make(map[string][]string),
		remote: make(map[peerID][]deliveryRemoteEntry),
	}
	peers := &deliveryPeers{
		aliases:   map[string]peerID{"hostB": peer},
		roles:     map[peerID]pairing.Role{peer: role},
		effective: map[peerID]string{peer: "hostB"},
	}
	settings := newFakeSettings(sendSettings{
		RequestTimeout: 2 * time.Second,
		LinkBackoff:    10 * time.Millisecond,
		InboxBackoff:   5 * time.Millisecond,
		InboxTimeout:   time.Second,
		DedupeWindow:   time.Minute,
		LinkRetries:    3,
		InboxRetries:   2,
	}, "hostA")

	d := newDelivery(deliveryDeps{
		Topics:   topics,
		Peers:    peers,
		Link:     net.end(self),
		Settings: settings,
	})
	return &sendPeerFixture{
		t:        t,
		net:      net,
		self:     self,
		peer:     peer,
		delivery: d,
		topics:   topics,
		peers:    peers,
		settings: settings,
	}
}

// caller returns a local Agent with the given name and ref, and an id that
// starts with the ref so the ref grammar holds.
func (f *sendPeerFixture) caller(name, ref string) agentView {
	return agentView{
		ID:   ref + strings.Repeat("0", 64-len(ref)),
		Name: name,
		Ref:  ref,
	}
}

// share gives a local Agent the listed topics.
func (f *sendPeerFixture) share(id string, shared ...string) {
	f.topics.local[id] = shared
}

// member adds one roster entry for the peer.
func (f *sendPeerFixture) member(m remoteMember, shared ...string) {
	f.topics.remote[f.peer] = append(f.topics.remote[f.peer], deliveryRemoteEntry{
		Member: m,
		Topics: shared,
	})
}

// handle registers the peer's answer to kind-400 requests.
func (f *sendPeerFixture) handle(fn func(context.Context, peerID, link.Frame) link.Frame) {
	f.net.end(f.peer).HandleRequest(link.FrameKind(deliverpb.KindDeliver), fn)
}

// send runs one sendToPeer with the fixture's peer and settings.
func (f *sendPeerFixture) send(caller agentView, to, text string) error {
	f.t.Helper()
	addr, ok := parseTarget(to)
	if !ok {
		f.t.Fatalf("parseTarget(%q) rejected the address", to)
	}
	return f.delivery.sendToPeer(context.Background(), peerSend{
		Caller:    caller,
		Peer:      f.peer,
		Addr:      addr,
		MessageID: sendPeerMsgID,
		Text:      text,
		Deadline:  time.Now().Add(f.settings.Send().RequestTimeout),
	})
}

// requireSendPeerError extracts the *Error a send returned.
func requireSendPeerError(t *testing.T, err error) *Error {
	t.Helper()
	if err == nil {
		t.Fatal("sendToPeer = nil, want an error")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("sendToPeer = %T %v, want *broker.Error", err, err)
	}
	return e
}

// deliveredAnswer decodes the request and reports success.
func deliveredAnswer(t *testing.T, got *deliverRequest) func(context.Context, peerID, link.Frame) link.Frame {
	return func(_ context.Context, _ peerID, fr link.Frame) link.Frame {
		req, err := decodeDeliverRequest(fr)
		if err != nil {
			t.Errorf("receiver could not decode the request: %v", err)
			return encodeDeliverResult(err)
		}
		if got != nil {
			*got = req
		}
		return encodeDeliverResult(nil)
	}
}

// TestSendPeerDelivered checks a successful delivery: the request carries the
// address the caller wrote, so a short or absent ref reaches the receiving
// Broker unchanged, and a positive budget (REQ-agent-messaging-013).
func TestSendPeerDelivered(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)

	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")

	var got deliverRequest
	f.handle(deliveredAnswer(t, &got))

	if err := f.send(caller, "frontend@hostB", "hello"); err != nil {
		t.Fatalf("sendToPeer = %v, want nil", err)
	}
	if got.TargetName != "frontend" || got.TargetRef != "" {
		t.Fatalf("target = %q [%s], want frontend []: the request must carry the address the caller wrote, never the roster's canonical ref", got.TargetName, got.TargetRef)
	}
	if got.MessageID != sendPeerMsgID || got.SenderName != "api-worker" || got.SenderRef != "1a2b3c" {
		t.Fatalf("request = %+v, want the caller's name, ref and message id", got)
	}
	budget := f.settings.Send().RequestTimeout
	if got.Remaining <= 0 || got.Remaining > budget {
		t.Fatalf("remaining budget = %v, want (0, %v]", got.Remaining, budget)
	}
}

// TestSendPeerForwardsShortRefVerbatim is the cross-link half of the colliding
// ref reproduction: a caller holds the owner's six-character ref, the owner has
// left, and the only roster entry is another account's worker with a colliding
// twelve-character ref. The sender must forward the caller's short ref, not
// upgrade it to the member's full ref, so the receiving Broker can refuse it.
func TestSendPeerForwardsShortRefVerbatim(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)

	caller := f.caller("observer", "1a2b3c")
	f.share(caller.ID, "t1")

	victimID := crossAccountID(t, collisionVictimPath)
	attackerID := crossAccountID(t, collisionAttackerPath)
	shared := string(victimID[:6])
	full := string(attackerID[:12])
	f.member(remoteMember{Name: "worker", Ref: full, RefRequired: true}, "t1")

	var got deliverRequest
	f.handle(func(_ context.Context, _ peerID, fr link.Frame) link.Frame {
		req, err := decodeDeliverRequest(fr)
		if err != nil {
			return encodeDeliverResult(err)
		}
		got = req
		// Model the receiving Broker: a short ref never selects a ref-required
		// member, and only that member's own full ref reaches it.
		if req.TargetRef == full {
			return encodeDeliverResult(nil)
		}
		return encodeDeliverResult(notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND,
			"no agent named \"worker\" on this Broker: the name belongs to another account"))
	})

	err := f.send(caller, "worker ["+shared+"]@hostB", "stolen?")
	if err == nil {
		t.Fatal("a send to the departed owner's short ref was upgraded to the collider's full ref and delivered")
	}
	if code := errorCode(err); code != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", code, err)
	}
	if got.TargetRef != shared {
		t.Fatalf("TargetRef = %q, want the caller's short ref %q, never the member's full %q", got.TargetRef, shared, full)
	}
}

// TestSendPeerPrefixNameShortRefNotFound checks the name-prefix bypass over the
// link: a member named worker2 whose ref collides with the owner's short ref
// must not answer an address that names worker.
func TestSendPeerPrefixNameShortRefNotFound(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)

	caller := f.caller("observer", "1a2b3c")
	f.share(caller.ID, "t1")

	victimID := crossAccountID(t, collisionVictimPath)
	attackerID := crossAccountID(t, collisionAttackerPath)
	shared := string(victimID[:6])
	full := string(attackerID[:12])
	f.member(remoteMember{Name: "worker2", Ref: full}, "t1")
	f.handle(deliveredAnswer(t, nil))

	err := f.send(caller, "worker ["+shared+"]@hostB", "stolen?")
	if err == nil {
		t.Fatal("a prefix-named member answered a short ref meant for worker")
	}
	if code := errorCode(err); code != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found (message %q)", code, err)
	}
	if n := len(f.net.requests(f.self, f.peer)); n != 0 {
		t.Fatalf("requests = %d, want 0: a prefix-named member must not be selected", n)
	}
}

// TestSendPeerErrorCarriesOrigin checks that a peer's error keeps its code and
// message, and that the originating fingerprint is the peer, never a
// fingerprint the peer claimed (REQ-agent-messaging-018, architecture.md:216).
func TestSendPeerErrorCarriesOrigin(t *testing.T) {
	setup := func(t *testing.T) *sendPeerFixture {
		f := newSendPeerFixture(t, pairing.RoleDialer)
		f.net.setUp(f.self, f.peer, true)
		caller := f.caller("api-worker", "1a2b3c")
		f.share(caller.ID, "t1")
		f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")
		return f
	}

	t.Run("peer error", func(t *testing.T) {
		f := setup(t)
		f.handle(func(context.Context, peerID, link.Frame) link.Frame {
			return encodeDeliverResult(&Error{
				Code:    localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE,
				Message: "Not sent: the session is gone",
			})
		})

		err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
		e := requireSendPeerError(t, err)
		if e.Code != localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE {
			t.Fatalf("Code = %v, want INBOX_UNWRITABLE", e.Code)
		}
		if e.Message != `Not sent: Broker hostB said: "the session is gone"` {
			t.Fatalf("Message = %q, want the peer's message quoted under this Broker's lead-in", e.Message)
		}
		if !bytes.Equal(e.Origin, f.peer[:]) {
			t.Fatalf("Origin = %x, want the peer fingerprint %x", e.Origin, f.peer)
		}

		st, ok := status.FromError(toStatus(err))
		if !ok {
			t.Fatal("toStatus did not return a status error")
		}
		details := st.Details()
		if len(details) != 1 {
			t.Fatalf("details = %d, want exactly 1", len(details))
		}
		d, ok := details[0].(*localpb.ErrorDetail)
		if !ok {
			t.Fatalf("detail type = %T, want *localpb.ErrorDetail", details[0])
		}
		if !bytes.Equal(d.GetOriginatingBrokerFingerprint(), f.peer[:]) {
			t.Fatalf("originating_broker_fingerprint = %x, want %x", d.GetOriginatingBrokerFingerprint(), f.peer)
		}
	})

	t.Run("peer claims another origin", func(t *testing.T) {
		f := setup(t)
		f.handle(func(context.Context, peerID, link.Frame) link.Frame {
			msg := &deliverpb.DeliverResult{Result: &deliverpb.DeliverResult_Error{Error: &localpb.ErrorDetail{
				Code:                         localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE,
				Message:                      "Not sent: relayed claim",
				OriginatingBrokerFingerprint: []byte{0xde, 0xad, 0xbe, 0xef},
			}}}
			payload, err := proto.Marshal(msg)
			if err != nil {
				t.Fatalf("marshal result: %v", err)
			}
			return link.Frame{Kind: link.FrameKind(deliverpb.KindDeliverResult), Payload: payload}
		})

		err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
		e := requireSendPeerError(t, err)
		if !bytes.Equal(e.Origin, f.peer[:]) {
			t.Fatalf("Origin = %x, want the peer fingerprint %x, not the claimed one", e.Origin, f.peer)
		}
		if bytes.Contains(e.Origin, []byte{0xde, 0xad, 0xbe, 0xef}) {
			t.Fatal("Origin carries the peer's claimed fingerprint")
		}
	})
}

// TestSendPeerNoRetryOnPeerError checks that a definite peer error ends the
// send: the peer has decided, so a retry cannot change the result
// (design-broker.md:244).
func TestSendPeerNoRetryOnPeerError(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")
	f.handle(func(context.Context, peerID, link.Frame) link.Frame {
		return encodeDeliverResult(notSent(localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, "not visible"))
	})

	err := f.send(caller, "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE {
		t.Fatalf("Code = %v, want NOT_VISIBLE", e.Code)
	}
	if n := len(f.net.requests(f.self, f.peer)); n != 1 {
		t.Fatalf("requests = %d, want 1: a definite peer error must not be retried", n)
	}
}

// TestSendPeerDialerKicksAndGivesUp checks that a dialer spends its retry
// budget on kicks and reports peer_unreachable without ever sending
// (REQ-agent-messaging-017, REQ-agent-messaging-019).
func TestSendPeerDialerKicksAndGivesUp(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.settings.SetSettings(sendSettings{
		RequestTimeout: 2 * time.Second,
		LinkBackoff:    10 * time.Millisecond,
		LinkRetries:    3,
	})

	start := time.Now()
	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	elapsed := time.Since(start)

	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE {
		t.Fatalf("Code = %v, want PEER_UNREACHABLE", e.Code)
	}
	if !strings.HasPrefix(e.Message, "Not sent") {
		t.Fatalf("Message = %q, want it to start with %q", e.Message, "Not sent")
	}
	if kicks := f.net.kicks(f.self, f.peer); kicks != 3 {
		t.Fatalf("kicks = %d, want 3", kicks)
	}
	if n := len(f.net.requests(f.self, f.peer)); n != 0 {
		t.Fatalf("requests = %d, want 0: nothing was ever sent", n)
	}
	if elapsed >= time.Second {
		t.Fatalf("elapsed = %v, want under 1s: retries must not wait out the budget", elapsed)
	}
}

// TestSendPeerDialerRecoversAfterKick checks that a dialer sends once the link
// comes up through a kick.
func TestSendPeerDialerRecoversAfterKick(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.settings.SetSettings(sendSettings{
		RequestTimeout: 2 * time.Second,
		LinkBackoff:    10 * time.Millisecond,
		LinkRetries:    5,
	})
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")
	f.handle(deliveredAnswer(t, nil))

	var kicks atomic.Int32
	f.net.onKick(f.self, f.peer, func() {
		if kicks.Add(1) == 2 {
			f.net.setUp(f.self, f.peer, true)
		}
	})

	if err := f.send(caller, "frontend@hostB", "hello"); err != nil {
		t.Fatalf("sendToPeer = %v, want nil after the link recovers", err)
	}
	if n := len(f.net.requests(f.self, f.peer)); n != 1 {
		t.Fatalf("requests = %d, want 1 after the link recovers", n)
	}
}

// upDuringEstablishedLink reports the link down once and brings it up before
// returning, so the up event lands after the sender read the link as down but
// before it takes the generation for the wait. The generation is taken at the
// start of each attempt, before Established, so this up must satisfy that wait.
type upDuringEstablishedLink struct {
	*fakeEnd
	once atomic.Bool
}

func (l *upDuringEstablishedLink) Established(p peerID) bool {
	if l.once.CompareAndSwap(false, true) {
		l.net.setUp(l.self, p, true)
		return false
	}
	return true
}

// TestSendPeerUpRaceWithGeneration checks that an up event landing after the
// link read as down but before the wait begins still wakes the wait. A dialer
// must not sit out one backoff, and an acceptor must not run into its deadline
// and report peer_unreachable, when the link is already back.
func TestSendPeerUpRaceWithGeneration(t *testing.T) {
	for _, role := range []pairing.Role{pairing.RoleDialer, pairing.RoleAcceptor} {
		name, wantKicks := "dialer", 1
		if role == pairing.RoleAcceptor {
			name, wantKicks = "acceptor", 0
		}
		t.Run(name, func(t *testing.T) {
			f := newSendPeerFixture(t, role)
			f.settings.SetSettings(sendSettings{
				RequestTimeout: 2 * time.Second,
				LinkBackoff:    time.Second,
				LinkRetries:    3,
			})
			caller := f.caller("api-worker", "1a2b3c")
			f.share(caller.ID, "t1")
			f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")
			f.handle(deliveredAnswer(t, nil))

			lnk := &upDuringEstablishedLink{fakeEnd: f.net.end(f.self)}
			f.delivery.deps.Link = lnk
			f.delivery.waiter = newLinkWaiter(lnk, nil)

			start := time.Now()
			err := f.send(caller, "frontend@hostB", "hello")
			elapsed := time.Since(start)

			if err != nil {
				t.Fatalf("sendToPeer = %v, want nil once the link is up", err)
			}
			if elapsed >= 100*time.Millisecond {
				t.Fatalf("elapsed = %v, want under 100ms: the wait must see the up event that landed before it began", elapsed)
			}
			if n := len(f.net.requests(f.self, f.peer)); n != 1 {
				t.Fatalf("requests = %d, want 1", n)
			}
			if kicks := f.net.kicks(f.self, f.peer); kicks != wantKicks {
				t.Fatalf("kicks = %d, want %d: the wait must not burn the retry budget", kicks, wantKicks)
			}
		})
	}
}

// TestSendPeerBackoffDoubles checks that the wait between kicks doubles: the
// dialer must not wake the peer at a fixed short interval.
func TestSendPeerBackoffDoubles(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.settings.SetSettings(sendSettings{
		RequestTimeout: 2 * time.Second,
		LinkBackoff:    10 * time.Millisecond,
		LinkRetries:    4,
	})

	var mu sync.Mutex
	var at []time.Time
	f.net.onKick(f.self, f.peer, func() {
		mu.Lock()
		defer mu.Unlock()
		at = append(at, time.Now())
	})

	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	_ = requireSendPeerError(t, err)

	mu.Lock()
	defer mu.Unlock()
	if len(at) != 4 {
		t.Fatalf("kicks = %d, want 4", len(at))
	}
	want := []time.Duration{10 * time.Millisecond, 20 * time.Millisecond, 40 * time.Millisecond}
	for i := range want {
		if gap := at[i+1].Sub(at[i]); gap < want[i] {
			t.Fatalf("gap %d = %v, want at least %v", i+1, gap, want[i])
		}
	}
}

// TestSendPeerAcceptorWaitsBudget checks that an acceptor never kicks and waits
// the full budget for the dialer to connect (REQ-agent-messaging-017).
func TestSendPeerAcceptorWaitsBudget(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleAcceptor)
	f.settings.SetSettings(sendSettings{
		RequestTimeout: 300 * time.Millisecond,
		LinkBackoff:    10 * time.Millisecond,
		LinkRetries:    3,
	})

	start := time.Now()
	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	elapsed := time.Since(start)

	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE {
		t.Fatalf("Code = %v, want PEER_UNREACHABLE", e.Code)
	}
	if kicks := f.net.kicks(f.self, f.peer); kicks != 0 {
		t.Fatalf("kicks = %d, want 0: an acceptor does not dial", kicks)
	}
	if elapsed < 300*time.Millisecond {
		t.Fatalf("elapsed = %v, want at least the 300ms budget", elapsed)
	}
	if elapsed > time.Second {
		t.Fatalf("elapsed = %v, want under 1s: the budget must bound the wait", elapsed)
	}
}

// TestSendPeerAcceptorDeliversWhenDialerConnects checks that an acceptor waits
// for the peer to connect and then delivers.
func TestSendPeerAcceptorDeliversWhenDialerConnects(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleAcceptor)
	f.settings.SetSettings(sendSettings{
		RequestTimeout: time.Second,
		LinkBackoff:    10 * time.Millisecond,
		LinkRetries:    3,
	})
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")
	f.handle(deliveredAnswer(t, nil))

	// Wait until the acceptor is blocked on the link, then bring it up: the
	// registration is the synchronization point, not a fixed delay.
	done := make(chan error, 1)
	go func() { done <- f.send(caller, "frontend@hostB", "hello") }()
	waitForRegistrations(t, f.delivery.waiter, f.peer, 1)
	f.net.setUp(f.self, f.peer, true)

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("sendToPeer = %v, want nil once the dialer connects", err)
		}
	case <-time.After(eventTimeout):
		t.Fatal("sendToPeer did not finish once the dialer connected")
	}
}

// TestSendPeerUnsupportedReply checks that an unsupported-kind reply becomes
// version_mismatch, is not retried and is never treated as delivered.
func TestSendPeerUnsupportedReply(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")
	// No kind-400 handler on the peer: the fake link answers UnsupportedError.

	err := f.send(caller, "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH {
		t.Fatalf("Code = %v, want VERSION_MISMATCH", e.Code)
	}
	if !strings.HasPrefix(e.Message, "Not sent") {
		t.Fatalf("Message = %q, want it to start with %q", e.Message, "Not sent")
	}
	if !bytes.Equal(e.Origin, f.peer[:]) {
		t.Fatalf("Origin = %x, want the peer fingerprint %x", e.Origin, f.peer)
	}
	if n := len(f.net.requests(f.self, f.peer)); n != 1 {
		t.Fatalf("requests = %d, want 1: an unsupported reply must not be retried", n)
	}
}

// TestSendPeerMalformedReply checks that a reply that cannot be decoded is an
// unknown result, never a success.
func TestSendPeerMalformedReply(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")
	f.handle(func(context.Context, peerID, link.Frame) link.Frame {
		return link.Frame{Kind: link.FrameKind(deliverpb.KindDeliverResult), Payload: []byte("not a result")}
	})

	err := f.send(caller, "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("Code = %v, want DELIVERY_UNKNOWN", e.Code)
	}
	if !strings.HasPrefix(e.Message, "Delivery unknown") {
		t.Fatalf("Message = %q, want it to start with %q", e.Message, "Delivery unknown")
	}
}

// unreadableReplyLink is a link that reports every request as sent but whose
// reply cannot be read. The error is not a link drop, and the link stays
// established throughout.
type unreadableReplyLink struct {
	*fakeEnd
	requests atomic.Int32
}

func (l *unreadableReplyLink) Request(context.Context, peerID, link.Frame) (link.Frame, bool, error) {
	l.requests.Add(1)
	return link.Frame{}, true, errors.New("undecodable reply")
}

// TestSendPeerUnreadableReplyIsDeliveryUnknown checks that an error other than a
// link drop after the frame left becomes delivery_unknown after one request:
// only a dropped link is waited out and resent with the same id
// (design-broker.md:259).
func TestSendPeerUnreadableReplyIsDeliveryUnknown(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)
	f.settings.SetSettings(sendSettings{
		RequestTimeout: 200 * time.Millisecond,
		LinkBackoff:    10 * time.Millisecond,
		LinkRetries:    3,
	})
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")

	lnk := &unreadableReplyLink{fakeEnd: f.net.end(f.self)}
	f.delivery.deps.Link = lnk
	f.delivery.waiter = newLinkWaiter(lnk, nil)

	err := f.send(caller, "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("Code = %v, want DELIVERY_UNKNOWN", e.Code)
	}
	if !strings.HasPrefix(e.Message, "Delivery unknown") {
		t.Fatalf("Message = %q, want it to start with %q", e.Message, "Delivery unknown")
	}
	if n := lnk.requests.Load(); n != 1 {
		t.Fatalf("requests = %d, want 1: an unreadable reply must not be resent", n)
	}
}

// TestSendPeerRemoteAmbiguous checks that duplicate remote names fail before a
// request is sent and list both ref-forced addresses with the peer's effective
// alias (REQ-agent-messaging-007, REQ-agent-messaging-004).
func TestSendPeerRemoteAmbiguous(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "api-worker", Ref: "111111"}, "t1")
	f.member(remoteMember{Name: "api-worker", Ref: "222222"}, "t1")

	err := f.send(caller, "api-worker@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_AMBIGUOUS {
		t.Fatalf("Code = %v, want AMBIGUOUS", e.Code)
	}
	for _, want := range []string{"api-worker [111111]@hostB", "api-worker [222222]@hostB"} {
		if !strings.Contains(e.Message, want) {
			t.Fatalf("Message = %q, want it to list %q", e.Message, want)
		}
	}
	if n := len(f.net.requests(f.self, f.peer)); n != 0 {
		t.Fatalf("requests = %d, want 0: ambiguity is found before sending", n)
	}
}

// TestSendPeerRemotePrevName checks that sending to a remote Agent's previous
// name names its current address (REQ-agent-messaging-031).
func TestSendPeerRemotePrevName(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "api-v2", Ref: "222222", PrevName: "api-worker"}, "t1")

	err := f.send(caller, "api-worker@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("Code = %v, want NOT_FOUND", e.Code)
	}
	if !strings.Contains(e.Message, "api-v2@hostB") {
		t.Fatalf("Message = %q, want it to name the current address api-v2@hostB", e.Message)
	}
	if n := len(f.net.requests(f.self, f.peer)); n != 0 {
		t.Fatalf("requests = %d, want 0: the rename is decided before sending", n)
	}
}

// TestSendPeerRemoteNotVisible checks that a roster member without a shared
// topic fails before any request (REQ-agent-messaging-014).
func TestSendPeerRemoteNotVisible(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "api-worker", Ref: "111111"}, "t2")

	err := f.send(caller, "api-worker@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE {
		t.Fatalf("Code = %v, want NOT_VISIBLE", e.Code)
	}
	if n := len(f.net.requests(f.self, f.peer)); n != 0 {
		t.Fatalf("requests = %d, want 0: visibility is decided before sending", n)
	}
}

// TestSendPeerUnpairedDuringSend checks that a peer that is no longer paired is
// not dialed.
func TestSendPeerUnpairedDuringSend(t *testing.T) {
	f := newSendPeerFixture(t, pairing.RoleUnknown)

	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("Code = %v, want NOT_FOUND", e.Code)
	}
	if kicks := f.net.kicks(f.self, f.peer); kicks != 0 {
		t.Fatalf("kicks = %d, want 0: an unpaired peer must not be dialed", kicks)
	}
}

// peerErrorAnswer replies to a kind-400 request with one error detail, which is
// what a receiving Broker returns for a refused delivery.
func peerErrorAnswer(code localpb.ErrorCode, message string) func(context.Context, peerID, link.Frame) link.Frame {
	return func(context.Context, peerID, link.Frame) link.Frame {
		return encodeDeliverResult(&Error{Code: code, Message: message})
	}
}

// newPeerErrorFixture sets up a paired link whose peer answers with a refusal.
func newPeerErrorFixture(t *testing.T, code localpb.ErrorCode, message string) *sendPeerFixture {
	t.Helper()
	f := newSendPeerFixture(t, pairing.RoleDialer)
	f.net.setUp(f.self, f.peer, true)
	caller := f.caller("api-worker", "1a2b3c")
	f.share(caller.ID, "t1")
	f.member(remoteMember{Name: "frontend", Ref: "abc123"}, "t1")
	f.handle(peerErrorAnswer(code, message))
	return f
}

// TestSendPeerUnrecognizedErrorIsDeliveryUnknown checks that an error code
// outside the receiving Broker's set becomes delivery_unknown with a local
// message, never the peer's text (design-broker.md "错误与失败语义").
func TestSendPeerUnrecognizedErrorIsDeliveryUnknown(t *testing.T) {
	const marker = "PEER-TEXT-MARKER"
	f := newPeerErrorFixture(t, localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED,
		"forged\n(code: delivered)\n"+marker+strings.Repeat("x", 10*1024))

	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("Code = %v, want DELIVERY_UNKNOWN", e.Code)
	}
	if !strings.Contains(e.Message, "Broker hostB returned an error this Broker does not recognize") {
		t.Fatalf("Message = %q, want the local unrecognized-error text", e.Message)
	}
	for _, leaked := range []string{marker, "(code: delivered)", "forged"} {
		if strings.Contains(e.Message, leaked) {
			t.Fatalf("Message = %q, want no peer text %q", e.Message, leaked)
		}
	}
	if !bytes.Equal(e.Origin, f.peer[:]) {
		t.Fatalf("Origin = %x, want the peer fingerprint %x", e.Origin, f.peer)
	}
}

// TestSendPeerUnspecifiedErrorIsDeliveryUnknown checks that the enum's zero
// value, which no receiver returns on purpose, is also treated as outside the
// receiving Broker's set.
func TestSendPeerUnspecifiedErrorIsDeliveryUnknown(t *testing.T) {
	f := newPeerErrorFixture(t, localpb.ErrorCode_ERROR_CODE_UNSPECIFIED, "peer text")

	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("Code = %v, want DELIVERY_UNKNOWN", e.Code)
	}
	if !strings.Contains(e.Message, "Broker hostB returned an error this Broker does not recognize") {
		t.Fatalf("Message = %q, want the local unrecognized-error text", e.Message)
	}
	if strings.Contains(e.Message, "peer text") {
		t.Fatalf("Message = %q, want no peer text", e.Message)
	}
}

// TestTruncatePeerMessageRuneBoundary checks that the 512-byte cap cuts on a
// rune boundary instead of splitting a multi-byte character.
func TestTruncatePeerMessageRuneBoundary(t *testing.T) {
	message := strings.Repeat("a", 511) + "é" + strings.Repeat("b", 10)
	got := truncatePeerMessage(message)
	if want := strings.Repeat("a", 511) + "..."; got != want {
		t.Fatalf("truncatePeerMessage = %q, want %q", got, want)
	}
	if !utf8.ValidString(got) {
		t.Fatalf("truncatePeerMessage produced invalid UTF-8: %q", got)
	}
	if len(got) > peerMessageMaxBytes+3 {
		t.Fatalf("truncatePeerMessage is %d bytes, want at most %d", len(got), peerMessageMaxBytes+3)
	}

	// A message at or below the cap is returned unchanged.
	short := strings.Repeat("é", peerMessageMaxBytes/2)
	if got := truncatePeerMessage(short); got != short {
		t.Fatalf("truncatePeerMessage changed a message at the cap")
	}
}

// TestSendPeerRecognizedErrorIsQuotedAndBounded checks that an allowed code
// keeps its code while its message is quoted by this Broker, free of control
// characters and bounded in length.
func TestSendPeerRecognizedErrorIsQuotedAndBounded(t *testing.T) {
	f := newPeerErrorFixture(t, localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE,
		"Not sent: line one\nline two\t"+strings.Repeat("z", 10*1024))

	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE {
		t.Fatalf("Code = %v, want INBOX_UNWRITABLE", e.Code)
	}
	const prefix = `Not sent: Broker hostB said: "`
	if !strings.HasPrefix(e.Message, prefix) {
		t.Fatalf("Message = %q, want prefix %q", e.Message, prefix)
	}
	if strings.ContainsAny(e.Message, "\n\r\t") {
		t.Fatalf("Message = %q, want no control characters", e.Message)
	}
	// The referenced content is capped at 512 bytes plus a trailing "...", and
	// this fixture is ASCII, so the quoted result cannot exceed that bound.
	if len(e.Message) > len(prefix)+520 {
		t.Fatalf("Message is %d bytes, want it bounded", len(e.Message))
	}
	if !strings.HasSuffix(e.Message, `..."`) {
		t.Fatalf("Message = %q, want a truncation marker", e.Message)
	}
	if !bytes.Equal(e.Origin, f.peer[:]) {
		t.Fatalf("Origin = %x, want the peer fingerprint %x", e.Origin, f.peer)
	}
}

// TestSendPeerQuotaExceededIsPreserved checks that the quota_exceeded a
// receiving Broker returns for a full per-source dedupe share or an exhausted
// inbound quota keeps its code and quoted text, so the model sees
// quota_exceeded rather than delivery_unknown.
func TestSendPeerQuotaExceededIsPreserved(t *testing.T) {
	const peerText = "Not sent: frontend is receiving too many messages from your Broker; try again in 8 seconds"
	f := newPeerErrorFixture(t, localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, peerText)

	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED {
		t.Fatalf("Code = %v, want QUOTA_EXCEEDED", e.Code)
	}
	const prefix = `Not sent: Broker hostB said: "`
	if !strings.HasPrefix(e.Message, prefix) {
		t.Fatalf("Message = %q, want prefix %q", e.Message, prefix)
	}
	if !strings.Contains(e.Message, "receiving too many messages") {
		t.Fatalf("Message = %q, want the peer's quota text", e.Message)
	}
	if !bytes.Equal(e.Origin, f.peer[:]) {
		t.Fatalf("Origin = %x, want the peer fingerprint %x", e.Origin, f.peer)
	}
}

// TestSendPeerDeliveryUnknownErrorKeepsPrefix checks that a delivery_unknown
// error keeps the outcome its code names even when the peer's message carried
// the other lead-in.
func TestSendPeerDeliveryUnknownErrorKeepsPrefix(t *testing.T) {
	f := newPeerErrorFixture(t, localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN,
		"Delivery unknown: the peer could not confirm")

	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	e := requireSendPeerError(t, err)
	if e.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("Code = %v, want DELIVERY_UNKNOWN", e.Code)
	}
	want := `Delivery unknown: Broker hostB said: "the peer could not confirm"`
	if e.Message != want {
		t.Fatalf("Message = %q, want %q", e.Message, want)
	}
}

// TestSendPeerUnrecognizedErrorLogsEvent checks that an unrecognized peer error
// is recorded once at warn level with the peer fingerprint and the integer
// code, and without the peer's message.
func TestSendPeerUnrecognizedErrorLogsEvent(t *testing.T) {
	const marker = "PEER-TEXT-MARKER"
	f := newPeerErrorFixture(t, localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED, marker)

	var logs bytes.Buffer
	f.delivery.deps.Log = NewLogger(&logs, new(slog.LevelVar))

	err := f.send(f.caller("api-worker", "1a2b3c"), "frontend@hostB", "hello")
	_ = requireSendPeerError(t, err)

	var found int
	for _, line := range strings.Split(strings.TrimSpace(logs.String()), "\n") {
		if line == "" {
			continue
		}
		var record map[string]any
		if err := json.Unmarshal([]byte(line), &record); err != nil {
			t.Fatalf("log line is not JSON: %v: %q", err, line)
		}
		event, _ := record["event"].(string)
		if event != EvPeerDeliveryErrorUnrecognized {
			continue
		}
		found++
		if got, _ := record["peer"].(string); got != hex.EncodeToString(f.peer[:]) {
			t.Fatalf("event peer = %q, want %q", got, hex.EncodeToString(f.peer[:]))
		}
		if got, _ := record["level"].(string); got != "WARN" {
			t.Fatalf("event level = %q, want WARN: %v", got, record)
		}
		code, ok := record["code"].(float64)
		if !ok {
			t.Fatalf("event has no integer code field: %v", record)
		}
		if got, want := localpb.ErrorCode(int32(code)), localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED; got != want {
			t.Fatalf("event code = %v, want %v", got, want)
		}
	}
	if found != 1 {
		t.Fatalf("unrecognized-error events = %d, want exactly 1", found)
	}
	if strings.Contains(logs.String(), marker) {
		t.Fatal("the log records the peer's message")
	}
}
