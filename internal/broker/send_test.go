package broker

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// sendTestMsgID is the message id the send fixture injects, so a log assertion
// can look for it.
const sendTestMsgID = "3f2a1c4e-5b6d-4e8f-9a0b-1c2d3e4f5a6b"

// sendAgents is a localAgents fake. Its name avoids the shared fakeAgents of the
// agent-table tests so the two can land independently.
type sendAgents struct {
	mu        sync.Mutex
	views     map[string]agentView
	rereadErr map[string]error
}

func newSendAgents() *sendAgents {
	return &sendAgents{views: make(map[string]agentView), rereadErr: make(map[string]error)}
}

func (a *sendAgents) put(v agentView) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.views[v.ID] = v
}

func (a *sendAgents) setRereadErr(id string, err error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	a.rereadErr[id] = err
}

func (a *sendAgents) Lookup(id string) (agentView, bool) {
	a.mu.Lock()
	defer a.mu.Unlock()
	v, ok := a.views[id]
	return v, ok
}

func (a *sendAgents) All() []agentView {
	a.mu.Lock()
	defer a.mu.Unlock()
	views := make([]agentView, 0, len(a.views))
	for _, v := range a.views {
		views = append(views, v)
	}
	return views
}

func (a *sendAgents) Reread(id string) (agentView, error) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if err, ok := a.rereadErr[id]; ok {
		return agentView{}, err
	}
	v, ok := a.views[id]
	if !ok {
		return agentView{}, errAgentGone
	}
	return v, nil
}

var _ localAgents = (*sendAgents)(nil)

// sendTestAgent builds an agentView the way the agent table derives one from a
// session record.
func sendTestAgent(name, socketPath string) agentView {
	sum := sha256.Sum256([]byte("session:" + socketPath))
	id := hex.EncodeToString(sum[:])
	return agentView{
		ID:      id,
		Name:    inbox.NormalizeName(name),
		Ref:     id[:6],
		Session: inbox.Session{SocketPath: socketPath},
		Inbox:   inboxPackage{},
	}
}

// sendFixture wires a delivery to fakes and a log buffer for the send tests.
type sendFixture struct {
	t        *testing.T
	delivery *delivery
	agents   *sendAgents
	topics   *deliveryTopics
	peers    *deliveryPeers
	settings *fakeSettings
	log      *bytes.Buffer
}

func defaultSendSettings() sendSettings {
	return sendSettings{
		RequestTimeout: 2 * time.Second,
		LinkBackoff:    10 * time.Millisecond,
		InboxBackoff:   5 * time.Millisecond,
		InboxTimeout:   time.Second,
		DedupeWindow:   time.Minute,
		LinkRetries:    3,
		InboxRetries:   2,
	}
}

func newSendFixture(t *testing.T, settings sendSettings, alias string) *sendFixture {
	t.Helper()
	agents := newSendAgents()
	topics := &deliveryTopics{
		local:  make(map[string][]string),
		remote: make(map[peerID][]deliveryRemoteEntry),
	}
	peers := &deliveryPeers{
		aliases:   make(map[string]peerID),
		roles:     make(map[peerID]pairing.Role),
		effective: make(map[peerID]string),
	}
	cfg := newFakeSettings(settings, alias)
	buf := &bytes.Buffer{}
	var level slog.LevelVar
	d := newDelivery(deliveryDeps{
		Agents:   agents,
		Topics:   topics,
		Peers:    peers,
		Settings: cfg,
		Log:      NewLogger(buf, &level),
		NewID:    func() string { return sendTestMsgID },
	})
	return &sendFixture{
		t:        t,
		delivery: d,
		agents:   agents,
		topics:   topics,
		peers:    peers,
		settings: cfg,
		log:      buf,
	}
}

// caller registers a local Agent that sends.
func (f *sendFixture) caller(name string) agentView {
	v := sendTestAgent(name, "/run/user/1000/"+name+".sock")
	f.agents.put(v)
	return v
}

// addSession registers a local Agent backed by a simulated session.
func (f *sendFixture) addSession(name string, mode fakesession.Mode) (agentView, *fakesession.Fake) {
	f.t.Helper()
	fs := fakesession.New(f.t, fakesession.Options{Name: name, Mode: mode})
	v := sendTestAgent(name, fs.SocketPath())
	v.Session = inbox.Session{
		ConfigDir:  fs.ConfigDir(),
		PID:        fs.PID(),
		Name:       v.Name,
		SocketPath: fs.SocketPath(),
	}
	f.agents.put(v)
	return v, fs
}

// share gives a local Agent the listed topics.
func (f *sendFixture) share(id string, topics ...string) {
	f.topics.local[id] = topics
}

// addPeer makes alias resolve to a paired peer.
func (f *sendFixture) addPeer(alias string, p peerID) {
	f.peers.aliases[alias] = p
	f.peers.roles[p] = pairing.RoleDialer
	f.peers.effective[p] = alias
}

// sendTextAtLimit returns a body whose longest-signature frame is exactly the
// limit. The body is repeated ASCII so each added byte adds one encoded byte.
func sendTextAtLimit(t *testing.T, name, ref string) string {
	t.Helper()
	base := MaxSendFrameSize(name, ref, "")
	if base > maxInboxFrameBytes {
		t.Fatalf("empty body already encodes to %d bytes, above the %d limit", base, maxInboxFrameBytes)
	}
	text := strings.Repeat("x", maxInboxFrameBytes-base)
	if got := MaxSendFrameSize(name, ref, text); got != maxInboxFrameBytes {
		t.Fatalf("MaxSendFrameSize at the boundary = %d, want %d", got, maxInboxFrameBytes)
	}
	return text
}

// TestSendLocalDelivers checks that a same-host send returns only after the
// target session recorded the frame, with the sender's address as its
// from-name (REQ-agent-messaging-013, REQ-agent-messaging-015,
// REQ-agent-messaging-028).
func TestSendLocalDelivers(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")
	target, fs := f.addSession("frontend", fakesession.CloseAfterRead)
	f.share(caller.ID, "t1")
	f.share(target.ID, "t1")

	if err := f.delivery.Send(context.Background(), caller, "frontend@hostA", "hello there"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	frames := fs.Frames()
	if len(frames) != 1 {
		t.Fatalf("target recorded %d frames after Send returned, want 1", len(frames))
	}
	frame := frames[0]
	if !strings.Contains(frame.Content, "hello there") {
		t.Fatalf("content = %q, want it to contain the body", frame.Content)
	}
	if frame.FromName != "api-worker@hostA" {
		t.Fatalf("from-name = %q, want %q", frame.FromName, "api-worker@hostA")
	}
}

// TestSendLocalDuplicateSenderHasRef checks that the caller's own signature
// carries its ref when another local Agent shares its name, even when that
// other Agent is not visible through the shared topic (REQ-agent-messaging-028).
func TestSendLocalDuplicateSenderHasRef(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")
	duplicate, _ := f.addSession("api-worker", fakesession.CloseAfterRead)
	target, fs := f.addSession("frontend", fakesession.CloseAfterRead)
	f.share(caller.ID, "t1")
	f.share(duplicate.ID, "t2")
	f.share(target.ID, "t1")

	// The Broker sets this bit on the view it hands the delivery when another
	// agent of the same account holds the caller's name.
	caller.RefMandatory = true
	f.agents.put(caller)

	if err := f.delivery.Send(context.Background(), caller, "frontend@hostA", "hi"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	frames := fs.Frames()
	if len(frames) != 1 {
		t.Fatalf("target recorded %d frames, want 1", len(frames))
	}
	want := "api-worker [" + caller.Ref + "]@hostA"
	if frames[0].FromName != want {
		t.Fatalf("from-name = %q, want %q", frames[0].FromName, want)
	}
}

// TestSendLocalSignatureUsesLocalAlias checks that a same-host signature names
// the receiver's alias for this Broker, not an address the caller typed
// (architecture.md:296).
func TestSendLocalSignatureUsesLocalAlias(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "lab-a")
	caller := f.caller("api-worker")
	target, fs := f.addSession("frontend", fakesession.CloseAfterRead)
	f.share(caller.ID, "t1")
	f.share(target.ID, "t1")

	if err := f.delivery.Send(context.Background(), caller, "frontend@lab-a", "hi"); err != nil {
		t.Fatalf("Send: %v", err)
	}
	frame, ok := fs.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("the target session recorded no frame")
	}
	if frame.FromName != "api-worker@lab-a" {
		t.Fatalf("from-name = %q, want %q", frame.FromName, "api-worker@lab-a")
	}
}

// TestSendLocalNotVisible checks that two local Agents without a shared topic
// cannot send, and nothing reaches the target (REQ-agent-messaging-014).
func TestSendLocalNotVisible(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")
	target, fs := f.addSession("frontend", fakesession.CloseAfterRead)
	f.share(caller.ID, "t1")
	f.share(target.ID, "t2")

	err := f.delivery.Send(context.Background(), caller, "frontend@hostA", "hi")
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE {
		t.Fatalf("code = %v, want not_visible", got)
	}
	if frames := fs.Frames(); len(frames) != 0 {
		t.Fatalf("target recorded %d frames, want 0", len(frames))
	}
}

// TestSendEmptyMessage checks that an empty body is rejected before anything is
// sent.
func TestSendEmptyMessage(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")

	err := f.delivery.Send(context.Background(), caller, "frontend@hostA", "")
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Fatalf("code = %v, want invalid_argument", got)
	}
}

// TestSendTooLargeBoundary checks the inclusive limit on the longest possible
// signature for a local and a remote target, and that an oversized message
// sends no request and writes nothing (REQ-agent-messaging-026).
func TestSendTooLargeBoundary(t *testing.T) {
	t.Run("local", func(t *testing.T) {
		f := newSendFixture(t, defaultSendSettings(), "hostA")
		caller := f.caller("api-worker")
		target, fs := f.addSession("frontend", fakesession.CloseAfterRead)
		f.share(caller.ID, "t1")
		f.share(target.ID, "t1")

		text := sendTextAtLimit(t, caller.Name, caller.Ref)
		if err := f.delivery.Send(context.Background(), caller, "frontend@hostA", text); err != nil {
			t.Fatalf("Send at the limit: %v", err)
		}
		if _, ok := fs.WaitFrame(eventTimeout); !ok {
			t.Fatal("the boundary message did not reach the target")
		}

		err := f.delivery.Send(context.Background(), caller, "frontend@hostA", text+"x")
		if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_TOO_LARGE {
			t.Fatalf("code = %v, want too_large", got)
		}
		if frames := fs.Frames(); len(frames) != 1 {
			t.Fatalf("target recorded %d frames, want 1", len(frames))
		}
	})

	t.Run("remote", func(t *testing.T) {
		f := newSendFixture(t, defaultSendSettings(), "hostA")
		caller := f.caller("api-worker")
		peer := fakenetPeer(20)
		f.addPeer("hostB", peer)
		var mu sync.Mutex
		var calls []peerSend
		f.delivery.toPeer = func(_ context.Context, r peerSend) error {
			mu.Lock()
			defer mu.Unlock()
			calls = append(calls, r)
			return nil
		}

		text := sendTextAtLimit(t, caller.Name, caller.Ref)
		if err := f.delivery.Send(context.Background(), caller, "frontend@hostB", text); err != nil {
			t.Fatalf("Send at the limit: %v", err)
		}
		mu.Lock()
		sent := len(calls)
		mu.Unlock()
		if sent != 1 {
			t.Fatalf("remote dispatches = %d, want 1", sent)
		}

		err := f.delivery.Send(context.Background(), caller, "frontend@hostB", text+"x")
		if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_TOO_LARGE {
			t.Fatalf("code = %v, want too_large", got)
		}
		mu.Lock()
		sent = len(calls)
		mu.Unlock()
		if sent != 1 {
			t.Fatalf("remote dispatches after the oversized send = %d, want 1", sent)
		}
	})
}

// TestSendUnknownAlias checks that an alias naming neither this Broker nor a
// peer is reported as not found, not treated as local.
func TestSendUnknownAlias(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")
	// A visible local frontend makes the assertion meaningful: the address is
	// unknown by its alias alone, not because no such Agent exists.
	target, targetSession := f.addSession("frontend", fakesession.CloseAfterRead)
	f.share(caller.ID, "t1")
	f.share(target.ID, "t1")

	err := f.delivery.Send(context.Background(), caller, "frontend@nowhere", "hi")
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("code = %v, want not_found", got)
	}
	if frames := targetSession.Frames(); len(frames) != 0 {
		t.Fatalf("target recorded %d frames, want 0", len(frames))
	}
}

// TestSendMalformedAddress checks that an address outside the grammar is
// reported as invalid rather than as an unknown Agent.
func TestSendMalformedAddress(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")

	err := f.delivery.Send(context.Background(), caller, "no-at", "hi")
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
		t.Fatalf("code = %v, want invalid_argument", got)
	}
}

// TestSendRemoteDispatch checks that a peer alias dispatches one peerSend whose
// budget starts at the send and whose message id is a UUID.
func TestSendRemoteDispatch(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")
	peer := fakenetPeer(20)
	f.addPeer("hostB", peer)

	var got peerSend
	called := 0
	f.delivery.toPeer = func(_ context.Context, r peerSend) error {
		got = r
		called++
		return nil
	}

	before := time.Now()
	err := f.delivery.Send(context.Background(), caller, "frontend@hostB", "hi")
	after := time.Now()
	if err != nil {
		t.Fatalf("Send: %v", err)
	}
	if called != 1 {
		t.Fatalf("remote dispatches = %d, want 1", called)
	}
	if got.Peer != peer {
		t.Fatalf("peer = %x, want %x", got.Peer, peer)
	}
	if got.Caller.ID != caller.ID {
		t.Fatalf("caller = %q, want %q", got.Caller.ID, caller.ID)
	}
	if got.Addr.Name != "frontend" || got.Addr.Alias != "hostB" {
		t.Fatalf("addr = %+v, want frontend@hostB", got.Addr)
	}
	if !isUUID(got.MessageID) {
		t.Fatalf("message id = %q, want a UUID", got.MessageID)
	}
	timeout := defaultSendSettings().RequestTimeout
	earliest := before.Add(timeout)
	latest := after.Add(timeout)
	if got.Deadline.Before(earliest) || got.Deadline.After(latest) {
		t.Fatalf("deadline = %v, want between %v and %v", got.Deadline, earliest, latest)
	}
	if got.Text != "hi" {
		t.Fatalf("text = %q, want %q", got.Text, "hi")
	}
}

// TestSendLocalInboxUnwritable checks that a missing target socket is reported
// as not written within the send budget (REQ-agent-messaging-018).
func TestSendLocalInboxUnwritable(t *testing.T) {
	settings := defaultSendSettings()
	settings.RequestTimeout = 500 * time.Millisecond
	settings.InboxTimeout = 100 * time.Millisecond
	settings.InboxBackoff = 10 * time.Millisecond
	f := newSendFixture(t, settings, "hostA")
	caller := f.caller("api-worker")
	target, fs := f.addSession("frontend", fakesession.CloseAfterRead)
	f.share(caller.ID, "t1")
	f.share(target.ID, "t1")
	fs.RemoveSocket()

	start := time.Now()
	err := f.delivery.Send(context.Background(), caller, "frontend@hostA", "hi")
	elapsed := time.Since(start)
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE {
		t.Fatalf("code = %v, want inbox_unwritable", got)
	}
	if elapsed > settings.RequestTimeout+500*time.Millisecond {
		t.Fatalf("Send took %v, want at most %v", elapsed, settings.RequestTimeout+500*time.Millisecond)
	}
}

// TestSendLocalUncertain checks that a possibly-written frame is reported as an
// unknown result, not as not sent (REQ-agent-messaging-027).
func TestSendLocalUncertain(t *testing.T) {
	settings := defaultSendSettings()
	settings.RequestTimeout = 500 * time.Millisecond
	settings.InboxTimeout = 50 * time.Millisecond
	f := newSendFixture(t, settings, "hostA")
	caller := f.caller("api-worker")
	target, _ := f.addSession("frontend", fakesession.HoldOpen)
	f.share(caller.ID, "t1")
	f.share(target.ID, "t1")

	err := f.delivery.Send(context.Background(), caller, "frontend@hostA", "hi")
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("code = %v, want delivery_unknown", got)
	}
	var e *Error
	if !errors.As(err, &e) || !strings.HasPrefix(e.Message, "Delivery unknown") {
		t.Fatalf("message = %q, want it to start with Delivery unknown", e.Message)
	}
}

// TestSendLocalTargetEnded checks that a target that left the table is reported
// as not writable and nothing is written.
func TestSendLocalTargetEnded(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")
	target, fs := f.addSession("frontend", fakesession.CloseAfterRead)
	f.share(caller.ID, "t1")
	f.share(target.ID, "t1")
	f.agents.setRereadErr(target.ID, errAgentGone)

	err := f.delivery.Send(context.Background(), caller, "frontend@hostA", "hi")
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE {
		t.Fatalf("code = %v, want inbox_unwritable", got)
	}
	if frames := fs.Frames(); len(frames) != 0 {
		t.Fatalf("target recorded %d frames, want 0", len(frames))
	}
}

// TestSendLocalRefRequiredWhenDuplicate checks that two same-named local Agents
// make a bare name ambiguous, and a ref selects one, matching cross-host
// behaviour (REQ-agent-messaging-007, REQ-agent-messaging-028).
func TestSendLocalRefRequiredWhenDuplicate(t *testing.T) {
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")
	first, firstSession := f.addSession("worker", fakesession.CloseAfterRead)
	second, secondSession := f.addSession("worker", fakesession.CloseAfterRead)
	if first.Ref == second.Ref {
		t.Fatalf("both duplicates have ref %q; the test cannot disambiguate", first.Ref)
	}
	f.share(caller.ID, "t1")
	f.share(first.ID, "t1")
	f.share(second.ID, "t1")

	err := f.delivery.Send(context.Background(), caller, "worker@hostA", "hi")
	if got := errorCode(err); got != localpb.ErrorCode_ERROR_CODE_AMBIGUOUS {
		t.Fatalf("code = %v, want ambiguous", got)
	}
	if len(firstSession.Frames()) != 0 || len(secondSession.Frames()) != 0 {
		t.Fatal("an ambiguous send wrote to a session")
	}

	if err := f.delivery.Send(context.Background(), caller, "worker ["+second.Ref+"]@hostA", "hi"); err != nil {
		t.Fatalf("Send with ref: %v", err)
	}
	if frames := firstSession.Frames(); len(frames) != 0 {
		t.Fatalf("the other duplicate recorded %d frames, want 0", len(frames))
	}
	if _, ok := secondSession.WaitFrame(eventTimeout); !ok {
		t.Fatal("the addressed duplicate recorded no frame")
	}

	// A local ref matches any ID prefix, not only the six-character ref the
	// table happens to spell, so a longer prefix selects the same Agent.
	if strings.HasPrefix(first.ID, second.ID[:8]) {
		t.Fatalf("both duplicates share the eight-character ID prefix %q; the test cannot disambiguate", second.ID[:8])
	}
	before := len(secondSession.Frames())
	if err := f.delivery.Send(context.Background(), caller, "worker ["+second.ID[:8]+"]@hostA", "hi"); err != nil {
		t.Fatalf("Send with an ID-prefix ref: %v", err)
	}
	if got := len(secondSession.Frames()); got != before+1 {
		t.Fatalf("the addressed duplicate recorded %d frames, want %d", got, before+1)
	}
	if frames := firstSession.Frames(); len(frames) != 0 {
		t.Fatalf("the other duplicate recorded %d frames, want 0", len(frames))
	}
}

// TestSendLogsWithoutBody checks that one send log line names the event, the
// message id and the result, and never the body (architecture.md:255).
func TestSendLogsWithoutBody(t *testing.T) {
	const body = "TOPSECRET-BODY"
	f := newSendFixture(t, defaultSendSettings(), "hostA")
	caller := f.caller("api-worker")
	target, fs := f.addSession("frontend", fakesession.CloseAfterRead)
	f.share(caller.ID, "t1")
	f.share(target.ID, "t1")

	if err := f.delivery.Send(context.Background(), caller, "frontend@hostA", body); err != nil {
		t.Fatalf("Send: %v", err)
	}
	if _, ok := fs.WaitFrame(eventTimeout); !ok {
		t.Fatal("the target session recorded no frame")
	}

	logged := f.log.String()
	for _, want := range []string{`"event":"send"`, sendTestMsgID, `"result":"delivered"`} {
		if !strings.Contains(logged, want) {
			t.Fatalf("log = %q, want it to contain %q", logged, want)
		}
	}
	if strings.Contains(logged, body) {
		t.Fatalf("log = %q, want it to omit the message body", logged)
	}
}
