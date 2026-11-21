package broker

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/pairing"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// scenarioMember is one Agent a scenario Broker advertises to its peers,
// together with the topics it shares. It stands in for one roster entry a topic
// exchange would deliver.
type scenarioMember struct {
	member remoteMember
	topics []string
}

// scenarioBroker is one delivery in a scenario: its own Agent table and its own
// view of the topic and peer tables, connected to the others through one
// fakeNet. The Inbox of every Agent is inboxPackage, so writes go to the
// simulated session's socket.
type scenarioBroker struct {
	t        *testing.T
	network  *scenarioNetwork
	net      *fakeNet
	fp       peerID
	alias    string
	d        *delivery
	agents   *fakeAgents
	topics   *deliveryTopics
	peers    *deliveryPeers
	settings *fakeSettings
	log      *syncBuffer

	// published is what this Broker announces to every other Broker. It is
	// guarded by the network lock.
	published []scenarioMember
}

// scenarioNetwork keeps several scenario Brokers on one fakeNet. Brokers join in
// creation order: the earlier Broker is the dialer towards the later one, which
// models the pairing that assigned the roles.
type scenarioNetwork struct {
	t       *testing.T
	net     *fakeNet
	mu      sync.Mutex
	brokers []*scenarioBroker
}

func newScenarioNetwork(t *testing.T) *scenarioNetwork {
	t.Helper()
	return &scenarioNetwork{t: t, net: newFakeNet(t)}
}

// defaultScenarioSettings are short enough for the tests to finish quickly and
// long enough that a healthy round trip never hits a deadline.
func defaultScenarioSettings() sendSettings {
	return sendSettings{
		RequestTimeout: 2 * time.Second,
		LinkBackoff:    5 * time.Millisecond,
		InboxBackoff:   2 * time.Millisecond,
		InboxTimeout:   time.Second,
		DedupeWindow:   time.Minute,
		LinkRetries:    3,
		InboxRetries:   2,
	}
}

// broker adds one Broker to the network and registers its kind-400 handler, as
// the assembly does before the link starts.
func (n *scenarioNetwork) broker(fp peerID, alias string) *scenarioBroker {
	n.t.Helper()

	logs := &syncBuffer{}
	var level slog.LevelVar
	b := &scenarioBroker{
		t:        n.t,
		network:  n,
		net:      n.net,
		fp:       fp,
		alias:    alias,
		agents:   newFakeAgents(),
		topics:   &deliveryTopics{local: make(map[string][]string), remote: make(map[peerID][]deliveryRemoteEntry)},
		peers:    &deliveryPeers{aliases: make(map[string]peerID), roles: make(map[peerID]pairing.Role), effective: make(map[peerID]string)},
		settings: newFakeSettings(defaultScenarioSettings(), alias),
		log:      logs,
	}
	b.d = newDelivery(deliveryDeps{
		Agents:   b.agents,
		Topics:   b.topics,
		Peers:    b.peers,
		Link:     n.net.end(fp),
		Settings: b.settings,
		Log:      NewLogger(logs, &level),
	})
	b.d.register()

	n.mu.Lock()
	for _, other := range n.brokers {
		b.connect(other)
	}
	n.brokers = append(n.brokers, b)
	n.mu.Unlock()
	return b
}

// connect wires the peer tables and rosters in both directions. b is the newer
// Broker, so other is the dialer towards b.
func (b *scenarioBroker) connect(other *scenarioBroker) {
	b.peers.aliases[other.alias] = other.fp
	b.peers.effective[other.fp] = other.alias
	b.peers.roles[other.fp] = pairing.RoleAcceptor
	b.topics.remote[other.fp] = copyScenarioEntries(other.published)

	other.peers.aliases[b.alias] = b.fp
	other.peers.effective[b.fp] = b.alias
	other.peers.roles[b.fp] = pairing.RoleDialer
	other.topics.remote[b.fp] = copyScenarioEntries(b.published)
}

func copyScenarioEntries(entries []scenarioMember) []deliveryRemoteEntry {
	out := make([]deliveryRemoteEntry, 0, len(entries))
	for _, e := range entries {
		out = append(out, deliveryRemoteEntry{
			Member: e.member,
			Topics: append([]string(nil), e.topics...),
		})
	}
	return out
}

// addAgent starts a simulated session, makes it a local Agent of b, and
// announces it to every other Broker, which is what the topic exchange would
// eventually do.
func (b *scenarioBroker) addAgent(name string, topics ...string) (agentView, *fakesession.Fake) {
	return b.addAgentNamed(name, "", topics...)
}

// addAgentNamed is addAgent with a previous name, used for the rename hint.
func (b *scenarioBroker) addAgentNamed(name, prevName string, topics ...string) (agentView, *fakesession.Fake) {
	b.t.Helper()

	fake := fakesession.New(b.t, fakesession.Options{Name: name})
	view := liveTestAgent(b.t, fake, name)
	view.PrevName = prevName

	b.agents.mu.Lock()
	b.agents.byID[view.ID] = view
	b.agents.mu.Unlock()
	b.topics.local[view.ID] = append([]string(nil), topics...)

	entry := scenarioMember{
		member: remoteMember{Name: view.Name, Ref: view.Ref, PrevName: prevName},
		topics: append([]string(nil), topics...),
	}
	n := b.network
	n.mu.Lock()
	b.published = append(b.published, entry)
	for _, other := range n.brokers {
		if other == b {
			continue
		}
		other.topics.remote[b.fp] = append(other.topics.remote[b.fp], deliveryRemoteEntry{
			Member: entry.member,
			Topics: append([]string(nil), entry.topics...),
		})
	}
	n.mu.Unlock()
	return view, fake
}

// setEffectiveAlias changes the alias this Broker uses for a peer, standing in
// for the peer's alias here.
func (b *scenarioBroker) setEffectiveAlias(p peerID, alias string) {
	b.peers.effective[p] = alias
}

func (b *scenarioBroker) send(caller agentView, to, text string) error {
	return b.d.Send(context.Background(), caller, to, text)
}

// requireScenarioError extracts the *Error a send returned.
func requireScenarioError(t *testing.T, err error) *Error {
	t.Helper()
	if err == nil {
		t.Fatal("send = nil, want an error")
	}
	var e *Error
	if !errors.As(err, &e) {
		t.Fatalf("send = %T %v, want *broker.Error", err, err)
	}
	return e
}

// requireScenarioNotSent checks a result that is certainly not delivered.
func requireScenarioNotSent(t *testing.T, label string, err error, want localpb.ErrorCode) *Error {
	t.Helper()
	e := requireScenarioError(t, err)
	if e.Code != want {
		t.Fatalf("%s: Code = %v, want %v", label, e.Code, want)
	}
	if !strings.HasPrefix(e.Message, "Not sent: ") {
		t.Fatalf("%s: Message = %q, want it to start with %q", label, e.Message, "Not sent: ")
	}
	return e
}

// scenarioRequestsWithID counts the requests carrying id.
func scenarioRequestsWithID(t *testing.T, frames []link.Frame, id string) int {
	t.Helper()
	n := 0
	for _, f := range frames {
		req, err := decodeDeliverRequest(f)
		if err != nil {
			t.Fatalf("decodeDeliverRequest: %v", err)
		}
		if req.MessageID == id {
			n++
		}
	}
	return n
}

// TestScenarioCrossHostSignature checks that a cross-host send arrives with the
// sender's address as the receiving Broker spells it: its own alias for the
// sender's Broker (REQ-agent-messaging-013, REQ-agent-messaging-022).
func TestScenarioCrossHostSignature(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, _ := a.addAgent("api-worker", "t1")
	_, frontend := b.addAgent("frontend", "t1")

	if err := a.send(sender, "frontend@hostB", "hello"); err != nil {
		t.Fatalf("Send = %v, want nil", err)
	}
	frame, ok := frontend.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no frame within %s", eventTimeout)
	}
	if frame.FromName != "api-worker@hostA" {
		t.Fatalf("FromName = %q, want %q", frame.FromName, "api-worker@hostA")
	}
	if !strings.Contains(frame.Content, "hello") {
		t.Fatalf("Content = %q, want it to carry the body", frame.Content)
	}

	// The receiver's alias for the sender's Broker is what decides the address.
	b.setEffectiveAlias(a.fp, "lab-a")
	if err := a.send(sender, "frontend@hostB", "again"); err != nil {
		t.Fatalf("second Send = %v, want nil", err)
	}
	frame, ok = frontend.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no second frame within %s", eventTimeout)
	}
	if frame.FromName != "api-worker@lab-a" {
		t.Fatalf("FromName = %q, want %q after the receiver changed its alias", frame.FromName, "api-worker@lab-a")
	}
}

// TestScenarioReplyBySignature checks that replying to the received FromName
// reaches the original sender: the address the receiver saw is a usable target
// on the sender's Broker (REQ-agent-messaging-025's Broker-side premise).
func TestScenarioReplyBySignature(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, senderSession := a.addAgent("api-worker", "t1")
	replier, replierSession := b.addAgent("frontend", "t1")

	if err := a.send(sender, "frontend@hostB", "hello"); err != nil {
		t.Fatalf("Send = %v, want nil", err)
	}
	frame, ok := replierSession.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no frame within %s", eventTimeout)
	}
	if frame.FromName != "api-worker@hostA" {
		t.Fatalf("FromName = %q, want %q", frame.FromName, "api-worker@hostA")
	}

	if err := b.send(replier, frame.FromName, "reply"); err != nil {
		t.Fatalf("reply Send = %v, want nil", err)
	}
	back, ok := senderSession.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no reply within %s", eventTimeout)
	}
	if back.FromName != "frontend@hostB" {
		t.Fatalf("reply FromName = %q, want %q", back.FromName, "frontend@hostB")
	}
	if !strings.Contains(back.Content, "reply") {
		t.Fatalf("reply Content = %q, want it to carry the body", back.Content)
	}
}

// TestScenarioCleanedNameReply checks that a name the receiver's cleaner would
// change gets a ref in the signature, and that replying to that signature
// reaches the name-cleaned Agent rather than a lookalike (REQ-agent-messaging-029).
func TestScenarioCleanedNameReply(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	cleaned, cleanedSession := a.addAgent("a<b", "t1")
	_, plainSession := a.addAgent("ab", "t1")
	replier, replierSession := b.addAgent("frontend", "t1")

	if err := a.send(cleaned, "frontend@hostB", "hello"); err != nil {
		t.Fatalf("Send = %v, want nil", err)
	}
	frame, ok := replierSession.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no frame within %s", eventTimeout)
	}
	if !strings.HasPrefix(frame.FromName, "ab [") || !strings.HasSuffix(frame.FromName, "]@hostA") {
		t.Fatalf("FromName = %q, want a cleaned name with a ref and the receiver's alias", frame.FromName)
	}

	if err := b.send(replier, frame.FromName, "reply"); err != nil {
		t.Fatalf("reply Send = %v, want nil", err)
	}
	back, ok := cleanedSession.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("the cleaned-name Agent received no reply within %s", eventTimeout)
	}
	if back.FromName != "frontend@hostB" {
		t.Fatalf("reply FromName = %q, want %q", back.FromName, "frontend@hostB")
	}
	if frames := plainSession.Frames(); len(frames) != 0 {
		t.Fatalf("the lookalike received %d frames, want 0", len(frames))
	}
}

// TestScenarioLongNameReply checks that a name too long for an address is
// truncated with its ref and that a reply to the truncated address still
// reaches the Agent (REQ-agent-messaging-037).
func TestScenarioLongNameReply(t *testing.T) {
	longName := strings.Repeat("x", 70)

	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, senderSession := a.addAgent(longName, "t1")
	replier, replierSession := b.addAgent("frontend", "t1")

	if err := a.send(sender, "frontend@hostB", "hello"); err != nil {
		t.Fatalf("Send = %v, want nil", err)
	}
	frame, ok := replierSession.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no frame within %s", eventTimeout)
	}
	if utf8.RuneCountInString(frame.FromName) > MaxAddressRunes {
		t.Fatalf("FromName is %d runes, want at most %d", utf8.RuneCountInString(frame.FromName), MaxAddressRunes)
	}
	name, ref, alias, ok := ParseAddress(frame.FromName)
	if !ok || ref == "" || alias != "hostA" {
		t.Fatalf("FromName = %q, want a truncated name with a ref and @hostA", frame.FromName)
	}
	if !strings.HasPrefix(longName, name) {
		t.Fatalf("signature name %q is not a prefix of the original name", name)
	}

	if err := b.send(replier, frame.FromName, "reply"); err != nil {
		t.Fatalf("reply Send = %v, want nil", err)
	}
	back, ok := senderSession.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("the long-named Agent received no reply within %s", eventTimeout)
	}
	if back.FromName != "frontend@hostB" {
		t.Fatalf("reply FromName = %q, want %q", back.FromName, "frontend@hostB")
	}
}

// TestScenarioSameHostMatchesCrossHost checks that a local send and a cross-host
// send report the same error code and the same fixed message prefix for the
// same fault, so one host behaves like two (REQ-agent-messaging-028).
func TestScenarioSameHostMatchesCrossHost(t *testing.T) {
	newPair := func(t *testing.T) (*scenarioNetwork, *scenarioBroker, *scenarioBroker) {
		t.Helper()
		n := newScenarioNetwork(t)
		a := n.broker(fakenetPeer(10), "hostA")
		b := n.broker(fakenetPeer(20), "hostB")
		n.net.setUp(a.fp, b.fp, true)
		return n, a, b
	}

	t.Run("not visible", func(t *testing.T) {
		n, a, b := newPair(t)
		caller, _ := a.addAgent("caller", "ta")
		a.addAgent("target", "tb")
		b.addAgent("target", "tb")

		local := requireScenarioNotSent(t, "local", a.send(caller, "target@hostA", "hello"),
			localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE)
		remote := requireScenarioNotSent(t, "remote", a.send(caller, "target@hostB", "hello"),
			localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE)
		if local.Code != remote.Code {
			t.Fatalf("local Code = %v, remote Code = %v, want the same code", local.Code, remote.Code)
		}
		if got := len(n.net.requests(a.fp, b.fp)); got != 0 {
			t.Fatalf("requests = %d, want 0: an invisible target is rejected before sending", got)
		}
	})

	t.Run("ambiguous", func(t *testing.T) {
		n, a, b := newPair(t)
		caller, _ := a.addAgent("caller", "t1")
		a.addAgent("dup", "t1")
		a.addAgent("dup", "t1")
		b.addAgent("dup", "t1")
		b.addAgent("dup", "t1")

		local := requireScenarioNotSent(t, "local", a.send(caller, "dup@hostA", "hello"),
			localpb.ErrorCode_ERROR_CODE_AMBIGUOUS)
		remote := requireScenarioNotSent(t, "remote", a.send(caller, "dup@hostB", "hello"),
			localpb.ErrorCode_ERROR_CODE_AMBIGUOUS)
		if local.Code != remote.Code {
			t.Fatalf("local Code = %v, remote Code = %v, want the same code", local.Code, remote.Code)
		}
		if got := len(n.net.requests(a.fp, b.fp)); got != 0 {
			t.Fatalf("requests = %d, want 0: ambiguity is decided before sending", got)
		}
	})

	t.Run("previous name", func(t *testing.T) {
		n, a, b := newPair(t)
		caller, _ := a.addAgent("caller", "t1")
		a.addAgentNamed("api-v2", "api-worker", "t1")
		b.addAgentNamed("api-v2", "api-worker", "t1")

		local := requireScenarioNotSent(t, "local", a.send(caller, "api-worker@hostA", "hello"),
			localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
		if !strings.Contains(local.Message, "api-v2@hostA") {
			t.Fatalf("local Message = %q, want it to name api-v2@hostA", local.Message)
		}
		remote := requireScenarioNotSent(t, "remote", a.send(caller, "api-worker@hostB", "hello"),
			localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
		if !strings.Contains(remote.Message, "api-v2@hostB") {
			t.Fatalf("remote Message = %q, want it to name api-v2@hostB", remote.Message)
		}
		if got := len(n.net.requests(a.fp, b.fp)); got != 0 {
			t.Fatalf("requests = %d, want 0: the rename is decided before sending", got)
		}
	})

	t.Run("too large", func(t *testing.T) {
		n, a, b := newPair(t)
		caller, _ := a.addAgent("caller", "t1")
		a.addAgent("target", "t1")
		b.addAgent("target", "t1")
		text := sendTextAtLimit(t, caller.Name, caller.Ref) + "x"

		local := requireScenarioNotSent(t, "local", a.send(caller, "target@hostA", text),
			localpb.ErrorCode_ERROR_CODE_TOO_LARGE)
		remote := requireScenarioNotSent(t, "remote", a.send(caller, "target@hostB", text),
			localpb.ErrorCode_ERROR_CODE_TOO_LARGE)
		if local.Code != remote.Code {
			t.Fatalf("local Code = %v, remote Code = %v, want the same code", local.Code, remote.Code)
		}
		if got := len(n.net.requests(a.fp, b.fp)); got != 0 {
			t.Fatalf("requests = %d, want 0: an oversized message is rejected before sending", got)
		}
	})
}

// TestScenarioUnknownNotUnreachable checks that a send whose replies never
// arrive reports an unknown result, not an unreachable peer, and that the
// target still received exactly one copy (REQ-agent-messaging-027).
func TestScenarioUnknownNotUnreachable(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, _ := a.addAgent("api-worker", "t1")
	_, frontend := b.addAgent("frontend", "t1")

	n.net.dropReplies(a.fp, b.fp, 100)
	n.net.onKick(a.fp, b.fp, func() { n.net.setUp(a.fp, b.fp, true) })

	e := requireScenarioError(t, a.send(sender, "frontend@hostB", "hello"))
	if e.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("Code = %v, want DELIVERY_UNKNOWN", e.Code)
	}
	if !strings.HasPrefix(e.Message, "Delivery unknown: ") {
		t.Fatalf("Message = %q, want it to start with %q", e.Message, "Delivery unknown: ")
	}
	if frames := frontend.Frames(); len(frames) != 1 {
		t.Fatalf("target received %d frames, want 1", len(frames))
	}
}

// TestScenarioUnreachableNeverDelivered checks that a down link reports
// peer_unreachable from either role and that the abandoned message is never
// delivered once the link recovers (REQ-agent-messaging-017,
// REQ-agent-messaging-019).
func TestScenarioUnreachableNeverDelivered(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")

	sender, senderSession := a.addAgent("api-worker", "t1")
	replier, frontend := b.addAgent("frontend", "t1")
	a.settings.SetSettings(defaultScenarioSettings())
	settings := defaultScenarioSettings()
	settings.RequestTimeout = 200 * time.Millisecond
	b.settings.SetSettings(settings)

	_ = requireScenarioNotSent(t, "dialer", a.send(sender, "frontend@hostB", "lost-original"),
		localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE)
	_ = requireScenarioNotSent(t, "acceptor", b.send(replier, "api-worker@hostA", "lost-original"),
		localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE)

	// A sentinel proves the link works afterwards; the abandoned message must
	// still be absent from both sessions.
	n.net.setUp(a.fp, b.fp, true)
	if err := a.send(sender, "frontend@hostB", "sentinel"); err != nil {
		t.Fatalf("sentinel Send = %v, want nil", err)
	}
	frame, ok := frontend.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no sentinel frame within %s", eventTimeout)
	}
	if !strings.Contains(frame.Content, "sentinel") {
		t.Fatalf("first frame = %q, want the sentinel", frame.Content)
	}
	for _, f := range frontend.Frames() {
		if strings.Contains(f.Content, "lost-original") {
			t.Fatalf("target received the abandoned message after all: %q", f.Content)
		}
	}
	for _, f := range senderSession.Frames() {
		if strings.Contains(f.Content, "lost-original") {
			t.Fatalf("sender's session received the abandoned message after all: %q", f.Content)
		}
	}
}

// TestScenarioTargetSessionGone checks that a target whose socket vanished
// reports inbox_unwritable, attributes it to the receiving Broker, and returns
// within the sender's budget (REQ-agent-messaging-018).
func TestScenarioTargetSessionGone(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, _ := a.addAgent("api-worker", "t1")
	_, frontend := b.addAgent("frontend", "t1")
	settings := defaultScenarioSettings()
	settings.RequestTimeout = 500 * time.Millisecond
	settings.InboxTimeout = 100 * time.Millisecond
	a.settings.SetSettings(settings)
	b.settings.SetSettings(settings)

	// The record stays, so the Agent still exists; only its listener is gone.
	frontend.RemoveSocket()

	start := time.Now()
	e := requireScenarioNotSent(t, "gone", a.send(sender, "frontend@hostB", "hello"),
		localpb.ErrorCode_ERROR_CODE_INBOX_UNWRITABLE)
	elapsed := time.Since(start)

	if !bytes.Equal(e.Origin, b.fp[:]) {
		t.Fatalf("Origin = %x, want the receiving Broker's fingerprint %x", e.Origin, b.fp)
	}
	if elapsed > settings.RequestTimeout+250*time.Millisecond {
		t.Fatalf("elapsed = %v, want it bounded by the %v request timeout", elapsed, settings.RequestTimeout)
	}
}

// TestScenarioNoForwarding checks that a Broker never relays a received message
// to another link: a name known only to the third Broker is not found
// (architecture.md:281).
func TestScenarioNoForwarding(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	c := n.broker(fakenetPeer(30), "hostC")
	n.net.setUp(a.fp, b.fp, true)
	n.net.setUp(b.fp, c.fp, true)

	caller, _ := a.addAgent("api-worker", "t1")
	b.addAgent("frontend", "t1")
	conly, _ := c.addAgent("conly", "t1")

	// A's roster for hostB says conly lives there, although it does not. The
	// message must reach B (so B is the one deciding), and B must not forward
	// it to C, where conly actually lives.
	a.topics.remote[b.fp] = append(a.topics.remote[b.fp], deliveryRemoteEntry{
		Member: remoteMember{Name: conly.Name, Ref: conly.Ref},
		Topics: []string{"t1"},
	})

	_ = requireScenarioNotSent(t, "forward", a.send(caller, "conly@hostB", "hello"),
		localpb.ErrorCode_ERROR_CODE_NOT_FOUND)
	if got := len(n.net.requests(a.fp, b.fp)); got != 1 {
		t.Fatalf("requests a -> b = %d, want 1: the message must reach the Broker the sender picked", got)
	}
	for _, pair := range [][2]peerID{{b.fp, c.fp}, {c.fp, b.fp}} {
		if got := len(n.net.requests(pair[0], pair[1])); got != 0 {
			t.Fatalf("requests %s -> %s = %d, want 0: a received message is never forwarded", pair[0], pair[1], got)
		}
	}
}

// TestScenarioNoCommonTopic checks that a peer member without a shared topic is
// rejected before any request, and the target session sees nothing
// (REQ-agent-messaging-014).
func TestScenarioNoCommonTopic(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	caller, _ := a.addAgent("api-worker", "ta")
	_, frontend := b.addAgent("frontend", "tb")

	_ = requireScenarioNotSent(t, "no-topic", a.send(caller, "frontend@hostB", "hello"),
		localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE)
	if got := len(n.net.requests(a.fp, b.fp)); got != 0 {
		t.Fatalf("requests = %d, want 0: visibility is decided before sending", got)
	}
	if frames := frontend.Frames(); len(frames) != 0 {
		t.Fatalf("target received %d frames, want 0", len(frames))
	}
}

// TestScenarioTooLargeNoRequest checks the frame-size boundary across a peer:
// the message at the limit arrives, and one byte over the limit is rejected
// without a request (REQ-agent-messaging-026).
func TestScenarioTooLargeNoRequest(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, _ := a.addAgent("api-worker", "t1")
	_, frontend := b.addAgent("frontend", "t1")

	atLimit := sendTextAtLimit(t, sender.Name, sender.Ref)
	if err := a.send(sender, "frontend@hostB", atLimit); err != nil {
		t.Fatalf("Send at the limit = %v, want nil", err)
	}
	frame, ok := frontend.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no frame within %s", eventTimeout)
	}
	if !strings.Contains(frame.Content, atLimit) {
		t.Fatal("the delivered body differs from the one at the limit")
	}

	before := len(n.net.requests(a.fp, b.fp))
	_ = requireScenarioNotSent(t, "over-limit", a.send(sender, "frontend@hostB", atLimit+"x"),
		localpb.ErrorCode_ERROR_CODE_TOO_LARGE)
	if after := len(n.net.requests(a.fp, b.fp)); after != before {
		t.Fatalf("requests after an oversized message = %d, want %d", after, before)
	}
}

// TestScenarioConcurrentSendsSameSession checks that one Agent can send
// concurrently to two Agents on one peer without losing or doubling a message
// (design-broker.md:298).
func TestScenarioConcurrentSendsSameSession(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, _ := a.addAgent("api-worker", "t1")
	_, first := b.addAgent("first", "t1")
	_, second := b.addAgent("second", "t1")
	settings := defaultScenarioSettings()
	settings.RequestTimeout = 3 * time.Second
	settings.LinkRetries = 5
	a.settings.SetSettings(settings)

	const perTarget = 10
	var wg sync.WaitGroup
	errs := make(chan error, 2*perTarget)
	for i := 0; i < perTarget; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			errs <- a.send(sender, "first@hostB", "to-first")
		}()
		go func() {
			defer wg.Done()
			errs <- a.send(sender, "second@hostB", "to-second")
		}()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent Send = %v, want nil", err)
		}
	}

	for name, fake := range map[string]*fakesession.Fake{"first": first, "second": second} {
		if _, ok := fake.WaitFrameN(perTarget, eventTimeout); !ok {
			t.Fatalf("%s received fewer than %d frames within %s", name, perTarget, eventTimeout)
		}
		if got := len(fake.Frames()); got != perTarget {
			t.Fatalf("%s received %d frames, want %d", name, got, perTarget)
		}
	}
}

// TestSendPeerResendSameIDAfterDrop checks that after a lost reply the sender
// resends the same message id and the real receiver writes once
// (REQ-agent-messaging-020).
func TestSendPeerResendSameIDAfterDrop(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, _ := a.addAgent("api-worker", "t1")
	_, frontend := b.addAgent("frontend", "t1")

	n.net.dropReplies(a.fp, b.fp, 1)
	n.net.onKick(a.fp, b.fp, func() { n.net.setUp(a.fp, b.fp, true) })

	if err := a.send(sender, "frontend@hostB", "hello"); err != nil {
		t.Fatalf("Send = %v, want nil", err)
	}
	if frames := frontend.Frames(); len(frames) != 1 {
		t.Fatalf("target received %d frames, want 1", len(frames))
	}

	reqs := n.net.requests(a.fp, b.fp)
	if len(reqs) != 2 {
		t.Fatalf("requests = %d, want 2", len(reqs))
	}
	first, err := decodeDeliverRequest(reqs[0])
	if err != nil {
		t.Fatalf("decode first request: %v", err)
	}
	second, err := decodeDeliverRequest(reqs[1])
	if err != nil {
		t.Fatalf("decode second request: %v", err)
	}
	if first.MessageID != second.MessageID {
		t.Fatalf("resend id = %q, want the original %q", second.MessageID, first.MessageID)
	}
}

// TestSendPeerUnknownWhenRepliesKeepFailing checks that a send whose replies
// keep failing is unknown, not unreachable, and that the target wrote once
// (REQ-agent-messaging-027, REQ-agent-messaging-020).
func TestSendPeerUnknownWhenRepliesKeepFailing(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, _ := a.addAgent("api-worker", "t1")
	_, frontend := b.addAgent("frontend", "t1")

	n.net.dropReplies(a.fp, b.fp, 100)
	n.net.onKick(a.fp, b.fp, func() { n.net.setUp(a.fp, b.fp, true) })

	e := requireScenarioError(t, a.send(sender, "frontend@hostB", "hello"))
	if e.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("Code = %v, want DELIVERY_UNKNOWN", e.Code)
	}
	if !strings.HasPrefix(e.Message, "Delivery unknown: ") {
		t.Fatalf("Message = %q, want it to start with %q", e.Message, "Delivery unknown: ")
	}
	if frames := frontend.Frames(); len(frames) != 1 {
		t.Fatalf("target received %d frames, want 1", len(frames))
	}
}

// TestSendPeerNothingAfterReturn checks that a send which reported an unknown
// result does not keep retrying in the background: a later sentinel send grows
// only its own request count and the abandoned message stays written once
// (REQ-agent-messaging-016, REQ-agent-messaging-019).
func TestSendPeerNothingAfterReturn(t *testing.T) {
	n := newScenarioNetwork(t)
	a := n.broker(fakenetPeer(10), "hostA")
	b := n.broker(fakenetPeer(20), "hostB")
	n.net.setUp(a.fp, b.fp, true)

	sender, _ := a.addAgent("api-worker", "t1")
	_, frontend := b.addAgent("frontend", "t1")

	n.net.dropReplies(a.fp, b.fp, 100)
	n.net.onKick(a.fp, b.fp, func() { n.net.setUp(a.fp, b.fp, true) })

	e := requireScenarioError(t, a.send(sender, "frontend@hostB", "first"))
	if e.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("first Send Code = %v, want DELIVERY_UNKNOWN", e.Code)
	}

	reqs := n.net.requests(a.fp, b.fp)
	if len(reqs) == 0 {
		t.Fatal("the first send never sent a request")
	}
	firstReq, err := decodeDeliverRequest(reqs[0])
	if err != nil {
		t.Fatalf("decode first request: %v", err)
	}
	before := scenarioRequestsWithID(t, reqs, firstReq.MessageID)

	// Stop losing replies so the sentinel can succeed.
	n.net.dropReplies(a.fp, b.fp, 0)
	n.net.setUp(a.fp, b.fp, true)
	if err := a.send(sender, "frontend@hostB", "sentinel"); err != nil {
		t.Fatalf("sentinel Send = %v, want nil", err)
	}
	firstFrame, ok := frontend.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no first frame within %s", eventTimeout)
	}
	sentinelFrame, ok := frontend.WaitFrame(eventTimeout)
	if !ok {
		t.Fatalf("no sentinel frame within %s", eventTimeout)
	}
	if !strings.Contains(firstFrame.Content, "first") || !strings.Contains(sentinelFrame.Content, "sentinel") {
		t.Fatalf("frames = %q, %q, want the abandoned message and the sentinel", firstFrame.Content, sentinelFrame.Content)
	}

	afterReqs := n.net.requests(a.fp, b.fp)
	if got := scenarioRequestsWithID(t, afterReqs, firstReq.MessageID); got != before {
		t.Fatalf("requests for the abandoned message grew from %d to %d after the send returned", before, got)
	}
	writes := 0
	for _, f := range frontend.Frames() {
		if strings.Contains(f.Content, "first") {
			writes++
		}
	}
	if writes != 1 {
		t.Fatalf("the abandoned message was written %d times, want 1", writes)
	}
}
