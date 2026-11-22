package directory

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"reflect"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
	"github.com/wangzw/peers/internal/pairing"
)

// The scenario tests wire several Directory instances into one fake network and
// walk the PRD acceptance cases end to end. They deliberately use Start's two
// side effects (frame registration and the startup check) and one maintenance
// wakeup through bringUp and tick, so they do not depend on the maintenance
// loop. Every helper is prefixed with "scenario" to avoid collisions with the
// other test files in this package.

// scenarioNode is one Directory wired to a fake network endpoint and a shared
// fake clock.
type scenarioNode struct {
	d     *Directory
	ep    *fakeLinks
	fp    Fingerprint
	peers *fakePeers
	set   *fakeSettings
	logs  *lockedBuffer
}

// newScenarioNode opens a private Broker identified by seed on the shared fake
// clock and network.
func newScenarioNode(t *testing.T, net *fakeNet, clock *fakeClock, seed byte) *scenarioNode {
	t.Helper()
	id := newTestIdentity(seed)
	peers := newFakePeers()
	settings := newFakeSettings(t.TempDir())
	logger, logs := newLogBuffer()
	ep := net.Endpoint(id.Fingerprint())
	d, err := openWith(Deps{
		Identity: id,
		Peers:    peers,
		Links:    ep,
		Settings: settings,
		Logger:   logger,
	}, clock, realFileOps{})
	if err != nil {
		t.Fatalf("open directory for seed %d: %v", seed, err)
	}
	t.Cleanup(d.Close)
	return &scenarioNode{
		d:     d,
		ep:    ep,
		fp:    id.Fingerprint(),
		peers: peers,
		set:   settings,
		logs:  logs,
	}
}

// bringUp replaces Start in the scenario tests: it registers the reconciliation
// handlers, subscribes to link notifications and runs the startup check. It does
// not start the maintenance loop.
func bringUp(d *Directory) {
	d.registerReconcile()
	d.trackLinks()
	d.startupCheck(d.now())
}

// tick replaces one wakeup of the maintenance loop: it purges expired records,
// renews anything due and persists a pending change. Offline instances are not
// ticked.
func tick(d *Directory) {
	now := d.now()
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return
	}
	if d.st.purgeExpired(now) > 0 {
		d.dirty = true
	}
	d.mu.Unlock()
	d.renewDue(now)
	d.persistIfDirty()
}

// scenarioConnect connects every adjacent pair of nodes.
func scenarioConnect(net *fakeNet, nodes ...*scenarioNode) {
	for i := 0; i+1 < len(nodes); i++ {
		net.Connect(nodes[i].fp, nodes[i+1].fp)
	}
}

// scenarioWait drains the fake network until cond reports true. The fake net
// delivers a full list sent from onUp only after the peer's onUp has returned,
// so the link-up exchange needs no resend. The loop re-settles instead of
// sleeping because a fetch goroutine can start after the network looked idle
// once.
func scenarioWait(t *testing.T, net *fakeNet, _ []*scenarioNode, desc string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventTimeout)
	for {
		net.Settle(t)
		if cond() {
			return
		}
		if !time.Now().Before(deadline) {
			t.Fatalf("timed out waiting for %s", desc)
		}
		runtime.Gosched()
	}
}

// scenarioRelaxReplaceInterval advances the shared clock past the replacement
// interval and makes every node reconcile, so a newer version the interval held
// back is fetched on the next pass, then waits for cond. It models the cost of
// the interval: a legitimate change can wait for the next reconciliation.
func scenarioRelaxReplaceInterval(t *testing.T, net *fakeNet, clock *fakeClock, nodes []*scenarioNode, desc string, cond func() bool) {
	t.Helper()
	clock.Advance(directoryReplaceInterval())
	for _, n := range nodes {
		n.d.reconcileAll()
	}
	scenarioWait(t, net, nodes, desc, cond)
}

// scenarioInject seeds n with an already signed record as if it had arrived
// from a link. A zero source keeps the per-link limit out of the way.
func scenarioInject(t *testing.T, n *scenarioNode, r *storedRecord) {
	t.Helper()
	cp := *r
	cp.source = Fingerprint{}
	n.d.mu.Lock()
	out := n.d.st.accept(&cp, n.d.now(), n.d.limits())
	n.d.mu.Unlock()
	if out != outcomeAccepted && out != outcomeReplaced {
		t.Fatalf("inject record %v: accept returned %v", r.id, out)
	}
}

// scenarioHeld returns the record n holds for id.
func scenarioHeld(d *Directory, id recordID) (*storedRecord, bool) {
	d.mu.RLock()
	defer d.mu.RUnlock()
	return d.st.get(id)
}

// scenarioHeldCard returns the card record n holds for fp or fails the test.
func scenarioHeldCard(t *testing.T, n *scenarioNode, fp Fingerprint) *storedRecord {
	t.Helper()
	r, ok := scenarioHeld(n.d, scenarioCardID(fp))
	if !ok {
		t.Fatalf("node %s holds no card for %s", n.fp, fp)
	}
	return r
}

func scenarioCardID(fp Fingerprint) recordID {
	return recordID{publisher: fp, kind: recKindCard}
}

func scenarioTopicID(fp Fingerprint, name string) recordID {
	return recordID{publisher: fp, kind: recKindTopic, object: name}
}

// scenarioFind returns the broker entry for fp.
func scenarioFind(brokers []KnownBroker, fp Fingerprint) (KnownBroker, bool) {
	for _, b := range brokers {
		if b.Fingerprint == fp {
			return b, true
		}
	}
	return KnownBroker{}, false
}

// scenarioHas reports whether fp is listed.
func scenarioHas(brokers []KnownBroker, fp Fingerprint) bool {
	_, ok := scenarioFind(brokers, fp)
	return ok
}

// scenarioNonPeerCount counts the card holders that are not paired peers.
func scenarioNonPeerCount(d *Directory) int {
	n := 0
	for _, b := range d.Brokers() {
		if !b.IsPeer {
			n++
		}
	}
	return n
}

// scenarioTopicFound reports whether name is in the public topic list.
func scenarioTopicFound(d *Directory, name string) bool {
	_, ok := scenarioTopicDescription(d, name)
	return ok
}

// scenarioTopicDescription returns the merged description of name.
func scenarioTopicDescription(d *Directory, name string) (string, bool) {
	for _, t := range d.PublicTopics() {
		if t.Name == name {
			return t.Description, true
		}
	}
	return "", false
}

// scenarioSentRequestIDs decodes every RecordRequest endpoint ep sent to to.
func scenarioSentRequestIDs(ep *fakeLinks, to Fingerprint) []*directorypb.RecordId {
	var ids []*directorypb.RecordId
	for _, f := range ep.Sent() {
		if f.to != to || f.kind != directorypb.KindRecordRequest {
			continue
		}
		var req directorypb.RecordRequest
		if err := proto.Unmarshal(f.payload, &req); err != nil {
			continue
		}
		ids = append(ids, req.GetRecords()...)
	}
	return ids
}

// scenarioPushedList reports whether ep sent to recipient an incremental
// RecordList holding exactly one entry for id with a version above
// afterVersion. A directory pushes such a list as soon as it signs a
// revocation, without waiting for the next reconciliation round.
func scenarioPushedList(t *testing.T, ep *fakeLinks, recipient Fingerprint, id recordID, afterVersion uint64) bool {
	t.Helper()
	for _, f := range ep.Sent() {
		if f.to != recipient || f.kind != directorypb.KindRecordList {
			continue
		}
		var list directorypb.RecordList
		if err := proto.Unmarshal(f.payload, &list); err != nil {
			t.Fatalf("unmarshal RecordList: %v", err)
		}
		if len(list.Records) != 1 {
			continue
		}
		got, ok := idFromProto(list.Records[0].GetId())
		if ok && got == id && list.Records[0].GetVersion() > afterVersion {
			return true
		}
	}
	return false
}

// scenarioRequested reports whether ids names id.
func scenarioRequested(ids []*directorypb.RecordId, want recordID) bool {
	for _, pid := range ids {
		if id, ok := idFromProto(pid); ok && id == want {
			return true
		}
	}
	return false
}

// scenarioCapture records the payloads of the reconciliation frames a node
// receives.
type scenarioCapture struct {
	mu       sync.Mutex
	payloads [][]byte
}

func (c *scenarioCapture) add(p []byte) {
	c.mu.Lock()
	c.payloads = append(c.payloads, append([]byte(nil), p...))
	c.mu.Unlock()
}

func (c *scenarioCapture) all() [][]byte {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([][]byte, len(c.payloads))
	for i, p := range c.payloads {
		out[i] = append([]byte(nil), p...)
	}
	return out
}

// scenarioCaptureFrames wraps the node's state and request handlers so every
// payload it receives is recorded before being handled.
func scenarioCaptureFrames(n *scenarioNode) *scenarioCapture {
	c := &scenarioCapture{}
	n.ep.mu.Lock()
	state := n.ep.stateHandlers[directorypb.KindRecordList]
	n.ep.stateHandlers[directorypb.KindRecordList] = func(peer Fingerprint, p []byte) {
		c.add(p)
		state(peer, p)
	}
	request := n.ep.requestHandlers[directorypb.KindRecordRequest]
	n.ep.requestHandlers[directorypb.KindRecordRequest] = func(ctx context.Context, peer Fingerprint, p []byte) (uint32, []byte) {
		c.add(p)
		return request(ctx, peer, p)
	}
	n.ep.mu.Unlock()
	return c
}

// TestWalkthrough follows the eight steps of the design walkthrough.
func TestWalkthrough(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	a.set.SetAlias("hostA")
	a.set.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}})
	for _, n := range []*scenarioNode{a, b, c} {
		bringUp(n.d)
	}
	scenarioConnect(net, a, b, c)
	all := []*scenarioNode{a, b, c}
	scenarioWait(t, net, all, "the links to settle", func() bool { return true })

	// Step 1 and 2: A becomes public and B fetches the card.
	if err := a.d.SetPublic(true); err != nil {
		t.Fatalf("SetPublic(true): %v", err)
	}
	cardID := scenarioCardID(a.fp)
	scenarioWait(t, net, all, "C to hold A's card", func() bool {
		r, ok := scenarioHeld(c.d, cardID)
		return ok && !r.isRevocation()
	})

	// Step 3 and 4: C lists hostA as a connectable card holder.
	entry, ok := scenarioFind(c.d.Brokers(), a.fp)
	if !ok {
		t.Fatalf("C does not list A")
	}
	if entry.Alias != "hostA" {
		t.Fatalf("C lists A with alias %q, want hostA", entry.Alias)
	}
	wantAddrs := []config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}}
	if !reflect.DeepEqual(entry.Addresses, wantAddrs) {
		t.Fatalf("C lists A with addresses %v, want %v", entry.Addresses, wantAddrs)
	}
	if entry.Status != StatusConnectable {
		t.Fatalf("C lists A as %v, want connectable", entry.Status)
	}

	// Step 5: a day later the card is renewed and reaches every node.
	v1 := scenarioHeldCard(t, c, a.fp).version
	clock.Advance(24 * time.Hour)
	tick(a.d)
	tick(b.d)
	tick(c.d)
	scenarioWait(t, net, all, "A's renewed card to reach C", func() bool {
		r, ok := scenarioHeld(c.d, cardID)
		return ok && !r.isRevocation() && r.version > v1
	})
	v2 := scenarioHeldCard(t, c, a.fp).version
	if got := scenarioHeldCard(t, a, a.fp).version; got != v2 {
		t.Fatalf("A holds version %d, C holds %d", got, v2)
	}
	if got := scenarioHeldCard(t, b, a.fp).version; got != v2 {
		t.Fatalf("B holds version %d, C holds %d", got, v2)
	}
	oldCard := *scenarioHeldCard(t, c, a.fp)

	// Step 6: going private revokes the card everywhere. The revocation is
	// pushed right away rather than on a later reconciliation round.
	if err := a.d.SetPublic(false); err != nil {
		t.Fatalf("SetPublic(false): %v", err)
	}
	if !scenarioPushedList(t, a.ep, b.fp, cardID, v2) {
		t.Fatal("A did not push the revocation to B immediately after going private")
	}
	scenarioRelaxReplaceInterval(t, net, clock, all, "A's revocation to reach C", func() bool {
		r, ok := scenarioHeld(c.d, cardID)
		return ok && r.isRevocation() && r.version > v2
	})
	rev := *scenarioHeldCard(t, c, a.fp)
	v3 := rev.version
	for _, n := range all {
		r := scenarioHeldCard(t, n, a.fp)
		if !r.isRevocation() || r.version != v3 {
			t.Fatalf("node %s holds version %d revocation=%v, want version %d revocation", n.fp, r.version, r.isRevocation(), v3)
		}
	}
	if scenarioHas(c.d.Brokers(), a.fp) {
		t.Fatalf("C still lists A after the revocation")
	}

	// Step 7: a Broker that still holds the old card learns the revocation
	// without C asking it for the stale card.
	d := newScenarioNode(t, net, clock, 4)
	bringUp(d.d)
	scenarioInject(t, d, &oldCard)
	net.Connect(c.fp, d.fp)
	scenarioWait(t, net, []*scenarioNode{a, b, c, d}, "D to learn A's revocation", func() bool {
		r, ok := scenarioHeld(d.d, cardID)
		return ok && r.isRevocation() && r.version == v3
	})
	got, _ := scenarioHeld(d.d, cardID)
	if !bytes.Equal(got.raw, rev.raw) || !bytes.Equal(got.sig, rev.sig) {
		t.Fatalf("D's revocation is not the bytes C forwarded")
	}
	if scenarioRequested(scenarioSentRequestIDs(c.ep, d.fp), cardID) {
		t.Fatalf("C asked D for A's card")
	}

	// Step 8: the revocation expires and no node keeps A's record.
	clock.Advance(7 * 24 * time.Hour)
	online := []*scenarioNode{a, b, c, d}
	for _, n := range online {
		tick(n.d)
	}
	net.Settle(t)
	for _, n := range online {
		if _, held := scenarioHeld(n.d, cardID); held {
			t.Fatalf("node %s still holds A's card record after expiry", n.fp)
		}
		if scenarioHas(n.d.Brokers(), a.fp) {
			t.Fatalf("node %s still lists A after expiry", n.fp)
		}
	}
}

// TestCardReachesWholeNetwork covers REQ-broker-discovery-003 and
// REQ-broker-discovery-002: a card crosses three hops unchanged.
func TestCardReachesWholeNetwork(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	d := newScenarioNode(t, net, clock, 4)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	a.set.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}})
	nodes := []*scenarioNode{a, b, c, d}
	for _, n := range nodes {
		bringUp(n.d)
	}
	scenarioConnect(net, a, b, c, d)
	scenarioWait(t, net, nodes, "D to hold A's card", func() bool {
		r, ok := scenarioHeld(d.d, scenarioCardID(a.fp))
		return ok && !r.isRevocation()
	})

	entry, ok := scenarioFind(d.d.Brokers(), a.fp)
	if !ok {
		t.Fatalf("D does not list A")
	}
	if entry.Alias != "hostA" {
		t.Fatalf("D lists A with alias %q, want hostA", entry.Alias)
	}
	if entry.IsPeer {
		t.Fatalf("D lists A as a peer, want a card holder")
	}
	want := []config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}}
	if !reflect.DeepEqual(entry.Addresses, want) {
		t.Fatalf("D lists A with addresses %v, want %v", entry.Addresses, want)
	}
}

// TestPrivateBrokerNotLeaked covers REQ-broker-discovery-004 and
// REQ-broker-discovery-001: nothing about a private Broker crosses its links.
func TestPrivateBrokerNotLeaked(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	a.set.SetAlias("hostA") // never published, must not appear anywhere
	b.set.SetAlias("hostB")
	b.set.SetPublic(true)
	c.set.SetAlias("hostC")
	c.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b, c} {
		bringUp(n.d)
	}
	b.peers.Add(pairing.Peer{Fingerprint: a.fp, Alias: "peer-a"})
	capture := scenarioCaptureFrames(c)

	net.Connect(a.fp, b.fp)
	net.Connect(b.fp, c.fp)
	scenarioWait(t, net, []*scenarioNode{a, b, c}, "C to hold B's card", func() bool {
		r, ok := scenarioHeld(c.d, scenarioCardID(b.fp))
		return ok && !r.isRevocation()
	})

	received := capture.all()
	if len(received) == 0 {
		t.Fatalf("C received no reconciliation frames; the leak check would be vacuous")
	}
	for _, payload := range received {
		if bytes.Contains(payload, a.fp[:]) {
			t.Fatalf("a frame C received carries A's fingerprint")
		}
	}
	if scenarioHas(c.d.Brokers(), a.fp) {
		t.Fatalf("C lists A")
	}
	for _, topic := range c.d.PublicTopics() {
		if strings.Contains(topic.Name, a.fp.String()) || strings.Contains(topic.Description, a.fp.String()) {
			t.Fatalf("C lists a public topic tied to A")
		}
	}
	logs := c.logs.String()
	if strings.Contains(logs, a.fp.String()) {
		t.Fatalf("C's log names A's fingerprint")
	}
	if strings.Contains(logs, "hostA") {
		t.Fatalf("C's log names A's alias")
	}
	if !b.peers.IsPeer(a.fp) {
		t.Fatalf("B's peer table does not contain A")
	}
	if c.peers.IsPeer(a.fp) {
		t.Fatalf("C's peer table contains A")
	}
}

// TestPrivateBrokerLearnsOthers covers REQ-broker-discovery-005: a private
// Broker still learns cards from the rest of the connected network.
func TestPrivateBrokerLearnsOthers(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	b.set.SetAlias("hostB")
	b.set.SetPublic(true)
	c.set.SetAlias("hostC")
	c.set.SetPublic(true)
	c.set.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.7:17443"}})
	for _, n := range []*scenarioNode{a, b, c} {
		bringUp(n.d)
	}

	net.Connect(b.fp, c.fp)
	scenarioWait(t, net, []*scenarioNode{b, c}, "B to hold C's card", func() bool {
		r, ok := scenarioHeld(b.d, scenarioCardID(c.fp))
		return ok && !r.isRevocation()
	})
	net.Connect(a.fp, b.fp)
	scenarioWait(t, net, []*scenarioNode{a, b, c}, "A to hold C's card", func() bool {
		_, ok := scenarioHeld(a.d, scenarioCardID(c.fp))
		return ok
	})

	entry, ok := scenarioFind(a.d.Brokers(), c.fp)
	if !ok {
		t.Fatalf("private A does not list C")
	}
	if entry.Alias != "hostC" {
		t.Fatalf("A lists C with alias %q, want hostC", entry.Alias)
	}
	if len(a.d.PublicTopics()) != 0 {
		t.Fatalf("private A published public topics")
	}
}

// TestCardLifetime covers REQ-broker-discovery-007: a card survives six days
// and is gone on the eighth.
func TestCardLifetime(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b} {
		bringUp(n.d)
	}
	net.Connect(a.fp, b.fp)
	scenarioWait(t, net, []*scenarioNode{a, b}, "B to hold A's card", func() bool {
		r, ok := scenarioHeld(b.d, scenarioCardID(a.fp))
		return ok && !r.isRevocation()
	})

	a.d.Close() // offline: no renewal any more

	clock.Advance(6 * 24 * time.Hour)
	tick(b.d)
	net.Settle(t)
	if !scenarioHas(b.d.Brokers(), a.fp) {
		t.Fatalf("B stopped listing A on day 6")
	}

	clock.Advance(2 * 24 * time.Hour)
	tick(b.d)
	net.Settle(t)
	if scenarioHas(b.d.Brokers(), a.fp) {
		t.Fatalf("B still lists A on day 8")
	}
}

// TestTenDaysOnline covers REQ-broker-discovery-008: a public Broker renews
// daily and stays visible for ten days of hourly steps.
func TestTenDaysOnline(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b} {
		bringUp(n.d)
	}
	net.Connect(a.fp, b.fp)
	nodes := []*scenarioNode{a, b}
	scenarioWait(t, net, nodes, "B to hold A's card", func() bool {
		r, ok := scenarioHeld(b.d, scenarioCardID(a.fp))
		return ok && !r.isRevocation()
	})

	for hour := 1; hour <= 240; hour++ {
		clock.Advance(time.Hour)
		tick(a.d)
		tick(b.d)
		scenarioWait(t, net, nodes, fmt.Sprintf("B to list A after hour %d", hour), func() bool {
			return scenarioHas(b.d.Brokers(), a.fp)
		})
	}
}

// TestRevocationWithin covers REQ-broker-discovery-009: a revocation is pushed
// at once and a stale card cannot bring the entry back.
func TestRevocationWithin(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b, c} {
		bringUp(n.d)
	}
	scenarioConnect(net, a, b, c)
	scenarioWait(t, net, []*scenarioNode{a, b, c}, "C to hold A's card", func() bool {
		r, ok := scenarioHeld(c.d, scenarioCardID(a.fp))
		return ok && !r.isRevocation()
	})
	oldCard := *scenarioHeldCard(t, c, a.fp)

	if err := a.d.SetPublic(false); err != nil {
		t.Fatalf("SetPublic(false): %v", err)
	}
	scenarioRelaxReplaceInterval(t, net, clock, []*scenarioNode{a, b, c}, "C to drop A", func() bool {
		return !scenarioHas(c.d.Brokers(), a.fp)
	})

	e := newScenarioNode(t, net, clock, 5)
	bringUp(e.d)
	scenarioInject(t, e, &oldCard)
	net.Connect(e.fp, c.fp)
	scenarioWait(t, net, []*scenarioNode{a, b, c, e}, "E to fetch the revocation", func() bool {
		r, ok := scenarioHeld(e.d, scenarioCardID(a.fp))
		return ok && r.isRevocation()
	})
	if scenarioHas(c.d.Brokers(), a.fp) {
		t.Fatalf("C lists A again after a stale card arrives")
	}
}

// TestTotalLimitScenario covers REQ-broker-discovery-011: a full directory
// rejects new identifiers but keeps accepting renewals of the ones it holds.
func TestTotalLimitScenario(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	p := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	b.set.SetMaxRecords(3)
	bringUp(p.d)
	bringUp(b.d)

	injectCards := func(n int, base byte, now time.Time) []Fingerprint {
		out := make([]Fingerprint, 0, n)
		for i := 0; i < n; i++ {
			seed := base + byte(i)
			alias := fmt.Sprintf("pub%d", i)
			r := mustSignRecord(t, seed, recKindCard, "", uint64(now.UnixMilli())+uint64(i+1),
				now.Add(recordTTL),
				recTestCard(alias, advAddr("direct", fmt.Sprintf("198.51.100.%d:17443", 20+i))))
			scenarioInject(t, p, r)
			out = append(out, newTestIdentity(seed).Fingerprint())
		}
		return out
	}
	injectCards(5, 100, clock.Now())

	net.Connect(p.fp, b.fp)
	nodes := []*scenarioNode{p, b}
	scenarioWait(t, net, nodes, "B to fill its three slots", func() bool {
		return scenarioNonPeerCount(b.d) == 3
	})
	net.Settle(t)
	if got := scenarioNonPeerCount(b.d); got != 3 {
		t.Fatalf("B lists %d card holders, want 3", got)
	}

	heldVersions := make(map[Fingerprint]uint64)
	for _, entry := range b.d.Brokers() {
		if entry.IsPeer {
			continue
		}
		r, ok := scenarioHeld(b.d, scenarioCardID(entry.Fingerprint))
		if !ok {
			t.Fatalf("B lists %s without holding its card", entry.Fingerprint)
		}
		heldVersions[entry.Fingerprint] = r.version
	}

	// A day later every publisher renews and the three held identifiers accept
	// the replacement even though the directory is full.
	clock.Advance(24 * time.Hour)
	injectCards(5, 100, clock.Now())
	p.d.reconcileAll()
	scenarioWait(t, net, nodes, "B to accept the renewals", func() bool {
		for fp, old := range heldVersions {
			r, ok := scenarioHeld(b.d, scenarioCardID(fp))
			if !ok || r.version <= old {
				return false
			}
		}
		return true
	})
	net.Settle(t)
	if got := scenarioNonPeerCount(b.d); got != 3 {
		t.Fatalf("B lists %d card holders after renewal, want 3", got)
	}

	// Past the original expiry of the first records the renewals are still live.
	clock.Advance(6*24*time.Hour + 23*time.Hour)
	tick(b.d)
	net.Settle(t)
	if got := scenarioNonPeerCount(b.d); got != 3 {
		t.Fatalf("B lists %d card holders past the original expiry, want 3", got)
	}
	for fp := range heldVersions {
		if !scenarioHas(b.d.Brokers(), fp) {
			t.Fatalf("B dropped renewed publisher %s", fp)
		}
	}
}

// TestPerLinkLimitScenario covers REQ-broker-discovery-015: one peer can fill
// only its per-link share, another peer can still introduce a card.
func TestPerLinkLimitScenario(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	p := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	b.set.SetMaxRecords(10)
	b.set.SetMaxRecordsPerLink(4)
	bringUp(p.d)
	bringUp(b.d)

	now := clock.Now()
	for i := 0; i < 10; i++ {
		seed := byte(100 + i)
		r := mustSignRecord(t, seed, recKindCard, "", uint64(now.UnixMilli())+uint64(i+1),
			now.Add(recordTTL),
			recTestCard(fmt.Sprintf("p%d", i), advAddr("direct", fmt.Sprintf("203.0.113.%d:17443", 10+i))))
		scenarioInject(t, p, r)
	}
	net.Connect(p.fp, b.fp)
	scenarioWait(t, net, []*scenarioNode{p, b}, "B to take four records from P", func() bool {
		return scenarioNonPeerCount(b.d) == 4
	})
	net.Settle(t)
	if got := scenarioNonPeerCount(b.d); got != 4 {
		t.Fatalf("B lists %d card holders from P, want 4", got)
	}
	b.d.mu.RLock()
	fromP := b.d.st.perLink[p.fp]
	b.d.mu.RUnlock()
	if fromP != 4 {
		t.Fatalf("B accepted %d records from P, want 4", fromP)
	}

	q := newScenarioNode(t, net, clock, 3)
	bringUp(q.d)
	seed := byte(120)
	qr := mustSignRecord(t, seed, recKindCard, "", uint64(now.UnixMilli())+1, now.Add(recordTTL),
		recTestCard("q0", advAddr("direct", "198.51.100.30:17443")))
	scenarioInject(t, q, qr)
	qfp := newTestIdentity(seed).Fingerprint()

	net.Connect(q.fp, b.fp)
	scenarioWait(t, net, []*scenarioNode{p, b, q}, "B to take Q's card", func() bool {
		return scenarioHas(b.d.Brokers(), qfp)
	})
	if got := scenarioNonPeerCount(b.d); got != 5 {
		t.Fatalf("B lists %d card holders after Q, want 5", got)
	}
	b.d.mu.RLock()
	fromQ := b.d.st.perLink[q.fp]
	b.d.mu.RUnlock()
	if fromQ != 1 {
		t.Fatalf("B accepted %d records from Q, want 1", fromQ)
	}
}

// TestUnknownAddressKindPropagates covers REQ-broker-discovery-017: an address
// kind this version does not know is skipped by the views but travels with the
// card.
func TestUnknownAddressKindPropagates(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	a.set.SetAdvertised([]config.AdvertisedAddr{
		{Kind: "relay", Addr: "r1"},
		{Kind: "direct", Addr: "198.51.100.4:17443"},
	})
	for _, n := range []*scenarioNode{a, b, c} {
		bringUp(n.d)
	}
	scenarioConnect(net, a, b, c)
	scenarioWait(t, net, []*scenarioNode{a, b, c}, "C to hold A's card", func() bool {
		r, ok := scenarioHeld(c.d, scenarioCardID(a.fp))
		return ok && !r.isRevocation()
	})

	want := []config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}}
	for _, n := range []*scenarioNode{b, c} {
		entry, ok := scenarioFind(n.d.Brokers(), a.fp)
		if !ok {
			t.Fatalf("node %s does not list A", n.fp)
		}
		if !reflect.DeepEqual(entry.Addresses, want) {
			t.Fatalf("node %s lists A with addresses %v, want %v", n.fp, entry.Addresses, want)
		}
		if entry.Status != StatusConnectable {
			t.Fatalf("node %s lists A as %v, want connectable", n.fp, entry.Status)
		}
	}
}

// TestAddressChangeReissue covers REQ-broker-discovery-016: Reissue publishes
// the new address list across the network.
func TestAddressChangeReissue(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	a.set.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.4:17443"}})
	for _, n := range []*scenarioNode{a, b, c} {
		bringUp(n.d)
	}
	scenarioConnect(net, a, b, c)
	nodes := []*scenarioNode{a, b, c}
	scenarioWait(t, net, nodes, "C to hold A's card", func() bool {
		r, ok := scenarioHeld(c.d, scenarioCardID(a.fp))
		return ok && !r.isRevocation()
	})
	old := scenarioHeldCard(t, c, a.fp).version

	a.set.SetAdvertised([]config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.9:17443"}})
	a.d.Reissue()
	want := []config.AdvertisedAddr{{Kind: "direct", Addr: "198.51.100.9:17443"}}
	scenarioRelaxReplaceInterval(t, net, clock, nodes, "C to see A's new address", func() bool {
		r, ok := scenarioHeld(c.d, scenarioCardID(a.fp))
		if !ok || r.version <= old {
			return false
		}
		entry, ok := scenarioFind(c.d.Brokers(), a.fp)
		return ok && reflect.DeepEqual(entry.Addresses, want)
	})
}

// TestPublicTopicAcrossNetwork covers REQ-topics-010 and REQ-topics-011: a
// declaration with and without a description reaches three hops away.
func TestPublicTopicAcrossNetwork(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b, c} {
		bringUp(n.d)
	}
	scenarioConnect(net, a, b, c)
	scenarioWait(t, net, []*scenarioNode{a, b, c}, "the links to settle", func() bool { return true })

	if err := a.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic(infra-help): %v", err)
	}
	if err := a.d.DeclarePublicTopic("ops", ""); err != nil {
		t.Fatalf("DeclarePublicTopic(ops): %v", err)
	}
	scenarioWait(t, net, []*scenarioNode{a, b, c}, "C to list both public topics", func() bool {
		return scenarioTopicFound(c.d, "infra-help") && scenarioTopicFound(c.d, "ops")
	})
	if got, _ := scenarioTopicDescription(c.d, "infra-help"); got != "ask here" {
		t.Fatalf("C lists infra-help with description %q, want %q", got, "ask here")
	}
	if got, _ := scenarioTopicDescription(c.d, "ops"); got != "" {
		t.Fatalf("C lists ops with description %q, want the empty string", got)
	}
}

// TestPublicTopicExpiresOffline covers REQ-topics-012: an offline declarer's
// declaration disappears after seven days.
func TestPublicTopicExpiresOffline(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b} {
		bringUp(n.d)
	}
	net.Connect(a.fp, b.fp)
	scenarioWait(t, net, []*scenarioNode{a, b}, "the links to settle", func() bool { return true })
	if err := a.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic: %v", err)
	}
	scenarioWait(t, net, []*scenarioNode{a, b}, "B to list the declaration", func() bool {
		return scenarioTopicFound(b.d, "infra-help")
	})

	a.d.Close()
	clock.Advance(8 * 24 * time.Hour)
	tick(b.d)
	net.Settle(t)
	if scenarioTopicFound(b.d, "infra-help") {
		t.Fatalf("B still lists infra-help on day 8")
	}
}

// TestPrivateDeclareFails covers REQ-topics-023: a private Broker cannot
// declare a public topic.
func TestPrivateDeclareFails(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	for _, n := range []*scenarioNode{a, b} {
		bringUp(n.d)
	}
	net.Connect(a.fp, b.fp)
	scenarioWait(t, net, []*scenarioNode{a, b}, "the links to settle", func() bool { return true })

	if err := a.d.DeclarePublicTopic("infra-help", ""); !errors.Is(err, ErrNotPublic) {
		t.Fatalf("DeclarePublicTopic on a private Broker returned %v, want ErrNotPublic", err)
	}
	if _, ok := scenarioHeld(a.d, scenarioTopicID(a.fp, "infra-help")); ok {
		t.Fatalf("the failed declaration left a record behind")
	}
	net.Settle(t)
	if scenarioTopicFound(b.d, "infra-help") {
		t.Fatalf("B lists infra-help after the rejected declaration")
	}
}

// TestGoPrivateRetractsTopics covers REQ-topics-024: going private revokes the
// public topic declarations across the network.
func TestGoPrivateRetractsTopics(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	c := newScenarioNode(t, net, clock, 3)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b, c} {
		bringUp(n.d)
	}
	scenarioConnect(net, a, b, c)
	scenarioWait(t, net, []*scenarioNode{a, b, c}, "the links to settle", func() bool { return true })

	if err := a.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic: %v", err)
	}
	scenarioWait(t, net, []*scenarioNode{a, b, c}, "C to list infra-help", func() bool {
		return scenarioTopicFound(c.d, "infra-help")
	})

	if err := a.d.SetPublic(false); err != nil {
		t.Fatalf("SetPublic(false): %v", err)
	}
	scenarioRelaxReplaceInterval(t, net, clock, []*scenarioNode{a, b, c}, "C to drop infra-help", func() bool {
		return !scenarioTopicFound(c.d, "infra-help")
	})
	if scenarioTopicFound(c.d, "infra-help") {
		t.Fatalf("C still lists infra-help after A went private")
	}
}

// TestLastPublicMemberLeaves covers REQ-topics-025: retiring the local
// declaration withdraws it, and declaring again brings it back.
func TestLastPublicMemberLeaves(t *testing.T) {
	net := newFakeNet()
	clock := newFakeClock(dirTestNow())
	a := newScenarioNode(t, net, clock, 1)
	b := newScenarioNode(t, net, clock, 2)
	a.set.SetAlias("hostA")
	a.set.SetPublic(true)
	for _, n := range []*scenarioNode{a, b} {
		bringUp(n.d)
	}
	net.Connect(a.fp, b.fp)
	nodes := []*scenarioNode{a, b}
	scenarioWait(t, net, nodes, "the links to settle", func() bool { return true })

	if err := a.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic: %v", err)
	}
	scenarioWait(t, net, nodes, "B to list infra-help", func() bool {
		return scenarioTopicFound(b.d, "infra-help")
	})

	a.d.RetirePublicTopic("infra-help")
	scenarioRelaxReplaceInterval(t, net, clock, nodes, "B to drop infra-help", func() bool {
		return !scenarioTopicFound(b.d, "infra-help")
	})
	if scenarioTopicFound(b.d, "infra-help") {
		t.Fatalf("B still lists infra-help after it was retired")
	}

	if err := a.d.DeclarePublicTopic("infra-help", "ask here"); err != nil {
		t.Fatalf("DeclarePublicTopic again: %v", err)
	}
	scenarioRelaxReplaceInterval(t, net, clock, nodes, "B to list infra-help again", func() bool {
		return scenarioTopicFound(b.d, "infra-help")
	})

	a.d.RetirePublicTopic("infra-help")
	scenarioRelaxReplaceInterval(t, net, clock, nodes, "B to drop infra-help again", func() bool {
		return !scenarioTopicFound(b.d, "infra-help")
	})
}
