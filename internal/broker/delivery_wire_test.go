package broker

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/deliverpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/internal/testkit/linktest"
)

// wireConfig is a ConfigSource whose Get can be counted and made to fail per
// key. Keys in override answer directly; the rest go to the embedded fake.
type wireConfig struct {
	*fakeConfig

	mu       sync.Mutex
	override map[string]config.Value
	errs     map[string]error
	gets     map[string]int
}

var _ ConfigSource = (*wireConfig)(nil)

// newWireConfig returns a config preloaded with the send defaults and the
// local alias, because fakeConfig answers only the keys it was given.
func newWireConfig(t *testing.T) *wireConfig {
	t.Helper()
	c := &wireConfig{
		fakeConfig: newFakeConfig(t.TempDir()),
		override:   make(map[string]config.Value),
		errs:       make(map[string]error),
		gets:       make(map[string]int),
	}
	c.override["broker.alias"] = config.Value{Kind: config.KindString, String: "hostA"}
	c.override["send.link_retries"] = config.Value{Kind: config.KindInt, Int: 5}
	c.override["send.link_backoff"] = config.Value{Kind: config.KindDuration, Duration: 500 * time.Millisecond}
	c.override["send.request_timeout"] = config.Value{Kind: config.KindDuration, Duration: 20 * time.Second}
	c.override["send.inbox_retries"] = config.Value{Kind: config.KindInt, Int: 2}
	c.override["send.inbox_backoff"] = config.Value{Kind: config.KindDuration, Duration: 200 * time.Millisecond}
	c.override["send.inbox_timeout"] = config.Value{Kind: config.KindDuration, Duration: 5 * time.Second}
	c.override["send.dedupe_window"] = config.Value{Kind: config.KindDuration, Duration: 5 * time.Minute}
	return c
}

func (c *wireConfig) Get(key string) (config.Value, error) {
	c.mu.Lock()
	c.gets[key]++
	err := c.errs[key]
	v, ok := c.override[key]
	c.mu.Unlock()
	if err != nil {
		return config.Value{}, err
	}
	if ok {
		return v, nil
	}
	return c.fakeConfig.Get(key)
}

func (c *wireConfig) setValue(key string, v config.Value) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.override[key] = v
}

func (c *wireConfig) fail(key string, err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.errs[key] = err
}

func (c *wireConfig) getCount(key string) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.gets[key]
}

func TestConfigSettingsReadsLive(t *testing.T) {
	// The delivery must see a reloaded send.request_timeout on the next send,
	// not only the value present at startup.
	c := newWireConfig(t)
	s := configSettings{cfg: c}

	if got := s.Send().RequestTimeout; got != 20*time.Second {
		t.Fatalf("initial RequestTimeout = %s, want 20s", got)
	}
	c.setValue("send.request_timeout", config.Value{Kind: config.KindDuration, Duration: 7 * time.Second})
	if got := s.Send().RequestTimeout; got != 7*time.Second {
		t.Fatalf("RequestTimeout after reload = %s, want 7s", got)
	}
	if got := c.getCount("send.request_timeout"); got != 2 {
		t.Fatalf("send.request_timeout was read %d times, want 2", got)
	}
}

func TestConfigSettingsFallsBackOnError(t *testing.T) {
	// A failed config read must keep the documented defaults instead of
	// disabling retries or sending with a zero timeout, and it must be logged.
	c := newWireConfig(t)
	c.fail("send.link_retries", errors.New("read failed"))
	c.fail("send.request_timeout", errors.New("read failed"))
	var buf bytes.Buffer
	level := new(slog.LevelVar)
	s := configSettings{cfg: c, log: NewLogger(&buf, level)}

	settings := s.Send()
	if got := settings.LinkRetries; got != 5 {
		t.Fatalf("LinkRetries after a failed read = %d, want the default 5", got)
	}
	if got := settings.RequestTimeout; got != 20*time.Second {
		t.Fatalf("RequestTimeout after a failed read = %s, want the default 20s", got)
	}
	logged := buf.String()
	for _, key := range []string{"send.link_retries", "send.request_timeout"} {
		if !strings.Contains(logged, evConfigRead) || !strings.Contains(logged, key) {
			t.Fatalf("log %q does not record the failed read of %s", logged, key)
		}
	}
}

func TestTopicVisibilityMapsTable(t *testing.T) {
	topics := newFakeTopics()
	a := AgentID("agent-a")
	b := AgentID("agent-b")
	c := AgentID("agent-c")
	for _, id := range []AgentID{a, b, c} {
		topics.AgentChanged(id, Member{Name: string(id), Ref: "1a2b3c", Status: "idle"})
	}
	if err := topics.Join(a, "t1"); err != nil {
		t.Fatalf("Join(a, t1): %v", err)
	}
	if err := topics.Join(b, "t1"); err != nil {
		t.Fatalf("Join(b, t1): %v", err)
	}

	v := topicVisibility{topics}
	if !v.VisibleLocal(string(a), string(b)) {
		t.Fatal("VisibleLocal(a, b) = false, want true: both are in t1")
	}
	if v.VisibleLocal(string(a), string(c)) {
		t.Fatal("VisibleLocal(a, c) = true, want false: c shares no topic")
	}

	peer := fp(7)
	topics.SetRemote(peer, []Member{
		{Name: "frontend", Ref: "1a2b3c", PrevName: "api-old", Status: "busy"},
	}, map[string][]string{"frontend": {"t1"}})
	if !v.VisibleRemote(string(a), peer, "frontend", "1a2b3c") {
		t.Fatal("VisibleRemote(a, peer, frontend) = false, want true")
	}
	if v.VisibleRemote(string(a), peer, "unrelated", "1a2b3c") {
		t.Fatal("VisibleRemote for an unknown member = true, want false")
	}

	members := v.RemoteMembers(peer)
	if len(members) != 1 {
		t.Fatalf("RemoteMembers returned %d entries, want 1", len(members))
	}
	if got := members[0]; got.Name != "frontend" || got.Ref != "1a2b3c" || got.PrevName != "api-old" {
		t.Fatalf("RemoteMembers entry = %+v, want the injected name, ref and previous name", got)
	}
}

// newWireLinkPair builds two real links whose fake peer tables know each
// other, without connecting them. The first link dials, the second accepts.
func newWireLinkPair(t *testing.T) (dialer, acceptor *link.Link, dialerPeers, acceptorPeers *linktest.FakePeers) {
	t.Helper()
	dialerPeers = linktest.NewFakePeers(t)
	acceptorPeers = linktest.NewFakePeers(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	clock := linktest.NewFakeClock(time.Unix(1_700_000_000, 0))

	dialer, err := link.New(link.Options{
		Peers:    dialerPeers,
		Settings: linktest.NewFakeSettings("dialer"),
		Clock:    clock,
		Logger:   logger,
	})
	if err != nil {
		t.Fatalf("link.New(dialer): %v", err)
	}
	acceptor, err = link.New(link.Options{
		Peers:    acceptorPeers,
		Settings: linktest.NewFakeSettings("acceptor"),
		Clock:    clock,
		Logger:   logger,
	})
	if err != nil {
		dialer.Close()
		t.Fatalf("link.New(acceptor): %v", err)
	}
	t.Cleanup(dialer.Close)
	t.Cleanup(acceptor.Close)
	return dialer, acceptor, dialerPeers, acceptorPeers
}

func TestLinkAdapterEstablished(t *testing.T) {
	dialer, acceptor, dialerPeers, acceptorPeers := newWireLinkPair(t)
	if err := acceptor.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	linktest.Introduce(dialerPeers, acceptorPeers, "dialer", "acceptor", acceptor.Addr().String())
	dialer.Start()
	acceptor.Start()

	adapter := linkAdapter{dialer}
	peer := acceptorPeers.Fingerprint()
	waitForReg(t, "link established", func() bool { return adapter.Established(peer) })

	dialer.Close()
	waitForReg(t, "link down", func() bool { return !adapter.Established(peer) })
}

func TestDeliveryAssemblerRequiresUpstream(t *testing.T) {
	// Each of the link, topics and pairing inputs must be in place before the
	// delivery is built; skipping one fails assembly instead of panicking on a
	// nil dependency.
	cases := []struct {
		name    string
		deps    *Deps
		asm     *Assembly
		wantMsg string
	}{
		{
			name:    "link missing",
			deps:    &Deps{Topics: newFakeTopics(), Pairing: &fakePairing{}},
			asm:     &Assembly{},
			wantMsg: "link",
		},
		{
			name:    "topics missing",
			deps:    &Deps{Pairing: &fakePairing{}},
			asm:     &Assembly{Link: &link.Link{}},
			wantMsg: "topic",
		},
		{
			name:    "pairing missing",
			deps:    &Deps{Topics: newFakeTopics()},
			asm:     &Assembly{Link: &link.Link{}},
			wantMsg: "pairing",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			components, err := newDeliveryAssembler().Assemble(context.Background(), nil, tc.deps, tc.asm)
			if err == nil {
				t.Fatalf("Assemble with %s returned no error", tc.name)
			}
			if !strings.Contains(err.Error(), tc.wantMsg) {
				t.Fatalf("error %q does not name the missing %s assembler", err, tc.wantMsg)
			}
			if tc.deps.Delivery != nil {
				t.Fatalf("Assemble with %s still filled deps.Delivery", tc.name)
			}
			if components != nil {
				t.Fatalf("Assemble with %s returned %d components, want none", tc.name, len(components))
			}
		})
	}
}

func TestDeliveryAssemblerRegistersBeforeListen(t *testing.T) {
	// The kind-400 handler is registered during assembly. A request that
	// arrives after the link starts must be answered with a kind-401 result,
	// not with the link's "unsupported kind" reply.
	remote, ours, remotePeers, ourPeers := newWireLinkPair(t)
	deps := &Deps{
		Topics:  newFakeTopics(),
		Pairing: &fakePairing{},
		Logger:  slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	asm := &Assembly{Link: ours}

	components, err := newDeliveryAssembler().Assemble(context.Background(), newWireConfig(t), deps, asm)
	if err != nil {
		t.Fatalf("Assemble: %v", err)
	}
	if deps.Delivery == nil {
		t.Fatal("Assemble did not fill deps.Delivery")
	}
	if len(components) != 1 {
		t.Fatalf("Assemble returned %d components, want 1", len(components))
	}

	if err := ours.Listen("127.0.0.1:0"); err != nil {
		t.Fatalf("Listen: %v", err)
	}
	linktest.Introduce(remotePeers, ourPeers, "remote", "ours", ours.Addr().String())
	for _, c := range components {
		if err := c.Start(context.Background(), nil); err != nil {
			t.Fatalf("component Start: %v", err)
		}
	}
	remote.Start()
	ours.Start()

	ourFP := ourPeers.Fingerprint()
	adapter := linkAdapter{remote}
	waitForReg(t, "link established", func() bool { return adapter.Established(ourFP) })

	frame, err := encodeDeliverRequest(deliverRequest{
		MessageID:  newMessageID(),
		SenderName: "remote-agent",
		SenderRef:  "1a2b3c",
		TargetName: "target",
		Remaining:  time.Second,
		Text:       "hello",
	})
	if err != nil {
		t.Fatalf("encodeDeliverRequest: %v", err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	reply, sent, err := remote.Request(ctx, ourFP, frame)
	if err != nil {
		t.Fatalf("Request: %v (sent=%v)", err, sent)
	}
	if !sent {
		t.Fatal("Request reports the frame was not sent")
	}
	if got, want := reply.Kind, link.FrameKind(deliverpb.KindDeliverResult); got != want {
		t.Fatalf("reply kind = %d, want %d (kind 4 would mean the handler was not registered)", got, want)
	}
	detail, err := decodeDeliverResult(reply)
	if err != nil {
		t.Fatalf("decodeDeliverResult: %v", err)
	}
	if detail == nil {
		t.Fatal("reply decoded as delivered; the empty roster must produce an error result")
	}
}

func TestDefaultAssemblersIncludesDelivery(t *testing.T) {
	asms := DefaultAssemblers()
	deliveryIndex := -1
	peerIndex := -1
	topicIndex := -1
	count := 0
	for i, a := range asms {
		switch a.Name() {
		case "delivery":
			count++
			deliveryIndex = i
		case "pairing and link":
			peerIndex = i
		case "topic":
			topicIndex = i
		}
	}
	if count != 1 {
		t.Fatalf("DefaultAssemblers contains %d delivery assemblers, want 1", count)
	}
	if peerIndex < 0 {
		t.Fatal("DefaultAssemblers no longer contains the pairing and link assembler")
	}
	if topicIndex < 0 {
		t.Fatal("DefaultAssemblers no longer contains the topic assembler")
	}
	if deliveryIndex < peerIndex {
		t.Fatalf("delivery assembler at %d runs before pairing and link at %d", deliveryIndex, peerIndex)
	}
	if deliveryIndex < topicIndex {
		t.Fatalf("delivery assembler at %d runs before topic at %d", deliveryIndex, topicIndex)
	}
}

// TestUnregisterForgetsTargetBuckets checks that the delivery component wires
// an Agent leaving the table to the inbound limiter: its buckets are dropped, so
// the quota table does not grow with every Agent that ever existed. Without the
// wiring, an ended Agent keeps its exhausted bucket forever.
func TestUnregisterForgetsTargetBuckets(t *testing.T) {
	tb := newTestBroker(t)
	lim := newInboundLimiter(nil)
	comp := &deliveryComponent{agents: &agentTableAdapter{}, inbound: lim}
	if err := comp.Start(context.Background(), tb.b); err != nil {
		t.Fatalf("Start: %v", err)
	}

	fake := fakesession.New(t, fakesession.Options{Name: "leaver"})
	stream := startRegistration(t, tb, fake)
	stream.complete(eventTimeout)
	id := mustAgentID(t, fake.SocketPath())

	peer := fakenetPeer(20)
	for i := 0; i < inboundLimitBurst; i++ {
		if ok, _ := lim.take(peer, string(id)); !ok {
			t.Fatalf("take %d for the registered Agent failed, want a full burst", i)
		}
	}
	if ok, _ := lim.take(peer, string(id)); ok {
		t.Fatal("take after the burst succeeded, want a refusal")
	}

	// The Agent leaves the table. The component's hook must drop its buckets,
	// so the next take starts a fresh one instead of staying refused.
	stream.cancel()
	waitForReg(t, "the Agent's inbound buckets to be dropped", func() bool {
		ok, _ := lim.take(peer, string(id))
		return ok
	})
}
