package broker

import (
	"context"
	"errors"
	"log/slog"
	"sync/atomic"
	"time"

	"github.com/wangzw/peers/internal/link"
)

// evConfigRead records a configuration read that failed. The delivery keeps
// running with the documented default for the key instead of failing the
// operation.
const evConfigRead = "config_read"

// linkAdapter exposes the part of *link.Link the delivery uses. Request,
// HandleRequest, Subscribe and Kick come from the embedded link; Established
// reads the link state once.
type linkAdapter struct{ *link.Link }

// Established reports whether the link to p is established.
func (a linkAdapter) Established(p peerID) bool {
	return a.State(p).Phase == link.PhaseEstablished
}

// topicVisibility adapts the broker's TopicTable to the delivery's visibility
// interface. The topic package is never imported here.
type topicVisibility struct{ t TopicTable }

// VisibleLocal reports whether two local Agents share a topic.
func (v topicVisibility) VisibleLocal(a, b string) bool {
	_, ok := v.t.Visible(AgentID(a), VisibleAgent{Local: AgentID(b)})
	return ok
}

// VisibleRemote reports whether a local Agent shares a topic with a member the
// peer reported.
func (v topicVisibility) VisibleRemote(local string, peer peerID, name, ref string) bool {
	_, ok := v.t.Visible(AgentID(local), VisibleAgent{
		Peer:   peer,
		Member: Member{Name: name, Ref: ref},
	})
	return ok
}

// RemoteMembers returns the peer's member entries in the current intersection.
func (v topicVisibility) RemoteMembers(peer peerID) []remoteMember {
	members := v.t.RemoteMembers(peer)
	out := make([]remoteMember, len(members))
	for i, m := range members {
		out[i] = remoteMember{Name: m.Name, Ref: m.Ref, PrevName: m.PrevName}
	}
	return out
}

// configSettings reads the send configuration through ConfigSource on every
// call, so a reload takes effect on the next operation. A failed read falls
// back to the documented default and is logged.
type configSettings struct {
	cfg ConfigSource
	log *slog.Logger
}

// Send returns the current send settings. Every key is read afresh; values are
// never cached across calls.
func (s configSettings) Send() sendSettings {
	return sendSettings{
		RequestTimeout: s.duration("send.request_timeout", 20*time.Second),
		LinkBackoff:    s.duration("send.link_backoff", 500*time.Millisecond),
		InboxBackoff:   s.duration("send.inbox_backoff", 200*time.Millisecond),
		InboxTimeout:   s.duration("send.inbox_timeout", 5*time.Second),
		DedupeWindow:   s.duration("send.dedupe_window", 5*time.Minute),
		LinkRetries:    s.integer("send.link_retries", 5),
		InboxRetries:   s.integer("send.inbox_retries", 2),
	}
}

// LocalAlias returns the effective broker.alias value. The empty string is
// returned when the read fails.
func (s configSettings) LocalAlias() string {
	v, err := s.cfg.Get("broker.alias")
	if err != nil {
		s.readFailed("broker.alias", err)
		return ""
	}
	return v.String
}

// Offline reports the current broker.offline value. It is never cached: the
// setting changes while the Broker runs. A failed read is treated as online
// and logged, the same way a failed duration read falls back to its default.
func (s configSettings) Offline() bool {
	v, err := s.cfg.Get("broker.offline")
	if err != nil {
		s.readFailed("broker.offline", err)
		return false
	}
	return v.Bool
}

// duration reads a duration key, falling back to def on error.
func (s configSettings) duration(key string, def time.Duration) time.Duration {
	v, err := s.cfg.Get(key)
	if err != nil {
		s.readFailed(key, err)
		return def
	}
	return v.Duration
}

// integer reads an integer key, falling back to def on error.
func (s configSettings) integer(key string, def int) int {
	v, err := s.cfg.Get(key)
	if err != nil {
		s.readFailed(key, err)
		return def
	}
	return int(v.Int)
}

// readFailed logs one failed configuration read.
func (s configSettings) readFailed(key string, err error) {
	logger := s.log
	if logger == nil {
		logger = slog.Default()
	}
	logger.Error(evConfigRead, "key", key, "error", err)
}

// agentTableAdapter adapts the Broker's agent table to localAgents. The Broker
// is attached when the delivery component starts; before that every call
// reports an empty table, and Reread reports errAgentGone. The pointer is
// atomic so a handler that arrives between the link listening and the component
// starting cannot race with the attachment.
type agentTableAdapter struct {
	broker atomic.Pointer[Broker]
}

// setBroker attaches the Broker the adapter reads from.
func (a *agentTableAdapter) setBroker(b *Broker) {
	a.broker.Store(b)
}

// Lookup returns the snapshot of the Agent with the given ID.
func (a *agentTableAdapter) Lookup(id string) (agentView, bool) {
	b := a.broker.Load()
	if b == nil {
		return agentView{}, false
	}
	s, ok := b.lookupAgent(AgentID(id))
	if !ok {
		return agentView{}, false
	}
	return viewOf(s), true
}

// All returns every local Agent's snapshot, sorted by ID.
func (a *agentTableAdapter) All() []agentView {
	b := a.broker.Load()
	if b == nil {
		return nil
	}
	agents := b.allAgents()
	out := make([]agentView, len(agents))
	for i, s := range agents {
		out[i] = viewOf(s)
	}
	return out
}

// Reread refreshes one Agent and returns the new snapshot. It reports
// errAgentGone exactly as the Broker does.
func (a *agentTableAdapter) Reread(id string) (agentView, error) {
	b := a.broker.Load()
	if b == nil {
		return agentView{}, errAgentGone
	}
	s, err := b.rereadAgent(AgentID(id))
	if err != nil {
		return agentView{}, err
	}
	return viewOf(s), nil
}

// viewOf converts an agent snapshot to the delivery's read-only view. A nil
// Inbox stays nil so writeInbox can report it.
func viewOf(s agentSnapshot) agentView {
	return agentView{
		ID:       string(s.ID),
		Name:     s.Name,
		Ref:      s.Ref,
		PrevName: s.PrevName,
		Session:  s.Session,
		Inbox:    s.Inbox,
	}
}

// deliveryAssembler builds the delivery from the link, topic and pairing state
// the earlier assemblers installed, registers the delivery request handler, and
// returns the component that attaches the Broker to the agent table adapter.
// Registration happens here, before Run binds the link listener, so no inbound
// request can arrive unhandled.
type deliveryAssembler struct{}

// newDeliveryAssembler returns the production assembler.
func newDeliveryAssembler() Assembler { return deliveryAssembler{} }

// Name is the assembler's name in startup errors.
func (deliveryAssembler) Name() string { return "delivery" }

// Assemble constructs the delivery and installs it in Deps.
func (deliveryAssembler) Assemble(_ context.Context, cfg ConfigSource, deps *Deps, asm *Assembly) ([]Component, error) {
	if asm.Link == nil || deps.Topics == nil || deps.Pairing == nil {
		return nil, errors.New("delivery needs the link, topic and pairing assemblers to run first")
	}

	agents := &agentTableAdapter{}
	d := newDelivery(deliveryDeps{
		Agents:   agents,
		Topics:   topicVisibility{deps.Topics},
		Peers:    deps.Pairing,
		Link:     linkAdapter{asm.Link},
		Settings: configSettings{cfg: cfg, log: deps.Logger},
		Clock:    time.Now,
		Log:      deps.Logger,
	})
	d.register()
	deps.Delivery = d
	return []Component{&deliveryComponent{agents: agents, inbound: d.inbound}}, nil
}

// deliveryComponent attaches the Broker to the delivery's agent table adapter
// when the Broker has been constructed.
type deliveryComponent struct {
	agents  *agentTableAdapter
	inbound *inboundLimiter
}

// Name is the component's name in startup errors.
func (*deliveryComponent) Name() string { return "delivery" }

// Start attaches the Broker. It runs before the link starts and before the
// local socket is served, so no delivery can observe a nil Broker.
func (c *deliveryComponent) Start(_ context.Context, b *Broker) error {
	if b == nil {
		// The wiring test starts the component without a Broker to check the
		// request handler is installed before the link listens.
		return nil
	}
	c.agents.setBroker(b)
	// A target Agent that leaves the table cannot receive another message;
	// dropping its buckets keeps the quota table from growing with every name
	// that ever existed.
	b.onUnregister(func(id AgentID) { c.inbound.dropTarget(string(id)) })
	return nil
}

// Stop releases nothing: the handler and subscriptions live as long as the
// process does.
func (*deliveryComponent) Stop() {}

var (
	_ deliveryLink   = linkAdapter{}
	_ visibility     = topicVisibility{}
	_ settingsSource = configSettings{}
	_ localAgents    = (*agentTableAdapter)(nil)
	_ peerTable      = PairingOps(nil)
	_ Assembler      = deliveryAssembler{}
	_ Component      = (*deliveryComponent)(nil)
	_ inboxWriter    = SessionBackend(nil)
)
