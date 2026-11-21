package topic

import (
	"encoding/hex"
	"log/slog"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

const (
	// maxInboundDigests caps the digests in one TopicDigests frame.
	maxInboundDigests = 4096
	// maxInboundMembers caps each member list in a Roster or RosterDelta frame,
	// and the entries one digest may hold in total.
	maxInboundMembers = 4096
	// maxPeerMembers caps the entries one peer may hold across every shared
	// digest.
	maxPeerMembers = 4 * maxInboundMembers
)

// New registers topic's three frame handlers and the link subscription on t.
// It must be called before t starts listening or dialing.
func New(t Transport, opts Options) *Registry {
	if opts.Self.IsZero() {
		panic("topic: Options.Self is required")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	r := &Registry{
		t:     t,
		log:   logger,
		self:  opts.Self,
		local: newLocalTable(),
		peers: map[link.Fingerprint]*peerState{},
	}
	t.HandleState(link.FrameKind(topicpb.KindTopicDigests), r.handleDigests)
	t.HandleState(link.FrameKind(topicpb.KindRoster), r.handleRoster)
	t.HandleState(link.FrameKind(topicpb.KindRosterDelta), r.handleDelta)
	t.Subscribe(r.onUp, r.onDown)
	return r
}

// onUp replaces any state for peer with a table built from the new salt and
// sends the local digest set. The link always carries one TopicDigests frame,
// even when the local set is empty.
func (r *Registry) onUp(peer link.Fingerprint, salt []byte) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps := newPeerState(salt, r.self, peer, r.local.topicNames())
	r.peers[peer] = ps
	r.sendLatestLocked(peer, topicpb.KindTopicDigests, digestsMsg(ps.localDigests()))
}

// onDown discards everything learned about peer.
func (r *Registry) onDown(peer link.Fingerprint) {
	r.mu.Lock()
	defer r.mu.Unlock()
	delete(r.peers, peer)
}

// handleDigests replaces the peer's digest set and sends a Roster for every
// topic that entered the intersection.
func (r *Registry) handleDigests(peer link.Fingerprint, f link.Frame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps, ok := r.peers[peer]
	if !ok {
		r.frameIgnored(peer, f)
		return
	}
	var msg topicpb.TopicDigests
	if err := decodeFrame(f, &msg); err != nil {
		r.frameDropped(peer, f, "decode", nil)
		return
	}
	if len(msg.Digests) > maxInboundDigests {
		r.frameDropped(peer, f, "too_many_digests", nil)
		return
	}
	set, err := parseDigests(&msg)
	if err != nil {
		r.frameDropped(peer, f, "digest_length", nil)
		return
	}
	ps.setRemote(set)
	for _, d := range ps.recompute() {
		send, name := ps.sentDigest(d)
		r.sendRosterLocked(peer, send, rosterMsg(send, r.local.members(name)))
	}
}

// handleRoster replaces the peer's entries for one shared digest.
func (r *Registry) handleRoster(peer link.Fingerprint, f link.Frame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps, ok := r.peers[peer]
	if !ok {
		r.frameIgnored(peer, f)
		return
	}
	var msg topicpb.Roster
	if err := decodeFrame(f, &msg); err != nil {
		r.frameDropped(peer, f, "decode", nil)
		return
	}
	d, err := parseDigest(msg.Digest)
	if err != nil {
		r.frameDropped(peer, f, "unknown_digest", nil)
		return
	}
	if !ps.knowsRemote(d) {
		// A merged TopicDigests can overtake a roster frame queued before it,
		// so a digest outside the peer's latest set is a legal arrival order.
		r.frameIgnored(peer, f)
		return
	}
	if _, ok := ps.shared[d]; !ok {
		r.frameIgnored(peer, f)
		return
	}
	if len(msg.Members) > maxInboundMembers {
		r.frameDropped(peer, f, "too_many_members", &d)
		return
	}
	members := make([]Member, 0, len(msg.Members))
	dropped := 0
	for _, p := range msg.Members {
		m := memberFromWire(p)
		if !validMemberEntry(m) {
			dropped++
			continue
		}
		members = append(members, m)
	}
	if dropped > 0 {
		r.membersDropped(peer, f, &d, dropped)
	}
	entries := entriesForRoster(members)
	if len(entries) > maxInboundMembers {
		r.frameDropped(peer, f, "roster_full", &d)
		return
	}
	if ps.rosterTotalExcept(d)+len(entries) > maxPeerMembers {
		r.frameDropped(peer, f, "peer_roster_full", &d)
		return
	}
	ps.rosters[d] = entries
}

// handleDelta applies one incremental change under a shared digest.
func (r *Registry) handleDelta(peer link.Fingerprint, f link.Frame) {
	r.mu.Lock()
	defer r.mu.Unlock()
	ps, ok := r.peers[peer]
	if !ok {
		r.frameIgnored(peer, f)
		return
	}
	var msg topicpb.RosterDelta
	if err := decodeFrame(f, &msg); err != nil {
		r.frameDropped(peer, f, "decode", nil)
		return
	}
	d, err := parseDigest(msg.Digest)
	if err != nil {
		r.frameDropped(peer, f, "unknown_digest", nil)
		return
	}
	if !ps.knowsRemote(d) {
		// See handleRoster: a merged TopicDigests makes this a legal arrival
		// order, so it is ignored rather than logged as a protocol fault.
		r.frameIgnored(peer, f)
		return
	}
	if _, ok := ps.shared[d]; !ok {
		r.frameIgnored(peer, f)
		return
	}
	if len(msg.Added) > maxInboundMembers || len(msg.Changed) > maxInboundMembers || len(msg.Removed) > maxInboundMembers {
		r.frameDropped(peer, f, "too_many_members", &d)
		return
	}
	dropped := 0
	added := make([]Member, 0, len(msg.Added))
	for _, p := range msg.Added {
		m := memberFromWire(p)
		if !validMemberEntry(m) {
			dropped++
			continue
		}
		added = append(added, m)
	}
	changed := make([]Member, 0, len(msg.Changed))
	for _, p := range msg.Changed {
		m := memberFromWire(p)
		if !validMemberEntry(m) {
			dropped++
			continue
		}
		changed = append(changed, m)
	}
	removed := make([]memberKey, 0, len(msg.Removed))
	for _, k := range msg.Removed {
		if k == nil {
			continue
		}
		removed = append(removed, memberKey{Name: k.Name, Ref: k.Ref})
	}
	if dropped > 0 {
		r.membersDropped(peer, f, &d, dropped)
	}
	cur := ps.rosters[d]
	count := deltaResultCount(cur, added, removed, changed)
	if count > maxInboundMembers {
		r.frameDropped(peer, f, "roster_full", &d)
		return
	}
	if ps.rosterTotalExcept(d)+count > maxPeerMembers {
		r.frameDropped(peer, f, "peer_roster_full", &d)
		return
	}
	if cur == nil && count > 0 {
		cur = make(map[string]Member, count)
		ps.rosters[d] = cur
	}
	applyDeltaInPlace(cur, added, removed, changed)
}

// frameIgnored records an inbound frame that cannot be used: it arrived without
// link state, or it names a digest outside the peer's latest set or the current
// intersection. These are legal arrival orders, not peer faults.
func (r *Registry) frameIgnored(peer link.Fingerprint, f link.Frame) {
	r.log.Debug("topic.frame_ignored", "peer", peer.String(), "kind", frameKind(f))
}

// membersDropped records one inbound roster frame that carried entries rejected
// by validMemberEntry. It logs once per frame with the number dropped, and
// never the member name, previous name or Ref.
func (r *Registry) membersDropped(peer link.Fingerprint, f link.Frame, d *[32]byte, count int) {
	args := []any{"peer", peer.String(), "kind", frameKind(f), "reason", "invalid_member", "count", count}
	if d != nil {
		args = append(args, "digest", hex.EncodeToString(d[:8]))
	}
	r.log.Warn("topic.member_dropped", args...)
}

// frameDropped records an inbound frame that could not be applied. d, when not
// nil, is logged by its first 8 bytes; topic names and member data never are.
func (r *Registry) frameDropped(peer link.Fingerprint, f link.Frame, reason string, d *[32]byte) {
	args := []any{"peer", peer.String(), "kind", frameKind(f), "reason", reason}
	if d != nil {
		args = append(args, "digest", hex.EncodeToString(d[:8]))
	}
	r.log.Warn("topic.frame_dropped", args...)
}

// TopicsOf returns the agent's topics in ascending order, or nil for an unknown
// agent.
func (r *Registry) TopicsOf(agent AgentID) []string {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.local.topicsOf(agent)
}

// Visible reports the local topic names that local and other share, ascending.
// ok is false when they share none.
func (r *Registry) Visible(local AgentID, other AgentRef) (shared []string, ok bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	shared = visible(r.local, r.peers, local, other)
	return shared, len(shared) > 0
}

// VisibleAgents lists the agents visible to local.
func (r *Registry) VisibleAgents(local AgentID) []VisibleAgent {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return visibleAgents(r.local, r.peers, local)
}

// RemoteMembers returns the peer's agents under the shared digests.
func (r *Registry) RemoteMembers(peer link.Fingerprint) []Member {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return remoteMembers(r.peers[peer])
}
