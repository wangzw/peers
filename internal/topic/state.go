package topic

import (
	"encoding/hex"
	"errors"
	"log/slog"
	"sync"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
	"google.golang.org/protobuf/proto"
)

type memberKey struct{ Name, Ref string }

type localAgent struct {
	info   Member
	topics map[string]struct{}
}
type localTable struct {
	agents map[AgentID]*localAgent
	topics map[string]map[AgentID]struct{} // only non-empty topics
}

// newLocalTable returns an empty table with both maps allocated.
func newLocalTable() *localTable {
	return &localTable{
		agents: map[AgentID]*localAgent{},
		topics: map[string]map[AgentID]struct{}{},
	}
}

type peerState struct {
	salt []byte
	self link.Fingerprint // this Broker, labelling what it sends
	peer link.Fingerprint // the other Broker, labelling what it sends

	send     map[string][32]byte // local topic -> digest this Broker sends
	recv     map[string][32]byte // local topic -> digest the peer would send
	recvName map[[32]byte]string // reverse of recv

	remote  map[[32]byte]struct{}          // peer's latest TopicDigests, peer direction
	shared  map[[32]byte]struct{}          // current intersection, peer direction
	rosters map[[32]byte]map[string]Member // peer's entries per peer-direction digest, keyed by Ref
}

// newPeerState copies salt and fills the two digests of every local topic:
// send under this Broker's fingerprint and recv under the peer's. remote,
// shared and rosters start empty (allocated).
func newPeerState(salt []byte, self, peer link.Fingerprint, localTopics []string) *peerState {
	ps := &peerState{
		salt:     append([]byte(nil), salt...),
		self:     self,
		peer:     peer,
		send:     make(map[string][32]byte, len(localTopics)),
		recv:     make(map[string][32]byte, len(localTopics)),
		recvName: make(map[[32]byte]string, len(localTopics)),
		remote:   map[[32]byte]struct{}{},
		shared:   map[[32]byte]struct{}{},
		rosters:  map[[32]byte]map[string]Member{},
	}
	for _, n := range localTopics {
		ps.indexLocal(n)
	}
	return ps
}

// indexLocal records both digests of a local topic and the reverse lookup of
// the peer direction, which is the one incoming frames are matched against.
func (ps *peerState) indexLocal(name string) {
	s := digest(ps.salt, ps.self, name)
	ps.send[name] = s
	r := digest(ps.salt, ps.peer, name)
	ps.recv[name] = r
	ps.recvName[r] = name
}

// sentDigest returns the digest this Broker sends for the local topic whose
// peer-direction digest is recv, along with the topic name. recv must be in the
// intersection.
func (ps *peerState) sentDigest(recv [32]byte) ([32]byte, string) {
	name := ps.recvName[recv]
	return ps.send[name], name
}

// Registry is the topic table. It holds the local agents and their topics and,
// for every established link, that peer's salt, digest sets and roster. One
// mutex guards both tables; queries take the read side and return copies.
type Registry struct {
	mu    sync.RWMutex
	t     Transport
	log   *slog.Logger
	self  link.Fingerprint
	local *localTable
	peers map[link.Fingerprint]*peerState
}

// sendLatestLocked encodes m and calls r.t.SendLatest, so a frame of the same
// kind still queued is replaced instead of queueing another. Callers hold
// r.mu.Lock.
func (r *Registry) sendLatestLocked(peer link.Fingerprint, kind uint32, m proto.Message) {
	f, err := encodeFrame(kind, m)
	if err != nil {
		r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", kind, "error", err)
		return
	}
	if err := r.t.SendLatest(peer, f); err != nil {
		r.logSendError(peer, f, err)
	}
}

// digestKey names the latest-slot of one digest. A Roster and a RosterDelta for
// the same digest share it, so the newer frame replaces the older instead of
// both waiting in the send queue. Callers hold r.mu.Lock.
func digestKey(d [32]byte) string { return "d:" + hex.EncodeToString(d[:]) }

// sendRosterLocked queues a full roster for digest d as the latest frame of d.
// A roster replaces the peer's entries, so replacing an earlier roster or delta
// still queued for d is safe. Callers hold r.mu.Lock.
func (r *Registry) sendRosterLocked(peer link.Fingerprint, d [32]byte, m *topicpb.Roster) {
	f, err := encodeFrame(topicpb.KindRoster, m)
	if err != nil {
		r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", topicpb.KindRoster, "error", err)
		return
	}
	r.deliverLatestKeyLocked(peer, digestKey(d), f)
}

// sendRosterForLocked sends the full roster of the local topic behind the
// peer-direction digest d. Callers hold r.mu.Lock.
func (r *Registry) sendRosterForLocked(peer link.Fingerprint, ps *peerState, d [32]byte) {
	send, name := ps.sentDigest(d)
	r.sendRosterLocked(peer, send, rosterMsg(send, r.local.members(name)))
}

// sendDeltaLocked queues a delta for digest d. If a frame for d is still
// queued, replacing it with a delta could drop changes the queued frame carries
// (another agent's change, or the snapshot a queued roster holds), so the
// current full roster for name is queued instead. That folds the unsent delta
// into a full roster and keeps one pending frame per digest. Callers hold
// r.mu.Lock.
func (r *Registry) sendDeltaLocked(peer link.Fingerprint, d [32]byte, name string, m *topicpb.RosterDelta) {
	f, err := encodeFrame(topicpb.KindRosterDelta, m)
	if err != nil {
		r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", topicpb.KindRosterDelta, "error", err)
		return
	}
	replaced, err := r.t.SendLatestKey(peer, digestKey(d), f)
	if err != nil {
		r.logSendError(peer, f, err)
		return
	}
	if !replaced {
		return
	}
	rf, err := encodeFrame(topicpb.KindRoster, rosterMsg(d, r.local.members(name)))
	if err != nil {
		r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", topicpb.KindRoster, "error", err)
		return
	}
	r.deliverLatestKeyLocked(peer, digestKey(d), rf)
}

// deliverLatestKeyLocked sends f as the latest frame of key. Callers hold
// r.mu.Lock.
func (r *Registry) deliverLatestKeyLocked(peer link.Fingerprint, key string, f link.Frame) {
	if _, err := r.t.SendLatestKey(peer, key, f); err != nil {
		r.logSendError(peer, f, err)
	}
}

// logSendError records a failed send. A frame above the link's message limit is
// logged at warn as topic.frame_too_large; every other failure stays at debug
// as topic.send_failed. Callers hold r.mu.Lock.
func (r *Registry) logSendError(peer link.Fingerprint, f link.Frame, err error) {
	if errors.Is(err, link.ErrFrameTooLarge) {
		r.log.Warn("topic.frame_too_large", "peer", peer.String(), "kind", frameKind(f), "size", len(f.Payload))
		return
	}
	r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", frameKind(f), "error", err)
}
