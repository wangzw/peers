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

func newLocalTable() *localTable {
	return &localTable{
		agents: map[AgentID]*localAgent{},
		topics: map[string]map[AgentID]struct{}{},
	}
}

type peerState struct {
	salt []byte
	self link.Fingerprint // this Broker, used for the send digests
	peer link.Fingerprint // the other Broker, used for the recv digests

	send     map[string][32]byte // local topic -> digest this Broker sends
	recv     map[string][32]byte // local topic -> digest the peer would send
	recvName map[[32]byte]string // reverse of recv

	remote  map[[32]byte]struct{}          // peer's latest TopicDigests, recv direction
	shared  map[[32]byte]struct{}          // current intersection, recv direction
	rosters map[[32]byte]map[string]Member // peer's entries per recv digest, keyed by Ref
}

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

func (ps *peerState) indexLocal(name string) {
	s := digest(ps.salt, ps.self, name)
	ps.send[name] = s
	r := digest(ps.salt, ps.peer, name)
	ps.recv[name] = r
	ps.recvName[r] = name
}

// sentDigest requires recv to be in the intersection.
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

// Callers hold r.mu.Lock.
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
// the same digest share it, so the newer frame replaces the older. Callers hold
// r.mu.Lock.
func digestKey(d [32]byte) string { return "d:" + hex.EncodeToString(d[:]) }

// sendRosterLocked queues a full roster for digest d as the latest frame of d.
// A roster replaces the peer's entries, so replacing an earlier queued frame for
// d is safe. Callers hold r.mu.Lock.
func (r *Registry) sendRosterLocked(peer link.Fingerprint, d [32]byte, m *topicpb.Roster) {
	f, err := encodeFrame(topicpb.KindRoster, m)
	if err != nil {
		r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", topicpb.KindRoster, "error", err)
		return
	}
	r.deliverLatestKeyLocked(peer, digestKey(d), f)
}

// sendRosterForLocked sends the roster of the local topic behind recv digest d.
// Callers hold r.mu.Lock.
func (r *Registry) sendRosterForLocked(peer link.Fingerprint, ps *peerState, d [32]byte) {
	send, name := ps.sentDigest(d)
	r.sendRosterLocked(peer, send, rosterMsg(send, r.local.members(name)))
}

// sendDeltaLocked queues a delta for digest d, replacing a queued frame for d
// with the full current roster so no queued change is lost. Callers hold
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

// Callers hold r.mu.Lock.
func (r *Registry) deliverLatestKeyLocked(peer link.Fingerprint, key string, f link.Frame) {
	if _, err := r.t.SendLatestKey(peer, key, f); err != nil {
		r.logSendError(peer, f, err)
	}
}

// logSendError logs a failed send at warn as topic.frame_too_large when the
// frame exceeds the link's message limit, otherwise at debug as
// topic.send_failed. Callers hold r.mu.Lock.
func (r *Registry) logSendError(peer link.Fingerprint, f link.Frame, err error) {
	if errors.Is(err, link.ErrFrameTooLarge) {
		r.log.Warn("topic.frame_too_large", "peer", peer.String(), "kind", frameKind(f), "size", len(f.Payload))
		return
	}
	r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", frameKind(f), "error", err)
}
