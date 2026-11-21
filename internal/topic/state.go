package topic

import (
	"log/slog"
	"sync"

	"github.com/wangzw/peers/internal/link"
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

// sendLocked encodes m and calls r.t.Send; on error it logs topic.send_failed
// at debug level (fields peer, kind, error) and returns. Callers hold r.mu.Lock.
func (r *Registry) sendLocked(peer link.Fingerprint, kind uint32, m proto.Message) {
	f, err := encodeFrame(kind, m)
	if err != nil {
		r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", kind, "error", err)
		return
	}
	if err := r.t.Send(peer, f); err != nil {
		r.log.Debug("topic.send_failed", "peer", peer.String(), "kind", kind, "error", err)
	}
}
