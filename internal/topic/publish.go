package topic

import (
	"bytes"
	"sort"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic/topicpb"
)

// publishSortedPeers returns the fingerprints that have link state, ascending
// by their bytes, so frames leave this Broker in a deterministic order.
func publishSortedPeers(peers map[link.Fingerprint]*peerState) []link.Fingerprint {
	out := make([]link.Fingerprint, 0, len(peers))
	for p := range peers {
		out = append(out, p)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out
}

// Join adds agent to the topic name. It returns ErrInvalidTopic for a name that
// ValidateName rejects, and ErrUnknownAgent when AgentChanged never introduced
// the agent. The whole event, including the frames it produces, happens under
// the registry lock.
func (r *Registry) Join(agent AgentID, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ValidateName(name) != nil {
		return ErrInvalidTopic
	}
	joined, created, err := r.local.join(agent, name)
	if err != nil {
		return err
	}
	if !joined {
		return nil
	}
	info, _ := r.local.info(agent)

	for _, peer := range publishSortedPeers(r.peers) {
		ps := r.peers[peer]
		if created {
			ps.addLocal(name)
			r.sendLatestLocked(peer, topicpb.KindTopicDigests, digestsMsg(ps.localDigests()))
			for _, d := range ps.recompute() {
				send, topicName := ps.sentDigest(d)
				r.sendRosterLocked(peer, send, rosterMsg(send, r.local.members(topicName)))
			}
			continue
		}
		d, ok := ps.sharedDigest(name)
		if !ok {
			continue
		}
		r.sendDeltaLocked(peer, d, name, deltaMsg(d, []Member{info}, nil, nil))
	}
	return nil
}

// Leave removes agent from the topic name. It returns ErrInvalidTopic for a
// name that ValidateName rejects; an agent that is not a member succeeds
// without producing frames.
func (r *Registry) Leave(agent AgentID, name string) error {
	r.mu.Lock()
	defer r.mu.Unlock()

	if ValidateName(name) != nil {
		return ErrInvalidTopic
	}
	left, emptied := r.local.leave(agent, name)
	if !left {
		return nil
	}
	info, _ := r.local.info(agent)

	for _, peer := range publishSortedPeers(r.peers) {
		ps := r.peers[peer]
		if emptied {
			ps.removeLocal(name)
			r.sendLatestLocked(peer, topicpb.KindTopicDigests, digestsMsg(ps.localDigests()))
			ps.recompute()
			continue
		}
		d, ok := ps.sharedDigest(name)
		if !ok {
			continue
		}
		r.sendDeltaLocked(peer, d, name, deltaMsg(d, nil, []memberKey{{Name: info.Name, Ref: info.Ref}}, nil))
	}
	return nil
}

// LeaveAll removes every membership and the agent's information. It produces
// one TopicDigests per peer carrying the topics that became empty, then a
// RosterDelta for each remaining topic inside that peer's intersection.
func (r *Registry) LeaveAll(agent AgentID) {
	r.mu.Lock()
	defer r.mu.Unlock()

	info, emptied, remaining, existed := r.local.leaveAll(agent)
	if !existed {
		return
	}

	type removal struct {
		d    [32]byte
		name string
		key  memberKey
	}

	for _, peer := range publishSortedPeers(r.peers) {
		ps := r.peers[peer]
		if len(emptied) > 0 {
			for _, name := range emptied {
				ps.removeLocal(name)
			}
			r.sendLatestLocked(peer, topicpb.KindTopicDigests, digestsMsg(ps.localDigests()))
			ps.recompute()
		}

		removals := make([]removal, 0, len(remaining))
		for _, name := range remaining {
			d, ok := ps.sharedDigest(name)
			if !ok {
				continue
			}
			removals = append(removals, removal{d: d, name: name, key: memberKey{Name: info.Name, Ref: info.Ref}})
		}
		sort.Slice(removals, func(i, j int) bool {
			return bytes.Compare(removals[i].d[:], removals[j].d[:]) < 0
		})
		for _, rem := range removals {
			r.sendDeltaLocked(peer, rem.d, rem.name, deltaMsg(rem.d, nil, []memberKey{rem.key}, nil))
		}
	}
}

// AgentChanged stores the agent's new information, which also introduces an
// agent the registry has not seen before. It pushes a RosterDelta for every
// topic the agent belongs to inside a peer's intersection: a rename or a Ref
// change sends the old key as removed and the new entry as added, while any
// other entry change, including the status, previous name, account and
// ref_required fields, is a change. When nothing differs it sends no frame.
func (r *Registry) AgentChanged(agent AgentID, m Member) {
	r.mu.Lock()
	defer r.mu.Unlock()

	m.Status = statusFromWire(statusToWire(m.Status))
	old, existed := r.local.setInfo(agent, m)
	if !existed {
		return
	}
	topics := r.local.topicsOf(agent)
	if len(topics) == 0 || old == m {
		return
	}

	nameOrRefChanged := old.Name != m.Name || old.Ref != m.Ref

	type delta struct {
		d       [32]byte
		name    string
		added   []Member
		removed []memberKey
		changed []Member
	}

	for _, peer := range publishSortedPeers(r.peers) {
		ps := r.peers[peer]
		deltas := make([]delta, 0, len(topics))
		for _, name := range topics {
			d, ok := ps.sharedDigest(name)
			if !ok {
				continue
			}
			if nameOrRefChanged {
				deltas = append(deltas, delta{
					d:       d,
					name:    name,
					added:   []Member{m},
					removed: []memberKey{{Name: old.Name, Ref: old.Ref}},
				})
			} else {
				deltas = append(deltas, delta{d: d, name: name, changed: []Member{m}})
			}
		}
		sort.Slice(deltas, func(i, j int) bool {
			return bytes.Compare(deltas[i].d[:], deltas[j].d[:]) < 0
		})
		for _, del := range deltas {
			r.sendDeltaLocked(peer, del.d, del.name, deltaMsg(del.d, del.added, del.removed, del.changed))
		}
	}
}
