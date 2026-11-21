package topic

import (
	"bytes"
	"sort"
)

// addLocal adds a local topic and both of its digests under this link's salt.
func (ps *peerState) addLocal(name string) {
	ps.indexLocal(name)
}

// removeLocal removes a local topic and both of its digests. The intersection
// is not recomputed here; the caller does that.
func (ps *peerState) removeLocal(name string) {
	if _, ok := ps.send[name]; !ok {
		return
	}
	delete(ps.send, name)
	r := ps.recv[name]
	delete(ps.recv, name)
	delete(ps.recvName, r)
}

// localDigests returns the digests this Broker sends for every local topic,
// ascending.
func (ps *peerState) localDigests() [][32]byte {
	out := make([][32]byte, 0, len(ps.send))
	for _, d := range ps.send {
		out = append(out, d)
	}
	sort.Slice(out, func(i, j int) bool {
		return bytes.Compare(out[i][:], out[j][:]) < 0
	})
	return out
}

// setRemote replaces the peer's digest set with a copy of set.
func (ps *peerState) setRemote(set map[[32]byte]struct{}) {
	remote := make(map[[32]byte]struct{}, len(set))
	for d := range set {
		remote[d] = struct{}{}
	}
	ps.remote = remote
}

// knowsRemote reports whether d is in the peer's latest digest set.
func (ps *peerState) knowsRemote(d [32]byte) bool {
	_, ok := ps.remote[d]
	return ok
}

// recompute replaces the intersection with the peer-direction digests that are
// both local and in the peer's set, and returns the digests that entered it,
// ascending. A digest that leaves the intersection loses its roster: entries
// that went stale while the topic was outside the intersection must not come
// back when it re-enters, so the peer has to send a new Roster.
func (ps *peerState) recompute() (entered [][32]byte) {
	shared := make(map[[32]byte]struct{}, len(ps.recv))
	for _, d := range ps.recv {
		if _, ok := ps.remote[d]; ok {
			shared[d] = struct{}{}
		}
	}
	for d := range ps.shared {
		if _, ok := shared[d]; !ok {
			delete(ps.rosters, d)
		}
	}
	for d := range shared {
		if _, ok := ps.shared[d]; !ok {
			entered = append(entered, d)
		}
	}
	ps.shared = shared
	sort.Slice(entered, func(i, j int) bool {
		return bytes.Compare(entered[i][:], entered[j][:]) < 0
	})
	return entered
}

// sharedDigest returns the digest this Broker sends for a local topic when the
// topic is in the intersection.
func (ps *peerState) sharedDigest(name string) ([32]byte, bool) {
	recv, ok := ps.recv[name]
	if !ok {
		return [32]byte{}, false
	}
	if _, ok := ps.shared[recv]; !ok {
		return [32]byte{}, false
	}
	return ps.send[name], true
}

// entriesForRoster returns the table a Roster frame would set under a digest.
// Entries with an empty Ref are skipped, and a later entry with the same Ref
// overwrites an earlier one.
func entriesForRoster(ms []Member) map[string]Member {
	entries := make(map[string]Member, len(ms))
	for _, m := range ms {
		if m.Ref == "" {
			continue
		}
		entries[m.Ref] = m
	}
	return entries
}

// deltaResultCount returns the number of entries the table under a digest would
// hold after applying added, removed and changed, without changing cur. It
// visits only the entries the delta names, never the whole table.
func deltaResultCount(cur map[string]Member, added []Member, removed []memberKey, changed []Member) int {
	n := len(cur)
	removedRefs := make(map[string]struct{}, len(removed))
	for _, k := range removed {
		if _, done := removedRefs[k.Ref]; done {
			continue
		}
		if m, ok := cur[k.Ref]; ok && m.Name == k.Name {
			removedRefs[k.Ref] = struct{}{}
			n--
		}
	}
	written := make(map[string]struct{}, len(added)+len(changed))
	write := func(m Member) {
		if m.Ref == "" {
			return
		}
		if _, done := written[m.Ref]; done {
			return
		}
		written[m.Ref] = struct{}{}
		if _, removed := removedRefs[m.Ref]; removed {
			return
		}
		if _, ok := cur[m.Ref]; ok {
			return
		}
		n++
	}
	for _, m := range added {
		write(m)
	}
	for _, m := range changed {
		write(m)
	}
	return n
}

// applyDeltaInPlace applies added, removed and changed to cur. Removals apply
// first and only take effect when both the Ref and the name match; added and
// changed entries are written by Ref, overwriting or creating the entry, and an
// empty Ref is skipped.
func applyDeltaInPlace(cur map[string]Member, added []Member, removed []memberKey, changed []Member) {
	for _, k := range removed {
		if m, ok := cur[k.Ref]; ok && m.Name == k.Name {
			delete(cur, k.Ref)
		}
	}
	for _, m := range added {
		if m.Ref != "" {
			cur[m.Ref] = m
		}
	}
	for _, m := range changed {
		if m.Ref != "" {
			cur[m.Ref] = m
		}
	}
}

// rosterTotalExcept returns the number of entries the peer holds across every
// digest other than exclude.
func (ps *peerState) rosterTotalExcept(exclude [32]byte) int {
	n := 0
	for d, entries := range ps.rosters {
		if d != exclude {
			n += len(entries)
		}
	}
	return n
}

// applyRoster replaces the peer's entries under d. It is used by tests that do
// not go through the inbound frame limits.
func (ps *peerState) applyRoster(d [32]byte, ms []Member) {
	ps.rosters[d] = entriesForRoster(ms)
}

// applyDelta applies one incremental change under d. It is used by tests that
// do not go through the inbound frame limits.
func (ps *peerState) applyDelta(d [32]byte, added []Member, removed []memberKey, changed []Member) {
	cur := ps.rosters[d]
	if cur == nil {
		cur = map[string]Member{}
		ps.rosters[d] = cur
	}
	applyDeltaInPlace(cur, added, removed, changed)
}
