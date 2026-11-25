package topic

func (ps *peerState) addLocal(name string) {
	ps.indexLocal(name)
}

// The caller must recompute the intersection.
func (ps *peerState) removeLocal(name string) {
	if _, ok := ps.send[name]; !ok {
		return
	}
	delete(ps.send, name)
	r := ps.recv[name]
	delete(ps.recv, name)
	delete(ps.recvName, r)
}

func (ps *peerState) localDigests() [][32]byte {
	out := make([][32]byte, 0, len(ps.send))
	for _, d := range ps.send {
		out = append(out, d)
	}
	sortByBytes(out)
	return out
}

func (ps *peerState) setRemote(set map[[32]byte]struct{}) {
	remote := make(map[[32]byte]struct{}, len(set))
	for d := range set {
		remote[d] = struct{}{}
	}
	ps.remote = remote
}

func (ps *peerState) knowsRemote(d [32]byte) bool {
	_, ok := ps.remote[d]
	return ok
}

// A digest that leaves the intersection must lose its roster, or entries that
// went stale while it was outside come back when it re-enters.
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
	sortByBytes(entered)
	return entered
}

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

// Removals apply before additions and changes, and only when both the Ref and
// the name match.
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

func (ps *peerState) rosterTotalExcept(exclude [32]byte) int {
	n := 0
	for d, entries := range ps.rosters {
		if d != exclude {
			n += len(entries)
		}
	}
	return n
}

// applyRoster replaces the peer's roster under d. It bypasses the inbound frame
// limits and is for tests only; production code uses handleRoster.
func (ps *peerState) applyRoster(d [32]byte, ms []Member) {
	ps.rosters[d] = entriesForRoster(ms)
}

// applyDelta applies a delta to the peer's roster under d. It bypasses the
// inbound frame limits and is for tests only; production code uses handleDelta.
func (ps *peerState) applyDelta(d [32]byte, added []Member, removed []memberKey, changed []Member) {
	cur := ps.rosters[d]
	if cur == nil {
		cur = map[string]Member{}
		ps.rosters[d] = cur
	}
	applyDeltaInPlace(cur, added, removed, changed)
}
