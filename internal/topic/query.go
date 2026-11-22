package topic

import (
	"sort"

	"github.com/wangzw/peers/internal/link"
)

// visible reports the local topic names that local and other share, ascending.
// A zero Peer means other is an agent on this Broker named by ID; otherwise
// Peer selects the link and Ref the agent's entry in that peer's roster.
// It returns nil when they share no topic, or when local is unknown or in no
// topic.
func visible(lt *localTable, peers map[link.Fingerprint]*peerState, local AgentID, other AgentRef) []string {
	if lt == nil {
		return nil
	}
	la, ok := lt.agents[local]
	if !ok || len(la.topics) == 0 {
		return nil
	}

	var shared []string
	if other.Peer.IsZero() {
		oa, ok := lt.agents[other.ID]
		if !ok {
			return nil
		}
		shared = sharedTopicNames(querySortedTopics(la.topics), oa.topics)
	} else {
		ps, ok := peers[other.Peer]
		if !ok {
			return nil
		}
		for _, name := range querySortedTopics(la.topics) {
			if _, ok := sharedMember(ps, name, other.Ref); ok {
				shared = append(shared, name)
			}
		}
	}
	if len(shared) == 0 {
		return nil
	}
	return shared
}

// visibleAgents lists the agents visible to local. Local agents come first,
// ordered by ID; remote agents follow, ordered by peer fingerprint and then by
// Ref. A remote agent in several shared topics appears once, with Shared merged
// and the entry taken from the first shared topic by local name.
func visibleAgents(lt *localTable, peers map[link.Fingerprint]*peerState, local AgentID) []VisibleAgent {
	if lt == nil {
		return nil
	}
	la, ok := lt.agents[local]
	if !ok || len(la.topics) == 0 {
		return nil
	}

	var out []VisibleAgent

	localTopics := querySortedTopics(la.topics)

	ids := make([]AgentID, 0, len(lt.agents))
	for id := range lt.agents {
		if id != local {
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		shared := sharedTopicNames(localTopics, lt.agents[id].topics)
		if len(shared) == 0 {
			continue
		}
		out = append(out, VisibleAgent{
			Agent:  AgentRef{ID: id},
			Member: lt.agents[id].info,
			Shared: shared,
		})
	}

	fps := make([]link.Fingerprint, 0, len(peers))
	for fp := range peers {
		fps = append(fps, fp)
	}
	sortByBytes(fps)

	for _, fp := range fps {
		ps := peers[fp]
		type remoteEntry struct {
			shared []string
			member Member
		}
		byRef := map[string]*remoteEntry{}
		for _, name := range localTopics {
			d, ok := ps.recv[name]
			if !ok {
				continue
			}
			if _, ok := ps.shared[d]; !ok {
				continue
			}
			for ref, member := range ps.rosters[d] {
				if ref == "" {
					continue
				}
				e, ok := byRef[ref]
				if !ok {
					e = &remoteEntry{member: member}
					byRef[ref] = e
				}
				e.shared = append(e.shared, name)
			}
		}

		refs := make([]string, 0, len(byRef))
		for ref := range byRef {
			refs = append(refs, ref)
		}
		sort.Strings(refs)
		for _, ref := range refs {
			e := byRef[ref]
			out = append(out, VisibleAgent{
				Agent:  AgentRef{Peer: fp, Ref: ref},
				Member: e.member,
				Shared: e.shared,
			})
		}
	}
	return out
}

// remoteMembers returns the peer's agents under the shared digests, deduplicated
// by Ref and ordered by Ref. When a Ref appears under several shared topics the
// entry comes from the first topic by local name. It returns nil when there is
// no link state or no shared entry.
func remoteMembers(ps *peerState) []Member {
	if ps == nil {
		return nil
	}
	first := map[string]Member{}
	for _, name := range sharedNames(ps) {
		d, ok := ps.recv[name]
		if !ok {
			continue
		}
		for ref, member := range ps.rosters[d] {
			if ref == "" {
				continue
			}
			if _, ok := first[ref]; !ok {
				first[ref] = member
			}
		}
	}
	if len(first) == 0 {
		return nil
	}
	refs := make([]string, 0, len(first))
	for ref := range first {
		refs = append(refs, ref)
	}
	sort.Strings(refs)
	out := make([]Member, 0, len(refs))
	for _, ref := range refs {
		out = append(out, first[ref])
	}
	return out
}

// sharedMember returns the entry with Ref == ref under the digest of the local
// topic name, when that digest exists and is in the shared set.
func sharedMember(ps *peerState, name, ref string) (Member, bool) {
	if ps == nil || ref == "" {
		return Member{}, false
	}
	d, ok := ps.recv[name]
	if !ok {
		return Member{}, false
	}
	if _, ok := ps.shared[d]; !ok {
		return Member{}, false
	}
	member, ok := ps.rosters[d][ref]
	return member, ok
}

// sharedNames returns the local topic names behind the shared digests,
// ascending.
func sharedNames(ps *peerState) []string {
	if ps == nil {
		return nil
	}
	names := make([]string, 0, len(ps.shared))
	for d := range ps.shared {
		names = append(names, ps.recvName[d])
	}
	sort.Strings(names)
	return names
}

func querySortedTopics(topics map[string]struct{}) []string {
	return sortedKeys(topics)
}

// sharedTopicNames returns the names in localTopics that other also has.
// localTopics is ascending, so the result is too.
func sharedTopicNames(localTopics []string, other map[string]struct{}) []string {
	var shared []string
	for _, name := range localTopics {
		if _, ok := other[name]; ok {
			shared = append(shared, name)
		}
	}
	return shared
}
