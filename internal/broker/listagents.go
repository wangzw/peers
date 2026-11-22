package broker

import (
	"context"
	"fmt"
	"slices"
	"sort"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
)

// listAgentEntry is one candidate entry together with the fields the request
// filters compare against.
type listAgentEntry struct {
	entry *localpb.AgentEntry
	alias string // the broker alias the address carries
	name  string // normalized agent name
}

// ListAgents answers a list_agents request: the caller's own address plus every
// agent visible to it through a shared topic, local and remote. The caller and
// the local visible agents are reread first so that the listing carries current
// addresses, statuses and previous names. A reread may unregister agents, so
// the visible set is taken again after it.
func (b *Broker) ListAgents(ctx context.Context, req *localpb.ListAgentsRequest) (*localpb.ListAgentsResponse, error) {
	self, err := b.agentFor(ctx, req.GetAgentId())
	if err != nil {
		return nil, toStatus(err)
	}
	selfID := self.id

	// Reread the caller first, then each local agent visible to it.
	vis := b.deps.Topics.VisibleAgents(selfID)
	ids := make([]AgentID, 0, len(vis)+1)
	ids = append(ids, selfID)
	for _, v := range vis {
		if v.Local != "" && v.Local != selfID {
			ids = append(ids, v.Local)
		}
	}
	b.refreshAgents(ids)

	// A reread may have unregistered the caller or one of the visible agents.
	// A caller that is gone may not use the interface any more.
	if _, err := b.agentFor(ctx, req.GetAgentId()); err != nil {
		return nil, toStatus(err)
	}
	vis = b.deps.Topics.VisibleAgents(selfID)

	b.mu.Lock()
	defer b.mu.Unlock()

	// A listing must not silently take a name from one account and give it to
	// another. Expire idle entries here and, when ownership moved, recompute the
	// refs and push agent_changed to every registration stream and the roster,
	// the same way the hourly sweep does.
	if b.nameOwnerPushPending || b.expireNameOwnersLocked(b.deps.Clock.Now()) {
		b.recomputeRefsLocked()
		b.nameOwnerPushPending = false
	}

	selfAgent, ok := b.agents[selfID]
	if !ok {
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED,
			"agent %q is not registered for this account", req.GetAgentId()))
	}

	items := make([]listAgentEntry, 0, len(vis))
	for _, v := range vis {
		if v.Local != "" {
			if v.Local == selfID {
				continue
			}
			// The agent table, not the roster entry, is authoritative: the
			// roster entry may lag behind the reread just performed.
			ag, ok := b.agents[v.Local]
			if !ok {
				continue
			}
			items = append(items, listAgentEntry{
				entry: &localpb.AgentEntry{
					Address:      b.addressOf(ag),
					SharedTopics: append([]string(nil), v.Shared...),
					Status:       statusToProto(ag.status),
					PreviousName: ag.prevName,
					Account:      callerAccount(ag.caller),
				},
				alias: b.localAlias(),
				name:  ag.name,
			})
			continue
		}

		// A remote agent is listed only when its peer's alias is known; an
		// entry without an address is not useful.
		if b.deps.Peers == nil {
			continue
		}
		alias, ok := b.deps.Peers.EffectiveAlias(v.Peer)
		if !ok {
			continue
		}
		// The ref comes from the peer, so it is untrusted: FormatAddress panics
		// on a malformed one. Skip the entry and warn instead of letting the
		// peer's data decide whether this process survives.
		if !validRef(v.Member.Ref) {
			b.log.Warn(EvRemoteMemberInvalid, "peer", fmt.Sprintf("%x", v.Peer))
			continue
		}
		// A member whose publisher requires a ref always shows one. Other
		// members of the same name count only among themselves, so a
		// ref-required collider does not add a ref to the owner's address.
		plainDuplicates := 0
		for _, m := range b.deps.Topics.RemoteMembers(v.Peer) {
			if m.Name == v.Member.Name && !m.RefRequired {
				plainDuplicates++
			}
		}
		needRef := v.Member.RefRequired || plainDuplicates > 1
		items = append(items, listAgentEntry{
			entry: &localpb.AgentEntry{
				Address:      FormatAddress(v.Member.Name, v.Member.Ref, alias, needRef),
				SharedTopics: append([]string(nil), v.Shared...),
				Status:       statusToProto(v.Member.Status),
				PreviousName: v.Member.PrevName,
				Account:      v.Member.Account,
			},
			alias: alias,
			name:  inbox.NormalizeName(v.Member.Name),
		})
	}

	// The name filter compares normalized names. An empty request field means
	// no filter; a field that normalizes to the empty string is still a filter
	// and matches no agent, whose names are never empty.
	nameFilter := req.GetName() != ""
	filterName := ""
	if nameFilter {
		filterName = inbox.NormalizeName(req.GetName())
	}
	agents := make([]*localpb.AgentEntry, 0, len(items))
	for _, it := range items {
		if topic := req.GetTopic(); topic != "" && !slices.Contains(it.entry.SharedTopics, topic) {
			continue
		}
		if brokerAlias := req.GetBroker(); brokerAlias != "" && it.alias != brokerAlias {
			continue
		}
		if nameFilter && it.name != filterName {
			continue
		}
		agents = append(agents, it.entry)
	}
	sort.Slice(agents, func(i, j int) bool { return agents[i].Address < agents[j].Address })

	return &localpb.ListAgentsResponse{
		SelfAddress: b.addressOf(selfAgent),
		Agents:      agents,
	}, nil
}
