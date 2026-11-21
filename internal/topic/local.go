package topic

import "sort"

const (
	// maxLocalTopics caps the topics that have at least one local member. It is
	// smaller than maxInboundDigests so that the TopicDigests one Broker sends
	// stays well inside what a peer accepts and the work one membership change
	// fans out to the links stays bounded. A system Broker splits the same
	// budget per account; see the broker.
	maxLocalTopics = 512
	// maxLocalTopicMembers caps the local members of one topic.
	maxLocalTopicMembers = maxInboundMembers
)

// setInfo stores m as the agent's information, creating the agent when it does
// not exist yet, and returns the previous information and whether the agent
// already existed.
func (lt *localTable) setInfo(id AgentID, m Member) (old Member, existed bool) {
	a := lt.agents[id]
	if a == nil {
		lt.agents[id] = &localAgent{info: m, topics: map[string]struct{}{}}
		return Member{}, false
	}
	old = a.info
	a.info = m
	return old, true
}

// info returns the agent's information.
func (lt *localTable) info(id AgentID) (Member, bool) {
	a := lt.agents[id]
	if a == nil {
		return Member{}, false
	}
	return a.info, true
}

// join adds id to the topic name. It returns ErrUnknownAgent when setInfo never
// ran for id. joined reports whether the membership changed; created reports
// whether the topic went from empty to non-empty.
func (lt *localTable) join(id AgentID, name string) (joined, created bool, err error) {
	a := lt.agents[id]
	if a == nil {
		return false, false, ErrUnknownAgent
	}
	if _, ok := a.topics[name]; ok {
		return false, false, nil
	}
	set := lt.topics[name]
	if set == nil {
		if len(lt.topics) >= maxLocalTopics {
			return false, false, ErrTopicLimit
		}
		set = map[AgentID]struct{}{}
		lt.topics[name] = set
		created = true
	} else if len(set) >= maxLocalTopicMembers {
		return false, false, ErrTopicLimit
	}
	a.topics[name] = struct{}{}
	set[id] = struct{}{}
	return true, created, nil
}

// leave removes id from the topic name. left reports whether the membership
// changed; emptied reports whether the topic has no members left, in which case
// it is removed from the table.
func (lt *localTable) leave(id AgentID, name string) (left, emptied bool) {
	a := lt.agents[id]
	if a == nil {
		return false, false
	}
	if _, ok := a.topics[name]; !ok {
		return false, false
	}
	delete(a.topics, name)
	set := lt.topics[name]
	delete(set, id)
	if len(set) == 0 {
		delete(lt.topics, name)
		return true, true
	}
	return true, false
}

// leaveAll removes every membership and the agent itself. It returns the
// agent's information, the topics that became empty and the topics that still
// have other members, both ascending, and whether the agent existed.
func (lt *localTable) leaveAll(id AgentID) (info Member, emptied, remaining []string, existed bool) {
	a := lt.agents[id]
	if a == nil {
		return Member{}, nil, nil, false
	}
	info = a.info
	for name := range a.topics {
		set := lt.topics[name]
		delete(set, id)
		if len(set) == 0 {
			delete(lt.topics, name)
			emptied = append(emptied, name)
		} else {
			remaining = append(remaining, name)
		}
	}
	delete(lt.agents, id)
	sort.Strings(emptied)
	sort.Strings(remaining)
	return info, emptied, remaining, true
}

// topicsOf returns the agent's topics in ascending order, or nil for an unknown
// agent.
func (lt *localTable) topicsOf(id AgentID) []string {
	a := lt.agents[id]
	if a == nil {
		return nil
	}
	names := make([]string, 0, len(a.topics))
	for name := range a.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// members returns the information of every agent in the topic name, ascending
// by Ref.
func (lt *localTable) members(name string) []Member {
	set := lt.topics[name]
	if len(set) == 0 {
		return nil
	}
	out := make([]Member, 0, len(set))
	for id := range set {
		if a := lt.agents[id]; a != nil {
			out = append(out, a.info)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Ref < out[j].Ref })
	return out
}

// topicNames returns every topic that has members, ascending.
func (lt *localTable) topicNames() []string {
	names := make([]string, 0, len(lt.topics))
	for name := range lt.topics {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

// inTopic reports whether id is a member of the topic name.
func (lt *localTable) inTopic(id AgentID, name string) bool {
	a := lt.agents[id]
	if a == nil {
		return false
	}
	_, ok := a.topics[name]
	return ok
}
