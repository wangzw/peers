package topic

import "sort"

const (
	maxLocalTopics       = 512
	maxLocalTopicMembers = maxInboundMembers
)

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

func (lt *localTable) info(id AgentID) (Member, bool) {
	a := lt.agents[id]
	if a == nil {
		return Member{}, false
	}
	return a.info, true
}

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

func (lt *localTable) topicsOf(id AgentID) []string {
	a := lt.agents[id]
	if a == nil {
		return nil
	}
	return sortedKeys(a.topics)
}

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

func (lt *localTable) topicNames() []string {
	return sortedKeys(lt.topics)
}

func sortedKeys[V any](m map[string]V) []string {
	names := make([]string, 0, len(m))
	for name := range m {
		names = append(names, name)
	}
	sort.Strings(names)
	return names
}

func (lt *localTable) inTopic(id AgentID, name string) bool {
	a := lt.agents[id]
	if a == nil {
		return false
	}
	_, ok := a.topics[name]
	return ok
}
