package broker

import (
	"errors"
	"fmt"

	"github.com/wangzw/peers/internal/link"
	"github.com/wangzw/peers/internal/topic"
)

type topicTable struct {
	r *topic.Registry
}

var _ TopicTable = topicTable{}

// NewTopicTable returns the TopicTable backed by r.
func NewTopicTable(r *topic.Registry) TopicTable { return topicTable{r} }

func (t topicTable) Join(agent AgentID, name string) error {
	return topicRegistryError(t.r.Join(topic.AgentID(agent), name))
}

func (t topicTable) Leave(agent AgentID, name string) error {
	return topicRegistryError(t.r.Leave(topic.AgentID(agent), name))
}

func (t topicTable) LeaveAll(agent AgentID) {
	t.r.LeaveAll(topic.AgentID(agent))
}

func (t topicTable) TopicsOf(agent AgentID) []string {
	return t.r.TopicsOf(topic.AgentID(agent))
}

func (t topicTable) AgentChanged(agent AgentID, m Member) {
	t.r.AgentChanged(topic.AgentID(agent), topic.Member{
		Name:        m.Name,
		Ref:         m.Ref,
		Status:      m.Status,
		PrevName:    m.PrevName,
		Account:     m.Account,
		RefRequired: m.RefRequired,
	})
}

func (t topicTable) Visible(local AgentID, other VisibleAgent) ([]string, bool) {
	ref := topic.AgentRef{ID: topic.AgentID(other.Local)}
	if other.Local == "" {
		ref = topic.AgentRef{Peer: link.Fingerprint(other.Peer), Ref: other.Member.Ref}
	}
	return t.r.Visible(topic.AgentID(local), ref)
}

func (t topicTable) VisibleAgents(local AgentID) []VisibleAgent {
	agents := t.r.VisibleAgents(topic.AgentID(local))
	if agents == nil {
		return nil
	}
	out := make([]VisibleAgent, 0, len(agents))
	for _, a := range agents {
		v := VisibleAgent{Member: memberFromTopic(a.Member), Shared: a.Shared}
		if a.Agent.Peer.IsZero() {
			v.Local = AgentID(a.Agent.ID)
		} else {
			v.Peer = [32]byte(a.Agent.Peer)
		}
		out = append(out, v)
	}
	return out
}

func (t topicTable) RemoteMembers(peer [32]byte) []Member {
	members := t.r.RemoteMembers(link.Fingerprint(peer))
	if members == nil {
		return nil
	}
	out := make([]Member, 0, len(members))
	for _, m := range members {
		out = append(out, memberFromTopic(m))
	}
	return out
}

func memberFromTopic(m topic.Member) Member {
	return Member{
		Name:        m.Name,
		Ref:         m.Ref,
		Status:      m.Status,
		PrevName:    m.PrevName,
		Account:     m.Account,
		RefRequired: m.RefRequired,
	}
}

func topicRegistryError(err error) error {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, topic.ErrInvalidTopic):
		return fmt.Errorf("%w: %v", ErrInvalidTopicName, err)
	case errors.Is(err, topic.ErrUnknownAgent):
		return fmt.Errorf("%w: %v", ErrUnknownAgent, err)
	case errors.Is(err, topic.ErrTopicLimit):
		return fmt.Errorf("%w: %v", ErrTopicQuotaExceeded, err)
	default:
		return err
	}
}
