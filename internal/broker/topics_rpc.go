package broker

import (
	"context"
	"encoding/hex"
	"errors"
	"sort"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/topic"
)

const invalidTopicNameMessage = "topic name must be 1 to 256 bytes of UTF-8 without control characters"

const maxAccountTopics = 128

type privateDirectory struct{}

func (privateDirectory) DeclarePublicTopic(string, string) error {
	return Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT,
		"this Broker is not public; set broker.public to true first")
}

func (privateDirectory) RetirePublicTopic(string) {}

func (privateDirectory) PublicTopics() []PublicTopic { return nil }

func (b *Broker) directory() PublicTopicDirectory {
	if b.deps.Directory != nil {
		return b.deps.Directory
	}
	return privateDirectory{}
}

func invalidTopicNameError() *Error {
	return Errorf(localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT, invalidTopicNameMessage)
}

func (b *Broker) checkTopicChange(a *agent, name string) *Error {
	if topic.ValidateName(name) != nil {
		return invalidTopicNameError()
	}
	if !b.topicRate.allow(a.caller.UID, b.deps.Clock.Now()) {
		return topicQuotaExceededError("Too many topic changes. Try again in a moment.")
	}
	return nil
}

func topicTableError(err error, id AgentID) error {
	switch {
	case errors.Is(err, ErrInvalidTopicName):
		return invalidTopicNameError()
	case errors.Is(err, ErrUnknownAgent):
		return Errorf(localpb.ErrorCode_ERROR_CODE_INTERNAL,
			"agent %s was not introduced to the topic table", id)
	case errors.Is(err, ErrTopicQuotaExceeded):
		return topicQuotaExceededError("This Broker has reached the limit on topics or on members of one topic.")
	default:
		return err
	}
}

// topicQuotaExceededError wraps a complete sentence for the model.
func topicQuotaExceededError(message string) *Error {
	return Errorf(localpb.ErrorCode_ERROR_CODE_QUOTA_EXCEEDED, "%s", message)
}

// overAccountTopicLimit reports whether joining name would exceed the account's
// topic limit. The caller must hold b.accountTopicMu; this method takes b.mu.
func (b *Broker) overAccountTopicLimit(a *agent, name string) bool {
	if !b.deps.System {
		return false
	}
	b.mu.Lock()
	topics := make(map[string]struct{})
	for _, other := range b.agents {
		if other.caller.UID != a.caller.UID {
			continue
		}
		for _, t := range b.deps.Topics.TopicsOf(other.id) {
			topics[t] = struct{}{}
		}
	}
	b.mu.Unlock()
	if _, ok := topics[name]; ok {
		return false
	}
	return len(topics) >= maxAccountTopics
}

// JoinTopic joins the calling agent to a topic, optionally declaring it public
// first; a failed join rolls back the declaration.
func (b *Broker) JoinTopic(ctx context.Context, req *localpb.JoinTopicRequest) (*localpb.JoinTopicResponse, error) {
	a, err := b.agentFor(ctx, req.GetAgentId())
	if err != nil {
		return nil, toStatus(err)
	}

	name := req.GetName()
	if err := b.checkTopicChange(a, name); err != nil {
		return nil, toStatus(err)
	}

	declared := false
	if req.GetIsPublic() {
		if err := b.directory().DeclarePublicTopic(name, req.GetDescription()); err != nil {
			return nil, toStatus(err)
		}
		declared = true
	}

	b.accountTopicMu.Lock()
	if b.overAccountTopicLimit(a, name) {
		b.accountTopicMu.Unlock()
		if declared {
			b.rollbackPublicDeclaration(a, name)
		}
		return nil, toStatus(topicQuotaExceededError("This account has reached the limit on topics."))
	}
	joinErr := b.deps.Topics.Join(a.id, name)
	b.accountTopicMu.Unlock()

	if joinErr != nil {
		if declared {
			b.rollbackPublicDeclaration(a, name)
		}
		return nil, toStatus(topicTableError(joinErr, a.id))
	}

	if req.GetIsPublic() && !b.recordPublicJoin(a, name, req.GetDescription()) {
		// The agent left the table while the join was in flight. Its
		// membership went away with LeaveAll; only the declaration remains.
		b.rollbackPublicDeclaration(a, name)
		return nil, toStatus(Errorf(localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED,
			"agent %q is no longer registered", a.id))
	}

	topics := b.deps.Topics.TopicsOf(a.id)
	sort.Strings(topics)
	return &localpb.JoinTopicResponse{Topics: topics}, nil
}

// LeaveTopic removes the calling agent from a topic and retires its public
// declaration when no other agent still holds the topic publicly.
func (b *Broker) LeaveTopic(ctx context.Context, req *localpb.LeaveTopicRequest) (*localpb.LeaveTopicResponse, error) {
	a, err := b.agentFor(ctx, req.GetAgentId())
	if err != nil {
		return nil, toStatus(err)
	}

	name := req.GetName()
	if err := b.checkTopicChange(a, name); err != nil {
		return nil, toStatus(err)
	}

	if err := b.deps.Topics.Leave(a.id, name); err != nil {
		return nil, toStatus(topicTableError(err, a.id))
	}

	b.mu.Lock()
	if _, ok := a.public[name]; ok {
		// Forget this agent's own public membership before the in-use check,
		// which otherwise counts the leaving agent as a holder.
		delete(a.public, name)
		b.retireOrphanedPublicTopicsLocked(a, []string{name})
	}
	b.mu.Unlock()

	topics := b.deps.Topics.TopicsOf(a.id)
	sort.Strings(topics)
	return &localpb.LeaveTopicResponse{Topics: topics}, nil
}

// ListTopics returns the topics the calling agent joined and the known public
// topics it has not joined, both sorted by name.
func (b *Broker) ListTopics(ctx context.Context, req *localpb.ListTopicsRequest) (*localpb.ListTopicsResponse, error) {
	a, err := b.agentFor(ctx, req.GetAgentId())
	if err != nil {
		return nil, toStatus(err)
	}

	joined := b.deps.Topics.TopicsOf(a.id)
	sort.Strings(joined)
	isJoined := make(map[string]bool, len(joined))
	for _, name := range joined {
		isJoined[name] = true
	}

	public := make([]*localpb.PublicTopic, 0)
	for _, p := range b.directory().PublicTopics() {
		if isJoined[p.Name] {
			continue
		}
		public = append(public, &localpb.PublicTopic{
			Name:                 p.Name,
			Description:          p.Description,
			PublisherFingerprint: hex.EncodeToString(p.Publisher[:]),
			PublisherAlias:       p.PublisherAlias,
			Declarers:            uint32(p.Declarers),
		})
	}
	sort.Slice(public, func(i, j int) bool { return public[i].GetName() < public[j].GetName() })

	return &localpb.ListTopicsResponse{
		JoinedTopics: joined,
		PublicTopics: public,
	}, nil
}

// recordPublicJoin remembers a successful public join and reports false when the
// agent has left the table, in which case the join must be rolled back.
func (b *Broker) recordPublicJoin(a *agent, name, description string) bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.agents[a.id] != a {
		return false
	}
	a.public[name] = description
	return true
}

func (b *Broker) rollbackPublicDeclaration(a *agent, name string) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.retireOrphanedPublicTopicsLocked(a, []string{name})
}
