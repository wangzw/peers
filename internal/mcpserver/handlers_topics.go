package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/wangzw/peers/internal/broker/localpb"
)

func topicLockError(tool string, err error) error {
	return toolError(tool+" failed", "another topic change was still in progress: "+err.Error(), localpb.ErrorCode_ERROR_CODE_INTERNAL)
}

func topicPartialError(tool, handledVerb, failedVerb, failed string, handled []string, err error) error {
	phrase := openingPhrase(tool, callGeneric) + ": "
	parts := make([]string, 0, 2)
	if len(handled) > 0 {
		names := make([]string, len(handled))
		for i, name := range handled {
			names[i] = fmt.Sprintf("%q", name)
		}
		parts = append(parts, handledVerb+" "+strings.Join(names, ", "))
	}
	parts = append(parts, failedVerb+" "+fmt.Sprintf("%q", failed)+" failed")
	return errors.New(phrase + strings.Join(parts, "; ") + ": " + strings.TrimPrefix(err.Error(), phrase))
}

func (h *handlers) ListTopics(ctx context.Context, _ NoArgs) (ListTopicsOut, error) {
	st, err := h.preflight("list_topics", callGeneric, true)
	if err != nil {
		return ListTopicsOut{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, topicRPCTimeout)
	defer cancel()
	resp, err := st.Client.ListTopics(ctx, &localpb.ListTopicsRequest{AgentId: st.AgentID})
	if err != nil {
		return ListTopicsOut{}, rpcError("list_topics", callGeneric, permNone, err)
	}

	out := ListTopicsOut{
		Joined: append([]string{}, resp.GetJoinedTopics()...),
		Public: make([]PublicTopicOut, 0, len(resp.GetPublicTopics())),
	}
	for _, t := range resp.GetPublicTopics() {
		out.Public = append(out.Public, PublicTopicOut{
			Name:        t.GetName(),
			Description: t.GetDescription(),
		})
	}
	return out, nil
}

func (h *handlers) JoinTopic(ctx context.Context, in JoinTopicIn) (TopicsOut, error) {
	st, err := h.preflight("join_topic", callGeneric, true)
	if err != nil {
		return TopicsOut{}, err
	}
	if in.Name == "" {
		return h.joinDefaultTopics(ctx, st, in.Public, in.Description)
	}
	// The Broker call and the memory write are one critical section, so a
	// registration restore of the same topic cannot run between them.
	if err := h.topics.lockTopic(ctx); err != nil {
		return TopicsOut{}, topicLockError("join_topic", err)
	}
	defer h.topics.unlockTopic()
	ctx, cancel := context.WithTimeout(ctx, topicRPCTimeout)
	defer cancel()
	resp, err := st.Client.JoinTopic(ctx, &localpb.JoinTopicRequest{
		AgentId:     st.AgentID,
		Name:        in.Name,
		IsPublic:    in.Public,
		Description: in.Description,
	})
	if err != nil {
		return TopicsOut{}, rpcError("join_topic", callGeneric, permNone, err)
	}
	h.topics.Remember(rememberedTopic(in))
	return TopicsOut{Topics: resp.GetTopics()}, nil
}

// joinDefaultTopics joins every default topic in one critical section, stopping
// at the first failure while the topics already joined stay in the memory.
func (h *handlers) joinDefaultTopics(ctx context.Context, st Status, public bool, description string) (TopicsOut, error) {
	names, err := h.defaultTopicNames(ctx, "join_topic")
	if err != nil {
		return TopicsOut{}, err
	}
	if err := h.topics.lockTopic(ctx); err != nil {
		return TopicsOut{}, topicLockError("join_topic", err)
	}
	defer h.topics.unlockTopic()
	ctx, cancel := context.WithTimeout(ctx, topicRPCTimeout)
	defer cancel()
	var topics, joined []string
	for _, name := range names {
		resp, err := st.Client.JoinTopic(ctx, &localpb.JoinTopicRequest{
			AgentId:     st.AgentID,
			Name:        name,
			IsPublic:    public,
			Description: description,
		})
		if err != nil {
			return TopicsOut{}, topicPartialError("join_topic", "joined", "joining", name, joined,
				rpcError("join_topic", callGeneric, permNone, err))
		}
		h.topics.Remember(rememberedTopic{Name: name, Public: public, Description: description})
		joined = append(joined, name)
		topics = resp.GetTopics()
	}
	return TopicsOut{Topics: topics}, nil
}

func (h *handlers) LeaveTopic(ctx context.Context, in LeaveTopicIn) (TopicsOut, error) {
	st, err := h.preflight("leave_topic", callGeneric, true)
	if err != nil {
		return TopicsOut{}, err
	}
	if in.Name == "" {
		return h.leaveDefaultTopics(ctx, st)
	}
	if err := h.topics.lockTopic(ctx); err != nil {
		return TopicsOut{}, topicLockError("leave_topic", err)
	}
	defer h.topics.unlockTopic()
	ctx, cancel := context.WithTimeout(ctx, topicRPCTimeout)
	defer cancel()
	resp, err := st.Client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{
		AgentId: st.AgentID,
		Name:    in.Name,
	})
	if err != nil {
		return TopicsOut{}, rpcError("leave_topic", callGeneric, permNone, err)
	}
	h.topics.Forget(in.Name)
	return TopicsOut{Topics: resp.GetTopics()}, nil
}

// leaveDefaultTopics leaves the joined default topics and forgets every default
// topic from the memory, joined or not; it stops at the first failure, so only
// the topics left before it are forgotten.
func (h *handlers) leaveDefaultTopics(ctx context.Context, st Status) (TopicsOut, error) {
	names, err := h.defaultTopicNames(ctx, "leave_topic")
	if err != nil {
		return TopicsOut{}, err
	}
	if err := h.topics.lockTopic(ctx); err != nil {
		return TopicsOut{}, topicLockError("leave_topic", err)
	}
	defer h.topics.unlockTopic()
	ctx, cancel := context.WithTimeout(ctx, topicRPCTimeout)
	defer cancel()
	list, err := st.Client.ListTopics(ctx, &localpb.ListTopicsRequest{AgentId: st.AgentID})
	if err != nil {
		return TopicsOut{}, rpcError("leave_topic", callGeneric, permNone, err)
	}
	topics := list.GetJoinedTopics()
	joined := make(map[string]bool, len(topics))
	for _, name := range topics {
		joined[name] = true
	}
	var left []string
	for _, name := range names {
		if joined[name] {
			resp, err := st.Client.LeaveTopic(ctx, &localpb.LeaveTopicRequest{
				AgentId: st.AgentID,
				Name:    name,
			})
			if err != nil {
				return TopicsOut{}, topicPartialError("leave_topic", "left", "leaving", name, left,
					rpcError("leave_topic", callGeneric, permNone, err))
			}
			topics = resp.GetTopics()
			left = append(left, name)
		}
		h.topics.Forget(name)
	}
	return TopicsOut{Topics: topics}, nil
}

func (h *handlers) defaultTopicNames(ctx context.Context, tool string) ([]string, error) {
	var names []string
	if h.sessionTopics != nil {
		names = h.sessionTopics(ctx)
	}
	if len(names) == 0 {
		return nil, toolError(openingPhrase(tool, callGeneric), textNoDefaultTopic,
			localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT)
	}
	return names, nil
}
