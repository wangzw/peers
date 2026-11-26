package mcpserver

import (
	"context"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
)

const (
	listAgentsRPCTimeout = 20 * time.Second
	// sendMessageRPCTimeout must stay longer than the Broker's
	// send.request_timeout (20s by default), or the call reports
	// delivery_unknown before the Broker decides whether the message arrived.
	sendMessageRPCTimeout = 30 * time.Second
)

func agentStatusName(s localpb.AgentStatus) string {
	switch s {
	case localpb.AgentStatus_AGENT_STATUS_IDLE:
		return "idle"
	case localpb.AgentStatus_AGENT_STATUS_BUSY:
		return "busy"
	case localpb.AgentStatus_AGENT_STATUS_WAITING:
		return "waiting"
	case localpb.AgentStatus_AGENT_STATUS_SHELL:
		return "shell"
	default:
		return ""
	}
}

// ListAgents forwards list_agents to the Broker verbatim; the Broker decides
// which agents are visible.
func (h *handlers) ListAgents(ctx context.Context, in ListAgentsIn) (ListAgentsOut, error) {
	st, err := h.preflight("list_agents", callGeneric, true)
	if err != nil {
		return ListAgentsOut{}, err
	}
	ctx, cancel := withRPCTimeout(ctx, h.listAgentsTimeout, listAgentsRPCTimeout)
	defer cancel()
	resp, err := st.Client.ListAgents(ctx, &localpb.ListAgentsRequest{
		AgentId: st.AgentID,
		Topic:   in.Topic,
		Broker:  in.Broker,
		Name:    in.Name,
	})
	if err != nil {
		return ListAgentsOut{}, rpcError("list_agents", callGeneric, permNone, err)
	}

	out := ListAgentsOut{Self: resp.GetSelfAddress()}
	for _, a := range resp.GetAgents() {
		topics := a.GetSharedTopics()
		if topics == nil {
			// The success schema promises an array, not null.
			topics = []string{}
		}
		out.Agents = append(out.Agents, AgentOut{
			Address:      a.GetAddress(),
			SharedTopics: topics,
			Status:       agentStatusName(a.GetStatus()),
			PreviousName: a.GetPreviousName(),
			Account:      a.GetAccount(),
		})
	}
	return out, nil
}

const sendMessageResult = "Delivered to the recipient's session (not necessarily read)."

// SendMessage forwards send_message to the Broker without trimming or
// rewriting To or Message.
func (h *handlers) SendMessage(ctx context.Context, in SendMessageIn) (SendMessageOut, error) {
	st, err := h.preflight("send_message", callSend, true)
	if err != nil {
		return SendMessageOut{}, err
	}
	ctx, cancel := withRPCTimeout(ctx, h.sendTimeout, sendMessageRPCTimeout)
	defer cancel()
	if _, err := st.Client.Send(ctx, &localpb.SendRequest{
		AgentId:   st.AgentID,
		ToAddress: in.To,
		Message:   in.Message,
	}); err != nil {
		return SendMessageOut{}, rpcError("send_message", callSend, permNone, err)
	}
	return SendMessageOut{Result: sendMessageResult, To: in.To}, nil
}
