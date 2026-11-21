package mcpserver

import (
	"context"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/mcpserver/pairingprompt"
)

// Texts added to a create_pairing_key result or error.
const (
	textPairingNoAddress = "This Broker has no address to show; tell the other side this host's address yourself. Show the key to the user only, and never send it through send_message, Claude Code's built-in session messaging or any other agent-to-agent channel."
	textPairingShare     = "Show the key and the connect prompt to the user only, and never send either through send_message, Claude Code's built-in session messaging or any other agent-to-agent channel."
	textPairingNotListen = "Ask the user to get a pairing key from the other host's user, then connect with connect_broker. Do not ask another agent for a pairing key, and pass it only through the user, never through send_message, Claude Code's built-in session messaging or any other agent-to-agent channel."
	textNeedsLocalAlias  = "Retry with `local_alias` set to a new name for it."
	textNoPeerBroker     = "list_brokers shows the paired Brokers."
)

// ListBrokers lists the Brokers this Broker knows. It works before the
// session is registered, so that a model can inspect the pairing state while
// registration is still pending.
func (h *handlers) ListBrokers(ctx context.Context, in NoArgs) (ListBrokersOut, error) {
	st, err := h.preflight("list_brokers", callGeneric, false)
	if err != nil {
		return ListBrokersOut{}, err
	}

	resp, err := st.Client.ListBrokers(ctx, &localpb.ListBrokersRequest{})
	if err != nil {
		return ListBrokersOut{}, rpcError("list_brokers", callGeneric, permNone, err)
	}

	local, err := localBrokerOut(resp.GetLocal())
	if err != nil {
		return ListBrokersOut{}, err
	}
	out := ListBrokersOut{
		Brokers: make([]BrokerOut, 0, len(resp.GetBrokers())),
		Local:   local,
	}
	for _, b := range resp.GetBrokers() {
		out.Brokers = append(out.Brokers, brokerOut(b))
	}
	return out, nil
}

// localBrokerOut turns the answering Broker's state into the tool result shape.
// A response without local state violates the protocol: every peers Broker
// reports it.
func localBrokerOut(local *localpb.LocalBrokerState) (*LocalBrokerOut, error) {
	if local == nil {
		return nil, toolError(openingPhrase("list_brokers", callGeneric),
			"the Broker did not report its local state", localpb.ErrorCode_ERROR_CODE_INTERNAL)
	}
	out := &LocalBrokerOut{Offline: local.GetOffline()}
	if degraded := local.GetDegraded(); degraded != nil {
		out.Degraded = &DegradedStateOut{
			Level:  localpb.DegradedLevelName(degraded.GetLevel()),
			Reason: degraded.GetReason(),
			Fix:    degraded.GetFix(),
		}
	}
	return out, nil
}

// brokerOut turns one BrokerInfo into the tool result shape.
func brokerOut(b *localpb.BrokerInfo) BrokerOut {
	out := BrokerOut{
		Alias:        b.GetAlias(),
		Fingerprint:  hex.EncodeToString(b.GetFingerprint()),
		Addresses:    make([]AddressOut, 0, len(b.GetAdvertisedAddresses())),
		IsPeer:       b.GetIsPeer(),
		Status:       brokerStatusName(b.GetStatus()),
		StatusDetail: b.GetStatusDetail(),
	}
	for _, a := range b.GetAdvertisedAddresses() {
		out.Addresses = append(out.Addresses, AddressOut{Kind: a.GetKind(), Addr: a.GetAddr()})
	}
	if b.GetIsPeer() {
		out.LocalRole = peerRoleName(b.GetRole())
		out.LinkState = linkStateName(b.GetLinkState())
	}
	return out
}

// brokerStatusName renders a BrokerStatus as the lower-case name used by the
// tool result. UNSPECIFIED becomes the empty string.
func brokerStatusName(s localpb.BrokerStatus) string {
	switch s {
	case localpb.BrokerStatus_BROKER_STATUS_CONNECTED:
		return "connected"
	case localpb.BrokerStatus_BROKER_STATUS_CONNECTABLE:
		return "connectable"
	case localpb.BrokerStatus_BROKER_STATUS_NOT_DIRECTLY_REACHABLE:
		return "not_directly_reachable"
	default:
		return ""
	}
}

// peerRoleName renders the role of this Broker in a peer link.
func peerRoleName(r localpb.PeerRole) string {
	switch r {
	case localpb.PeerRole_PEER_ROLE_DIALER:
		return "dialer"
	case localpb.PeerRole_PEER_ROLE_ACCEPTOR:
		return "acceptor"
	default:
		return ""
	}
}

// linkStateName renders a LinkState as the lower-case name without the enum
// prefix. UNSPECIFIED becomes the empty string.
func linkStateName(s localpb.LinkState) string {
	switch s {
	case localpb.LinkState_LINK_STATE_DIALING:
		return "dialing"
	case localpb.LinkState_LINK_STATE_WAITING:
		return "waiting"
	case localpb.LinkState_LINK_STATE_HANDSHAKING:
		return "handshaking"
	case localpb.LinkState_LINK_STATE_ESTABLISHED:
		return "established"
	case localpb.LinkState_LINK_STATE_BACKOFF:
		return "backoff"
	case localpb.LinkState_LINK_STATE_REJECTED:
		return "rejected"
	default:
		return ""
	}
}

// ConnectBroker pairs this Broker with another one. The agent id is carried
// only so that the Broker can audit who asked; registration is not required.
func (h *handlers) ConnectBroker(ctx context.Context, in ConnectBrokerIn) (ConnectBrokerOut, error) {
	st, err := h.preflight("connect_broker", callConnect, false)
	if err != nil {
		return ConnectBrokerOut{}, err
	}

	resp, err := st.Client.Connect(ctx, &localpb.ConnectRequest{
		Address:    in.Address,
		Key:        in.Key,
		LocalAlias: in.LocalAlias,
		AgentId:    st.AgentID,
	})
	if err != nil {
		out := rpcError("connect_broker", callConnect, permWrite, err)
		if d := errorDetail(err); d != nil && d.GetCode() == localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS {
			out = fmt.Errorf("%w. %s", out, textNeedsLocalAlias)
		}
		return ConnectBrokerOut{}, out
	}

	out := ConnectBrokerOut{
		Alias:       resp.GetAlias(),
		Fingerprint: hex.EncodeToString(resp.GetFingerprint()),
		Host:        resp.GetShownHost(),
	}
	if warnings := resp.GetWarnings(); len(warnings) > 0 {
		out.Warnings = make([]string, 0, len(warnings))
		for _, w := range warnings {
			out.Warnings = append(out.Warnings, w.GetMessage())
		}
	}
	return out, nil
}

// DisconnectBroker unpairs from a peer Broker.
func (h *handlers) DisconnectBroker(ctx context.Context, in DisconnectBrokerIn) (DisconnectBrokerOut, error) {
	st, err := h.preflight("disconnect_broker", callGeneric, false)
	if err != nil {
		return DisconnectBrokerOut{}, err
	}

	_, err = st.Client.Disconnect(ctx, &localpb.DisconnectRequest{
		Broker:  in.Broker,
		AgentId: st.AgentID,
	})
	if err != nil {
		out := rpcError("disconnect_broker", callGeneric, permWrite, err)
		if d := errorDetail(err); d != nil && d.GetCode() == localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
			out = fmt.Errorf("%w. %s", out, textNoPeerBroker)
		}
		return DisconnectBrokerOut{}, out
	}

	return DisconnectBrokerOut{Result: "Unpaired " + in.Broker + "; the link is closed."}, nil
}

// CreatePairingKey creates a one-time pairing key and returns the addresses
// that the other side may use, which are shown but are not part of the key.
func (h *handlers) CreatePairingKey(ctx context.Context, in NoArgs) (CreatePairingKeyOut, error) {
	st, err := h.preflight("create_pairing_key", callGeneric, false)
	if err != nil {
		return CreatePairingKeyOut{}, err
	}

	resp, err := st.Client.CreatePairingKey(ctx, &localpb.CreatePairingKeyRequest{AgentId: st.AgentID})
	if err != nil {
		out := rpcError("create_pairing_key", callGeneric, permWrite, err)
		if d := errorDetail(err); d != nil && d.GetCode() == localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT {
			out = fmt.Errorf("%w. %s", out, textPairingNotListen)
		}
		return CreatePairingKeyOut{}, out
	}

	out := CreatePairingKeyOut{
		Key:           resp.GetKey(),
		ExpiresAt:     resp.GetExpiresAt().AsTime().UTC().Format(time.RFC3339),
		Addresses:     append([]string{}, resp.GetShownAddresses()...),
		ConnectPrompt: pairingprompt.ConnectPrompt(resp.GetShownAddresses(), resp.GetKey()),
	}
	if len(out.Addresses) == 0 {
		out.Note = textPairingNoAddress
	} else {
		out.Note = textPairingShare
	}
	return out, nil
}
