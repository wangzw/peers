package topic

import (
	"errors"
	"log/slog"

	"github.com/wangzw/peers/internal/link"
)

// AgentID is the broker's agent ID: lowercase hex SHA-256 of "session:" + socket path.
type AgentID string

// AgentRef names an agent for Visible: a local one when Peer is zero, otherwise
// a remote one on Peer's link.
type AgentRef struct {
	// Peer is the agent's Broker; the zero value means the local Broker.
	Peer link.Fingerprint
	// ID is the local agent's identifier, ignored for a remote agent.
	ID AgentID
	// Ref is a remote agent's disambiguator from the peer's roster.
	Ref string
}

// Member is one roster entry.
type Member struct {
	// Name is the agent's name.
	Name string
	// Ref is the agent's disambiguator.
	Ref string
	// Status is "idle", "busy", "waiting", "shell", or "" when unknown.
	Status string
	// PrevName is the previous name, or "" when there is none.
	PrevName string
	// Account is the publisher's account name, for display only.
	Account string
	// RefRequired means the publisher requires this member to be addressed by
	// its disambiguator; a bare name must not select it.
	RefRequired bool
}

// VisibleAgent is one agent visible to a local agent, with the topics they share.
type VisibleAgent struct {
	Agent  AgentRef // Peer zero and ID set for a local agent; Peer and Ref set for a remote one
	Member Member   // the agent's current entry
	Shared []string // shared topic names, ascending
}

// Options configures a Registry.
type Options struct {
	// Logger receives this package's log records; nil means slog.Default().
	Logger *slog.Logger

	// Self is this Broker's certificate fingerprint, the value the peer sees on
	// the link. New panics when Self is the zero Fingerprint.
	Self link.Fingerprint
}

var (
	// ErrInvalidTopic: the topic name is empty, longer than 256 bytes, not
	// valid UTF-8, or contains a control character. broker maps it to invalid_argument.
	ErrInvalidTopic = errors.New("invalid topic name")
	// ErrUnknownAgent: Join for an agent that AgentChanged never introduced,
	// or that LeaveAll removed. A broker bug; broker maps it to internal.
	ErrUnknownAgent = errors.New("unknown agent")
	// ErrTopicLimit: Join would exceed the number of local topics or the number
	// of local members of one topic. broker maps it to quota_exceeded.
	ErrTopicLimit = errors.New("topic limit reached")
)
