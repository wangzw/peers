package topic

import (
	"errors"
	"log/slog"

	"github.com/wangzw/peers/internal/link"
)

// AgentID is the broker's agent ID: lowercase hex SHA-256 of "session:" + socket path.
type AgentID string

// AgentRef names an agent for Visible. Peer is zero for an agent on this
// Broker, which ID then names; otherwise Ref is the agent's disambiguator as
// carried in the peer's roster entries and ID is ignored.
type AgentRef struct {
	Peer link.Fingerprint
	ID   AgentID
	Ref  string
}

// Member is one roster entry. Status is "idle", "busy", "waiting", "shell",
// or "" when unknown. PrevName is "" when there is none.
type Member struct {
	Name     string
	Ref      string
	Status   string
	PrevName string
}

// VisibleAgent is one agent visible to a local agent, with the topics they share.
type VisibleAgent struct {
	Agent  AgentRef // Peer zero and ID set for a local agent; Peer and Ref set for a remote one
	Member Member   // the agent's current entry
	Shared []string // shared topic names, ascending
}

// Options configures a Registry. A nil Logger means slog.Default().
type Options struct {
	Logger *slog.Logger

	// Self is this Broker's certificate fingerprint, the value the peer sees on
	// the link. Digests this Broker sends are bound to it so a peer cannot echo
	// a digest set back and match it. New panics when Self is the zero
	// Fingerprint.
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
