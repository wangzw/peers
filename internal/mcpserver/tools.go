package mcpserver

import (
	"context"
	"fmt"
	"log/slog"
	"runtime/debug"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// ListAgentsIn carries the filters of list_agents. All of them are optional and
// are forwarded to the Broker, which decides which agents are visible.
type ListAgentsIn struct {
	Topic  string `json:"topic,omitempty"  jsonschema:"only agents that share this topic with you"`
	Broker string `json:"broker,omitempty" jsonschema:"only agents on the Broker with this alias (the part after @)"`
	Name   string `json:"name,omitempty"   jsonschema:"only agents with this name"`
}

// AgentOut describes one visible agent.
type AgentOut struct {
	Address      string   `json:"address"`
	SharedTopics []string `json:"shared_topics"`
	Status       string   `json:"status,omitempty"`        // idle, busy, waiting or shell
	PreviousName string   `json:"previous_name,omitempty"` // set within 10 minutes of a rename
}

// ListAgentsOut is the result of list_agents.
type ListAgentsOut struct {
	Self   string     `json:"self_address"`
	Agents []AgentOut `json:"agents"`
}

// SendMessageIn is the input of send_message.
type SendMessageIn struct {
	To      string `json:"to"      jsonschema:"the agent address (name@alias), copied verbatim from list_agents or from a message's sender"`
	Message string `json:"message" jsonschema:"plain text"`
}

// SendMessageOut is the result of send_message. Result only means the message
// reached the recipient's session, not that the recipient read it.
type SendMessageOut struct {
	Result string `json:"result"`
	To     string `json:"to"`
}

// NoArgs is the empty input of a tool that takes no parameters.
type NoArgs struct{}

// PublicTopicOut describes one known public topic.
type PublicTopicOut struct {
	Name        string `json:"name"`
	Description string `json:"description,omitempty"`
}

// ListTopicsOut is the result of list_topics.
type ListTopicsOut struct {
	Joined []string         `json:"joined_topics"`
	Public []PublicTopicOut `json:"public_topics"`
}

// JoinTopicIn is the input of join_topic. An empty Name selects this session's
// default topics.
type JoinTopicIn struct {
	Name        string `json:"name,omitempty"        jsonschema:"the topic to join; omit to join this session's default topics"`
	Public      bool   `json:"public,omitempty"      jsonschema:"declare the topic public; requires the Broker to be public"`
	Description string `json:"description,omitempty" jsonschema:"short description shown with a public topic"`
}

// LeaveTopicIn is the input of leave_topic. An empty Name selects this
// session's default topics.
type LeaveTopicIn struct {
	Name string `json:"name,omitempty" jsonschema:"the topic to leave; omit to leave this session's default topics"`
}

// TopicsOut is the list of topics a session has joined.
type TopicsOut struct {
	Topics []string `json:"topics"`
}

// AddressOut is one address advertised by a Broker.
type AddressOut struct {
	Kind string `json:"kind"`
	Addr string `json:"addr"`
}

// BrokerOut describes one Broker known to this Broker.
type BrokerOut struct {
	Alias        string       `json:"alias"`
	Fingerprint  string       `json:"fingerprint"` // lowercase hex
	Addresses    []AddressOut `json:"advertised_addresses"`
	IsPeer       bool         `json:"is_peer"`
	Status       string       `json:"status"`               // connected, connectable, not_directly_reachable
	LocalRole    string       `json:"role,omitempty"`       // peers only: dialer or acceptor (this Broker's role)
	LinkState    string       `json:"link_state,omitempty"` // dialing, waiting, handshaking, established, backoff, rejected
	StatusDetail string       `json:"status_detail,omitempty"`
}

// ListBrokersOut is the result of list_brokers.
type ListBrokersOut struct {
	Brokers []BrokerOut `json:"brokers"`
	// Local is the state of the Broker that answered. It is set even when the
	// Broker is online and not degraded, so a caller never has to guess what an
	// absent value would mean.
	Local *LocalBrokerOut `json:"local,omitempty"`
}

// LocalBrokerOut is the state of the Broker that answered, as opposed to its
// peers.
type LocalBrokerOut struct {
	Offline  bool              `json:"offline"`
	Degraded *DegradedStateOut `json:"degraded,omitempty"`
}

// DegradedStateOut says why a Broker runs degraded. Level is "local_only" or
// "diagnostics_only"; the startup step is for display on the terminal and is
// not repeated here.
type DegradedStateOut struct {
	Level  string `json:"level"`
	Reason string `json:"reason"`
	Fix    string `json:"fix,omitempty"`
}

// ConnectBrokerIn is the input of connect_broker.
type ConnectBrokerIn struct {
	Address    string `json:"address"     jsonschema:"the other Broker's address, written as host:port or hostname(host:port)"`
	Key        string `json:"key"         jsonschema:"the pairing key the other side created"`
	LocalAlias string `json:"local_alias,omitempty" jsonschema:"alias to use for the other Broker when its own alias is taken"`
}

// ConnectBrokerOut is the result of connect_broker.
type ConnectBrokerOut struct {
	Alias       string   `json:"alias"`
	Fingerprint string   `json:"fingerprint"`
	Host        string   `json:"shown_host,omitempty"`
	Warnings    []string `json:"warnings,omitempty"`
}

// DisconnectBrokerIn is the input of disconnect_broker.
type DisconnectBrokerIn struct {
	Broker string `json:"broker" jsonschema:"alias or 64-hex-digit fingerprint of the peer Broker"`
}

// DisconnectBrokerOut is the result of disconnect_broker.
type DisconnectBrokerOut struct {
	Result string `json:"result"`
}

// CreatePairingKeyOut is the result of create_pairing_key. The addresses are
// shown to the user but are not part of the key. ConnectPrompt is text for the
// other host's user, shown verbatim; it is empty when no address can be named.
type CreatePairingKeyOut struct {
	Key           string   `json:"key"`
	ExpiresAt     string   `json:"expires_at"` // RFC 3339, UTC
	Addresses     []string `json:"shown_addresses"`
	Note          string   `json:"note,omitempty"`
	ConnectPrompt string   `json:"connect_prompt,omitempty"`
}

// GetConfigIn is the input of get_config.
type GetConfigIn struct {
	Key string `json:"key,omitempty" jsonschema:"one key such as broker.alias; omit for all keys"`
}

// ConfigEntryOut is one configuration entry.
type ConfigEntryOut struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

// GetConfigOut is the result of get_config.
type GetConfigOut struct {
	Entries []ConfigEntryOut `json:"entries"`
}

// SetConfigIn is the input of set_config. Value is the new value as text.
type SetConfigIn struct {
	Key   string `json:"key"   jsonschema:"the configuration key to change, such as broker.alias"`
	Value string `json:"value" jsonschema:"the new value as text, for example 10m, true or [a, b]"`
}

// SetConfigOut is the result of set_config, with the old and the new value.
type SetConfigOut struct {
	Key string `json:"key"`
	Old string `json:"old_value"`
	New string `json:"new_value"`
}

// SetBrokerOnlineIn is the input of set_broker_online. Online only affects
// messages other hosts send to this Broker; links and messages this Broker
// sends stay available either way.
type SetBrokerOnlineIn struct {
	Online bool `json:"online" jsonschema:"true to accept messages from other hosts, false to refuse them; links and local messages are not affected"`
}

// SetBrokerOnlineOut is the result of set_broker_online. Changed reports
// whether the stored value differed from the requested one.
type SetBrokerOnlineOut struct {
	Online  bool `json:"online"`
	Changed bool `json:"changed"`
}

// UpdateBrokerOut is the result of update_broker. The three version fields
// mirror the Broker's ApplyUpdate reply; an empty to_version means the Broker
// is already current. Result carries the sentence shown to the model.
type UpdateBrokerOut struct {
	FromVersion string `json:"from_version"`
	ToVersion   string `json:"to_version"`
	Restarting  bool   `json:"restarting"`
	Result      string `json:"result"`
}

// toolSet is what newMCPServer exposes as MCP tools. *handlers implements it.
type toolSet interface {
	ListAgents(ctx context.Context, in ListAgentsIn) (ListAgentsOut, error)
	SendMessage(ctx context.Context, in SendMessageIn) (SendMessageOut, error)
	ListTopics(ctx context.Context, in NoArgs) (ListTopicsOut, error)
	JoinTopic(ctx context.Context, in JoinTopicIn) (TopicsOut, error)
	LeaveTopic(ctx context.Context, in LeaveTopicIn) (TopicsOut, error)
	ListBrokers(ctx context.Context, in NoArgs) (ListBrokersOut, error)
	ConnectBroker(ctx context.Context, in ConnectBrokerIn) (ConnectBrokerOut, error)
	DisconnectBroker(ctx context.Context, in DisconnectBrokerIn) (DisconnectBrokerOut, error)
	CreatePairingKey(ctx context.Context, in NoArgs) (CreatePairingKeyOut, error)
	GetConfig(ctx context.Context, in GetConfigIn) (GetConfigOut, error)
	SetConfig(ctx context.Context, in SetConfigIn) (SetConfigOut, error)
	SetBrokerOnline(ctx context.Context, in SetBrokerOnlineIn) (SetBrokerOnlineOut, error)
	UpdateBroker(ctx context.Context, in NoArgs) (UpdateBrokerOut, error)
}

// newMCPServer declares the "peer" MCP server with all thirteen tools and the
// instructions that tell the model how to use them. log receives the panic
// reports of the tool handlers; nil disables them.
func newMCPServer(version string, ts toolSet, log *slog.Logger) *mcp.Server {
	s := mcp.NewServer(
		&mcp.Implementation{Name: "peer", Version: version},
		&mcp.ServerOptions{Instructions: instructions},
	)
	addTool(s, log, "list_agents",
		"List the agents visible to this session, optionally filtered by topic, Broker alias or agent name.",
		true, ts.ListAgents)
	addTool(s, log, "send_message",
		"Send plain text to another agent; success only means the message reached the other session, not that the other agent read it.",
		false, ts.SendMessage)
	addTool(s, log, "list_topics",
		"List the topics this session has joined and the public topics known to this Broker.",
		true, ts.ListTopics)
	addTool(s, log, "join_topic",
		"Join a topic; omit `name` to join this session's default topics. Declaring a topic public requires this Broker to be public too.",
		false, ts.JoinTopic)
	addTool(s, log, "leave_topic",
		"Leave a topic this session has joined; omit `name` to leave this session's default topics.",
		false, ts.LeaveTopic)
	addTool(s, log, "list_brokers",
		"List the Brokers this Broker knows about, including peer status, addresses and fingerprints.",
		true, ts.ListBrokers)
	addTool(s, log, "connect_broker",
		"Pair with another Broker using its address and a pairing key; requires write permission on this Broker's config file.",
		false, ts.ConnectBroker)
	addTool(s, log, "disconnect_broker",
		"Unpair from a peer Broker; requires write permission on this Broker's config file.",
		false, ts.DisconnectBroker)
	addTool(s, log, "create_pairing_key",
		"Create a one-time pairing key and show this Broker's addresses; requires write permission on this Broker's config file. "+
			"Show `connect_prompt` to the user only, and never send it through send_message, Claude Code's built-in session messaging, or any other agent-to-agent channel.",
		false, ts.CreatePairingKey)
	addTool(s, log, "get_config",
		"Read this Broker's configuration; requires read permission on the Broker's config file.",
		true, ts.GetConfig)
	addTool(s, log, "set_config",
		"Change this Broker's configuration; requires write permission on the Broker's config file.",
		false, ts.SetConfig)
	addReversibleTool(s, log, "set_broker_online",
		"Set whether this Broker accepts messages from other hosts; links and messages sent from this Broker are not affected. "+
			"Requires write permission on the Broker's config file.",
		ts.SetBrokerOnline)
	addDestructiveTool(s, log, "update_broker",
		"Upgrade this Broker to the latest peers release and restart it; requires write permission on the Broker's config file. "+
			"The connection recovers automatically, and this session's MCP server uses the new version only after the session is reopened.",
		ts.UpdateBroker)
	return s
}

// addTool registers one typed tool handler behind safe.
func addTool[In, Out any](s *mcp.Server, log *slog.Logger, name, description string, readOnly bool,
	h func(context.Context, In) (Out, error)) {
	mcp.AddTool(s, &mcp.Tool{
		Name:        name,
		Description: description,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: readOnly},
	}, safe(name, log, h))
}

// addDestructiveTool registers a tool that changes state in a way the tool
// itself cannot undo. The destructive hint makes the client present it as its
// own authorization decision, which for update_broker is the last confirmation
// before the Broker is replaced and restarted.
func addDestructiveTool[In, Out any](s *mcp.Server, log *slog.Logger, name, description string,
	h func(context.Context, In) (Out, error)) {
	destructive := true
	mcp.AddTool(s, &mcp.Tool{
		Name:        name,
		Description: description,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &destructive},
	}, safe(name, log, h))
}

// addReversibleTool registers a tool that changes state but can undo the
// change through the same tool. The destructive hint is explicitly false, so
// a client does not infer one from the missing value and treat the call like
// update_broker.
func addReversibleTool[In, Out any](s *mcp.Server, log *slog.Logger, name, description string,
	h func(context.Context, In) (Out, error)) {
	destructive := false
	mcp.AddTool(s, &mcp.Tool{
		Name:        name,
		Description: description,
		Annotations: &mcp.ToolAnnotations{ReadOnlyHint: false, DestructiveHint: &destructive},
	}, safe(name, log, h))
}

// safe wraps a tool handler so that a panic becomes an ordinary tool error
// instead of terminating the MCP server. The SDK does not recover handler
// panics by itself, and a Broker-side fault must never make this process exit.
func safe[In, Out any](name string, log *slog.Logger, h func(context.Context, In) (Out, error)) mcp.ToolHandlerFor[In, Out] {
	return func(ctx context.Context, _ *mcp.CallToolRequest, in In) (res *mcp.CallToolResult, out Out, err error) {
		defer func() {
			if r := recover(); r != nil {
				if log != nil {
					log.Error("panic in MCP tool handler",
						"tool", name, "panic", r, "stack", string(debug.Stack()))
				}
				var zero Out
				res, out, err = nil, zero,
					fmt.Errorf("%s failed: internal error in the MCP server (code: internal)", name)
			}
		}()
		out, err = h(ctx, in)
		return nil, out, err
	}
}
