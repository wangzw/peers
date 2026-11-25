package broker

import (
	"context"
	"errors"
	"log/slog"
	"net"
	"net/http"

	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/broker/update"
)

// ErrInvalidTopicName is returned by TopicTable.Join and Leave for a name that
// breaks the topic name rules; the broker maps it to invalid_argument.
var ErrInvalidTopicName = errors.New("invalid topic name")

// ErrUnknownAgent is returned by TopicTable.Join for an agent that AgentChanged
// never introduced or that LeaveAll removed; the broker maps it to internal.
var ErrUnknownAgent = errors.New("agent not introduced to the topic table")

// ErrTopicQuotaExceeded is returned by TopicTable.Join when the number of local
// topics or the number of local members of one topic has reached its limit; the
// broker maps it to quota_exceeded.
var ErrTopicQuotaExceeded = errors.New("topic quota exceeded")

// Member is one agent's current roster entry in the topic table.
type Member struct {
	// Name is the member's agent name.
	Name string
	// Ref is the member's disambiguator.
	Ref string
	// Status is the member's session status.
	Status string
	// PrevName is the member's most recent former name, empty when none.
	PrevName string
	// Account is the publisher's account name, for display only.
	Account string
	// RefRequired means this member must be addressed by its ref, not a bare name.
	RefRequired bool
}

// VisibleAgent is one agent visible to a local agent: the local agent itself
// for a local entry, or a peer member for a remote entry.
type VisibleAgent struct {
	Local  AgentID  // non-empty for a local agent
	Peer   [32]byte // fingerprint of the peer Broker for a remote agent
	Member Member   // the agent's current roster entry (local and remote)
	Shared []string // shared topics, sorted
}

// TopicTable is the broker's view of the topic registry.
type TopicTable interface {
	// Join adds agent to topic name, returning ErrInvalidTopicName,
	// ErrUnknownAgent or ErrTopicQuotaExceeded.
	Join(agent AgentID, name string) error
	// Leave removes agent from topic name, returning ErrInvalidTopicName.
	Leave(agent AgentID, name string) error
	// LeaveAll removes agent from every topic.
	LeaveAll(agent AgentID)
	// TopicsOf returns the topics agent belongs to.
	TopicsOf(agent AgentID) []string
	// AgentChanged records agent's current roster entry m.
	AgentChanged(agent AgentID, m Member)
	// Visible reports whether other is visible to local and the topics they share.
	Visible(local AgentID, other VisibleAgent) (shared []string, ok bool)
	// VisibleAgents returns the agents visible to local, excluding local itself.
	VisibleAgents(local AgentID) []VisibleAgent
	// RemoteMembers returns the members of peer's roster.
	RemoteMembers(peer [32]byte) []Member
}

// PublicTopic is one public topic declaration, merged by name.
type PublicTopic struct {
	// Name is the topic name.
	Name string
	// Description is the declaration's description.
	Description string
	// Publisher is the fingerprint of the Broker whose declaration supplied
	// Description.
	Publisher [32]byte
	// PublisherAlias is the alias known for Publisher, empty when none is.
	PublisherAlias string
	// Declarers is the number of Brokers declaring the name.
	Declarers int
}

// PublicTopicDirectory is the broker's view of the directory's public topic
// declarations.
type PublicTopicDirectory interface {
	// DeclarePublicTopic records a public declaration. It returns an *Error to
	// choose the status the client receives; any other error reaches the client
	// as internal.
	DeclarePublicTopic(name, description string) error
	// RetirePublicTopic withdraws the declaration for name.
	RetirePublicTopic(name string)
	// PublicTopics returns the merged public topic declarations.
	PublicTopics() []PublicTopic
}

// PeerView is the broker's view of paired Brokers: their effective local
// aliases and their information for Status.
type PeerView interface {
	// EffectiveAlias returns the local alias for fp, and whether one is known.
	EffectiveAlias(fp [32]byte) (alias string, ok bool)
	// Peers returns the paired Brokers' information for Status.
	Peers(ctx context.Context) []*localpb.BrokerInfo
}

// LinkRunner is the link lifecycle Run drives at startup and shutdown.
type LinkRunner interface {
	// Listen binds the link listener at address.
	Listen(address string) error
	// Start dials the peers for which this side is the dialer.
	Start()
	// Close stops the link and its handlers.
	Close()
}

// Component is a part of the Broker started after New and before the link
// starts listening.
type Component interface {
	// Name identifies the component.
	Name() string
	// Start registers RPC handlers, event subscriptions and link frame handling.
	Start(ctx context.Context, b *Broker) error
	// Stop releases what Start registered.
	Stop()
}

// PostReadyComponent is a Component that runs additional work after the local
// socket is ready and the link has started.
type PostReadyComponent interface {
	Component
	// AfterReady runs after the local socket is bound and the link has started.
	AfterReady(ctx context.Context) error
}

type listenBoundReporter interface {
	listenBound(address string)
}

// CardIssuer re-signs the Broker's own directory card.
type CardIssuer interface {
	// Reissue re-signs the card; a no-op when the Broker is not public.
	Reissue()
}

// Deps is everything New needs from the process entry point and the assemblers.
type Deps struct {
	Config               ConfigSource                   // nil in Run: PackageConfig()
	Sessions             SessionRouter                  // nil: UserRouter()
	Topics               TopicTable                     // required
	Directory            PublicTopicDirectory           // nil: built-in "not public"
	Peers                PeerView                       // nil: no peers
	Clock                Clock                          // nil: SystemClock()
	Logger               *slog.Logger                   // nil: NewLogger(os.Stderr, info)
	InSession            func() bool                    // nil: inbox.InSession
	Geteuid              func() int                     // nil: os.Geteuid
	ReadPeerCred         func(net.Conn) (Caller, error) // nil: ReadPeerCred
	ServerOptions        []grpc.ServerOption            // appended to the gRPC server options (e.g. the access interceptor)
	LocalProtocolVersion uint32                         // 0: localpb.ProtocolVersion; Run copies RunOptions.LocalProtocolVersion here
	// System is true for the system Broker.
	System bool

	// Version is the version this process reports, without a leading v. It is
	// empty in tests that do not care; UpdateStatus returns it and the
	// periodic update check runs only when it is a formal release.
	Version string
	// UpdateAPI overrides the GitHub API base; empty: update.APIBase().
	UpdateAPI string
	// UpdateHTTPClient is the client for the update lookup; nil:
	// http.DefaultClient.
	UpdateHTTPClient *http.Client
	// LatestRelease looks up the latest formal release; nil: update.Latest.
	LatestRelease func(ctx context.Context, c *http.Client, api string) (update.Release, error)
	// InstallRelease installs a release over the running binary and runs the
	// new binary's post-update step at the given service level; nil:
	// update.Install. ApplyUpdate uses it so tests can substitute an installer.
	InstallRelease func(ctx context.Context, o update.ApplyOptions, level string) error
	// ServiceManager reports whether this process runs under a service manager
	// (systemd or launchd), which starts the Broker again after it exits. nil:
	// detected from the process environment. ApplyUpdate refuses when it
	// reports false, because nothing would start the new binary.
	ServiceManager func() bool
	// RequestShutdown asks the process to shut down normally. Run sets it to
	// cancel the run context, so the process exits with its normal code and
	// the service manager starts the installed binary. nil: ApplyUpdate
	// refuses, because it could not complete the restart.
	RequestShutdown func()

	Pairing        PairingOps    // nil: peer management RPCs return internal; ListBrokers lists only the catalog's non-peers
	PeerLinks      PeerLinks     // nil: peer management RPCs return internal; ListBrokers lists only the catalog's non-peers
	Catalog        BrokerCatalog // nil: no directory
	Warner         ConnectWarner // nil: no warnings
	ListenOverride string        // --listen value of this run, set by Run after Load; empty: not given
	CardIssuer     CardIssuer    // nil: the card is not re-signed; filled by the directory assembler

	Delivery *delivery // nil: Send returns internal

	AllowCaller func(Caller) bool // nil: allow only the Broker's own account; set by the system assembler
}
