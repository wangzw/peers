package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"os"
	"reflect"
	"sync"
	"sync/atomic"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
)

// Broker serves the local interface and holds the agent table.
type Broker struct {
	// UnimplementedLocalServer provides the forward-compatible LocalServer stubs.
	localpb.UnimplementedLocalServer
	deps   Deps
	log    *slog.Logger
	grpc   *grpc.Server
	ctx    context.Context
	cancel context.CancelFunc
	// mu guards agents, waiting, watches, events, unregHooks, gone, refreshOne,
	// lastAlias, nextAgentSeq, nameOwners (except the fields nameOwnerState
	// documents otherwise) and nameOwnerPushPending.
	mu sync.Mutex

	agents     map[AgentID]*agent
	waiting    map[*regConn]struct{}
	watches    map[watchKey]*dirWatch
	events     registryQueue
	unregHooks []func(AgentID)
	gone       []AgentID
	refreshOne func(a *agent)
	lastAlias  string

	nextAgentSeq uint64
	nameOwners   *nameOwnerState

	nameOwnerPushPending bool

	// degradedMu guards degraded.
	degradedMu sync.Mutex
	degraded   *localpb.DegradedState

	// configDirChecks has its own lock, so an ownership check never takes
	// Broker.mu.
	configDirChecks configDirCheckSet

	topicRate topicRateLimiter

	// accountTopicMu serializes the per-account topic limit check with the Join
	// it guards.
	accountTopicMu sync.Mutex

	conns     *accountLimit
	registers *accountLimit

	updateMu         sync.Mutex
	updateAvailable  string
	updateReleaseURL string
	updateTimer      Timer
	// updateBusy is set for an upgrade and stays set after a successful install.
	updateBusy atomic.Bool
}

// New assembles the Broker. Deps.Topics and Deps.Config are required; the
// other nil fields take the defaults documented on Deps. The version
// interceptors are installed before Deps.ServerOptions so a rejected
// connection gets version_mismatch before the access interceptor runs.
func New(deps Deps) (*Broker, error) {
	if deps.Topics == nil {
		return nil, fmt.Errorf("cannot create the Broker: Deps.Topics is required")
	}
	if deps.Config == nil {
		return nil, fmt.Errorf("cannot create the Broker: Deps.Config is required")
	}
	if deps.Sessions == nil {
		deps.Sessions = UserRouter()
	}
	if deps.Clock == nil {
		deps.Clock = SystemClock()
	}
	if deps.Logger == nil {
		// The zero LevelVar means info.
		deps.Logger = NewLogger(os.Stderr, &slog.LevelVar{})
	}
	if deps.InSession == nil {
		deps.InSession = inbox.InSession
	}
	if deps.Geteuid == nil {
		deps.Geteuid = os.Geteuid
	}
	if deps.ReadPeerCred == nil {
		deps.ReadPeerCred = ReadPeerCred
	}
	if deps.LocalProtocolVersion == 0 {
		deps.LocalProtocolVersion = localpb.ProtocolVersion
	}

	allowSameAccount := func(c Caller) bool {
		return c.UID == uint32(deps.Geteuid())
	}
	allow := allowSameAccount
	if deps.AllowCaller != nil {
		// The system Broker runs as root and serves every account; which
		// accounts may connect is decided by the local socket's permissions.
		allow = deps.AllowCaller
	}
	logReject := func(c Caller) {
		deps.Logger.Warn(EvConnectionRejected, "uid", c.UID, "pid", c.PID)
	}
	conns, registers := newLocalAccountLimits(deps.Logger)
	peerCreds := newPeerCredCreds(deps.ReadPeerCred, allow, logReject, conns)

	// The peer credentials are the server's credentials. A caller-supplied
	// credentials option would replace them because the last option wins, so it
	// is rejected rather than silently disabling the account check.
	if err := checkServerOptions(peerCreds, deps.ServerOptions); err != nil {
		return nil, err
	}

	opts := []grpc.ServerOption{
		grpc.Creds(peerCreds),
		// Make the handshake deadline explicit: the account lookup that
		// decides the connection's bucket is bounded by the same value.
		grpc.ConnectionTimeout(localHandshakeTimeout),
		// Bound the streams one connection may run at once. A client that
		// ignores the advertised limit is refused by the HTTP/2 server.
		grpc.MaxConcurrentStreams(uint32(localStreamLimit)),
		// Stop must wait for method handlers: the Register handler removes its
		// agent and releases its watch after the stream ends, and shutdown
		// closes the configuration and the account lock right after Stop.
		grpc.WaitForHandlers(true),
	}
	opts = append(opts, versionServerOptions()...)
	opts = append(opts, deps.ServerOptions...)

	ctx, cancel := context.WithCancel(context.Background())
	b := &Broker{
		deps:      deps,
		log:       deps.Logger,
		grpc:      grpc.NewServer(opts...),
		ctx:       ctx,
		cancel:    cancel,
		agents:    make(map[AgentID]*agent),
		waiting:   make(map[*regConn]struct{}),
		watches:   make(map[watchKey]*dirWatch),
		events:    newRegistryQueue(),
		conns:     conns,
		registers: registers,
	}
	b.refreshOne = b.refreshAgent
	if err := b.initNameOwners(); err != nil {
		cancel()
		return nil, err
	}
	b.events.start(ctx, b.handleRegistryEvent)
	localpb.RegisterLocalServer(b.grpc, b)
	return b, nil
}

func checkServerOptions(base credentials.TransportCredentials, opts []grpc.ServerOption) error {
	if len(opts) == 0 {
		return nil
	}
	if serverOptionsReplaceCreds(base, opts) {
		return fmt.Errorf("cannot create the Broker: Deps.ServerOptions must not set transport credentials; New installs the peer credentials")
	}
	return nil
}

// grpc.ServerOption has no accessors, so this reads the probe's resolved
// credentials through reflection; a nil or unrecognised field fails closed.
func serverOptionsReplaceCreds(base credentials.TransportCredentials, opts []grpc.ServerOption) bool {
	all := make([]grpc.ServerOption, 0, len(opts)+1)
	all = append(all, grpc.Creds(base))
	all = append(all, opts...)
	probe := grpc.NewServer(all...)
	defer probe.Stop()

	creds := reflect.ValueOf(probe).Elem().FieldByName("opts").FieldByName("creds")
	if !creds.IsValid() || creds.IsNil() {
		return true
	}
	// The peer credentials are a pointer. A different dynamic type means some
	// option replaced them; the same type means the same kind of value, which
	// for a pointer is compared by identity.
	elem := creds.Elem()
	if elem.Type() != reflect.TypeOf(base) {
		return true
	}
	return elem.Pointer() != reflect.ValueOf(base).Pointer()
}

// Serve serves the Local service on ln. It may be called once per listener;
// each call blocks until Stop.
func (b *Broker) Serve(ln net.Listener) error {
	return b.grpc.Serve(ln)
}

// Stop cancels the Broker's lifetime context and stops the gRPC server. It is
// safe to call more than once.
func (b *Broker) Stop() {
	b.cancel()
	b.grpc.Stop()
	b.stopNameOwners()
}

// Logger returns the Broker's structured logger.
func (b *Broker) Logger() *slog.Logger { return b.log }
