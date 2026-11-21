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

// Broker serves the local interface and holds the agent table. Handler methods
// live in the files of the work item that owns the RPC; this file owns the
// structure, the gRPC server and the process-lifetime context.
type Broker struct {
	localpb.UnimplementedLocalServer
	deps   Deps
	log    *slog.Logger
	grpc   *grpc.Server
	ctx    context.Context
	cancel context.CancelFunc
	mu     sync.Mutex

	// agent table fields
	agents     map[AgentID]*agent
	waiting    map[*regConn]struct{}
	watches    map[watchKey]*dirWatch
	events     registryQueue
	unregHooks []func(AgentID)
	gone       []AgentID
	refreshOne func(a *agent)
	lastAlias  string // last broker.alias value read successfully, guarded by mu

	// degradedMu guards degraded, the state a --allow-degraded run reports.
	degradedMu sync.Mutex
	degraded   *localpb.DegradedState

	// configDirChecks limits an account to one unfinished ownership check.
	// It has its own lock so a check never takes Broker.mu.
	configDirChecks configDirCheckSet

	// topicRate limits topic changes per account.
	topicRate topicRateLimiter

	// accountTopicMu serialises the per-account topic limit check with the Join
	// it guards, so two concurrent joins on a system Broker cannot both pass
	// while the account is one topic short of its limit.
	accountTopicMu sync.Mutex

	// conns and registers bound what one account may hold on the local
	// interface. conns is the same value the peer credentials count with.
	conns     *accountLimit
	registers *accountLimit

	// update state is the in-memory result of the periodic release check. It
	// is never persisted.
	updateMu         sync.Mutex
	updateAvailable  string
	updateReleaseURL string
	updateTimer      Timer
	// updateBusy guards ApplyUpdate. It stays set after a successful install:
	// the process is about to exit, so no second update may start.
	updateBusy atomic.Bool

	// downstream state
}

// New assembles the Broker. Deps.Topics and Deps.Config are required; the
// other nil fields take the defaults documented on Deps. The gRPC server
// options are assembled in a fixed order: the peer credentials, then
// versionServerOptions, then Deps.ServerOptions. Appending the version
// interceptors before the caller's ones is what makes a rejected connection
// get version_mismatch before the access interceptor runs.
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
	conns := newAccountLimit(localConnLimit, EvLocalConnectionLimitReached, deps.Logger)
	registers := newAccountLimit(localRegisterLimit, EvRegisterLimitReached, deps.Logger)
	peerCreds := newPeerCredCreds(deps.ReadPeerCred, allow, logReject, conns)

	// The peer credentials are the server's credentials. A caller-supplied
	// credentials option would replace them because the last option wins, so it
	// is rejected rather than silently disabling the account check.
	if err := checkServerOptions(peerCreds, deps.ServerOptions); err != nil {
		return nil, err
	}

	opts := []grpc.ServerOption{
		grpc.Creds(peerCreds),
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
	b.events.start(ctx, b.handleRegistryEvent)
	localpb.RegisterLocalServer(b.grpc, b)
	return b, nil
}

// checkServerOptions rejects Deps.ServerOptions that set transport credentials.
// New installs the process-credential handshake itself, and grpc.Creds applied
// later would replace it silently, letting any account connect to a user
// Broker.
func checkServerOptions(base credentials.TransportCredentials, opts []grpc.ServerOption) error {
	if len(opts) == 0 {
		return nil
	}
	if serverOptionsReplaceCreds(base, opts) {
		return fmt.Errorf("cannot create the Broker: Deps.ServerOptions must not set transport credentials; New installs the peer credentials")
	}
	return nil
}

// serverOptionsReplaceCreds applies base and then opts to a probe server and
// reports whether the resolved credentials are no longer base. grpc.ServerOption
// has no accessors, so the probe's resolved field is read through reflection; it
// is an interface field, and comparing the concrete pointer does not expose the
// unexported value. A nil or unrecognised field fails closed.
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
}

// Logger returns the Broker's structured logger.
func (b *Broker) Logger() *slog.Logger { return b.log }
