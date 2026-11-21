package broker

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/peer"

	"github.com/wangzw/peers/internal/accountdb"
)

// Caller is the account and process on the other end of a local connection.
type Caller struct {
	UID      uint32
	GID      uint32
	PID      int32
	Username string
}

// Local interface resource limits. One account may hold at most
// maxLocalConnections local connections and maxLocalRegistrations Register
// streams, and one connection at most maxLocalStreams concurrent streams. A
// user Broker serves only its own account, so the values are large enough not
// to affect normal use; they keep one account from exhausting the goroutines,
// memory and file descriptors of a system Broker that runs as another account.
const (
	maxLocalConnections   = 256
	maxLocalStreams       = 64
	maxLocalRegistrations = 256
)

// localConnLimit, localStreamLimit and localRegisterLimit are the limits the
// next New applies. They are variables so tests can lower them; production
// always leaves the constants above.
var (
	localConnLimit     = maxLocalConnections
	localStreamLimit   = maxLocalStreams
	localRegisterLimit = maxLocalRegistrations
)

// callerAccount returns the name a caller error message uses: the resolved
// account name, or the decimal uid when it has none.
func callerAccount(c Caller) string {
	if c.Username != "" {
		return c.Username
	}
	return strconv.FormatUint(uint64(c.UID), 10)
}

// accountLimit counts concurrent local resources per account and refuses the
// ones past its limit. It logs event once each time an account first goes over
// the limit and writes it again only after the count has fallen back below the
// limit, so a client that keeps retrying cannot flood the runtime log.
type accountLimit struct {
	mu     sync.Mutex
	limit  int
	event  string
	log    *slog.Logger
	counts map[uint32]int
	over   map[uint32]struct{}
}

func newAccountLimit(limit int, event string, log *slog.Logger) *accountLimit {
	return &accountLimit{
		limit:  limit,
		event:  event,
		log:    log,
		counts: make(map[uint32]int),
		over:   make(map[uint32]struct{}),
	}
}

// acquire takes one slot for uid and reports whether the account was below its
// limit. The first refusal of an over-limit episode is logged.
func (l *accountLimit) acquire(uid uint32) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[uid] >= l.limit {
		if _, logged := l.over[uid]; !logged {
			l.over[uid] = struct{}{}
			l.log.Warn(l.event, "uid", uid, "limit", l.limit)
		}
		return false
	}
	l.counts[uid]++
	return true
}

// release returns one slot for uid. Once the account is back below its limit,
// a later refusal is logged again.
func (l *accountLimit) release(uid uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[uid] > 1 {
		l.counts[uid]--
	} else {
		delete(l.counts, uid)
	}
	if l.counts[uid] < l.limit {
		delete(l.over, uid)
	}
}

// countedConn returns the account's connection slot when the gRPC server closes
// the connection. The server may close a connection more than once; the slot
// must be returned only once.
type countedConn struct {
	net.Conn
	once    sync.Once
	release func()
}

func (c *countedConn) Close() error {
	err := c.Conn.Close()
	c.once.Do(c.release)
	return err
}

// ReadPeerCred reads the process credentials of the peer of conn. It is called
// once per connection during the gRPC handshake.
func ReadPeerCred(conn net.Conn) (Caller, error) {
	u, ok := conn.(*net.UnixConn)
	if !ok {
		return Caller{}, fmt.Errorf("cannot read peer credentials: connection is %T, not *net.UnixConn", conn)
	}
	return readPeerCred(u)
}

// usernameCacheTTL bounds how long a resolved caller name is reused. The name
// is display data, and every local connection would otherwise cost one account
// database lookup.
const usernameCacheTTL = 5 * time.Minute

// usernameCacheEntry is one cached caller name.
type usernameCacheEntry struct {
	name    string
	expires time.Time
}

var (
	// lookupUsername and usernameNow are seams for tests.
	lookupUsername = accountdb.LookupUID
	usernameNow    = time.Now

	usernameMu    sync.Mutex
	usernameCache = make(map[uint32]usernameCacheEntry)
)

// usernameFor returns the account name for uid, or the decimal uid when the
// account cannot be looked up. A failed lookup is not cached.
func usernameFor(uid uint32) string {
	now := usernameNow()
	usernameMu.Lock()
	entry, ok := usernameCache[uid]
	usernameMu.Unlock()
	if ok && now.Before(entry.expires) {
		return entry.name
	}

	id := strconv.FormatUint(uint64(uid), 10)
	u, err := lookupUsername(uid)
	if err != nil || u.Name == "" {
		return id
	}
	usernameMu.Lock()
	usernameCache[uid] = usernameCacheEntry{name: u.Name, expires: now.Add(usernameCacheTTL)}
	usernameMu.Unlock()
	return u.Name
}

// callerAuthInfo is the AuthInfo of one local connection.
type callerAuthInfo struct {
	credentials.CommonAuthInfo
	caller Caller
	// rejected is set by Hello after a version mismatch; checked by the
	// version interceptors.
	rejected atomic.Bool
	// rejectedClientVersion is the client version of the rejected Hello; valid
	// when rejected is set.
	rejectedClientVersion atomic.Uint32
	// rejectedBrokerVersion is this Broker's version at that Hello; valid when
	// rejected is set.
	rejectedBrokerVersion atomic.Uint32
}

// AuthType implements credentials.AuthInfo.
func (a *callerAuthInfo) AuthType() string { return "peercred" }

// authInfoFrom returns the caller's per-connection AuthInfo. It is the hook the
// version-compat unit uses to set the rejected flag.
func authInfoFrom(ctx context.Context) (*callerAuthInfo, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return nil, false
	}
	info, ok := p.AuthInfo.(*callerAuthInfo)
	return info, ok
}

// CallerFrom returns the caller identified on the connection that carries ctx.
func CallerFrom(ctx context.Context) (Caller, bool) {
	info, ok := authInfoFrom(ctx)
	if !ok {
		return Caller{}, false
	}
	return info.caller, true
}

// peerCredCreds identifies the caller of every local connection.
type peerCredCreds struct {
	read     func(net.Conn) (Caller, error)
	allow    func(Caller) bool
	onReject func(Caller)
	// conns bounds the connections one account may hold. A value built without
	// a Broker leaves it nil, which serves without the bound.
	conns *accountLimit
}

func newPeerCredCreds(read func(net.Conn) (Caller, error), allow func(Caller) bool, onReject func(Caller), conns *accountLimit) credentials.TransportCredentials {
	return &peerCredCreds{read: read, allow: allow, onReject: onReject, conns: conns}
}

// ClientHandshake implements credentials.TransportCredentials.
func (c *peerCredCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, fmt.Errorf("cannot read peer credentials on the client side")
}

// ServerHandshake implements credentials.TransportCredentials.
func (c *peerCredCreds) ServerHandshake(conn net.Conn) (net.Conn, credentials.AuthInfo, error) {
	caller, err := c.read(conn)
	if err != nil {
		return nil, nil, err
	}
	if c.allow != nil && !c.allow(caller) {
		if c.onReject != nil {
			c.onReject(caller)
		}
		return nil, nil, fmt.Errorf("account %d is not served by this Broker", caller.UID)
	}
	if c.conns != nil {
		if !c.conns.acquire(caller.UID) {
			return nil, nil, fmt.Errorf("cannot accept a connection: account %s already has %d open",
				callerAccount(caller), c.conns.limit)
		}
		// Hold the account's slot until the server closes the connection, even
		// though the handshake itself succeeds.
		conn = &countedConn{Conn: conn, release: func() { c.conns.release(caller.UID) }}
	}
	info := &callerAuthInfo{
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity},
		caller:         caller,
	}
	return conn, info, nil
}

// Info implements credentials.TransportCredentials.
func (c *peerCredCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "peercred"}
}

// Clone implements credentials.TransportCredentials.
func (c *peerCredCreds) Clone() credentials.TransportCredentials {
	clone := *c
	return &clone
}

// OverrideServerName implements credentials.TransportCredentials.
func (c *peerCredCreds) OverrideServerName(string) error { return nil }
