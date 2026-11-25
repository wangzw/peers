package broker

import (
	"container/list"
	"context"
	"errors"
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
	"github.com/wangzw/peers/internal/broker/lookupgate"
)

// Caller is the account and process on the other end of a local connection.
type Caller struct {
	// UID is the caller's effective user id.
	UID uint32
	// GID is the caller's primary group id.
	GID uint32
	// PID is the caller's process id.
	PID int32
	// Username is the account name resolved for the uid, the decimal uid when
	// the account database has no entry for it, and empty when no lookup ran.
	Username string
}

// Local interface resource limits: connections and Register streams per
// account, streams per connection, and local connections Broker-wide.
const (
	maxLocalConnections   = 256
	maxLocalStreams       = 64
	maxLocalRegistrations = 256
	// maxLocalConnectionsTotal must stay far below the system unit's
	// LimitNOFILE of 65536.
	maxLocalConnectionsTotal = 4096
)

const localHandshakeTimeout = 120 * time.Second

// handshakeLookupTimeout matches helper.defaultAccessCheckTimeout: both wait on
// the same process-wide getent gate.
var handshakeLookupTimeout = 5 * time.Second

const maxHandshakeLookupWaiters = 64

var handshakeLookupWaiters = make(chan struct{}, maxHandshakeLookupWaiters)

func acquireHandshakeLookupWaiter() bool {
	select {
	case handshakeLookupWaiters <- struct{}{}:
		return true
	default:
		return false
	}
}

func releaseHandshakeLookupWaiter() {
	<-handshakeLookupWaiters
}

// The limits the next New applies; tests lower these variables and production
// leaves the constants above.
var (
	localConnLimit      = maxLocalConnections
	localStreamLimit    = maxLocalStreams
	localRegisterLimit  = maxLocalRegistrations
	localTotalConnLimit = maxLocalConnectionsTotal
)

func newLocalAccountLimits(log *slog.Logger) (conns, registers *accountLimit) {
	conns = newAccountLimit(localConnLimit, EvLocalConnectionLimitReached, log)
	conns.totalLimit = localTotalConnLimit
	registers = newAccountLimit(localRegisterLimit, EvRegisterLimitReached, log)
	return conns, registers
}

const unknownAccountKey = ^uint32(0)

func callerAccount(c Caller) string {
	if c.Username != "" {
		return c.Username
	}
	return strconv.FormatUint(uint64(c.UID), 10)
}

const overLimitLogInterval = time.Minute

type accountLimit struct {
	mu         sync.Mutex
	limit      int
	totalLimit int
	total      int
	event      string
	log        *slog.Logger
	counts     map[uint32]int
	loggedAt   map[uint32]time.Time
	now        func() time.Time
}

func newAccountLimit(limit int, event string, log *slog.Logger) *accountLimit {
	return &accountLimit{
		limit:    limit,
		event:    event,
		log:      log,
		counts:   make(map[uint32]int),
		loggedAt: make(map[uint32]time.Time),
	}
}

func (l *accountLimit) acquire(key, uid uint32) bool {
	if !l.acquireTotal(uid) {
		return false
	}
	if !l.acquireAccount(key, uid) {
		l.releaseTotal()
		return false
	}
	return true
}

func (l *accountLimit) acquireTotal(uid uint32) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.totalLimit > 0 && l.total >= l.totalLimit {
		l.logRefusalLocked(0, uid, "broker", l.totalLimit)
		return false
	}
	l.total++
	return true
}

func (l *accountLimit) acquireAccount(key, uid uint32) bool {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key] >= l.limit {
		l.logRefusalLocked(key, uid, "account", l.limit)
		return false
	}
	l.counts[key]++
	return true
}

func (l *accountLimit) release(key uint32) {
	l.releaseAccount(key)
	l.releaseTotal()
}

func (l *accountLimit) releaseAccount(key uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key] > 1 {
		l.counts[key]--
	} else {
		delete(l.counts, key)
	}
}

func (l *accountLimit) releaseTotal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total > 0 {
		l.total--
	}
}

// logRefusalLocked logs a limit refusal for key at most once per
// overLimitLogInterval. The caller must hold l.mu.
func (l *accountLimit) logRefusalLocked(key, uid uint32, scope string, limit int) {
	now := time.Now
	if l.now != nil {
		now = l.now
	}
	t := now()
	if last, ok := l.loggedAt[key]; ok && t.Sub(last) < overLimitLogInterval {
		return
	}
	for other, at := range l.loggedAt {
		if t.Sub(at) >= overLimitLogInterval {
			delete(l.loggedAt, other)
		}
	}
	l.loggedAt[key] = t
	l.log.Warn(l.event, "uid", uid, "limit", limit, "scope", scope)
}

// countedConn returns the account's connection slot when the gRPC server closes
// the connection; the server may close it more than once, so the slot is
// returned only once.
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
// once per connection during the gRPC handshake and does not resolve the
// account name. It returns an error when conn is not a Unix connection.
func ReadPeerCred(conn net.Conn) (Caller, error) {
	u, ok := conn.(*net.UnixConn)
	if !ok {
		return Caller{}, fmt.Errorf("cannot read peer credentials: connection is %T, not *net.UnixConn", conn)
	}
	return readPeerCred(u)
}

const usernameCacheTTL = 5 * time.Minute

const usernameFailureTTL = 30 * time.Second

type usernameCacheEntry struct {
	name    string
	expires time.Time
}

const usernameUnknownCacheMax = 1024

type unknownAccountEntry struct {
	uid     uint32
	expires time.Time
}

// unknownAccountCache is a bounded LRU cache of uids with no account entry;
// its fields are guarded by usernameMu.
type unknownAccountCache struct {
	max   int
	items map[uint32]*list.Element
	order *list.List
}

func newUnknownAccountCache(max int) *unknownAccountCache {
	return &unknownAccountCache{max: max, items: make(map[uint32]*list.Element), order: list.New()}
}

func (c *unknownAccountCache) fresh(uid uint32, now time.Time) bool {
	el, ok := c.items[uid]
	if !ok {
		return false
	}
	entry := el.Value.(*unknownAccountEntry)
	if !now.Before(entry.expires) {
		c.order.Remove(el)
		delete(c.items, uid)
		return false
	}
	c.order.MoveToFront(el)
	return true
}

func (c *unknownAccountCache) store(uid uint32, expires time.Time) {
	if el, ok := c.items[uid]; ok {
		el.Value.(*unknownAccountEntry).expires = expires
		c.order.MoveToFront(el)
		return
	}
	c.items[uid] = c.order.PushFront(&unknownAccountEntry{uid: uid, expires: expires})
	for c.order.Len() > c.max {
		back := c.order.Back()
		c.order.Remove(back)
		delete(c.items, back.Value.(*unknownAccountEntry).uid)
	}
}

func (c *unknownAccountCache) len() int { return len(c.items) }

var (
	// lookupUsername and usernameNow are test seams.
	lookupUsername = accountdb.LookupUID
	usernameNow    = time.Now
	// usernameWaiterHook, when set by a test, runs after a caller decides to
	// wait for an in-flight lookup and before it blocks.
	usernameWaiterHook func(uid uint32)

	// usernameMu guards usernameCache, unknownAccounts and usernameInflight.
	usernameMu       sync.Mutex
	usernameCache    = make(map[uint32]usernameCacheEntry)
	unknownAccounts  = newUnknownAccountCache(usernameUnknownCacheMax)
	usernameInflight = make(map[uint32]chan struct{})
)

func usernameFor(uid uint32) string {
	name, _, _ := resolveUsername(context.Background(), uid)
	return name
}

// accountKeyFor returns the key that per-account limits count uid under; a
// lookup failure is returned as an error to be refused, not treated as an
// unknown account.
func accountKeyFor(uid uint32) (uint32, error) {
	ctx, cancel := context.WithTimeout(context.Background(), handshakeLookupTimeout)
	defer cancel()
	_, known, err := resolveUsername(ctx, uid)
	if err != nil {
		return 0, err
	}
	if !known {
		return unknownAccountKey, nil
	}
	return uid, nil
}

func cachedRealUsername(uid uint32) (name string, stale, ok bool) {
	now := usernameNow()
	usernameMu.Lock()
	defer usernameMu.Unlock()
	entry, found := usernameCache[uid]
	if !found {
		return "", false, false
	}
	return entry.name, !now.Before(entry.expires), true
}

// resolveUsername returns the account name for uid and whether the account
// database has an entry for it; a lookup failure is returned as an error and
// must be handled fail closed.
func resolveUsername(ctx context.Context, uid uint32) (string, bool, error) {
	id := strconv.FormatUint(uint64(uid), 10)
	now := usernameNow()
	usernameMu.Lock()
	for {
		if entry, ok := usernameCache[uid]; ok {
			if now.Before(entry.expires) {
				usernameMu.Unlock()
				return entry.name, true, nil
			}
			usernameMu.Unlock()
			refreshUsername(uid)
			return entry.name, true, nil
		}
		if unknownAccounts.fresh(uid, now) {
			usernameMu.Unlock()
			return id, false, nil
		}
		if wait, ok := usernameInflight[uid]; ok {
			usernameMu.Unlock()
			if hook := usernameWaiterHook; hook != nil {
				hook(uid)
			}
			select {
			case <-wait:
			case <-ctx.Done():
				return id, false, ctx.Err()
			}
			now = usernameNow()
			usernameMu.Lock()
			continue
		}
		break
	}
	done := make(chan struct{})
	usernameInflight[uid] = done
	usernameMu.Unlock()

	release, gateErr := lookupgate.Acquire(ctx)
	if gateErr != nil {
		usernameMu.Lock()
		delete(usernameInflight, uid)
		usernameMu.Unlock()
		close(done)
		return id, false, gateErr
	}
	u, lookupErr := lookupUsername(uid)
	release()

	now = usernameNow()
	usernameMu.Lock()
	defer usernameMu.Unlock()
	delete(usernameInflight, uid)
	close(done)

	switch {
	case lookupErr == nil && u.Name != "":
		storeRealUsernameLocked(uid, usernameCacheEntry{name: u.Name, expires: now.Add(usernameCacheTTL)})
		return u.Name, true, nil
	case lookupErr == nil || errors.Is(lookupErr, accountdb.ErrNotFound):
		unknownAccounts.store(uid, now.Add(usernameFailureTTL))
		return id, false, nil
	default:
		return id, false, lookupErr
	}
}

// storeRealUsernameLocked writes one real account's cache entry. The caller must
// hold usernameMu.
func storeRealUsernameLocked(uid uint32, entry usernameCacheEntry) {
	usernameCache[uid] = entry
}

func refreshUsername(uid uint32) {
	usernameMu.Lock()
	if _, running := usernameInflight[uid]; running {
		usernameMu.Unlock()
		return
	}
	done := make(chan struct{})
	usernameInflight[uid] = done
	usernameMu.Unlock()

	go func() {
		defer close(done)

		ctx, cancel := context.WithTimeout(context.Background(), handshakeLookupTimeout)
		defer cancel()
		release, err := lookupgate.Acquire(ctx)
		if err != nil {
			usernameMu.Lock()
			delete(usernameInflight, uid)
			usernameMu.Unlock()
			return
		}
		u, lookupErr := lookupUsername(uid)
		release()

		now := usernameNow()
		usernameMu.Lock()
		defer usernameMu.Unlock()
		defer delete(usernameInflight, uid)

		entry, present := usernameCache[uid]
		switch {
		case lookupErr == nil && u.Name != "":
			storeRealUsernameLocked(uid, usernameCacheEntry{name: u.Name, expires: now.Add(usernameCacheTTL)})
		case lookupErr == nil || errors.Is(lookupErr, accountdb.ErrNotFound):
			delete(usernameCache, uid)
			unknownAccounts.store(uid, now.Add(usernameFailureTTL))
		case present:
			entry.expires = now.Add(usernameFailureTTL)
			usernameCache[uid] = entry
		}
	}()
}

type callerAuthInfo struct {
	credentials.CommonAuthInfo
	caller                Caller
	rejected              atomic.Bool
	rejectedClientVersion atomic.Uint32
	rejectedBrokerVersion atomic.Uint32
}

func (a *callerAuthInfo) AuthType() string { return "peercred" }

func authInfoFrom(ctx context.Context) (*callerAuthInfo, bool) {
	p, ok := peer.FromContext(ctx)
	if !ok || p.AuthInfo == nil {
		return nil, false
	}
	info, ok := p.AuthInfo.(*callerAuthInfo)
	return info, ok
}

// CallerFrom returns the caller identified on the connection that carries ctx,
// and false when ctx carries none.
func CallerFrom(ctx context.Context) (Caller, bool) {
	info, ok := authInfoFrom(ctx)
	if !ok {
		return Caller{}, false
	}
	return info.caller, true
}

type peerCredCreds struct {
	read     func(net.Conn) (Caller, error)
	allow    func(Caller) bool
	onReject func(Caller)
	// conns bounds the connections one account may hold; nil serves without
	// the bound.
	conns *accountLimit
}

func newPeerCredCreds(read func(net.Conn) (Caller, error), allow func(Caller) bool, onReject func(Caller), conns *accountLimit) credentials.TransportCredentials {
	return &peerCredCreds{read: read, allow: allow, onReject: onReject, conns: conns}
}

func (c *peerCredCreds) brokerFullError() error {
	return fmt.Errorf("cannot accept a connection: the Broker already has %d open", c.conns.totalLimit)
}

func (c *peerCredCreds) accountFullError(caller Caller) error {
	return fmt.Errorf("cannot accept a connection: account %s already has %d open",
		callerAccount(caller), c.conns.limit)
}

func (c *peerCredCreds) ClientHandshake(context.Context, string, net.Conn) (net.Conn, credentials.AuthInfo, error) {
	return nil, nil, fmt.Errorf("cannot read peer credentials on the client side")
}

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
		name, stale, cached := cachedRealUsername(caller.UID)
		if cached {
			if !c.conns.acquireTotal(caller.UID) {
				return nil, nil, c.brokerFullError()
			}
			caller.Username = name
			if stale {
				refreshUsername(caller.UID)
			}
			if !c.conns.acquireAccount(caller.UID, caller.UID) {
				c.conns.releaseTotal()
				return nil, nil, c.accountFullError(caller)
			}
			conn = &countedConn{Conn: conn, release: func() {
				c.conns.releaseAccount(caller.UID)
				c.conns.releaseTotal()
			}}
		} else {
			// An uncached connection takes the lookup wait slot before a global
			// slot, looks up the account, then takes the account slot; the order
			// must not change, or getent-slow connections fill every global slot.
			if !acquireHandshakeLookupWaiter() {
				return nil, nil, fmt.Errorf("cannot accept a connection: %d connections are already waiting for an account lookup",
					maxHandshakeLookupWaiters)
			}
			if !c.conns.acquireTotal(caller.UID) {
				releaseHandshakeLookupWaiter()
				return nil, nil, c.brokerFullError()
			}
			ctx, cancel := context.WithTimeout(context.Background(), handshakeLookupTimeout)
			name, known, lookupErr := resolveUsername(ctx, caller.UID)
			cancel()
			releaseHandshakeLookupWaiter()
			if lookupErr != nil {
				c.conns.releaseTotal()
				return nil, nil, fmt.Errorf("cannot accept a connection: looking up account %d: %w", caller.UID, lookupErr)
			}
			caller.Username = name
			key := caller.UID
			if !known {
				key = unknownAccountKey
			}
			if !c.conns.acquireAccount(key, caller.UID) {
				c.conns.releaseTotal()
				return nil, nil, c.accountFullError(caller)
			}
			conn = &countedConn{Conn: conn, release: func() {
				c.conns.releaseAccount(key)
				c.conns.releaseTotal()
			}}
		}
	} else if caller.Username == "" {
		caller.Username = usernameFor(caller.UID)
	}
	info := &callerAuthInfo{
		CommonAuthInfo: credentials.CommonAuthInfo{SecurityLevel: credentials.NoSecurity},
		caller:         caller,
	}
	return conn, info, nil
}

func (c *peerCredCreds) Info() credentials.ProtocolInfo {
	return credentials.ProtocolInfo{SecurityProtocol: "peercred"}
}

func (c *peerCredCreds) Clone() credentials.TransportCredentials {
	clone := *c
	return &clone
}

func (c *peerCredCreds) OverrideServerName(string) error { return nil }
