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
//
// maxLocalConnectionsTotal bounds every local connection at once, across all
// accounts. It must stay far below the system unit's LimitNOFILE of 65536:
// each accepted connection holds at least one descriptor, and the Broker also
// keeps helper pipes, the link listener and its configuration files open, so
// 4096 leaves headroom for all of them.
const (
	maxLocalConnections      = 256
	maxLocalStreams          = 64
	maxLocalRegistrations    = 256
	maxLocalConnectionsTotal = 4096
)

// localHandshakeTimeout bounds one local connection's handshake. It is the
// gRPC server's connection timeout: grpc's documented default is 120 seconds,
// and setting the option explicitly keeps that existing bound in place.
const localHandshakeTimeout = 120 * time.Second

// handshakeLookupTimeout is the bound applied to one account lookup during the
// handshake. It is the same five seconds a configuration permission check
// allows a check to start within (helper.defaultAccessCheckTimeout): the two
// wait on the same process-wide getent gate, so a lookup that cannot take a
// gate slot in five seconds is refused for the same reason a permission check
// is. Using the 120-second connection timeout instead would let one blocked
// account database hold connection slots for two minutes. It is a variable so
// tests can lower it.
var handshakeLookupTimeout = 5 * time.Second

// maxHandshakeLookupWaiters bounds the connections that may wait for an
// account lookup at once. The lookup gate runs four getent processes, so a
// connection that is not already cached can wait up to handshakeLookupTimeout
// behind it. Without this separate bound, socket connections arrive faster
// than getent answers and fill every global connection slot with waiters; a
// cached real account would then be refused too. A connection past the bound
// is refused at once instead of queued.
const maxHandshakeLookupWaiters = 64

// handshakeLookupWaiters is the semaphore of connections admitted to wait for
// a lookup. It is a variable so tests can shrink it.
var handshakeLookupWaiters = make(chan struct{}, maxHandshakeLookupWaiters)

// acquireHandshakeLookupWaiter admits one connection to the lookup wait and
// reports whether it was below the bound. It never blocks.
func acquireHandshakeLookupWaiter() bool {
	select {
	case handshakeLookupWaiters <- struct{}{}:
		return true
	default:
		return false
	}
}

// releaseHandshakeLookupWaiter returns one lookup wait slot.
func releaseHandshakeLookupWaiter() {
	<-handshakeLookupWaiters
}

// localConnLimit, localStreamLimit, localRegisterLimit and
// localTotalConnLimit are the limits the next New applies. They are variables
// so tests can lower them; production always leaves the constants above.
var (
	localConnLimit      = maxLocalConnections
	localStreamLimit    = maxLocalStreams
	localRegisterLimit  = maxLocalRegistrations
	localTotalConnLimit = maxLocalConnectionsTotal
)

// newLocalAccountLimits builds the connection and registration limits shared by
// the full and the diagnostics-only local service.
func newLocalAccountLimits(log *slog.Logger) (conns, registers *accountLimit) {
	conns = newAccountLimit(localConnLimit, EvLocalConnectionLimitReached, log)
	conns.totalLimit = localTotalConnLimit
	registers = newAccountLimit(localRegisterLimit, EvRegisterLimitReached, log)
	return conns, registers
}

// unknownAccountKey is the account key shared by every uid with no account
// database entry. A normal account may own a subuid range, so a rootless
// container can connect from tens of thousands of uids that resolve to no
// account; without a shared key each uid would get a private share of every
// per-account limit. The unknown uids keep the ordinary per-account quota
// between them instead of being refused outright, so rootless containers can
// still connect.
const unknownAccountKey = ^uint32(0)

// callerAccount returns the name used for a caller in messages and in display:
// the resolved account name, or the decimal uid when it has none. The peercred
// handshake stores usernameFor(uid), which already falls back to the decimal
// uid; an agent built without that handshake falls back the same way.
func callerAccount(c Caller) string {
	if c.Username != "" {
		return c.Username
	}
	return strconv.FormatUint(uint64(c.UID), 10)
}

// overLimitLogInterval is how long one account's over-limit warning is
// suppressed. A client that keeps retrying a full quota would otherwise write
// one record per attempt.
const overLimitLogInterval = time.Minute

// accountLimit counts concurrent local resources per account and refuses the
// ones past its limit. It also counts a global total when totalLimit is set.
// A refusal is logged at most once per account per overLimitLogInterval, so a
// client that keeps retrying cannot flood the runtime log.
type accountLimit struct {
	mu         sync.Mutex
	limit      int
	totalLimit int // 0 means no global total
	total      int
	event      string
	log        *slog.Logger
	counts     map[uint32]int
	loggedAt   map[uint32]time.Time
	// now is a test seam; nil means time.Now.
	now func() time.Time
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

// acquire takes one slot for key and reports whether it was below both the
// account's limit and the global total. uid is the caller whose refusal is
// logged; key is the account bucket, which is unknownAccountKey for every uid
// without an account entry.
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

// acquireTotal takes one slot of the global total and reports whether it was
// below the limit. It is separate from acquireAccount so the peer-credential
// handshake can count a connection before it looks up the caller's account: a
// lookup queue must not grow past the global connection limit.
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

// acquireAccount takes one slot for key and reports whether it was below the
// account's limit.
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

// release returns one account slot and one global slot.
func (l *accountLimit) release(key uint32) {
	l.releaseAccount(key)
	l.releaseTotal()
}

// releaseAccount returns one slot for key.
func (l *accountLimit) releaseAccount(key uint32) {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.counts[key] > 1 {
		l.counts[key]--
	} else {
		delete(l.counts, key)
	}
}

// releaseTotal returns one global slot.
func (l *accountLimit) releaseTotal() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.total > 0 {
		l.total--
	}
}

// logRefusalLocked writes the refusal record unless the same account was
// already recorded within overLimitLogInterval. The caller must hold l.mu.
func (l *accountLimit) logRefusalLocked(key, uid uint32, scope string, limit int) {
	now := time.Now
	if l.now != nil {
		now = l.now
	}
	t := now()
	if last, ok := l.loggedAt[key]; ok && t.Sub(last) < overLimitLogInterval {
		return
	}
	// Drop records whose minute has passed so the map tracks only the accounts
	// currently over a limit.
	for other, at := range l.loggedAt {
		if t.Sub(at) >= overLimitLogInterval {
			delete(l.loggedAt, other)
		}
	}
	l.loggedAt[key] = t
	l.log.Warn(l.event, "uid", uid, "limit", limit, "scope", scope)
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
// once per connection during the gRPC handshake. It does not resolve the
// account name: the handshake fills it after taking the connection's global
// slot, so a blocked account database cannot hold a handshake before it is
// counted.
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

// usernameFailureTTL bounds how long "no such account" is reused. A uid with
// no account entry, such as a subuid mapped by a rootless container, would
// otherwise cost one lookup per connection for as long as it keeps connecting.
// It is also how long a failed refresh of a known account waits before the
// next attempt.
const usernameFailureTTL = 30 * time.Second

// usernameCacheEntry is one cached real-account name. The cache holds only a
// uid the account database names; a definite "no such account" goes into the
// separate negative cache below. A lookup failure for any other reason is not
// cached: it refuses the request that ran it, and the next request asks the
// account database again instead of reusing the failure.
type usernameCacheEntry struct {
	name    string
	expires time.Time
}

// usernameUnknownCacheMax bounds the negative cache of uids with no account
// entry. A normal account can own a subuid range, so the set of such uids is
// not small; the cache keeps the most recently used entries and evicts the
// least recently used one.
const usernameUnknownCacheMax = 1024

// unknownAccountEntry is one uid the account database has no entry for.
type unknownAccountEntry struct {
	uid     uint32
	expires time.Time
}

// unknownAccountCache is the negative cache of uids with no account entry. It
// is a least-recently-used list bounded at max entries: a hit moves the uid to
// the front and writing a new uid evicts the back. It is kept apart from
// usernameCache so a rootless container rotating through subuids cannot
// displace the cached name of a real account.
type unknownAccountCache struct {
	max   int
	items map[uint32]*list.Element
	order *list.List
}

func newUnknownAccountCache(max int) *unknownAccountCache {
	return &unknownAccountCache{max: max, items: make(map[uint32]*list.Element), order: list.New()}
}

// fresh reports whether uid is cached as unknown and not past its expiry, and
// moves it to the front when it is. The caller must hold usernameMu.
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

// store records uid as unknown until expires, refreshing its place or evicting
// the least recently used uid when the cache is full. The caller must hold
// usernameMu.
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

// len reports how many uids are cached. The caller must hold usernameMu.
func (c *unknownAccountCache) len() int { return len(c.items) }

var (
	// lookupUsername and usernameNow are seams for tests.
	lookupUsername = accountdb.LookupUID
	usernameNow    = time.Now
	// usernameWaiterHook, when set by a test, runs after a caller has decided to
	// wait for an in-flight lookup and before it blocks. It lets a test wait
	// until every caller has reached the coalescing path instead of sleeping.
	usernameWaiterHook func(uid uint32)

	usernameMu sync.Mutex
	// usernameCache holds every real account that has connected. Its size is
	// bounded by the number of accounts on the host: only a uid the account
	// database names is stored here, so a rotation of uids with no entry never
	// displaces one. There is no entry cap.
	usernameCache = make(map[uint32]usernameCacheEntry)
	// unknownAccounts is the negative cache, separate so unknown uids cannot
	// crowd out a real account's name.
	unknownAccounts = newUnknownAccountCache(usernameUnknownCacheMax)
	// usernameInflight holds the uid whose lookup is running and the channel
	// closed when it finishes. A caller that misses the cache while another
	// lookup for the same uid is running waits on that channel instead of
	// starting a second lookup.
	usernameInflight = make(map[uint32]chan struct{})
)

// usernameFor returns the account name for uid, or the decimal uid when the
// account cannot be looked up. The result is display data: a failed lookup
// still returns the decimal uid, while the enforcement paths below reject the
// request instead.
func usernameFor(uid uint32) string {
	name, _, _ := resolveUsername(context.Background(), uid)
	return name
}

// accountKeyFor returns the key that per-account limits count uid under. A uid
// with no account entry shares unknownAccountKey with every other such uid, so
// a rotating subuid cannot multiply a per-account quota. A lookup that fails
// for any other reason is an error: the caller must refuse the request rather
// than treat the uid as unknown.
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

// cachedRealUsername returns the cached name of a uid the account database has
// an entry for, and whether that entry has expired. It never blocks and never
// starts a lookup. A known account stays usable after its entry expires
// because the name is display data, and using it keeps the account out of the
// lookup queue when the database is blocked; the caller refreshes the entry.
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

// resolveUsername returns the account name for uid, whether the account
// database has an entry for it, and the lookup failure when the database could
// not answer. A uid with no entry is a decision (known is false, err is nil); a
// failure is reported as an error and must be handled fail closed, and it is
// not cached, so the next request asks the database again. A real account
// whose entry expired keeps its cached name and is refreshed in the
// background. Misses for one uid are coalesced, lookups are bounded in number
// at once by the process-wide gate, the negative cache is bounded, and the
// real-account cache grows only with the accounts the host has.
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
			// The account is still real. Refresh behind the caller instead
			// of making it wait for a getent slot: expiry alone must not
			// push a real account into the lookup queue.
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
		// No account entry. The uid shares the unknown bucket and its own
		// negative cache.
		unknownAccounts.store(uid, now.Add(usernameFailureTTL))
		return id, false, nil
	default:
		// A failure or timeout answers only this request. Caching it would
		// refuse a real account for the failure TTL instead of asking again.
		return id, false, lookupErr
	}
}

// storeRealUsernameLocked writes one real account's cache entry. The caller
// must hold usernameMu.
func storeRealUsernameLocked(uid uint32, entry usernameCacheEntry) {
	usernameCache[uid] = entry
}

// refreshUsername rereads a real account whose cached entry expired, without
// blocking the caller that found the entry. Only one refresh runs per uid at a
// time. A refresh that fails keeps the cached name and waits usernameFailureTTL
// before the next attempt; only a definitive "no such account" removes the
// entry and demotes the uid to the negative cache, because that is the database
// saying the account is gone.
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
			// Keep the cached name; a later connection tries again.
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
			// Definitive "no such account": drop the real entry and demote the
			// uid to the negative cache, where it shares the unknown bucket
			// until the account reappears.
			delete(usernameCache, uid)
			unknownAccounts.store(uid, now.Add(usernameFailureTTL))
		case present:
			// Keep the known name and back off before the next attempt.
			entry.expires = now.Add(usernameFailureTTL)
			usernameCache[uid] = entry
		}
	}()
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

// brokerFullError is the handshake error when every global connection slot is
// taken.
func (c *peerCredCreds) brokerFullError() error {
	return fmt.Errorf("cannot accept a connection: the Broker already has %d open", c.conns.totalLimit)
}

// accountFullError is the handshake error when the caller's account bucket is
// at its connection limit.
func (c *peerCredCreds) accountFullError(caller Caller) error {
	return fmt.Errorf("cannot accept a connection: account %s already has %d open",
		callerAccount(caller), c.conns.limit)
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
		// A uid already cached as a real account takes its global slot and
		// skips the lookup queue. Its name is display data, and an expired
		// entry is refreshed behind the connection, so a blocked account
		// database cannot refuse an account it has already resolved.
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
			// Hold the account's slot until the server closes the connection,
			// even though the handshake itself succeeds.
			conn = &countedConn{Conn: conn, release: func() {
				c.conns.releaseAccount(caller.UID)
				c.conns.releaseTotal()
			}}
		} else {
			// Admit the connection to the lookup wait before taking a global
			// slot. The wait is bounded separately, so connections arriving
			// faster than getent answers cannot fill every global slot; past
			// the bound the connection is refused at once instead of queued.
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
		// Without a slot set there is no account lookup to bound; keep the
		// display name usable for callers that build these credentials directly.
		caller.Username = usernameFor(caller.UID)
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
