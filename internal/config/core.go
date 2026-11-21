package config

import (
	"log/slog"
	"net/netip"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/accountdb"
)

// ticker is the small part of time.Ticker this package uses.
type ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct{ *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.Ticker.C }
func (t realTicker) Stop()               { t.Ticker.Stop() }

func newRealTicker(d time.Duration) ticker { return realTicker{time.NewTicker(d)} }

// addressSource supplies the addresses of the interface that carries the
// default route.
type addressSource interface {
	DefaultRouteAddresses() ([]netip.Addr, error)
}

// core holds all state and dependencies. Package-level functions only fetch the
// singleton and call a method on it, so tests can build an independent core.
type core struct {
	mu sync.Mutex

	mode   Mode
	paths  PathSet
	values map[string]Value

	effectiveListen listenAddr
	listenOverride  bool

	explicitAlias bool

	advertised  []AdvertisedAddr
	routes      []netip.Addr
	routesValid bool

	validators map[string][]func(old, new Value) error
	subs       map[string][]func(old, new Value)
	advSubs    []func(old, new []AdvertisedAddr)

	notify notifier

	// auditMu guards the audit pointer. Audit and ReadAudit run without
	// core.mu, while close clears the pointer, so the pointer needs its own
	// lock.
	auditMu sync.RWMutex
	audit   *auditLog

	watch *configWatcher

	file              *configDocument
	lastFileHash      [32]byte
	lastIgnoredListen string

	closed        bool
	noBackground  bool
	skipDataCheck bool

	addrTicker     ticker
	addrTickerDone chan struct{}

	now          func() time.Time
	newTicker    func(time.Duration) ticker
	hostname     func() (string, error)
	accountName  func() (string, error)
	homeDir      func() (string, error)
	euid         func() int
	routesSrc    addressSource
	writeFile    func(path string, data []byte, mode Mode) error
	readFile     func(path string) ([]byte, error)
	statFile     func(path string) (os.FileInfo, error)
	logger       *slog.Logger
	checkRuntime func() (string, error)

	// configFileRootOnly reports whether the configuration file may be written
	// only by root. It is a seam so tests can state the permission rule
	// without being root. lowPrivileged listen ports are allowed only while it
	// returns true.
	configFileRootOnly func(path string) (bool, error)
	// systemPaths overrides the fixed system path set. It is a seam tests use
	// to place the system paths under a temporary directory.
	systemPaths func() PathSet
}

var defaultLogger = func() *slog.Logger { return slog.Default() }

func accountHomeDir() (string, error) {
	if u, err := accountdb.LookupUID(uint32(os.Getuid())); err == nil && u.Home != "" {
		return u.Home, nil
	}
	return os.UserHomeDir()
}

// homeDirSource is the seam the peers_testenv build replaces; production code never replaces it.
var homeDirSource = accountHomeDir

// runningAccountName looks up the name of the account the Broker runs as, the
// same account database the audit log takes its account names from.
func runningAccountName() (string, error) {
	u, err := accountdb.LookupUID(uint32(os.Geteuid()))
	if err != nil {
		return "", err
	}
	return u.Name, nil
}

func newCore() *core {
	hostname := os.Hostname
	return &core{
		values:             make(map[string]Value),
		validators:         make(map[string][]func(old, new Value) error),
		subs:               make(map[string][]func(old, new Value)),
		now:                time.Now,
		newTicker:          newRealTicker,
		hostname:           hostname,
		accountName:        runningAccountName,
		homeDir:            homeDirSource,
		euid:               os.Geteuid,
		routesSrc:          platformAddressSource{},
		writeFile:          writeConfigFile,
		readFile:           readConfigFileLimited,
		statFile:           os.Stat,
		logger:             defaultLogger(),
		configFileRootOnly: configFileRootOnlyWritable,
	}
}

// hostnameString returns the sanitized hostname without taking the lock; the
// caller must hold it or be initializing the core.
func (c *core) hostnameString() string {
	return sanitizeHostnameFrom(c.hostname, c.logger)
}

func sanitizeHostnameFrom(fn func() (string, error), logger *slog.Logger) string {
	name, err := fn()
	if err != nil {
		logger.Error("config: cannot read the system hostname", "error", err)
		return ""
	}
	return sanitizeHostname(name)
}

// accountString returns the raw account name the Broker runs as. A system
// level Broker has no account part, so it never looks one up. When the lookup
// fails or yields no name, the numeric effective uid takes its place: it is
// stable for the process and unique on the host. That fallback is logged at
// warning level, since the alias then shows an id instead of a name.
func (c *core) accountString() string {
	if c.mode != ModeUser {
		return ""
	}
	name, err := c.accountName()
	if err == nil && name != "" {
		return name
	}
	uid := strconv.Itoa(os.Geteuid())
	c.logger.Warn("config: cannot look up the account name; using the uid instead",
		"uid", uid, "error", err)
	return uid
}

// defaultAliasLocked derives the default alias for the current deployment
// mode, hostname and account. The caller must hold c.mu or be initializing the
// core.
func (c *core) defaultAliasLocked() string {
	return defaultAlias(c.mode, c.hostnameString(), c.accountString())
}

// close stops every goroutine and open file. It is idempotent.
func (c *core) close() {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return
	}
	c.closed = true
	w := c.watch
	stop := c.addrTickerDone
	t := c.addrTicker
	c.watch = nil
	c.addrTicker = nil
	notify := c.notify
	c.auditMu.Lock()
	audit := c.audit
	c.audit = nil
	c.auditMu.Unlock()
	c.mu.Unlock()

	if w != nil {
		w.stop()
	}
	if t != nil {
		t.Stop()
	}
	if stop != nil {
		close(stop)
	}
	if notify != nil {
		notify.close()
	}
	if audit != nil {
		_ = audit.close()
	}
}
