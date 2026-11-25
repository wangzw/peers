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

type ticker interface {
	C() <-chan time.Time
	Stop()
}

type realTicker struct{ *time.Ticker }

func (t realTicker) C() <-chan time.Time { return t.Ticker.C }
func (t realTicker) Stop()               { t.Ticker.Stop() }

func newRealTicker(d time.Duration) ticker { return realTicker{time.NewTicker(d)} }

type addressSource interface {
	DefaultRouteAddresses() ([]netip.Addr, error)
}

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
	// only by root; a test seam, and low listen ports are allowed only while it
	// returns true.
	configFileRootOnly func(path string) (bool, error)
	// systemPaths overrides the fixed system path set; a test seam placing the
	// system paths under a temporary directory.
	systemPaths func() PathSet
}

var defaultLogger = func() *slog.Logger { return slog.Default() }

// lookupAccount reads one entry of the account database; a seam so a test can
// state its answer, including its failure, instead of using the host's database.
var lookupAccount = accountdb.LookupUID

func accountHomeDir() (string, error) {
	if u, err := lookupAccount(uint32(os.Getuid())); err == nil && u.Home != "" {
		return u.Home, nil
	}
	return os.UserHomeDir()
}

// homeDirSource is the seam the peers_testenv build replaces; production code never replaces it.
var homeDirSource = accountHomeDir

func runningAccountName() (string, error) {
	u, err := lookupAccount(uint32(os.Geteuid()))
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

// accountString returns the account name the Broker runs as, or its numeric uid
// when the lookup fails or the name is empty; a system Broker has no account
// part and never looks one up.
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

// close stops every goroutine and open file; it is idempotent.
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
