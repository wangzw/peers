package main

import (
	"context"
	"io"
	"net/http"

	"github.com/wangzw/peers/internal/broker/update"
)

// brokerOptions is the subset of the Broker's run options that the command
// line controls. It keeps cmd independent of the broker package so that the
// subcommands can be built and tested before the Broker's entry point exists.
type brokerOptions struct {
	System        bool
	Listen        string
	AllowDegraded bool
	Version       string
	LogOutput     io.Writer
}

// startFailure classifies a Broker startup error into the exit-code families
// the command line offers. The zero value is failOther so an unknown or
// unclassified error falls back to the generic failure exit code.
type startFailure int

const (
	failOther startFailure = iota
	failAlreadyRunning
	failListenUnavailable
	failInvalidConfig
	failInSession
	failNotRoot
	failRuntimeDir
	failInvalidListen
)

// Process exit codes. They are split by cause so a service manager and the
// subprocess tests can tell the failures apart.
const (
	exitOK             = 0
	exitFailure        = 1
	exitUsage          = 2
	exitAlreadyRunning = 3
	// exitServiceDegraded reports that the service is installed and running
	// but its Broker runs degraded. Only service install, start and restart
	// use it; it shares 3 with exitAlreadyRunning, which belongs to the broker
	// command.
	exitServiceDegraded   = 3
	exitListenUnavailable = 4
	exitInvalidConfig     = 5
	exitInSession         = 6
	exitNotRoot           = 7
	exitRuntimeDir        = 8
)

// exitCodeFor maps a Broker startup failure to the process exit code. A usage
// problem in the listen address shares the generic usage code; any unclassified
// failure uses the generic failure code.
func exitCodeFor(f startFailure) int {
	switch f {
	case failAlreadyRunning:
		return exitAlreadyRunning
	case failListenUnavailable:
		return exitListenUnavailable
	case failInvalidConfig:
		return exitInvalidConfig
	case failInSession:
		return exitInSession
	case failNotRoot:
		return exitNotRoot
	case failRuntimeDir:
		return exitRuntimeDir
	case failInvalidListen:
		return exitUsage
	default:
		return exitFailure
	}
}

// deps holds the upstream capabilities the command line needs. Each field is a
// function so tests can substitute a stub and the wiring point can be checked
// by the compiler.
type deps struct {
	// euid reports the executing account. The level rules use it when neither
	// --system nor --user is given; tests substitute it through this field
	// instead of a package-level seam.
	euid func() int

	runBroker         func(ctx context.Context, o brokerOptions) error
	classifyBrokerErr func(err error) startFailure
	runMCP            func(ctx context.Context) error // MCP over the process's own stdin/stdout

	// The three commands below connect to the level the command selected, not
	// to the Broker of the calling account: their system argument names the
	// system socket when true and the user one when false, and
	// explicit says whether a level option named it.
	runStatus    func(ctx context.Context, system, explicit bool, stdout io.Writer) error
	runAudit     func(ctx context.Context, system, explicit bool, stdout io.Writer, limit uint32) error
	runSetOnline func(ctx context.Context, system, explicit, online bool, stdout io.Writer) error

	// Broker configuration and pairing commands. The Broker is selected by the
	// calling account, with no explicit level; warn carries the warnings of a
	// successful broker connect. peerNames supplies the disconnect completion
	// candidates: the aliases and fingerprints of the paired Brokers.
	runConfigGet        func(ctx context.Context, key string, stdout io.Writer) error
	runConfigSet        func(ctx context.Context, key, value string, stdout io.Writer) error
	runPairingKeyCreate func(ctx context.Context, stdout io.Writer) error
	runPeerList         func(ctx context.Context, stdout io.Writer) error
	runPeerConnect      func(ctx context.Context, address, key, alias string, stdout, warn io.Writer) error
	runPeerDisconnect   func(ctx context.Context, broker string, stdout io.Writer) error
	peerNames           func(ctx context.Context) ([]string, error)

	// Update command. The command asks the selected Broker to install the
	// newest release itself. When that Broker is this account's
	// own and declines, the command replaces the user binary locally.
	updateAPIBase        func() string
	updateLatest         func(ctx context.Context, client *http.Client, api string) (update.Release, error)
	updateNewer          func(current, candidate string) bool
	updateApply          func(ctx context.Context, o update.ApplyOptions) error
	applyUpdateViaBroker func(ctx context.Context) (brokerUpdateResult, error)
	brokerUpdateStatus   func(ctx context.Context) (brokerUpdateStatus, error)
	// restartService is a service operation the update command must not
	// perform. The delegated route leaves restarting to the Broker's service
	// manager, and a local replacement leaves an installed service alone
	// because it may have been stopped on purpose, so nothing calls this. It
	// exists so the test double records an attempt: a change that restarts the
	// service from the command is caught instead of slipping through.
	restartService func(ctx context.Context) error
}

// brokerUpdateResult is the outcome of the Broker's ApplyUpdate RPC as the
// command line needs it. Keeping a local value type lets the command be built
// and tested without the generated local client.
type brokerUpdateResult struct {
	FromVersion string
	ToVersion   string
	Restarting  bool
	// UserLevel reports whether the Broker that answered runs as this
	// account's user service. A timeout names that Broker's log.
	UserLevel bool
}

// brokerUpdateStatus is the part of UpdateStatus the command reads while it
// waits for a restarted Broker.
type brokerUpdateStatus struct {
	CurrentVersion string
}
