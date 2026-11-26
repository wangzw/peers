package main

import (
	"context"
	"io"
	"net/http"

	"github.com/wangzw/peers/internal/broker/update"
)

type brokerOptions struct {
	System        bool
	Listen        string
	AllowDegraded bool
	Version       string
	LogOutput     io.Writer
}

// startFailure classifies a Broker startup error. The zero value is failOther,
// so an unclassified error exits with the generic failure code.
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

// Process exit codes, split by cause so a service manager and the subprocess
// tests can tell the failures apart.
const (
	exitOK             = 0
	exitFailure        = 1
	exitUsage          = 2
	exitAlreadyRunning = 3
	// exitServiceDegraded reports an installed and running service whose Broker
	// runs degraded; it shares 3 with exitAlreadyRunning, which belongs to the
	// broker command.
	exitServiceDegraded   = 3
	exitListenUnavailable = 4
	exitInvalidConfig     = 5
	exitInSession         = 6
	exitNotRoot           = 7
	exitRuntimeDir        = 8
)

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

// deps holds the upstream capabilities the command line needs; each is a
// function so a test can substitute a stub.
type deps struct {
	euid func() int

	runBroker         func(ctx context.Context, o brokerOptions) error
	classifyBrokerErr func(err error) startFailure
	runMCP            func(ctx context.Context) error

	// The three commands below connect to the level the command selected rather
	// than the calling account's Broker: system names the system socket, and
	// explicit says whether a level option named it.
	runStatus    func(ctx context.Context, system, explicit bool, stdout io.Writer) error
	runAudit     func(ctx context.Context, system, explicit bool, stdout io.Writer, limit uint32) error
	runSetOnline func(ctx context.Context, system, explicit, online bool, stdout io.Writer) error

	runConfigGet        func(ctx context.Context, key string, stdout io.Writer) error
	runConfigSet        func(ctx context.Context, key, value string, stdout io.Writer) error
	runPairingKeyCreate func(ctx context.Context, stdout io.Writer) error
	runPeerList         func(ctx context.Context, stdout io.Writer) error
	runPeerConnect      func(ctx context.Context, address, key, alias string, stdout, warn io.Writer) error
	runPeerDisconnect   func(ctx context.Context, broker string, stdout io.Writer) error
	peerNames           func(ctx context.Context) ([]string, error)

	updateAPIBase        func() string
	updateLatest         func(ctx context.Context, client *http.Client, api string) (update.Release, error)
	updateNewer          func(current, candidate string) bool
	updateApply          func(ctx context.Context, o update.ApplyOptions) error
	applyUpdateViaBroker func(ctx context.Context) (brokerUpdateResult, error)
	brokerUpdateStatus   func(ctx context.Context) (brokerUpdateStatus, error)
	// restartService is never called: the delegated route leaves restarting to the
	// service manager, and a local replacement must not touch an installed
	// service that may have been stopped on purpose. It exists so a test records
	// an attempt.
	restartService func(ctx context.Context) error
}

type brokerUpdateResult struct {
	FromVersion string
	ToVersion   string
	Restarting  bool
	// UserLevel reports whether the answering Broker is this account's user
	// Broker; a timeout names that Broker's log.
	UserLevel bool
}

type brokerUpdateStatus struct {
	CurrentVersion string
}
