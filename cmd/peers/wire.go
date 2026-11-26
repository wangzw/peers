package main

import (
	"context"
	"errors"
	"io"
	"os"

	"github.com/wangzw/peers/internal/broker"
	"github.com/wangzw/peers/internal/broker/update"
	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/internal/mcpserver/termcmd"
)

// protocolOverrides returns the local and link protocol versions for the
// handshake; zero means the generated packages' version. Only the peers_testenv
// build replaces it.
var protocolOverrides = func() (local, link uint32) { return 0, 0 }

// brokerRun is the process entry point behind the broker subcommand, a variable
// so a test can record the RunOptions without starting a real Broker.
var brokerRun = broker.Run

var _ termcmd.Selector = mcpserver.FindBrokerWithDial

// findBroker is the account-based selector the terminal commands use; it is a
// variable so a test can point the real dep closures at a fake Broker.
var findBroker termcmd.Selector = mcpserver.FindBrokerWithDial

// findBrokerForLevel is the selector for commands that take --system or --user;
// it is a variable for the same reason as findBroker.
var findBrokerForLevel = mcpserver.FindBrokerWithDialForLevel

var _ func(context.Context, bool, bool) (string, termcmd.Dialer, error) = findBrokerForLevel

func selectorForLevel(system, explicit bool) termcmd.Selector {
	return func(ctx context.Context) (string, termcmd.Dialer, error) {
		return findBrokerForLevel(ctx, system, explicit)
	}
}

// realDeps connects the command line to the real Broker and MCP server entry
// points; main calls it once before any subcommand runs.
func realDeps(version string) deps {
	d := deps{
		euid: os.Geteuid,
		runBroker: func(ctx context.Context, o brokerOptions) error {
			local, link := protocolOverrides()
			opts := broker.RunOptions{
				System:               o.System,
				Listen:               o.Listen,
				AllowDegraded:        o.AllowDegraded,
				LogOutput:            o.LogOutput,
				Version:              o.Version,
				LocalProtocolVersion: local,
				LinkProtocolVersion:  link,
			}
			return brokerRun(ctx, opts)
		},
		classifyBrokerErr: classifyBrokerErr,
		runMCP: func(ctx context.Context) error {
			local, _ := protocolOverrides()
			return mcpserver.Run(ctx, mcpserver.Options{
				Version:              version,
				LocalProtocolVersion: local,
			})
		},
		runStatus: func(ctx context.Context, system, explicit bool, stdout io.Writer) error {
			return termcmd.RunStatus(ctx, selectorForLevel(system, explicit), stdout)
		},
		runAudit: func(ctx context.Context, system, explicit bool, stdout io.Writer, limit uint32) error {
			return termcmd.RunAudit(ctx, selectorForLevel(system, explicit), limit, stdout)
		},
		runConfigGet: func(ctx context.Context, key string, stdout io.Writer) error {
			return termcmd.RunConfigGet(ctx, findBroker, key, stdout)
		},
		runConfigSet: func(ctx context.Context, key, value string, stdout io.Writer) error {
			return termcmd.RunConfigSet(ctx, findBroker, key, value, stdout)
		},
		runSetOnline: func(ctx context.Context, system, explicit, online bool, stdout io.Writer) error {
			return termcmd.RunSetOnline(ctx, selectorForLevel(system, explicit), online, stdout)
		},
		runPairingKeyCreate: func(ctx context.Context, stdout io.Writer) error {
			return termcmd.RunPairingKeyCreate(ctx, findBroker, stdout)
		},
		runPeerList: func(ctx context.Context, stdout io.Writer) error {
			return termcmd.RunPeerList(ctx, findBroker, stdout)
		},
		runPeerConnect: func(ctx context.Context, address, key, alias string, stdout, warn io.Writer) error {
			return termcmd.RunPeerConnect(ctx, findBroker, address, key, alias, stdout, warn)
		},
		runPeerDisconnect: func(ctx context.Context, broker string, stdout io.Writer) error {
			return termcmd.RunPeerDisconnect(ctx, findBroker, broker, stdout)
		},
		peerNames: func(ctx context.Context) ([]string, error) {
			return termcmd.PeerNames(ctx, findBroker)
		},

		updateAPIBase:      update.APIBase,
		updateLatest:       update.Latest,
		updateNewer:        update.Newer,
		updateApply:        update.Apply,
		brokerUpdateStatus: fetchBrokerUpdateStatus,
	}
	d.applyUpdateViaBroker = func(ctx context.Context) (brokerUpdateResult, error) {
		return applyUpdateThroughBroker(ctx, d.euid)
	}
	return d
}

// classifyBrokerErr maps a Broker startup error to its exit-code family. Run
// wraps every sentinel with its cause, so the checks must use errors.Is.
func classifyBrokerErr(err error) startFailure {
	switch {
	case errors.Is(err, broker.ErrAlreadyRunning):
		return failAlreadyRunning
	case errors.Is(err, broker.ErrListenUnavailable):
		return failListenUnavailable
	case errors.Is(err, broker.ErrInvalidConfig):
		return failInvalidConfig
	case errors.Is(err, broker.ErrInSession):
		return failInSession
	case errors.Is(err, broker.ErrNotRoot):
		return failNotRoot
	case errors.Is(err, broker.ErrRuntimeDir):
		return failRuntimeDir
	case errors.Is(err, broker.ErrInvalidListen):
		return failInvalidListen
	default:
		return failOther
	}
}
