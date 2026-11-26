package main

import (
	"context"
	"errors"

	"github.com/wangzw/peers/internal/mcpserver"
	"github.com/wangzw/peers/internal/mcpserver/termcmd"
)

// errUserLevelUpdateUnsupported reports that the selected Broker is this
// account's user Broker and declined to apply the update itself; the command
// then replaces the user binary in place.
var errUserLevelUpdateUnsupported = errors.New("the user Broker does not apply updates itself")

// errNoRunningBroker reports that no Broker could be selected or reached, so
// the command replaces the binary locally.
var errNoRunningBroker = errors.New("no running Broker")

// updateUserSocketPath is a variable so a test can decide which selected Broker
// counts as this account's own.
var updateUserSocketPath = mcpserver.UserSocketPath

func applyUpdateThroughBroker(ctx context.Context, euid func() int) (brokerUpdateResult, error) {
	var selectedPath string
	selectBroker := termcmd.Selector(func(ctx context.Context) (string, termcmd.Dialer, error) {
		path, dial, err := findBroker(ctx)
		if err == nil {
			selectedPath = path
		}
		return path, dial, err
	})

	result, err := termcmd.RunApplyUpdate(ctx, selectBroker)
	if err != nil {
		var answered *termcmd.ApplyUpdateError
		switch {
		case errors.As(err, &answered):
			if euid() != 0 && selectedPath != "" && selectedPath == updateUserSocketPath() && termcmd.IsFailedPrecondition(err) {
				return brokerUpdateResult{}, errUserLevelUpdateUnsupported
			}
			return brokerUpdateResult{}, err
		default:
			return brokerUpdateResult{}, errNoRunningBroker
		}
	}
	return brokerUpdateResult{
		FromVersion: result.FromVersion,
		ToVersion:   result.ToVersion,
		Restarting:  result.Restarting,
		UserLevel:   selectedPath != "" && selectedPath == updateUserSocketPath(),
	}, nil
}

func fetchBrokerUpdateStatus(ctx context.Context) (brokerUpdateStatus, error) {
	current, _, err := termcmd.RunUpdateStatus(ctx, findBroker)
	if err != nil {
		return brokerUpdateStatus{}, err
	}
	return brokerUpdateStatus{CurrentVersion: current}, nil
}
