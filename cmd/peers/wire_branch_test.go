package main

import (
	"context"
	"errors"
	"testing"

	"github.com/wangzw/peers/internal/mcpserver/termcmd"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// TestRealDepsApplyUpdateWithoutABroker checks that applyUpdateViaBroker
// returns errNoRunningBroker when no Broker can be selected.
func TestRealDepsApplyUpdateWithoutABroker(t *testing.T) {
	previous := findBroker
	findBroker = func(context.Context) (string, termcmd.Dialer, error) {
		return "", nil, errors.New("no Broker is running")
	}
	t.Cleanup(func() { findBroker = previous })

	d := realDeps("v")
	if _, err := d.applyUpdateViaBroker(context.Background()); !errors.Is(err, errNoRunningBroker) {
		t.Fatalf("applyUpdateViaBroker = %v, want errNoRunningBroker", err)
	}
}

// TestRealDepsPeerNames checks that the completion dep returns the paired
// Broker's alias through PeerNames.
func TestRealDepsPeerNames(t *testing.T) {
	path := fakelocal.SocketPath(t)
	fakelocal.Start(t, path, fakelocal.Handlers{
		ListBrokers: fakelocal.ListBrokersAnswer(fakelocal.ListedBroker{Alias: "hostB", IsPeer: true}),
	})
	previous := findBroker
	findBroker = fakelocal.Selector(path)
	t.Cleanup(func() { findBroker = previous })

	d := realDeps("v")
	names, err := d.peerNames(context.Background())
	if err != nil {
		t.Fatalf("peerNames: %v", err)
	}
	if len(names) != 1 || names[0] != "hostB" {
		t.Errorf("peerNames = %q, want the paired alias hostB", names)
	}
}
