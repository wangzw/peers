package main

import (
	"context"
	"errors"
	"testing"

	"github.com/wangzw/peers/internal/mcpserver/termcmd"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// TestRealDepsApplyUpdateWithoutABroker checks the production wiring of the
// update command's Broker call: with no Broker to select it asks for a local
// replacement, which is the same answer the fallback reads.
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

// TestRealDepsPeerNames checks that the completion dep reaches PeerNames: the
// paired Broker's alias comes back, and the local-only Broker that the same
// listing carries contributes nothing.
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
