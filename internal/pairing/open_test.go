package pairing

import (
	"crypto/rand"
	"errors"
	"os"
	"path/filepath"
	"testing"

	"github.com/wangzw/peers/internal/config"
)

// TestOpenRejectsNilTransport checks that Open with a nil transport returns
// ErrNoTransport.
func TestOpenRejectsNilTransport(t *testing.T) {
	p, err := Open(nil)
	if !errors.Is(err, ErrNoTransport) {
		t.Fatalf("Open(nil) error = %v, want ErrNoTransport", err)
	}
	if p != nil {
		t.Fatalf("Open(nil) returned %+v, want nil", p)
	}
}

// TestOpenRejectsEmptyDataDir checks that opening with an empty data directory
// returns ErrNotOpen.
func TestOpenRejectsEmptyDataDir(t *testing.T) {
	s := configSettings{paths: func() config.PathSet { return config.PathSet{} }}
	p, err := newPairing(s, &hsTransport{}, newHSClock(), realFileOps{}, nil)
	if !errors.Is(err, ErrNotOpen) {
		t.Fatalf("open with an empty data directory error = %v, want ErrNotOpen", err)
	}
	if p != nil {
		t.Fatalf("open with an empty data directory returned %+v, want nil", p)
	}
}

// TestOpenGeneratesIdentityAndReloadsIt checks that the first open writes the
// identity with the expected mode, a restart keeps the fingerprint, and the
// first open writes no peer table.
func TestOpenGeneratesIdentityAndReloadsIt(t *testing.T) {
	s := newHSSettings(t)
	clk := newHSClock()

	first, err := newPairing(s, &hsTransport{}, clk, realFileOps{}, rand.Read)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	fp := first.Fingerprint()
	if fp.IsZero() {
		t.Fatal("first open produced a zero fingerprint")
	}
	for _, name := range []string{identityKeyFileName, identityCertFileName} {
		info, statErr := os.Stat(filepath.Join(s.dataDir, name))
		if statErr != nil {
			t.Fatalf("%s was not created: %v", name, statErr)
		}
		if got := info.Mode().Perm(); got != identityFileMode {
			t.Fatalf("%s mode = %o, want %o", name, got, identityFileMode)
		}
	}

	second, err := newPairing(s, &hsTransport{}, clk, realFileOps{}, rand.Read)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if got := second.Fingerprint(); got != fp {
		t.Fatalf("fingerprint after restart = %s, want %s", got, fp)
	}
	if _, statErr := os.Stat(filepath.Join(s.dataDir, peersFileName)); !errors.Is(statErr, os.ErrNotExist) {
		t.Fatalf("first open wrote %s unexpectedly: %v", peersFileName, statErr)
	}
}

// TestREQBrokerPairing001_FingerprintIgnoresAliasAndListen checks that the
// fingerprint stays the same when the alias, listen address and hostname change.
func TestREQBrokerPairing001_FingerprintIgnoresAliasAndListen(t *testing.T) {
	s := newHSSettings(t)
	s.alias = "alpha"
	s.listen = "127.0.0.1:17443"
	clk := newHSClock()

	first, err := newPairing(s, &hsTransport{}, clk, realFileOps{}, rand.Read)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	fp := first.Fingerprint()

	s.mu.Lock()
	s.alias = "beta"
	s.listen = "192.0.2.1:47443"
	s.hostname = "renamed-host"
	s.mu.Unlock()

	second, err := newPairing(s, &hsTransport{}, clk, realFileOps{}, rand.Read)
	if err != nil {
		t.Fatalf("second open: %v", err)
	}
	if got := second.Fingerprint(); got != fp {
		t.Fatalf("fingerprint after changing alias and listen address = %s, want %s", got, fp)
	}
}

// TestOpenLoadsExistingPeerTableOnRestart checks that the peer, its role and
// its effective alias survive a restart.
func TestOpenLoadsExistingPeerTableOnRestart(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	s.alias = "alpha"

	tr := &hsTransport{}
	first, err := newPairing(s, tr, clk, realFileOps{}, rand.Read)
	if err != nil {
		t.Fatalf("first open: %v", err)
	}
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	acc := hsNewPairing(t, accS, &hsTransport{}, clk)

	if _, connectErr, acceptErr := pairWithHello(t, first, acc, tr, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
		t.Fatalf("pairing before restart: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	fp := acc.identity.fingerprint()

	restarted, err := newPairing(s, &hsTransport{}, clk, realFileOps{}, rand.Read)
	if err != nil {
		t.Fatalf("restart open: %v", err)
	}
	if !restarted.IsPeer(fp) {
		t.Fatal("the peer table did not survive a restart")
	}
	if got := restarted.Role(fp); got != RoleDialer {
		t.Fatalf("role after restart = %v, want dialer", got)
	}
	if got := restarted.EffectiveAlias(fp); got != "beta" {
		t.Fatalf("effective alias after restart = %q, want beta", got)
	}
}

// TestOpenFailsOnBrokenPeerTable checks that a malformed peers.json makes open
// fail with ErrStore.
func TestOpenFailsOnBrokenPeerTable(t *testing.T) {
	s := newHSSettings(t)
	if err := os.WriteFile(filepath.Join(s.dataDir, peersFileName), []byte(`{"version":1,"peers":[`), peersFileMode); err != nil {
		t.Fatalf("write broken peers.json: %v", err)
	}

	p, err := newPairing(s, &hsTransport{}, newHSClock(), realFileOps{}, rand.Read)
	if !errors.Is(err, ErrStore) {
		t.Fatalf("open with a malformed peers.json error = %v, want ErrStore", err)
	}
	if p != nil {
		t.Fatalf("open with a malformed peers.json returned %+v, want nil", p)
	}
}

// TestOpenFailsOnUnknownPeerTableVersion checks that an unknown peers.json
// version makes open fail with ErrStore.
func TestOpenFailsOnUnknownPeerTableVersion(t *testing.T) {
	s := newHSSettings(t)
	doc := `{"version":2,"peers":[]}`
	if err := os.WriteFile(filepath.Join(s.dataDir, peersFileName), []byte(doc), peersFileMode); err != nil {
		t.Fatalf("write peers.json: %v", err)
	}

	if _, err := newPairing(s, &hsTransport{}, newHSClock(), realFileOps{}, rand.Read); !errors.Is(err, ErrStore) {
		t.Fatalf("open with version 2 error = %v, want ErrStore", err)
	}
}
