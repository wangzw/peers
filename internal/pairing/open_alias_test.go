package pairing

import (
	"crypto/rand"
	"testing"
)

// TestOpenSeedsOwnAliasCache checks that opening a Pairing seeds the own-alias
// cache so a peer reporting the Broker's alias gets a generated local alias.
func TestOpenSeedsOwnAliasCache(t *testing.T) {
	t.Parallel()

	cases := []struct {
		name string
		open func(t *testing.T, s *hsSettings) *Pairing
	}{
		{
			name: "newPairing",
			open: func(t *testing.T, s *hsSettings) *Pairing {
				t.Helper()
				p, err := newPairing(s, &hsTransport{}, newHSClock(), realFileOps{}, rand.Read)
				if err != nil {
					t.Fatalf("newPairing: %v", err)
				}
				return p
			},
		},
		{
			name: "hsNewPairing",
			open: func(t *testing.T, s *hsSettings) *Pairing {
				t.Helper()
				return hsNewPairing(t, s, &hsTransport{}, newHSClock())
			},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()

			s := newHSSettings(t)
			s.alias = "beta"
			p := tc.open(t, s)

			fp, pub := hsKey(t)
			if _, err := p.peers.put(peerEntry{
				fingerprint: fp,
				publicKey:   pub,
				alias:       "gamma",
				role:        RoleAcceptor,
				pairedAt:    p.clock.Now(),
			}); err != nil {
				t.Fatalf("seed peer table: %v", err)
			}

			p.ObserveAlias(fp, "beta")

			if got := p.EffectiveAlias(fp); got == "beta" {
				t.Fatalf("peer kept effective alias %q after reporting the local alias; want a generated local alias", got)
			}
			entry, ok := p.peers.get(fp)
			if !ok {
				t.Fatal("peer disappeared from the table")
			}
			if entry.localAlias == "" {
				t.Fatalf("no local alias generated: %+v", entry)
			}
		})
	}
}
