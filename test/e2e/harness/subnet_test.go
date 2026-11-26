package harness

import (
	"net/netip"
	"os"
	"strings"
	"sync"
	"testing"
)

func testSubnetPrefixes(blocks int) []netip.Prefix {
	out := make([]netip.Prefix, 0, blocks*16)
	for block := 0; block < blocks; block++ {
		for offset := 0; offset < 256; offset += 16 {
			out = append(out, netip.PrefixFrom(
				netip.AddrFrom4([4]byte{198, 18, byte(block), byte(offset)}),
				28,
			))
		}
	}
	return out
}

// TestReserveSubnetPrefixExhaustion checks that a full pool hands out no subnet
// and reports the exhaustion with the pool range and holders.
func TestReserveSubnetPrefixExhaustion(t *testing.T) {
	store := subnetStore{dir: t.TempDir()}
	pool := testSubnetPrefixes(1)
	o := currentOwner()

	for i := range pool {
		if _, err := store.reservePrefix(o, pool); err != nil {
			t.Fatalf("allocating slot %d of %d: %v", i+1, len(pool), err)
		}
	}

	_, err := store.reservePrefix(o, pool)
	if err == nil {
		t.Fatal("a full pool handed out another subnet")
	}
	msg := err.Error()
	t.Logf("exhaustion error:\n%s", msg)
	for _, want := range []string{
		pool[0].String(),
		pool[len(pool)-1].String(),
		o.String(),
		subnetStateDir,
		"reclaimed",
	} {
		if !strings.Contains(msg, want) {
			t.Errorf("the exhaustion error does not mention %q:\n%s", want, msg)
		}
	}
}

// TestReserveSubnetPrefixReclaimsDeadOwner checks a reservation whose process
// is gone is removed and handed to the next caller, while a live owner's
// reservation is kept.
func TestReserveSubnetPrefixReclaimsDeadOwner(t *testing.T) {
	store := subnetStore{dir: t.TempDir()}
	pool := testSubnetPrefixes(1)
	live := currentOwner()
	dead := owner{pid: 999999999, start: 1, ns: live.ns}

	liveHeld, deadHeld := pool[0], pool[1]
	if err := os.WriteFile(store.prefixPath(liveHeld), []byte(live.String()), 0o600); err != nil {
		t.Fatalf("writing the live reservation: %v", err)
	}
	if err := os.WriteFile(store.prefixPath(deadHeld), []byte(dead.String()), 0o600); err != nil {
		t.Fatalf("writing the dead reservation: %v", err)
	}

	got, err := store.reservePrefix(live, pool)
	if err != nil {
		t.Fatalf("reserving from the pool: %v", err)
	}
	if got != deadHeld {
		t.Fatalf("reserved %s, want the slot of the dead owner %s", got, deadHeld)
	}
	data, err := os.ReadFile(store.prefixPath(deadHeld))
	if err != nil {
		t.Fatalf("reading the reclaimed reservation: %v", err)
	}
	if parsed, ok := parseOwnerValue(string(data)); !ok || parsed != live {
		t.Errorf("the reclaimed reservation names %q, want %s", strings.TrimSpace(string(data)), live)
	}
	if _, err := os.Stat(store.prefixPath(liveHeld)); err != nil {
		t.Errorf("the live owner's reservation was removed: %v", err)
	}
}

// TestReserveSubnetPrefixConcurrent requires concurrent reservations from one
// process to stay distinct and to exhaust the pool exactly.
func TestReserveSubnetPrefixConcurrent(t *testing.T) {
	store := subnetStore{dir: t.TempDir()}
	pool := testSubnetPrefixes(4)
	o := currentOwner()
	const workers = 64

	got := make([]netip.Prefix, workers)
	errs := make([]error, workers)
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			got[i], errs[i] = store.reservePrefix(o, pool)
		}(i)
	}
	wg.Wait()

	seen := make(map[netip.Prefix]bool, workers)
	for i := range got {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if seen[got[i]] {
			t.Fatalf("%s was reserved by two workers", got[i])
		}
		seen[got[i]] = true
	}
	if _, err := store.reservePrefix(o, pool); err == nil {
		t.Fatal("a full pool handed out another subnet")
	}
}

// TestSubnetPrefixFileNameRoundTrips checks the state file name encodes and
// decodes the prefix without losing the CIDR bits.
func TestSubnetPrefixFileNameRoundTrips(t *testing.T) {
	for _, prefix := range testSubnetPrefixes(2) {
		name := subnetPrefixFileName(prefix)
		if strings.Contains(name, "/") {
			t.Fatalf("file name %q contains a path separator", name)
		}
		parsed, ok := parseSubnetPrefixName(name)
		if !ok || parsed != prefix {
			t.Fatalf("parseSubnetPrefixName(%q) = %s, %t; want %s", name, parsed, ok, prefix)
		}
	}
}
