package link

import (
	"net"
	"testing"
	"time"
)

func mustCIDR(t *testing.T, s string) *net.IPNet {
	t.Helper()
	_, network, err := net.ParseCIDR(s)
	if err != nil {
		t.Fatalf("net.ParseCIDR(%q): %v", s, err)
	}
	return network
}

func assertAddresses(t *testing.T, got, want []string) {
	t.Helper()
	if len(got) != len(want) {
		t.Fatalf("orderCandidates returned %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("orderCandidates returned %v, want %v", got, want)
		}
	}
}

// TestOrderCandidatesWalkthrough checks that the last connected address goes
// first, then the address on a local network, then the rest.
func TestOrderCandidatesWalkthrough(t *testing.T) {
	addrs := []string{"203.0.113.5:17443", "10.0.0.5:17443"}
	local := []*net.IPNet{mustCIDR(t, "10.0.0.0/24")}

	got := orderCandidates(addrs, "203.0.113.5:17443", local)
	assertAddresses(t, got, []string{"203.0.113.5:17443", "10.0.0.5:17443"})

	got = orderCandidates(addrs, "", local)
	assertAddresses(t, got, []string{"10.0.0.5:17443", "203.0.113.5:17443"})
}

// TestOrderCandidatesLastNotInList checks that the last connected address is
// kept when it is not a candidate and is not duplicated when it is one.
func TestOrderCandidatesLastNotInList(t *testing.T) {
	addrs := []string{"198.51.100.7:17443"}
	local := []*net.IPNet{mustCIDR(t, "10.0.0.0/24")}

	got := orderCandidates(addrs, "203.0.113.5:17443", local)
	assertAddresses(t, got, []string{"203.0.113.5:17443", "198.51.100.7:17443"})

	got = orderCandidates(addrs, "198.51.100.7:17443", local)
	assertAddresses(t, got, []string{"198.51.100.7:17443"})
}

// TestOrderCandidatesNamesAndIPv6 checks that host names and malformed
// addresses fall in the rest group while a bracketed local IPv6 literal does
// not.
func TestOrderCandidatesNamesAndIPv6(t *testing.T) {
	addrs := []string{"a.example:1", "[fd00::5]:1", "not-a-host-port", "203.0.113.5:1"}
	local := []*net.IPNet{mustCIDR(t, "fd00::/64")}

	got := orderCandidates(addrs, "", local)
	assertAddresses(t, got, []string{"[fd00::5]:1", "a.example:1", "not-a-host-port", "203.0.113.5:1"})
}

// TestBackoffDelaySequence checks that the delay doubles from min to max and
// never wraps around.
func TestBackoffDelaySequence(t *testing.T) {
	const min, max = time.Second, 60 * time.Second
	want := []time.Duration{
		1 * time.Second,
		2 * time.Second,
		4 * time.Second,
		8 * time.Second,
		16 * time.Second,
		32 * time.Second,
		60 * time.Second,
		60 * time.Second,
		60 * time.Second,
	}
	for failures, w := range want {
		if got := backoffDelay(failures, min, max); got != w {
			t.Fatalf("backoffDelay(%d, %v, %v) = %v, want %v", failures, min, max, got, w)
		}
	}
	if got := backoffDelay(200, min, max); got != max {
		t.Fatalf("backoffDelay(200, %v, %v) = %v, want %v", min, max, got, max)
	}
}

// TestBackoffDelayMaxBelowMin checks that a max below min still returns min.
func TestBackoffDelayMaxBelowMin(t *testing.T) {
	if got := backoffDelay(3, 5*time.Second, time.Second); got != 5*time.Second {
		t.Fatalf("backoffDelay(3, 5s, 1s) = %v, want 5s", got)
	}
}
