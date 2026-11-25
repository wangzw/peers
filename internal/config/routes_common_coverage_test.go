package config

import (
	"net/netip"
	"testing"
)

// TestAddressClassificationByFamily checks that each link-local predicate
// answers only for its own address family.
func TestAddressClassificationByFamily(t *testing.T) {
	tests := []struct {
		addr       netip.Addr
		wantV4Link bool
		wantV6Link bool
	}{
		{netip.MustParseAddr("169.254.1.2"), true, false},
		{netip.MustParseAddr("192.0.2.1"), false, false},
		{netip.MustParseAddr("fe80::1"), false, true},
		{netip.MustParseAddr("fec0::1"), false, false},
		{netip.MustParseAddr("2001:db8::1"), false, false},
	}
	for _, tc := range tests {
		if got := isIPv4LinkLocal(tc.addr); got != tc.wantV4Link {
			t.Errorf("isIPv4LinkLocal(%s) = %v, want %v", tc.addr, got, tc.wantV4Link)
		}
		if got := isIPv6LinkLocal(tc.addr); got != tc.wantV6Link {
			t.Errorf("isIPv6LinkLocal(%s) = %v, want %v", tc.addr, got, tc.wantV6Link)
		}
	}
}

// TestDedupeAddrsRemovesRepeats checks that dedupeAddrs removes repeated
// addresses and keeps the first position of each.
func TestDedupeAddrsRemovesRepeats(t *testing.T) {
	in := []netip.Addr{
		netip.MustParseAddr("192.0.2.1"),
		netip.MustParseAddr("2001:db8::1"),
		netip.MustParseAddr("192.0.2.1"),
		netip.MustParseAddr("2001:db8::1"),
		netip.MustParseAddr("192.0.2.2"),
	}
	want := []netip.Addr{
		netip.MustParseAddr("192.0.2.1"),
		netip.MustParseAddr("2001:db8::1"),
		netip.MustParseAddr("192.0.2.2"),
	}
	got := dedupeAddrs(in)
	if len(got) != len(want) {
		t.Fatalf("dedupeAddrs() returned %d addresses, want %d: %v", len(got), len(want), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("dedupeAddrs()[%d] = %s, want %s", i, got[i], want[i])
		}
	}
}
