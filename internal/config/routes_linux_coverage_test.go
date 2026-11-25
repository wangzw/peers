//go:build linux

package config

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"strings"
	"testing"
)

// TestInterfaceAddressesUnknownInterface checks that an unknown interface name
// is the platform's "no such network interface" error.
func TestInterfaceAddressesUnknownInterface(t *testing.T) {
	_, err := interfaceAddresses("peers-no-such-interface-7f3a")
	if err == nil {
		t.Fatal("interfaceAddresses accepted a name that matches no interface")
	}
	if !strings.Contains(err.Error(), "no such network interface") {
		t.Fatalf("interfaceAddresses error = %v, want it to contain %q", err, "no such network interface")
	}
}

// TestDefaultRouteAddressesFromFailures checks that every enumerator failure
// names the proc file it read.
func TestDefaultRouteAddressesFromFailures(t *testing.T) {
	zero := strings.Repeat("0", 32)
	defaultV6 := zero + " 00 " + zero + " 00 " + zero + " 00000000 00000001 00000000 00000001 eth0\n"

	tests := []struct {
		name     string
		files    map[string]string
		readErr  map[string]error
		ifaceErr error
		want     string
	}{
		{
			name:  "malformed ipv4 route table",
			files: map[string]string{"/proc/net/route": "eth0 00000000\n"},
			want:  "cannot parse /proc/net/route",
		},
		{
			name:     "interface address lookup fails",
			files:    map[string]string{"/proc/net/route": procNetRouteFixture},
			ifaceErr: errors.New("link went away"),
			want:     "cannot read the addresses of interface",
		},
		{
			name: "malformed ipv6 route table",
			files: map[string]string{
				"/proc/net/route":      procNetRouteFixture,
				"/proc/net/ipv6_route": "abcd\n",
			},
			want: "cannot parse /proc/net/ipv6_route",
		},
		{
			name: "if_inet6 read fails",
			files: map[string]string{
				"/proc/net/route":      procNetRouteFixture,
				"/proc/net/ipv6_route": defaultV6,
			},
			readErr: map[string]error{"/proc/net/if_inet6": errors.New("permission denied")},
			want:    "cannot read /proc/net/if_inet6",
		},
		{
			name: "malformed if_inet6",
			files: map[string]string{
				"/proc/net/route":      procNetRouteFixture,
				"/proc/net/ipv6_route": defaultV6,
				"/proc/net/if_inet6":   "short\n",
			},
			want: "cannot parse /proc/net/if_inet6",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			readFile := func(name string) ([]byte, error) {
				if err, ok := tc.readErr[name]; ok {
					return nil, err
				}
				if data, ok := tc.files[name]; ok {
					return []byte(data), nil
				}
				return nil, os.ErrNotExist
			}
			ifaceAddrs := func(string) ([]net.Addr, error) {
				if tc.ifaceErr != nil {
					return nil, tc.ifaceErr
				}
				return nil, nil
			}

			_, err := defaultRouteAddressesFrom(readFile, ifaceAddrs)
			if err == nil {
				t.Fatalf("%s was accepted", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestParseProcNetRouteFieldErrors checks that a malformed numeric column is an
// error naming the column.
func TestParseProcNetRouteFieldErrors(t *testing.T) {
	const header = "Iface Destination Gateway Flags RefCnt Use Metric Mask\n"
	tests := []struct {
		name string
		line string
		want string
	}{
		{"destination", "eth0 ZZZZ 00000000 0001 0 0 0 00000000", "destination"},
		{"flags", "eth0 00000000 00000000 ZZZZ 0 0 0 00000000", "flags"},
		{"metric", "eth0 00000000 00000000 0001 0 0 xx 00000000", "metric"},
		{"mask", "eth0 00000000 00000000 0001 0 0 0 ZZZZ", "mask"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseProcNetRoute([]byte(header + tc.line + "\n"))
			if err == nil {
				t.Fatalf("a bad %s column was accepted", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestParseProcNetIPv6RouteFieldErrors checks that a malformed numeric column
// is an error naming the column.
func TestParseProcNetIPv6RouteFieldErrors(t *testing.T) {
	zero := strings.Repeat("0", 32)
	tests := []struct {
		name string
		line string
		want string
	}{
		{
			name: "short destination",
			line: "abcd 00 " + zero + " 00 " + zero + " 00000000 00000001 00000000 00000001 eth0",
			want: "destination must be 32 hex digits",
		},
		{
			name: "prefix length",
			line: zero + " zz " + zero + " 00 " + zero + " 00000000 00000001 00000000 00000001 eth0",
			want: "prefix length",
		},
		{
			name: "metric",
			line: zero + " 00 " + zero + " 00 " + zero + " zz 00000001 00000000 00000001 eth0",
			want: "metric",
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, _, err := parseProcNetIPv6Route([]byte(tc.line + "\n"))
			if err == nil {
				t.Fatalf("a bad %s column was accepted", tc.name)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestParseProcNetIPv6RouteSkipsNonDefaultPrefix checks that only a default
// route with prefix 0 is returned.
func TestParseProcNetIPv6RouteSkipsNonDefaultPrefix(t *testing.T) {
	zero := strings.Repeat("0", 32)
	line := zero + " 40 " + zero + " 00 " + zero + " 00000000 00000001 00000000 00000001 eth0\n"
	iface, ok, err := parseProcNetIPv6Route([]byte(line))
	if err != nil {
		t.Fatalf("parseProcNetIPv6Route: %v", err)
	}
	if ok {
		t.Fatalf("a /64 route was accepted as a default route on %q", iface)
	}
}

// TestParseIfInet6FieldErrors checks that each malformed column is an error
// naming it.
func TestParseIfInet6FieldErrors(t *testing.T) {
	addr := strings.Repeat("0", 32)
	tests := []struct {
		name string
		line string
		want string
	}{
		{"too few fields", addr + " 02 40 00", "expected at least 6 fields"},
		{"short address", "abcd 02 40 00 00 eth0", "address must be 32 hex digits"},
		{"invalid address hex", strings.Repeat("z", 32) + " 02 40 00 00 eth0", "address"},
		{"scope", addr + " 02 40 zz 00 eth0", "scope"},
		{"flags", addr + " 02 40 00 zz eth0", "flags"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := parseIfInet6([]byte(tc.line + "\n")); err == nil {
				t.Fatalf("a bad %s column was accepted", tc.name)
			} else if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("error = %v, want it to contain %q", err, tc.want)
			}
		})
	}
}

// TestDefaultRouteAddressesSkipsUnusableAddresses checks that unusable and
// duplicate interface addresses are left out while the usable global addresses
// of both families are kept.
func TestDefaultRouteAddressesSkipsUnusableAddresses(t *testing.T) {
	zero := strings.Repeat("0", 32)
	files := map[string]string{
		"/proc/net/route": procNetRouteFixture,
		"/proc/net/ipv6_route": zero + " 00 " + zero + " 00 " + zero +
			" 00000000 00000001 00000000 00000001 eth1\n",
		"/proc/net/if_inet6": strings.Join([]string{
			"fe800000000000000000000000000020 02 40 00 00 eth1", // link-local, universe scope
			"20010db8000000000000000000000020 02 40 00 00 eth1", // global, kept
			"20010db8000000000000000000000020 02 40 00 00 eth1", // duplicate, dropped
		}, "\n") + "\n",
	}
	readFile := func(name string) ([]byte, error) {
		data, ok := files[name]
		if !ok {
			return nil, os.ErrNotExist
		}
		return []byte(data), nil
	}
	ifaceAddrs := func(name string) ([]net.Addr, error) {
		if name != "eth1" {
			t.Fatalf("unexpected interface %s", name)
		}
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("192.0.2.20"), Mask: net.CIDRMask(24, 32)},
			&net.IPNet{IP: net.ParseIP("192.0.2.20"), Mask: net.CIDRMask(24, 32)},   // duplicate
			&net.IPNet{IP: net.ParseIP("169.254.10.1"), Mask: net.CIDRMask(16, 32)}, // link-local
			&net.IPNet{IP: net.ParseIP("2001:db8::1"), Mask: net.CIDRMask(64, 128)}, // not IPv4
		}, nil
	}

	got, err := defaultRouteAddressesFrom(readFile, ifaceAddrs)
	if err != nil {
		t.Fatalf("defaultRouteAddressesFrom: %v", err)
	}
	want := []netip.Addr{
		netip.MustParseAddr("192.0.2.20"),
		netip.MustParseAddr("2001:db8::20"),
	}
	if len(got) != len(want) {
		t.Fatalf("addresses = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("addresses = %v, want %v", got, want)
		}
	}
}
