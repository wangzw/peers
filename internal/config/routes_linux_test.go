//go:build linux

package config

import (
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"strings"
	"testing"
)

const procNetRouteFixture = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT
eth0	00000000	0100000A	0003	0	0	100	00000000	0	0	0
eth1	00000000	0100000A	0003	0	0	10	00000000	0	0	0
eth2	00000000	00000000	0001	0	0	1000	00000000	0	0	0
eth3	00000000	0100000A	0002	0	0	1	00000000	0	0	0
eth4	00480C0A	00000000	0001	0	0	0	00FFFFFF	0	0	0
`

const procNetRouteDeviceOnlyFixture = `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT
eth0	00000000	0100000A	0003	0	0	10	00000000	0	0	0
wg0	00000000	00000000	0001	0	0	5	00000000	0	0	0
`

// TestParseProcNetRoutePicksLowestMetric checks that the default route with the
// lowest metric is chosen.
func TestParseProcNetRoutePicksLowestMetric(t *testing.T) {
	iface, ok, err := parseProcNetRoute([]byte(procNetRouteFixture))
	if err != nil {
		t.Fatalf("parseProcNetRoute: %v", err)
	}
	if !ok {
		t.Fatal("no default route found")
	}
	if iface != "eth1" {
		t.Fatalf("interface = %q, want eth1", iface)
	}
}

// TestParseProcNetRouteTieKeepsFirstLine checks that equal-metric default
// routes keep the earlier line.
func TestParseProcNetRouteTieKeepsFirstLine(t *testing.T) {
	data := `Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT
ethA 00000000 0100000A 0003 0 0 7 00000000 0 0 0
ethB 00000000 0100000A 0003 0 0 7 00000000 0 0 0
`
	iface, ok, err := parseProcNetRoute([]byte(data))
	if err != nil || !ok {
		t.Fatalf("parseProcNetRoute: iface=%q ok=%v err=%v", iface, ok, err)
	}
	if iface != "ethA" {
		t.Fatalf("interface = %q, want ethA", iface)
	}
}

// TestREQBrokerConfig030_DeviceOnlyDefaultRoute checks that a default route
// naming only a device counts even without the gateway flag.
func TestREQBrokerConfig030_DeviceOnlyDefaultRoute(t *testing.T) {
	iface, ok, err := parseProcNetRoute([]byte(procNetRouteDeviceOnlyFixture))
	if err != nil {
		t.Fatalf("parseProcNetRoute: %v", err)
	}
	if !ok || iface != "wg0" {
		t.Fatalf("interface = %q, ok = %v; want wg0", iface, ok)
	}
}

// TestParseProcNetRouteRequiresUp checks that a default route without RTF_UP is
// not selected.
func TestParseProcNetRouteRequiresUp(t *testing.T) {
	data := `Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT
eth0 00000000 0100000A 0002 0 0 1 00000000 0 0 0
`
	iface, ok, err := parseProcNetRoute([]byte(data))
	if err != nil {
		t.Fatalf("parseProcNetRoute: %v", err)
	}
	if ok {
		t.Fatalf("interface = %q, ok = true; want no default route without RTF_UP", iface)
	}
}

// TestParseProcNetRouteWithoutDefault checks that a route table without a
// default entry reports none.
func TestParseProcNetRouteWithoutDefault(t *testing.T) {
	data := `Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT
eth0 00480C0A 00000000 0001 0 0 0 00FFFFFF 0 0 0
`
	iface, ok, err := parseProcNetRoute([]byte(data))
	if err != nil {
		t.Fatalf("parseProcNetRoute: %v", err)
	}
	if ok {
		t.Fatalf("found interface %q, want no default route", iface)
	}
}

// TestParseProcNetRouteRejectsMalformedLine checks that a short route line is
// an error.
func TestParseProcNetRouteRejectsMalformedLine(t *testing.T) {
	data := "Iface Destination Gateway Flags RefCnt Use Metric Mask MTU Window IRTT\neth0 00000000\n"
	if _, _, err := parseProcNetRoute([]byte(data)); err == nil {
		t.Fatal("malformed line was accepted")
	}
}

// TestParseProcNetIPv6RoutePicksLowestMetric checks that the default IPv6 route
// with the lowest metric is chosen.
func TestParseProcNetIPv6RoutePicksLowestMetric(t *testing.T) {
	zero := strings.Repeat("0", 32)
	data := strings.Join([]string{
		zero + " 00 " + zero + " 00 " + zero + " 00000100 00000001 00000000 00000001 eth0",
		zero + " 00 " + zero + " 00 " + zero + " 00000010 00000001 00000000 00000001 eth1",
	}, "\n")
	iface, ok, err := parseProcNetIPv6Route([]byte(data))
	if err != nil {
		t.Fatalf("parseProcNetIPv6Route: %v", err)
	}
	if !ok || iface != "eth1" {
		t.Fatalf("interface = %q, ok = %v; want eth1", iface, ok)
	}
}

// TestParseProcNetIPv6RouteRejectsMalformedLine checks that a short IPv6 route
// line is an error.
func TestParseProcNetIPv6RouteRejectsMalformedLine(t *testing.T) {
	if _, _, err := parseProcNetIPv6Route([]byte("abcd\n")); err == nil {
		t.Fatal("malformed line was accepted")
	}
}

// TestParseIfInet6 checks that if_inet6 columns are parsed into address, scope,
// flags and interface.
func TestParseIfInet6(t *testing.T) {
	data := "20010db8000000000000000000000020 02 40 00 01 eth0\n"
	entries, err := parseIfInet6([]byte(data))
	if err != nil {
		t.Fatalf("parseIfInet6: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("got %d entries, want 1", len(entries))
	}
	e := entries[0]
	if e.iface != "eth0" || e.scope != 0 || e.flags&ifaFTemporary == 0 {
		t.Fatalf("entry = %+v, want eth0 scope 0 with the temporary flag", e)
	}
	if e.addr.String() != "2001:db8::20" {
		t.Fatalf("address = %s, want 2001:db8::20", e.addr)
	}
}

// TestREQBrokerConfig030_LinuxDefaultRouteEnumeration checks that the Linux
// enumerator returns the default interface's addresses from fixed proc files
// and interface addresses.
func TestREQBrokerConfig030_LinuxDefaultRouteEnumeration(t *testing.T) {
	zero := strings.Repeat("0", 32)
	files := map[string]string{
		"/proc/net/route": procNetRouteFixture,
		"/proc/net/ipv6_route": strings.Join([]string{
			zero + " 00 " + zero + " 00 " + zero + " 00000010 00000001 00000000 00000001 eth1",
		}, "\n"),
		"/proc/net/if_inet6": strings.Join([]string{
			"20010db8000000000000000000000020 02 40 00 80 eth1", // global permanent
			"fe800000000000000000000000000020 02 40 20 80 eth1", // link-local
			"20010db8000000000000000000000030 02 40 00 01 eth1", // global temporary
			"20010db8000000000000000000000040 02 80 10 80 eth1", // host scope
			"20010db8000000000000000000000050 03 40 00 80 eth0", // other interface
		}, "\n"),
	}
	readFile := func(name string) ([]byte, error) {
		data, ok := files[name]
		if !ok {
			return nil, fmt.Errorf("unexpected file %s", name)
		}
		return []byte(data), nil
	}
	ifaceAddrs := func(name string) ([]net.Addr, error) {
		if name != "eth1" {
			return nil, fmt.Errorf("unexpected interface %s", name)
		}
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("192.0.2.20"), Mask: net.CIDRMask(24, 32)},
			&net.IPNet{IP: net.ParseIP("169.254.10.1"), Mask: net.CIDRMask(16, 32)},
			&net.IPAddr{IP: net.ParseIP("192.0.2.99")}, // not an IPNet, ignored
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

// TestDefaultRouteAddressesDeviceOnlyDefaultRoute checks that a device-only
// default route returns its interface's address.
func TestDefaultRouteAddressesDeviceOnlyDefaultRoute(t *testing.T) {
	readFile := func(name string) ([]byte, error) {
		if name == "/proc/net/route" {
			return []byte(procNetRouteDeviceOnlyFixture), nil
		}
		return nil, os.ErrNotExist
	}
	ifaceAddrs := func(name string) ([]net.Addr, error) {
		if name != "wg0" {
			return nil, fmt.Errorf("unexpected interface %s", name)
		}
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("10.8.0.2"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}
	got, err := defaultRouteAddressesFrom(readFile, ifaceAddrs)
	if err != nil {
		t.Fatalf("defaultRouteAddressesFrom: %v", err)
	}
	want := []netip.Addr{netip.MustParseAddr("10.8.0.2")}
	if len(got) != len(want) || got[0] != want[0] {
		t.Fatalf("addresses = %v, want %v", got, want)
	}
}

// TestDefaultRouteAddressesMissingFileIsError checks that a missing
// /proc/net/route is an error.
func TestDefaultRouteAddressesMissingFileIsError(t *testing.T) {
	readFile := func(string) ([]byte, error) { return nil, errors.New("missing") }
	if _, err := defaultRouteAddressesFrom(readFile, nil); err == nil {
		t.Fatal("missing /proc/net/route was accepted")
	}
}

// TestDefaultRouteAddressesMissingIPv6FileIsEmptyIPv6 checks that a missing
// /proc/net/ipv6_route leaves the IPv6 part empty while IPv4 is still
// enumerated.
func TestDefaultRouteAddressesMissingIPv6FileIsEmptyIPv6(t *testing.T) {
	readFile := func(name string) ([]byte, error) {
		if name == "/proc/net/route" {
			return []byte(procNetRouteFixture), nil
		}
		return nil, os.ErrNotExist
	}
	ifaceAddrs := func(name string) ([]net.Addr, error) {
		if name != "eth1" {
			return nil, fmt.Errorf("unexpected interface %s", name)
		}
		return []net.Addr{
			&net.IPNet{IP: net.ParseIP("192.0.2.20"), Mask: net.CIDRMask(24, 32)},
		}, nil
	}
	got, err := defaultRouteAddressesFrom(readFile, ifaceAddrs)
	if err != nil {
		t.Fatalf("defaultRouteAddressesFrom: %v", err)
	}
	want := []netip.Addr{netip.MustParseAddr("192.0.2.20")}
	if len(got) != len(want) {
		t.Fatalf("addresses = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Fatalf("addresses = %v, want %v", got, want)
		}
	}
}

// TestDefaultRouteAddressesNoDefaultRouteIsEmpty checks that a route table
// without a default entry yields no addresses and queries no interface.
func TestDefaultRouteAddressesNoDefaultRouteIsEmpty(t *testing.T) {
	files := map[string]string{
		"/proc/net/route": `Iface	Destination	Gateway 	Flags	RefCnt	Use	Metric	Mask		MTU	Window	IRTT
eth0	00480C0A	00000000	0001	0	0	0	00FFFFFF	0	0	0
`,
		// A non-default IPv6 route: a nonzero destination.
		"/proc/net/ipv6_route": "20010db8000000000000000000000000 40 00000000000000000000000000000000 00 00000000000000000000000000000000 00000100 00000001 00000000 00000001 eth0\n",
	}
	readFile := func(name string) ([]byte, error) {
		data, ok := files[name]
		if !ok {
			return nil, fmt.Errorf("unexpected file %s", name)
		}
		return []byte(data), nil
	}
	ifaceAddrs := func(name string) ([]net.Addr, error) {
		t.Fatalf("interface %s was queried although there is no default route", name)
		return nil, nil
	}
	got, err := defaultRouteAddressesFrom(readFile, ifaceAddrs)
	if err != nil {
		t.Fatalf("defaultRouteAddressesFrom: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("addresses = %v, want none", got)
	}
}

// TestDefaultRouteAddressesIPv6ReadErrorIsError checks that an IPv6 route file
// read error is returned as an error.
func TestDefaultRouteAddressesIPv6ReadErrorIsError(t *testing.T) {
	readFile := func(name string) ([]byte, error) {
		if name == "/proc/net/route" {
			return []byte(procNetRouteFixture), nil
		}
		return nil, errors.New("permission denied")
	}
	ifaceAddrs := func(string) ([]net.Addr, error) { return nil, nil }
	if _, err := defaultRouteAddressesFrom(readFile, ifaceAddrs); err == nil {
		t.Fatal("a permission error on /proc/net/ipv6_route was accepted")
	}
}

// TestSmokeRealDefaultRouteAddresses calls the production enumerator against
// this host's real routing table and asserts only that enumeration succeeds.
func TestSmokeRealDefaultRouteAddresses(t *testing.T) {
	if _, err := defaultRouteAddresses(); err != nil {
		t.Fatalf("defaultRouteAddresses() error = %v", err)
	}
	if _, err := (platformAddressSource{}).DefaultRouteAddresses(); err != nil {
		t.Fatalf("platformAddressSource.DefaultRouteAddresses() error = %v", err)
	}
}
