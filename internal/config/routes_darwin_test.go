//go:build darwin

package config

import (
	"errors"
	"fmt"
	"net/netip"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

// darwinRoute4 is real "route -n get default" output captured on a macOS CI
// runner; do not edit it by hand.
const darwinRoute4 = `   route to: default
destination: default
       mask: default
    gateway: 192.168.64.1
  interface: en0
      flags: <UP,GATEWAY,DONE,STATIC,PRCLONING,GLOBAL>
 recvpipe  sendpipe  ssthresh  rtt,msec    rttvar  hopcount      mtu     expire
       0         0         0         0         0         0      1500         0
`

const darwinRoute6 = `   route to: default
destination: default
       mask: default
    gateway: fe80::1%en0
  interface: en0
      flags: <UP,GATEWAY,DONE,STATIC,PRCLONING,GLOBAL>
`

// darwinRouteNoInterface is the shape some macOS route builds print when the
// host has no default route: it names the route without an interface line, so
// the empty result is the intended one rather than a parse failure.
const darwinRouteNoInterface = `   route to: default
destination: default
       mask: default
    gateway: 192.0.2.1
      flags: <UP,GATEWAY,DONE,STATIC,PRCLONING,GLOBAL>
`

// darwinRouteNotInTable is the exact "route -n get -inet6 default" output
// captured on a macOS CI runner; do not edit it by hand.
const darwinRouteNotInTable = "route: writing to routing socket: not in table\n"

const darwinIfconfig = `en0: flags=8863<UP,BROADCAST,SMART,RUNNING,SIMPLEX,MULTICAST> mtu 1500
	options=400<CHANNEL_IO>
	ether 00:11:22:33:44:55
	inet6 fe80::1%en0 prefixlen 64 secured scopeid 0x4
	inet6 2001:db8::20 prefixlen 64 autoconf secured
	inet6 2001:db8::30 prefixlen 64 autoconf temporary
	inet 192.0.2.20 netmask 0xffffff00 broadcast 192.0.2.255
	inet 169.254.10.1 netmask 0xffff0000 broadcast 169.254.255.255
	nd6 options=201<PERFORMNUD,DAD>
`

// TestParseDarwinRouteInterface checks that a normal route output names the
// interface and that output without an interface line means no default route.
func TestParseDarwinRouteInterface(t *testing.T) {
	iface, ok, err := parseDarwinRouteInterface([]byte(darwinRoute4))
	if err != nil {
		t.Fatalf("parseDarwinRouteInterface: %v", err)
	}
	if !ok || iface != "en0" {
		t.Fatalf("interface = %q, ok = %v; want en0", iface, ok)
	}
	iface, ok, err = parseDarwinRouteInterface([]byte(darwinRouteNoInterface))
	if err != nil {
		t.Fatalf("parseDarwinRouteInterface without an interface line: %v", err)
	}
	if ok || iface != "" {
		t.Fatalf("interface = %q, ok = %v; want no route", iface, ok)
	}
}

// TestParseDarwinRouteInterfaceEmptyValueIsError keeps a malformed interface
// line distinguishable from the "no default route" case.
func TestParseDarwinRouteInterfaceEmptyValueIsError(t *testing.T) {
	if _, _, err := parseDarwinRouteInterface([]byte("interface:\n")); err == nil {
		t.Fatal("an empty interface value was accepted")
	}
}

// TestParseDarwinIfconfigExcludesLinkLocalAndTemporary checks that ifconfig
// parsing drops IPv4 and IPv6 link-local addresses and temporary IPv6 addresses.
func TestParseDarwinIfconfigExcludesLinkLocalAndTemporary(t *testing.T) {
	v4, err := parseDarwinIfconfig([]byte(darwinIfconfig), false)
	if err != nil {
		t.Fatalf("parseDarwinIfconfig v4: %v", err)
	}
	if len(v4) != 1 || v4[0].String() != "192.0.2.20" {
		t.Fatalf("IPv4 addresses = %v, want [192.0.2.20]", v4)
	}
	v6, err := parseDarwinIfconfig([]byte(darwinIfconfig), true)
	if err != nil {
		t.Fatalf("parseDarwinIfconfig v6: %v", err)
	}
	if len(v6) != 1 || v6[0].String() != "2001:db8::20" {
		t.Fatalf("IPv6 addresses = %v, want [2001:db8::20]", v6)
	}
}

// TestREQBrokerConfig030_DarwinDefaultRouteEnumeration checks that the Darwin
// enumerator returns the default interface's IPv4 and IPv6 addresses from
// injected command output.
func TestREQBrokerConfig030_DarwinDefaultRouteEnumeration(t *testing.T) {
	run := func(name string, args ...string) ([]byte, error) {
		switch name {
		case "/sbin/route":
			for _, a := range args {
				if a == "-inet6" {
					return []byte(darwinRoute6), nil
				}
			}
			return []byte(darwinRoute4), nil
		case "/sbin/ifconfig":
			if len(args) != 1 || args[0] != "en0" {
				return nil, fmt.Errorf("unexpected ifconfig args %v", args)
			}
			return []byte(darwinIfconfig), nil
		default:
			return nil, fmt.Errorf("unexpected command %s", name)
		}
	}

	got, err := defaultRouteAddressesFrom(run)
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

// TestDarwinDefaultRouteAbsent checks that a not-in-table route output yields
// no addresses rather than an error.
func TestDarwinDefaultRouteAbsent(t *testing.T) {
	run := func(name string, args ...string) ([]byte, error) {
		if name == "/sbin/route" {
			return []byte(darwinRouteNotInTable), errors.New("exit status 1")
		}
		t.Fatalf("unexpected command %s", name)
		return nil, nil
	}
	got, err := defaultRouteAddressesFrom(run)
	if err != nil {
		t.Fatalf("defaultRouteAddressesFrom: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("addresses = %v, want none", got)
	}
}

// TestDarwinRouteNotInTableWithZeroExitIsNoRoute checks that a not-in-table
// IPv6 lookup that exits zero yields no route and an empty address list.
func TestDarwinRouteNotInTableWithZeroExitIsNoRoute(t *testing.T) {
	run := func(name string, args ...string) ([]byte, error) {
		if name != "/sbin/route" {
			t.Fatalf("unexpected command %s", name)
		}
		if slices.Contains(args, "-inet6") {
			return []byte(darwinRouteNotInTable), nil
		}
		return []byte(darwinRouteNoInterface), nil
	}

	iface, ok, err := darwinDefaultInterface(run, true)
	if err != nil {
		t.Fatalf("darwinDefaultInterface for the IPv6 default route: %v", err)
	}
	if ok || iface != "" {
		t.Fatalf("IPv6 interface = %q, ok = %v; want no route", iface, ok)
	}

	got, err := defaultRouteAddressesFrom(run)
	if err != nil {
		t.Fatalf("defaultRouteAddressesFrom: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("addresses = %v, want none", got)
	}
}

// TestDarwinRouteOutputWithoutRouteLinesIsError checks that route output with
// no route to or destination line is an error.
func TestDarwinRouteOutputWithoutRouteLinesIsError(t *testing.T) {
	if _, _, err := parseDarwinRouteInterface([]byte("some unrelated output\n")); err == nil {
		t.Fatal("output without a route to or destination line was accepted")
	}
}

// TestDarwinIfconfigFailureIsReported checks that an ifconfig failure is
// returned as an error.
func TestDarwinIfconfigFailureIsReported(t *testing.T) {
	run := func(name string, args ...string) ([]byte, error) {
		switch name {
		case "/sbin/route":
			return []byte(darwinRoute4), nil
		case "/sbin/ifconfig":
			return []byte("ifconfig: interface en0 does not exist"), errors.New("exit status 1")
		default:
			t.Fatalf("unexpected command %s", name)
			return nil, nil
		}
	}
	_, err := defaultRouteAddressesFrom(run)
	if err == nil {
		t.Fatal("an ifconfig failure was swallowed")
	}
	if !strings.Contains(err.Error(), "ifconfig") {
		t.Fatalf("error = %v, want it to name ifconfig", err)
	}
}

// TestDarwinRouteOutputWithoutInterfaceIsEmpty checks that route output naming
// no interface yields an empty address list for both address families.
func TestDarwinRouteOutputWithoutInterfaceIsEmpty(t *testing.T) {
	run := func(name string, args ...string) ([]byte, error) {
		if name != "/sbin/route" {
			t.Fatalf("unexpected command %s", name)
		}
		return []byte(darwinRouteNoInterface), nil
	}
	got, err := defaultRouteAddressesFrom(run)
	if err != nil {
		t.Fatalf("defaultRouteAddressesFrom: %v", err)
	}
	if len(got) != 0 {
		t.Fatalf("addresses = %v, want none", got)
	}
}

// TestDarwinRouteCommandNotFoundIsError checks that a route command that cannot
// be run is an error.
func TestDarwinRouteCommandNotFoundIsError(t *testing.T) {
	run := func(string, ...string) ([]byte, error) {
		return nil, exec.ErrNotFound
	}
	if _, err := defaultRouteAddressesFrom(run); err == nil {
		t.Fatal("a missing route tool was treated as no default route")
	}
}

// TestDarwinRouteCommandFailureIsReported checks that a route command failure
// is returned as an error.
func TestDarwinRouteCommandFailureIsReported(t *testing.T) {
	run := func(name string, args ...string) ([]byte, error) {
		return []byte("route: unexpected failure"), errors.New("exit status 1")
	}
	if _, err := defaultRouteAddressesFrom(run); err == nil {
		t.Fatal("route command failure was swallowed")
	}
}

// TestDarwinIfconfigTemporaryWithoutToken checks that an IPv6 address without
// the temporary flag is kept.
func TestDarwinIfconfigTemporaryWithoutToken(t *testing.T) {
	out := "inet6 2001:db8::40 prefixlen 64 autoconf secured\n"
	addrs, err := parseDarwinIfconfig([]byte(out), true)
	if err != nil {
		t.Fatalf("parseDarwinIfconfig: %v", err)
	}
	if len(addrs) != 1 || !strings.HasPrefix(addrs[0].String(), "2001:db8::40") {
		t.Fatalf("addresses = %v, want the non-temporary address", addrs)
	}
}

// TestDarwinRouteParseErrorIncludesRawOutput checks that a parse error includes
// the raw route output.
func TestDarwinRouteParseErrorIncludesRawOutput(t *testing.T) {
	_, _, err := parseDarwinRouteInterface([]byte("some unrelated output\nwith two lines\n"))
	if err == nil {
		t.Fatal("output without a route to or destination line was accepted")
	}
	for _, want := range []string{"some unrelated output", `\nwith two lines`} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error = %q, want it to contain %q", err, want)
		}
	}
}

// TestSmokeRealDefaultRouteAddresses calls the production enumerator against
// this host's real routing table and asserts only that enumeration succeeds.
func TestSmokeRealDefaultRouteAddresses(t *testing.T) {
	addrs, err := defaultRouteAddresses()
	if err != nil {
		logDarwinRouteDiagnostics(t)
		t.Fatalf("defaultRouteAddresses() error = %v", err)
	}
	t.Logf("defaultRouteAddresses() = %v", addrs)
	if _, err := (platformAddressSource{}).DefaultRouteAddresses(); err != nil {
		logDarwinRouteDiagnostics(t)
		t.Fatalf("platformAddressSource.DefaultRouteAddresses() error = %v", err)
	}
}

func logDarwinRouteDiagnostics(t *testing.T) {
	t.Helper()
	run := func(name string, args ...string) ([]byte, error) {
		out, err := runCommand(name, args...)
		t.Logf("command %s %s: err=%v, output=%q", name, strings.Join(args, " "), err, string(out))
		return out, err
	}
	if _, err := defaultRouteAddressesFrom(run); err != nil {
		t.Logf("defaultRouteAddressesFrom with logging: %v", err)
	}
}
