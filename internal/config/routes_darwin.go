//go:build darwin

package config

import (
	"fmt"
	"net/netip"
	"os/exec"
	"slices"
	"strings"
)

// defaultRouteAddresses reads the addresses from ifconfig because macOS does
// not expose IFA_F_TEMPORARY through net.InterfaceAddrs; ifconfig's IPv6 lines
// mark temporary addresses with the "temporary" flag.
func defaultRouteAddresses() ([]netip.Addr, error) {
	return defaultRouteAddressesFrom(runCommand)
}

type commandRunner func(name string, args ...string) ([]byte, error)

func runCommand(name string, args ...string) ([]byte, error) {
	return exec.Command(name, args...).CombinedOutput()
}

func defaultRouteAddressesFrom(run commandRunner) ([]netip.Addr, error) {
	var out []netip.Addr

	iface4, ok, err := darwinDefaultInterface(run, false)
	if err != nil {
		return nil, err
	}
	if ok {
		data, err := run("/sbin/ifconfig", iface4)
		if err != nil {
			return nil, fmt.Errorf("cannot run ifconfig %s: %w: %s", iface4, err, darwinOutputSnippet(data))
		}
		addrs, err := parseDarwinIfconfig(data, false)
		if err != nil {
			return nil, fmt.Errorf("cannot run ifconfig %s: %w: %s", iface4, err, darwinOutputSnippet(data))
		}
		out = append(out, addrs...)
	}

	iface6, ok, err := darwinDefaultInterface(run, true)
	if err != nil {
		return nil, err
	}
	if ok {
		data, err := run("/sbin/ifconfig", iface6)
		if err != nil {
			return nil, fmt.Errorf("cannot run ifconfig %s: %w: %s", iface6, err, darwinOutputSnippet(data))
		}
		addrs, err := parseDarwinIfconfig(data, true)
		if err != nil {
			return nil, fmt.Errorf("cannot run ifconfig %s: %w: %s", iface6, err, darwinOutputSnippet(data))
		}
		out = append(out, addrs...)
	}

	out = dedupeAddrs(out)
	slices.SortFunc(out, netip.Addr.Compare)
	return out, nil
}

// darwinDefaultInterface returns the interface named by "route -n get [-inet6]
// default"; a missing default route yields ok false rather than an error, and
// its "not in table" output is recognized regardless of exit status because
// macOS exits zero for an absent IPv6 route and nonzero for an absent IPv4
// route.
func darwinDefaultInterface(run commandRunner, inet6 bool) (iface string, ok bool, err error) {
	args := []string{"-n", "get"}
	if inet6 {
		args = append(args, "-inet6")
	}
	args = append(args, "default")
	out, err := run("/sbin/route", args...)
	if isDarwinNoRoute(out) {
		return "", false, nil
	}
	if err != nil {
		return "", false, fmt.Errorf("cannot run route %s: %w: %s", strings.Join(args, " "), err, darwinOutputSnippet(out))
	}
	return parseDarwinRouteInterface(out)
}

func darwinOutputSnippet(out []byte) string {
	const maxBytes = 500
	s := strings.TrimSpace(string(out))
	if s == "" {
		return "<empty>"
	}
	if len(s) > maxBytes {
		s = strings.ToValidUTF8(s[:maxBytes], "") + "...(truncated)"
	}
	s = strings.ReplaceAll(s, "\r\n", "\n")
	s = strings.ReplaceAll(s, "\n", `\n`)
	s = strings.ReplaceAll(s, "\r", `\r`)
	s = strings.ReplaceAll(s, "\t", `\t`)
	return s
}

func isDarwinNoRoute(out []byte) bool {
	s := strings.ToLower(string(out))
	return strings.Contains(s, "not in table") || strings.Contains(s, "no such process")
}

// parseDarwinRouteInterface returns the interface named by a route command's
// output; output that names the route but no interface means no default route,
// while output that does not describe a route at all is an error.
func parseDarwinRouteInterface(out []byte) (string, bool, error) {
	var iface string
	sawRoute := false
	sawInterface := false
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "route to:") || strings.HasPrefix(line, "destination:") {
			sawRoute = true
		}
		value, ok := strings.CutPrefix(line, "interface:")
		if !ok {
			continue
		}
		value = strings.TrimSpace(value)
		if value == "" {
			return "", false, fmt.Errorf("cannot parse the route output: the interface is empty: %s", darwinOutputSnippet(out))
		}
		if !sawInterface {
			iface = value
			sawInterface = true
		}
	}
	if !sawRoute {
		return "", false, fmt.Errorf("cannot parse the route output: it does not describe a route: %s", darwinOutputSnippet(out))
	}
	return iface, sawInterface, nil
}

func parseDarwinIfconfig(out []byte, inet6 bool) ([]netip.Addr, error) {
	var addrs []netip.Addr
	for i, raw := range strings.Split(string(out), "\n") {
		fields := strings.Fields(strings.TrimSpace(raw))
		if len(fields) < 2 {
			continue
		}
		keyword := "inet"
		if inet6 {
			keyword = "inet6"
		}
		if fields[0] != keyword {
			continue
		}
		text := fields[1]
		if percent := strings.IndexByte(text, '%'); percent >= 0 {
			text = text[:percent]
		}
		addr, err := netip.ParseAddr(text)
		if err != nil {
			return nil, fmt.Errorf("line %d: %w", i+1, err)
		}
		if inet6 {
			if !addr.Is6() || isIPv6LinkLocal(addr) {
				continue
			}
			if hasToken(fields[2:], "temporary") {
				continue
			}
		} else {
			if !addr.Is4() || isIPv4LinkLocal(addr) {
				continue
			}
		}
		addrs = append(addrs, addr)
	}
	return addrs, nil
}

func hasToken(fields []string, want string) bool {
	for _, f := range fields {
		if f == want {
			return true
		}
	}
	return false
}
