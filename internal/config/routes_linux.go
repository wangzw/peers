//go:build linux

package config

import (
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"os"
	"slices"
	"strconv"
	"strings"
)

// Route and address flags from linux/rtnetlink.h and linux/if_addr.h;
// IFA_F_TEMPORARY and IFA_F_SECONDARY share the same bit.
const (
	rtfUp           = 0x0001
	ifaFTemporary   = 0x0001 // IFA_F_TEMPORARY == IFA_F_SECONDARY
	rtScopeUniverse = 0x00   // RT_SCOPE_UNIVERSE: a global address
)

func defaultRouteAddresses() ([]netip.Addr, error) {
	return defaultRouteAddressesFrom(os.ReadFile, interfaceAddresses)
}

func interfaceAddresses(name string) ([]net.Addr, error) {
	iface, err := net.InterfaceByName(name)
	if err != nil {
		return nil, err
	}
	return iface.Addrs()
}

func defaultRouteAddressesFrom(readFile func(string) ([]byte, error), ifaceAddrs func(string) ([]net.Addr, error)) ([]netip.Addr, error) {
	routeData, err := readFile("/proc/net/route")
	if err != nil {
		return nil, fmt.Errorf("cannot read /proc/net/route: %w", err)
	}
	iface4, ok4, err := parseProcNetRoute(routeData)
	if err != nil {
		return nil, fmt.Errorf("cannot parse /proc/net/route: %w", err)
	}

	var out []netip.Addr
	if ok4 {
		addrs, err := ifaceAddrs(iface4)
		if err != nil {
			return nil, fmt.Errorf("cannot read the addresses of interface %s: %w", iface4, err)
		}
		for _, a := range addrs {
			ipnet, ok := a.(*net.IPNet)
			if !ok {
				continue
			}
			ip4 := ipnet.IP.To4()
			if ip4 == nil {
				continue
			}
			addr, ok := netip.AddrFromSlice(ip4)
			if !ok {
				continue
			}
			addr = addr.Unmap()
			if isIPv4LinkLocal(addr) {
				continue
			}
			out = append(out, addr)
		}
	}

	// A host with IPv6 disabled has no /proc/net/ipv6_route, so a missing file
	// means no IPv6 default route rather than an error; other read failures are
	// errors.
	var iface6 string
	var ok6 bool
	v6Data, v6Err := readFile("/proc/net/ipv6_route")
	switch {
	case errors.Is(v6Err, os.ErrNotExist):
	case v6Err != nil:
		return nil, fmt.Errorf("cannot read /proc/net/ipv6_route: %w", v6Err)
	default:
		iface6, ok6, err = parseProcNetIPv6Route(v6Data)
		if err != nil {
			return nil, fmt.Errorf("cannot parse /proc/net/ipv6_route: %w", err)
		}
	}
	if ok6 {
		inet6Data, err := readFile("/proc/net/if_inet6")
		if err != nil {
			return nil, fmt.Errorf("cannot read /proc/net/if_inet6: %w", err)
		}
		entries, err := parseIfInet6(inet6Data)
		if err != nil {
			return nil, fmt.Errorf("cannot parse /proc/net/if_inet6: %w", err)
		}
		for _, e := range entries {
			if e.iface != iface6 {
				continue
			}
			if e.scope != rtScopeUniverse {
				continue
			}
			if e.flags&ifaFTemporary != 0 {
				continue
			}
			if isIPv6LinkLocal(e.addr) {
				continue
			}
			out = append(out, e.addr)
		}
	}

	out = dedupeAddrs(out)
	slices.SortFunc(out, netip.Addr.Compare)
	return out, nil
}

// parseProcNetRoute does not require RTF_GATEWAY: a default route that names
// only a device (`ip route add default dev wg0`) carries RTF_UP alone and still
// counts.
func parseProcNetRoute(data []byte) (iface string, ok bool, err error) {
	var bestMetric uint64
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if fields[0] == "Iface" {
			continue
		}
		if len(fields) < 8 {
			return "", false, fmt.Errorf("line %d: expected at least 8 fields, got %d", i+1, len(fields))
		}
		dest, err := strconv.ParseUint(fields[1], 16, 32)
		if err != nil {
			return "", false, fmt.Errorf("line %d: destination: %w", i+1, err)
		}
		flags, err := strconv.ParseUint(fields[3], 16, 32)
		if err != nil {
			return "", false, fmt.Errorf("line %d: flags: %w", i+1, err)
		}
		metric, err := strconv.ParseUint(fields[6], 10, 32)
		if err != nil {
			return "", false, fmt.Errorf("line %d: metric: %w", i+1, err)
		}
		mask, err := strconv.ParseUint(fields[7], 16, 32)
		if err != nil {
			return "", false, fmt.Errorf("line %d: mask: %w", i+1, err)
		}
		if dest != 0 || mask != 0 {
			continue
		}
		if flags&rtfUp == 0 {
			continue
		}
		if !ok || metric < bestMetric {
			ok = true
			bestMetric = metric
			iface = fields[0]
		}
	}
	return iface, ok, nil
}

func parseProcNetIPv6Route(data []byte) (iface string, ok bool, err error) {
	var bestMetric uint64
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 10 {
			return "", false, fmt.Errorf("line %d: expected at least 10 fields, got %d", i+1, len(fields))
		}
		if len(fields[0]) != 32 {
			return "", false, fmt.Errorf("line %d: destination must be 32 hex digits", i+1)
		}
		if !isZeroHex(fields[0]) {
			continue
		}
		prefix, err := strconv.ParseUint(fields[1], 16, 8)
		if err != nil {
			return "", false, fmt.Errorf("line %d: prefix length: %w", i+1, err)
		}
		if prefix != 0 {
			continue
		}
		metric, err := strconv.ParseUint(fields[5], 16, 32)
		if err != nil {
			return "", false, fmt.Errorf("line %d: metric: %w", i+1, err)
		}
		if !ok || metric < bestMetric {
			ok = true
			bestMetric = metric
			iface = fields[9]
		}
	}
	return iface, ok, nil
}

type inet6Entry struct {
	addr  netip.Addr
	scope uint32
	flags uint32
	iface string
}

// parseIfInet6 parses /proc/net/if_inet6, whose columns are address, interface
// index, prefix length, scope, flags and interface name, all but the last in
// hexadecimal.
func parseIfInet6(data []byte) ([]inet6Entry, error) {
	var out []inet6Entry
	for i, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 6 {
			return nil, fmt.Errorf("line %d: expected at least 6 fields, got %d", i+1, len(fields))
		}
		if len(fields[0]) != 32 {
			return nil, fmt.Errorf("line %d: address must be 32 hex digits", i+1)
		}
		raw, err := hex.DecodeString(fields[0])
		if err != nil {
			return nil, fmt.Errorf("line %d: address: %w", i+1, err)
		}
		var bytes [16]byte
		copy(bytes[:], raw)
		scope, err := strconv.ParseUint(fields[3], 16, 32)
		if err != nil {
			return nil, fmt.Errorf("line %d: scope: %w", i+1, err)
		}
		flags, err := strconv.ParseUint(fields[4], 16, 32)
		if err != nil {
			return nil, fmt.Errorf("line %d: flags: %w", i+1, err)
		}
		out = append(out, inet6Entry{
			addr:  netip.AddrFrom16(bytes),
			scope: uint32(scope),
			flags: uint32(flags),
			iface: fields[5],
		})
	}
	return out, nil
}

func isZeroHex(s string) bool {
	for i := 0; i < len(s); i++ {
		if s[i] != '0' {
			return false
		}
	}
	return true
}
