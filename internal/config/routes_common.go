package config

import "net/netip"

type platformAddressSource struct{}

func (platformAddressSource) DefaultRouteAddresses() ([]netip.Addr, error) {
	return defaultRouteAddresses()
}

func isIPv4LinkLocal(addr netip.Addr) bool {
	if !addr.Is4() {
		return false
	}
	b := addr.As4()
	return b[0] == 169 && b[1] == 254
}

func isIPv6LinkLocal(addr netip.Addr) bool {
	if !addr.Is6() {
		return false
	}
	b := addr.As16()
	return b[0] == 0xfe && b[1]&0xc0 == 0x80
}

func dedupeAddrs(in []netip.Addr) []netip.Addr {
	seen := make(map[netip.Addr]struct{}, len(in))
	out := make([]netip.Addr, 0, len(in))
	for _, a := range in {
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	return out
}
