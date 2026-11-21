package link

import "net/netip"

// sourcePrefix returns the network prefix that stands for the source of a
// connection whose remote address is remote, in host:port form: the address
// itself as a /32 for IPv4 and the enclosing /64 for IPv6. An IPv4-mapped
// IPv6 address counts as IPv4 and an IPv6 zone is dropped. It returns ""
// when remote carries no IP address.
func sourcePrefix(remote string) string {
	ap, err := netip.ParseAddrPort(remote)
	if err != nil {
		return ""
	}
	addr := ap.Addr().Unmap()
	if addr.Is4() {
		return netip.PrefixFrom(addr, 32).Masked().String()
	}
	return netip.PrefixFrom(addr.WithZone(""), 64).Masked().String()
}
