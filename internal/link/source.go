package link

import "net/netip"

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
