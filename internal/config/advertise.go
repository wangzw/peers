package config

import (
	"fmt"
	"net"
	"net/netip"
	"slices"
	"strconv"
	"strings"
)

const maxAdvertisedAddresses = 8

type advertiseKind uint8

const (
	advertiseKindNone advertiseKind = iota
	advertiseKindHost
	advertiseKindHostPort
	advertiseKindPort
)

type advertiseSpec struct {
	Kind advertiseKind
	Host string
	Port int
}

func parseAdvertise(s string) (advertiseSpec, error) {
	if s == "" {
		return advertiseSpec{Kind: advertiseKindNone}, nil
	}
	if strings.HasPrefix(s, ":") {
		port, err := parseAdvertisePort(s[1:])
		if err != nil {
			return advertiseSpec{}, err
		}
		return advertiseSpec{Kind: advertiseKindPort, Port: port}, nil
	}
	host, port, hasPort, err := splitAdvertiseHostPort(s)
	if err != nil {
		return advertiseSpec{}, err
	}
	if !hasPort {
		return advertiseSpec{Kind: advertiseKindHost, Host: host}, nil
	}
	return advertiseSpec{Kind: advertiseKindHostPort, Host: host, Port: port}, nil
}

func validateExtraItem(item string) error {
	_, _, _, err := splitAdvertiseHostPort(item)
	return err
}

func parseExtra(items []string, adv advertiseSpec, listen listenAddr) ([]AdvertisedAddr, error) {
	if len(items) == 0 {
		return nil, nil
	}
	defaultPort := listen.port
	if adv.Kind == advertiseKindHostPort || adv.Kind == advertiseKindPort {
		defaultPort = adv.Port
	}
	out := make([]AdvertisedAddr, 0, len(items))
	for i, item := range items {
		host, port, hasPort, err := splitAdvertiseHostPort(item)
		if err != nil {
			return nil, fmt.Errorf("item %d: %w", i+1, err)
		}
		if !hasPort {
			if defaultPort == 0 {
				return nil, fmt.Errorf("item %d: port is required when no listen address or advertise port is set", i+1)
			}
			port = defaultPort
		}
		out = append(out, AdvertisedAddr{Kind: AdvertisedKindDirect, Addr: joinHostPort(host, port)})
	}
	return out, nil
}

func splitAdvertiseHostPort(s string) (host string, port int, hasPort bool, err error) {
	if strings.HasPrefix(s, "[") {
		end := strings.IndexByte(s, ']')
		if end < 0 {
			return "", 0, false, fmt.Errorf("IPv6 address is missing the closing bracket")
		}
		rawHost := s[1:end]
		rest := s[end+1:]
		h, err := parseIPv6Host(rawHost)
		if err != nil {
			return "", 0, false, err
		}
		if rest == "" {
			return h, 0, false, nil
		}
		if !strings.HasPrefix(rest, ":") {
			return "", 0, false, fmt.Errorf("unexpected %q after the IPv6 address", rest)
		}
		p, err := parseAdvertisePort(rest[1:])
		if err != nil {
			return "", 0, false, err
		}
		return h, p, true, nil
	}

	switch strings.Count(s, ":") {
	case 0:
		h, err := parseAdvertiseHost(s)
		return h, 0, false, err
	case 1:
		rawHost, rawPort, _ := strings.Cut(s, ":")
		h, err := parseAdvertiseHost(rawHost)
		if err != nil {
			return "", 0, false, err
		}
		p, err := parseAdvertisePort(rawPort)
		if err != nil {
			return "", 0, false, err
		}
		return h, p, true, nil
	default:
		return "", 0, false, fmt.Errorf("IPv6 addresses must be written in brackets")
	}
}

func parseAdvertiseHost(raw string) (string, error) {
	if raw == "" {
		return "", fmt.Errorf("host must not be empty")
	}
	if addr, err := netip.ParseAddr(raw); err == nil {
		return canonicalAdvertiseAddr(addr)
	}
	if !ValidDialHost(raw) {
		return "", fmt.Errorf("host %q is not a valid host name", raw)
	}
	return raw, nil
}

// ValidDialHost reports whether host, written without brackets, is an IP
// address or a domain name this system can dial: labels of 1 to 63 characters
// from letters, digits, '-' and '_' that do not begin or end with '-', at most
// 253 characters plus an optional trailing dot.
func ValidDialHost(host string) bool {
	if net.ParseIP(host) != nil {
		return true
	}
	host = strings.TrimSuffix(host, ".")
	if len(host) == 0 || len(host) > 253 {
		return false
	}
	for _, label := range strings.Split(host, ".") {
		if len(label) == 0 || len(label) > 63 {
			return false
		}
		for i := 0; i < len(label); i++ {
			c := label[i]
			switch {
			case c >= 'a' && c <= 'z', c >= 'A' && c <= 'Z', c >= '0' && c <= '9', c == '-', c == '_':
			default:
				return false
			}
		}
		if label[0] == '-' || label[len(label)-1] == '-' {
			return false
		}
	}
	return true
}

func parseIPv6Host(raw string) (string, error) {
	addr, err := netip.ParseAddr(raw)
	if err != nil || !addr.Is6() {
		return "", fmt.Errorf("invalid IPv6 address %q", raw)
	}
	return canonicalAdvertiseAddr(addr)
}

func canonicalAdvertiseAddr(addr netip.Addr) (string, error) {
	if addr.Zone() != "" {
		return "", fmt.Errorf("advertised address must not carry a zone")
	}
	if addr.IsUnspecified() {
		return "", fmt.Errorf("wildcard address %s cannot be advertised", addr)
	}
	return addr.String(), nil
}

func parseAdvertisePort(s string) (int, error) {
	if s == "" {
		return 0, fmt.Errorf("port must not be empty")
	}
	for i := 0; i < len(s); i++ {
		if s[i] < '0' || s[i] > '9' {
			return 0, fmt.Errorf("port %q must be a decimal number", s)
		}
	}
	n, err := strconv.Atoi(s)
	if err != nil || n < 1 || n > 65535 {
		return 0, fmt.Errorf("port %q must be between 1 and 65535", s)
	}
	return n, nil
}

func joinHostPort(host string, port int) string {
	if strings.HasPrefix(host, "[") && strings.HasSuffix(host, "]") {
		host = host[1 : len(host)-1]
	}
	return net.JoinHostPort(host, strconv.Itoa(port))
}

func computeAdvertised(listen listenAddr, adv advertiseSpec, extra []AdvertisedAddr, routes []netip.Addr) ([]AdvertisedAddr, error) {
	if !listen.set {
		if adv.Kind != advertiseKindNone {
			return nil, fmt.Errorf("%w: broker.listen and broker.advertise: an advertise address requires a listen address", ErrInvalidCombination)
		}
		if len(extra) > 0 {
			return nil, fmt.Errorf("%w: broker.listen and broker.advertise_extra: extra addresses require a listen address", ErrInvalidCombination)
		}
		return []AdvertisedAddr{}, nil
	}
	if adv.Kind == advertiseKindPort && !listen.all {
		return nil, fmt.Errorf("%w: broker.listen and broker.advertise: a port-only broker.advertise requires listening on every address", ErrInvalidCombination)
	}

	configured := len(extra)
	switch adv.Kind {
	case advertiseKindHost, advertiseKindHostPort:
		configured++
	case advertiseKindNone:
		if !listen.all {
			configured++
		}
	}
	if configured > maxAdvertisedAddresses {
		return nil, fmt.Errorf("%w: broker.advertise_extra: %d configured addresses; at most %d are allowed", ErrTooManyAddresses, configured, maxAdvertisedAddresses)
	}

	out := make([]AdvertisedAddr, 0, maxAdvertisedAddresses)
	switch {
	case adv.Kind == advertiseKindHostPort:
		out = append(out, AdvertisedAddr{Kind: AdvertisedKindDirect, Addr: joinHostPort(adv.Host, adv.Port)})
	case adv.Kind == advertiseKindHost:
		out = append(out, AdvertisedAddr{Kind: AdvertisedKindDirect, Addr: joinHostPort(adv.Host, listen.port)})
	case adv.Kind == advertiseKindNone && !listen.all:
		out = append(out, AdvertisedAddr{Kind: AdvertisedKindDirect, Addr: joinHostPort(listen.host, listen.port)})
	default:
		port := listen.port
		if adv.Kind == advertiseKindPort {
			port = adv.Port
		}
		keep := maxAdvertisedAddresses - len(extra)
		if keep < 0 {
			keep = 0
		}
		for i, addr := range routes {
			if i >= keep {
				break
			}
			out = append(out, AdvertisedAddr{Kind: AdvertisedKindDirect, Addr: joinHostPort(addr.String(), port)})
		}
	}
	out = append(out, extra...)
	return dedupeAdvertised(out), nil
}

func needsRoutes(listen listenAddr, adv advertiseSpec) bool {
	if !listen.set || !listen.all {
		return false
	}
	return adv.Kind == advertiseKindNone || adv.Kind == advertiseKindPort
}

func dedupeAdvertised(in []AdvertisedAddr) []AdvertisedAddr {
	seen := make(map[AdvertisedAddr]struct{}, len(in))
	out := make([]AdvertisedAddr, 0, len(in))
	for _, a := range in {
		if _, ok := seen[a]; ok {
			continue
		}
		seen[a] = struct{}{}
		out = append(out, a)
	}
	return out
}

func sameAddresses(a, b []AdvertisedAddr) bool {
	return slices.Equal(a, b)
}
