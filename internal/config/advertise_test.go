package config

import (
	"errors"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"testing"
)

func wildcardListen(port int) listenAddr {
	return listenAddr{set: true, all: true, host: "", port: port}
}

func specificListen(host string, port int) listenAddr {
	return listenAddr{set: true, all: false, host: host, port: port}
}

func directAddr(addr string) AdvertisedAddr {
	return AdvertisedAddr{Kind: AdvertisedKindDirect, Addr: addr}
}

func directHostPort(host string, port int) AdvertisedAddr {
	return directAddr(net.JoinHostPort(host, strconv.Itoa(port)))
}

func mustAddr(t *testing.T, s string) netip.Addr {
	t.Helper()
	a, err := netip.ParseAddr(s)
	if err != nil {
		t.Fatalf("parse %q: %v", s, err)
	}
	return a
}

func assertAddresses(t *testing.T, got, want []AdvertisedAddr) {
	t.Helper()
	if !sameAddresses(got, want) {
		t.Fatalf("advertised addresses = %v, want %v", got, want)
	}
	for _, a := range got {
		if a.Kind != AdvertisedKindDirect {
			t.Fatalf("address %v has kind %q, want %q", a, a.Kind, AdvertisedKindDirect)
		}
	}
}

func makeExtra(n int) []AdvertisedAddr {
	out := make([]AdvertisedAddr, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, directAddr("extra"+strconv.Itoa(i+1)+".example:17443"))
	}
	return out
}

func makeRoutes(n int) []netip.Addr {
	out := make([]netip.Addr, 0, n)
	for i := 0; i < n; i++ {
		out = append(out, netip.AddrFrom4([4]byte{192, 0, 2, byte(i + 1)}))
	}
	return out
}

// TestAdvertisedCombinationTable exercises every row of the combination
// table in design-config.md without touching the host network.
func TestAdvertisedCombinationTable(t *testing.T) {
	routes := []netip.Addr{mustAddr(t, "192.0.2.20"), mustAddr(t, "2001:db8::20")}
	tests := []struct {
		name   string
		listen listenAddr
		adv    advertiseSpec
		extra  []AdvertisedAddr
		want   []AdvertisedAddr
	}{
		{
			name:   "row 1 host:port ignores default routes",
			listen: wildcardListen(17443),
			adv:    advertiseSpec{Kind: advertiseKindHostPort, Host: "b.example", Port: 443},
			want:   []AdvertisedAddr{directHostPort("b.example", 443)},
		},
		{
			name:   "row 2 host takes the listen port",
			listen: wildcardListen(17443),
			adv:    advertiseSpec{Kind: advertiseKindHost, Host: "b.example"},
			want:   []AdvertisedAddr{directHostPort("b.example", 17443)},
		},
		{
			name:   "row 3 specific listen is the first item",
			listen: specificListen("192.0.2.20", 17443),
			adv:    advertiseSpec{Kind: advertiseKindNone},
			want:   []AdvertisedAddr{directHostPort("192.0.2.20", 17443)},
		},
		{
			name:   "row 4 default routes with the listen port",
			listen: wildcardListen(17443),
			adv:    advertiseSpec{Kind: advertiseKindNone},
			want: []AdvertisedAddr{
				directHostPort("192.0.2.20", 17443),
				directHostPort("2001:db8::20", 17443),
			},
		},
		{
			name:   "row 5 default routes with the advertise port",
			listen: wildcardListen(17443),
			adv:    advertiseSpec{Kind: advertiseKindPort, Port: 18443},
			want: []AdvertisedAddr{
				directHostPort("192.0.2.20", 18443),
				directHostPort("2001:db8::20", 18443),
			},
		},
		{
			name:   "row 6 no listen and no advertise",
			listen: listenAddr{},
			adv:    advertiseSpec{Kind: advertiseKindNone},
			want:   []AdvertisedAddr{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := computeAdvertised(tt.listen, tt.adv, tt.extra, routes)
			if err != nil {
				t.Fatalf("computeAdvertised: %v", err)
			}
			assertAddresses(t, got, tt.want)
		})
	}
}

func TestREQBrokerConfig030_DefaultRouteOnly(t *testing.T) {
	routes := []netip.Addr{mustAddr(t, "192.0.2.20"), mustAddr(t, "2001:db8::20")}
	got, err := computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindNone}, nil, routes)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, []AdvertisedAddr{
		directHostPort("192.0.2.20", 17443),
		directHostPort("2001:db8::20", 17443),
	})
}

func TestREQBrokerConfig030_PortOnly(t *testing.T) {
	routes := []netip.Addr{mustAddr(t, "192.0.2.20")}
	got, err := computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindPort, Port: 18443}, nil, routes)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, []AdvertisedAddr{directHostPort("192.0.2.20", 18443)})
}

func TestREQBrokerConfig030_ExtraAppended(t *testing.T) {
	routes := []netip.Addr{mustAddr(t, "192.0.2.20")}
	extra := []AdvertisedAddr{directAddr("b.vpn.example:18443")}
	got, err := computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindPort, Port: 18443}, extra, routes)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, []AdvertisedAddr{
		directHostPort("192.0.2.20", 18443),
		directAddr("b.vpn.example:18443"),
	})
}

func TestREQBrokerConfig031_HostPortOnly(t *testing.T) {
	routes := []netip.Addr{mustAddr(t, "192.0.2.20")}
	got, err := computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindHostPort, Host: "b.example", Port: 443}, nil, routes)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, []AdvertisedAddr{directHostPort("b.example", 443)})
}

func TestREQBrokerConfig031_HostOnly(t *testing.T) {
	got, err := computeAdvertised(specificListen("192.0.2.20", 17443), advertiseSpec{Kind: advertiseKindHost, Host: "b.example"}, nil, nil)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, []AdvertisedAddr{directHostPort("b.example", 17443)})
}

func TestREQBrokerConfig031_ExtraAfterHost(t *testing.T) {
	extra := []AdvertisedAddr{directAddr("b.vpn.example:17443")}
	got, err := computeAdvertised(specificListen("192.0.2.20", 17443), advertiseSpec{Kind: advertiseKindHost, Host: "b.example"}, extra, nil)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, []AdvertisedAddr{
		directHostPort("b.example", 17443),
		directAddr("b.vpn.example:17443"),
	})
}

func TestREQBrokerConfig035_SpecificListenFirst(t *testing.T) {
	extra := []AdvertisedAddr{directAddr("b.vpn.example:17443")}
	got, err := computeAdvertised(specificListen("192.0.2.20", 17443), advertiseSpec{Kind: advertiseKindNone}, extra, nil)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, []AdvertisedAddr{
		directHostPort("192.0.2.20", 17443),
		directAddr("b.vpn.example:17443"),
	})
}

func TestREQBrokerConfig032_CombinationErrorsNameTheKeys(t *testing.T) {
	tests := []struct {
		name     string
		listen   listenAddr
		adv      advertiseSpec
		extra    []AdvertisedAddr
		wantKeys []string
	}{
		{
			name:     "specific listen with a port-only advertise",
			listen:   specificListen("192.0.2.20", 17443),
			adv:      advertiseSpec{Kind: advertiseKindPort, Port: 18443},
			wantKeys: []string{"broker.listen", "broker.advertise"},
		},
		{
			name:     "not listening with an advertise address",
			listen:   listenAddr{},
			adv:      advertiseSpec{Kind: advertiseKindHost, Host: "b.example"},
			wantKeys: []string{"broker.listen", "broker.advertise"},
		},
		{
			name:     "not listening with a port-only advertise",
			listen:   listenAddr{},
			adv:      advertiseSpec{Kind: advertiseKindPort, Port: 18443},
			wantKeys: []string{"broker.listen", "broker.advertise"},
		},
		{
			name:     "not listening with extra addresses",
			listen:   listenAddr{},
			adv:      advertiseSpec{Kind: advertiseKindNone},
			extra:    []AdvertisedAddr{directAddr("b.example:17443")},
			wantKeys: []string{"broker.listen", "broker.advertise_extra"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := computeAdvertised(tt.listen, tt.adv, tt.extra, nil)
			if !errors.Is(err, ErrInvalidCombination) {
				t.Fatalf("error = %v, want ErrInvalidCombination", err)
			}
			for _, key := range tt.wantKeys {
				if !strings.Contains(err.Error(), key) {
					t.Fatalf("error %q does not name %s", err, key)
				}
			}
		})
	}
}

func TestREQBrokerConfig033_ExtraAddresses(t *testing.T) {
	adv := advertiseSpec{Kind: advertiseKindPort, Port: 18443}
	extra, err := parseExtra([]string{"b.vpn.example", "198.51.100.7:8443", "[2001:db8::7]"}, adv, wildcardListen(17443))
	if err != nil {
		t.Fatalf("parseExtra: %v", err)
	}
	assertAddresses(t, extra, []AdvertisedAddr{
		directAddr("b.vpn.example:18443"),
		directAddr("198.51.100.7:8443"),
		directAddr("[2001:db8::7]:18443"),
	})
}

func TestREQBrokerConfig033_ExtraPortDefault(t *testing.T) {
	tests := []struct {
		name   string
		adv    advertiseSpec
		listen listenAddr
		want   string
	}{
		{"advertise host:port wins", advertiseSpec{Kind: advertiseKindHostPort, Host: "b.example", Port: 443}, wildcardListen(17443), "b.vpn.example:443"},
		{"advertise port wins", advertiseSpec{Kind: advertiseKindPort, Port: 18443}, wildcardListen(17443), "b.vpn.example:18443"},
		{"listen port otherwise", advertiseSpec{Kind: advertiseKindNone}, wildcardListen(17443), "b.vpn.example:17443"},
		{"advertise host uses the listen port", advertiseSpec{Kind: advertiseKindHost, Host: "b.example"}, wildcardListen(17443), "b.vpn.example:17443"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := parseExtra([]string{"b.vpn.example"}, tt.adv, tt.listen)
			if err != nil {
				t.Fatalf("parseExtra: %v", err)
			}
			assertAddresses(t, got, []AdvertisedAddr{directAddr(tt.want)})
		})
	}
}

func TestREQBrokerConfig036_TooManyConfigured(t *testing.T) {
	hostPort := advertiseSpec{Kind: advertiseKindHostPort, Host: "b.example", Port: 443}
	if _, err := computeAdvertised(wildcardListen(17443), hostPort, makeExtra(8), nil); !errors.Is(err, ErrTooManyAddresses) {
		t.Fatalf("host + 8 extra: error = %v, want ErrTooManyAddresses", err)
	} else if !strings.Contains(err.Error(), "broker.advertise_extra") {
		t.Fatalf("error %q does not name broker.advertise_extra", err)
	}
	if got, err := computeAdvertised(wildcardListen(17443), hostPort, makeExtra(7), nil); err != nil {
		t.Fatalf("host + 7 extra: %v", err)
	} else if len(got) != 8 {
		t.Fatalf("host + 7 extra: %d addresses, want 8", len(got))
	}

	specific := specificListen("192.0.2.20", 17443)
	if _, err := computeAdvertised(specific, advertiseSpec{Kind: advertiseKindNone}, makeExtra(8), nil); !errors.Is(err, ErrTooManyAddresses) {
		t.Fatalf("specific listen + 8 extra: error = %v, want ErrTooManyAddresses", err)
	}

	portOnly := advertiseSpec{Kind: advertiseKindPort, Port: 18443}
	got, err := computeAdvertised(wildcardListen(17443), portOnly, makeExtra(8), makeRoutes(3))
	if err != nil {
		t.Fatalf("port-only + 8 extra: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("port-only + 8 extra: %d addresses, want 8", len(got))
	}
	for _, a := range got {
		if !strings.HasPrefix(a.Addr, "extra") {
			t.Fatalf("port-only + 8 extra kept a default-route address: %v", got)
		}
	}
}

func TestREQBrokerConfig036_TruncateDefaultRoute(t *testing.T) {
	extra7 := makeExtra(7)
	got, err := computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindNone}, extra7, makeRoutes(10))
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("%d addresses, want 8", len(got))
	}
	assertAddresses(t, got[:1], []AdvertisedAddr{directHostPort("192.0.2.1", 17443)})
	assertAddresses(t, got[1:], extra7)

	extra8 := makeExtra(8)
	got, err = computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindNone}, extra8, makeRoutes(10))
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, extra8)

	got, err = computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindNone}, nil, makeRoutes(10))
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("%d addresses, want 8", len(got))
	}

	got, err = computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindNone}, extra7, makeRoutes(3))
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	if len(got) != 8 {
		t.Fatalf("%d addresses, want 8", len(got))
	}
}

func TestConfigAdvertisedOrderAndDedup(t *testing.T) {
	routes := []netip.Addr{mustAddr(t, "192.0.2.20")}
	extra := []AdvertisedAddr{
		directAddr("192.0.2.20:17443"),
		directAddr("b.example:17443"),
		directAddr("b.example:17443"),
	}
	got, err := computeAdvertised(wildcardListen(17443), advertiseSpec{Kind: advertiseKindNone}, extra, routes)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	assertAddresses(t, got, []AdvertisedAddr{
		directAddr("192.0.2.20:17443"),
		directAddr("b.example:17443"),
	})
}

func TestParseAdvertise(t *testing.T) {
	tests := []struct {
		in      string
		want    advertiseSpec
		wantErr error
	}{
		{in: "", want: advertiseSpec{Kind: advertiseKindNone}},
		{in: "b.example", want: advertiseSpec{Kind: advertiseKindHost, Host: "b.example"}},
		{in: "b.example:443", want: advertiseSpec{Kind: advertiseKindHostPort, Host: "b.example", Port: 443}},
		{in: "_dmz.example:443", want: advertiseSpec{Kind: advertiseKindHostPort, Host: "_dmz.example", Port: 443}},
		{in: "192.0.2.20:443", want: advertiseSpec{Kind: advertiseKindHostPort, Host: "192.0.2.20", Port: 443}},
		{in: "[2001:0db8::1]:443", want: advertiseSpec{Kind: advertiseKindHostPort, Host: "2001:db8::1", Port: 443}},
		{in: "[2001:db8::1]", want: advertiseSpec{Kind: advertiseKindHost, Host: "2001:db8::1"}},
		{in: ":443", want: advertiseSpec{Kind: advertiseKindPort, Port: 443}},
		{in: "0.0.0.0:443", wantErr: ErrInvalidValue},
		{in: "[::]:443", wantErr: ErrInvalidValue},
		{in: "[::]", wantErr: ErrInvalidValue},
		{in: "2001:db8::1:443", wantErr: ErrInvalidValue},
		{in: "2001:db8::1", wantErr: ErrInvalidValue},
		{in: "[fe80::1%en0]", wantErr: ErrInvalidValue},
		{in: ":0", wantErr: ErrInvalidValue},
		{in: ":65536", wantErr: ErrInvalidValue},
		{in: ":abc", wantErr: ErrInvalidValue},
		{in: "b.example:0", wantErr: ErrInvalidValue},
		{in: "b.example:65536", wantErr: ErrInvalidValue},
		{in: "b.example:abc", wantErr: ErrInvalidValue},
		{in: "b.example:", wantErr: ErrInvalidValue},
		{in: "bad host:443", wantErr: ErrInvalidValue},
		{in: "relay://b.example:443", wantErr: ErrInvalidValue},
		{in: "h.example.:443", want: advertiseSpec{Kind: advertiseKindHostPort, Host: "h.example.", Port: 443}},
		{in: "h.example..:443", wantErr: ErrInvalidValue},
		{in: "-h.example:443", wantErr: ErrInvalidValue},
		{in: strings.Repeat("a", 64) + ".example:443", wantErr: ErrInvalidValue},
		{in: strings.Repeat("a", 63) + ".example:443", want: advertiseSpec{Kind: advertiseKindHostPort, Host: strings.Repeat("a", 63) + ".example", Port: 443}},
	}
	for _, tt := range tests {
		t.Run(tt.in, func(t *testing.T) {
			got, err := parseAdvertise(tt.in)
			if tt.wantErr != nil {
				if err == nil {
					t.Fatalf("parseAdvertise(%q) succeeded, want an error", tt.in)
				}
				return
			}
			if err != nil {
				t.Fatalf("parseAdvertise(%q): %v", tt.in, err)
			}
			if got != tt.want {
				t.Fatalf("parseAdvertise(%q) = %+v, want %+v", tt.in, got, tt.want)
			}
		})
	}
}

// TestValidDialHostTrailingDot pins the trailing-dot rule: an FQDN written
// with one trailing dot is dialable and accepted, while two trailing dots
// leave an empty label and are rejected.
func TestValidDialHostTrailingDot(t *testing.T) {
	tests := []struct {
		host string
		want bool
	}{
		{"h.example.", true},
		{"h.example..", false},
		{"h.example", true},
		{".", false},
		{"..", false},
	}
	for _, tt := range tests {
		if got := ValidDialHost(tt.host); got != tt.want {
			t.Fatalf("ValidDialHost(%q) = %v, want %v", tt.host, got, tt.want)
		}
	}
}

func TestParseExtraRejectsInvalidItems(t *testing.T) {
	adv := advertiseSpec{Kind: advertiseKindNone}
	listen := wildcardListen(17443)
	tests := []struct {
		name    string
		item    string
		wantErr error
	}{
		{"empty", "", ErrInvalidValue},
		{"port only", ":8443", ErrInvalidValue},
		{"wildcard", "0.0.0.0:8443", ErrInvalidValue},
		{"unbracketed IPv6", "2001:db8::1", ErrInvalidValue},
		{"unbracketed IPv6 with port", "2001:db8::1:8443", ErrInvalidValue},
		{"prefixed item", "relay://b.example", ErrInvalidValue},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parseExtra([]string{tt.item}, adv, listen)
			if err == nil {
				t.Fatalf("parseExtra(%q) succeeded, want an error", tt.item)
			}
		})
	}
}

func TestNeedsRoutes(t *testing.T) {
	tests := []struct {
		name   string
		listen listenAddr
		adv    advertiseSpec
		want   bool
	}{
		{"wildcard with no advertise", wildcardListen(17443), advertiseSpec{Kind: advertiseKindNone}, true},
		{"wildcard with a port", wildcardListen(17443), advertiseSpec{Kind: advertiseKindPort, Port: 18443}, true},
		{"wildcard with a host", wildcardListen(17443), advertiseSpec{Kind: advertiseKindHost, Host: "b.example"}, false},
		{"specific with no advertise", specificListen("192.0.2.20", 17443), advertiseSpec{Kind: advertiseKindNone}, false},
		{"not listening", listenAddr{}, advertiseSpec{Kind: advertiseKindNone}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := needsRoutes(tt.listen, tt.adv); got != tt.want {
				t.Fatalf("needsRoutes = %v, want %v", got, tt.want)
			}
		})
	}
}

// TestREQBrokerDiscovery016_ReenumerateChanges proves that a changed
// default-route enumeration produces a different list, which is what the
// periodic checker and SubscribeAdvertised report.
func TestREQBrokerDiscovery016_ReenumerateChanges(t *testing.T) {
	first := []netip.Addr{mustAddr(t, "198.51.100.4")}
	second := []netip.Addr{mustAddr(t, "198.51.100.9")}
	listen := wildcardListen(17443)
	adv := advertiseSpec{Kind: advertiseKindNone}

	before, err := computeAdvertised(listen, adv, nil, first)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	after, err := computeAdvertised(listen, adv, nil, second)
	if err != nil {
		t.Fatalf("computeAdvertised: %v", err)
	}
	if sameAddresses(before, after) {
		t.Fatalf("address lists are equal after the default route changed: %v", before)
	}
	assertAddresses(t, before, []AdvertisedAddr{directHostPort("198.51.100.4", 17443)})
	assertAddresses(t, after, []AdvertisedAddr{directHostPort("198.51.100.9", 17443)})
}

func TestSameAddressesIgnoresNilVersusEmpty(t *testing.T) {
	if !sameAddresses(nil, []AdvertisedAddr{}) {
		t.Fatal("nil and empty lists must compare equal")
	}
}
