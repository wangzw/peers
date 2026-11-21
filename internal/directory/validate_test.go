package directory

import (
	"reflect"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/directory/directorypb"
)

func TestValidPublicTopicName(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"one byte", "a", true},
		{"256 bytes", strings.Repeat("n", 256), true},
		{"257 bytes", strings.Repeat("n", 257), false},
		{"empty", "", false},
		{"multi-byte UTF-8", "话题", true},
		{"newline", "a\nb", false},
		{"escape", "a\x1bb", false},
		{"invalid UTF-8", "a\xffb", false},
	}
	for _, tc := range cases {
		if got := validPublicTopicName(tc.in); got != tc.want {
			t.Errorf("validPublicTopicName(%q) = %v, want %v", tc.in, got, tc.want)
		}
	}
}

func TestValidDirectAddr(t *testing.T) {
	cases := []struct {
		name string
		in   string
		want bool
	}{
		{"host name", "example:443", true},
		{"ipv4", "198.51.100.4:17443", true},
		{"ipv6", "[2001:db8::1]:443", true},
		{"port with leading zeros", "SERVICE.Example:017443", true},
		{"largest port", "example:65535", true},
		{"port zero", "example:0", false},
		{"port above 65535", "example:65536", false},
		{"empty port", "example:", false},
		{"no port", "example", false},
		{"empty host", ":443", false},
		{"space in host", "exa mple:443", false},
		{"tab in host", "exa\tmple:443", false},
		{"escape in host", "\x1bexample:443", false},
		{"no-break space in host", "exa mple:443", false},
		{"253-byte host", strings.Repeat("h", 253) + ":443", true},
		{"254-byte host", strings.Repeat("h", 254) + ":443", false},
		{"signed port", "example:+443", false},
		{"hex port", "example:0x1bb", false},
		{"garbage", "not an address", false},
	}
	for _, tc := range cases {
		if got := validDirectAddr(tc.in); got != tc.want {
			t.Errorf("%s: validDirectAddr(%q) = %v, want %v", tc.name, tc.in, got, tc.want)
		}
	}
}

func TestKnownAddrsSkipsMalformedDirectAddresses(t *testing.T) {
	card := &directorypb.BrokerCard{Addresses: []*directorypb.AdvertisedAddress{
		{Kind: "direct", Addr: "\x1b198.51.100.5:17443"},
		{Kind: "direct", Addr: "198.51.100.4:17443"},
		{Kind: "direct", Addr: "no-port"},
		{Kind: "relay", Addr: "relay.example:17443"},
		{Kind: "direct", Addr: "198.51.100.6:17443"},
	}}

	want := []config.AdvertisedAddr{
		{Kind: "direct", Addr: "198.51.100.4:17443"},
		{Kind: "direct", Addr: "198.51.100.6:17443"},
	}
	if got := knownAddrs(card, 8); !reflect.DeepEqual(got, want) {
		t.Fatalf("knownAddrs = %v, want %v", got, want)
	}

	// The limit counts only entries that pass the syntax check.
	if got := knownAddrs(card, 1); !reflect.DeepEqual(got, want[:1]) {
		t.Errorf("knownAddrs with max 1 = %v, want %v", got, want[:1])
	}
}
