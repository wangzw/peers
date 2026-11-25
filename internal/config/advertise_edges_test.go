package config

import (
	"strings"
	"testing"
)

// TestSplitAdvertiseHostPortIPv6Errors checks that each of the five malformed
// bracketed IPv6 advertise forms is reported and a well-formed one still
// parses.
func TestSplitAdvertiseHostPortIPv6Errors(t *testing.T) {
	host, port, hasPort, err := splitAdvertiseHostPort("[2001:db8::1]:8443")
	if err != nil {
		t.Fatalf("splitAdvertiseHostPort(bracketed IPv6): %v", err)
	}
	if host != "2001:db8::1" || port != 8443 || !hasPort {
		t.Fatalf("splitAdvertiseHostPort(bracketed IPv6) = %q, %d, %v", host, port, hasPort)
	}

	bad := []struct {
		in   string
		want string
	}{
		{"[2001:db8::1", "missing the closing bracket"},
		{"[2001:db8::1]x", `unexpected "x" after the IPv6 address`},
		{"[2001:db8::1]:", "port must not be empty"},
		{"[2001:db8::1]:abc", `port "abc" must be a decimal number`},
		{"[192.0.2.1]:443", "invalid IPv6 address"},
	}
	for _, tc := range bad {
		_, _, _, err := splitAdvertiseHostPort(tc.in)
		if err == nil || !strings.Contains(err.Error(), tc.want) {
			t.Errorf("splitAdvertiseHostPort(%q) error = %v, want %q", tc.in, err, tc.want)
		}
	}
}

// TestParseIPv6HostAcceptsOnlyIPv6 checks that parseIPv6Host accepts IPv6
// literals and canonicalises them.
func TestParseIPv6HostAcceptsOnlyIPv6(t *testing.T) {
	if got, err := parseIPv6Host("2001:db8::1"); err != nil || got != "2001:db8::1" {
		t.Fatalf("parseIPv6Host(2001:db8::1) = %q, %v", got, err)
	}
	for _, in := range []string{"192.0.2.1", "b.example"} {
		if _, err := parseIPv6Host(in); err == nil || !strings.Contains(err.Error(), "invalid IPv6 address") {
			t.Errorf("parseIPv6Host(%q) = %v, want the invalid-IPv6 error", in, err)
		}
	}
}

// TestValidDialHostAcceptsIPLiterals checks that ValidDialHost accepts an IP
// literal through its fast path and still rejects the malformed inputs the
// label rules catch.
func TestValidDialHostAcceptsIPLiterals(t *testing.T) {
	for _, host := range []string{"192.0.2.1", "2001:db8::1"} {
		if !ValidDialHost(host) {
			t.Errorf("ValidDialHost(%q) = false, want true for an IP literal", host)
		}
	}
	for _, host := range []string{"", "a..b", "-a.example", "a-.example"} {
		if ValidDialHost(host) {
			t.Errorf("ValidDialHost(%q) = true, want false", host)
		}
	}
}

// TestJoinHostPortFormatsHosts checks that joinHostPort brackets an IPv6 host
// and leaves a domain name alone.
func TestJoinHostPortFormatsHosts(t *testing.T) {
	cases := []struct {
		host string
		port int
		want string
	}{
		{"2001:db8::1", 8443, "[2001:db8::1]:8443"},
		{"b.example", 443, "b.example:443"},
	}
	for _, tc := range cases {
		if got := joinHostPort(tc.host, tc.port); got != tc.want {
			t.Errorf("joinHostPort(%q, %d) = %q, want %q", tc.host, tc.port, got, tc.want)
		}
	}
}
