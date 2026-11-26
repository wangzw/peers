//go:build peers_testenv

package main

import "testing"

// TestParseProtocolOverrides checks that valid values parse and malformed or
// out-of-range ones become 0.
func TestParseProtocolOverrides(t *testing.T) {
	cases := []struct {
		local, link string
		wantLocal   uint32
		wantLink    uint32
	}{
		{local: "2", link: "3", wantLocal: 2, wantLink: 3},
		{local: "", link: "", wantLocal: 0, wantLink: 0},
		{local: "x", link: "", wantLocal: 0, wantLink: 0},
		{local: "-1", link: "", wantLocal: 0, wantLink: 0},
		{local: "4294967296", link: "", wantLocal: 0, wantLink: 0},
	}
	for _, c := range cases {
		env := map[string]string{
			"PEERS_TEST_LOCAL_PROTOCOL_VERSION": c.local,
			"PEERS_TEST_LINK_PROTOCOL_VERSION":  c.link,
		}
		local, link := parseProtocolOverrides(func(k string) string { return env[k] })
		if local != c.wantLocal || link != c.wantLink {
			t.Errorf("parseProtocolOverrides(local=%q, link=%q) = (%d, %d), want (%d, %d)",
				c.local, c.link, local, link, c.wantLocal, c.wantLink)
		}
	}
}

// TestProtocolOverridesInstalled checks that the testenv init hook makes
// protocolOverrides read the environment.
func TestProtocolOverridesInstalled(t *testing.T) {
	t.Setenv("PEERS_TEST_LOCAL_PROTOCOL_VERSION", "5")
	t.Setenv("PEERS_TEST_LINK_PROTOCOL_VERSION", "6")

	local, link := protocolOverrides()
	if local != 5 || link != 6 {
		t.Fatalf("protocolOverrides() = (%d, %d), want (5, 6)", local, link)
	}
}
