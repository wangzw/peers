package link

import "testing"

func TestSourcePrefix(t *testing.T) {
	tests := []struct {
		name   string
		remote string
		want   string
	}{
		{"ipv4", "203.0.113.7:443", "203.0.113.7/32"},
		{"ipv4 another address", "203.0.113.8:443", "203.0.113.8/32"},
		{"ipv6 same network first", "[2001:db8:1:2::1]:443", "2001:db8:1:2::/64"},
		{"ipv6 same network second", "[2001:db8:1:2::abcd]:443", "2001:db8:1:2::/64"},
		{"ipv6 other network", "[2001:db8:1:3::1]:443", "2001:db8:1:3::/64"},
		{"ipv4-mapped ipv6", "[::ffff:203.0.113.7]:443", "203.0.113.7/32"},
		{"ipv6 with zone", "[fe80::1%eth0]:443", "fe80::/64"},
		{"no port", "203.0.113.7", ""},
		{"empty", "", ""},
		{"non-ip", "pipe", ""},
		{"hostname", "example.com:443", ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := sourcePrefix(tt.remote); got != tt.want {
				t.Fatalf("sourcePrefix(%q) = %q, want %q", tt.remote, got, tt.want)
			}
		})
	}
}
