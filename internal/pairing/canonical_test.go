package pairing

import "testing"

// TestCanonicalAddress checks lowercasing, canonical IP form, port
// normalization and the fallback for an unparsable address.
func TestCanonicalAddress(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		in   string
		want string
	}{
		{"HOSTA.Example:017443", "hosta.example:17443"},
		{"hosta.example:17443", "hosta.example:17443"},
		{"[2001:0DB8:0:0:0:0:0:1]:017443", "[2001:db8::1]:17443"},
		{"[::1]:00080", "[::1]:80"},
		{"10.0.0.1:0001", "10.0.0.1:1"},
		{"HOSTA.Example:99999", "hosta.example:99999"},
		{"HOSTA.Example:abc", "hosta.example:abc"},
		{"NotAnAddress", "notanaddress"},
		{"", ""},
	} {
		if got := CanonicalAddress(tc.in); got != tc.want {
			t.Errorf("CanonicalAddress(%q) = %q, want %q", tc.in, got, tc.want)
		}
	}
}
