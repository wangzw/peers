package syscontainer

import (
	"regexp"
	"strings"
	"testing"

	"github.com/wangzw/peers/test/e2e/harness"
)

// TestSystemSubnetsDisjointFromRoutedAllocator checks that the subnets the
// system container network can take never overlap the subnets the harness routed
// allocator hands out.
func TestSystemSubnetsDisjointFromRoutedAllocator(t *testing.T) {
	routed := harness.RoutedSubnetPrefixes()
	if len(routed) == 0 {
		t.Fatal("RoutedSubnetPrefixes returned no prefixes")
	}
	for _, system := range systemSubnetPrefixes() {
		if system.Bits() != systemSubnetBits {
			t.Fatalf("systemSubnetPrefixes returned %s; want a /%d", system, systemSubnetBits)
		}
		for _, prefix := range routed {
			if system.Overlaps(prefix) {
				t.Fatalf("the system subnet %s overlaps the routed allocator's subnet %s", system, prefix)
			}
		}
	}
}

// TestRunCommandLine checks that runTestFlag anchors and escapes each
// slash-separated segment of a test name.
func TestRunCommandLine(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{"TestX/sub", `^TestX$/^sub$`},
		{"TestX", `^TestX$`},
		{"TestX/sub.plus+", `^TestX$/^sub\.plus\+$`},
	}
	for _, tc := range cases {
		if got := runTestFlag(tc.name); got != tc.want {
			t.Errorf("runTestFlag(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}

	pattern := runTestFlag("TestX/sub.plus+")
	if !matchesTestRun(pattern, "TestX/sub.plus+") {
		t.Errorf("pattern %q does not match its own test name", pattern)
	}
	for _, other := range []string{"TestX/subXplus+", "TestXOther/sub.plus+", "TestX/sub.plus"} {
		if matchesTestRun(pattern, other) {
			t.Errorf("pattern %q matches %q", pattern, other)
		}
	}
}

func matchesTestRun(pattern, name string) bool {
	patterns := strings.Split(pattern, "/")
	names := strings.Split(name, "/")
	if len(patterns) != len(names) {
		return false
	}
	for i := range patterns {
		if !regexp.MustCompile(patterns[i]).MatchString(names[i]) {
			return false
		}
	}
	return true
}
