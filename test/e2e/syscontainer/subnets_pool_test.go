package syscontainer

import "testing"

// twoSuiteSubnets is the number of system subnets two suites on one host at the
// same time need; each has about thirty top-level tests that start a container,
// and the allocator hands out one subnet per compose project.
const twoSuiteSubnets = 64

// TestSystemSubnetPoolServesTwoSuites checks that the system pool hands out
// enough distinct subnets for two suites running at the same time.
func TestSystemSubnetPoolServesTwoSuites(t *testing.T) {
	seen := make(map[string]bool, twoSuiteSubnets)
	for i := 0; i < twoSuiteSubnets; i++ {
		subnet := allocSystemSubnet(t)
		if seen[subnet] {
			t.Fatalf("allocation %d returned %s twice", i+1, subnet)
		}
		seen[subnet] = true
	}
}
