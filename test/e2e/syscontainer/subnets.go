// This file carries no e2e build tag so that the allocator's range and its
// host-side test compile without one.

package syscontainer

import (
	"net/netip"
	"testing"

	"github.com/wangzw/peers/test/e2e/harness"
)

const (
	// systemSubnetFirst and systemSubnetLast bound the /24s the container
	// network may use, as indices into 198.18.0.0/15; the harness routed
	// allocator stops at 479 and reserves this block, so the two allocators
	// never hand out the same subnet.
	systemSubnetFirst = 480
	systemSubnetLast  = 511
	// systemSubnetBits is the size of one pool slot; each reserved /24 is
	// divided into sixteen /28s, which gives 512 slots for two suites of about
	// thirty containers each. A /28 leaves thirteen container addresses, enough
	// for the one or two services a project starts.
	systemSubnetBits = 28
)

// systemSubnetPrefixes returns every prefix allocSystemSubnet can hand out.
func systemSubnetPrefixes() []netip.Prefix {
	step := 1 << (32 - systemSubnetBits)
	out := make([]netip.Prefix, 0, (systemSubnetLast-systemSubnetFirst+1)*step)
	for index := systemSubnetFirst; index <= systemSubnetLast; index++ {
		for offset := 0; offset < 256; offset += step {
			out = append(out, systemSubnetPrefix(index, offset))
		}
	}
	return out
}

// systemSubnetPrefix formats the index-th /24 of 198.18.0.0/15 with offset
// added inside it. The index is the same one the harness routed allocator
// counts, which keeps the two ranges provably disjoint.
func systemSubnetPrefix(index, offset int) netip.Prefix {
	return netip.PrefixFrom(
		netip.AddrFrom4([4]byte{198, byte(18 + index/256), byte(index % 256), byte(offset)}),
		systemSubnetBits,
	)
}

// allocSystemSubnet reserves one subnet for one compose project through the
// harness allocator, which locks the shared state directory and records the
// owner, and returns it as a CIDR. The range is disjoint from the routed
// allocator's, so parallel projects and suites never overlap.
func allocSystemSubnet(t *testing.T) string {
	t.Helper()

	prefix, err := harness.ReserveSubnetPrefix(t, systemSubnetPrefixes())
	if err != nil {
		t.Fatalf("syscontainer: reserving a system network subnet: %v", err)
	}
	return prefix.String()
}
