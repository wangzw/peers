package harness

import (
	"net/netip"
	"os"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// routedRange is the only address range the routed topologies may use: it
// avoids both Docker's default pools and this host's k8s networks.
var routedRange = netip.MustParsePrefix("198.18.0.0/15")

// TestRoutedSubnetBlocks checks that consecutive allocations are disjoint /24s inside 198.18.0.0/15, with two blocks taken before either cleanup runs.
func TestRoutedSubnetBlocks(t *testing.T) {
	seen := map[string]bool{}
	for _, block := range [][3]string{allocRoutedSubnets(t), allocRoutedSubnets(t)} {
		for _, cidr := range block {
			prefix, err := netip.ParsePrefix(cidr)
			if err != nil {
				t.Fatalf("allocRoutedSubnets returned %q: %v", cidr, err)
			}
			if prefix.Bits() != 24 {
				t.Fatalf("allocRoutedSubnets returned %q; want a /24", cidr)
			}
			if !routedRange.Contains(prefix.Addr()) {
				t.Fatalf("allocRoutedSubnets returned %q outside %s", cidr, routedRange)
			}
			if seen[cidr] {
				t.Fatalf("allocRoutedSubnets returned %q twice", cidr)
			}
			seen[cidr] = true
		}
	}
}

// TestRoutedComposeSubnets checks the defaults in the three routed compose files: every subnet lies inside 198.18.0.0/15 and no two defaults share a subnet.
func TestRoutedComposeSubnets(t *testing.T) {
	var all []string
	for _, file := range []string{"nat.yaml", "portfwd.yaml", "mitm.yaml"} {
		path, err := composeSourcePath(file)
		if err != nil {
			t.Fatalf("locating %s: %v", file, err)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", file, err)
		}
		var doc struct {
			Networks map[string]struct {
				IPAM struct {
					Config []struct {
						Subnet string `yaml:"subnet"`
					} `yaml:"config"`
				} `yaml:"ipam"`
			} `yaml:"networks"`
		}
		if err := yaml.Unmarshal(data, &doc); err != nil {
			t.Fatalf("parsing %s: %v", file, err)
		}
		if len(doc.Networks) == 0 {
			t.Fatalf("%s defines no networks", file)
		}
		for name, network := range doc.Networks {
			if len(network.IPAM.Config) != 1 {
				t.Fatalf("%s: network %s has %d ipam configs; want 1", file, name, len(network.IPAM.Config))
			}
			cidr := composeSubnetDefault(t, file, name, network.IPAM.Config[0].Subnet)
			prefix, err := netip.ParsePrefix(cidr)
			if err != nil {
				t.Fatalf("%s: network %s: %v", file, name, err)
			}
			if prefix.Bits() != 24 {
				t.Fatalf("%s: network %s uses %q; want a /24", file, name, cidr)
			}
			if !routedRange.Contains(prefix.Addr()) {
				t.Fatalf("%s: network %s uses %q outside %s", file, name, cidr, routedRange)
			}
			all = append(all, cidr)
		}
	}
	seen := map[string]bool{}
	for _, cidr := range all {
		if seen[cidr] {
			t.Fatalf("two routed networks use the same subnet %q", cidr)
		}
		seen[cidr] = true
	}
}

// composeSubnetDefault returns the fallback of a compose subnet expression such
// as ${NAT_WAN_SUBNET:-198.18.0.0/24}.
func composeSubnetDefault(t *testing.T, file, network, value string) string {
	t.Helper()
	if !strings.HasPrefix(value, "${") || !strings.HasSuffix(value, "}") {
		return value
	}
	expression := strings.TrimSuffix(strings.TrimPrefix(value, "${"), "}")
	index := strings.Index(expression, ":-")
	if index < 0 {
		t.Fatalf("%s: network %s uses %q without a default", file, network, value)
	}
	return expression[index+2:]
}
