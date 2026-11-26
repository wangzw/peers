//go:build e2e

package e2e

import (
	"net"
	"net/netip"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
)

// egressProbeBound bounds one probe that must find no way out: a refused or
// unreachable destination fails at once, and the bound only keeps a silent
// drop from hanging the test.
const egressProbeBound = 2 * time.Second

// egressTargets are the addresses no container may reach: a public resolver
// and the test host's own address.
var egressTargets = []struct {
	what string
	addr string
}{
	{"a public resolver", "1.1.1.1:443"},
	{"the host", "10.12.72.1:443"},
}

// egressRouteTargets are the public resolver and the test host's own address as
// plain IPs, for the route lookup.
var egressRouteTargets = []string{"1.1.1.1", "10.12.72.11"}

// egressHostServicePorts are the services the test host listens on; the bridge
// gateway of a routed network is a host address.
var egressHostServicePorts = []string{"22", "6443", "10250"}

type routedTopology struct {
	name string
	topo harness.Topology
}

var routedNodes = []string{"hostA", "router", "hostB"}

var routedTopologies = []routedTopology{
	{name: "NAT", topo: harness.TopoNAT},
	{name: "PortForward", topo: harness.TopoPortForward},
	{name: "MITM", topo: harness.TopoMITM},
}

// egressForbiddenPrefixes are the pod and service networks of the k8s node the
// tests run on; a new host route that overlaps either one would take cluster
// traffic away from its own bridges.
var egressForbiddenPrefixes = []netip.Prefix{
	netip.MustParsePrefix("10.244.0.0/16"),
	netip.MustParsePrefix("10.96.0.0/12"),
}

// TestInfraNoEgress checks that the routed topologies have no way out: no
// route, no reachable host service, no DNS, and no k8s route or leftover
// network.
func TestInfraNoEgress(t *testing.T) {
	t.Parallel()

	for _, topo := range routedTopologies {
		topo := topo
		t.Run(topo.name, func(t *testing.T) {
			t.Parallel()

			before := egressHostRoutes(t)
			c := harness.Up(t, topo.topo)
			prefix := egressProjectPrefix(t, c)

			for _, node := range []string{"router", "hostB"} {
				egressAssertNoRoute(t, c, node)
			}

			for _, target := range egressTargets {
				if c.TCPReachable(t, "hostB", target.addr, egressProbeBound) {
					t.Errorf("%s: hostB reached %s at %s; the routed topology must not offer a way out", topo.name, target.what, target.addr)
				}
			}

			for _, node := range routedNodes {
				egressAssertGatewaysUnreachable(t, c, node)
				egressAssertNoDNS(t, c, node)
			}

			egressAssertNoForbiddenRoutes(t, before, egressHostRoutes(t), "while the project is up")
			c.Down()
			egressAssertNoForbiddenRoutes(t, before, egressHostRoutes(t), "after Down")
			egressAssertNoProjectNetworks(t, prefix, "after Down")
		})
	}
}

// The route lookup must print "unreachable" or fail; a route that merely times
// out is not proof, because a router that forwards nothing also times out.
func egressAssertNoRoute(t *testing.T, c *harness.Cluster, node string) {
	t.Helper()
	for _, dest := range egressRouteTargets {
		out, err := c.RootOK(t, node, "ip", "route", "get", dest)
		if err != nil {
			continue
		}
		if !strings.Contains(out, "unreachable") {
			t.Errorf("%s: ip route get %s found a route: %s", node, dest, strings.TrimSpace(out))
		}
	}
}

func egressAssertGatewaysUnreachable(t *testing.T, c *harness.Cluster, node string) {
	t.Helper()
	for _, network := range c.Networks(node) {
		gateway := c.Gateway(node, network)
		for _, port := range egressHostServicePorts {
			addr := net.JoinHostPort(gateway, port)
			if c.TCPReachable(t, node, addr, egressProbeBound) {
				t.Errorf("node %s reached the host service at %s on the bridge gateway of network %s", node, addr, network)
			}
		}
	}
}

func egressAssertNoDNS(t *testing.T, c *harness.Cluster, node string) {
	t.Helper()
	lookup := []string{"timeout", "3", "getent", "hosts", "example.com"}
	res := c.Node(node).Exec(t, lookup, nil)
	if res.ExitCode == 0 {
		t.Errorf("%s resolved example.com although the routed topology must have no DNS: %s", node, strings.TrimSpace(res.Stdout))
	}
}

// TestInfraNoNetworkResidue checks on its own that a routed project leaves no
// docker network behind after Down.
func TestInfraNoNetworkResidue(t *testing.T) {
	t.Parallel()

	c := harness.Up(t, harness.TopoNAT)
	prefix := egressProjectPrefix(t, c)

	c.Down()
	egressAssertNoProjectNetworks(t, prefix, "after Down")
}

func egressProjectPrefix(t *testing.T, c *harness.Cluster) string {
	t.Helper()
	prefix := c.Project() + "_"
	for _, name := range egressDockerNetworks(t) {
		if strings.HasPrefix(name, prefix) {
			return prefix
		}
	}
	t.Fatalf("no docker network starts with the project prefix %q", prefix)
	return ""
}

func egressDockerNetworks(t *testing.T) []string {
	t.Helper()
	out, err := exec.Command("docker", "network", "ls", "--format", "{{.Name}}").CombinedOutput()
	if err != nil {
		t.Fatalf("listing docker networks: %v: %s", err, strings.TrimSpace(string(out)))
	}
	var names []string
	for _, line := range strings.Split(string(out), "\n") {
		if line = strings.TrimSpace(line); line != "" {
			names = append(names, line)
		}
	}
	return names
}

func egressHostRoutes(t *testing.T) map[netip.Prefix]string {
	t.Helper()
	dests := map[netip.Prefix]string{}
	for _, args := range [][]string{{"route", "show"}, {"-6", "route", "show"}} {
		out, err := exec.Command("ip", args...).CombinedOutput()
		if err != nil {
			if args[0] == "-6" {
				t.Logf("reading the host's IPv6 routes: %v", err)
				continue
			}
			t.Fatalf("reading the host's routes: %v: %s", err, strings.TrimSpace(string(out)))
		}
		for _, line := range strings.Split(string(out), "\n") {
			line = strings.TrimSpace(line)
			if line == "" {
				continue
			}
			fields := strings.Fields(line)
			dst := fields[0]
			switch dst {
			case "blackhole", "unreachable", "prohibit", "throw":
				if len(fields) < 2 {
					continue
				}
				dst = fields[1]
			}
			if dst == "default" {
				continue
			}
			prefix, err := netip.ParsePrefix(dst)
			if err != nil {
				continue
			}
			dests[prefix] = line
		}
	}
	return dests
}

func egressAssertNoForbiddenRoutes(t *testing.T, before, after map[netip.Prefix]string, when string) {
	t.Helper()
	for prefix, line := range after {
		if _, ok := before[prefix]; ok {
			continue
		}
		for _, forbidden := range egressForbiddenPrefixes {
			if prefix.Overlaps(forbidden) {
				t.Errorf("%s: the project added the host route %q, which overlaps the k8s network %s", when, line, forbidden)
			}
		}
	}
}

func egressAssertNoProjectNetworks(t *testing.T, prefix, when string) {
	t.Helper()
	for _, name := range egressDockerNetworks(t) {
		if strings.HasPrefix(name, prefix) {
			t.Errorf("%s: the docker network %s remains; Down must remove every network of the project", when, name)
		}
	}
}
