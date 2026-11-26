package harness

import (
	"crypto/rand"
	"fmt"
	"strconv"
	"strings"
	"testing"
)

// TopoNAT is one host behind a masquerading router and one host on the wide
// area side. hostB can open connections to hostA; hostA cannot open a
// connection to hostB.
var TopoNAT = Topology{
	file:     "nat.yaml",
	nodes:    []string{"hostA", "router", "hostB"},
	networks: map[string]string{"hostA": "wan", "router": "wan", "hostB": "lanb"},
	envFunc: func(t *testing.T) map[string]string {
		subnets := allocRoutedSubnets(t)
		return map[string]string{
			"NAT_WAN_SUBNET":  subnets[0],
			"NAT_WAN2_SUBNET": subnets[1],
			"NAT_LANB_SUBNET": subnets[2],
		}
	},
	init: func(t *testing.T, c *Cluster) {
		c.blockHostEgress(t)

		wanDev := c.device(t, "router", "wan")
		wan2Dev := c.device(t, "router", "wan2")
		lanbDev := c.device(t, "router", "lanb")

		c.SetDefaultRoute(t, "hostA", c.Addr("router", "wan"))
		// hostB has no default route: its only exit would be the router,
		// and the route table itself must say there is none. It reaches hostA
		// through a route for each WAN subnet via the router instead.
		c.RemoveDefaultRoute(t, "hostB")
		for _, dev := range []string{wanDev, wan2Dev} {
			c.Root(t, "hostB", "ip", "route", "replace", c.subnet(t, "router", dev), "via", c.Addr("router", "lanb"))
		}
		// Docker installs a default route over one bridge gateway; the router
		// must have connected routes only, or it would offer a way out.
		c.RemoveDefaultRoute(t, "router")

		c.Root(t, "router", "iptables", "-P", "FORWARD", "DROP")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-i", lanbDev, "-o", wanDev, "-j", "ACCEPT")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-i", lanbDev, "-o", wan2Dev, "-j", "ACCEPT")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT")
		c.Root(t, "router", "iptables", "-t", "nat", "-A", "POSTROUTING",
			"-s", c.subnet(t, "router", lanbDev), "-o", wanDev, "-j", "MASQUERADE")

		registerFaults(c, faultPlan{
			router: "router",
			peer: map[string]string{
				// hostA accepts connections whose source the router
				// masquerades; hostB dialed hostA's own WAN address.
				"hostA": c.Addr("router", "wan"),
				"hostB": c.Addr("hostA", "wan"),
			},
		})
	},
}

// TopoMITM has the services and networks of TopoNAT. The router additionally
// redirects connections aimed at hostA's Broker port to its own port 17443,
// where a scenario starts the relay that impersonates hostA.
var TopoMITM = Topology{
	file:     "mitm.yaml",
	nodes:    []string{"hostA", "router", "hostB"},
	networks: map[string]string{"hostA": "wan", "router": "wan", "hostB": "lanb"},
	envFunc: func(t *testing.T) map[string]string {
		subnets := allocRoutedSubnets(t)
		return map[string]string{
			"MITM_WAN_SUBNET":  subnets[0],
			"MITM_WAN2_SUBNET": subnets[1],
			"MITM_LANB_SUBNET": subnets[2],
		}
	},
	init: func(t *testing.T, c *Cluster) {
		c.blockHostEgress(t)

		wanDev := c.device(t, "router", "wan")
		wan2Dev := c.device(t, "router", "wan2")
		lanbDev := c.device(t, "router", "lanb")

		c.SetDefaultRoute(t, "hostA", c.Addr("router", "wan"))
		// hostB has no default route: its only exit would be the router,
		// and the route table itself must say there is none. It reaches hostA
		// through a route for each WAN subnet via the router instead.
		c.RemoveDefaultRoute(t, "hostB")
		for _, dev := range []string{wanDev, wan2Dev} {
			c.Root(t, "hostB", "ip", "route", "replace", c.subnet(t, "router", dev), "via", c.Addr("router", "lanb"))
		}
		c.RemoveDefaultRoute(t, "router")

		c.Root(t, "router", "iptables", "-P", "FORWARD", "DROP")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-i", lanbDev, "-o", wanDev, "-j", "ACCEPT")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-i", lanbDev, "-o", wan2Dev, "-j", "ACCEPT")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT")
		c.Root(t, "router", "iptables", "-t", "nat", "-A", "POSTROUTING",
			"-s", c.subnet(t, "router", lanbDev), "-o", wanDev, "-j", "MASQUERADE")
		c.Root(t, "router", "iptables", "-t", "nat", "-A", "PREROUTING",
			"-i", lanbDev, "-p", "tcp", "-d", c.Addr("hostA", "wan"), "--dport", "17443",
			"-j", "REDIRECT", "--to-ports", "17443")

		registerFaults(c, faultPlan{
			router: "router",
			peer: map[string]string{
				// hostA sees the relay's upstream connection from the
				// router; hostB dialed hostA's own WAN address.
				"hostA": c.Addr("router", "wan"),
				"hostB": c.Addr("hostA", "wan"),
			},
		})
	},
}

// TopoPortForward puts hostA behind a router whose port 17443 is forwarded to
// hostA. hostB can reach hostA only through that forwarded port; hostA's own
// address is not routable from hostB.
var TopoPortForward = Topology{
	file:     "portfwd.yaml",
	nodes:    []string{"hostA", "router", "hostB"},
	networks: map[string]string{"hostA": "lana", "router": "lana", "hostB": "wan"},
	envFunc: func(t *testing.T) map[string]string {
		subnets := allocRoutedSubnets(t)
		return map[string]string{
			"PORTFWD_LANA_SUBNET": subnets[0],
			"PORTFWD_WAN_SUBNET":  subnets[1],
		}
	},
	init: func(t *testing.T, c *Cluster) {
		c.blockHostEgress(t)

		lanaDev := c.device(t, "router", "lana")
		wanDev := c.device(t, "router", "wan")

		c.SetDefaultRoute(t, "hostA", c.Addr("router", "lana"))
		// hostB reaches the router's WAN address on-link; a default route would
		// be a second, unchecked path out of the topology.
		c.RemoveDefaultRoute(t, "hostB")
		c.RemoveDefaultRoute(t, "router")

		c.Root(t, "router", "iptables", "-t", "nat", "-A", "PREROUTING",
			"-i", wanDev, "-p", "tcp", "--dport", "17443",
			"-j", "DNAT", "--to", c.Addr("hostA", "lana")+":17443")
		c.Root(t, "router", "iptables", "-t", "nat", "-A", "POSTROUTING",
			"-s", c.subnet(t, "router", lanaDev), "-o", wanDev, "-j", "MASQUERADE")

		c.Root(t, "router", "iptables", "-P", "FORWARD", "DROP")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-m", "conntrack", "--ctstate", "DNAT", "-j", "ACCEPT")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-i", lanaDev, "-o", wanDev, "-j", "ACCEPT")
		c.Root(t, "router", "iptables", "-A", "FORWARD", "-m", "conntrack", "--ctstate", "ESTABLISHED,RELATED", "-j", "ACCEPT")

		registerFaults(c, faultPlan{
			router: "router",
			peer: map[string]string{
				// hostA accepts the DNATed connection from hostB; hostB
				// dialed the router's forwarded port, whose replies are
				// masqueraded back to the router's WAN address.
				"hostA": c.Addr("hostB", "wan"),
				"hostB": c.Addr("router", "wan"),
			},
		})
	},
}

// TopoFlat returns a topology of n hosts on one flat network, named hostA to
// hostD. n must be between 2 and 4.
func TopoFlat(n int) Topology {
	if n < 2 || n > 4 {
		panic(fmt.Sprintf("harness.TopoFlat: n must be between 2 and 4, got %d", n))
	}
	all := []string{"hostA", "hostB", "hostC", "hostD"}
	nodes := append([]string(nil), all[:n]...)
	networks := make(map[string]string, n)
	for _, node := range nodes {
		networks[node] = "net"
	}
	return Topology{
		file:     "flat.yaml",
		nodes:    nodes,
		networks: networks,
		env:      map[string]string{"COMPOSE_PROFILES": strconv.Itoa(n)},
		init: func(t *testing.T, c *Cluster) {
			for _, node := range nodes {
				c.SetDefaultRoute(t, node, "")
			}
			registerFaults(c, faultPlan{network: "net"})
		},
	}
}

// TopoFlatV6 has hostA and hostB on one dual-stack network and hostB on a
// second dual-stack network. Neither node has an IPv6 default route.
var TopoFlatV6 = Topology{
	file:     "flatv6.yaml",
	nodes:    []string{"hostA", "hostB"},
	networks: map[string]string{"hostA": "net", "hostB": "net"},
	envFunc: func(t *testing.T) map[string]string {
		return map[string]string{
			"V6_NET":  randomULA(1),
			"V6_VIRT": randomULA(2),
		}
	},
	init: func(t *testing.T, c *Cluster) {
		for _, node := range []string{"hostA", "hostB"} {
			c.setDefaultRouteOn(t, node, c.device(t, node, "net"))
			// Docker may install an IPv6 default route of its own; the
			// topology is meant to have none until AddDefaultRoute6 runs.
			_, _ = c.RootOK(t, node, "ip", "-6", "route", "del", "default")
		}
	},
}

func randomULA(suffix byte) string {
	var b [5]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("harness: generating an IPv6 prefix: %v", err))
	}
	return fmt.Sprintf("fd%02x:%02x%02x:%02x%02x:%x::/64", b[0], b[1], b[2], b[3], b[4], suffix)
}

// blockHostEgress empties every node's /etc/resolv.conf, because this Docker
// writes the host's resolver into the container instead of the embedded
// 127.0.0.11.
func (c *Cluster) blockHostEgress(t testing.TB) {
	t.Helper()
	for _, node := range c.order {
		for _, network := range c.Networks(node) {
			c.Root(t, node, "iptables", "-A", "OUTPUT", "-d", c.Gateway(node, network), "-j", "DROP")
		}
		c.Root(t, node, "sh", "-c", ": > /etc/resolv.conf")
	}
}

// RemoveDefaultRoute deletes node's IPv4 default route and asserts that none
// remains; this is not best effort, because a kept route and a router that drops
// packets look the same to a probe that only times out.
func (c *Cluster) RemoveDefaultRoute(t testing.TB, node string) {
	t.Helper()
	c.Root(t, node, "ip", "route", "del", "default")
	c.AssertNoDefaultRoute(t, node)
}

// AssertNoDefaultRoute fails when node has an IPv4 default route.
func (c *Cluster) AssertNoDefaultRoute(t testing.TB, node string) {
	t.Helper()
	if out := strings.TrimSpace(c.Root(t, node, "ip", "route", "show", "default")); out != "" {
		t.Fatalf("harness: node %s has a default route: %q", node, out)
	}
}

func (c *Cluster) device(t testing.TB, node, network string) string {
	t.Helper()
	addr := c.Addr(node, network)
	out := c.Root(t, node, "ip", "-o", "-4", "addr", "show", "to", addr)
	fields := strings.Fields(out)
	if len(fields) < 2 {
		t.Fatalf("harness: node %s: no interface carries %s: %q", node, addr, out)
	}
	return strings.TrimSuffix(fields[1], ":")
}

func (c *Cluster) subnet(t testing.TB, node, dev string) string {
	t.Helper()
	out := c.Root(t, node, "ip", "-o", "-4", "route", "show", "dev", dev, "scope", "link")
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) > 0 && strings.Contains(fields[0], "/") {
			return fields[0]
		}
	}
	t.Fatalf("harness: node %s: no subnet on %s: %q", node, dev, out)
	return ""
}
