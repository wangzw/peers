//go:build e2e

package e2e

import (
	"bytes"
	"fmt"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
)

// faultDialBound is the fallback for a probe that is expected to succeed and
// for the state waits built on it. The probe crosses one router, so the
// success path answers at once; the bound only decides how long a probe that
// never answers takes to fail.
const faultDialBound = harness.EventTimeout

// faultFailBound is the bound of a probe that is expected to fail. A REJECT
// answers at once, so one second is generous while keeping a silent DROP
// visible as a slow failure rather than a fast one.
const faultFailBound = time.Second

// TestInfraNAT checks that the NAT topology is one-directional: hostB can
// reach hostA, hostA cannot reach hostB, and hostA's default route points at
// the router.
func TestInfraNAT(t *testing.T) {
	t.Parallel()
	c := harness.Up(t, harness.TopoNAT)

	c.HoldPort(t, "hostA", ":17443")
	c.HoldPort(t, "hostB", ":17443")

	hostA := net.JoinHostPort(c.Addr("hostA", "wan"), "17443")
	hostB := net.JoinHostPort(c.Addr("hostB", "lanb"), "17443")
	if !c.TCPReachable(t, "hostB", hostA, faultDialBound) {
		t.Fatalf("hostB cannot reach hostA at %s; the NAT topology is not connected", hostA)
	}
	if c.TCPReachable(t, "hostA", hostB, faultFailBound) {
		t.Fatalf("hostA reached hostB at %s; it must not be able to reach a host behind the router", hostB)
	}

	route := c.Node("hostA").Exec(t, []string{"ip", "-j", "route", "show", "default"}, nil)
	if route.ExitCode != 0 {
		t.Fatalf("hostA: listing the default route: exit %d: %s", route.ExitCode, route.Stderr)
	}
	want := `"gateway":"` + c.Addr("router", "wan") + `"`
	if !strings.Contains(route.Stdout, want) {
		t.Fatalf("hostA's default route does not go through the router on wan: %s", route.Stdout)
	}
}

// TestInfraCutRestore checks that Cut ends established connections at once,
// that new connections fail fast with a reset rather than hanging, and that
// Restore brings the network back.
func TestInfraCutRestore(t *testing.T) {
	t.Parallel()
	c := harness.Up(t, harness.TopoNAT)

	c.HoldPort(t, "hostA", ":17443")
	addr := net.JoinHostPort(c.Addr("hostA", "wan"), "17443")
	if !c.TCPReachable(t, "hostB", addr, faultDialBound) {
		t.Fatalf("hostB cannot reach hostA at %s before the cut", addr)
	}

	c.OpenConn(t, "hostB", addr)
	waitEstablished(t, c, "hostA", 17443, 1)

	c.Cut(t, "hostA", "hostB")

	// ss -K is what makes the established connection go away before the gRPC
	// keepalive would notice, so its disappearance is the check that it ran.
	harness.Eventually(t, faultFailBound, "the established connection to be torn down", func() (bool, string) {
		got := establishedOn(t, c, "hostA", 17443)
		return got == 0, fmt.Sprintf("%d established connections remain", got)
	})

	start := time.Now()
	if c.TCPReachable(t, "hostB", addr, faultFailBound) {
		t.Fatalf("hostB still reaches hostA at %s after the cut", addr)
	}
	if elapsed := time.Since(start); elapsed >= faultFailBound {
		t.Fatalf("a connection after the cut took %s; a REJECT should fail at once", elapsed)
	}

	c.Restore(t)
	harness.Eventually(t, faultDialBound, "hostA to be reachable after Restore", func() (bool, string) {
		if c.TCPReachable(t, "hostB", addr, faultFailBound) {
			return true, ""
		}
		return false, "still unreachable"
	})
}

// TestInfraCountersAndCapture proves the netfilter counters and the packet
// capture see real traffic. The plaintext marker is the positive control: a
// capture that is silently empty would fail this test.
func TestInfraCountersAndCapture(t *testing.T) {
	t.Parallel()
	c := harness.Up(t, harness.TopoNAT)

	c.HoldPort(t, "hostA", ":17443")
	addr := net.JoinHostPort(c.Addr("hostA", "wan"), "17443")

	routerWan := c.Addr("router", "wan")
	hostAWan := c.Addr("hostA", "wan")
	syncIn := c.SYNCounter(t, "hostA", harness.Inbound, routerWan, 17443)
	syncOut := c.SYNCounter(t, "hostB", harness.Outbound, hostAWan, 17443)

	for i := 0; i < 3; i++ {
		if !c.TCPReachable(t, "hostB", addr, faultDialBound) {
			t.Fatalf("connection %d from hostB to %s failed", i+1, addr)
		}
	}
	if got := syncIn(); got != 3 {
		t.Fatalf("hostA counted %d inbound SYNs, want 3", got)
	}
	if got := syncOut(); got != 3 {
		t.Fatalf("hostB counted %d outbound SYNs, want 3", got)
	}

	byteIn := c.ByteCounter(t, "hostA", routerWan, 17443)
	before := byteIn()
	if result := c.Node("hostB").Exec(t, []string{"sh", "-c",
		fmt.Sprintf("dd if=/dev/zero bs=1000 count=10 2>/dev/null | nc -N -w 2 %s 17443", hostAWan)}, nil); result.ExitCode != 0 {
		t.Fatalf("hostB: sending 10000 bytes: exit %d: %s", result.ExitCode, result.Stderr)
	}
	harness.Eventually(t, faultDialBound, "the byte counter to see the payload", func() (bool, string) {
		got := byteIn() - before
		return got >= 10000, fmt.Sprintf("the counter grew by %d bytes", got)
	})

	stop := c.Capture(t, "hostA", 17443)
	const marker = "E2E-PLAINTEXT-MARKER"
	if result := c.Node("hostB").Exec(t, []string{"sh", "-c",
		fmt.Sprintf("printf %s | nc -N -w 2 %s 17443", marker, hostAWan)}, nil); result.ExitCode != 0 {
		t.Fatalf("hostB: sending the capture marker: exit %d: %s", result.ExitCode, result.Stderr)
	}
	pcap := stop()
	if len(pcap) == 0 {
		t.Fatal("the capture is empty")
	}
	if !bytes.Contains(pcap, []byte(marker)) {
		t.Fatalf("the capture does not contain the plaintext marker %q", marker)
	}
}

// TestInfraBlock checks that Block stops only the blocked direction and that
// its undo restores it.
func TestInfraBlock(t *testing.T) {
	t.Parallel()
	c := harness.Up(t, harness.TopoNAT)

	c.HoldPort(t, "hostA", ":17443")
	wan := net.JoinHostPort(c.Addr("hostA", "wan"), "17443")
	wan2 := net.JoinHostPort(c.Addr("hostA", "wan2"), "17443")
	if !c.TCPReachable(t, "hostB", wan, faultDialBound) {
		t.Fatalf("hostB cannot reach hostA at %s before the block", wan)
	}

	undo := c.Block(t, "hostB", c.Addr("hostA", "wan"))
	if c.TCPReachable(t, "hostB", wan, faultFailBound) {
		t.Fatalf("hostB still reaches the blocked address %s", wan)
	}
	if !c.TCPReachable(t, "hostB", wan2, faultDialBound) {
		t.Fatalf("blocking %s also blocked the second address %s", wan, wan2)
	}

	undo()
	harness.Eventually(t, faultDialBound, "the blocked address to become reachable again", func() (bool, string) {
		if c.TCPReachable(t, "hostB", wan, faultFailBound) {
			return true, ""
		}
		return false, "still blocked"
	})
}

// TestInfraPortForward checks that the forwarded port reaches hostA and that
// hostA's own address does not.
func TestInfraPortForward(t *testing.T) {
	t.Parallel()
	c := harness.Up(t, harness.TopoPortForward)

	c.HoldPort(t, "hostA", ":17443")
	forwarded := net.JoinHostPort(c.Addr("router", "wan"), "17443")
	if !c.TCPReachable(t, "hostB", forwarded, faultDialBound) {
		t.Fatalf("hostB cannot reach the forwarded port %s", forwarded)
	}

	direct := net.JoinHostPort(c.Addr("hostA", "lana"), "17443")
	if c.TCPReachable(t, "hostB", direct, faultFailBound) {
		t.Fatalf("hostB reached hostA's own address %s; only the forwarded port may work", direct)
	}
}

// TestInfraMITMRedirect checks that a connection aimed at hostA's port is
// redirected to the router, where the relay listens, and never reaches hostA.
func TestInfraMITMRedirect(t *testing.T) {
	t.Parallel()
	c := harness.Up(t, harness.TopoMITM)

	c.HoldPort(t, "router", ":17443")
	c.HoldPort(t, "hostA", ":17443")

	addr := net.JoinHostPort(c.Addr("hostA", "wan"), "17443")
	// Keep the connection open: a probe that closes at once leaves nothing to
	// count as established.
	c.OpenConn(t, "hostB", addr)

	waitEstablished(t, c, "router", 17443, 1)
	if got := establishedOn(t, c, "hostA", 17443); got != 0 {
		t.Fatalf("hostA has %d established connections on 17443; the redirect let the connection through", got)
	}
}

// TestInfraFlatV6 checks both IPv6 directions on the flat network, that no
// IPv6 default route exists, and that AddDefaultRoute6 installs one.
func TestInfraFlatV6(t *testing.T) {
	t.Parallel()
	c := harness.Up(t, harness.TopoFlatV6)

	c.HoldPort(t, "hostA", ":17443")
	c.HoldPort(t, "hostB", ":17443")

	hostA := net.JoinHostPort(c.Addr6("hostA", "net"), "17443")
	hostB := net.JoinHostPort(c.Addr6("hostB", "net"), "17443")
	if !c.TCPReachable(t, "hostB", hostA, faultDialBound) {
		t.Fatalf("hostB cannot reach hostA at %s over IPv6", hostA)
	}
	if !c.TCPReachable(t, "hostA", hostB, faultDialBound) {
		t.Fatalf("hostA cannot reach hostB at %s over IPv6", hostB)
	}

	for _, node := range []string{"hostA", "hostB"} {
		out := c.Root(t, node, "ip", "-6", "route", "show", "default")
		if strings.TrimSpace(out) != "" {
			t.Fatalf("%s has an IPv6 default route before AddDefaultRoute6: %q", node, out)
		}
	}

	c.AddDefaultRoute6(t, "hostB")
	out := c.Root(t, "hostB", "ip", "-6", "route", "show", "default")
	if strings.TrimSpace(out) == "" {
		t.Fatal("hostB has no IPv6 default route after AddDefaultRoute6")
	}
}

// waitEstablished waits until node has at least want established connections
// on port.
func waitEstablished(t testing.TB, c *harness.Cluster, node string, port, want int) {
	t.Helper()
	harness.Eventually(t, faultDialBound, fmt.Sprintf("%s to have %d established connections on %d", node, want, port), func() (bool, string) {
		got := establishedOn(t, c, node, port)
		return got >= want, fmt.Sprintf("%d established connections", got)
	})
}

// establishedOn counts the established TCP connections whose local port is
// port on node.
func establishedOn(t testing.TB, c *harness.Cluster, node string, port int) int {
	t.Helper()
	out := c.Root(t, node, "ss", "-Htan")
	suffix := ":" + strconv.Itoa(port)
	count := 0
	for _, line := range strings.Split(out, "\n") {
		fields := strings.Fields(line)
		if len(fields) < 4 {
			continue
		}
		if strings.Contains(fields[0], "ESTAB") && strings.HasSuffix(strings.TrimSpace(fields[3]), suffix) {
			count++
		}
	}
	return count
}
