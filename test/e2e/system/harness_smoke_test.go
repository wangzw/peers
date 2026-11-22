//go:build e2e

package e2e

import (
	"errors"
	"net"
	"net/netip"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// externalProbeIP is in the TEST-NET-2 documentation range. It can never be
	// a real service, so a successful connection would mean the container
	// found a way out.
	externalProbeIP      = "198.51.100.1"
	externalProbeAddress = externalProbeIP + ":443"
	// systemSubnetEnv carries the internal network's subnet into the container.
	// syscontainer.Run sets it from the subnet it reserved for the project.
	systemSubnetEnv = "PEERS_E2E_SYSTEM_SUBNET"
	// egressProbeBound bounds one probe of the internal network's gateway.
	egressProbeBound = 2 * time.Second
)

// gatewayProbePorts are services this host runs on every address. A container
// on the internal network must not reach them through the docker bridge
// gateway, which is the host end of that network.
var gatewayProbePorts = []string{"22", "6443", "10250"}

// TestHarnessAccountsAndBinary checks that the container provides the release
// binary and the two accounts with the supplementary groups the permission
// tests rely on.
func TestHarnessAccountsAndBinary(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	version := syscontainer.RunAs(t, "peer", nil, "--version")
	if version.ExitCode != 0 {
		t.Fatalf("peers --version as peer: exit %d\n%s", version.ExitCode, version.Stderr)
	}
	if !strings.Contains(version.Stdout, "peers e2e") {
		t.Errorf("peers --version output = %q, want it to contain %q", version.Stdout, "peers e2e")
	}

	peerUID := syscontainer.AccountUID(t, "peer")
	if got := runID(t, "peer", "-u"); got != strconv.FormatUint(uint64(peerUID), 10) {
		t.Errorf("id -u as peer = %q, want %d", got, peerUID)
	}
	if groups := runID(t, "peer", "-G"); !hasGroup(groups, "2001") {
		t.Errorf("id -G as peer = %q, want it to contain 2001", groups)
	}

	if got := syscontainer.AccountUID(t, "other"); got != 2000 {
		t.Errorf("AccountUID(other) = %d, want 2000", got)
	}
	if got := runID(t, "other", "-u"); got != "2000" {
		t.Errorf("id -u as other = %q, want 2000", got)
	}
	if groups := runID(t, "other", "-G"); hasGroup(groups, "2001") {
		t.Errorf("id -G as other = %q, want it not to contain 2001", groups)
	}
}

// TestHarnessNoEgress checks that the network declared in
// compose/system/compose.yaml gives the container no way out: there is no
// default route, or an external address has no route, and neither an external
// address nor the internal network's host-side IPAM gateway is reachable.
func TestHarnessNoEgress(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	routes := runCommandAs(t, "root", "ip", "-4", "route", "show")
	if routes.ExitCode != 0 {
		t.Fatalf("ip -4 route show: exit %d\nstderr: %s", routes.ExitCode, strings.TrimSpace(routes.Stderr))
	}

	hasDefault, _ := parseDefaultRoute(routes.Stdout)
	lookup := runCommandAs(t, "root", "ip", "route", "get", externalProbeIP)
	unreachable := lookup.ExitCode != 0 &&
		strings.Contains(strings.ToLower(lookup.Stdout+lookup.Stderr), "unreachable")
	if hasDefault && !unreachable {
		t.Errorf("the container has a default route and %s is still routable:\nroutes:\n%s\nroute get: exit %d\n%s",
			externalProbeIP, strings.TrimSpace(routes.Stdout), lookup.ExitCode,
			strings.TrimSpace(lookup.Stdout+lookup.Stderr))
	}
	if !hasDefault && !unreachable {
		t.Logf("no default route; route get %s: exit %d, %s", externalProbeIP, lookup.ExitCode,
			strings.TrimSpace(lookup.Stdout+lookup.Stderr))
	}
	assertDialFails(t, externalProbeAddress)

	// The host end of the internal network is its IPAM gateway. Probing it is
	// discriminating: the container has a connected route to it, so a
	// successful connection or a refusal both mean the packet reached the host.
	gateway := internalGateway(t)
	reached := false
	for _, port := range gatewayProbePorts {
		addr := net.JoinHostPort(gateway, port)
		if outcome, ok := dialGateway(addr); ok {
			reached = true
			t.Logf("the container reached the internal network's host-side gateway at %s: %s", addr, outcome)
		}
	}
	if reached {
		// The gateway is the host's bridge address. Block it inside this
		// container only; the host's own iptables and routes are never touched.
		cmd := syscontainer.Command("root", nil, "iptables", "-A", "OUTPUT", "-d", gateway, "-j", "DROP")
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("blocking the internal network gateway %s in the container: %v\n%s",
				gateway, err, strings.TrimSpace(string(out)))
		}
	}
	for _, port := range gatewayProbePorts {
		addr := net.JoinHostPort(gateway, port)
		if outcome, ok := dialGateway(addr); ok {
			t.Errorf("dialing the internal network's host-side gateway at %s %s; the container must not reach the host's services",
				addr, outcome)
		}
	}
}

// internalGateway returns the first host address of the internal network's
// subnet, which is the address docker assigns to the bridge on the host side.
func internalGateway(t *testing.T) string {
	t.Helper()
	cidr := os.Getenv(systemSubnetEnv)
	if cidr == "" {
		t.Fatalf("the internal network's subnet is not in %s", systemSubnetEnv)
	}
	prefix, err := netip.ParsePrefix(cidr)
	if err != nil {
		t.Fatalf("parsing %s = %q: %v", systemSubnetEnv, cidr, err)
	}
	gateway := prefix.Masked().Addr().Next()
	if !gateway.IsValid() {
		t.Fatalf("%s = %q has no gateway address", systemSubnetEnv, cidr)
	}
	return gateway.String()
}

// dialGateway reports whether a TCP connection to addr succeeded or was
// refused. Both outcomes mean the packet reached the host; a timeout or an
// unreachable route means it did not.
func dialGateway(addr string) (string, bool) {
	conn, err := net.DialTimeout("tcp", addr, egressProbeBound)
	if err == nil {
		_ = conn.Close()
		return "accepted the connection", true
	}
	if errors.Is(err, syscall.ECONNREFUSED) {
		return "refused the connection, so the packet reached the host", true
	}
	return "", false
}

// parseDefaultRoute reports whether routes has a default route and returns the
// gateway it names.
func parseDefaultRoute(routes string) (bool, string) {
	for _, line := range strings.Split(routes, "\n") {
		fields := strings.Fields(line)
		if len(fields) == 0 || fields[0] != "default" {
			continue
		}
		for i, field := range fields {
			if field == "via" && i+1 < len(fields) {
				return true, fields[i+1]
			}
		}
		return true, ""
	}
	return false, ""
}

// assertDialFails checks that a TCP connection to addr cannot be opened.
func assertDialFails(t *testing.T, addr string) {
	t.Helper()

	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err == nil {
		_ = conn.Close()
		t.Errorf("dialing %s succeeded, want the container to have no egress", addr)
	}
}

// TestHarnessSystemBrokerAndSessions starts the system Broker and reaches it
// from two accounts' sessions. It checks the process and socket the design
// places under /run/peers.
func TestHarnessSystemBrokerAndSessions(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	broker := syscontainer.StartSystemBroker(t, "")
	if _, err := os.Stat("/run/peers/broker.sock"); err != nil {
		t.Fatalf("the system Broker socket is not at /run/peers/broker.sock: %v", err)
	}
	found := false
	for _, proc := range syscontainer.Pids("peers", "service", "run", "--system") {
		if proc.PID == broker.PID {
			found = true
		}
	}
	if !found {
		t.Errorf("process %d does not appear in Pids(peers, broker, --system)", broker.PID)
	}

	a := syscontainer.NewSession(t, "peer", "a1")
	b := syscontainer.NewSession(t, "other", "b1")
	for _, session := range []*syscontainer.Session{a, b} {
		waitRegistered(t, session)
		if text, isError := session.Call(t, "join_topic", map[string]any{"name": "e2e/smoke"}); isError {
			t.Fatalf("join_topic as %s reported an error: %s", session.Account, text)
		}
	}
	if text, isError := a.Call(t, "list_agents", map[string]any{"topic": "e2e/smoke"}); isError {
		t.Fatalf("list_agents as peer reported an error: %s", text)
	}
}

// TestHarnessSystemService installs the system service through systemd and
// starts it, so the tests downstream that exercise a managed system Broker can
// rely on StartSystemService. It checks the unit is active and that the Broker
// created its socket under /run/peers.
func TestHarnessSystemService(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	syscontainer.StartSystemService(t, "")

	if _, err := os.Stat("/run/peers/broker.sock"); err != nil {
		t.Fatalf("the system service socket is not at /run/peers/broker.sock: %v", err)
	}
	active := runCommandAs(t, "root", "systemctl", "is-active", "peers.service")
	if state := strings.TrimSpace(active.Stdout); state != "active" {
		t.Errorf("systemctl is-active peers.service = %q (exit %d), want %q\nstderr: %s",
			state, active.ExitCode, "active", strings.TrimSpace(active.Stderr))
	}
}

// waitRegistered waits until the MCP server has registered the session with the
// Broker. Registration happens in the background after the MCP handshake.
func waitRegistered(t *testing.T, session *syscontainer.Session) {
	t.Helper()
	harness.EventuallyPoll(t, harness.ListAgentsWaitBound, "session "+session.Account+" to register", func(deadline time.Time) (bool, string) {
		_, isError, err := session.PollCall(t, deadline, "list_agents", map[string]any{})
		if err != nil {
			return false, err.Error()
		}
		return !isError, ""
	})
}

// runID runs id as account and returns its trimmed standard output.
func runID(t *testing.T, account string, args ...string) string {
	t.Helper()
	out, err := syscontainer.Command(account, nil, "id", args...).Output()
	if err != nil {
		t.Fatalf("id %s as %s: %v", strings.Join(args, " "), account, err)
	}
	return strings.TrimSpace(string(out))
}

// hasGroup reports whether the space-separated group list contains gid.
func hasGroup(groups, gid string) bool {
	for _, field := range strings.Fields(groups) {
		if field == gid {
			return true
		}
	}
	return false
}

// TestHarnessInnerFailurePropagates checks that a failure inside the container
// makes the host test fail rather than pass silently.
func TestHarnessInnerFailurePropagates(t *testing.T) {
	if syscontainer.InContainer() {
		t.Skip("the propagation check runs on the host")
	}

	cmd := exec.Command("go", "test", "-tags", "e2e", "-count=1", "-run", "^TestHarnessAlwaysFails$", ".")
	cmd.Env = append(os.Environ(), "PEERS_E2E_EXPECT_FAIL=1")
	out, err := cmd.CombinedOutput()
	if err == nil {
		t.Fatalf("the inner container failure was not propagated; output:\n%s", out)
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running the inner test: %v", err)
	}
}

// TestHarnessAlwaysFails fails inside the container when the propagation smoke
// test asks for it and skips otherwise.
func TestHarnessAlwaysFails(t *testing.T) {
	if os.Getenv("PEERS_E2E_EXPECT_FAIL") != "1" {
		t.Skip("only run by TestHarnessInnerFailurePropagates")
	}
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}
	t.Fatal("intentional failure inside the plain container")
}
