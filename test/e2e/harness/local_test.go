package harness

import (
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/node"
)

// envMap turns NAME=value strings into a map.
func envMap(env []string) map[string]string {
	values := make(map[string]string, len(env))
	for _, variable := range env {
		name, value, ok := strings.Cut(variable, "=")
		if ok {
			values[name] = value
		}
	}
	return values
}

func TestLocalHostEnvIsolates(t *testing.T) {
	first := NewLocalHost(t, LocalOptions{Name: "first"})
	second := NewLocalHost(t, LocalOptions{Name: "second"})

	firstEnv := envMap(first.impl.(*localNode).env.Build(nil))
	secondEnv := envMap(second.impl.(*localNode).env.Build(nil))

	for name, env := range map[string]map[string]string{
		"first":  firstEnv,
		"second": secondEnv,
	} {
		root, ok := env["PEERS_TEST_RUNTIME_ROOT"]
		if !ok {
			t.Fatalf("%s node has no PEERS_TEST_RUNTIME_ROOT in %v", name, env)
		}
		acct, ok := env["PEERS_TEST_HOME"]
		if !ok {
			t.Fatalf("%s node has no PEERS_TEST_HOME in %v", name, env)
		}
		if !filepath.IsAbs(root) {
			t.Errorf("%s node runtime root %q is not absolute", name, root)
		}
		if !filepath.IsAbs(acct) {
			t.Errorf("%s node account home %q is not absolute", name, acct)
		}
		if root == acct {
			t.Errorf("%s node uses %q as both runtime root and account home", name, root)
		}
		if !filepath.IsAbs(env["HOME"]) {
			t.Errorf("%s node HOME %q is not absolute", name, env["HOME"])
		}
	}

	for _, name := range []string{"PEERS_TEST_RUNTIME_ROOT", "PEERS_TEST_HOME", "HOME"} {
		if firstEnv[name] == secondEnv[name] {
			t.Errorf("two nodes share %s=%q", name, firstEnv[name])
		}
	}
}

// pidsWithRuntimeRoot returns the process IDs whose environment carries the
// given runtime root. It uses the node library's scan; no private /proc reader
// lives in the harness.
func pidsWithRuntimeRoot(t *testing.T, root string) []int {
	t.Helper()
	procs, err := node.ListProcs(func(p node.Proc) bool {
		return p.Env["PEERS_TEST_RUNTIME_ROOT"] == root
	})
	if err != nil {
		t.Fatalf("listing processes: %v", err)
	}
	pids := make([]int, 0, len(procs))
	for _, proc := range procs {
		pids = append(pids, proc.PID)
	}
	return pids
}

// childBrokerPids returns the pids of this process's direct children whose
// command line runs the test Broker. It reads them from ps, which reports the
// same columns on Linux and macOS, so the check needs no /proc.
func childBrokerPids(t *testing.T) []int {
	t.Helper()
	out, err := exec.Command("ps", "-ww", "-A", "-o", "pid=,ppid=,command=").Output()
	if err != nil {
		t.Fatalf("listing processes with ps: %v", err)
	}
	self := os.Getpid()
	var pids []int
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 3 {
			continue
		}
		pid, pidErr := strconv.Atoi(fields[0])
		ppid, ppidErr := strconv.Atoi(fields[1])
		if pidErr != nil || ppidErr != nil || ppid != self {
			continue
		}
		if strings.Contains(strings.Join(fields[2:], " "), "service run --user") {
			pids = append(pids, pid)
		}
	}
	return pids
}

func TestLocalHostCleanupKillsDetached(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("looking up processes by environment scans /proc")
	}

	host := NewLocalHost(t, LocalOptions{Name: "cleanup"})
	local := host.impl.(*localNode)

	// The shell exits at once; setsid detaches the sleep, which is reparented
	// and outlives the process that started it, like a Broker started by an
	// MCP server process.
	cmd := exec.Command("sh", "-c", "setsid sleep 60 >/dev/null 2>&1 & echo $!")
	cmd.Env = local.env.Build(nil)
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("starting a detached process: %v", err)
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("reading the detached process ID from %q: %v", out, err)
	}

	Eventually(t, EventTimeout, "the detached process starts", func() (bool, string) {
		pids := pidsWithRuntimeRoot(t, local.root)
		if containsPID(pids, pid) {
			return true, fmt.Sprintf("pid %d is running", pid)
		}
		return false, fmt.Sprintf("pid %d not found among %v", pid, pids)
	})

	local.cleanup()

	Eventually(t, EventTimeout, "cleanup kills the detached process", func() (bool, string) {
		pids := pidsWithRuntimeRoot(t, local.root)
		if containsPID(pids, pid) {
			return false, fmt.Sprintf("pid %d still running", pid)
		}
		if len(pids) != 0 {
			return false, fmt.Sprintf("processes %v still carry the runtime root", pids)
		}
		return true, ""
	})
}

// TestLocalEnvCarriesCoverageDir checks that the children of a host-local node
// inherit GOCOVERDIR only when coverage is on, and that the node's own
// variables are always present.
func TestLocalEnvCarriesCoverageDir(t *testing.T) {
	t.Setenv(CoverageDirEnv, "")
	off := localEnvExtra("/runtime", "/account")
	if _, ok := off["GOCOVERDIR"]; ok {
		t.Fatalf("GOCOVERDIR without coverage: %v", off)
	}
	if off["PEERS_TEST_RUNTIME_ROOT"] != "/runtime" || off["PEERS_TEST_HOME"] != "/account" {
		t.Fatalf("node variables = %v", off)
	}

	t.Setenv(CoverageDirEnv, "/tmp/cover")
	on := localEnvExtra("/runtime", "/account")
	if got := on["GOCOVERDIR"]; got != "/tmp/cover" {
		t.Fatalf("GOCOVERDIR under coverage = %q, want /tmp/cover", got)
	}
	if on["PEERS_TEST_RUNTIME_ROOT"] != "/runtime" || on["PEERS_TEST_HOME"] != "/account" {
		t.Fatalf("node variables under coverage = %v", on)
	}
}

// sentSignal is one signal killPids sent to a process.
type sentSignal struct {
	pid int
	sig syscall.Signal
}

// TestKillPids fixes the process cleanup: without coverage every process gets
// SIGKILL at once; under coverage every process gets SIGTERM first, one that
// exits on it is never killed, and one that survives is killed after the wait.
func TestKillPids(t *testing.T) {
	cases := []struct {
		name      string
		coverDir  string
		termExits map[int]bool
		want      []sentSignal
	}{
		{
			name: "coverage off kills at once",
			want: []sentSignal{{11, syscall.SIGKILL}, {12, syscall.SIGKILL}},
		},
		{
			name:      "coverage on waits for a process that exits on SIGTERM",
			coverDir:  "/tmp/cover",
			termExits: map[int]bool{11: true, 12: true},
			want:      []sentSignal{{11, syscall.SIGTERM}, {12, syscall.SIGTERM}},
		},
		{
			name:      "coverage on kills a process that ignores SIGTERM",
			coverDir:  "/tmp/cover",
			termExits: map[int]bool{11: true},
			want:      []sentSignal{{11, syscall.SIGTERM}, {12, syscall.SIGTERM}, {12, syscall.SIGKILL}},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(CoverageDirEnv, tc.coverDir)
			alive := map[int]bool{11: true, 12: true}
			var sent []sentSignal
			send := func(pid int, sig syscall.Signal) error {
				sent = append(sent, sentSignal{pid, sig})
				if sig == syscall.SIGKILL || tc.termExits[pid] {
					alive[pid] = false
				}
				return nil
			}
			aliveFunc := func(pid int) bool { return alive[pid] }
			startTime := func(int) (uint64, error) { return 1, nil }
			killPids([]int{11, 12}, send, aliveFunc, startTime, 20*time.Millisecond, time.Millisecond)
			if !slices.Equal(sent, tc.want) {
				t.Fatalf("killPids sent %v, want %v", sent, tc.want)
			}
		})
	}
}

// TestKillPidsDropsAPidSeenDead checks that a pid observed dead is never
// considered again. The number may report alive later because an unrelated
// process reused it, and that process must not receive SIGKILL.
func TestKillPidsDropsAPidSeenDead(t *testing.T) {
	t.Setenv(CoverageDirEnv, "/tmp/cover")
	calls := map[int]int{}
	alive := func(pid int) bool {
		calls[pid]++
		// 21 is first seen dead and then reused by a process that is alive.
		return pid != 21 || calls[pid] > 1
	}
	startTime := func(int) (uint64, error) { return 100, nil }
	var sent []sentSignal
	send := func(pid int, sig syscall.Signal) error {
		sent = append(sent, sentSignal{pid, sig})
		return nil
	}
	killPids([]int{21, 22}, send, alive, startTime, 20*time.Millisecond, time.Millisecond)
	if hasSignal(sent, 21, syscall.SIGKILL) {
		t.Fatalf("killPids sent SIGKILL to pid 21 after seeing it dead: %v", sent)
	}
	if !hasSignal(sent, 22, syscall.SIGKILL) {
		t.Fatalf("killPids did not SIGKILL the surviving pid 22: %v", sent)
	}
}

// TestKillPidsSkipsAReusedPid checks that a pid whose identity changed before
// the SIGKILL deadline is left alone: the number now names a different
// process, which the cleanup must not kill.
func TestKillPidsSkipsAReusedPid(t *testing.T) {
	t.Setenv(CoverageDirEnv, "/tmp/cover")
	calls := map[int]int{}
	startTime := func(pid int) (uint64, error) {
		calls[pid]++
		if pid != 31 {
			return 400, nil
		}
		if calls[pid] > 1 {
			return 200, nil // the number now names a different process
		}
		return 100, nil
	}
	alive := func(int) bool { return true }
	var sent []sentSignal
	send := func(pid int, sig syscall.Signal) error {
		sent = append(sent, sentSignal{pid, sig})
		return nil
	}
	killPids([]int{31, 32}, send, alive, startTime, 20*time.Millisecond, time.Millisecond)
	if hasSignal(sent, 31, syscall.SIGKILL) {
		t.Fatalf("killPids sent SIGKILL to reused pid 31: %v", sent)
	}
	if !hasSignal(sent, 32, syscall.SIGKILL) {
		t.Fatalf("killPids did not SIGKILL the surviving pid 32: %v", sent)
	}
}

func hasSignal(sent []sentSignal, pid int, sig syscall.Signal) bool {
	for _, s := range sent {
		if s.pid == pid && s.sig == sig {
			return true
		}
	}
	return false
}

func containsPID(pids []int, want int) bool {
	for _, pid := range pids {
		if pid == want {
			return true
		}
	}
	return false
}

func TestFreeAddrIsLoopback(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "addr"})

	for i := 0; i < 3; i++ {
		addr := host.FreeAddr(t)
		if !strings.HasPrefix(addr, "127.0.0.1:") {
			t.Fatalf("FreeAddr returned %q, want a 127.0.0.1 address", addr)
		}
		hostPart, port, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("splitting %q: %v", addr, err)
		}
		if hostPart != "127.0.0.1" {
			t.Errorf("FreeAddr returned host %q, want 127.0.0.1", hostPart)
		}
		if number, err := strconv.Atoi(port); err != nil || number == 0 {
			t.Errorf("FreeAddr returned port %q, want a non-zero number", port)
		}
	}
}

func TestNodeConfigFile(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "config"})

	if data := host.ReadConfig(t); data != nil {
		t.Fatalf("ReadConfig on a fresh node returned %q, want nil", data)
	}

	const yaml = "broker:\n  alias: x\n"
	host.WriteConfig(t, yaml)

	if got := string(host.ReadConfig(t)); got != yaml {
		t.Fatalf("ReadConfig returned %q, want %q", got, yaml)
	}

	path := filepath.Join(host.AccountHome(), ".peers", "config.yaml")
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if mode := info.Mode().Perm(); mode != 0o600 {
		t.Errorf("config file mode is %o, want 600", mode)
	}
	dirInfo, err := os.Stat(filepath.Dir(path))
	if err != nil {
		t.Fatalf("stat %s: %v", filepath.Dir(path), err)
	}
	if mode := dirInfo.Mode().Perm(); mode != 0o700 {
		t.Errorf("config directory mode is %o, want 700", mode)
	}
	if got := string(host.ReadFile(t, path)); got != yaml {
		t.Errorf("ReadFile returned %q, want %q", got, yaml)
	}

	entries, err := os.ReadDir(filepath.Dir(path))
	if err != nil {
		t.Fatalf("reading %s: %v", filepath.Dir(path), err)
	}
	if len(entries) != 1 || entries[0].Name() != "config.yaml" {
		names := make([]string, 0, len(entries))
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Errorf("config directory holds %v, want only config.yaml", names)
	}
}

// failureRecorder records a failure instead of ending the test, so a test can
// check that another call reported what it should.
type failureRecorder struct {
	testing.TB
	message string
}

// Helper does nothing; the recorder never points at a caller's line.
func (r *failureRecorder) Helper() {}

// Fatalf records the message.
func (r *failureRecorder) Fatalf(format string, args ...any) {
	r.message = fmt.Sprintf(format, args...)
}

// TestSessionEndReportsBackgroundError checks that a background failure the
// fake recorded fails the test at the end of the session instead of being
// lost. The injected failure is the positive control: without the check, End
// would report nothing.
func TestSessionEndReportsBackgroundError(t *testing.T) {
	n := NewLocalHost(t, LocalOptions{Name: "hostA"})
	s := n.StartSession(t, SessionSpec{Name: "background"})

	injected := errors.New("injected background failure")
	s.Fake().InjectBackgroundError(injected)
	if errs := s.TakeFakeErrors(t); len(errs) != 1 || !strings.Contains(errs[0].Error(), "injected background failure") {
		t.Fatalf("TakeFakeErrors() = %v, want the injected failure", errs)
	}

	// The error is reported once. Inject it again so End has one to report.
	s.Fake().InjectBackgroundError(injected)
	recorder := &failureRecorder{TB: t}
	s.End(recorder)
	if !strings.Contains(recorder.message, "injected background failure") {
		t.Fatalf("End reported %q, want the injected background failure", recorder.message)
	}
}

// TestSessionCallReportsOnePrefix checks that a failed tool call names the
// session and the tool in its failure message once. The call on a session
// without an MCP server process fails in the backend, whose error already
// carries that prefix; a second one added by Session.Call would make the
// message read "harness: session ...: calling ...: harness: session ...".
func TestSessionCallReportsOnePrefix(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "prefix"})
	s := host.StartSession(t, SessionSpec{Name: "a*b"})

	recorder := &failureRecorder{TB: t}
	s.Call(recorder, "list_agents", nil)
	const prefix = "harness: session a*b: calling list_agents:"
	if got := strings.Count(recorder.message, prefix); got != 1 {
		t.Fatalf("the failure message carries %q %d times, want 1: %s", prefix, got, recorder.message)
	}
}

func TestSessionEndIsIdempotent(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "end"})
	session := host.StartSession(t, SessionSpec{Name: "e"})

	record := session.Fake().RecordPath()
	session.End(t)
	session.End(t)

	if _, err := os.Stat(record); !os.IsNotExist(err) {
		t.Fatalf("session record %s still exists after End: %v", record, err)
	}
}

// TestSessionStartAllowsSlowMCPLaunch checks that starting a session is bounded
// by the startup allowance rather than by the three-second test wait. The
// wrapper delays the real MCP server by four seconds, longer than the test wait
// and shorter than the allowance: the start must succeed even though the
// handshake has not begun when the test wait would have expired.
func TestSessionStartAllowsSlowMCPLaunch(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "slow-launch"})
	local := host.impl.(*localNode)
	realBinary := local.bin

	const delay = 4 * time.Second
	wrapper := filepath.Join(t.TempDir(), "slow-mcp.sh")
	body := fmt.Sprintf("#!/bin/sh\nsleep %d\nexec \"%s\" \"$@\"\n", int(delay/time.Second), realBinary)
	if err := os.WriteFile(wrapper, []byte(body), 0o755); err != nil {
		t.Fatalf("writing %s: %v", wrapper, err)
	}
	local.bin = wrapper

	started := time.Now()
	session := host.StartSession(t, SessionSpec{Name: "slow", StartMCP: true})
	defer session.End(t)
	if elapsed := time.Since(started); elapsed < delay {
		t.Fatalf("the session started in %s, before the wrapper's %s delay", elapsed, delay)
	}
}

// fakeBrokerScript writes a stand-in Broker binary that records its arguments
// and fails with the given line on standard error.
func fakeBrokerScript(t *testing.T, dir, failure string) (script, record string) {
	t.Helper()
	script = filepath.Join(dir, "fake-broker.sh")
	record = filepath.Join(dir, "runs")
	body := fmt.Sprintf(`#!/bin/sh
printf '%%s\n' "$*" >> "$FAKE_BROKER_RECORD"
listen=
prev=
for arg in "$@"; do
  if [ "$prev" = "--listen" ]; then listen="$arg"; fi
  prev="$arg"
done
echo "peers service run: %s" >&2
exit 4
`, failure)
	if err := os.WriteFile(script, []byte(body), 0o755); err != nil {
		t.Fatalf("writing %s: %v", script, err)
	}
	return script, record
}

// recordedRuns returns the lines the stand-in Broker wrote.
func recordedRuns(t *testing.T, record string) []string {
	t.Helper()
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("reading %s: %v", record, err)
	}
	return strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
}

// listenArg returns the value after --listen in a recorded command line.
func listenArg(t *testing.T, line string) string {
	t.Helper()
	fields := strings.Fields(line)
	for i := 0; i+1 < len(fields); i++ {
		if fields[i] == "--listen" {
			return fields[i+1]
		}
	}
	t.Fatalf("no --listen in %q", line)
	return ""
}

func TestStartBrokerRetriesFreeAddrConflict(t *testing.T) {
	const addressTaken = "listen address $listen is not available: listen tcp $listen: bind: address already in use"

	t.Run("FreeAddr conflict is retried", func(t *testing.T) {
		host := NewLocalHost(t, LocalOptions{Name: "retry"})
		local := host.impl.(*localNode)
		script, record := fakeBrokerScript(t, t.TempDir(), addressTaken)
		local.bin = script

		addr := host.FreeAddr(t)
		result := host.StartBroker(t, BrokerSpec{
			Args:       []string{"--listen", addr},
			ConfigYAML: "broker:\n  listen: " + addr + "\n",
			Env:        map[string]string{"FAKE_BROKER_RECORD": record},
		})

		runs := recordedRuns(t, record)
		if len(runs) != 2 {
			t.Fatalf("the Broker ran %d times, want 2: %q", len(runs), runs)
		}
		second := listenArg(t, runs[1])
		if second == addr {
			t.Errorf("the second attempt reused %q", addr)
		}
		if !strings.HasPrefix(second, "127.0.0.1:") {
			t.Errorf("the second address is %q, want a 127.0.0.1 address", second)
		}
		if config := string(host.ReadConfig(t)); !strings.Contains(config, second) {
			t.Errorf("configuration %q does not carry the second address %q", config, second)
		}
		if result.Running {
			t.Errorf("StartBroker reports Running, want false")
		}
		if result.ExitCode != 4 {
			t.Errorf("ExitCode is %d, want 4", result.ExitCode)
		}
	})

	t.Run("an address the test chose is not retried", func(t *testing.T) {
		host := NewLocalHost(t, LocalOptions{Name: "intentional"})
		local := host.impl.(*localNode)
		script, record := fakeBrokerScript(t, t.TempDir(), addressTaken)
		local.bin = script

		listener, err := net.Listen("tcp", "127.0.0.1:0")
		if err != nil {
			t.Fatalf("reserving a port: %v", err)
		}
		defer func() { _ = listener.Close() }()

		result := host.StartBroker(t, BrokerSpec{
			Args: []string{"--listen", listener.Addr().String()},
			Env:  map[string]string{"FAKE_BROKER_RECORD": record},
		})

		if runs := recordedRuns(t, record); len(runs) != 1 {
			t.Errorf("the Broker ran %d times, want 1: %q", len(runs), runs)
		}
		if result.Running || result.ExitCode != 4 {
			t.Errorf("StartBroker = %+v, want a failure with exit code 4", result)
		}
	})

	t.Run("a different failure is not retried", func(t *testing.T) {
		host := NewLocalHost(t, LocalOptions{Name: "denied"})
		local := host.impl.(*localNode)
		script, record := fakeBrokerScript(t, t.TempDir(), "listen address $listen is not available: permission denied")
		local.bin = script

		result := host.StartBroker(t, BrokerSpec{
			Args: []string{"--listen", host.FreeAddr(t)},
			Env:  map[string]string{"FAKE_BROKER_RECORD": record},
		})

		if runs := recordedRuns(t, record); len(runs) != 1 {
			t.Errorf("the Broker ran %d times, want 1: %q", len(runs), runs)
		}
		if result.Running || result.ExitCode != 4 {
			t.Errorf("StartBroker = %+v, want a failure with exit code 4", result)
		}
	})
}

// TestStartBrokerReportsActualAddrAfterRetry holds the address FreeAddr handed
// out, so the first start fails on "address already in use" and the retry moves
// the Broker to a new one. The caller must learn the address the Broker
// actually listens on, and that address must answer.
func TestStartBrokerReportsActualAddrAfterRetry(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "retry-address"})

	var (
		addr string
		held net.Listener
		err  error
	)
	for attempt := 0; attempt < 5; attempt++ {
		addr = host.FreeAddr(t)
		held, err = net.Listen("tcp", addr)
		if err == nil {
			break
		}
	}
	if err != nil {
		t.Fatalf("holding a FreeAddr address: %v", err)
	}
	defer func() { _ = held.Close() }()

	start := host.StartBroker(t, BrokerSpec{Args: []string{"--listen", addr}})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}

	actual := start.Address(addr)
	if actual == addr {
		t.Fatalf("StartBroker reports %s, the address it was given; the Broker listens elsewhere", actual)
	}
	conn, err := net.DialTimeout("tcp", actual, 3*time.Second)
	if err != nil {
		t.Fatalf("dialing the reported address %s: %v", actual, err)
	}
	_ = conn.Close()
}

// TestLocalHostCleanupStopsBroker checks the rule the retry test depends on: a
// running Broker is gone once its node is cleaned up. On a build whose handle
// cannot signal a child it has not waited for, cleanup fails to stop the
// Broker, which survives as an orphan that holds its port and account lock for
// the rest of the run. The check reads the surviving children from ps rather
// than trusting the pid alone, so it reports the process by what it runs.
func TestLocalHostCleanupStopsBroker(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "cleanup-broker"})
	local := host.impl.(*localNode)
	addr := host.FreeAddr(t)
	start := host.StartBroker(t, BrokerSpec{Args: []string{"--listen", addr}})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}

	local.cleanup()

	if pids := childBrokerPids(t); len(pids) > 0 {
		t.Fatalf("cleanup left Broker children of this process running: %v", pids)
	}
}

// TestExportedTypeShapes pins the names and signatures downstream work items
// quote from the plan: the three aliases, the field names of the broker and
// exec results, and the methods scenarios call.
func TestExportedTypeShapes(t *testing.T) {
	var _ = BrokerStart(node.StartResult{})
	var _ = SessionSpec(node.SessionSpec{})
	var _ = BaseEnv(node.BaseEnv{})

	var _ = BrokerSpec{Args: nil, ConfigYAML: "", Env: nil, Bound: 0}
	var _ = ExecResult{ExitCode: 0, Stdout: "", Stderr: ""}
	var _ = BrokerStart{Running: false, PID: 0, ExitCode: 0, StderrLast: "", Addrs: nil}
	var _ = (func(string) string)(BrokerStart{}.Address)

	var _ = (func(testing.TB) *localpb.HelloResponse)((*OracleClient)(nil).Hello)
	var _ = (func(testing.TB) (*localpb.ListBrokersResponse, *localpb.ErrorDetail))((*OracleClient)(nil).ListBrokers)
	var _ = (func(testing.TB) (*localpb.StatusResponse, *localpb.ErrorDetail))((*OracleClient)(nil).Status)
	var _ = (func(testing.TB, string) (*localpb.GetConfigResponse, *localpb.ErrorDetail))((*OracleClient)(nil).GetConfig)
	var _ = (func(testing.TB, time.Time, uint32) ([]*localpb.AuditEvent, *localpb.ErrorDetail))((*OracleClient)(nil).Audit)

	var _ = (func(testing.TB, BrokerSpec) BrokerStart)((*Node)(nil).StartBroker)
	var _ = (func(testing.TB, SessionSpec) *Session)((*Node)(nil).StartSession)
	var _ = (func(testing.TB, []string, map[string]string) ExecResult)((*Node)(nil).Exec)
	var _ = (func() *OracleClient)((*Node)(nil).Oracle)
	var _ = (func(testing.TB) string)((*Node)(nil).FreeAddr)
	var _ = (func() string)((*Node)(nil).AccountHome)
	var _ = (func(testing.TB, string))((*Node)(nil).WriteConfig)
	var _ = (func(testing.TB) []byte)((*Node)(nil).ReadConfig)
	var _ = (func(testing.TB, string) []byte)((*Node)(nil).ReadFile)

	var _ = (func(testing.TB, string, map[string]any) Result)((*Session)(nil).Call)
	var _ = (func(testing.TB, time.Duration) fakesession.Frame)((*Session)(nil).WaitFrame)
	var _ = (func(testing.TB))((*Session)(nil).End)

	var _ = (func(string) (struct{}, error))(Decode[struct{}])
}
