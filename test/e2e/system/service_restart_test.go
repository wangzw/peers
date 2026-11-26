//go:build e2e

package e2e

import (
	"fmt"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/procsig"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

// serviceRestartBound bounds the wait for systemd to replace a killed system
// Broker; the drop-in below sets RestartSec to 100ms.
const serviceRestartBound = 3 * time.Second

// TestSystemServiceStartsAndRestarts checks that systemd restarts the system
// Broker under a new MainPID after it is killed with SIGKILL.
func TestSystemServiceStartsAndRestarts(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Systemd)
		return
	}

	dropInDir := "/run/systemd/system/peers.service.d"
	if err := os.MkdirAll(dropInDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dropInDir, err)
	}
	dropIn := filepath.Join(dropInDir, "restart.conf")
	content := "[Service]\nRestart=always\nRestartSec=100ms\n"
	if err := os.WriteFile(dropIn, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", dropIn, err)
	}
	if out, err := syscontainer.Command("root", nil, "systemctl", "daemon-reload").CombinedOutput(); err != nil {
		t.Fatalf("systemctl daemon-reload: %v\n%s", err, strings.TrimSpace(string(out)))
	}

	syscontainer.StartSystemService(t, "")
	before, ok := systemServiceMainPID(t)
	if !ok || before == 0 {
		t.Fatalf("systemctl show MainPID after the start = %d (found %v), want a non-zero pid", before, ok)
	}
	if err := procsig.SendSignal(before, syscall.SIGKILL); err != nil {
		t.Fatalf("kill -9 the system Broker %d: %v", before, err)
	}
	t.Logf("the system Broker started with MainPID %d and was killed", before)

	var after int
	harness.Eventually(t, serviceRestartBound, "systemd to restart the system Broker",
		func() (bool, string) {
			pid, ok := systemServiceMainPID(t)
			if !ok {
				return false, "systemctl show did not report a MainPID"
			}
			if pid == 0 || pid == before {
				return false, fmt.Sprintf("MainPID = %d, want a new non-zero pid (was %d)", pid, before)
			}
			// systemd sets the new MainPID when it forks the replacement, before the child
			// execs peers, so the Broker listing can lag; wait for the pid rather than
			// scanning once.
			procs := syscontainer.Pids("peers", "service", "run", "--system")
			if !hasPID(procs, pid) {
				return false, fmt.Sprintf("MainPID = %d, but the running system Brokers are %v", pid, procs)
			}
			after = pid
			return true, ""
		})
	t.Logf("systemd restarted the system Broker with MainPID %d", after)
}

func systemServiceMainPID(t *testing.T) (int, bool) {
	t.Helper()
	cmd := syscontainer.Command("root", nil,
		"systemctl", "show", "--property", "MainPID", "--value", "peers.service")
	out, err := cmd.Output()
	if err != nil {
		return 0, false
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0, false
	}
	return pid, true
}

func hasPID(procs []syscontainer.ProcInfo, pid int) bool {
	for _, proc := range procs {
		if proc.PID == pid {
			return true
		}
	}
	return false
}
