//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"google.golang.org/protobuf/encoding/protodelim"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/procsig"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// forgeAccessCheckRole is the TestMain role that runs the attack loop as
	// the unprivileged account.
	forgeAccessCheckRole = "forge-access-check"
	// forgeAttempts is how many configuration writes the test attempts while
	// the attack loop runs.
	forgeAttempts = 150
	// forgeStartBound is the fallback for the wait for the attack loop to write
	// its ready file. The loop writes it before its first scan; the fallback
	// covers starting the test binary as another account in a loaded container.
	forgeStartBound = harness.EventTimeout
	// forgeScanPause paces the attack loop. The loop runs in the unprivileged
	// account's own process, where harness.Eventually is unavailable, so it
	// sleeps between scans itself. The pause keeps a core free while still
	// scanning often enough to catch a short-lived check process.
	forgeScanPause = 50 * time.Microsecond
)

// TestREQBrokerConfig021_AccessCheckCannotBeForged checks that an account
// cannot substitute the access-check result by stopping the helper and writing
// a forged response into its standard output. Every write must stay denied.
//
// The check's stdout pipe belongs to the root Broker, so a plain open of
// /proc/<pid>/fd/1 is refused to the account even before the fix;
// pidfd_getfd duplicates the same descriptor but is gated by ptrace access.
// The test therefore also requires that the account can neither stop nor
// inspect the check process, and that no write reached its stdout.
func TestREQBrokerConfig021_AccessCheckCannotBeForged(t *testing.T) {
	if !syscontainer.InContainer() {
		syscontainer.Run(t, syscontainer.Plain)
		return
	}

	syscontainer.StartSystemBroker(t, "broker:\n  alias: sys\n")

	other := syscontainer.NewSession(t, "other", "otheragent")
	permWaitConnected(t, other)

	forger := startAccessCheckForger(t)
	t.Cleanup(func() { forger.stop(t) })

	for attempt := 0; attempt < forgeAttempts; attempt++ {
		text, isError := other.Call(t, "set_config",
			map[string]any{"key": "broker.alias", "value": fmt.Sprintf("forged-%d", attempt)})
		if !isError {
			t.Fatalf("attempt %d: set_config succeeded while a forged access-check response was being fed: %s",
				attempt, text)
		}
		if !strings.Contains(text, "(code: permission_denied)") {
			t.Fatalf("attempt %d: set_config error = %q, want permission_denied", attempt, text)
		}
	}

	forger.stop(t)
	stats := forger.stats(t)
	t.Logf("forger: %+v", stats)
	if stats.Pids == 0 {
		t.Fatalf("the forger found no access-check processes; the scenario did not run")
	}
	if stats.Stops != 0 {
		t.Errorf("the forger stopped %d access-check processes; want 0: the caller must not signal the helper", stats.Stops)
	}
	if stats.Readlinks != 0 {
		t.Errorf("the forger inspected %d access-check stdout links; want 0: the caller must not inspect the helper", stats.Readlinks)
	}
	if stats.Writes != 0 {
		t.Errorf("the forger wrote into %d access-check stdout streams; want 0", stats.Writes)
	}
	if stats.Writes == 0 && stats.FirstErr != "" {
		t.Logf("the write attempts could not complete: %s", stats.FirstErr)
	}
}

// forger is one running attack-loop process.
type forger struct {
	cmd      *exec.Cmd
	stopPath string
	exited   chan error
	stdout   *bytes.Buffer
	stderr   *bytes.Buffer

	stopOnce sync.Once
	waitErr  error
}

// startAccessCheckForger starts the test binary's attack role as other.
func startAccessCheckForger(t *testing.T) *forger {
	t.Helper()

	otherUID := syscontainer.AccountUID(t, "other")
	// The attack loop runs as other, so the directory holding its ready and
	// stop files must be writable by that account.
	dir, err := os.MkdirTemp("/tmp", "ap-e2e-forge-")
	if err != nil {
		t.Fatalf("creating the forger directory: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(dir) })
	if err := os.Chmod(dir, 0o777); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	stopPath := filepath.Join(dir, "stop")
	readyPath := filepath.Join(dir, "ready")

	cmd := syscontainer.Command("other", []string{
		registerClientRoleEnv + "=" + forgeAccessCheckRole,
		"PEERS_E2E_FORGE_UID=" + strconv.FormatUint(uint64(otherUID), 10),
		"PEERS_E2E_FORGE_STOP=" + stopPath,
		"PEERS_E2E_FORGE_READY=" + readyPath,
	}, copyTestBinary(t))

	stdout := new(bytes.Buffer)
	stderr := new(bytes.Buffer)
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the access-check forger as other: %v", err)
	}
	exited := make(chan error, 1)
	go func() { exited <- cmd.Wait() }()

	f := &forger{
		cmd:      cmd,
		stopPath: stopPath,
		exited:   exited,
		stdout:   stdout,
		stderr:   stderr,
	}
	t.Cleanup(func() { f.stop(t) })
	harness.Eventually(t, forgeStartBound, "the access-check forger to write its ready file",
		func() (bool, string) {
			if _, err := os.Stat(readyPath); err == nil {
				return true, ""
			}
			select {
			case err := <-exited:
				t.Fatalf("the access-check forger exited before it was ready: %v: %s",
					err, strings.TrimSpace(stderr.String()))
			default:
			}
			return false, "the ready file is absent"
		})
	return f
}

// stop ends the attack loop and waits for it to exit. It is idempotent.
func (f *forger) stop(t *testing.T) {
	t.Helper()
	f.stopOnce.Do(func() {
		_ = os.WriteFile(f.stopPath, nil, 0o644)
		select {
		case f.waitErr = <-f.exited:
		case <-time.After(3 * time.Second):
			_ = f.cmd.Process.Kill()
			f.waitErr = <-f.exited
			t.Logf("the access-check forger did not stop in time and was killed")
		}
	})
	if f.waitErr != nil {
		t.Logf("the access-check forger exited with %v: %s", f.waitErr, strings.TrimSpace(f.stderr.String()))
	}
}

// stats returns the counters the attack loop printed when it exited.
func (f *forger) stats(t *testing.T) forgeSummary {
	t.Helper()
	var stats forgeSummary
	out := strings.TrimSpace(f.stdout.String())
	if out == "" {
		return stats
	}
	line := out
	if index := strings.LastIndexByte(out, '\n'); index >= 0 {
		line = out[index+1:]
	}
	if err := json.Unmarshal([]byte(line), &stats); err != nil {
		t.Logf("decoding the forger summary %q: %v", line, err)
	}
	return stats
}

// forgeSummary is the line the attack role prints when it exits.
type forgeSummary struct {
	Pids      int    `json:"pids"`
	Stops     int    `json:"stops"`
	Readlinks int    `json:"readlinks"`
	Writes    int    `json:"writes"`
	FirstErr  string `json:"first_error,omitempty"`
}

// runForgeAccessCheckRole is the body of the attack role. It repeatedly finds
// access-check processes, stops them, writes a forged successful response into
// their standard output and continues them.
func runForgeAccessCheckRole() int {
	uid, err := strconv.ParseUint(os.Getenv("PEERS_E2E_FORGE_UID"), 10, 32)
	if err != nil {
		fmt.Fprintf(os.Stderr, "forge-access-check: bad uid: %v\n", err)
		return 2
	}
	stopPath := os.Getenv("PEERS_E2E_FORGE_STOP")
	readyPath := os.Getenv("PEERS_E2E_FORGE_READY")

	var forged bytes.Buffer
	if _, err := protodelim.MarshalTo(&forged, &helperpb.AccessCheckResponse{
		ProtocolVersion: helperpb.ProtocolVersion,
		Uid:             uint32(uid),
		Allowed:         true,
	}); err != nil {
		fmt.Fprintf(os.Stderr, "forge-access-check: encoding the forged response: %v\n", err)
		return 2
	}
	payload := forged.Bytes()

	if readyPath != "" {
		if err := os.WriteFile(readyPath, nil, 0o644); err != nil {
			fmt.Fprintf(os.Stderr, "forge-access-check: writing the ready file: %v\n", err)
			return 2
		}
	}

	var stats forgeSummary
	for {
		if _, err := os.Stat(stopPath); err == nil {
			break
		}
		for _, pid := range accessCheckPids() {
			stats.Pids++
			stopErr := procsig.SendSignal(pid, syscall.SIGSTOP)
			if stopErr == nil {
				stats.Stops++
			}
			link := fmt.Sprintf("/proc/%d/fd/1", pid)
			if _, err := os.Readlink(link); err == nil {
				stats.Readlinks++
			}
			// Try the write whether or not the stop succeeded: the stop only
			// widens the window, and the fd access is the property under test.
			out, err := openProcessStdout(pid)
			if err == nil {
				if _, werr := out.Write(payload); werr == nil {
					stats.Writes++
				} else if stats.FirstErr == "" {
					stats.FirstErr = werr.Error()
				}
				_ = out.Close()
			} else if stats.FirstErr == "" {
				stats.FirstErr = err.Error()
			}
			if stopErr == nil {
				_ = procsig.SendSignal(pid, syscall.SIGCONT)
			}
		}
		// Pace the loop; see forgeScanPause.
		time.Sleep(forgeScanPause)
	}

	line, _ := json.Marshal(stats)
	fmt.Println(string(line))
	return 0
}

// accessCheckPids returns the pids whose command line is an access-check run.
func accessCheckPids() []int {
	procs, err := node.ListProcs(func(p node.Proc) bool {
		args := p.Args
		for len(args) > 0 && args[len(args)-1] == "" {
			args = args[:len(args)-1]
		}
		return len(args) >= 2 && args[len(args)-2] == "internal" && args[len(args)-1] == "access-check"
	})
	if err != nil {
		return nil
	}
	self := os.Getpid()
	pids := make([]int, 0, len(procs))
	for _, p := range procs {
		if p.PID == self {
			continue
		}
		pids = append(pids, p.PID)
	}
	return pids
}
