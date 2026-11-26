//go:build e2e_darwin

package darwinservice

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakesession"
	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/node"
)

const (
	helperRegisterRole      = "helper-register-many-groups"
	helperRegisterWatchRole = "helper-register-many-groups-watch"

	helperGroupUser = "aphelper"
	helperGroupUID  = 6502
	helperGroupGID  = 6300
	// helperGroupCount is the number of supplementary groups. The primary group
	// is added on top, so the account ends up in one more than this.
	helperGroupCount   = 17
	helperPrimaryGroup = "ahgp"

	// darwinNGroups is Darwin's NGROUPS, the largest group table a single
	// setgroups call accepts.
	darwinNGroups = 16

	helperRegisterBound = 45 * time.Second
	helperIdentityBound = 15 * time.Second
	helperWatchQuiet    = 2 * time.Second
)

// TestSystemServiceRegistersManyGroups checks that the system Broker registers
// and watches a session of an account in more groups than Darwin's NGROUPS.
func TestSystemServiceRegistersManyGroups(t *testing.T) {
	ids := setupHelperGroups(t)
	t.Logf("account %s uid=%d gid=%d belongs to %d groups: %v",
		helperGroupUser, helperGroupUID, helperGroupGID, len(ids), ids)

	resetSystemService(t)
	t.Cleanup(func() { resetSystemService(t) })
	ensureSystemConfig(t)

	res := run(t, "sudo", "-n", installedBinary, "service", "install", "--system")
	if res.exitCode != 0 {
		t.Fatalf("service install --system: exit %d\nstdout: %s\nstderr: %s",
			res.exitCode, res.stdout, res.stderr)
	}
	waitForPID(t, "system", serviceStartBound)

	t.Run("registration", func(t *testing.T) {
		out := runRoleAsUser(t, helperGroupUser, helperRegisterRole)
		if out.exitCode != 0 {
			t.Fatalf("registering the %d-group account: exit %d\nstdout: %s\nstderr: %s",
				len(ids), out.exitCode, out.stdout, out.stderr)
		}
		if !strings.Contains(out.stdout, "RESULT registered=true") {
			t.Fatalf("registering the %d-group account: %s, want a completed registration",
				len(ids), out.stdout)
		}
		t.Logf("registration completed: %s", strings.TrimSpace(out.stdout))
	})

	t.Run("helper identity", func(t *testing.T) {
		role := startRoleAsUser(t, helperGroupUser, helperRegisterWatchRole)

		pids := waitForAccountHelper(t, helperGroupUID, role, helperIdentityBound)
		for _, pid := range pids {
			id := readHelperIdentity(t, pid)
			if id.uid != helperGroupUID || id.ruid != helperGroupUID ||
				id.svuid != helperGroupUID || id.rgid != helperGroupGID {
				t.Fatalf("account-helper pid %d identity: uid=%d ruid=%d svuid=%d rgid=%d, want uid=ruid=svuid=%d and rgid=%d",
					pid, id.uid, id.ruid, id.svuid, id.rgid, helperGroupUID, helperGroupGID)
			}
			t.Logf("account-helper pid %d identity: uid=%d ruid=%d svuid=%d rgid=%d",
				pid, id.uid, id.ruid, id.svuid, id.rgid)
		}

		out := role.wait()
		if out.exitCode != 0 {
			t.Fatalf("registering the %d-group account behind the identity check: exit %d\nstdout: %s\nstderr: %s",
				len(ids), out.exitCode, out.stdout, out.stderr)
		}
		if !strings.Contains(out.stdout, "RESULT registered=true") {
			t.Fatalf("registering the %d-group account behind the identity check: %s, want a completed registration",
				len(ids), out.stdout)
		}
	})

	t.Run("directory watch", func(t *testing.T) {
		out := runRoleAsUser(t, helperGroupUser, helperRegisterWatchRole)
		if out.exitCode != 0 {
			t.Fatalf("registering the %d-group account after writing its record: exit %d\nstdout: %s\nstderr: %s",
				len(ids), out.exitCode, out.stdout, out.stderr)
		}
		if !strings.Contains(out.stdout, "RESULT registered=true") {
			t.Fatalf("registering the %d-group account after writing its record: %s, want a completed registration through the directory watch",
				len(ids), out.stdout)
		}
		t.Logf("registration through the directory watch completed: %s", strings.TrimSpace(out.stdout))
	})
}

func setupHelperGroups(t *testing.T) []uint32 {
	t.Helper()
	preflightHelperGroupIDs(t)
	cleanupHelperGroups(t)
	t.Cleanup(func() { cleanupHelperGroups(t) })

	mustSudo(t, "dscl", ".", "-create", "/Groups/"+helperPrimaryGroup)
	mustSudo(t, "dscl", ".", "-create", "/Groups/"+helperPrimaryGroup,
		"PrimaryGroupID", strconv.Itoa(helperGroupGID))
	sudoIgnore("dscl", ".", "-create", "/Groups/"+helperPrimaryGroup, "RealName", "peers e2e primary group")
	for i := 1; i <= helperGroupCount; i++ {
		name := helperGroupName(i)
		mustSudo(t, "dscl", ".", "-create", "/Groups/"+name)
		mustSudo(t, "dscl", ".", "-create", "/Groups/"+name,
			"PrimaryGroupID", strconv.Itoa(helperGroupGID+i))
		sudoIgnore("dscl", ".", "-create", "/Groups/"+name, "RealName", "peers e2e group")
	}

	sudoIgnore("dscl", ".", "-create", "/Users/"+helperGroupUser)
	mustSudo(t, "dscl", ".", "-create", "/Users/"+helperGroupUser,
		"UniqueID", strconv.Itoa(helperGroupUID))
	mustSudo(t, "dscl", ".", "-create", "/Users/"+helperGroupUser,
		"PrimaryGroupID", strconv.Itoa(helperGroupGID))
	sudoIgnore("dscl", ".", "-create", "/Users/"+helperGroupUser, "UserShell", "/bin/bash")
	sudoIgnore("dscl", ".", "-create", "/Users/"+helperGroupUser, "NFSHomeDirectory", "/var/empty")

	for i := 1; i <= helperGroupCount; i++ {
		mustSudo(t, "dseditgroup", "-o", "edit", "-a", helperGroupUser,
			"-t", "user", helperGroupName(i))
	}
	sudoIgnore("dscacheutil", "-flushcache")

	ids := waitForGroups(t, helperGroupUser, helperGroupCount+1)
	if len(ids) <= darwinNGroups {
		t.Fatalf("the account belongs to %d groups, want more than Darwin's NGROUPS (%d)", len(ids), darwinNGroups)
	}
	return ids
}

func cleanupHelperGroups(t *testing.T) {
	t.Helper()
	sudoIgnore("dscl", ".", "-delete", "/Users/"+helperGroupUser)
	for i := 1; i <= helperGroupCount; i++ {
		sudoIgnore("dscl", ".", "-delete", "/Groups/"+helperGroupName(i))
	}
	sudoIgnore("dscl", ".", "-delete", "/Groups/"+helperPrimaryGroup)
	sudoIgnore("dscacheutil", "-flushcache")
}

func helperGroupName(i int) string {
	return fmt.Sprintf("ahg%02d", i)
}

func preflightHelperGroupIDs(t *testing.T) {
	t.Helper()
	users := dsclList(t, "/Users", "UniqueID")
	groups := dsclList(t, "/Groups", "PrimaryGroupID")
	checkNameID(t, users, helperGroupUser, helperGroupUID, "user", "uid")
	checkNameID(t, groups, helperPrimaryGroup, helperGroupGID, "group", "gid")
	for i := 1; i <= helperGroupCount; i++ {
		checkNameID(t, groups, helperGroupName(i), helperGroupGID+i, "group", "gid")
	}
}

func runHelperRegisterRole() int { return runHelperRegistration(false) }

func runHelperRegisterWatchRole() int { return runHelperRegistration(true) }

func runHelperRegistration(watch bool) int {
	tb := node.NewTB("helper-register", "/tmp")
	defer tb.Close()

	dir, err := os.MkdirTemp("/tmp", "peers-helper-groups-")
	if err != nil {
		return helperProbeFailure("error=%q", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	configDir := filepath.Join(dir, "claude")
	if err := os.MkdirAll(configDir, 0o700); err != nil {
		return helperProbeFailure("error=%q", err)
	}

	fake := fakesession.New(tb, fakesession.Options{
		ConfigDir: configDir,
		PID:       os.Getpid(),
		Name:      "helper-probe",
		Status:    "idle",
	})
	if watch {
		fake.RemoveRecord()
	}

	conn, err := grpc.NewClient("unix://"+systemSocketPath, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return helperProbeFailure("error=%q", err)
	}
	defer func() { _ = conn.Close() }()

	ctx, cancel := context.WithTimeout(context.Background(), helperRegisterBound)
	defer cancel()

	stream, err := localpb.NewLocalClient(conn).Register(ctx)
	if err != nil {
		return helperProbeFailure("error=%q", err)
	}
	if err := stream.Send(&localpb.RegisterRequest{
		InboxSocketPath: fake.SocketPath(),
		ClaudeConfigDir: fake.ConfigDir(),
	}); err != nil {
		return helperProbeFailure("error=%q", err)
	}

	type registerEvent struct {
		resp *localpb.RegisterResponse
		err  error
	}
	events := make(chan registerEvent, 8)
	go func() {
		for {
			resp, err := stream.Recv()
			events <- registerEvent{resp: resp, err: err}
			if err != nil {
				return
			}
		}
	}()

	if watch {
		quiet := time.NewTimer(helperWatchQuiet)
		defer quiet.Stop()
	waitQuiet:
		for {
			select {
			case ev := <-events:
				if ev.err != nil {
					return helperProbeFailure("error=%q", ev.err)
				}
				switch e := ev.resp.Event.(type) {
				case *localpb.RegisterResponse_RegistrationComplete:
					return helperProbeFailure("error=%q", "registration completed before the session record existed")
				case *localpb.RegisterResponse_NotRegistered:
					return helperProbeFailure("reason=%s detail=%q",
						e.NotRegistered.GetReason(), e.NotRegistered.GetDetail())
				}
			case <-quiet.C:
				break waitQuiet
			case <-ctx.Done():
				return helperProbeFailure("error=%q", ctx.Err())
			}
		}
		fake.RestoreRecord()
	}

	lastProblem := ""
	for {
		select {
		case ev := <-events:
			if ev.err != nil {
				return helperProbeFailure("%serror=%q", lastProblem, ev.err)
			}
			switch e := ev.resp.Event.(type) {
			case *localpb.RegisterResponse_RegistrationComplete:
				fmt.Printf("RESULT registered=true address=%s\n", e.RegistrationComplete.GetAgentAddress())
				return 0
			case *localpb.RegisterResponse_NotRegistered:
				problem := fmt.Sprintf("reason=%s detail=%q ",
					e.NotRegistered.GetReason(), e.NotRegistered.GetDetail())
				if !watch {
					return helperProbeFailure("%s", problem)
				}
				// The watch probe may see a not_registered report already in flight,
				// so it keeps waiting until the bound.
				lastProblem = problem
			}
		case <-ctx.Done():
			return helperProbeFailure("%serror=%q", lastProblem, ctx.Err())
		}
	}
}

func helperProbeFailure(format string, args ...any) int {
	fmt.Printf("RESULT registered=false "+format+"\n", args...)
	return 1
}

type roleRun struct {
	cmd    *exec.Cmd
	stdout strings.Builder
	stderr strings.Builder
	done   chan struct{}
	res    result
}

func startRoleAsUser(t *testing.T, user, role string) *roleRun {
	t.Helper()
	r := &roleRun{done: make(chan struct{})}
	r.cmd = exec.Command("sudo", "-n", "-u", user, "env", roleEnv+"="+role, e2eBinary)
	r.cmd.Stdout = &r.stdout
	r.cmd.Stderr = &r.stderr
	if err := r.cmd.Start(); err != nil {
		t.Fatalf("starting %s as %s: %v", role, user, err)
	}
	go func() {
		err := r.cmd.Wait()
		r.res = result{stdout: r.stdout.String(), stderr: r.stderr.String()}
		if err != nil {
			var exitErr *exec.ExitError
			if errors.As(err, &exitErr) {
				r.res.exitCode = exitErr.ExitCode()
			} else {
				r.res.exitCode = -1
				r.res.stderr += fmt.Sprintf("\nwaiting for the process: %v", err)
			}
		}
		close(r.done)
	}()
	t.Cleanup(func() {
		select {
		case <-r.done:
			return
		default:
		}
		_ = r.cmd.Process.Signal(os.Interrupt)
		select {
		case <-r.done:
		case <-time.After(harness.EventTimeout):
			_ = r.cmd.Process.Kill()
			<-r.done
		}
	})
	return r
}

func (r *roleRun) wait() result {
	<-r.done
	return r.res
}

// accountHelperPIDs matches only the command line, because a helper starts as
// root and switches to the account before it serves, and ps -ww keeps the
// subcommand name from being truncated.
func accountHelperPIDs(t *testing.T) []int {
	t.Helper()
	out, err := exec.Command("ps", "-ww", "-axo", "pid=,command=").Output()
	if err != nil {
		t.Fatalf("listing processes: %v", err)
	}
	var pids []int
	for _, line := range strings.Split(string(out), "\n") {
		fields := strings.Fields(line)
		if len(fields) < 2 || !strings.Contains(line, "internal account-helper") {
			continue
		}
		pid, err := strconv.Atoi(fields[0])
		if err != nil {
			continue
		}
		pids = append(pids, pid)
	}
	return pids
}

// waitForAccountHelper returns a helper that never switched identity too, so
// the identity check reports the failed switch instead of this wait timing out.
func waitForAccountHelper(t *testing.T, uid int, role *roleRun, bound time.Duration) []int {
	t.Helper()
	deadline := time.Now().Add(bound)
	for {
		if pids := accountHelperPIDs(t); len(pids) > 0 {
			return pids
		}
		select {
		case <-role.done:
			res := role.wait()
			t.Fatalf("the Broker never started the account-helper for uid %d: the registration probe exited %d\nstdout: %s\nstderr: %s",
				uid, res.exitCode, res.stdout, res.stderr)
		default:
		}
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for the Broker to start the account-helper for uid %d", bound, uid)
		}
		time.Sleep(pollInterval)
	}
}

type helperIdentity struct {
	uid, ruid, svuid, rgid int
}

func readHelperIdentity(t *testing.T, pid int) helperIdentity {
	t.Helper()
	out, err := exec.Command("ps", "-o", "uid,ruid,svuid,rgid", "-p", strconv.Itoa(pid)).Output()
	if err != nil {
		t.Fatalf("ps -o uid,ruid,svuid,rgid -p %d: %v", pid, err)
	}
	lines := strings.Split(strings.TrimSpace(string(out)), "\n")
	if len(lines) != 2 {
		t.Fatalf("ps -o uid,ruid,svuid,rgid -p %d: output %q, want a header and one row", pid, out)
	}
	fields := strings.Fields(lines[1])
	if len(fields) != 4 {
		t.Fatalf("ps -o uid,ruid,svuid,rgid -p %d: row %q, want four ids", pid, lines[1])
	}
	var ids [4]int
	for i, field := range fields {
		v, err := strconv.Atoi(field)
		if err != nil {
			t.Fatalf("ps -o uid,ruid,svuid,rgid -p %d: column %d %q: %v", pid, i, field, err)
		}
		ids[i] = v
	}
	return helperIdentity{uid: ids[0], ruid: ids[1], svuid: ids[2], rgid: ids[3]}
}
