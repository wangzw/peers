package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"syscall"
	"testing"
	"time"
)

func envTestClaude(t *testing.T, versionLine string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "claude")
	script := "#!/bin/sh\necho '" + versionLine + "'\n"
	if err := os.WriteFile(path, []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func envTestPeers(t *testing.T) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "peers")
	if err := os.WriteFile(path, []byte("#!/bin/sh\nexit 0\n"), 0o755); err != nil {
		t.Fatal(err)
	}
	return path
}

func envTestInit(t *testing.T) (root, home string) {
	t.Helper()
	base := t.TempDir()
	root = filepath.Join(base, "root")
	home = filepath.Join(base, "home", ".peers")
	err := runEnvInit([]string{
		"--root", root,
		"--claude-bin", envTestClaude(t, "2.1.311 (Claude Code)"),
		"--peers-bin", envTestPeers(t),
		"--peers-home", home,
	}, io.Discard)
	if err != nil {
		t.Fatalf("env init: %v", err)
	}
	return root, home
}

// TestEnvInitLayout checks the directories, settings, wrapper and permissions
// env init writes.
func TestEnvInitLayout(t *testing.T) {
	root, home := envTestInit(t)

	for _, dir := range []string{"bin", "cfg", "cfg/sessions", "home", "debug", "ctl"} {
		info, err := os.Stat(filepath.Join(root, dir))
		if err != nil || !info.IsDir() {
			t.Fatalf("directory %s missing: %v", dir, err)
		}
	}
	sessions, err := os.Stat(filepath.Join(root, "cfg", "sessions"))
	if err != nil {
		t.Fatal(err)
	}
	if got := sessions.Mode().Perm(); got != 0o700 {
		t.Errorf("cfg/sessions mode = %o, want 700", got)
	}
	for _, name := range probeNames {
		for _, kind := range []string{"work", "out"} {
			if info, err := os.Stat(filepath.Join(root, kind, name)); err != nil || !info.IsDir() {
				t.Errorf("%s/%s missing: %v", kind, name, err)
			}
		}
		project, err := os.ReadFile(filepath.Join(root, "work", name, ".peers.yaml"))
		if err != nil {
			t.Fatal(err)
		}
		if got, want := string(project), "topics: [rs-probe]\n"; got != want {
			t.Errorf("work/%s/.peers.yaml = %q, want %q", name, got, want)
		}
	}

	const claudeScript = "#!/bin/sh\necho '2.1.311 (Claude Code)'\n"
	if data, err := os.ReadFile(filepath.Join(root, "bin", "claude")); err != nil {
		t.Errorf("read bin/claude: %v", err)
	} else if string(data) != claudeScript {
		t.Errorf("bin/claude content = %q", data)
	}
	if data, err := os.ReadFile(filepath.Join(root, "bin", "peers")); err != nil {
		t.Errorf("read bin/peers: %v", err)
	} else if string(data) != "#!/bin/sh\nexit 0\n" {
		t.Errorf("bin/peers content = %q", data)
	}
	if _, err := os.Stat(filepath.Join(root, "bin", "rsprobe")); err != nil {
		t.Errorf("bin/rsprobe missing: %v", err)
	}

	version, err := os.ReadFile(filepath.Join(root, "VERSION"))
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasPrefix(strings.TrimSpace(string(version)), "2.1.311") {
		t.Errorf("VERSION = %q, want a 2.1.311 version", version)
	}

	var settings struct {
		SkipDangerousModePermissionPrompt bool              `json:"skipDangerousModePermissionPrompt"`
		Env                               map[string]string `json:"env"`
	}
	data, err := os.ReadFile(filepath.Join(root, "cfg", "settings.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &settings); err != nil {
		t.Fatalf("settings.json: %v", err)
	}
	if !settings.SkipDangerousModePermissionPrompt {
		t.Error("settings.json skipDangerousModePermissionPrompt = false")
	}
	if settings.Env["DISABLE_AUTOUPDATER"] != "1" {
		t.Errorf("settings.json env = %v", settings.Env)
	}

	var claudeConfig struct {
		HasCompletedOnboarding bool `json:"hasCompletedOnboarding"`
		Projects               map[string]struct {
			HasTrustDialogAccepted bool `json:"hasTrustDialogAccepted"`
		} `json:"projects"`
	}
	data, err = os.ReadFile(filepath.Join(root, "cfg", ".claude.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(data, &claudeConfig); err != nil {
		t.Fatalf(".claude.json: %v", err)
	}
	if !claudeConfig.HasCompletedOnboarding {
		t.Error(".claude.json hasCompletedOnboarding = false")
	}
	for _, name := range probeNames {
		project, ok := claudeConfig.Projects[filepath.Join(root, "work", name)]
		if !ok || !project.HasTrustDialogAccepted {
			t.Errorf(".claude.json trust entry for work/%s = %+v, present=%v", name, project, ok)
		}
	}

	homeInfo, err := os.Stat(home)
	if err != nil {
		t.Fatal(err)
	}
	if got := homeInfo.Mode().Perm(); got != 0o700 {
		t.Errorf("peers home mode = %o, want 700", got)
	}
	configPath := filepath.Join(home, "config.yaml")
	configData, err := os.ReadFile(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := string(configData), "broker:\n  listen: 127.0.0.1:27443\n  alias: hostB\n"; got != want {
		t.Errorf("config.yaml = %q, want %q", got, want)
	}
	configInfo, err := os.Stat(configPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := configInfo.Mode().Perm(); got != 0o600 {
		t.Errorf("config.yaml mode = %o, want 600", got)
	}

	runPath := filepath.Join(root, "run")
	runInfo, err := os.Stat(runPath)
	if err != nil {
		t.Fatal(err)
	}
	if got := runInfo.Mode().Perm(); got != 0o755 {
		t.Errorf("run mode = %o, want 755", got)
	}
	runData, err := os.ReadFile(runPath)
	if err != nil {
		t.Fatal(err)
	}
	run := string(runData)
	for _, want := range []string{
		"env -i",
		"HOME=" + filepath.Join(root, "home"),
		"CLAUDE_CONFIG_DIR=" + filepath.Join(root, "cfg"),
		`CLAUDE_CODE_OAUTH_TOKEN="$CLAUDE_CODE_OAUTH_TOKEN"`,
	} {
		if !strings.Contains(run, want) {
			t.Errorf("run script does not contain %q:\n%s", want, run)
		}
	}

	// No token value may be written to disk; the copied executables under bin/
	// are skipped because they are byte copies of pre-existing binaries.
	leaked := regexp.MustCompile(`CLAUDE_CODE_OAUTH_TOKEN=([^\s"$][^\s]*)`)
	binPrefix := filepath.Join(root, "bin") + string(os.PathSeparator)
	for _, tree := range []string{root, home} {
		err = filepath.WalkDir(tree, func(path string, d os.DirEntry, walkErr error) error {
			if walkErr != nil {
				return walkErr
			}
			if d.IsDir() || strings.HasPrefix(path, binPrefix) {
				return nil
			}
			data, err := os.ReadFile(path)
			if err != nil {
				return err
			}
			if match := leaked.FindSubmatch(data); match != nil {
				t.Errorf("%s contains a literal CLAUDE_CODE_OAUTH_TOKEN value %q", path, match[1])
			}
			return nil
		})
		if err != nil {
			t.Fatal(err)
		}
	}
}

// TestEnvInitRejectsWrongVersion checks that env init refuses a Claude Code
// version other than the pinned one.
func TestEnvInitRejectsWrongVersion(t *testing.T) {
	base := t.TempDir()
	err := runEnvInit([]string{
		"--root", filepath.Join(base, "root"),
		"--claude-bin", envTestClaude(t, "2.1.327 (Claude Code)"),
		"--peers-bin", envTestPeers(t),
		"--peers-home", filepath.Join(base, "home", ".peers"),
	}, io.Discard)
	if err == nil {
		t.Fatal("env init accepted version 2.1.327")
	}
	if !strings.Contains(err.Error(), "2.1.311") {
		t.Fatalf("error = %v, want it to name 2.1.311", err)
	}
}

// TestEnvInitRefusesExistingPeersHome checks that env init leaves an existing
// Broker data directory untouched.
func TestEnvInitRefusesExistingPeersHome(t *testing.T) {
	base := t.TempDir()
	home := filepath.Join(base, "existing")
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	configPath := filepath.Join(home, "config.yaml")
	const original = "keep: me\n"
	if err := os.WriteFile(configPath, []byte(original), 0o600); err != nil {
		t.Fatal(err)
	}
	root := filepath.Join(base, "root")

	err := runEnvInit([]string{
		"--root", root,
		"--claude-bin", envTestClaude(t, "2.1.311 (Claude Code)"),
		"--peers-bin", envTestPeers(t),
		"--peers-home", home,
	}, io.Discard)
	if err == nil {
		t.Fatal("env init accepted an existing peers home")
	}
	if !strings.Contains(err.Error(), "refusing to touch existing") {
		t.Fatalf("error = %v, want a refusal", err)
	}
	if data, err := os.ReadFile(configPath); err != nil || string(data) != original {
		t.Fatalf("config.yaml changed: %q (%v)", data, err)
	}
	if _, err := os.Stat(root); !os.IsNotExist(err) {
		t.Errorf("env init modified the probe root despite refusing: %v", err)
	}
}

// TestRunWrapperStripsSessionEnv checks that the run wrapper drops inherited
// session variables and passes the token through.
func TestRunWrapperStripsSessionEnv(t *testing.T) {
	root, _ := envTestInit(t)

	cmd := exec.Command(filepath.Join(root, "run"), "/usr/bin/env")
	cmd.Env = []string{
		"PATH=/usr/local/bin:/usr/bin:/bin",
		"CLAUDE_CODE_MESSAGING_SOCKET=/x",
		"CLAUDE_CODE_CHILD_SESSION=1",
		"CLAUDE_CODE_OAUTH_TOKEN=tok-test-123",
	}
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("run wrapper: %v", err)
	}
	env := make(map[string]string)
	for _, line := range strings.Split(string(out), "\n") {
		if i := strings.IndexByte(line, '='); i > 0 {
			env[line[:i]] = line[i+1:]
		}
	}
	for _, leaked := range []string{"CLAUDE_CODE_MESSAGING_SOCKET", "CLAUDE_CODE_CHILD_SESSION"} {
		if _, ok := env[leaked]; ok {
			t.Errorf("wrapped environment contains %s", leaked)
		}
	}
	if got, want := env["CLAUDE_CONFIG_DIR"], filepath.Join(root, "cfg"); got != want {
		t.Errorf("CLAUDE_CONFIG_DIR = %q, want %q", got, want)
	}
	if got := env["CLAUDE_CODE_OAUTH_TOKEN"]; got != "tok-test-123" {
		t.Errorf("CLAUDE_CODE_OAUTH_TOKEN = %q, want it passed through", got)
	}
}

// TestEnvCheckNeverPrintsToken checks that env check reports token presence
// without printing its value.
func TestEnvCheckNeverPrintsToken(t *testing.T) {
	root, _ := envTestInit(t)

	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "tok-SECRET-123")
	var buf bytes.Buffer
	_ = runEnvCheck([]string{"--root", root}, &buf)
	if got := buf.String(); strings.Contains(got, "tok-SECRET-123") {
		t.Fatalf("env check printed the token:\n%s", got)
	}
	if got := buf.String(); !strings.Contains(got, "CLAUDE_CODE_OAUTH_TOKEN: set") {
		t.Fatalf("env check output = %q, want the token presence line", got)
	}

	t.Setenv("CLAUDE_CODE_OAUTH_TOKEN", "")
	buf.Reset()
	err := runEnvCheck([]string{"--root", root}, &buf)
	if err == nil {
		t.Fatal("env check succeeded without a token")
	}
	if !strings.Contains(err.Error(), "CLAUDE_CODE_OAUTH_TOKEN is not set") {
		t.Fatalf("error = %v, want it to report the missing token", err)
	}
	if got := buf.String(); strings.Contains(got, "tok-SECRET-123") {
		t.Fatalf("env check printed a token after it was unset:\n%s", got)
	}
}

// TestEnvLockHoldersSkipsSelf checks that the holder scan excludes the process
// that opened the lock to test it.
func TestEnvLockHoldersSkipsSelf(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.lock")
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		t.Fatalf("open %s: %v", path, err)
	}
	defer func() { _ = file.Close() }()

	if pids := envLockHolders(path, 0); !containsPID(pids, os.Getpid()) {
		t.Fatalf("envLockHolders = %v, want it to contain this process %d", pids, os.Getpid())
	}
	if pids := envLockHolders(path, os.Getpid()); containsPID(pids, os.Getpid()) {
		t.Fatalf("envLockHolders = %v, want this process %d excluded", pids, os.Getpid())
	}
}

// TestEnvLockHoldersFindsAnotherHolder checks that a child process holding the
// lock is reported as a holder.
func TestEnvLockHoldersFindsAnotherHolder(t *testing.T) {
	path := filepath.Join(t.TempDir(), "broker.lock")
	if err := os.WriteFile(path, nil, 0o600); err != nil {
		t.Fatalf("creating %s: %v", path, err)
	}

	cmd := exec.Command("/bin/sh", "-c", `exec 3>>"$1"; echo ready; read line`, "sh", path)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("opening the holder's stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("opening the holder's stdout: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the holder: %v", err)
	}
	defer func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "ready" {
		t.Fatalf("the holder did not report its open descriptor: line %q, err %v", line, err)
	}
	if pids := envLockHolders(path, os.Getpid()); !containsPID(pids, cmd.Process.Pid) {
		t.Fatalf("envLockHolders = %v, want it to contain the holder %d", pids, cmd.Process.Pid)
	}
}

func containsPID(pids []int, want int) bool {
	for _, pid := range pids {
		if pid == want {
			return true
		}
	}
	return false
}

// TestEnvLsofPIDsParsesTheHolderScan checks the lsof parser, including that a
// process with several descriptors appears once.
func TestEnvLsofPIDsParsesTheHolderScan(t *testing.T) {
	cases := []struct {
		name   string
		output string
		self   int
		want   []int
	}{
		{
			name:   "two holders",
			output: "p123\nn/tmp/broker.lock\np456\nn/tmp/broker.lock\n",
			want:   []int{123, 456},
		},
		{
			name:   "self excluded",
			output: "p123\nn/tmp/broker.lock\np456\nn/tmp/broker.lock\n",
			self:   123,
			want:   []int{456},
		},
		{
			name:   "one process with two descriptors",
			output: "p123\nn/tmp/broker.lock\nn/tmp/broker.lock\n",
			want:   []int{123},
		},
		{
			name:   "file field line is ignored",
			output: "p123\nf3\nn/tmp/broker.lock\n",
			want:   []int{123},
		},
		{
			name:   "no holders",
			output: "",
			want:   nil,
		},
		{
			name:   "malformed pid",
			output: "pabc\nn/tmp/broker.lock\n",
			want:   nil,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := envLsofPIDs(tc.output, tc.self); !reflect.DeepEqual(got, tc.want) {
				t.Errorf("envLsofPIDs = %v, want %v", got, tc.want)
			}
		})
	}
}

// TestEnvSameExecutableResolvesSymlinks checks that executables are compared
// after resolving symlinks, including /tmp against /private/tmp.
func TestEnvSameExecutableResolvesSymlinks(t *testing.T) {
	root := t.TempDir()
	exe := filepath.Join(root, "peers")
	if err := os.WriteFile(exe, nil, 0o755); err != nil {
		t.Fatalf("creating %s: %v", exe, err)
	}
	link := filepath.Join(root, "link")
	if err := os.Symlink(root, link); err != nil {
		t.Fatalf("linking %s to %s: %v", link, root, err)
	}
	if !envSameExecutable(filepath.Join(link, "peers"), exe) {
		t.Errorf("envSameExecutable did not resolve the symlinked root")
	}

	other := filepath.Join(root, "other")
	if err := os.WriteFile(other, nil, 0o755); err != nil {
		t.Fatalf("creating %s: %v", other, err)
	}
	if envSameExecutable(other, exe) {
		t.Errorf("envSameExecutable matched two different files")
	}
	if envSameExecutable("", exe) {
		t.Errorf("envSameExecutable matched an empty path")
	}

	// The probe root under /tmp and its canonical path under /private/tmp are
	// the same executable.
	tmpRoot, err := os.MkdirTemp("/tmp", "rsprobe-exe-")
	if err != nil {
		t.Fatalf("creating the /tmp root: %v", err)
	}
	t.Cleanup(func() { _ = os.RemoveAll(tmpRoot) })
	tmpExe := filepath.Join(tmpRoot, "peers")
	if err := os.WriteFile(tmpExe, nil, 0o755); err != nil {
		t.Fatalf("creating %s: %v", tmpExe, err)
	}
	canonicalRoot, err := filepath.EvalSymlinks(tmpRoot)
	if err != nil {
		t.Fatalf("resolving %s: %v", tmpRoot, err)
	}
	if !envSameExecutable(tmpExe, filepath.Join(canonicalRoot, "peers")) {
		t.Errorf("envSameExecutable did not match /tmp with its canonical path %s", canonicalRoot)
	}
}

// TestEnvBrokerLockFreeRejectsAForeignHolder checks that a lock held by another
// executable is reported as foreign.
func TestEnvBrokerLockFreeRejectsAForeignHolder(t *testing.T) {
	root := t.TempDir()
	if err := os.MkdirAll(filepath.Join(root, "bin"), 0o755); err != nil {
		t.Fatalf("creating the probe bin directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(root, "bin", "peers"), nil, 0o755); err != nil {
		t.Fatalf("creating the probe's peers: %v", err)
	}
	lockPath := filepath.Join(t.TempDir(), "broker.lock")
	if err := os.WriteFile(lockPath, nil, 0o600); err != nil {
		t.Fatalf("creating %s: %v", lockPath, err)
	}
	original := envBrokerLockPath
	envBrokerLockPath = func() string { return lockPath }
	t.Cleanup(func() { envBrokerLockPath = original })

	cmd := exec.Command(os.Args[0], "-test.run=^TestEnvBrokerLockHolderHelper$")
	cmd.Env = append(os.Environ(), "RSPROBE_LOCK_HOLDER_PATH="+lockPath)
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("opening the holder's stdin: %v", err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("opening the holder's stdout: %v", err)
	}
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting the holder: %v", err)
	}
	defer func() {
		_ = stdin.Close()
		_ = cmd.Process.Kill()
		_ = cmd.Wait()
	}()

	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || strings.TrimSpace(line) != "ready" {
		t.Fatalf("the holder did not report its lock: line %q, err %v", line, err)
	}
	ok, detail := envBrokerLockFree(root)
	if ok {
		t.Fatalf("envBrokerLockFree accepted a foreign holder: %s", detail)
	}
	if !strings.Contains(detail, fmt.Sprintf("broker.lock is held by pid %d", cmd.Process.Pid)) {
		t.Fatalf("detail = %q, want the holder's pid %d", detail, cmd.Process.Pid)
	}
}

// TestEnvBrokerLockHolderHelper holds the lock in a child process for
// TestEnvBrokerLockFreeRejectsAForeignHolder, and skips otherwise.
func TestEnvBrokerLockHolderHelper(t *testing.T) {
	path := os.Getenv("RSPROBE_LOCK_HOLDER_PATH")
	if path == "" {
		t.Skip("only runs as the parent's lock holder")
	}
	file, err := os.OpenFile(path, os.O_RDWR, 0)
	if err != nil {
		t.Fatalf("opening %s: %v", path, err)
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX); err != nil {
		t.Fatalf("locking %s: %v", path, err)
	}
	if _, err := fmt.Fprintln(os.Stdout, "ready"); err != nil {
		t.Fatalf("reporting the lock: %v", err)
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	_ = file.Close()
}

// TestEnvDarwinProbePIDs checks the ps parser selects only executables under
// R/bin and skips self.
func TestEnvDarwinProbePIDs(t *testing.T) {
	root := filepath.Join(string(os.PathSeparator), "private", "tmp", "peers-rs")
	prefix := filepath.Join(root, "bin") + string(os.PathSeparator)
	psOutput := strings.Join([]string{
		"  123 " + prefix + "claude --model haiku",
		"  124 /usr/bin/python3 -c x",
		"  125 " + prefix + "peers service run --ready-fd=3",
		"  126 " + prefix + "rsprobe watch --dir x",
		"  abc " + prefix + "claude",
		"  127 " + root + "binother/peers",
		"",
	}, "\n")
	got := envDarwinProbePIDs(psOutput, prefix, 125)
	want := []int{123, 126}
	if len(got) != len(want) {
		t.Fatalf("pids = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("pids[%d] = %d, want %d (all: %v)", i, got[i], want[i], got)
		}
	}
}

// TestUsageCountsDistinctMessageIDs checks that usage counts distinct assistant
// message ids per file and model.
func TestUsageCountsDistinctMessageIDs(t *testing.T) {
	root := t.TempDir()
	dir := filepath.Join(root, "cfg", "projects", "some-project")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	lines := []string{
		`{"type":"assistant","timestamp":"2026-11-19T01:00:00Z","message":{"id":"msg_1","model":"claude-x"}}`,
		`{"type":"assistant","timestamp":"2026-11-19T01:00:01Z","message":{"id":"msg_1","model":"claude-x"}}`,
		`{"type":"assistant","timestamp":"2026-11-19T01:00:02Z","message":{"id":"msg_1","model":"claude-x"}}`,
		`{"type":"assistant","timestamp":"2026-11-19T01:00:03Z","message":{"id":"msg_2","model":"claude-x"}}`,
		`{"type":"user","timestamp":"2026-11-19T01:00:04Z","message":{"id":"msg_3","model":"claude-x"}}`,
	}
	path := filepath.Join(dir, "session.jsonl")
	if err := os.WriteFile(path, []byte(strings.Join(lines, "\n")+"\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	rows, total, err := envCollectUsage(root, time.Time{})
	if err != nil {
		t.Fatal(err)
	}
	if total != 2 {
		t.Errorf("total = %d, want 2 distinct assistant message ids", total)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %+v, want one file/model group", rows)
	}
	if rows[0].count != 2 {
		t.Errorf("row count = %d, want 2 (a split message counts once)", rows[0].count)
	}
}

// TestCleanKeepsOut checks that env clean removes the probe root's contents but
// keeps out/.
func TestCleanKeepsOut(t *testing.T) {
	root := filepath.Join(t.TempDir(), "rs")
	home := filepath.Join(t.TempDir(), "home", ".peers")

	outFile := filepath.Join(root, "out", "x", "a.txt")
	if err := os.MkdirAll(filepath.Dir(outFile), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(outFile, []byte("keep\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	homeInRoot := filepath.Join(root, "home")
	if err := os.MkdirAll(homeInRoot, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(homeInRoot, "b.txt"), []byte("drop\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(home, "config.yaml"), []byte("broker: {}\n"), 0o600); err != nil {
		t.Fatal(err)
	}

	if err := runEnvClean([]string{"--root", root, "--peers-home", home}, io.Discard); err != nil {
		t.Fatalf("env clean: %v", err)
	}
	if _, err := os.Stat(outFile); err != nil {
		t.Errorf("clean removed the artifact directory: %v", err)
	}
	if _, err := os.Stat(homeInRoot); !os.IsNotExist(err) {
		t.Errorf("clean left R/home: %v", err)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Errorf("clean left the peers home: %v", err)
	}
	if _, err := os.Stat(root); err != nil {
		t.Errorf("clean removed the probe root: %v", err)
	}
}

// TestCleanPassesRSRootToCompose checks that env clean passes RS_ROOT to docker
// compose.
func TestCleanPassesRSRootToCompose(t *testing.T) {
	base := t.TempDir()

	// The clean path looks for test/realsession/compose.yaml relative to the
	// working directory, so the test runs from a scratch tree that holds it.
	compose := filepath.Join(base, "test", "realsession", "compose.yaml")
	if err := os.MkdirAll(filepath.Dir(compose), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(compose, []byte("services: {}\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	record := filepath.Join(base, "docker-env.txt")
	binDir := filepath.Join(base, "fakebin")
	if err := os.MkdirAll(binDir, 0o755); err != nil {
		t.Fatal(err)
	}
	script := "#!/bin/sh\nenv > '" + record + "'\n"
	if err := os.WriteFile(filepath.Join(binDir, "docker"), []byte(script), 0o755); err != nil {
		t.Fatal(err)
	}

	root := filepath.Join(base, "rs")
	home := filepath.Join(base, "peers-home")
	if err := os.MkdirAll(root, 0o755); err != nil {
		t.Fatal(err)
	}

	t.Setenv("PATH", binDir+string(os.PathListSeparator)+os.Getenv("PATH"))
	t.Chdir(base)

	if err := runEnvClean([]string{"--root", root, "--peers-home", home}, io.Discard); err != nil {
		t.Fatalf("env clean: %v", err)
	}
	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("fake docker did not run: %v", err)
	}
	got := ""
	for _, kv := range strings.Split(string(data), "\n") {
		if strings.HasPrefix(kv, "RS_ROOT=") {
			got = strings.TrimPrefix(kv, "RS_ROOT=")
		}
	}
	if got != root {
		t.Errorf("docker compose RS_ROOT = %q, want %q", got, root)
	}
}
