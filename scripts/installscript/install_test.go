package installscript

import (
	"context"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// installScriptPath is the script under test, relative to this package.
const installScriptPath = "../install.sh"

// systemPath is a fixed search path for the real tools the script needs. It
// deliberately omits the usual locations of claude, which live under $HOME or
// /usr/local, so a test that needs claude missing can drop the fake bin
// directory and still run every standard tool.
var systemPath = strings.Join([]string{"/usr/bin", "/bin", "/usr/sbin", "/sbin"}, string(os.PathListSeparator))

// fixture is one extracted archive plus the fake command directory and the
// environment the script runs with.
type fixture struct {
	t     *testing.T
	base  string
	home  string
	fakes string

	archive string
	script  string
	destdir string

	agentLog     string
	pathAgentLog string
	claudeLog    string
	claudeState  string
	xattrLog     string
	installLog   string
	orderLog     string
	groupLog     string
	sudoLog      string
	writeLog     string
}

// newFixture builds an archive root with install.sh and a fake peers binary, and
// a fake bin directory holding every command the tests replace.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	base := t.TempDir()
	f := &fixture{
		t:            t,
		base:         base,
		home:         filepath.Join(base, "home"),
		fakes:        filepath.Join(base, "fakebin"),
		archive:      filepath.Join(base, "archive"),
		destdir:      filepath.Join(base, "destdir"),
		agentLog:     filepath.Join(base, "agent.log"),
		pathAgentLog: filepath.Join(base, "path-agent.log"),
		claudeLog:    filepath.Join(base, "claude.log"),
		claudeState:  filepath.Join(base, "claude-state"),
		xattrLog:     filepath.Join(base, "xattr.log"),
		installLog:   filepath.Join(base, "install.log"),
		orderLog:     filepath.Join(base, "order.log"),
		groupLog:     filepath.Join(base, "group.log"),
		sudoLog:      filepath.Join(base, "sudo.log"),
		writeLog:     filepath.Join(base, "write.log"),
	}
	for _, dir := range []string{f.home, f.fakes, f.archive} {
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("creating %s: %v", dir, err)
		}
	}

	realInstall := mustLookPath(t, "install")
	realID := mustLookPath(t, "id")
	realUname := mustLookPath(t, "uname")
	realMktemp := mustLookPath(t, "mktemp")
	realChmod := mustLookPath(t, "chmod")
	realMv := mustLookPath(t, "mv")
	realCp := mustLookPath(t, "cp")
	realMkdir := mustLookPath(t, "mkdir")

	source, err := filepath.Abs(installScriptPath)
	if err != nil {
		t.Fatalf("resolving %s: %v", installScriptPath, err)
	}
	data, err := os.ReadFile(source)
	if err != nil {
		t.Fatalf("reading %s: %v", source, err)
	}
	f.script = filepath.Join(f.archive, "install.sh")
	writeExecutable(t, f.script, string(data))
	writeExecutable(t, filepath.Join(f.archive, "peers"), fakePeers)

	writeExecutable(t, filepath.Join(f.fakes, "claude"), fakeClaude)
	writeExecutable(t, filepath.Join(f.fakes, "peers"), fakePathPeers)
	writeExecutable(t, filepath.Join(f.fakes, "fish"), fakeFish)
	writeExecutable(t, filepath.Join(f.fakes, "stat"), fakeStat)
	writeExecutable(t, filepath.Join(f.fakes, "id"), strings.ReplaceAll(fakeID, "@REAL_ID@", shellQuote(realID)))
	writeExecutable(t, filepath.Join(f.fakes, "uname"), strings.ReplaceAll(fakeUname, "@REAL_UNAME@", shellQuote(realUname)))
	writeExecutable(t, filepath.Join(f.fakes, "xattr"), fakeXattr)
	writeExecutable(t, filepath.Join(f.fakes, "install"), strings.ReplaceAll(fakeInstall, "@REAL_INSTALL@", shellQuote(realInstall)))
	writeExecutable(t, filepath.Join(f.fakes, "sudo"), fakeSudo)
	writeExecutable(t, filepath.Join(f.fakes, "usermod"), fakeUsermod)
	writeExecutable(t, filepath.Join(f.fakes, "dseditgroup"), fakeDseditgroup)
	writeExecutable(t, filepath.Join(f.fakes, "mktemp"), strings.ReplaceAll(fakeMktemp, "@REAL_MKTEMP@", shellQuote(realMktemp)))
	writeExecutable(t, filepath.Join(f.fakes, "chmod"), strings.ReplaceAll(fakeChmod, "@REAL_CHMOD@", shellQuote(realChmod)))
	writeExecutable(t, filepath.Join(f.fakes, "mv"), strings.ReplaceAll(fakeMv, "@REAL_MV@", shellQuote(realMv)))
	writeExecutable(t, filepath.Join(f.fakes, "cp"), strings.ReplaceAll(fakeCp, "@REAL_CP@", shellQuote(realCp)))
	writeExecutable(t, filepath.Join(f.fakes, "mkdir"), strings.ReplaceAll(fakeMkdir, "@REAL_MKDIR@", shellQuote(realMkdir)))
	return f
}

// environ returns the environment the script runs with. overrides replace or
// add an environment variable; a nil map keeps the defaults.
func (f *fixture) environ(overrides map[string]string) []string {
	env := map[string]string{
		"HOME":                    f.home,
		"PATH":                    f.fakes + string(os.PathListSeparator) + systemPath,
		"LC_ALL":                  "C",
		"DESTDIR":                 f.destdir,
		"FAKE_AGENT_LOG":          f.agentLog,
		"FAKE_AGENT_VERSION":      "peers test-version",
		"FAKE_AGENT_SERVICE_EXIT": "0",
		"FAKE_PATH_AGENT_LOG":     f.pathAgentLog,
		"FAKE_CLAUDE_LOG":         f.claudeLog,
		"FAKE_CLAUDE_STATE":       f.claudeState,
		"FAKE_STAT_OWNER":         "0",
		"FAKE_STAT_MODE":          "755",
		"FAKE_STAT_REJECT_GNU":    "0",
		"FAKE_INSTALL_COPY":       "0",
		"FAKE_INSTALL_LOG":        f.installLog,
		"FAKE_XATTR_LOG":          f.xattrLog,
		"FAKE_XATTR_EXIT":         "0",
		"FAKE_ORDER_LOG":          f.orderLog,
		"FAKE_GROUP_LOG":          f.groupLog,
		"FAKE_GROUP_EXIT":         "0",
		"FAKE_SUDO_LOG":           f.sudoLog,
		"FAKE_WRITE_LOG":          f.writeLog,
	}
	for key, value := range overrides {
		env[key] = value
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	environ := make([]string, 0, len(keys))
	for _, key := range keys {
		environ = append(environ, key+"="+env[key])
	}
	return environ
}

// run executes the copied install.sh with the fixture environment in a new
// session without a controlling terminal. The level prompt then has no terminal
// to read, so a test that runs without --system or --user exercises the
// no-terminal path instead of blocking on the terminal that runs the tests.
func (f *fixture) run(overrides map[string]string, args ...string) (stdout, stderr string, code int) {
	f.t.Helper()
	cmd := exec.Command("sh", append([]string{f.script}, args...)...)
	cmd.Dir = f.archive
	cmd.Env = f.environ(overrides)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	var out, errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			f.t.Fatalf("running install.sh: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return out.String(), errOut.String(), code
}

// runDetached runs install.sh in a new session without a controlling terminal,
// so /dev/tty cannot be opened. stdin holds stdin for the script; a prompt that
// reads stdin instead of /dev/tty would see it.
func (f *fixture) runDetached(stdin string, overrides map[string]string, args ...string) (stdout, stderr string, code int) {
	f.t.Helper()
	cmd := exec.Command("sh", append([]string{f.script}, args...)...)
	cmd.Dir = f.archive
	cmd.Env = f.environ(overrides)
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	cmd.Stdin = strings.NewReader(stdin)
	var out, errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			f.t.Fatalf("running install.sh: %v", err)
		}
		code = exitErr.ExitCode()
	}
	return out.String(), errOut.String(), code
}

// ttyTimeout bounds one pseudo-terminal run. The install waits for the service
// to answer, and a loaded CI runner can take several seconds.
const ttyTimeout = time.Minute

// runWithTTY runs install.sh under a pseudo-terminal, so /dev/tty exists.
// answer is typed on that terminal while the script's stdin is /dev/null, which
// means the script can only learn the answer by reading /dev/tty. stdout and
// stderr go to files so they stay separate from the terminal conversation,
// which runWithTTY returns as tty.
func (f *fixture) runWithTTY(t *testing.T, answer string, overrides map[string]string, args ...string) (stdout, stderr, tty string, code int) {
	t.Helper()
	scriptPath, err := exec.LookPath("script")
	if err != nil {
		t.Fatalf("script(1) is required to run the prompt tests: %v", err)
	}

	outPath := filepath.Join(f.base, "tty-stdout")
	errPath := filepath.Join(f.base, "tty-stderr")
	statusPath := filepath.Join(f.base, "tty-status")
	inner := "sh " + shellQuote(f.script)
	for _, arg := range args {
		inner += " " + shellQuote(arg)
	}
	inner += " </dev/null >" + shellQuote(outPath) + " 2>" + shellQuote(errPath)
	inner += "; printf '%s' \"$?\" >" + shellQuote(statusPath)

	ctx, cancel := context.WithTimeout(t.Context(), ttyTimeout)
	defer cancel()
	var cmd *exec.Cmd
	if runtime.GOOS == "darwin" {
		cmd = exec.CommandContext(ctx, scriptPath, "-q", "/dev/null", "sh", "-c", inner)
	} else {
		cmd = exec.CommandContext(ctx, scriptPath, "-q", "-c", inner, "/dev/null")
	}
	cmd.Dir = f.archive
	cmd.Env = f.environ(overrides)
	cmd.Stdin = strings.NewReader(answer)
	var terminal strings.Builder
	cmd.Stdout = &terminal
	cmd.Stderr = &terminal
	// The exit status travels through statusPath because script(1) does not
	// report the command's status the same way on every platform.
	_ = cmd.Run()

	data, err := os.ReadFile(statusPath)
	if err != nil {
		t.Fatalf("install.sh recorded no exit status under the terminal: %v\nterminal output:\n%s", err, terminal.String())
	}
	code, err = strconv.Atoi(strings.TrimSpace(string(data)))
	if err != nil {
		t.Fatalf("parsing the recorded exit status %q: %v", data, err)
	}
	return readFile(t, outPath), readFile(t, errPath), terminal.String(), code
}

// systemPrefix creates the install directory a system test installs into.
func (f *fixture) systemPrefix(t *testing.T) string {
	t.Helper()
	prefix := filepath.Join(f.base, "sysbin")
	if err := os.MkdirAll(prefix, 0o755); err != nil {
		t.Fatalf("creating %s: %v", prefix, err)
	}
	return prefix
}

// systemInstallEnv returns overrides for a system install that passes the
// directory checks with the fake stat. sudoUser is set only when non-empty, and
// the fake group listing then contains just that account, so the account is not
// yet in the admin group.
func (f *fixture) systemInstallEnv(sudoUser string) map[string]string {
	env := map[string]string{
		"FAKE_ID_UID":       "0",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
		"FAKE_GROUP_LOG":    f.groupLog,
	}
	if sudoUser != "" {
		env["SUDO_USER"] = sudoUser
		env["FAKE_ID_GROUPS"] = sudoUser
	}
	return env
}

// systemInstallArgs returns the arguments for a system install that
// installs the service and skips MCP registration.
func systemInstallArgs(prefix string, extra ...string) []string {
	args := []string{"--system", "--no-mcp", "--prefix", prefix}
	return append(args, extra...)
}

// pathUnderHome returns an absolute path under the home directory as the
// script sees it. The user prefix keeps the caller's spelling rather
// than a resolved physical path, so PATH comparisons and registered command
// paths match what the user wrote.
func (f *fixture) pathUnderHome(t *testing.T, elems ...string) string {
	t.Helper()
	if !filepath.IsAbs(f.home) {
		t.Fatalf("the fixture home %s is not absolute", f.home)
	}
	return filepath.Join(append([]string{f.home}, elems...)...)
}

// userBinary returns the installed binary path for a user prefix, which
// keeps the caller's spelling.
func (f *fixture) userBinary(dir string) string {
	return filepath.Join(dir, "peers")
}

// installedBinary returns the installed binary path for a prefix directory
// that exists, resolving symlinks the same way the script does.
func (f *fixture) installedBinary(t *testing.T, dir string) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(dir)
	if err != nil {
		t.Fatalf("resolving %s: %v", dir, err)
	}
	return filepath.Join(resolved, "peers")
}

// resolvedArchive returns the archive root after the same symlink resolution
// the script applies to its own directory.
func (f *fixture) resolvedArchive(t *testing.T) string {
	t.Helper()
	resolved, err := filepath.EvalSymlinks(f.archive)
	if err != nil {
		t.Fatalf("resolving %s: %v", f.archive, err)
	}
	return resolved
}

func TestInstallScriptHelpAndUsageErrors(t *testing.T) {
	t.Run("help", func(t *testing.T) {
		f := newFixture(t)
		stdout, _, code := f.run(nil, "--help")
		if code != 0 {
			t.Fatalf("--help exit = %d, want 0", code)
		}
		for _, want := range []string{
			"--system", "--user", "--prefix <dir>", "--no-service", "--no-mcp",
			"--admin-group <name>", "--join-admin-group", "--no-admin-group",
			"peers uninstall",
		} {
			mustContain(t, "help output", stdout, want)
		}
	})

	t.Run("admin group without value", func(t *testing.T) {
		f := newFixture(t)
		_, stderr, code := f.run(nil, "--admin-group")
		if code != 2 {
			t.Fatalf("--admin-group without a value exit = %d, want 2", code)
		}
		mustContain(t, "stderr", stderr, "--admin-group needs a group name")
	})

	t.Run("empty admin group", func(t *testing.T) {
		f := newFixture(t)
		_, stderr, code := f.run(nil, "--admin-group=")
		if code != 2 {
			t.Fatalf("--admin-group= exit = %d, want 2", code)
		}
		mustContain(t, "stderr", stderr, "--admin-group needs a non-empty group name")
	})

	t.Run("admin group option without system", func(t *testing.T) {
		f := newFixture(t)
		_, stderr, code := f.runDetached("", nil, "--join-admin-group")
		if code != 2 {
			t.Fatalf("--join-admin-group without --system exit = %d, want 2", code)
		}
		mustContain(t, "stderr", stderr, "only valid with --system")
	})

	t.Run("unknown argument", func(t *testing.T) {
		f := newFixture(t)
		_, stderr, code := f.run(nil, "--bogus")
		if code != 2 {
			t.Fatalf("unknown argument exit = %d, want 2", code)
		}
		mustContain(t, "stderr", stderr, "unknown argument")
		mustContain(t, "stderr", stderr, "usage:")
	})

	t.Run("missing prefix value", func(t *testing.T) {
		f := newFixture(t)
		_, stderr, code := f.run(nil, "--prefix")
		if code != 2 {
			t.Fatalf("--prefix without a value exit = %d, want 2", code)
		}
		mustContain(t, "stderr", stderr, "--prefix needs a directory")
	})

	t.Run("empty prefix value", func(t *testing.T) {
		f := newFixture(t)
		_, stderr, code := f.run(nil, "--prefix=")
		if code != 2 {
			t.Fatalf("--prefix= exit = %d, want 2", code)
		}
		mustContain(t, "stderr", stderr, "--prefix needs a non-empty directory")
	})
}

func TestInstallScriptUserInstall(t *testing.T) {
	f := newFixture(t)
	stdout, stderr, code := f.run(nil)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	binary := f.pathUnderHome(t, ".local", "bin", "peers")
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("the installed binary is missing: %v", err)
	}

	agent := readLines(t, f.agentLog)
	if got := countLines(agent, "service install"); got != 1 {
		t.Errorf("service install calls = %d, want 1; calls: %q", got, agent)
	}
	if got := countLines(agent, "--version"); got != 1 {
		t.Errorf("--version calls = %d, want 1; calls: %q", got, agent)
	}
	if got := countLines(agent, "status"); got != 0 {
		t.Errorf("status calls = %d, want 0; the service summary reports the state; calls: %q", got, agent)
	}
	// The completions run after the service, so a failure there cannot undo an
	// installed service.
	if serviceAt, bashAt := indexOf(agent, "service install"), indexOf(agent, "completion bash"); serviceAt < 0 || bashAt < 0 || bashAt < serviceAt {
		t.Errorf("completion calls do not follow service install: %q", agent)
	}

	claude := readLines(t, f.claudeLog)
	if got := countLines(claude, "mcp get peer"); got != 1 {
		t.Errorf("mcp get calls = %d, want 1; calls: %q", got, claude)
	}
	if got := countLines(claude, "mcp add --scope user peer -- "+binary+" mcp"); got != 1 {
		t.Errorf("mcp add calls = %d, want 1 with %s; calls: %q", got, binary, claude)
	}

	mustContain(t, "stdout", stdout, "peers test-version installed (user service, starts at login)")
	mustContain(t, "stdout", stdout, "MCP:        registered")
	mustContain(t, "stdout", stdout, "Restart any Claude Code session that is already running so it loads the peer MCP server")
	mustContain(t, "stdout", stdout, binary[:strings.LastIndex(binary, "/")]+" is not on PATH")

	// The binary, version, service state and status belong to service install;
	// the script must not repeat them.
	mustNotContain(t, "stdout", stdout, "  version:")
	mustNotContain(t, "stdout", stdout, "  service:")
	mustNotContain(t, "stdout", stdout, "  status:")

	// The script must run the installed binary, never one found on PATH.
	if calls := readLines(t, f.pathAgentLog); len(calls) != 0 {
		t.Errorf("install.sh called peers from PATH: %q", calls)
	}
}

func TestInstallScriptUserCompletions(t *testing.T) {
	f := newFixture(t)
	stdout, stderr, code := f.run(nil, "--no-service", "--no-mcp")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	bash := f.pathUnderHome(t, ".local", "share", "bash-completion", "completions", "peers")
	fish := f.pathUnderHome(t, ".config", "fish", "completions", "peers.fish")
	for _, path := range []string{bash, fish} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the completion file %s is missing: %v", path, err)
		}
	}
	mustContain(t, "bash completion", readFile(t, bash), "# bash completion")
	mustContain(t, "fish completion", readFile(t, fish), "# fish completion")

	zsh := f.pathUnderHome(t, ".local", "share", "zsh", "site-functions", "_peers")
	if _, err := os.Stat(zsh); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the user install wrote the zsh completion %s: %v", zsh, err)
	}
	mustContain(t, "stdout", stdout, "the zsh completion was not installed")
	mustContain(t, "stdout", stdout, "completion zsh")

	if _, err := os.Stat(f.pathUnderHome(t, ".local", "share", "man")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the user install wrote man pages: %v", err)
	}

	agent := readLines(t, f.agentLog)
	for _, want := range []string{"completion bash", "completion fish"} {
		if got := countLines(agent, want); got != 1 {
			t.Errorf("%q calls = %d, want 1; calls: %q", want, got, agent)
		}
	}
	if got := countLines(agent, "completion zsh"); got != 0 {
		t.Errorf("completion zsh calls = %d, want none; calls: %q", got, agent)
	}
}

// TestInstallScriptSkipsFishWhenFishIsMissing checks that the fish completion
// and the fish configuration directory are not created when fish is absent.
func TestInstallScriptSkipsFishWhenFishIsMissing(t *testing.T) {
	f := newFixture(t)
	stdout, stderr, code := f.run(map[string]string{"PATH": f.pathWithout(t, "fish")},
		"--no-service", "--no-mcp")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	fish := f.pathUnderHome(t, ".config", "fish", "completions", "peers.fish")
	if _, err := os.Stat(fish); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the fish completion %s exists although fish is missing: %v", fish, err)
	}
	if _, err := os.Stat(f.pathUnderHome(t, ".config")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the fish configuration directory was created although fish is missing")
	}
	if got := countLines(readLines(t, f.agentLog), "completion fish"); got != 0 {
		t.Errorf("completion fish calls = %d, want none; calls: %q", got, readLines(t, f.agentLog))
	}
	bash := f.pathUnderHome(t, ".local", "share", "bash-completion", "completions", "peers")
	if _, err := os.Stat(bash); err != nil {
		t.Errorf("the bash completion is missing: %v", err)
	}
}

func TestInstallScriptSystemCompletionsAndManPages(t *testing.T) {
	f := newFixture(t)
	writeArchiveManPages(t, f)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("")
	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	for _, path := range []string{
		f.systemShare(t, "bash-completion", "completions", "peers"),
		f.systemShare(t, "zsh", "site-functions", "_peers"),
		f.systemShare(t, "fish", "vendor_completions.d", "peers.fish"),
		f.systemShare(t, "man", "man1", "peers.1"),
		f.systemShare(t, "man", "man1", "peers-update.1"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the installed file %s is missing: %v", path, err)
		}
	}
	mustContain(t, "zsh completion", readFile(t, f.systemShare(t, "zsh", "site-functions", "_peers")), "# zsh completion")
	mustContain(t, "man page", readFile(t, f.systemShare(t, "man", "man1", "peers.1")), ".TH PEERS")

	agent := readLines(t, f.agentLog)
	for _, want := range []string{"completion bash", "completion zsh", "completion fish"} {
		if got := countLines(agent, want); got != 1 {
			t.Errorf("%q calls = %d, want 1; calls: %q", want, got, agent)
		}
	}
	mustNotContain(t, "stdout", stdout, "the zsh completion was not installed")
}

// TestInstallScriptSystemSkipsFishWhenFishIsMissing checks that a system
// install without fish writes the other completions and the man pages, but
// neither the fish completion nor its configuration directory.
func TestInstallScriptSystemSkipsFishWhenFishIsMissing(t *testing.T) {
	f := newFixture(t)
	writeArchiveManPages(t, f)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("")
	env["PATH"] = f.pathWithout(t, "fish")
	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	for _, path := range []string{
		f.systemShare(t, "bash-completion", "completions", "peers"),
		f.systemShare(t, "zsh", "site-functions", "_peers"),
		f.systemShare(t, "man", "man1", "peers.1"),
	} {
		if _, err := os.Stat(path); err != nil {
			t.Errorf("the installed file %s is missing: %v", path, err)
		}
	}
	if _, err := os.Stat(f.systemShare(t, "fish")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the fish configuration directory was created although fish is missing: %v", err)
	}
	if got := countLines(readLines(t, f.agentLog), "completion fish"); got != 0 {
		t.Errorf("completion fish calls = %d, want none; calls: %q", got, readLines(t, f.agentLog))
	}
}

// TestInstallScriptWarnsAboutCompletionFailure checks that a completion that
// cannot be written is a warning: the service is already installed and the
// script still succeeds.
func TestInstallScriptWarnsAboutCompletionFailure(t *testing.T) {
	f := newFixture(t)
	stdout, stderr, code := f.run(map[string]string{"FAKE_AGENT_COMPLETION_EXIT": "1"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	mustContain(t, "stderr", stderr, "warning: could not write the bash completion")
	if got := countLines(readLines(t, f.agentLog), "service install"); got != 1 {
		t.Errorf("service install calls = %d, want 1; the failure must not undo the install", got)
	}
	mustContain(t, "stdout", stdout, "MCP:")
}

// TestInstallScriptWarnsAboutManPageFailure checks the same for the man pages:
// a directory that cannot be created only produces a warning.
func TestInstallScriptWarnsAboutManPageFailure(t *testing.T) {
	f := newFixture(t)
	writeArchiveManPages(t, f)
	// A regular file where the man directory belongs is refused, not written
	// through.
	manDir := filepath.Join(f.destdir, "usr", "local", "share", "man", "man1")
	if err := os.MkdirAll(filepath.Dir(manDir), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(manDir, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	prefix := f.systemPrefix(t)
	stdout, stderr, code := f.run(f.systemInstallEnv(""), systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, want := range []string{"skipped the man page peers.1", "is not a directory", "install it with:"} {
		mustContain(t, "stderr", stderr, want)
	}
}

// TestInstallScriptSystemCompletionReplacesSymlink checks that a system install
// replaces a symlink at the completion target instead of writing through it.
// The directory may be owned by another account, so a symlink there would
// otherwise let that account truncate any file the install can write.
func TestInstallScriptSystemCompletionReplacesSymlink(t *testing.T) {
	f := newFixture(t)
	zshDir := f.systemShare(t, "zsh", "site-functions")
	if err := os.MkdirAll(zshDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshDir, err)
	}
	victim := filepath.Join(f.base, "symlink-target")
	const victimContent = "the install must not rewrite this file\n"
	if err := os.WriteFile(victim, []byte(victimContent), 0o644); err != nil {
		t.Fatalf("writing %s: %v", victim, err)
	}
	target := filepath.Join(zshDir, "_peers")
	if err := os.Symlink(victim, target); err != nil {
		t.Fatalf("creating the symlink %s: %v", target, err)
	}

	prefix := f.systemPrefix(t)
	_, stderr, code := f.run(f.systemInstallEnv(""), systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}

	if got := readFile(t, victim); got != victimContent {
		t.Errorf("the symlink target was rewritten: %q", got)
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatalf("reading %s: %v", target, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Errorf("%s is still a symlink; the completion must replace the link itself", target)
	}
	mustContain(t, "zsh completion", readFile(t, target), "# zsh completion")
}

// TestInstallScriptSystemWritesInSudoUserDirectoryAsTheAccount checks that a
// completion directory owned by the account that ran sudo is written by that
// account through sudo -u: mktemp, the copy of the generated content, chmod and
// mv all run as the account, and root runs none of them in the directory. A
// process of the account that swapped a temporary file for a link therefore
// cannot make root write through it.
func TestInstallScriptSystemWritesInSudoUserDirectoryAsTheAccount(t *testing.T) {
	f := newFixture(t)
	zshDir := f.systemShare(t, "zsh", "site-functions")
	if err := os.MkdirAll(zshDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshDir, err)
	}
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_ID_UID_FOR_NAME"] = "2100"
	env["FAKE_STAT_UNSAFE_PATH"] = zshDir
	env["FAKE_STAT_UNSAFE_OWNER"] = "2100"
	env["FAKE_STAT_UNSAFE_MODE"] = "755"

	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	mustContain(t, "zsh completion", readFile(t, filepath.Join(zshDir, "_peers")), "# zsh completion")

	seen := map[string]bool{}
	for _, line := range readLines(t, f.writeLog) {
		if !strings.Contains(line, zshDir) {
			continue
		}
		if !strings.Contains(line, "as=alice") {
			t.Errorf("root operated in the account's directory: %q", line)
		}
		for _, op := range []string{"mktemp", "cp", "chmod", "mv"} {
			if strings.HasPrefix(line, op+" ") {
				seen[op] = true
			}
		}
	}
	for _, op := range []string{"mktemp", "cp", "chmod", "mv"} {
		if !seen[op] {
			t.Errorf("no %s call for %s ran as the account; log: %q", op, zshDir, readLines(t, f.writeLog))
		}
	}
}

// TestInstallScriptSystemCreatesSudoUserDirectoryAsTheAccount checks that a
// missing completion directory under one owned by the account that ran sudo is
// created by that account, so root never creates a directory inside it.
func TestInstallScriptSystemCreatesSudoUserDirectoryAsTheAccount(t *testing.T) {
	f := newFixture(t)
	zshParent := f.systemShare(t, "zsh")
	if err := os.MkdirAll(zshParent, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshParent, err)
	}
	zshDir := filepath.Join(zshParent, "site-functions")
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_ID_UID_FOR_NAME"] = "2100"
	env["FAKE_STAT_UNSAFE_PATH"] = zshParent
	env["FAKE_STAT_UNSAFE_OWNER"] = "2100"
	env["FAKE_STAT_UNSAFE_MODE"] = "755"

	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	created := false
	for _, line := range readLines(t, f.writeLog) {
		if !strings.HasPrefix(line, "mkdir ") || !strings.Contains(line, zshDir) {
			continue
		}
		created = true
		if !strings.Contains(line, "as=alice") {
			t.Errorf("root created the directory in the account's tree: %q", line)
		}
	}
	if !created {
		t.Errorf("no mkdir call for %s was logged; log: %q", zshDir, readLines(t, f.writeLog))
	}
}

// TestInstallScriptSystemCreatesNestedDirectoryAsTheAccount checks that a
// missing directory below an ancestor owned by the account that ran sudo is
// created by that account even when the immediate parent is root-owned. The
// account owns share but not zsh, so after zsh passes its check the account can
// replace zsh with a link to a root-owned directory. The link is put in place
// just before the mkdir: root's mkdir would create site-functions inside the
// root-owned directory, while the account's own mkdir fails there.
func TestInstallScriptSystemCreatesNestedDirectoryAsTheAccount(t *testing.T) {
	f := newFixture(t)
	zshParent := f.systemShare(t, "zsh")
	if err := os.MkdirAll(zshParent, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshParent, err)
	}
	shareDir := f.systemShare(t)
	rootDir := filepath.Join(f.base, "root-owned")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", rootDir, err)
	}
	zshDir := filepath.Join(zshParent, "site-functions")
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_ID_UID_FOR_NAME"] = "2100"
	env["FAKE_STAT_UNSAFE_PATH"] = shareDir
	env["FAKE_STAT_UNSAFE_OWNER"] = "2100"
	env["FAKE_STAT_UNSAFE_MODE"] = "755"
	env["FAKE_MKDIR_SWAP_DIR"] = zshParent
	env["FAKE_MKDIR_SWAP_TARGET"] = rootDir
	env["FAKE_MKDIR_REFUSE_DROPPED"] = "1"

	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		t.Fatalf("reading %s: %v", rootDir, err)
	}
	if len(entries) != 0 {
		t.Errorf("the install created %v in the root-owned directory through the swapped parent", entries)
	}
	mustContain(t, "stderr", stderr, "skipped the zsh completion")
	attempted := false
	for _, line := range readLines(t, f.writeLog) {
		if !strings.HasPrefix(line, "mkdir ") || !strings.Contains(line, zshDir) {
			continue
		}
		attempted = true
		if !strings.Contains(line, "as=alice") {
			t.Errorf("root created the directory below the account's ancestor: %q", line)
		}
	}
	if !attempted {
		t.Errorf("no mkdir call for %s was logged; log: %q", zshDir, readLines(t, f.writeLog))
	}
}

// TestInstallScriptSystemDoesNotWriteThroughSwappedTemporaryFile reproduces the
// write race: after mktemp the temporary file is replaced with a link to a
// victim, which a process of the account can do because it owns the directory.
// With the write run as that account the victim is untouched; root writing
// through the link would truncate it.
func TestInstallScriptSystemDoesNotWriteThroughSwappedTemporaryFile(t *testing.T) {
	f := newFixture(t)
	zshDir := f.systemShare(t, "zsh", "site-functions")
	if err := os.MkdirAll(zshDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshDir, err)
	}
	victim := filepath.Join(f.base, "victim")
	const victimContent = "the install must not truncate this file\n"
	if err := os.WriteFile(victim, []byte(victimContent), 0o644); err != nil {
		t.Fatalf("writing %s: %v", victim, err)
	}

	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_ID_UID_FOR_NAME"] = "2100"
	env["FAKE_STAT_UNSAFE_PATH"] = zshDir
	env["FAKE_STAT_UNSAFE_OWNER"] = "2100"
	env["FAKE_STAT_UNSAFE_MODE"] = "755"
	env["FAKE_MKTEMP_SYMLINK"] = victim
	env["FAKE_MKTEMP_SYMLINK_DIR"] = zshDir
	env["FAKE_CP_REFUSE_SYMLINK"] = "1"

	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if got := readFile(t, victim); got != victimContent {
		t.Errorf("the swapped temporary file let the install rewrite the victim: %q", got)
	}
	mustContain(t, "stderr", stderr, "could not write the zsh completion")
}

// TestInstallScriptSystemDoesNotWriteThroughSwappedAncestor reproduces the
// ancestor-symlink attack. The account that ran sudo owns an intermediate
// directory, so it can replace the completion directory with a link to a
// root-owned directory right after the install checks the path. A second stat
// of the target would follow the link and read root as the owner, and root
// would then create and rename files inside the root-owned directory. The
// install must carry the result of the first check into the write, write as
// the account, and leave the root-owned directory untouched.
func TestInstallScriptSystemDoesNotWriteThroughSwappedAncestor(t *testing.T) {
	f := newFixture(t)
	zshDir := f.systemShare(t, "zsh", "site-functions")
	if err := os.MkdirAll(zshDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshDir, err)
	}
	// The account owns the parent of the completion directory, which is what
	// lets it replace the directory itself; the directory is reported as
	// root-owned, as it is on the Homebrew layout.
	zshParent := f.systemShare(t, "zsh")
	rootDir := filepath.Join(f.base, "root-dir")
	if err := os.MkdirAll(rootDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", rootDir, err)
	}
	victim := filepath.Join(rootDir, "_peers")
	const victimContent = "the install must not overwrite this root-owned file\n"
	if err := os.WriteFile(victim, []byte(victimContent), 0o644); err != nil {
		t.Fatalf("writing %s: %v", victim, err)
	}

	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_ID_UID_FOR_NAME"] = "2100"
	env["FAKE_STAT_UNSAFE_PATH"] = zshParent
	env["FAKE_STAT_UNSAFE_OWNER"] = "2100"
	env["FAKE_STAT_UNSAFE_MODE"] = "755"
	// When a mktemp runs against the completion directory, the account has
	// already swapped it for a link to the root-owned directory. The account's
	// own mktemp cannot create a file there and fails; root must never run the
	// mktemp that would follow the link.
	env["FAKE_MKTEMP_SWAP_DIR"] = zshDir
	env["FAKE_MKTEMP_SWAP_TARGET"] = rootDir
	env["FAKE_MKTEMP_REFUSE_DROPPED"] = "1"

	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	if got := readFile(t, victim); got != victimContent {
		t.Errorf("the root-owned file was overwritten through the swapped ancestor: %q", got)
	}
	entries, err := os.ReadDir(rootDir)
	if err != nil {
		t.Fatalf("reading %s: %v", rootDir, err)
	}
	if len(entries) != 1 || entries[0].Name() != "_peers" {
		t.Errorf("the install created files in the root-owned directory: %v", entries)
	}
	mustContain(t, "stderr", stderr, "could not write the zsh completion")
}

// TestInstallScriptRemovesStagingFileOnInterrupt checks that an interrupted
// system install does not leave its root-owned staging file behind. The
// completion is generated into a temporary file under /tmp; the fake peers
// signals the installer after that file is created, and the fake mktemp puts
// the staging file in a directory the test can inspect instead of /tmp.
func TestInstallScriptRemovesStagingFileOnInterrupt(t *testing.T) {
	f := newFixture(t)
	zshDir := f.systemShare(t, "zsh", "site-functions")
	if err := os.MkdirAll(zshDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshDir, err)
	}
	stageDir := filepath.Join(f.base, "stage")
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", stageDir, err)
	}

	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_ID_UID_FOR_NAME"] = "2100"
	env["FAKE_STAT_UNSAFE_PATH"] = zshDir
	env["FAKE_STAT_UNSAFE_OWNER"] = "2100"
	env["FAKE_STAT_UNSAFE_MODE"] = "755"
	env["FAKE_MKTEMP_STAGE_DIR"] = stageDir
	env["FAKE_PEERS_SIGNAL"] = "TERM"
	env["FAKE_PEERS_SIGNAL_SHELL"] = "zsh"

	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 143 {
		t.Fatalf("exit = %d, want 143 from the SIGTERM trap\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	entries, err := os.ReadDir(stageDir)
	if err != nil {
		t.Fatalf("reading %s: %v", stageDir, err)
	}
	if len(entries) != 0 {
		t.Errorf("the interrupt left staging files behind: %v", entries)
	}
}

// TestInstallScriptDoesNotRemoveARecreatedStagingPath checks that the install
// clears the staging path once it has removed the file. Otherwise the exit trap
// removes whatever another process creates at that path between the removal and
// the exit. The chmod of the completion's temporary file runs right after the
// removal and recreates a file at the recorded path; the trap must leave it.
func TestInstallScriptDoesNotRemoveARecreatedStagingPath(t *testing.T) {
	f := newFixture(t)
	zshDir := f.systemShare(t, "zsh", "site-functions")
	if err := os.MkdirAll(zshDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshDir, err)
	}
	stageDir := filepath.Join(f.base, "stage")
	if err := os.MkdirAll(stageDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", stageDir, err)
	}
	stageLog := filepath.Join(f.base, "stage-path")

	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_ID_UID_FOR_NAME"] = "2100"
	env["FAKE_STAT_UNSAFE_PATH"] = zshDir
	env["FAKE_STAT_UNSAFE_OWNER"] = "2100"
	env["FAKE_STAT_UNSAFE_MODE"] = "755"
	env["FAKE_MKTEMP_STAGE_DIR"] = stageDir
	env["FAKE_STAGE_PATH_LOG"] = stageLog
	env["FAKE_CHMOD_RESTORE_STAGE"] = "1"

	stdout, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	stage := strings.TrimSpace(readFile(t, stageLog))
	if stage == "" {
		t.Fatalf("the fake mktemp recorded no staging path")
	}
	content, err := os.ReadFile(stage)
	if err != nil {
		t.Fatalf("the exit trap removed the file recreated at the staging path %s: %v", stage, err)
	}
	mustContain(t, "recreated file", string(content), "recreated after the install removed the staging file")
}

// TestInstallScriptSystemSkipsCompletionThroughSymlinkedParent checks that a
// symbolic link in the install path is refused before any directory is created
// through it.
func TestInstallScriptSystemSkipsCompletionThroughSymlinkedParent(t *testing.T) {
	f := newFixture(t)
	shareDir := filepath.Dir(f.systemShare(t, "zsh"))
	if err := os.MkdirAll(shareDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", shareDir, err)
	}
	elsewhere := filepath.Join(f.base, "elsewhere")
	if err := os.MkdirAll(elsewhere, 0o755); err != nil {
		t.Fatalf("creating %s: %v", elsewhere, err)
	}
	if err := os.Symlink(elsewhere, filepath.Join(shareDir, "zsh")); err != nil {
		t.Fatalf("creating the symlinked parent: %v", err)
	}

	prefix := f.systemPrefix(t)
	stdout, stderr, code := f.run(f.systemInstallEnv(""), systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	for _, want := range []string{"skipped the zsh completion", "is a symbolic link", "install it with:"} {
		mustContain(t, "stderr", stderr, want)
	}
	entries, err := os.ReadDir(elsewhere)
	if err != nil {
		t.Fatalf("reading %s: %v", elsewhere, err)
	}
	if len(entries) != 0 {
		t.Errorf("the install created %v through the symlinked parent", entries)
	}
	if _, err := os.Stat(f.systemShare(t, "bash-completion", "completions", "peers")); err != nil {
		t.Errorf("the bash completion is missing although only the zsh tree is unsafe: %v", err)
	}
}

// TestInstallScriptSystemCompletionReplacesSymlinkToDirectory checks the same
// replacement for a symlink that points at a directory, which mv would
// otherwise follow and move the completed file inside.
func TestInstallScriptSystemCompletionReplacesSymlinkToDirectory(t *testing.T) {
	f := newFixture(t)
	zshDir := f.systemShare(t, "zsh", "site-functions")
	if err := os.MkdirAll(zshDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", zshDir, err)
	}
	elsewhere := filepath.Join(f.base, "elsewhere")
	if err := os.MkdirAll(elsewhere, 0o755); err != nil {
		t.Fatalf("creating %s: %v", elsewhere, err)
	}
	target := filepath.Join(zshDir, "_peers")
	if err := os.Symlink(elsewhere, target); err != nil {
		t.Fatalf("creating the symlink %s: %v", target, err)
	}

	prefix := f.systemPrefix(t)
	_, stderr, code := f.run(f.systemInstallEnv(""), systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	entries, err := os.ReadDir(elsewhere)
	if err != nil {
		t.Fatalf("reading %s: %v", elsewhere, err)
	}
	if len(entries) != 0 {
		t.Errorf("the completion was moved into the linked directory: %v", entries)
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatalf("reading %s: %v", target, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Errorf("%s is still a symlink; the completion must replace the link itself", target)
	}
	mustContain(t, "zsh completion", readFile(t, target), "# zsh completion")
}

// TestInstallScriptSystemCompletionSkipsDirectoryTarget checks that a directory
// at the completion target is refused with a warning instead of receiving the
// renamed temporary file.
func TestInstallScriptSystemCompletionSkipsDirectoryTarget(t *testing.T) {
	f := newFixture(t)
	target := filepath.Join(f.systemShare(t, "zsh", "site-functions"), "_peers")
	if err := os.MkdirAll(target, 0o755); err != nil {
		t.Fatalf("creating %s: %v", target, err)
	}
	sentinel := filepath.Join(target, "keep")
	if err := os.WriteFile(sentinel, []byte("keep\n"), 0o644); err != nil {
		t.Fatalf("writing %s: %v", sentinel, err)
	}

	prefix := f.systemPrefix(t)
	_, stderr, code := f.run(f.systemInstallEnv(""), systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	for _, want := range []string{"skipped the zsh completion", target + " is a directory", "install it with:"} {
		mustContain(t, "stderr", stderr, want)
	}
	entries, err := os.ReadDir(target)
	if err != nil {
		t.Fatalf("reading %s: %v", target, err)
	}
	if len(entries) != 1 || entries[0].Name() != "keep" {
		t.Errorf("the completion was moved into the target directory: %v", entries)
	}
	if _, err := os.Stat(f.systemShare(t, "bash-completion", "completions", "peers")); err != nil {
		t.Errorf("the bash completion is missing although only the zsh target is a directory: %v", err)
	}
}

// TestInstallScriptSystemCompletionFailureKeepsTarget checks that a completion
// that cannot be generated leaves the existing file alone and removes its
// temporary file.
func TestInstallScriptSystemCompletionFailureKeepsTarget(t *testing.T) {
	f := newFixture(t)
	bashDir := f.systemShare(t, "bash-completion", "completions")
	if err := os.MkdirAll(bashDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", bashDir, err)
	}
	target := filepath.Join(bashDir, "peers")
	const original = "# completion from an earlier install\n"
	if err := os.WriteFile(target, []byte(original), 0o644); err != nil {
		t.Fatalf("writing %s: %v", target, err)
	}

	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("")
	env["FAKE_AGENT_COMPLETION_EXIT"] = "1"
	_, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	mustContain(t, "stderr", stderr, "warning: could not write the bash completion")
	if got := readFile(t, target); got != original {
		t.Errorf("the failed completion run changed the target: %q", got)
	}
	entries, err := os.ReadDir(bashDir)
	if err != nil {
		t.Fatalf("reading %s: %v", bashDir, err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".peers-") {
			t.Errorf("a temporary file %s was left behind", entry.Name())
		}
	}
}

// TestInstallScriptSystemSkipsWritableCompletionDirectory checks that a
// completion directory writable by group or others is left alone with a
// warning, while the other completions are installed.
func TestInstallScriptSystemSkipsWritableCompletionDirectory(t *testing.T) {
	f := newFixture(t)
	bashDir := f.systemShare(t, "bash-completion", "completions")
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("")
	env["FAKE_STAT_UNSAFE_PATH"] = bashDir
	env["FAKE_STAT_UNSAFE_OWNER"] = "0"
	env["FAKE_STAT_UNSAFE_MODE"] = "775"
	_, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	for _, want := range []string{"skipped the bash completion", bashDir + " is writable by group or others", "install it with:"} {
		mustContain(t, "stderr", stderr, want)
	}
	if _, err := os.Stat(filepath.Join(bashDir, "peers")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the completion was written into a writable directory: %v", err)
	}
	if _, err := os.Stat(f.systemShare(t, "zsh", "site-functions", "_peers")); err != nil {
		t.Errorf("the zsh completion is missing although only the bash directory is unsafe: %v", err)
	}
}

// TestInstallScriptSystemSkipsWritableManDirectory checks that the man pages
// get the same directory check as the completions.
func TestInstallScriptSystemSkipsWritableManDirectory(t *testing.T) {
	f := newFixture(t)
	writeArchiveManPages(t, f)
	manDir := f.systemShare(t, "man", "man1")
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("")
	env["FAKE_STAT_UNSAFE_PATH"] = manDir
	env["FAKE_STAT_UNSAFE_OWNER"] = "0"
	env["FAKE_STAT_UNSAFE_MODE"] = "777"
	_, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	for _, want := range []string{"skipped the man page peers.1", manDir + " is writable by group or others", "install it with:"} {
		mustContain(t, "stderr", stderr, want)
	}
	if _, err := os.Stat(filepath.Join(manDir, "peers.1")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the man page was installed into a writable directory: %v", err)
	}
}

// TestInstallScriptSystemChecksShareDirectoriesWithBSDStat checks that the
// directory check for the completions falls back to the BSD stat form, like the
// binary's directory check does.
func TestInstallScriptSystemChecksShareDirectoriesWithBSDStat(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("")
	env["FAKE_STAT_REJECT_GNU"] = "1"
	_, stderr, code := f.run(env, systemInstallArgs(prefix, "--no-service")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0; the BSD stat fallback did not work\nstderr:\n%s", code, stderr)
	}
	if _, err := os.Stat(f.systemShare(t, "zsh", "site-functions", "_peers")); err != nil {
		t.Errorf("the zsh completion is missing: %v", err)
	}
}

// TestInstallScriptUserCompletionReplacesSymlink checks that a user install
// also replaces a symlink at the completion target rather than writing through
// it; the user install has no owner check.
func TestInstallScriptUserCompletionReplacesSymlink(t *testing.T) {
	f := newFixture(t)
	bashDir := f.pathUnderHome(t, ".local", "share", "bash-completion", "completions")
	if err := os.MkdirAll(bashDir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", bashDir, err)
	}
	victim := filepath.Join(f.base, "user-symlink-target")
	const victimContent = "the user install must not rewrite this file\n"
	if err := os.WriteFile(victim, []byte(victimContent), 0o644); err != nil {
		t.Fatalf("writing %s: %v", victim, err)
	}
	target := filepath.Join(bashDir, "peers")
	if err := os.Symlink(victim, target); err != nil {
		t.Fatalf("creating the symlink %s: %v", target, err)
	}

	_, stderr, code := f.run(nil, "--no-service", "--no-mcp")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	if got := readFile(t, victim); got != victimContent {
		t.Errorf("the symlink target was rewritten: %q", got)
	}
	info, err := os.Lstat(target)
	if err != nil {
		t.Fatalf("reading %s: %v", target, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		t.Errorf("%s is still a symlink; the completion must replace the link itself", target)
	}
	mustContain(t, "bash completion", readFile(t, target), "# bash completion")
}

// writeArchiveManPages adds the man pages a release archive carries to the
// fixture's archive directory.
func writeArchiveManPages(t *testing.T, f *fixture) {
	t.Helper()
	dir := filepath.Join(f.archive, "man")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	for _, name := range []string{"peers.1", "peers-update.1"} {
		content := ".TH " + strings.ToUpper(strings.TrimSuffix(name, ".1")) + " 1\n"
		if err := os.WriteFile(filepath.Join(dir, name), []byte(content), 0o644); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
}

// systemShare returns a path under the staged system share directory.
func (f *fixture) systemShare(t *testing.T, elems ...string) string {
	t.Helper()
	return filepath.Join(append([]string{f.destdir, "usr", "local", "share"}, elems...)...)
}

func TestInstallScriptUserInstallRejectsRoot(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(map[string]string{"FAKE_ID_UID": "0"}, "--user")
	if code != 1 {
		t.Fatalf("--user as root exited %d, want 1\nstderr:\n%s", code, stderr)
	}
	mustContain(t, "stderr", stderr, "must not run as root")
	mustContain(t, "stderr", stderr, "drop --user")

	if got := readLines(t, f.installLog); len(got) != 0 {
		t.Errorf("the binary was installed before the root check failed: %q", got)
	}
	if got := readLines(t, f.agentLog); len(got) != 0 {
		t.Errorf("the binary ran before the root check failed: %q", got)
	}
	if got := readLines(t, f.claudeLog); len(got) != 0 {
		t.Errorf("claude ran before the root check failed: %q", got)
	}
	if _, err := os.Stat(filepath.Join(f.home, ".local")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the root check left files under $HOME: %v", err)
	}
}

func TestInstallScriptCustomPrefix(t *testing.T) {
	f := newFixture(t)
	prefix := filepath.Join(f.base, "custom", "bin")
	stdout, stderr, code := f.run(nil, "--prefix", prefix, "--no-service")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	binary := f.userBinary(prefix)
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("the installed binary is missing: %v", err)
	}
	if _, err := os.Stat(filepath.Join(f.home, ".local", "bin", "peers")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the default prefix was used as well: %v", err)
	}
	claude := readLines(t, f.claudeLog)
	if got := countLines(claude, "mcp add --scope user peer -- "+binary+" mcp"); got != 1 {
		t.Errorf("mcp add calls = %d, want 1 with %s; calls: %q", got, binary, claude)
	}
	mustContain(t, "stdout", stdout, "peers test-version installed (no service)")
	mustContain(t, "stdout", stdout, "binary:     "+binary)
}

func TestInstallScriptNoPathHintWhenPrefixIsOnPath(t *testing.T) {
	f := newFixture(t)
	prefix := f.pathUnderHome(t, ".local", "bin")
	path := f.fakes + string(os.PathListSeparator) + prefix + string(os.PathListSeparator) + systemPath
	stdout, stderr, code := f.run(map[string]string{"PATH": path})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	if strings.Contains(stdout+stderr, "is not on PATH") {
		t.Errorf("output mentions PATH although %s is already on it:\n%s\n%s", prefix, stdout, stderr)
	}
}

func TestInstallScriptClaudeMissing(t *testing.T) {
	f := newFixture(t)
	// Without the fake bin directory there is no claude on PATH. The real
	// install still runs from the system directories.
	stdout, stderr, code := f.run(map[string]string{"PATH": systemPath})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	binary := f.pathUnderHome(t, ".local", "bin", "peers")
	combined := stdout + stderr
	mustContain(t, "output", combined, "claude mcp add --scope user peer -- "+binary+" mcp")
	mustContain(t, "stdout", stdout, "MCP:        skipped (claude not found)")
	if calls := readLines(t, f.claudeLog); len(calls) != 0 {
		t.Errorf("claude was called although it is missing: %q", calls)
	}
}

func TestInstallScriptClaudeAlreadyRegistered(t *testing.T) {
	f := newFixture(t)
	binary := f.pathUnderHome(t, ".local", "bin", "peers")
	writeRegistered(t, f.claudeState, binary)

	stdout, stderr, code := f.run(nil)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	if got := countLines(readLines(t, f.claudeLog), "mcp add"); got != 0 {
		t.Errorf("mcp add calls = %d, want 0", got)
	}
	mustContain(t, "stdout", stdout, "already registered")
	if strings.Contains(stdout, "different command") {
		t.Errorf("stdout reports a different command for the same path:\n%s", stdout)
	}
}

func TestInstallScriptClaudeRegisteredElsewhere(t *testing.T) {
	f := newFixture(t)
	writeRegistered(t, f.claudeState, "/opt/other/peers")

	stdout, stderr, code := f.run(nil)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	if got := countLines(readLines(t, f.claudeLog), "mcp add"); got != 0 {
		t.Errorf("mcp add calls = %d, want 0; the existing registration must stay untouched", got)
	}
	mustContain(t, "stdout", stdout, "/opt/other/peers")
	mustContain(t, "stdout", stdout, "leaving it unchanged")
	mustContain(t, "stdout", stdout, "claude mcp remove -s user peer")
}

func TestInstallScriptRegistersOnceAcrossRuns(t *testing.T) {
	f := newFixture(t)
	for run := 1; run <= 2; run++ {
		stdout, stderr, code := f.run(nil)
		if code != 0 {
			t.Fatalf("run %d exit = %d, want 0\nstderr:\n%s", run, code, stderr)
		}
		if run == 2 {
			mustContain(t, "stdout on the second run", stdout, "already registered")
		}
	}
	if got := countLines(readLines(t, f.claudeLog), "mcp add"); got != 1 {
		t.Errorf("mcp add calls across two runs = %d, want 1; calls: %q", got, readLines(t, f.claudeLog))
	}
}

func TestInstallScriptNoServiceNoMCP(t *testing.T) {
	f := newFixture(t)
	stdout, stderr, code := f.run(nil, "--no-service", "--no-mcp")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	agent := readLines(t, f.agentLog)
	if got := countLines(agent, "service install"); got != 0 {
		t.Errorf("service install calls = %d, want 0; calls: %q", got, agent)
	}
	if got := countLines(agent, "status"); got != 0 {
		t.Errorf("status calls = %d, want 0; calls: %q", got, agent)
	}
	if calls := readLines(t, f.claudeLog); len(calls) != 0 {
		t.Errorf("claude was called with --no-mcp: %q", calls)
	}
	mustContain(t, "stdout", stdout, "peers test-version installed (no service)")
	mustContain(t, "stdout", stdout, "binary:     "+f.pathUnderHome(t, ".local", "bin", "peers"))
	mustContain(t, "stdout", stdout, "MCP:        skipped (--no-mcp)")
}

func TestInstallScriptRejectsUnexpectedVersion(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(map[string]string{"FAKE_AGENT_VERSION": "not peers"})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	mustContain(t, "stderr", stderr, "unexpected output from")
	if got := countLines(readLines(t, f.agentLog), "service install"); got != 0 {
		t.Errorf("service install calls = %d, want 0 after a version mismatch", got)
	}
}

func TestInstallScriptRequiresTheArchiveBinary(t *testing.T) {
	f := newFixture(t)
	if err := os.Remove(filepath.Join(f.archive, "peers")); err != nil {
		t.Fatalf("removing the archive binary: %v", err)
	}
	_, stderr, code := f.run(nil)
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	mustContain(t, "stderr", stderr, "no peers binary")
}

func TestInstallScriptReportsInstallFailure(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(map[string]string{"FAKE_INSTALL_COPY": "fail"})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	mustContain(t, "stderr", stderr, "cannot install the binary to")
}

func TestInstallScriptReportsServiceFailure(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(map[string]string{"FAKE_AGENT_SERVICE_EXIT": "1"})
	if code != 1 {
		t.Fatalf("exit = %d, want 1", code)
	}
	mustContain(t, "stderr", stderr, "cannot install the user service")
}

func TestInstallScriptAcceptsDegradedService(t *testing.T) {
	f := newFixture(t)
	stdout, stderr, code := f.run(map[string]string{"FAKE_AGENT_SERVICE_EXIT": "3"})
	if code != 3 {
		t.Fatalf("exit = %d, want 3\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	// service install printed the summary; the script kept going through the
	// MCP registration and its own summary.
	mustContain(t, "stdout", stdout, "installed (user service, starts at login)")
	mustContain(t, "stdout", stdout, "peers test-version installed")
	mustContain(t, "stdout", stdout, "MCP:        registered")
	mustContain(t, "stderr", stderr, "Broker: degraded (local only)")
	mustContain(t, "stderr", stderr, "Fix:")

	agent := readLines(t, f.agentLog)
	if got := countLines(agent, "service install"); got != 1 {
		t.Errorf("service install calls = %d, want 1; calls: %q", got, agent)
	}
	claude := readLines(t, f.claudeLog)
	if got := countLines(claude, "mcp add --scope user peer --"); got != 1 {
		t.Errorf("mcp add calls = %d, want 1; calls: %q", got, claude)
	}
}

func TestInstallScriptReportsUpgrade(t *testing.T) {
	writeOldBinary := func(t *testing.T, prefix, version string) {
		t.Helper()
		if err := os.MkdirAll(prefix, 0o755); err != nil {
			t.Fatalf("creating %s: %v", prefix, err)
		}
		writeExecutable(t, filepath.Join(prefix, "peers"),
			"#!/bin/sh\nprintf 'peers "+version+"\\n'\n")
	}

	t.Run("no service", func(t *testing.T) {
		f := newFixture(t)
		prefix := filepath.Join(f.base, "upbin")
		writeOldBinary(t, prefix, "0.2.0")

		stdout, stderr, code := f.run(
			map[string]string{"FAKE_AGENT_VERSION": "peers 0.5.0"},
			"--prefix", prefix, "--no-service", "--no-mcp")
		if code != 0 {
			t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
		}
		mustContain(t, "stdout", stdout, "Upgraded from 0.2.0.")
		mustContain(t, "stdout", stdout, "peers 0.5.0 installed (no service)")
		mustContain(t, "stdout", stdout, "binary:     "+f.userBinary(prefix))
		if got := countLines(readLines(t, f.agentLog), "--version"); got != 1 {
			t.Errorf("new binary --version calls = %d, want 1", got)
		}
	})

	t.Run("before the service summary", func(t *testing.T) {
		f := newFixture(t)
		prefix := filepath.Join(f.base, "upbin")
		writeOldBinary(t, prefix, "0.1.0")

		stdout, stderr, code := f.run(
			map[string]string{"FAKE_AGENT_VERSION": "peers 0.5.0"},
			"--prefix", prefix, "--no-mcp")
		if code != 0 {
			t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
		}
		upgraded := strings.Index(stdout, "Upgraded from 0.1.0.")
		summary := strings.Index(stdout, "peers 0.5.0 installed")
		if upgraded < 0 || summary < 0 || upgraded > summary {
			t.Errorf("stdout does not report the upgrade before the service summary:\n%s", stdout)
		}
	})

	t.Run("unexpected version output", func(t *testing.T) {
		f := newFixture(t)
		prefix := filepath.Join(f.base, "upbin")
		if err := os.MkdirAll(prefix, 0o755); err != nil {
			t.Fatalf("creating %s: %v", prefix, err)
		}
		writeExecutable(t, filepath.Join(prefix, "peers"), "#!/bin/sh\nprintf 'something else\\n'\n")

		stdout, stderr, code := f.run(nil, "--prefix", prefix, "--no-service", "--no-mcp")
		if code != 0 {
			t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
		}
		mustNotContain(t, "stdout", stdout, "Upgraded from")
	})
}

// TestInstallScriptReportsVersionChange checks that the line before the
// summary compares the installed version with the one it replaced: an upgrade,
// a reinstall and a downgrade each get their own sentence, the comparison is
// numeric, so 0.10.0 is newer than 0.9.0, and a version whose order cannot be
// established is reported as a replacement. Two identical version strings are
// the same version even when they have no release form, so that case is a
// reinstall too.
func TestInstallScriptReportsVersionChange(t *testing.T) {
	cases := []struct {
		name       string
		oldVersion string
		newVersion string
		want       string
		unwanted   []string
	}{
		{
			name:       "older version is upgraded",
			oldVersion: "0.4.0",
			newVersion: "0.5.0",
			want:       "Upgraded from 0.4.0.",
			unwanted:   []string{"Downgraded", "Reinstalled"},
		},
		{
			name:       "same version is reinstalled",
			oldVersion: "0.4.0",
			newVersion: "0.4.0",
			want:       "Reinstalled peers 0.4.0.",
			unwanted:   []string{"Upgraded", "Downgraded"},
		},
		{
			name:       "identical version without a release form is reinstalled",
			oldVersion: "e2e",
			newVersion: "e2e",
			want:       "Reinstalled peers e2e.",
			unwanted:   []string{"Upgraded", "Downgraded", "Replaced"},
		},
		{
			name:       "newer version is downgraded",
			oldVersion: "0.5.0",
			newVersion: "0.4.0",
			want:       "Downgraded from 0.5.0.",
			unwanted:   []string{"Upgraded", "Reinstalled"},
		},
		{
			name:       "0.10.0 is newer than 0.9.0",
			oldVersion: "0.9.0",
			newVersion: "0.10.0",
			want:       "Upgraded from 0.9.0.",
			unwanted:   []string{"Downgraded", "Reinstalled"},
		},
		{
			name:       "development version is replaced without an order",
			oldVersion: "0.4.0",
			newVersion: "dev",
			want:       "Replaced peers 0.4.0.",
			unwanted:   []string{"Upgraded", "Downgraded", "Reinstalled"},
		},
		{
			name:       "pre-release version is replaced without an order",
			oldVersion: "0.5.0-rc1",
			newVersion: "0.5.0",
			want:       "Replaced peers 0.5.0-rc1.",
			unwanted:   []string{"Upgraded", "Downgraded", "Reinstalled"},
		},
		{
			name:       "replaced development version is named as is",
			oldVersion: "dev",
			newVersion: "0.5.0",
			want:       "Replaced peers dev.",
			unwanted:   []string{"Upgraded", "Downgraded", "Reinstalled"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := filepath.Join(f.base, "vbin")
			if err := os.MkdirAll(prefix, 0o755); err != nil {
				t.Fatalf("creating %s: %v", prefix, err)
			}
			writeExecutable(t, filepath.Join(prefix, "peers"),
				"#!/bin/sh\nprintf 'peers "+tc.oldVersion+"\\n'\n")

			stdout, stderr, code := f.run(
				map[string]string{"FAKE_AGENT_VERSION": "peers " + tc.newVersion},
				"--prefix", prefix, "--no-service", "--no-mcp")
			if code != 0 {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
			}
			mustContain(t, "stdout", stdout, tc.want)
			for _, unwanted := range tc.unwanted {
				mustNotContain(t, "stdout", stdout, unwanted)
			}
		})
	}
}

// hostSystemDefinitionPath is the existence check the script makes for a
// system service on this platform.
func hostSystemDefinitionPath() string {
	if runtime.GOOS == "darwin" {
		return "/Library/LaunchDaemons/io.github.wangzw.peers.plist"
	}
	return "/etc/systemd/system/peers.service"
}

const (
	levelInstallPrompt = "Install for every account on this host? [y/N]"
	levelUpgradePrompt = "Upgrade the system installation? [Y/n]"
)

func TestInstallScriptSystemReexecutesThroughSudo(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := map[string]string{
		"FAKE_ID_UID":       "1000",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
	}
	stdout, stderr, code := f.run(env, "--system", "--prefix", prefix,
		"--no-service", "--no-mcp", "--admin-group", "ops", "--join-admin-group")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	want := "sudo sh " + filepath.Join(f.resolvedArchive(t), "install.sh") +
		" --system --prefix " + prefix +
		" --no-service --no-mcp --admin-group ops --join-admin-group"
	calls := readLines(t, f.sudoLog)
	if len(calls) != 1 || calls[0] != want {
		t.Fatalf("sudo calls = %q, want exactly %q", calls, want)
	}
	if _, err := os.Stat(f.installedBinary(t, prefix)); err != nil {
		t.Fatalf("the binary is missing after the sudo re-exec: %v", err)
	}
	if got := countLines(readLines(t, f.installLog), "-o root -g 0 -m 0755"); got != 1 {
		t.Errorf("root-owned install calls = %d, want 1; calls: %q", got, readLines(t, f.installLog))
	}
}

func TestInstallScriptSystemWithoutSudo(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := map[string]string{
		"FAKE_ID_UID":       "1000",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
		"PATH":              f.pathWithoutSudo(t),
	}
	_, stderr, code := f.run(env, "--system", "--prefix", prefix, "--no-service", "--no-mcp")
	if code != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", code, stderr)
	}
	mustContain(t, "stderr", stderr, "needs sudo")
	mustContain(t, "stderr", stderr, "--user")
	if got := readLines(t, f.installLog); len(got) != 0 {
		t.Errorf("the binary was installed without sudo: %q", got)
	}
}

func TestInstallScriptReportsSudoFailure(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := map[string]string{
		"FAKE_ID_UID":       "1000",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
		"FAKE_SUDO_EXIT":    "1",
	}
	_, stderr, code := f.run(env, "--system", "--prefix", prefix, "--no-service", "--no-mcp")
	if code != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", code, stderr)
	}
	mustContain(t, "stderr", stderr, "the system installation did not complete")
	mustContain(t, "stderr", stderr, "sudo exited with status 1")
	// The command under sudo failed while sudo itself works. The sudo run has
	// already printed its own error; the outer message must only tell the
	// caller to fix it and rerun, not suggest a level change that would hit the
	// same failure.
	mustContain(t, "stderr", stderr, "Fix the error above and run the script again.")
	mustNotContain(t, "stderr", stderr, "--user")
	mustNotContain(t, "stderr", stderr, "Rerun the installation command with sudo")
	mustNotContain(t, "stderr", stderr, "rerun with sudo")
	if calls := readLines(t, f.sudoLog); len(calls) != 2 || calls[1] != "sudo -n -v" {
		t.Errorf("sudo calls = %q, want the install and a sudo -n -v check", calls)
	}
	if got := readLines(t, f.installLog); len(got) != 0 {
		t.Errorf("the binary was installed although sudo failed: %q", got)
	}
}

// TestInstallScriptReportsSudoVerifyFailure checks that a sudo that cannot run
// at all, through an authentication failure or an account absent from
// sudoers, is distinguished from a command that failed under sudo: only the
// former suggests a user install, because rerunning the same command cannot
// fix the sudo problem.
func TestInstallScriptReportsSudoVerifyFailure(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := map[string]string{
		"FAKE_ID_UID":           "1000",
		"FAKE_STAT_OWNER":       "0",
		"FAKE_STAT_MODE":        "755",
		"FAKE_INSTALL_COPY":     "1",
		"FAKE_SUDO_EXIT":        "1",
		"FAKE_SUDO_VERIFY_EXIT": "1",
	}
	_, stderr, code := f.run(env, "--system", "--prefix", prefix, "--no-service", "--no-mcp")
	if code != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", code, stderr)
	}
	mustContain(t, "stderr", stderr, "the system installation did not complete")
	mustContain(t, "stderr", stderr, "sudo exited with status 1")
	mustContain(t, "stderr", stderr, "sudo is not usable; pass --user for a user install.")
	mustNotContain(t, "stderr", stderr, "Fix the error above and run the script again.")
	if calls := readLines(t, f.sudoLog); len(calls) != 2 || calls[1] != "sudo -n -v" {
		t.Errorf("sudo calls = %q, want the install and a sudo -n -v check", calls)
	}
	if got := readLines(t, f.installLog); len(got) != 0 {
		t.Errorf("the binary was installed although sudo failed: %q", got)
	}
}

// TestInstallScriptPassesThroughSudoDegradedExit checks that the degraded
// service status reaches the caller unchanged and is not reported as a failed
// installation.
func TestInstallScriptPassesThroughSudoDegradedExit(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := map[string]string{
		"FAKE_ID_UID":       "1000",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
		"FAKE_SUDO_EXIT":    "3",
	}
	_, stderr, code := f.run(env, "--system", "--prefix", prefix, "--no-service", "--no-mcp")
	if code != 3 {
		t.Fatalf("exit = %d, want 3 from the degraded service\nstderr:\n%s", code, stderr)
	}
	if strings.Contains(stderr, "did not complete") {
		t.Errorf("a degraded service was reported as a failed install:\n%s", stderr)
	}
	if got := readLines(t, f.installLog); len(got) != 0 {
		t.Errorf("the outer install installed a binary although the sudo run reported a degraded service: %q", got)
	}
}

// pathWithout builds a search path with every fake command except name, plus
// the real tools the script needs outside the fake set. The host's real command
// is then invisible, so a test can check the behavior for its absence.
func (f *fixture) pathWithout(t *testing.T, name string) string {
	t.Helper()
	dir := filepath.Join(f.base, "no"+name+"bin")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	entries, err := os.ReadDir(f.fakes)
	if err != nil {
		t.Fatalf("reading %s: %v", f.fakes, err)
	}
	// These tools are linked from their real paths below; a fake with the same
	// name is skipped here so the second link does not collide.
	realTools := []string{"grep", "dirname", "mkdir", "cp", "mktemp", "chmod", "mv", "rm"}
	isReal := func(name string) bool {
		for _, tool := range realTools {
			if name == tool {
				return true
			}
		}
		return false
	}
	for _, entry := range entries {
		if entry.Name() == name || isReal(entry.Name()) {
			continue
		}
		if err := os.Symlink(filepath.Join(f.fakes, entry.Name()), filepath.Join(dir, entry.Name())); err != nil {
			t.Fatalf("linking %s: %v", entry.Name(), err)
		}
	}
	for _, tool := range realTools {
		if err := os.Symlink(mustLookPath(t, tool), filepath.Join(dir, tool)); err != nil {
			t.Fatalf("linking the real %s: %v", tool, err)
		}
	}
	return dir
}

// pathWithoutSudo is the search path used to test a missing sudo.
func (f *fixture) pathWithoutSudo(t *testing.T) string {
	t.Helper()
	return f.pathWithout(t, "sudo")
}

// skipWithoutTTYForwarding skips the prompt tests on the platforms whose
// script(1) does not forward a piped answer to the terminal.
func skipWithoutTTYForwarding(t *testing.T) {
	t.Helper()
	if runtime.GOOS == "darwin" {
		t.Skip("the macOS script(1) does not forward a piped answer to /dev/tty; " +
			"the no-terminal, flag and Darwin branch tests cover the level choice")
	}
}

func TestInstallScriptLevelPromptInstallsSystemLevel(t *testing.T) {
	skipWithoutTTYForwarding(t)
	for _, tc := range []struct {
		name   string
		answer string
	}{
		{"y", "y\n"},
		{"yes", "yes\n"},
		{"mixed-case yes", "YeS\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := f.systemPrefix(t)
			env := map[string]string{
				"FAKE_ID_UID":       "1000",
				"FAKE_STAT_OWNER":   "0",
				"FAKE_STAT_MODE":    "755",
				"FAKE_INSTALL_COPY": "1",
			}
			stdout, stderr, tty, code := f.runWithTTY(t, tc.answer, env,
				"--prefix", prefix, "--no-service", "--no-mcp")
			if code != 0 {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
			}
			mustContain(t, "terminal", tty, levelInstallPrompt)
			if calls := readLines(t, f.sudoLog); len(calls) != 1 || !strings.Contains(calls[0], " --system ") {
				t.Fatalf("sudo calls = %q, want one system re-exec", calls)
			}
			if _, err := os.Stat(f.installedBinary(t, prefix)); err != nil {
				t.Fatalf("the binary is missing after choosing the system installation: %v", err)
			}
		})
	}
}

func TestInstallScriptLevelPromptInstallsUserLevel(t *testing.T) {
	skipWithoutTTYForwarding(t)
	for _, tc := range []struct {
		name   string
		answer string
	}{
		{"enter", "\n"},
		{"n", "n\n"},
		{"other answer", "maybe\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := filepath.Join(f.base, "userbin")
			env := map[string]string{"FAKE_ID_UID": "1000"}
			stdout, stderr, tty, code := f.runWithTTY(t, tc.answer, env,
				"--prefix", prefix, "--no-service", "--no-mcp")
			if code != 0 {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
			}
			mustContain(t, "terminal", tty, levelInstallPrompt)
			if calls := readLines(t, f.sudoLog); len(calls) != 0 {
				t.Errorf("sudo calls = %q, want none for a user install", calls)
			}
			if _, err := os.Stat(f.userBinary(prefix)); err != nil {
				t.Fatalf("the user binary is missing: %v", err)
			}
		})
	}
}

func TestInstallScriptLevelPromptUpgrades(t *testing.T) {
	skipWithoutTTYForwarding(t)
	for _, tc := range []struct {
		name   string
		answer string
	}{
		{"enter", "\n"},
		{"y", "y\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := f.systemPrefix(t)
			env := map[string]string{
				"FAKE_ID_UID":            "1000",
				"FAKE_STAT_OWNER":        "0",
				"FAKE_STAT_MODE":         "755",
				"FAKE_INSTALL_COPY":      "1",
				"FAKE_STAT_PRESENT_PATH": hostSystemDefinitionPath(),
			}
			stdout, stderr, tty, code := f.runWithTTY(t, tc.answer, env,
				"--prefix", prefix, "--no-service", "--no-mcp")
			if code != 0 {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
			}
			mustContain(t, "terminal", tty, levelUpgradePrompt)
			mustNotContain(t, "terminal", tty, levelInstallPrompt)
			if calls := readLines(t, f.sudoLog); len(calls) != 1 || !strings.Contains(calls[0], " --system ") {
				t.Fatalf("sudo calls = %q, want one system re-exec", calls)
			}
			if _, err := os.Stat(f.installedBinary(t, prefix)); err != nil {
				t.Fatalf("the binary is missing after the upgrade: %v", err)
			}
		})
	}
}

func TestInstallScriptLevelPromptDeclinesUpgrade(t *testing.T) {
	skipWithoutTTYForwarding(t)
	f := newFixture(t)
	prefix := filepath.Join(f.base, "userbin")
	env := map[string]string{
		"FAKE_ID_UID":            "1000",
		"FAKE_STAT_PRESENT_PATH": hostSystemDefinitionPath(),
	}
	stdout, stderr, tty, code := f.runWithTTY(t, "n\n", env,
		"--prefix", prefix, "--no-service", "--no-mcp")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
	}
	mustContain(t, "terminal", tty, levelUpgradePrompt)
	mustContain(t, "stdout", stdout, "install.sh: the system installation was left unchanged.")
	mustNotContain(t, "stdout", stdout, "--user")
	for what, path := range map[string]string{"sudo log": f.sudoLog, "install log": f.installLog, "agent log": f.agentLog, "claude log": f.claudeLog} {
		if calls := readLines(t, path); len(calls) != 0 {
			t.Errorf("%s = %q, want no changes when the upgrade is declined", what, calls)
		}
	}
	if _, err := os.Stat(prefix); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("the declined upgrade created %s: %v", prefix, err)
	}
}

func TestInstallScriptRootDoesNotAskForLevel(t *testing.T) {
	skipWithoutTTYForwarding(t)
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := map[string]string{
		"FAKE_ID_UID":       "0",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
	}
	stdout, stderr, tty, code := f.runWithTTY(t, "", env,
		"--prefix", prefix, "--no-service", "--no-mcp")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
	}
	mustNotContain(t, "terminal", tty, levelInstallPrompt)
	mustNotContain(t, "terminal", tty, levelUpgradePrompt)
	if _, err := os.Stat(f.installedBinary(t, prefix)); err != nil {
		t.Fatalf("the system binary is missing: %v", err)
	}
}

func TestInstallScriptExplicitLevelSkipsPrompt(t *testing.T) {
	skipWithoutTTYForwarding(t)
	t.Run("--user", func(t *testing.T) {
		f := newFixture(t)
		prefix := filepath.Join(f.base, "userbin")
		_, stderr, tty, code := f.runWithTTY(t, "", map[string]string{"FAKE_ID_UID": "1000"},
			"--user", "--prefix", prefix, "--no-service", "--no-mcp")
		if code != 0 {
			t.Fatalf("exit = %d, want 0\nstderr:\n%s\nterminal:\n%s", code, stderr, tty)
		}
		mustNotContain(t, "terminal", tty, levelInstallPrompt)
		mustNotContain(t, "terminal", tty, levelUpgradePrompt)
		if _, err := os.Stat(f.userBinary(prefix)); err != nil {
			t.Fatalf("the user binary is missing: %v", err)
		}
	})

	t.Run("--system", func(t *testing.T) {
		f := newFixture(t)
		prefix := f.systemPrefix(t)
		env := map[string]string{
			"FAKE_ID_UID":       "1000",
			"FAKE_STAT_OWNER":   "0",
			"FAKE_STAT_MODE":    "755",
			"FAKE_INSTALL_COPY": "1",
		}
		_, stderr, tty, code := f.runWithTTY(t, "", env,
			"--system", "--prefix", prefix, "--no-service", "--no-mcp")
		if code != 0 {
			t.Fatalf("exit = %d, want 0\nstderr:\n%s\nterminal:\n%s", code, stderr, tty)
		}
		mustNotContain(t, "terminal", tty, levelInstallPrompt)
		mustNotContain(t, "terminal", tty, levelUpgradePrompt)
		if calls := readLines(t, f.sudoLog); len(calls) != 1 {
			t.Fatalf("sudo calls = %q, want one system re-exec", calls)
		}
	})
}

func TestInstallScriptBothLevelsIsAUsageError(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(nil, "--system", "--user")
	if code != 2 {
		t.Fatalf("--system --user exit = %d, want 2\nstderr:\n%s", code, stderr)
	}
	mustContain(t, "stderr", stderr, "--system and --user cannot be used together")
	for what, path := range map[string]string{"install log": f.installLog, "agent log": f.agentLog, "sudo log": f.sudoLog} {
		if calls := readLines(t, path); len(calls) != 0 {
			t.Errorf("%s = %q, want no changes", what, calls)
		}
	}
}

func TestInstallScriptWithoutTerminalInstallsUserLevel(t *testing.T) {
	f := newFixture(t)
	prefix := filepath.Join(f.base, "userbin")
	// stdin says yes, but without a terminal the answer must not be read, so
	// the install stays a user install.
	stdout, stderr, code := f.runDetached("y\n", map[string]string{"FAKE_ID_UID": "1000"},
		"--prefix", prefix, "--no-service", "--no-mcp")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	mustNotContain(t, "stdout", stdout, levelInstallPrompt)
	mustNotContain(t, "stderr", stderr, "/dev/tty")
	if calls := readLines(t, f.sudoLog); len(calls) != 0 {
		t.Errorf("sudo calls = %q, want none without a terminal", calls)
	}
	if _, err := os.Stat(f.userBinary(prefix)); err != nil {
		t.Fatalf("the user binary is missing: %v", err)
	}
}

func TestInstallScriptAdminGroupAcceptedAfterPrompt(t *testing.T) {
	skipWithoutTTYForwarding(t)
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := map[string]string{
		"FAKE_ID_UID":       "1000",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
	}
	_, stderr, tty, code := f.runWithTTY(t, "y\n", env,
		"--admin-group", "operators", "--prefix", prefix, "--no-service", "--no-mcp")
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s\nterminal:\n%s", code, stderr, tty)
	}
	if calls := readLines(t, f.sudoLog); len(calls) != 1 || !strings.Contains(calls[0], "--admin-group operators") {
		t.Fatalf("sudo calls = %q, want the admin group to reach the system re-exec", calls)
	}
	mustNotContain(t, "stderr", stderr, "only valid with --system")
}

func TestInstallScriptAdminGroupRejectedForPromptedUserLevel(t *testing.T) {
	skipWithoutTTYForwarding(t)
	f := newFixture(t)
	prefix := filepath.Join(f.base, "userbin")
	_, stderr, tty, code := f.runWithTTY(t, "\n", map[string]string{"FAKE_ID_UID": "1000"},
		"--admin-group", "operators", "--prefix", prefix, "--no-service", "--no-mcp")
	if code != 2 {
		t.Fatalf("exit = %d, want 2\nstderr:\n%s\nterminal:\n%s", code, stderr, tty)
	}
	mustContain(t, "terminal", tty, levelInstallPrompt)
	mustContain(t, "stderr", stderr, "--admin-group is only valid with --system")
	if got := readLines(t, f.installLog); len(got) != 0 {
		t.Errorf("the binary was installed before the admin group was rejected: %q", got)
	}
}

func TestInstallScriptSystemRejectsUnsafeDirectory(t *testing.T) {
	cases := []struct {
		name  string
		owner string
		mode  string
	}{
		{"not owned by root", "1000", "755"},
		{"group writable", "0", "775"},
		{"other writable", "0", "757"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := filepath.Join(f.base, "sysbin")
			if err := os.MkdirAll(prefix, 0o755); err != nil {
				t.Fatalf("creating %s: %v", prefix, err)
			}
			env := map[string]string{
				"FAKE_ID_UID":       "0",
				"FAKE_STAT_OWNER":   tc.owner,
				"FAKE_STAT_MODE":    tc.mode,
				"FAKE_INSTALL_COPY": "1",
			}
			_, stderr, code := f.run(env, "--system", "--no-service", "--no-mcp", "--prefix", prefix)
			if code != 1 {
				t.Fatalf("exit = %d, want 1\nstderr:\n%s", code, stderr)
			}
			mustContain(t, "stderr", stderr, "--prefix")
			if got := readLines(t, f.installLog); len(got) != 0 {
				t.Errorf("the binary was installed into an unsafe directory: %q", got)
			}
		})
	}
}

func TestInstallScriptSystemRejectsUnsafeAncestor(t *testing.T) {
	cases := []struct {
		name   string
		owner  string
		mode   string
		detail string
	}{
		{"group writable ancestor", "0", "775", "writable by group or others"},
		{"ancestor not owned by root", "1000", "755", "not owned by root"},
		{"ancestor not searchable by others", "0", "750", "not searchable by others"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := filepath.Join(f.base, "sysbin")
			if err := os.MkdirAll(prefix, 0o755); err != nil {
				t.Fatalf("creating %s: %v", prefix, err)
			}
			// Only this ancestor answers with the unsafe values; the install
			// directory itself stays root-owned and safe.
			ancestor, err := filepath.EvalSymlinks(f.base)
			if err != nil {
				t.Fatalf("resolving %s: %v", f.base, err)
			}
			env := map[string]string{
				"FAKE_ID_UID":            "0",
				"FAKE_STAT_UNSAFE_PATH":  ancestor,
				"FAKE_STAT_UNSAFE_OWNER": tc.owner,
				"FAKE_STAT_UNSAFE_MODE":  tc.mode,
				"FAKE_INSTALL_COPY":      "1",
			}
			_, stderr, code := f.run(env, "--system", "--no-service", "--no-mcp", "--prefix", prefix)
			if code != 1 {
				t.Fatalf("exit = %d, want 1\nstderr:\n%s", code, stderr)
			}
			mustContain(t, "stderr", stderr, tc.detail)
			mustContain(t, "stderr", stderr, ancestor)
			if got := readLines(t, f.installLog); len(got) != 0 {
				t.Errorf("the binary was installed before the ancestor check failed: %q", got)
			}
		})
	}
}

func TestInstallScriptSystemInstallsIntoRootOwnedDirectory(t *testing.T) {
	f := newFixture(t)
	prefix := filepath.Join(f.base, "sysbin")
	if err := os.MkdirAll(prefix, 0o755); err != nil {
		t.Fatalf("creating %s: %v", prefix, err)
	}
	env := map[string]string{
		"FAKE_ID_UID":       "0",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
	}
	stdout, stderr, code := f.run(env, "--system", "--no-service", "--no-mcp", "--prefix", prefix)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}

	installs := readLines(t, f.installLog)
	if got := countLines(installs, "-o root -g 0 -m 0755"); got != 1 {
		t.Errorf("root-owned install calls = %d, want 1; calls: %q", got, installs)
	}
	binary := f.installedBinary(t, prefix)
	if _, err := os.Stat(binary); err != nil {
		t.Fatalf("the installed binary is missing: %v", err)
	}
	if got := countLines(readLines(t, f.agentLog), "--version"); got != 1 {
		t.Errorf("--version calls = %d, want 1", got)
	}
	mustContain(t, "stdout", stdout, "peers test-version installed (no service)")
	mustContain(t, "stdout", stdout, "binary:     "+binary)
	mustContain(t, "stdout", stdout, "MCP:        skipped (--no-mcp)")
}

func TestInstallScriptSystemChecksDirectoriesWithBSDStat(t *testing.T) {
	f := newFixture(t)
	prefix := filepath.Join(f.base, "sysbin")
	if err := os.MkdirAll(prefix, 0o755); err != nil {
		t.Fatalf("creating %s: %v", prefix, err)
	}
	env := map[string]string{
		"FAKE_ID_UID":          "0",
		"FAKE_STAT_OWNER":      "0",
		"FAKE_STAT_MODE":       "755",
		"FAKE_STAT_REJECT_GNU": "1",
		"FAKE_INSTALL_COPY":    "1",
	}
	_, stderr, code := f.run(env, "--system", "--no-service", "--no-mcp", "--prefix", prefix)
	if code != 0 {
		t.Fatalf("exit = %d, want 0; the BSD stat fallback did not work\nstderr:\n%s", code, stderr)
	}
}

func TestInstallScriptSystemPrintsManualMCPCommand(t *testing.T) {
	cases := []struct {
		name     string
		sudoUser string // empty means the variable is not set
	}{
		{"no sudo user", ""},
		{"sudo user is root", "root"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := filepath.Join(f.base, "sysbin")
			if err := os.MkdirAll(prefix, 0o755); err != nil {
				t.Fatalf("creating %s: %v", prefix, err)
			}
			env := map[string]string{
				"FAKE_ID_UID":       "0",
				"FAKE_STAT_OWNER":   "0",
				"FAKE_STAT_MODE":    "755",
				"FAKE_INSTALL_COPY": "1",
			}
			if tc.sudoUser != "" {
				env["SUDO_USER"] = tc.sudoUser
			}
			stdout, stderr, code := f.run(env, "--system", "--no-service", "--prefix", prefix)
			if code != 0 {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
			}
			binary := f.installedBinary(t, prefix)
			combined := stdout + stderr
			mustContain(t, "output", combined, "claude mcp add --scope user peer -- "+binary+" mcp")
			mustContain(t, "stdout", stdout, "MCP:        skipped (no account to register for)")
			if calls := readLines(t, f.claudeLog); len(calls) != 0 {
				t.Errorf("claude was called without a user to register for: %q", calls)
			}
		})
	}
}

func TestInstallScriptSystemRegistersForSudoUser(t *testing.T) {
	f := newFixture(t)
	prefix := filepath.Join(f.base, "sysbin")
	if err := os.MkdirAll(prefix, 0o755); err != nil {
		t.Fatalf("creating %s: %v", prefix, err)
	}
	env := map[string]string{
		"FAKE_ID_UID":       "0",
		"FAKE_STAT_OWNER":   "0",
		"FAKE_STAT_MODE":    "755",
		"FAKE_INSTALL_COPY": "1",
		"SUDO_USER":         "alice",
	}
	binary := f.installedBinary(t, prefix)
	for run := 1; run <= 2; run++ {
		stdout, stderr, code := f.run(env, "--system", "--no-service", "--prefix", prefix)
		if code != 0 {
			t.Fatalf("run %d exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", run, code, stdout, stderr)
		}
		if run == 1 {
			mustContain(t, "stdout", stdout, "MCP:        registered")
		} else {
			mustContain(t, "stdout", stdout, "MCP:        already registered")
		}
	}
	if got := countLines(readLines(t, f.claudeLog), "mcp add --scope user peer -- "+binary+" mcp"); got != 1 {
		t.Errorf("mcp add calls across two system runs = %d, want 1; calls: %q", got, readLines(t, f.claudeLog))
	}
}

// adminGroupPrompt is the question the script asks on the controlling terminal.
func adminGroupPrompt(user, group string) string {
	return "Add " + user + " to the " + group + " group so " + user + " can manage the Broker without sudo? [y/N]"
}

// adminGroupHint is the closing notice after the account joined the group.
func adminGroupHint(user, group string) string {
	return user + " was added to the " + group + " group; log in again or run `newgrp " + group + "` to manage the Broker without sudo."
}

// adminGroupJoinCommand is the command the script prints when the account is
// left out of the group on Linux.
func adminGroupJoinCommand(user, group string) string {
	return "install.sh: to add " + user + " to the " + group + " group, run: sudo usermod -aG " + group + " " + user
}

// darwinAdminGroupJoinCommand is the same command on macOS.
func darwinAdminGroupJoinCommand(user, group string) string {
	return "install.sh: to add " + user + " to the " + group + " group, run: sudo dseditgroup -o edit -a " + user + " -t user " + group
}

// hostAdminGroupJoinCommand is the join command the script prints on the host
// platform. The fake uname reports the host OS unless a test forces Darwin.
func hostAdminGroupJoinCommand(user, group string) string {
	if runtime.GOOS == "darwin" {
		return darwinAdminGroupJoinCommand(user, group)
	}
	return adminGroupJoinCommand(user, group)
}

// hostAdminGroupJoinCall is the group command the script runs on the host
// platform.
func hostAdminGroupJoinCall(user, group string) string {
	if runtime.GOOS == "darwin" {
		return "dseditgroup -o edit -a " + user + " -t user " + group
	}
	return "usermod -aG " + group + " " + user
}

func TestInstallScriptSystemPromptJoinsAdminGroup(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("the macOS script(1) does not forward a piped answer to /dev/tty; " +
			"the join flag and no-terminal tests cover the Darwin branch")
	}
	for _, tc := range []struct {
		name   string
		answer string
	}{
		{"y", "y\n"},
		{"Y", "Y\n"},
		{"yes", "yes\n"},
		{"mixed-case yes", "YeS\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := f.systemPrefix(t)
			env := f.systemInstallEnv("alice")
			stdout, stderr, tty, code := f.runWithTTY(t, tc.answer, env, systemInstallArgs(prefix)...)
			if code != 0 {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
			}
			if calls := readLines(t, f.groupLog); len(calls) != 1 || calls[0] != "usermod -aG peers alice" {
				t.Fatalf("group command calls = %q, want one usermod call", calls)
			}
			mustContain(t, "stdout", stdout, adminGroupHint("alice", "peers"))
			mustContain(t, "terminal", tty, adminGroupPrompt("alice", "peers"))
			mustNotContain(t, "stdout", stdout, adminGroupJoinCommand("alice", "peers"))
		})
	}
}

func TestInstallScriptSystemPromptDeclinesAdminGroup(t *testing.T) {
	if runtime.GOOS == "darwin" {
		t.Skip("the macOS script(1) does not forward a piped answer to /dev/tty; " +
			"the no-terminal and Darwin decline tests cover the refusal")
	}
	for _, tc := range []struct {
		name   string
		answer string
	}{
		{"enter", "\n"},
		{"n", "n\n"},
		{"other answer", "maybe\n"},
		{"a word starting with y", "yeah\n"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := f.systemPrefix(t)
			env := f.systemInstallEnv("alice")
			stdout, stderr, tty, code := f.runWithTTY(t, tc.answer, env, systemInstallArgs(prefix)...)
			if code != 0 {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
			}
			if calls := readLines(t, f.groupLog); len(calls) != 0 {
				t.Fatalf("group command calls = %q, want none", calls)
			}
			mustContain(t, "stdout", stdout, adminGroupJoinCommand("alice", "peers"))
			mustNotContain(t, "stdout", stdout, adminGroupHint("alice", "peers"))
			mustContain(t, "terminal", tty, adminGroupPrompt("alice", "peers"))
		})
	}
}

func TestInstallScriptSystemPromptWithoutTerminal(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	// stdin says yes, but without a terminal the answer must not come from
	// stdin, so the account stays out of the group.
	stdout, stderr, code := f.runDetached("y\n", env, systemInstallArgs(prefix)...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if calls := readLines(t, f.groupLog); len(calls) != 0 {
		t.Fatalf("group command calls = %q, want none", calls)
	}
	mustContain(t, "stdout", stdout, hostAdminGroupJoinCommand("alice", "peers"))
	mustNotContain(t, "stdout", stdout, adminGroupPrompt("alice", "peers"))
	mustNotContain(t, "stdout", stdout, adminGroupHint("alice", "peers"))
	mustNotContain(t, "stderr", stderr, "/dev/tty")
}

func TestInstallScriptSystemJoinAdminGroupWithoutPrompt(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	// stdin says no and there is no terminal: --join-admin-group must join
	// without reading either.
	stdout, stderr, code := f.runDetached("n\n", env, systemInstallArgs(prefix, "--join-admin-group")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if calls := readLines(t, f.groupLog); len(calls) != 1 || calls[0] != hostAdminGroupJoinCall("alice", "peers") {
		t.Fatalf("group command calls = %q, want %q", calls, hostAdminGroupJoinCall("alice", "peers"))
	}
	mustContain(t, "stdout", stdout, adminGroupHint("alice", "peers"))
	mustNotContain(t, "stdout", stdout, hostAdminGroupJoinCommand("alice", "peers"))
}

func TestInstallScriptSystemNoAdminGroup(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	// A terminal answers yes, but --no-admin-group must suppress the prompt and
	// the membership change.
	stdout, stderr, tty, code := f.runWithTTY(t, "y\n", env, systemInstallArgs(prefix, "--no-admin-group")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
	}
	if calls := readLines(t, f.groupLog); len(calls) != 0 {
		t.Fatalf("group command calls = %q, want none", calls)
	}
	mustNotContain(t, "terminal", tty, adminGroupPrompt("alice", "peers"))
	mustNotContain(t, "stdout", stdout, adminGroupHint("alice", "peers"))
	mustNotContain(t, "stdout", stdout, hostAdminGroupJoinCommand("alice", "peers"))
}

func TestInstallScriptSystemAdminGroupFlagsConflict(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	_, stderr, code := f.run(env, systemInstallArgs(prefix, "--join-admin-group", "--no-admin-group")...)
	if code != 2 {
		t.Fatalf("exit = %d, want 2\nstderr:\n%s", code, stderr)
	}
	mustContain(t, "stderr", stderr, "--join-admin-group and --no-admin-group cannot be used together")
	mustContain(t, "stderr", stderr, "usage:")
	if got := readLines(t, f.installLog); len(got) != 0 {
		t.Errorf("the conflict was rejected after installing the binary: %q", got)
	}
	if got := readLines(t, f.agentLog); len(got) != 0 {
		t.Errorf("the conflict was rejected after running the binary: %q", got)
	}
}

func TestInstallScriptSystemRejectsInvalidAdminGroupName(t *testing.T) {
	for _, name := range []string{"-bad", "Agent", "1ops", "a b", "a.b"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			prefix := f.systemPrefix(t)
			env := f.systemInstallEnv("alice")
			_, stderr, code := f.run(env, systemInstallArgs(prefix, "--admin-group="+name)...)
			if code != 2 {
				t.Fatalf("invalid --admin-group %q: exit %d, want 2\nstderr:\n%s", name, code, stderr)
			}
			mustContain(t, "stderr", stderr, "invalid --admin-group name")
			if got := readLines(t, f.installLog); len(got) != 0 {
				t.Errorf("the invalid name was installed after validation: %q", got)
			}
		})
	}
}

func TestInstallScriptSystemAcceptsValidAdminGroupName(t *testing.T) {
	for _, name := range []string{"a", "_ops"} {
		t.Run(name, func(t *testing.T) {
			f := newFixture(t)
			prefix := f.systemPrefix(t)
			env := f.systemInstallEnv("alice")
			stdout, stderr, code := f.runDetached("", env,
				systemInstallArgs(prefix, "--admin-group="+name, "--join-admin-group")...)
			if code != 0 {
				t.Fatalf("valid --admin-group %q: exit %d\nstdout:\n%s\nstderr:\n%s", name, code, stdout, stderr)
			}
			wantCall := "service install --system --admin-group " + name
			if got := countLines(readLines(t, f.agentLog), wantCall); got != 1 {
				t.Errorf("service install calls with %q = %d, want 1; calls: %q", wantCall, got, readLines(t, f.agentLog))
			}
		})
	}
}

func TestInstallScriptSystemSkipsAdminGroupWhenAlreadyMember(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_ID_GROUPS"] = "alice staff peers"
	stdout, stderr, tty, code := f.runWithTTY(t, "y\n", env, systemInstallArgs(prefix)...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s\nterminal:\n%s", code, stdout, stderr, tty)
	}
	if calls := readLines(t, f.groupLog); len(calls) != 0 {
		t.Fatalf("group command calls = %q, want none for an existing member", calls)
	}
	mustNotContain(t, "terminal", tty, adminGroupPrompt("alice", "peers"))
	mustNotContain(t, "stdout", stdout, adminGroupHint("alice", "peers"))
}

func TestInstallScriptSystemSkipsAdminGroupWithoutSudoUser(t *testing.T) {
	for _, tc := range []struct {
		name string
		user string
	}{
		{"no sudo user", ""},
		{"sudo user is root", "root"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			prefix := f.systemPrefix(t)
			env := f.systemInstallEnv(tc.user)
			stdout, stderr, code := f.runDetached("y\n", env, systemInstallArgs(prefix)...)
			if code != 0 {
				t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
			}
			if calls := readLines(t, f.groupLog); len(calls) != 0 {
				t.Fatalf("group command calls = %q, want none", calls)
			}
			mustNotContain(t, "stdout", stdout, "was added to")
			mustNotContain(t, "stdout", stdout, "to add ")
		})
	}
}

func TestInstallScriptSystemAdminGroupOnDarwin(t *testing.T) {
	t.Run("join", func(t *testing.T) {
		f := newFixture(t)
		prefix := f.systemPrefix(t)
		env := f.systemInstallEnv("alice")
		env["FAKE_UNAME"] = "Darwin"
		// --join-admin-group picks the platform command without a terminal, so
		// this checks the Darwin dseditgroup call without the pty harness.
		stdout, stderr, code := f.runDetached("", env, systemInstallArgs(prefix, "--join-admin-group")...)
		if code != 0 {
			t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
		}
		if calls := readLines(t, f.groupLog); len(calls) != 1 || calls[0] != "dseditgroup -o edit -a alice -t user peers" {
			t.Fatalf("group command calls = %q, want one dseditgroup call", calls)
		}
		mustContain(t, "stdout", stdout, adminGroupHint("alice", "peers"))
	})

	t.Run("decline", func(t *testing.T) {
		f := newFixture(t)
		prefix := f.systemPrefix(t)
		env := f.systemInstallEnv("alice")
		env["FAKE_UNAME"] = "Darwin"
		stdout, stderr, _, code := f.runWithTTY(t, "n\n", env, systemInstallArgs(prefix)...)
		if code != 0 {
			t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
		}
		if calls := readLines(t, f.groupLog); len(calls) != 0 {
			t.Fatalf("group command calls = %q, want none", calls)
		}
		mustContain(t, "stdout", stdout, darwinAdminGroupJoinCommand("alice", "peers"))
		mustNotContain(t, "stdout", stdout, "sudo usermod")
	})
}

func TestInstallScriptSystemCustomAdminGroup(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	stdout, stderr, code := f.runDetached("n\n", env,
		systemInstallArgs(prefix, "--admin-group", "operators", "--join-admin-group")...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstdout:\n%s\nstderr:\n%s", code, stdout, stderr)
	}
	if calls := readLines(t, f.groupLog); len(calls) != 1 || calls[0] != hostAdminGroupJoinCall("alice", "operators") {
		t.Fatalf("group command calls = %q, want %q", calls, hostAdminGroupJoinCall("alice", "operators"))
	}
	if got := countLines(readLines(t, f.agentLog), "service install --system --admin-group operators"); got != 1 {
		t.Errorf("service install calls with --admin-group = %d, want 1; calls: %q", got, readLines(t, f.agentLog))
	}
	mustContain(t, "stdout", stdout, adminGroupHint("alice", "operators"))
}

func TestInstallScriptSystemAdminGroupFailure(t *testing.T) {
	f := newFixture(t)
	prefix := f.systemPrefix(t)
	env := f.systemInstallEnv("alice")
	env["FAKE_GROUP_EXIT"] = "1"
	_, stderr, code := f.runDetached("", env, systemInstallArgs(prefix, "--join-admin-group")...)
	if code != 1 {
		t.Fatalf("exit = %d, want 1\nstderr:\n%s", code, stderr)
	}
	mustContain(t, "stderr", stderr, "cannot add alice to the peers group")
}

func TestInstallScriptRemovesQuarantineOnDarwin(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(map[string]string{"FAKE_UNAME": "Darwin"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0\nstderr:\n%s", code, stderr)
	}
	want := "-d com.apple.quarantine " + filepath.Join(f.resolvedArchive(t), "peers")
	mustContain(t, "xattr log", strings.Join(readLines(t, f.xattrLog), "\n"), want)

	order := readLines(t, f.orderLog)
	if len(order) < 2 {
		t.Fatalf("the order log = %q, want the xattr step before the install step", order)
	}
	if !strings.HasPrefix(order[0], "xattr ") {
		t.Errorf("first step = %q, want xattr", order[0])
	}
	if !strings.HasPrefix(order[1], "install ") {
		t.Errorf("second step = %q, want install", order[1])
	}
}

func TestInstallScriptIgnoresQuarantineFailure(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(map[string]string{"FAKE_UNAME": "Darwin", "FAKE_XATTR_EXIT": "1"})
	if code != 0 {
		t.Fatalf("exit = %d, want 0; a missing quarantine attribute is not an error\nstderr:\n%s", code, stderr)
	}
}

func TestInstallScriptHasNoNetworkOperations(t *testing.T) {
	data, err := os.ReadFile(installScriptPath)
	if err != nil {
		t.Fatalf("reading %s: %v", installScriptPath, err)
	}
	text := string(data)
	for _, tool := range []string{"curl", "wget"} {
		if strings.Contains(text, tool) {
			t.Errorf("install.sh mentions %q; the installer must not download anything", tool)
		}
	}
	if !strings.HasPrefix(text, "#!/bin/sh\n") {
		t.Errorf("install.sh does not start with a POSIX sh shebang")
	}
	if !strings.Contains(text, "set -eu") {
		t.Errorf("install.sh does not set -eu")
	}
}

const fakePeers = `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_AGENT_LOG"
case $1 in
--version)
	printf '%s\n' "$FAKE_AGENT_VERSION"
	;;
completion)
	# FAKE_PEERS_SIGNAL_SHELL names the shell whose completion sends
	# FAKE_PEERS_SIGNAL to the installer, standing for a Ctrl-C or a kill while
	# the staging file exists.
	if [ "${FAKE_PEERS_SIGNAL_SHELL:-}" = "$2" ] && [ -n "${FAKE_PEERS_SIGNAL:-}" ]; then
		kill -s "$FAKE_PEERS_SIGNAL" "$PPID" 2>/dev/null || :
	fi
	if [ "${FAKE_AGENT_COMPLETION_EXIT:-0}" != "0" ]; then
		exit "$FAKE_AGENT_COMPLETION_EXIT"
	fi
	printf '# %s completion\n' "$2"
	;;
service)
	_exit=${FAKE_AGENT_SERVICE_EXIT:-0}
	if [ "$_exit" = "0" ] || [ "$_exit" = "3" ]; then
		printf 'peers %s installed (user service, starts at login)\n' "${FAKE_AGENT_VERSION#peers }"
		printf '  binary:     %s\n' "${FAKE_AGENT_BINARY:-/usr/local/bin/peers}"
		printf '  uninstall:  peers uninstall\n'
	fi
	if [ "$_exit" = "3" ]; then
		printf 'Broker: degraded (local only): cannot bind 127.0.0.1:17443\n' >&2
		printf 'Fix: change broker.listen in the configuration\n' >&2
	fi
	if [ "$_exit" != "0" ]; then
		exit "$_exit"
	fi
	;;
esac
exit 0
`

const fakePathPeers = `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_PATH_AGENT_LOG"
exit 0
`

const fakeFish = `#!/bin/sh
exit 0
`

const fakeClaude = `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_CLAUDE_LOG"
case "$1 $2" in
"mcp get")
	if [ -f "$FAKE_CLAUDE_STATE" ]; then
		cat "$FAKE_CLAUDE_STATE"
		exit 0
	fi
	printf 'No MCP server found with name: peer\n' >&2
	exit 1
	;;
"mcp add")
	{
		printf 'peer:\n'
		printf '  Scope: User config\n'
		printf '  Command: %s\n' "$7"
		printf '  Args: mcp\n'
	} > "$FAKE_CLAUDE_STATE"
	;;
esac
exit 0
`

const fakeStat = `#!/bin/sh
gnu=0
format=
path=
for arg in "$@"; do
	case $arg in
	-c) gnu=1 ;;
	-f) gnu=0 ;;
	-L) ;;
	%u|%a|%Lp) format=$arg ;;
	*) path=$arg ;;
	esac
done
if [ "$gnu" = 1 ] && [ "${FAKE_STAT_REJECT_GNU:-0}" = "1" ]; then
	exit 1
fi
# A bare path asks only whether the path exists. FAKE_STAT_PRESENT_PATH makes the
# platform's system service definition answer yes, so the level prompt can take
# its upgrade branch without touching the real system directories.
if [ -z "$format" ] && [ -n "${FAKE_STAT_PRESENT_PATH:-}" ] && [ "$path" = "$FAKE_STAT_PRESENT_PATH" ]; then
	exit 0
fi
owner=${FAKE_STAT_OWNER:-0}
mode=${FAKE_STAT_MODE:-755}
if [ -n "${FAKE_STAT_UNSAFE_PATH:-}" ] && [ "$path" = "$FAKE_STAT_UNSAFE_PATH" ]; then
	owner=${FAKE_STAT_UNSAFE_OWNER:-$owner}
	mode=${FAKE_STAT_UNSAFE_MODE:-$mode}
fi
case $format in
%u) printf '%s\n' "$owner" ;;
%a|%Lp) printf '%s\n' "$mode" ;;
*) exit 1 ;;
esac
`

const fakeID = `#!/bin/sh
case $1 in
-u)
	if [ -n "${2:-}" ]; then
		if [ -n "${FAKE_ID_UID_FOR_NAME:-}" ]; then
			printf '%s\n' "$FAKE_ID_UID_FOR_NAME"
		else
			# A named lookup is the account that ran sudo. It gets its own
			# non-zero id so a root-owned directory is never mistaken for one
			# that account owns.
			printf '1000\n'
		fi
	elif [ -n "${FAKE_ID_UID:-}" ]; then
		printf '%s\n' "$FAKE_ID_UID"
	else
		exec @REAL_ID@ -u
	fi
	;;
-un)
	if [ -n "${FAKE_ID_UN:-}" ]; then
		printf '%s\n' "$FAKE_ID_UN"
	else
		exec @REAL_ID@ -un
	fi
	;;
-Gn|-nG)
	if [ -n "${FAKE_ID_GROUPS+x}" ]; then
		printf '%s\n' "$FAKE_ID_GROUPS"
	else
		exec @REAL_ID@ "$@"
	fi
	;;
*)
	exec @REAL_ID@ "$@"
	;;
esac
`

const fakeUname = `#!/bin/sh
if [ -n "${FAKE_UNAME:-}" ]; then
	printf '%s\n' "$FAKE_UNAME"
	exit 0
fi
exec @REAL_UNAME@ "$@"
`

const fakeXattr = `#!/bin/sh
printf 'xattr %s\n' "$*" >> "$FAKE_ORDER_LOG"
printf '%s\n' "$*" >> "$FAKE_XATTR_LOG"
exit "${FAKE_XATTR_EXIT:-0}"
`

const fakeSudo = `#!/bin/sh
printf 'sudo %s\n' "$*" >> "$FAKE_SUDO_LOG"
if [ "$1" = "-n" ] && [ "$2" = "-v" ]; then
	exit "${FAKE_SUDO_VERIFY_EXIT:-0}"
fi
if [ "${FAKE_SUDO_EXIT:-0}" != "0" ]; then
	exit "$FAKE_SUDO_EXIT"
fi
# Drop the options the installer passes, then run the command. A login shell
# command (-lc) is run with sh -c so the test keeps its own PATH; the positional
# arguments still reach the command the same way. A plain sudo call is the
# system re-exec: it runs the rest of the installation as root, so the
# fake id reports uid 0 and SUDO_USER names the invoking account.
privileged=1
dropped=
while [ "$#" -gt 0 ]; do
	case $1 in
	-u) privileged=0; dropped=$2; shift 2 ;;
	-H) shift ;;
	*) break ;;
	esac
done
if [ "$2" = "-lc" ]; then
	shift 2
	exec sh -c "$@"
fi
if [ -n "$dropped" ]; then
	FAKE_SUDO_DROPPED=$dropped
	export FAKE_SUDO_DROPPED
fi
if [ "$privileged" = 1 ]; then
	FAKE_ID_UID=0
	SUDO_USER=${FAKE_SUDO_USER:-alice}
	export FAKE_ID_UID SUDO_USER
fi
exec "$@"
`

const fakeUsermod = `#!/bin/sh
printf 'usermod %s\n' "$*" >> "$FAKE_GROUP_LOG"
exit "${FAKE_GROUP_EXIT:-0}"
`

const fakeDseditgroup = `#!/bin/sh
printf 'dseditgroup %s\n' "$*" >> "$FAKE_GROUP_LOG"
exit "${FAKE_GROUP_EXIT:-0}"
`

const fakeInstall = `#!/bin/sh
printf 'install %s\n' "$*" >> "$FAKE_ORDER_LOG"
case ${FAKE_INSTALL_COPY:-0} in
1)
	printf '%s\n' "$*" >> "$FAKE_INSTALL_LOG"
	src=
	dst=
	for arg in "$@"; do
		src=$dst
		dst=$arg
	done
	cp "$src" "$dst"
	exit $?
	;;
fail)
	exit 1
	;;
esac
exec @REAL_INSTALL@ "$@"
`

// fakeMktemp logs every call with the account it would run as. In the
// temporary-file-race test, FAKE_MKTEMP_SYMLINK names a victim and a template
// ending in .peers-XXXXXX is created and then replaced with a link to it,
// standing for another process of the account swapping the file after mktemp.
const fakeMktemp = `#!/bin/sh
printf 'mktemp as=%s %s\n' "${FAKE_SUDO_DROPPED:-root}" "$*" >> "${FAKE_WRITE_LOG:-/dev/null}"
if [ -n "${FAKE_MKTEMP_SYMLINK:-}" ] && [ -n "${FAKE_MKTEMP_SYMLINK_DIR:-}" ]; then
	case $1 in
	"$FAKE_MKTEMP_SYMLINK_DIR"/.peers-XXXXXX)
		created=$(@REAL_MKTEMP@ "$@") || exit $?
		rm -f "$created"
		ln -s "$FAKE_MKTEMP_SYMLINK" "$created" || exit 1
		printf '%s\n' "$created"
		exit 0
		;;
	esac
fi
# FAKE_MKTEMP_SWAP_DIR reproduces the ancestor swap: right before the template
# under that directory is filled in, the directory is replaced by a link to
# FAKE_MKTEMP_SWAP_TARGET. FAKE_MKTEMP_REFUSE_DROPPED makes the call fail for
# the account the installer dropped to, standing for that account's inability
# to create a file in the root-owned directory the link points at.
if [ -n "${FAKE_MKTEMP_SWAP_DIR:-}" ] && [ "$1" = "$FAKE_MKTEMP_SWAP_DIR/.peers-XXXXXX" ]; then
	if [ ! -L "$FAKE_MKTEMP_SWAP_DIR" ]; then
		rmdir "$FAKE_MKTEMP_SWAP_DIR" || exit 1
		ln -s "$FAKE_MKTEMP_SWAP_TARGET" "$FAKE_MKTEMP_SWAP_DIR" || exit 1
	fi
	if [ -n "${FAKE_MKTEMP_REFUSE_DROPPED:-}" ] && [ -n "${FAKE_SUDO_DROPPED:-}" ]; then
		exit 1
	fi
fi
# FAKE_MKTEMP_STAGE_DIR sends the installer's /tmp staging template into a
# directory the test can inspect, so the cleanup can be checked without
# touching the real /tmp.
if [ -n "${FAKE_MKTEMP_STAGE_DIR:-}" ] && [ "$1" = "/tmp/peers-completion-XXXXXX" ]; then
	set -- "$FAKE_MKTEMP_STAGE_DIR/peers-completion-XXXXXX"
	stage=1
fi
# FAKE_STAGE_PATH_LOG records the staging file mktemp created, so a later fake
# command can recreate a file at that path and the test can check whether the
# exit trap removes it again.
if [ -n "${FAKE_STAGE_PATH_LOG:-}" ] && [ "${stage:-0}" = 1 ]; then
	created=$(@REAL_MKTEMP@ "$@") || exit $?
	printf '%s\n' "$created" >> "$FAKE_STAGE_PATH_LOG"
	printf '%s\n' "$created"
	exit 0
fi
exec @REAL_MKTEMP@ "$@"
`

// fakeChmod and fakeMv log the call with the account they would run as.
// FAKE_CHMOD_RESTORE_STAGE makes the chmod of the completion's temporary file,
// which runs after the install removed its staging file, recreate a file at the
// recorded staging path. It stands for a process that writes there between the
// removal and the exit; the exit trap must not remove that file.
const fakeChmod = `#!/bin/sh
printf 'chmod as=%s %s\n' "${FAKE_SUDO_DROPPED:-root}" "$*" >> "${FAKE_WRITE_LOG:-/dev/null}"
if [ -n "${FAKE_CHMOD_RESTORE_STAGE:-}" ] && [ -n "${FAKE_STAGE_PATH_LOG:-}" ]; then
	case $2 in
	*/.peers-*)
		stage=$(sed -n '1p' "$FAKE_STAGE_PATH_LOG")
		if [ -n "$stage" ] && [ ! -e "$stage" ]; then
			printf 'recreated after the install removed the staging file\n' > "$stage" || exit 1
		fi
		;;
	esac
fi
exec @REAL_CHMOD@ "$@"
`

const fakeMv = `#!/bin/sh
printf 'mv as=%s %s\n' "${FAKE_SUDO_DROPPED:-root}" "$*" >> "${FAKE_WRITE_LOG:-/dev/null}"
exec @REAL_MV@ "$@"
`

// fakeCp logs the call with the account it would run as. When the destination
// is a symbolic link and the call runs as the dropped account, it fails the way
// that account's open of a file it cannot write does, so a test can show that
// root no longer writes through the link.
const fakeCp = `#!/bin/sh
printf 'cp as=%s %s\n' "${FAKE_SUDO_DROPPED:-root}" "$*" >> "${FAKE_WRITE_LOG:-/dev/null}"
if [ -n "${FAKE_CP_REFUSE_SYMLINK:-}" ] && [ -n "${FAKE_SUDO_DROPPED:-}" ]; then
	for arg in "$@"; do
		dest=$arg
	done
	if [ -L "$dest" ]; then
		exit 1
	fi
fi
exec @REAL_CP@ "$@"
`

// fakeMkdir logs the call with the account it would run as. FAKE_MKDIR_SWAP_DIR
// reproduces the ancestor swap: right before the directory below it is created,
// that directory is replaced by a link to FAKE_MKDIR_SWAP_TARGET, standing for
// the account that owns a higher ancestor swapping the checked parent after the
// check. FAKE_MKDIR_REFUSE_DROPPED makes the call fail for the account the
// installer dropped to, which cannot create a directory in the root-owned target
// the link points at.
const fakeMkdir = `#!/bin/sh
printf 'mkdir as=%s %s\n' "${FAKE_SUDO_DROPPED:-root}" "$*" >> "${FAKE_WRITE_LOG:-/dev/null}"
for arg in "$@"; do
	dest=$arg
done
if [ -n "${FAKE_MKDIR_SWAP_DIR:-}" ] && [ "$dest" = "$FAKE_MKDIR_SWAP_DIR/site-functions" ]; then
	if [ ! -L "$FAKE_MKDIR_SWAP_DIR" ]; then
		rmdir "$FAKE_MKDIR_SWAP_DIR" || exit 1
		ln -s "$FAKE_MKDIR_SWAP_TARGET" "$FAKE_MKDIR_SWAP_DIR" || exit 1
	fi
	if [ -n "${FAKE_MKDIR_REFUSE_DROPPED:-}" ] && [ -n "${FAKE_SUDO_DROPPED:-}" ]; then
		exit 1
	fi
fi
exec @REAL_MKDIR@ "$@"
`

// writeExecutable writes a file with the executable bit set.
func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// writeRegistered writes the output claude mcp get returns for a registered
// peer server whose command is the given path.
func writeRegistered(t *testing.T, path, command string) {
	t.Helper()
	content := "peer:\n  Scope: User config\n  Command: " + command + "\n  Args: mcp\n"
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

// mustLookPath resolves a real command before the fake bin directory is on
// PATH, so a fake can delegate to it.
func mustLookPath(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Fatalf("looking up %s: %v", name, err)
	}
	return path
}

// shellQuote returns a single-quoted shell word.
func shellQuote(value string) string {
	return "'" + strings.ReplaceAll(value, "'", `'\''`) + "'"
}

// readLines returns the non-empty lines of a file, or nil when it does not
// exist.
func readLines(t *testing.T, path string) []string {
	t.Helper()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	var lines []string
	for _, line := range strings.Split(string(data), "\n") {
		if line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}

// readFile returns the contents of a file the test expects the script to write.
func readFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	return string(data)
}

// indexOf returns the position of the first line that starts with prefix, or
// -1 when there is none.
func indexOf(lines []string, prefix string) int {
	for i, line := range lines {
		if strings.HasPrefix(line, prefix) {
			return i
		}
	}
	return -1
}

// countLines counts the lines that start with prefix.
func countLines(lines []string, prefix string) int {
	count := 0
	for _, line := range lines {
		if strings.HasPrefix(line, prefix) {
			count++
		}
	}
	return count
}

func mustContain(t *testing.T, what, got, want string) {
	t.Helper()
	if !strings.Contains(got, want) {
		t.Errorf("%s = %q, want it to contain %q", what, got, want)
	}
}

func mustNotContain(t *testing.T, what, got, want string) {
	t.Helper()
	if strings.Contains(got, want) {
		t.Errorf("%s = %q, want it not to contain %q", what, got, want)
	}
}
