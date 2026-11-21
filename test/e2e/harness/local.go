package harness

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/procsig"
	"github.com/wangzw/peers/test/e2e/node"
)

// localNode runs a node's processes directly on the test host.
type localNode struct {
	t    *testing.T
	name string
	bin  string
	root string
	acct string
	home string
	env  BaseEnv

	cleanupOnce sync.Once

	mu            sync.Mutex
	sessions      []*Session
	oracles       []*OracleClient
	oracleClient  *OracleClient
	broker        *node.BrokerProc
	brokerLogPath string
	brokers       int
	freeAddrs     map[string]bool
	reservedPorts []int
}

// NewLocalHost starts a node that runs on the test host. Its runtime root and
// account home are fresh temporary directories, so it shares neither the
// account lock nor ~/.peers with the developer or with another node. All
// processes and directories are removed when the test ends.
func NewLocalHost(t *testing.T, opts LocalOptions) *Node {
	t.Helper()

	bin := BuildBinary(t, runtime.GOOS, runtime.GOARCH, "peers_testenv")

	// A short path keeps the Broker's Unix socket under the length limit. The
	// name carries the owner marker, including its PID namespace, so a cleanup
	// can leave a live suite's root alone and remove one whose owner is gone.
	root, err := os.MkdirTemp("/tmp", "ape2e-"+currentOwner().String()+"-")
	if err != nil {
		t.Fatalf("harness: creating runtime root: %v", err)
	}
	acct := t.TempDir()
	home := t.TempDir()

	l := &localNode{
		t:    t,
		name: opts.Name,
		bin:  bin,
		root: root,
		acct: acct,
		home: home,
		env: BaseEnv{
			Home: home,
			Path: os.Getenv("PATH"),
			Extra: map[string]string{
				"PEERS_TEST_RUNTIME_ROOT": root,
				"PEERS_TEST_HOME":         acct,
			},
		},
		freeAddrs: map[string]bool{},
	}
	t.Cleanup(l.cleanup)
	return &Node{impl: l}
}

// brokerSocket is the local socket of the node's Broker.
func (l *localNode) brokerSocket() string {
	return filepath.Join(l.root, strconv.Itoa(os.Geteuid()), "broker.sock")
}

// configPath is the account's configuration file.
func (l *localNode) configPath() string {
	return filepath.Join(l.acct, ".peers", "config.yaml")
}

func (l *localNode) accountHome() string { return l.acct }

func (l *localNode) freeAddr(t testing.TB) string {
	t.Helper()
	addr, port, err := reservePort(currentOwner())
	if err != nil {
		t.Fatalf("harness: node %s: reserving a free address: %v", l.name, err)
		return ""
	}
	l.mu.Lock()
	l.freeAddrs[addr] = true
	l.reservedPorts = append(l.reservedPorts, port)
	l.mu.Unlock()
	return addr
}

func (l *localNode) writeConfig(t testing.TB, yaml string) {
	t.Helper()
	dir := filepath.Join(l.acct, ".peers")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("harness: node %s: creating config dir %s: %v", l.name, dir, err)
		return
	}

	// A Broker may be watching the directory, so write a complete file and
	// rename it into place; a reader never sees a half-written configuration.
	tmp, err := os.CreateTemp(dir, "config-*.yaml")
	if err != nil {
		t.Fatalf("harness: node %s: creating temporary config: %v", l.name, err)
		return
	}
	tmpName := tmp.Name()
	if _, err := tmp.WriteString(yaml); err != nil {
		_ = tmp.Close()
		_ = os.Remove(tmpName)
		t.Fatalf("harness: node %s: writing %s: %v", l.name, tmpName, err)
		return
	}
	if err := tmp.Close(); err != nil {
		_ = os.Remove(tmpName)
		t.Fatalf("harness: node %s: closing %s: %v", l.name, tmpName, err)
		return
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		_ = os.Remove(tmpName)
		t.Fatalf("harness: node %s: setting mode of %s: %v", l.name, tmpName, err)
		return
	}
	if err := os.Rename(tmpName, l.configPath()); err != nil {
		_ = os.Remove(tmpName)
		t.Fatalf("harness: node %s: installing %s: %v", l.name, l.configPath(), err)
	}
}

func (l *localNode) readConfig(t testing.TB) []byte {
	t.Helper()
	data, err := os.ReadFile(l.configPath())
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		t.Fatalf("harness: node %s: reading %s: %v", l.name, l.configPath(), err)
		return nil
	}
	return data
}

// readConfigMaybe returns the configuration file without failing the test.
func (l *localNode) readConfigMaybe() ([]byte, bool) {
	data, err := os.ReadFile(l.configPath())
	if err != nil {
		return nil, false
	}
	return data, true
}

func (l *localNode) readFile(t testing.TB, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("harness: node %s: reading %s: %v", l.name, path, err)
		return nil
	}
	return data
}

// workspace creates a workspace directly on the test host, using the same
// helpers the container's node agent uses.
func (l *localNode) workspace(t testing.TB, kind, origin, projectYAML string) string {
	t.Helper()
	dir := t.TempDir()
	env := l.env.Build(map[string]string{"GIT_CONFIG_NOSYSTEM": "1"})
	var (
		path string
		err  error
	)
	switch kind {
	case "git":
		path, err = node.GitWorkspace(dir, origin, env)
	case "project":
		path, err = node.ProjectWorkspace(dir, projectYAML, origin, env)
	case "plain":
		path, err = node.PlainWorkspace(dir)
	default:
		t.Fatalf("harness: node %s: unknown workspace kind %q", l.name, kind)
		return ""
	}
	if err != nil {
		t.Fatalf("harness: node %s: creating a %s workspace: %v", l.name, kind, err)
		return ""
	}
	return path
}

func (l *localNode) exec(t testing.TB, argv []string, env map[string]string) ExecResult {
	t.Helper()
	return l.execWithin(t, argv, env, 0)
}

func (l *localNode) execWithin(t testing.TB, argv []string, env map[string]string, timeout time.Duration) ExecResult {
	t.Helper()
	if len(argv) == 0 {
		t.Fatalf("harness: node %s: Exec needs a command", l.name)
		return ExecResult{}
	}

	var ctx context.Context
	var cancel context.CancelFunc
	if timeout > 0 {
		ctx, cancel = context.WithTimeout(context.Background(), timeout)
		defer cancel()
	} else {
		ctx, cancel = context.WithCancel(context.Background())
		defer cancel()
	}
	cmd := exec.CommandContext(ctx, argv[0], argv[1:]...)
	cmd.Dir = l.home
	cmd.Env = l.env.Build(env)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	result := ExecResult{}
	err := cmd.Run()
	var exitErr *exec.ExitError
	switch {
	case err == nil:
		result.ExitCode = 0
	case errors.As(err, &exitErr):
		result.ExitCode = exitErr.ExitCode()
	case ctx.Err() != nil:
		t.Fatalf("harness: node %s: %v did not finish within %s", l.name, argv, timeout)
	default:
		t.Fatalf("harness: node %s: running %v: %v", l.name, argv, err)
	}
	result.Stdout = stdout.String()
	result.Stderr = stderr.String()
	return result
}

func (l *localNode) startSession(tb testing.TB, spec SessionSpec) *Session {
	tb.Helper()

	// StartSession completes the MCP initialize handshake before it returns;
	// the deadline is the startup allowance, not the project-wide test wait.
	ctx, cancel := context.WithTimeout(context.Background(), sessionStartBound)
	defer cancel()

	session, err := node.StartSession(ctx, tb, l.env, node.MCPCommand{Path: l.bin, Args: []string{"mcp"}}, spec)
	if err != nil {
		tb.Fatalf("harness: node %s: starting session %s: %v", l.name, spec.Name, err)
		return nil
	}

	s := &Session{
		name: spec.Name,
		impl: &localSession{name: spec.Name, session: session},
	}
	l.mu.Lock()
	l.sessions = append(l.sessions, s)
	l.mu.Unlock()
	return s
}

// tryStartSession starts a session without failing the test. Everything
// fakesession would report through the test goes into a deferredTB instead:
// a test that drove the start from another goroutine reports those errors
// after it has joined the goroutine.
func (l *localNode) tryStartSession(spec SessionSpec) (*Session, error) {
	ctx, cancel := context.WithTimeout(context.Background(), sessionStartBound)
	defer cancel()

	tb := &deferredTB{TB: l.t}
	var (
		session  *node.Session
		startErr error
	)
	if err := node.Guard(func() {
		session, startErr = node.StartSession(ctx, tb, l.env, node.MCPCommand{Path: l.bin, Args: []string{"mcp"}}, spec)
	}); err != nil {
		return nil, fmt.Errorf("harness: node %s: starting session %s: %w", l.name, spec.Name, err)
	}
	if startErr != nil {
		return nil, fmt.Errorf("harness: node %s: starting session %s: %w", l.name, spec.Name, startErr)
	}
	if errs := tb.TakeErrors(); len(errs) > 0 {
		return nil, fmt.Errorf("harness: node %s: starting session %s: %s", l.name, spec.Name, strings.Join(errs, "; "))
	}

	s := &Session{
		name: spec.Name,
		impl: &localSession{name: spec.Name, session: session, deferred: tb},
	}
	l.mu.Lock()
	l.sessions = append(l.sessions, s)
	l.mu.Unlock()
	return s, nil
}

// deferredTB records the failures fakesession reports, so a session started
// from another goroutine can hand them to the test goroutine instead of
// failing the test from that goroutine. Temporary directories, cleanups and
// the test name still come from the real testing.TB.
type deferredTB struct {
	testing.TB

	mu   sync.Mutex
	errs []string
}

func (d *deferredTB) Helper() {}

func (d *deferredTB) Errorf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	d.mu.Lock()
	d.errs = append(d.errs, msg)
	d.mu.Unlock()
}

func (d *deferredTB) Fatalf(format string, args ...any) {
	msg := fmt.Sprintf(format, args...)
	d.mu.Lock()
	d.errs = append(d.errs, msg)
	d.mu.Unlock()
	panic(node.FatalError{Msg: msg})
}

// TakeErrors returns the recorded failures and forgets them.
func (d *deferredTB) TakeErrors() []string {
	d.mu.Lock()
	errs := d.errs
	d.errs = nil
	d.mu.Unlock()
	return errs
}

// localSession adapts node.Session to the harness session interface, turning
// errors into test failures.
type localSession struct {
	name    string
	session *node.Session
	// deferred carries the failures fakesession reports when the session was
	// started through TryStartSession. A session started on the test goroutine
	// reports them through the real testing.T immediately.
	deferred *deferredTB
}

func (s *localSession) callErrWithin(bound time.Duration, tool string, args map[string]any) (Result, error) {
	ctx := context.Background()
	if bound > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, bound)
		defer cancel()
	}
	result, err := s.session.Call(ctx, tool, args)
	if err != nil {
		return Result{}, fmt.Errorf("harness: session %s: calling %s: %w", s.name, tool, err)
	}
	return Result{ToolResult: result}, nil
}

func (s *localSession) mcpLogTail(lines int) (string, bool) { return s.session.MCPLogTail(lines) }

func (s *localSession) tools(t testing.TB) []node.ToolInfo {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	tools, err := s.session.Tools(ctx)
	if err != nil {
		t.Fatalf("harness: session %s: listing tools: %v", s.name, err)
		return nil
	}
	return tools
}

func (s *localSession) stopMCP(t testing.TB, sig syscall.Signal) int {
	t.Helper()
	code, err := s.session.StopMCP(sig)
	if err != nil {
		t.Fatalf("harness: session %s: stopping the MCP server process: %v", s.name, err)
		return -1
	}
	return code
}

func (s *localSession) restartMCP(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), sessionStartBound)
	defer cancel()
	if err := s.session.RestartMCP(ctx); err != nil {
		t.Fatalf("harness: session %s: restarting the MCP server process: %v", s.name, err)
	}
}

func (s *localSession) startExtraMCP(t testing.TB) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), sessionStartBound)
	defer cancel()
	if _, err := s.session.StartExtraMCP(ctx); err != nil {
		t.Fatalf("harness: session %s: starting an extra MCP server process: %v", s.name, err)
	}
}

func (s *localSession) inboxConnections(t testing.TB) int {
	t.Helper()
	count, err := s.session.InboxConnections()
	if err != nil {
		t.Fatalf("harness: session %s: counting inbox connections: %v", s.name, err)
		return 0
	}
	return count
}

func (s *localSession) fake() FakeSession { return s.session.Fake() }

func (s *localSession) takeFakeErrors(t testing.TB) []error {
	t.Helper()
	return s.session.Fake().TakeErrors()
}

func (s *localSession) end() error {
	// A background failure never reaches the test on its own, so the end of
	// the session is where it is reported.
	fakeErrs := s.session.Fake().TakeErrors()
	err := s.session.End()
	if s.deferred != nil {
		if errs := s.deferred.TakeErrors(); len(errs) > 0 {
			fakeErrs = append(fakeErrs, stringErrors(errs)...)
		}
	}
	if len(fakeErrs) > 0 {
		background := fmt.Errorf("harness: session %s: simulated session failures: %s", s.name, joinErrors(fakeErrs))
		if err == nil {
			return background
		}
		return fmt.Errorf("%v; %w", err, background)
	}
	return err
}

// stringErrors turns recorded messages back into errors.
func stringErrors(messages []string) []error {
	errs := make([]error, 0, len(messages))
	for _, message := range messages {
		errs = append(errs, errors.New(message))
	}
	return errs
}

// joinErrors joins error messages with a semicolon.
func joinErrors(errs []error) string {
	parts := make([]string, 0, len(errs))
	for _, err := range errs {
		parts = append(parts, err.Error())
	}
	return strings.Join(parts, "; ")
}

// cleanup ends the node's sessions, stops its Broker, kills every process that
// still carries this node's runtime root and removes the root. It is
// idempotent.
func (l *localNode) cleanup() {
	l.cleanupOnce.Do(func() {
		l.mu.Lock()
		sessions := l.sessions
		l.sessions = nil
		broker := l.broker
		l.broker = nil
		oracles := l.oracles
		l.oracles = nil
		ports := l.reservedPorts
		l.reservedPorts = nil
		l.mu.Unlock()

		for _, s := range sessions {
			if err := s.endIfNotEnded(); err != nil {
				l.t.Errorf("harness: node %s: ending session %s: %v", l.name, s.name, err)
			}
		}
		for _, o := range oracles {
			o.close()
		}
		if broker != nil {
			terminateBroker(broker)
		}

		l.killProcesses()

		// The ports are free once the processes are gone, so another suite may
		// take them.
		releasePorts(ports, currentOwner())

		if err := os.RemoveAll(l.root); err != nil {
			l.t.Errorf("harness: node %s: removing runtime root %s: %v", l.name, l.root, err)
		}
	})
}

// terminateBroker asks the Broker to exit with SIGTERM and kills it when it has
// not exited within two seconds.
func terminateBroker(b *node.BrokerProc) {
	_ = b.Signal(syscall.SIGTERM)
	if _, err := b.Wait(2 * time.Second); err == nil {
		return
	}
	_ = b.Signal(syscall.SIGKILL)
	_, _ = b.Wait(2 * time.Second)
}

// killProcesses kills every process whose environment carries this node's
// runtime root, including a Broker that detached from the process that started
// it. The scan is the node library's; no second implementation lives here.
func (l *localNode) killProcesses() {
	procs, err := node.ListProcs(func(p node.Proc) bool {
		return p.Env["PEERS_TEST_RUNTIME_ROOT"] == l.root
	})
	if err != nil {
		// Other platforms cannot scan processes; the processes the node
		// started itself have already been stopped above.
		if !errors.Is(err, errors.ErrUnsupported) {
			l.t.Errorf("harness: node %s: listing processes: %v", l.name, err)
		}
		return
	}
	self := os.Getpid()
	for _, proc := range procs {
		if proc.PID == self {
			continue
		}
		_ = procsig.SendSignal(proc.PID, syscall.SIGKILL)
	}
}

// logHasAddressInUse reports whether the Broker log contains the failure that
// the one retry accepts as a port race.
func logHasAddressInUse(path string) bool {
	data, err := os.ReadFile(path)
	if err != nil {
		return false
	}
	return strings.Contains(string(data), "address already in use")
}

// replaceFreeAddrs replaces every address this node handed out through
// FreeAddr that appears in args or configText with a fresh one. Addresses the
// test obtained elsewhere are left alone, so a scenario that deliberately
// occupies a port still sees the Broker fail. The returned map records each
// replacement so callers can learn the address the Broker actually used.
func (l *localNode) replaceFreeAddrs(tb testing.TB, args []string, configText string) ([]string, string, map[string]string) {
	l.mu.Lock()
	addrs := make([]string, 0, len(l.freeAddrs))
	for addr := range l.freeAddrs {
		addrs = append(addrs, addr)
	}
	l.mu.Unlock()
	// Longer addresses first, so an address that is a prefix of another is not
	// replaced inside it.
	sort.Slice(addrs, func(i, j int) bool { return len(addrs[i]) > len(addrs[j]) })

	replacements := map[string]string{}
	replace := func(text string) string {
		for _, old := range addrs {
			if !strings.Contains(text, old) {
				continue
			}
			fresh, ok := replacements[old]
			if !ok {
				fresh = l.freeAddr(tb)
				replacements[old] = fresh
			}
			text = strings.ReplaceAll(text, old, fresh)
		}
		return text
	}

	newArgs := make([]string, len(args))
	for i, arg := range args {
		newArgs[i] = replace(arg)
	}
	return newArgs, replace(configText), replacements
}

func (l *localNode) startBroker(tb testing.TB, spec BrokerSpec) BrokerStart {
	tb.Helper()

	l.mu.Lock()
	if l.broker != nil {
		l.mu.Unlock()
		tb.Fatalf("harness: node %s: a Broker is already running", l.name)
		return BrokerStart{}
	}
	l.mu.Unlock()

	args := append([]string(nil), spec.Args...)
	configText := spec.ConfigYAML
	if configText == "" {
		// Retrying rewrites the file the Broker is about to read, so keep its
		// current contents at hand even when the caller did not pass any.
		if data, ok := l.readConfigMaybe(); ok {
			configText = string(data)
		}
	}
	if spec.ConfigYAML != "" {
		l.writeConfig(tb, spec.ConfigYAML)
	}

	// replaced records the address changes the one retry made, so the caller
	// learns the address the Broker actually used.
	var replaced map[string]string

	for attempt := 0; ; attempt++ {
		l.mu.Lock()
		l.brokers++
		logPath := filepath.Join(l.root, fmt.Sprintf("broker-%d.log", l.brokers))
		l.brokerLogPath = logPath
		l.mu.Unlock()

		proc, start, err := node.StartBroker(context.Background(), node.BrokerSpec{
			Path:    l.bin,
			Args:    args,
			Env:     l.env.Build(spec.Env),
			LogPath: logPath,
			Socket:  l.brokerSocket(),
		}, spec.Bound)
		if err != nil {
			tb.Fatalf("harness: node %s: starting Broker: %v", l.name, err)
			return BrokerStart{}
		}
		if start.Running {
			l.mu.Lock()
			l.broker = proc
			l.mu.Unlock()
			start.Addrs = replaced
			return start
		}

		// A port handed out by FreeAddr can be taken between the probe and the
		// bind. Accept that race once: move every such address to a new one,
		// rewrite the configuration file, and start again.
		if attempt == 0 && logHasAddressInUse(logPath) {
			newArgs, newConfig, replacements := l.replaceFreeAddrs(tb, args, configText)
			if len(replacements) > 0 {
				if newConfig != "" {
					l.writeConfig(tb, newConfig)
				}
				args = newArgs
				configText = newConfig
				replaced = replacements
				continue
			}
		}
		start.Addrs = replaced
		return start
	}
}

func (l *localNode) stopBroker(tb testing.TB, sig syscall.Signal) int {
	tb.Helper()

	l.mu.Lock()
	broker := l.broker
	l.mu.Unlock()
	if broker == nil {
		tb.Fatalf("harness: node %s: no Broker is running", l.name)
		return -1
	}

	// A signal to a process that already exited reports an error; the exit
	// code below still tells the caller what happened.
	_ = broker.Signal(sig)
	// The Broker exits as soon as its context is cancelled and has no drain
	// timer; the same bound the container path uses applies here.
	code, err := broker.Wait(brokerStopBound)
	if err != nil {
		_ = broker.Signal(syscall.SIGKILL)
		code, _ = broker.Wait(2 * time.Second)
	}

	l.mu.Lock()
	if l.broker == broker {
		l.broker = nil
	}
	l.mu.Unlock()
	return code
}

func (l *localNode) brokerLog(tb testing.TB) []byte {
	tb.Helper()

	l.mu.Lock()
	path := l.brokerLogPath
	l.mu.Unlock()
	if path == "" {
		return nil
	}
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		tb.Fatalf("harness: node %s: reading Broker log %s: %v", l.name, path, err)
		return nil
	}
	return data
}

// oracle returns the node's one Oracle client, dialing on the first call. The
// Broker caps how many local connections one account may hold, so a client per
// call would let a polling test exhaust that limit although it uses one
// connection at a time.
func (l *localNode) oracle() *OracleClient {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.oracleClient != nil {
		return l.oracleClient
	}
	oracle, err := node.DialOracle(l.brokerSocket())
	if err != nil {
		l.t.Fatalf("harness: node %s: connecting to the local socket: %v", l.name, err)
		return nil
	}
	o := &OracleClient{oracle: oracle}
	l.oracles = append(l.oracles, o)
	l.oracleClient = o
	return o
}
