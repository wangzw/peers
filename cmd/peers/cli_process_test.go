package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// lineWriter is a concurrency-safe io.Writer for a child's standard error that
// delivers each complete line on lines. os/exec copies the child's output from
// its own goroutine while the test reads, so both the buffer and the line
// splitting are guarded. Using cmd.Stderr rather than cmd.StderrPipe leaves the
// pipe owned by os/exec, so Wait may run concurrently with the test's reads
// without racing a pipe close.
type lineWriter struct {
	mu    sync.Mutex
	buf   bytes.Buffer
	lines chan string
}

func newLineWriter() *lineWriter {
	return &lineWriter{lines: make(chan string, 16)}
}

func (w *lineWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()

	w.buf.Write(p)
	for {
		data := w.buf.Bytes()
		i := bytes.IndexByte(data, '\n')
		if i < 0 {
			break
		}
		line := strings.TrimRight(string(data[:i]), "\r")
		w.buf.Next(i + 1)
		w.lines <- line
	}
	return len(p), nil
}

// cliResult is one finished terminal command.
type cliResult struct {
	stdout string
	stderr string
	code   int
}

// runCLI runs the test binary with the sandbox environment and collects its
// streams and exit code. A command that does not exit within timeout is
// killed and fails the test.
func runCLI(t *testing.T, sb *sandbox, timeout time.Duration, args ...string) cliResult {
	t.Helper()

	var stdout, stderr lockedBuffer
	cmd := exec.Command(testBinary, args...)
	cmd.Env = sb.env()
	cmd.Dir = sb.home
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	cmd.WaitDelay = waitDelay

	if err := cmd.Start(); err != nil {
		t.Fatalf("start peers %s: %v", strings.Join(args, " "), err)
	}
	done := make(chan struct{}, 1)
	go func() {
		_ = cmd.Wait()
		done <- struct{}{}
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		_ = cmd.Process.Kill()
		<-done
		t.Fatalf("peers %s did not exit within %s (stdout %q, stderr %q)",
			strings.Join(args, " "), timeout, stdout.String(), stderr.String())
	}
	return cliResult{stdout: stdout.String(), stderr: stderr.String(), code: cmd.ProcessState.ExitCode()}
}

// lineResult is one line read from the MCP server's standard output, or the
// read error that ended the stream.
type lineResult struct {
	line string
	err  error
}

// mcpProc is a "peers mcp" child with connected standard streams.
type mcpProc struct {
	t      *testing.T
	cmd    *exec.Cmd
	stdin  *os.File
	lines  chan lineResult
	stderr *lockedBuffer

	exited chan struct{}
}

// startMCP starts "peers mcp" with the sandbox environment. Standard
// output is read line by line in the background, because the MCP protocol is
// newline-delimited JSON on that stream.
func startMCP(t *testing.T, sb *sandbox) *mcpProc {
	t.Helper()

	inR, inW, err := os.Pipe()
	if err != nil {
		t.Fatalf("create stdin pipe: %v", err)
	}
	outR, outW, err := os.Pipe()
	if err != nil {
		_ = inR.Close()
		_ = inW.Close()
		t.Fatalf("create stdout pipe: %v", err)
	}

	stderr := &lockedBuffer{}
	cmd := exec.Command(testBinary, "mcp")
	cmd.Env = sb.env()
	cmd.Dir = sb.home
	cmd.Stdin = inR
	cmd.Stdout = outW
	cmd.Stderr = stderr
	cmd.WaitDelay = waitDelay

	if err := cmd.Start(); err != nil {
		_ = inR.Close()
		_ = inW.Close()
		_ = outR.Close()
		_ = outW.Close()
		t.Fatalf("start peers mcp: %v", err)
	}
	// The child holds the other ends. Close the parent's copies so the reader
	// sees EOF when the child exits and the writer sees a broken pipe.
	_ = inR.Close()
	_ = outW.Close()

	p := &mcpProc{
		t:      t,
		cmd:    cmd,
		stdin:  inW,
		lines:  make(chan lineResult, 16),
		stderr: stderr,
		exited: make(chan struct{}),
	}
	go func() {
		reader := bufio.NewReader(outR)
		for {
			line, err := reader.ReadString('\n')
			if line != "" {
				p.lines <- lineResult{line: strings.TrimRight(line, "\r\n")}
			}
			if err != nil {
				p.lines <- lineResult{err: err}
				close(p.lines)
				return
			}
		}
	}()
	go func() {
		_ = cmd.Wait()
		close(p.exited)
	}()
	t.Cleanup(func() {
		_ = inW.Close()
		_ = outR.Close()
		p.killIfRunning()
	})
	return p
}

// writeLine sends one line to the MCP server's standard input.
func (p *mcpProc) writeLine(line string) {
	p.t.Helper()

	if _, err := io.WriteString(p.stdin, line+"\n"); err != nil {
		p.t.Fatalf("write to mcp stdin: %v (stderr: %s)", err, p.stderr.String())
	}
}

// nextLine waits for one line of the MCP server's standard output.
func (p *mcpProc) nextLine(timeout time.Duration) string {
	p.t.Helper()

	select {
	case r, ok := <-p.lines:
		if !ok {
			p.t.Fatalf("mcp stdout closed (stderr: %s)", p.stderr.String())
		}
		if r.err != nil {
			p.t.Fatalf("read mcp stdout: %v (stderr: %s)", r.err, p.stderr.String())
		}
		return r.line
	case <-time.After(timeout):
		p.t.Fatalf("no output from mcp within %s (stderr: %s)", timeout, p.stderr.String())
		return ""
	}
}

// initialize performs the MCP handshake and checks the result frame. The
// handshake must complete before signals are tested, so a failure to answer is
// not confused with a failure to exit.
func (p *mcpProc) initialize() {
	p.t.Helper()

	p.writeLine(`{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2025-06-18","capabilities":{},"clientInfo":{"name":"cmdtest","version":"0"}}}`)
	raw := p.nextLine(processTimeout)
	var msg map[string]any
	if err := json.Unmarshal([]byte(raw), &msg); err != nil {
		p.t.Fatalf("initialize response is not JSON: %v (line %q)", err, raw)
	}
	if id, ok := msg["id"].(float64); !ok || id != 1 {
		p.t.Fatalf("initialize response id = %v, want 1 (line %q)", msg["id"], raw)
	}
	if _, ok := msg["result"]; !ok {
		p.t.Fatalf("initialize response has no result (line %q)", raw)
	}
}

// signal sends sig to the MCP server.
func (p *mcpProc) signal(sig os.Signal) {
	p.t.Helper()

	if err := p.cmd.Process.Signal(sig); err != nil {
		p.t.Fatalf("signal mcp: %v", err)
	}
}

// wait waits for the MCP server to exit and returns its exit code.
func (p *mcpProc) wait(timeout time.Duration) int {
	p.t.Helper()

	select {
	case <-p.exited:
	case <-time.After(timeout):
		p.t.Fatalf("mcp did not exit within %s (stderr: %s)", timeout, p.stderr.String())
	}
	return p.cmd.ProcessState.ExitCode()
}

func (p *mcpProc) killIfRunning() {
	select {
	case <-p.exited:
		return
	default:
	}
	_ = p.cmd.Process.Kill()
	<-p.exited
}

// assertNoBrokerFiles fails when root (searched recursively) contains a lock
// or socket, which proves a Broker was launched.
func assertNoBrokerFiles(t *testing.T, root string) {
	t.Helper()

	err := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		switch d.Name() {
		case "broker.lock", "broker.sock":
			t.Errorf("a Broker left %s behind", path)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk %s: %v", root, err)
	}
}

func countCalls(calls []fakelocal.Call, method string) int {
	n := 0
	for _, c := range calls {
		if c.Method == method {
			n++
		}
	}
	return n
}

func TestBinaryVersionFromLdflags(t *testing.T) {
	t.Parallel()

	res := runCLI(t, newSandbox(t), processTimeout, "--version")
	if res.code != 0 {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", res.code, res.stderr)
	}
	if want := "peers 0.0.0-cmdtest\n"; res.stdout != want {
		t.Fatalf("stdout = %q, want %q", res.stdout, want)
	}
	if res.stderr != "" {
		t.Fatalf("stderr = %q, want empty", res.stderr)
	}
}

func TestBinaryUnknownCommandExitCode(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)

	res := runCLI(t, sb, processTimeout, "nope")
	if res.code != exitUsage {
		t.Fatalf("unknown command exit code = %d, want %d (stderr %q)", res.code, exitUsage, res.stderr)
	}

	// A bad flag fails before the command reaches the installation, which would
	// otherwise write a unit file and run systemctl against the real system.
	res = runCLI(t, sb, processTimeout, "service", "install", "--bogus")
	if res.code != exitUsage {
		t.Fatalf("service install --bogus exit code = %d, want %d (stderr %q)", res.code, exitUsage, res.stderr)
	}
	if !strings.Contains(res.stderr, "unknown flag: --bogus") {
		t.Fatalf("service install --bogus stderr = %q, want it to name the bad flag", res.stderr)
	}
}

// TestStatusThroughExplicitSocket covers REQ-broker-config-010: status reads
// the agents, topics and peers from the selected Broker.
func TestStatusThroughExplicitSocket(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	sock := filepath.Join(sb.root, "x.sock")
	fakelocal.Start(t, sock, fakelocal.SampleHandlers(nil, nil))
	sb.extraEnv = append(sb.extraEnv, "PEERS_TEST_SOCKET="+sock)

	res := runCLI(t, sb, processTimeout, "service", "status")
	if res.code != exitOK {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", res.code, res.stderr)
	}
	if want := fakelocal.SampleStatusText(); res.stdout != want {
		t.Fatalf("stdout:\ngot  %q\nwant %q", res.stdout, want)
	}
	if res.stderr != "" {
		t.Fatalf("stderr = %q, want empty", res.stderr)
	}
}

func TestStatusExplicitSocketUnavailable(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	missing := filepath.Join(sb.root, "missing.sock")

	// A user Broker that would answer if the command fell back to it.
	dir := sb.runtimeDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create runtime directory: %v", err)
	}
	fake := fakelocal.Start(t, filepath.Join(dir, "broker.sock"), fakelocal.SampleHandlers(nil, nil))

	sb.extraEnv = append(sb.extraEnv, "PEERS_TEST_SOCKET="+missing)

	res := runCLI(t, sb, processTimeout, "service", "status")
	if res.code != exitFailure {
		t.Fatalf("exit code = %d, want %d (stderr %q)", res.code, exitFailure, res.stderr)
	}
	if !strings.Contains(res.stderr, "missing.sock") {
		t.Fatalf("stderr = %q, want it to name %q", res.stderr, missing)
	}
	if !strings.Contains(res.stderr, "cannot reach the Broker") {
		t.Fatalf("stderr = %q, want it to contain %q", res.stderr, "cannot reach the Broker")
	}
	if n := countCalls(fake.Calls(), "Status"); n != 0 {
		t.Fatalf("the user fake served %d Status calls; the command must not fall back to it", n)
	}
}

func TestStatusPrefersUserBroker(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	dir := sb.runtimeDir()
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("create runtime directory: %v", err)
	}
	fake := fakelocal.Start(t, filepath.Join(dir, "broker.sock"), fakelocal.SampleHandlers(nil, nil))

	res := runCLI(t, sb, processTimeout, "service", "status")
	if res.code != exitOK {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", res.code, res.stderr)
	}
	if want := fakelocal.SampleStatusText(); res.stdout != want {
		t.Fatalf("stdout:\ngot  %q\nwant %q", res.stdout, want)
	}
	if n := countCalls(fake.Calls(), "Status"); n != 1 {
		t.Fatalf("Status calls = %d, want 1", n)
	}
}

func TestStatusDoesNotLaunchBroker(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)

	res := runCLI(t, sb, processTimeout, "service", "status")
	if res.code != exitFailure {
		t.Fatalf("exit code = %d, want %d (stderr %q)", res.code, exitFailure, res.stderr)
	}
	for _, want := range []string{"no Broker is available", "ask the administrator", "peers service install", "peers service run"} {
		if !strings.Contains(res.stderr, want) {
			t.Fatalf("stderr = %q, want it to contain %q", res.stderr, want)
		}
	}
	assertNoBrokerFiles(t, sb.root)
}

// TestAuditThroughExplicitSocket covers REQ-broker-config-009: audit streams
// the log from the selected Broker.
func TestAuditThroughExplicitSocket(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	sock := filepath.Join(sb.root, "x.sock")
	reqs := &fakelocal.AuditRequests{}
	fakelocal.Start(t, sock, fakelocal.SampleHandlers(reqs, nil))
	sb.extraEnv = append(sb.extraEnv, "PEERS_TEST_SOCKET="+sock)

	res := runCLI(t, sb, processTimeout, "service", "audit", "--limit", "2")
	if res.code != exitOK {
		t.Fatalf("exit code = %d, want 0 (stderr %q)", res.code, res.stderr)
	}
	if want := fakelocal.SampleAuditText(2); res.stdout != want {
		t.Fatalf("stdout:\ngot  %q\nwant %q", res.stdout, want)
	}
	if got := reqs.Limits(); len(got) != 1 || got[0] != 2 {
		t.Fatalf("audit limits = %v, want [2]", got)
	}
}

// TestAuditPermissionDenied covers REQ-broker-config-009: a caller without
// read permission gets a permission error.
func TestAuditPermissionDenied(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	sock := filepath.Join(sb.root, "x.sock")
	reqs := &fakelocal.AuditRequests{}
	fakelocal.Start(t, sock, fakelocal.SampleHandlers(reqs, fakelocal.PermissionDenied("not allowed")))
	sb.extraEnv = append(sb.extraEnv, "PEERS_TEST_SOCKET="+sock)

	res := runCLI(t, sb, processTimeout, "service", "audit")
	if res.code != exitFailure {
		t.Fatalf("exit code = %d, want %d (stderr %q)", res.code, exitFailure, res.stderr)
	}
	if want := "peers service audit: permission_denied: not allowed\n"; res.stderr != want {
		t.Fatalf("stderr = %q, want %q", res.stderr, want)
	}
	if res.stdout != "" {
		t.Fatalf("stdout = %q, want empty", res.stdout)
	}
}

func TestMCPHandshakeAndSignal(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	// An explicit socket that does not exist makes the selection fail without
	// launching a Broker.
	sb.extraEnv = append(sb.extraEnv, "PEERS_TEST_SOCKET="+filepath.Join(sb.root, "none.sock"))

	p := startMCP(t, sb)
	p.initialize()

	p.signal(syscall.SIGTERM)
	if code := p.wait(processTimeout); code != 0 {
		t.Fatalf("exit code after SIGTERM = %d, want 0 (stderr %s)", code, p.stderr.String())
	}
	assertNoBrokerFiles(t, sb.root)
}

// TestMCPSignalClosesRegistration covers REQ-agent-messaging-034: a signal
// closes the registration stream, so the agent is unregistered.
func TestMCPSignalClosesRegistration(t *testing.T) {
	t.Parallel()

	for _, tt := range []struct {
		name string
		sig  os.Signal
	}{
		{"SIGTERM", syscall.SIGTERM},
		{"SIGINT", os.Interrupt},
	} {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			sb := newSandbox(t)
			sock := filepath.Join(sb.root, "x.sock")
			fake := fakelocal.Start(t, sock, fakelocal.Handlers{})
			sb.extraEnv = append(sb.extraEnv,
				"PEERS_TEST_SOCKET="+sock,
				"CLAUDE_CODE_MESSAGING_SOCKET=/s",
			)

			p := startMCP(t, sb)
			p.initialize()

			ctx, cancel := context.WithTimeout(context.Background(), processTimeout)
			defer cancel()
			conn, err := fake.NextRegister(ctx)
			if err != nil {
				t.Fatalf("NextRegister: %v (stderr: %s)", err, p.stderr.String())
			}
			if got := conn.First.GetInboxSocketPath(); got != "/s" {
				t.Fatalf("first Register frame inbox socket = %q, want %q", got, "/s")
			}
			if err := conn.Complete("id1", "me@h", false); err != nil {
				t.Fatalf("complete registration: %v", err)
			}

			p.signal(tt.sig)
			if code := p.wait(processTimeout); code != 0 {
				t.Fatalf("exit code after %s = %d, want 0 (stderr %s)", tt.name, code, p.stderr.String())
			}

			select {
			case <-conn.Done():
			case <-time.After(processTimeout):
				t.Fatalf("the Register stream was still open after %s", tt.name)
			}
		})
	}
}

// TestSecondSignalTerminates checks the process-level signal contract: the
// first SIGTERM cancels the running command, and a second SIGTERM terminates
// the process even while the command is still shutting down.
func TestSecondSignalTerminates(t *testing.T) {
	t.Parallel()

	sb := newSandbox(t)
	cmd := exec.Command(testBinary, "internal", "signal-block")
	cmd.Env = sb.env()
	cmd.WaitDelay = waitDelay
	stderr := newLineWriter()
	cmd.Stderr = stderr

	if err := cmd.Start(); err != nil {
		t.Fatalf("start signal-block: %v", err)
	}

	exited := make(chan struct{})
	go func() {
		_ = cmd.Wait()
		close(exited)
	}()
	t.Cleanup(func() {
		select {
		case <-exited:
			return
		default:
		}
		_ = cmd.Process.Kill()
		select {
		case <-exited:
		case <-time.After(processTimeout):
			t.Errorf("signal-block process %d did not exit after SIGKILL", cmd.Process.Pid)
		}
	})

	// Read the lines the child hands to cmd.Stderr's writer while Wait runs in
	// its own goroutine; os/exec's copy goroutine drains the pipe into the
	// writer before Wait returns.
	nextLine := func(want string) {
		t.Helper()
		select {
		case line := <-stderr.lines:
			if line != want {
				t.Fatalf("stderr line = %q, want %q", line, want)
			}
		case <-time.After(processTimeout):
			t.Fatalf("no %q line from signal-block within %s", want, processTimeout)
		}
	}

	// Wait for the handler before signalling, so the first SIGTERM cannot race
	// the installation of the process signal handler.
	nextLine("ready")

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send first SIGTERM: %v", err)
	}
	nextLine("cancelled")

	if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
		t.Fatalf("send second SIGTERM: %v", err)
	}
	select {
	case <-exited:
	case <-time.After(processTimeout):
		t.Fatalf("process did not exit within %s of the second SIGTERM", processTimeout)
	}

	status, ok := cmd.ProcessState.Sys().(syscall.WaitStatus)
	if !ok {
		t.Fatalf("process state %T, want syscall.WaitStatus", cmd.ProcessState.Sys())
	}
	if !status.Signaled() || status.Signal() != syscall.SIGTERM {
		t.Fatalf("process ended with %v, want termination by %v", status, syscall.SIGTERM)
	}
}
