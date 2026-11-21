package helper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/helperpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// processTimeout is the package-wide fallback for a wait on a helper child,
// whether it is a real process or a scripted child on a goroutine. It only
// decides how long a wait that will never complete takes to fail: the success
// path selects on the child's exit or pipe traffic and never spends this time,
// so a larger value costs nothing. Thirty seconds is far above the worst
// normal latency under load; it is not a claim that a child should take that
// long.
const processTimeout = 30 * time.Second

// testRoleEnv returns the parent environment with PEERS_TEST_HELPER_ROLE
// set to role, so the re-executed test binary takes the matching branch in
// TestMain.
func testRoleEnv(role string) []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, kv := range os.Environ() {
		if strings.HasPrefix(kv, "PEERS_TEST_HELPER_ROLE=") {
			continue
		}
		env = append(env, kv)
	}
	return append(env, "PEERS_TEST_HELPER_ROLE="+role)
}

// testRoleProcess is one real child running a helper role.
type testRoleProcess struct {
	cmd    *exec.Cmd
	stdin  io.WriteCloser
	stdout io.Reader

	stdinOnce sync.Once
	waitOnce  sync.Once
	waitErr   error
}

// startTestRole re-executes the test binary as role. The child is killed and
// reaped by t.Cleanup when the test ends.
func startTestRole(t *testing.T, role string) *testRoleProcess {
	t.Helper()
	exe, err := os.Executable()
	if err != nil {
		t.Fatalf("os.Executable: %v", err)
	}
	cmd := exec.Command(exe)
	cmd.Env = testRoleEnv(role)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatalf("stdin pipe for %s child: %v", role, err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatalf("stdout pipe for %s child: %v", role, err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatalf("starting %s child: %v", role, err)
	}
	p := &testRoleProcess{cmd: cmd, stdin: stdin, stdout: stdout}
	t.Cleanup(func() {
		p.closeStdin()
		_ = cmd.Process.Kill()
		_ = p.wait()
	})
	return p
}

// closeStdin tells the child that no more requests follow.
func (p *testRoleProcess) closeStdin() {
	p.stdinOnce.Do(func() { _ = p.stdin.Close() })
}

// wait reaps the child once, so the test and the cleanup can both ask for the
// exit status without racing each other.
func (p *testRoleProcess) wait() error {
	p.waitOnce.Do(func() { p.waitErr = p.cmd.Wait() })
	return p.waitErr
}

// waitWithin reaps the child, failing the test if it does not exit in time.
func (p *testRoleProcess) waitWithin(t *testing.T) error {
	t.Helper()
	done := make(chan error, 1)
	go func() { done <- p.wait() }()
	select {
	case err := <-done:
		return err
	case <-time.After(processTimeout):
		_ = p.cmd.Process.Kill()
		if err := <-done; err != nil {
			t.Logf("test child exited after kill: %v", err)
		}
		t.Fatalf("test child did not exit within %v", processTimeout)
		return nil
	}
}

// testContext returns a context bounded by processTimeout.
func testContext(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), processTimeout)
	t.Cleanup(cancel)
	return ctx
}

// newProcessSession writes a session record without binding a socket, so a
// helper child can look it up from the filesystem.
func newProcessSession(t *testing.T, name, status string) *fakesession.Fake {
	t.Helper()
	return fakesession.New(t, fakesession.Options{
		Name:       name,
		Status:     status,
		NoBind:     true,
		SocketPath: filepath.Join(t.TempDir(), "inbox.sock"),
	})
}

// startAccountHelperClient starts the real account-helper child and connects a
// Client to it.
func startAccountHelperClient(t *testing.T) (*Client, *testRoleProcess) {
	t.Helper()
	child := startTestRole(t, "account-helper")
	client := NewClient(child.stdout, child.stdin)
	t.Cleanup(func() { _ = client.Close() })
	return client, child
}

func TestProcessHelperLookupRoundTrip(t *testing.T) {
	fake := newProcessSession(t, "worker", "busy")
	client, _ := startAccountHelperClient(t)
	ctx := testContext(t)

	ready, err := client.WaitReady(ctx)
	if err != nil {
		t.Fatalf("WaitReady: %v", err)
	}
	if got, want := ready.GetUid(), uint32(os.Getuid()); got != want {
		t.Errorf("Ready uid = %d, want %d", got, want)
	}
	if got, want := ready.GetGid(), uint32(os.Getgid()); got != want {
		t.Errorf("Ready gid = %d, want %d", got, want)
	}

	sess, err := client.Lookup(ctx, fake.ConfigDir(), fake.SocketPath())
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got, want := sess.PID, fake.PID(); got != want {
		t.Errorf("PID = %d, want %d", got, want)
	}
	if got, want := sess.Name, "worker"; got != want {
		t.Errorf("Name = %q, want %q", got, want)
	}
	if got, want := sess.Status, "busy"; got != want {
		t.Errorf("Status = %q, want %q", got, want)
	}
	if got, want := sess.SocketPath, fake.SocketPath(); got != want {
		t.Errorf("SocketPath = %q, want %q", got, want)
	}
	if got, want := sess.ConfigDir, fake.ConfigDir(); got != want {
		t.Errorf("ConfigDir = %q, want %q", got, want)
	}
}

func TestProcessHelperErrorPropagates(t *testing.T) {
	client, _ := startAccountHelperClient(t)
	ctx := testContext(t)
	if _, err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	missing := filepath.Join(t.TempDir(), "missing")
	_, err := client.Lookup(ctx, missing, filepath.Join(missing, "inbox.sock"))
	if !errors.Is(err, inbox.ErrNoRecord) {
		t.Fatalf("Lookup error = %v, want inbox.ErrNoRecord", err)
	}

	_, err = client.Reread(ctx, inbox.Session{ConfigDir: missing, PID: 4242})
	if !errors.Is(err, inbox.ErrNoRecord) {
		t.Fatalf("Reread error = %v, want inbox.ErrNoRecord", err)
	}
}

func TestProcessHelperFramingAcrossPipeBuffer(t *testing.T) {
	// A message above the kernel pipe buffer must be reassembled by the child
	// when it spans several reads, and a reply of the same size must arrive
	// whole on this side.
	const size = 128 << 10
	name := strings.Repeat("n", size)
	fake := newProcessSession(t, name, "idle")
	client, _ := startAccountHelperClient(t)
	ctx := testContext(t)
	if _, err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	sess, err := client.Lookup(ctx, fake.ConfigDir(), fake.SocketPath())
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if len(sess.Name) != size || sess.Name != name {
		t.Fatalf("Name has %d bytes, want the %d-byte record name", len(sess.Name), size)
	}

	// Reread carries a session whose Name is also far above one pipe buffer,
	// though the child only uses PID and ConfigDir.
	got, err := client.Reread(ctx, inbox.Session{
		ConfigDir:  fake.ConfigDir(),
		PID:        fake.PID(),
		Name:       strings.Repeat("r", size),
		SocketPath: "/ignored",
	})
	if err != nil {
		t.Fatalf("Reread: %v", err)
	}
	if got.Name != name {
		t.Fatalf("Reread Name has %d bytes, want the record name", len(got.Name))
	}
}

func TestProcessHelperPipelinedRequests(t *testing.T) {
	// Several requests share one pipe at once; every response must carry its
	// own id and data.
	const requests = 8
	fakes := make([]*fakesession.Fake, requests)
	for i := range fakes {
		fakes[i] = newProcessSession(t, fmt.Sprintf("session-%d", i), "idle")
	}
	client, _ := startAccountHelperClient(t)
	ctx := testContext(t)
	if _, err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	sessions := make([]inbox.Session, requests)
	errs := make([]error, requests)
	var wg sync.WaitGroup
	for i := 0; i < requests; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			sessions[i], errs[i] = client.Lookup(ctx, fakes[i].ConfigDir(), fakes[i].SocketPath())
		}(i)
	}
	wg.Wait()

	for i := 0; i < requests; i++ {
		if errs[i] != nil {
			t.Fatalf("Lookup %d: %v", i, errs[i])
		}
		if got, want := sessions[i].Name, fmt.Sprintf("session-%d", i); got != want {
			t.Fatalf("Lookup %d Name = %q, want %q", i, got, want)
		}
		if got, want := sessions[i].PID, fakes[i].PID(); got != want {
			t.Fatalf("Lookup %d PID = %d, want %d", i, got, want)
		}
	}
}

func TestProcessHelperGoneAfterExit(t *testing.T) {
	client, child := startAccountHelperClient(t)
	ctx := testContext(t)
	if _, err := client.WaitReady(ctx); err != nil {
		t.Fatalf("WaitReady: %v", err)
	}

	// Closing stdin makes the child take a clean EOF, exit, and close its
	// stdout; the client then observes the end of the connection.
	child.closeStdin()
	select {
	case <-client.Done():
	case <-time.After(processTimeout):
		t.Fatal("client did not observe the helper exit")
	}
	if err := child.waitWithin(t); err != nil {
		t.Fatalf("helper exited with %v, want a clean exit", err)
	}

	if _, err := client.Lookup(ctx, "/c", "/s"); !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Lookup after exit = %v, want ErrHelperGone", err)
	}
	err := client.Deliver(ctx, inbox.Session{SocketPath: "/s"}, "sig", "text", "m1", time.Second)
	if !errors.Is(err, inbox.ErrNotWritten) || !errors.Is(err, ErrHelperGone) {
		t.Fatalf("Deliver after exit = %v, want inbox.ErrNotWritten and ErrHelperGone", err)
	}
}

func TestProcessPoolUsesRealChild(t *testing.T) {
	fake := newProcessSession(t, "pooled", "idle")
	pool := NewPool(PoolOptions{
		Start:        testRoleStart(t, "account-helper"),
		ReadyTimeout: processTimeout,
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	t.Cleanup(func() { _ = pool.Close() })

	ctx := testContext(t)
	backend, release, err := pool.Acquire(ctx, uint32(os.Getuid()))
	if err != nil {
		t.Fatalf("Acquire: %v", err)
	}
	defer release()

	sess, err := backend.Lookup(ctx, fake.ConfigDir(), fake.SocketPath())
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if got, want := sess.Name, "pooled"; got != want {
		t.Fatalf("Name = %q, want %q", got, want)
	}
	if got, want := sess.PID, fake.PID(); got != want {
		t.Fatalf("PID = %d, want %d", got, want)
	}
}

// testRoleStart returns a PoolOptions.Start function that runs the test binary
// in role.
func testRoleStart(t *testing.T, role string) func(context.Context, Account) (*Process, error) {
	return func(context.Context, Account) (*Process, error) {
		child := startTestRole(t, role)
		return &Process{
			Stdin:  child.stdin,
			Stdout: child.stdout,
			Wait:   child.wait,
			Kill:   func() error { return child.cmd.Process.Kill() },
		}, nil
	}
}

func TestProcessAccessCheckAllowedAndDenied(t *testing.T) {
	skipUnlessAccessChecksAreMeaningful(t)

	path := writeAccessTestFile(t, t.TempDir(), "config.yaml", 0o400)
	cases := []struct {
		name   string
		access helperpb.Access
		want   bool
	}{
		{"read", helperpb.Access_ACCESS_READ, true},
		{"write", helperpb.Access_ACCESS_WRITE, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp := runAccessCheckChild(t, &helperpb.AccessCheckRequest{
				ProtocolVersion: helperpb.ProtocolVersion,
				Path:            path,
				Access:          tc.access,
			})
			if got := resp.GetAllowed(); got != tc.want {
				t.Errorf("allowed = %v, want %v", got, tc.want)
			}
			if got := resp.GetError(); got != "" {
				t.Errorf("error = %q, want empty", got)
			}
			if got, want := resp.GetUid(), uint32(os.Getuid()); got != want {
				t.Errorf("uid = %d, want %d", got, want)
			}
			if got, want := resp.GetProtocolVersion(), helperpb.ProtocolVersion; got != want {
				t.Errorf("protocol version = %d, want %d", got, want)
			}
		})
	}
}

// runAccessCheckChild feeds one request to a real access-check child and
// decodes its response.
func runAccessCheckChild(t *testing.T, req *helperpb.AccessCheckRequest) *helperpb.AccessCheckResponse {
	t.Helper()
	child := startTestRole(t, "access-check")
	if err := newMsgWriter(child.stdin).write(req); err != nil {
		t.Fatalf("writing request: %v", err)
	}
	child.closeStdin()

	var resp helperpb.AccessCheckResponse
	done := make(chan error, 1)
	go func() { done <- newMsgReader(child.stdout).read(&resp) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("reading response: %v", err)
		}
	case <-time.After(processTimeout):
		t.Fatal("timed out waiting for the access check response")
	}
	if err := child.waitWithin(t); err != nil {
		t.Fatalf("access check exited with %v", err)
	}
	return &resp
}
