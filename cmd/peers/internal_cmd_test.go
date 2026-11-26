package main

import (
	"context"
	"errors"
	"io"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

func setAccountHelper(t *testing.T, fn func(context.Context, io.Reader, io.Writer) error) {
	t.Helper()
	previous := runAccountHelperFn
	runAccountHelperFn = fn
	t.Cleanup(func() { runAccountHelperFn = previous })
}

func setAccessCheck(t *testing.T, fn func(io.Reader, io.Writer) error) {
	t.Helper()
	previous := runAccessCheckFn
	runAccessCheckFn = fn
	t.Cleanup(func() { runAccessCheckFn = previous })
}

type helperContextKey struct{}

// TestAccountHelperRoutesStreamsAndContext checks that account-helper receives
// the caller's context and the app's streams.
func TestAccountHelperRoutesStreamsAndContext(t *testing.T) {
	var (
		gotContext context.Context
		gotStdin   io.Reader
		gotStdout  io.Writer
	)
	setAccountHelper(t, func(ctx context.Context, stdin io.Reader, stdout io.Writer) error {
		gotContext = ctx
		gotStdin = stdin
		gotStdout = stdout
		_, err := io.WriteString(stdout, "served\n")
		return err
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	ctx := context.WithValue(context.Background(), helperContextKey{}, "marker")
	if got := a.run(ctx, []string{"internal", "account-helper"}); got != exitOK {
		t.Fatalf("run(internal account-helper) = %d, want %d", got, exitOK)
	}
	if gotContext == nil || gotContext.Value(helperContextKey{}) != "marker" {
		t.Fatalf("helper context = %v, want the caller's context", gotContext)
	}
	if gotStdin != a.stdin {
		t.Fatalf("helper stdin = %T, want the app's stdin", gotStdin)
	}
	if gotStdout != a.stdout {
		t.Fatalf("helper stdout = %T, want the app's stdout", gotStdout)
	}
	if want := "served\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

// TestAccessCheckRoutesStreams checks that access-check receives the app's
// standard input and output.
func TestAccessCheckRoutesStreams(t *testing.T) {
	var (
		gotStdin  io.Reader
		gotStdout io.Writer
	)
	setAccessCheck(t, func(stdin io.Reader, stdout io.Writer) error {
		gotStdin = stdin
		gotStdout = stdout
		_, err := io.WriteString(stdout, "checked\n")
		return err
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"internal", "access-check"}); got != exitOK {
		t.Fatalf("run(internal access-check) = %d, want %d", got, exitOK)
	}
	if gotStdin != a.stdin {
		t.Fatalf("access-check stdin = %T, want the app's stdin", gotStdin)
	}
	if gotStdout != a.stdout {
		t.Fatalf("access-check stdout = %T, want the app's stdout", gotStdout)
	}
	if want := "checked\n"; stdout.String() != want {
		t.Fatalf("stdout = %q, want %q", stdout.String(), want)
	}
	if stderr.Len() != 0 {
		t.Fatalf("stderr = %q, want empty", stderr.String())
	}
}

// TestInternalHelpersReportErrors checks that a helper failure writes one
// prefixed line and exits 1.
func TestInternalHelpersReportErrors(t *testing.T) {
	cases := []struct {
		args    []string
		install func(*testing.T)
		want    string
	}{
		{
			args: []string{"internal", "account-helper"},
			install: func(t *testing.T) {
				setAccountHelper(t, func(context.Context, io.Reader, io.Writer) error {
					return errors.New("boom")
				})
			},
			want: "peers internal account-helper: boom\n",
		},
		{
			args: []string{"internal", "access-check"},
			install: func(t *testing.T) {
				setAccessCheck(t, func(io.Reader, io.Writer) error {
					return errors.New("boom")
				})
			},
			want: "peers internal access-check: boom\n",
		},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			tc.install(t)
			a, stdout, stderr, _ := newDispatchApp("dev")

			if got := a.run(context.Background(), tc.args); got != exitFailure {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitFailure)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if got := stderr.String(); got != tc.want {
				t.Fatalf("stderr = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestInternalHelpersRejectArguments checks that an extra argument is a usage
// error that prints no usage line.
func TestInternalHelpersRejectArguments(t *testing.T) {
	cases := []struct {
		args    []string
		install func(*testing.T, *bool)
		want    string
	}{
		{
			args: []string{"internal", "account-helper", "x"},
			install: func(t *testing.T, called *bool) {
				setAccountHelper(t, func(context.Context, io.Reader, io.Writer) error {
					*called = true
					return nil
				})
			},
			want: "peers internal account-helper: unexpected argument \"x\"\n",
		},
		{
			args: []string{"internal", "access-check", "x"},
			install: func(t *testing.T, called *bool) {
				setAccessCheck(t, func(io.Reader, io.Writer) error {
					*called = true
					return nil
				})
			},
			want: "peers internal access-check: unexpected argument \"x\"\n",
		},
	}
	for _, tc := range cases {
		t.Run(strings.Join(tc.args, " "), func(t *testing.T) {
			var called bool
			tc.install(t, &called)
			a, stdout, stderr, _ := newDispatchApp("dev")

			if got := a.run(context.Background(), tc.args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitUsage)
			}
			if called {
				t.Fatalf("run(%q) reached the helper, want it rejected before dispatch", tc.args)
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want empty", stdout.String())
			}
			if got := stderr.String(); got != tc.want {
				t.Fatalf("stderr = %q, want %q", got, tc.want)
			}
			if strings.Contains(stderr.String(), "usage: peers") {
				t.Fatalf("stderr = %q, want the hidden command to print no usage", stderr.String())
			}
		})
	}
}

func setLaunchdReload(t *testing.T, fn func(service.Options, string, string, int, service.ExecutableID, string) error) {
	t.Helper()
	previous := launchdReloadFn
	launchdReloadFn = fn
	t.Cleanup(func() { launchdReloadFn = previous })
}

// TestLaunchdReloadPassesItsArguments checks that every launchd-reload argument
// reaches the service function unchanged on success.
func TestLaunchdReloadPassesItsArguments(t *testing.T) {
	var (
		gotOpts   service.Options
		gotDomain string
		gotPlist  string
		gotPID    int
		gotID     service.ExecutableID
		gotAudit  string
	)
	setLaunchdReload(t, func(opts service.Options, domain, plist string, pid int, id service.ExecutableID, audit string) error {
		gotOpts, gotDomain, gotPlist, gotPID, gotID, gotAudit = opts, domain, plist, pid, id, audit
		return nil
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	args := []string{
		"internal", "launchd-reload",
		"--level", "system",
		"--domain", "system",
		"--plist", "/Library/LaunchDaemons/io.github.wangzw.peers.plist",
		"--pid", "4242",
		"--exe-dev", "16777233",
		"--exe-ino", "987654321",
		"--audit", "/var/log/peers/audit.log",
	}
	if got := a.run(context.Background(), args); got != exitOK {
		t.Fatalf("run(%q) = %d, want %d (stderr %q)", args, got, exitOK, stderr.String())
	}
	if !gotOpts.System {
		t.Error("the helper ran for the user installation, want system")
	}
	if gotDomain != "system" {
		t.Errorf("domain = %q, want system", gotDomain)
	}
	if gotPlist != "/Library/LaunchDaemons/io.github.wangzw.peers.plist" {
		t.Errorf("plist = %q", gotPlist)
	}
	if gotPID != 4242 {
		t.Errorf("pid = %d, want 4242", gotPID)
	}
	if want := (service.ExecutableID{Dev: 16777233, Ino: 987654321}); gotID != want {
		t.Errorf("executable id = %+v, want %+v", gotID, want)
	}
	if gotAudit != "/var/log/peers/audit.log" {
		t.Errorf("audit = %q", gotAudit)
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Errorf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
	}
}

// TestLaunchdReloadReportsFailure checks that a failed reload writes to standard
// error and exits 1.
func TestLaunchdReloadReportsFailure(t *testing.T) {
	setLaunchdReload(t, func(service.Options, string, string, int, service.ExecutableID, string) error {
		return errors.New("bootstrap failed")
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	args := []string{
		"internal", "launchd-reload",
		"--level", "user",
		"--domain", "gui/501",
		"--plist", "/Users/alice/Library/LaunchAgents/io.github.wangzw.peers.plist",
		"--pid", "4242",
		"--exe-dev", "1",
		"--exe-ino", "2",
	}
	if got := a.run(context.Background(), args); got != exitFailure {
		t.Fatalf("run(%q) = %d, want %d", args, got, exitFailure)
	}
	if got, want := stderr.String(), "peers internal launchd-reload: bootstrap failed\n"; got != want {
		t.Errorf("stderr = %q, want %q", got, want)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

// TestLaunchdReloadRejectsBadArguments checks that a malformed launchd-reload
// invocation is a usage error before the service function runs.
func TestLaunchdReloadRejectsBadArguments(t *testing.T) {
	cases := [][]string{
		{"internal", "launchd-reload"},
		{"internal", "launchd-reload", "--level", "internet", "--domain", "system", "--plist", "/x", "--pid", "1", "--exe-dev", "1", "--exe-ino", "2"},
		{"internal", "launchd-reload", "--level", "system", "--domain", "", "--plist", "/x", "--pid", "1", "--exe-dev", "1", "--exe-ino", "2"},
		{"internal", "launchd-reload", "--level", "system", "--domain", "system", "--plist", "relative", "--pid", "1", "--exe-dev", "1", "--exe-ino", "2"},
		{"internal", "launchd-reload", "--level", "system", "--domain", "system", "--plist", "/x", "--pid", "zero", "--exe-dev", "1", "--exe-ino", "2"},
		{"internal", "launchd-reload", "--level", "system", "--domain", "system", "--plist", "/x", "--pid", "0", "--exe-dev", "1", "--exe-ino", "2"},
		{"internal", "launchd-reload", "--level", "system", "--domain", "system", "--plist", "/x", "--pid", "1", "extra", "--exe-dev", "1", "--exe-ino", "2"},
		{"internal", "launchd-reload", "--level", "system", "--domain", "system", "--plist", "/x", "--pid", "1", "--exe-dev", "device", "--exe-ino", "2"},
		{"internal", "launchd-reload", "--level", "system", "--domain", "system", "--plist", "/x", "--pid", "1", "--exe-dev", "1", "--exe-ino", "inode"},
		{"internal", "launchd-reload", "--level", "system", "--domain", "system", "--plist", "/x", "--pid", "1", "--exe-dev", "1"},
	}
	for _, args := range cases {
		t.Run(strings.Join(args, " "), func(t *testing.T) {
			called := false
			setLaunchdReload(t, func(service.Options, string, string, int, service.ExecutableID, string) error {
				called = true
				return nil
			})
			a, stdout, stderr, _ := newDispatchApp("dev")

			if got := a.run(context.Background(), args); got != exitUsage {
				t.Fatalf("run(%q) = %d, want %d (stderr %q)", args, got, exitUsage, stderr.String())
			}
			if called {
				t.Fatalf("run(%q) reached the service function, want it rejected first", args)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
			if strings.Contains(stderr.String(), "usage: peers") {
				t.Errorf("stderr = %q, want the hidden command to print no usage", stderr.String())
			}
		})
	}
}
