package main

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

func setPostUpdateSeams(
	t *testing.T,
	options func(bool) (service.Options, error),
	post func(service.Options) error,
) {
	t.Helper()
	previousOptions, previousPost := postUpdateOptionsFn, postUpdateFn
	if options != nil {
		postUpdateOptionsFn = options
	}
	if post != nil {
		postUpdateFn = post
	}
	t.Cleanup(func() {
		postUpdateOptionsFn = previousOptions
		postUpdateFn = previousPost
	})
}

// TestPostUpdateCmdMapsLevel checks that --level selects the system or user
// installation.
func TestPostUpdateCmdMapsLevel(t *testing.T) {
	cases := []struct {
		level      string
		wantSystem bool
	}{
		{"user", false},
		{"system", true},
	}
	for _, tc := range cases {
		t.Run(tc.level, func(t *testing.T) {
			var (
				optionsSystem bool
				gotSystem     bool
				calls         int
			)
			setPostUpdateSeams(t,
				func(system bool) (service.Options, error) {
					optionsSystem = system
					return service.Options{System: system, Exe: "/usr/local/bin/peers"}, nil
				},
				func(opts service.Options) error {
					calls++
					gotSystem = opts.System
					return nil
				},
			)

			a, stdout, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), []string{"internal", "post-update", "--level", tc.level}); got != exitOK {
				t.Fatalf("run(internal post-update --level %s) = %d, want %d", tc.level, got, exitOK)
			}
			if optionsSystem != tc.wantSystem {
				t.Errorf("PostUpdateOptions(system) got %t, want %t", optionsSystem, tc.wantSystem)
			}
			if calls != 1 || gotSystem != tc.wantSystem {
				t.Errorf("PostUpdate calls = %d with system %t, want one call with %t", calls, gotSystem, tc.wantSystem)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
			if stderr.Len() != 0 {
				t.Errorf("stderr = %q, want empty", stderr.String())
			}
		})
	}
}

// TestPostUpdateCmdSystemNeedsNoHome checks that the system installation
// rewrites with no account home.
func TestPostUpdateCmdSystemNeedsNoHome(t *testing.T) {
	t.Setenv("HOME", "")
	var got *service.Options
	setPostUpdateSeams(t, nil, func(opts service.Options) error {
		got = &opts
		return nil
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if code := a.run(context.Background(), []string{"internal", "post-update", "--level", "system"}); code != exitOK {
		t.Fatalf("run(internal post-update --level system) = %d, want %d; stderr: %s", code, exitOK, stderr.String())
	}
	if got == nil {
		t.Fatal("PostUpdate did not run")
	}
	if !got.System {
		t.Errorf("System = false, want the system installation")
	}
	if got.Home != "" {
		t.Errorf("Home = %q, want empty: the system installation must not read the account home", got.Home)
	}
	if got.Exe == "" {
		t.Error("Exe is empty, so the rewritten definition would not name a binary")
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

// TestPostUpdateCmdUserNeedsHome checks that the user installation without a
// home reports the failure instead of rewriting.
func TestPostUpdateCmdUserNeedsHome(t *testing.T) {
	t.Setenv("HOME", "")
	var called bool
	setPostUpdateSeams(t, nil, func(service.Options) error {
		called = true
		return nil
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if code := a.run(context.Background(), []string{"internal", "post-update", "--level", "user"}); code != exitFailure {
		t.Fatalf("run(internal post-update --level user) = %d, want %d", code, exitFailure)
	}
	if called {
		t.Fatal("PostUpdate ran without a home directory for the user installation")
	}
	if !strings.Contains(stderr.String(), "cannot locate the home directory") {
		t.Errorf("stderr = %q, want it to name the missing home directory", stderr.String())
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

// TestPostUpdateCmdIgnoresUnknownOptions checks that an option this version
// does not know does not stop the step.
func TestPostUpdateCmdIgnoresUnknownOptions(t *testing.T) {
	var called bool
	setPostUpdateSeams(t, nil, func(service.Options) error {
		called = true
		return nil
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	args := []string{"internal", "post-update", "--level", "user", "--future-option", "--future=value"}
	if got := a.run(context.Background(), args); got != exitOK {
		t.Fatalf("run(%q) = %d, want %d (stderr %q)", args, got, exitOK, stderr.String())
	}
	if !called {
		t.Fatal("the unknown options kept the post-update step from running")
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Errorf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
	}
}

// TestPostUpdateCmdPassesTheBrokerPID checks that --pid reaches the service
// rewrite.
func TestPostUpdateCmdPassesTheBrokerPID(t *testing.T) {
	var gotPPID func() int
	setPostUpdateSeams(t, nil, func(opts service.Options) error {
		gotPPID = opts.PPID
		return nil
	})

	a, _, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"internal", "post-update", "--level", "system", "--pid", "4242"}); got != exitOK {
		t.Fatalf("run(internal post-update --pid 4242) = %d, want %d (stderr %q)", got, exitOK, stderr.String())
	}
	if gotPPID == nil {
		t.Fatal("the command did not pass the Broker pid to the service rewrite")
	}
	if got := gotPPID(); got != 4242 {
		t.Errorf("the Broker pid = %d, want 4242", got)
	}
}

// TestPostUpdateCmdRejectsUsage checks that a usage error exits 1 with the
// reason and prints no usage line.
func TestPostUpdateCmdRejectsUsage(t *testing.T) {
	cases := []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing level",
			args: []string{"internal", "post-update"},
			want: "peers internal post-update: missing --level; want system or user\n",
		},
		{
			name: "invalid level",
			args: []string{"internal", "post-update", "--level", "host"},
			want: "peers internal post-update: invalid --level \"host\"; want system or user\n",
		},
		{
			name: "unexpected argument",
			args: []string{"internal", "post-update", "--level", "user", "extra"},
			want: "peers internal post-update: unexpected argument \"extra\"\n",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var called bool
			setPostUpdateSeams(t, nil, func(service.Options) error {
				called = true
				return nil
			})

			a, stdout, stderr, _ := newDispatchApp("dev")
			if got := a.run(context.Background(), tc.args); got != exitFailure {
				t.Fatalf("run(%q) = %d, want %d", tc.args, got, exitFailure)
			}
			if called {
				t.Fatalf("run(%q) reached PostUpdate, want it rejected before dispatch", tc.args)
			}
			if stdout.Len() != 0 {
				t.Errorf("stdout = %q, want empty", stdout.String())
			}
			if got := stderr.String(); got != tc.want {
				t.Errorf("stderr = %q, want %q", got, tc.want)
			}
			if strings.Contains(stderr.String(), "usage: peers") {
				t.Errorf("stderr = %q, want the hidden command to print no usage", stderr.String())
			}
		})
	}
}

// TestPostUpdateCmdReportsFailure checks that a rewrite failure writes one line
// and exits 1.
func TestPostUpdateCmdReportsFailure(t *testing.T) {
	setPostUpdateSeams(t,
		func(bool) (service.Options, error) { return service.Options{Exe: "/usr/local/bin/peers"}, nil },
		func(service.Options) error { return errors.New("boom") },
	)

	a, stdout, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"internal", "post-update", "--level", "system"}); got != exitFailure {
		t.Fatalf("run(internal post-update) = %d, want %d", got, exitFailure)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if want := "peers internal post-update: boom\n"; stderr.String() != want {
		t.Errorf("stderr = %q, want %q", stderr.String(), want)
	}
}

// TestPostUpdateCmdReportsOptionFailure checks that failing to build the options
// exits 1 without running the rewrite.
func TestPostUpdateCmdReportsOptionFailure(t *testing.T) {
	var called bool
	setPostUpdateSeams(t,
		func(bool) (service.Options, error) { return service.Options{}, errors.New("no executable") },
		func(service.Options) error {
			called = true
			return nil
		},
	)

	a, _, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"internal", "post-update", "--level", "user"}); got != exitFailure {
		t.Fatalf("run(internal post-update) = %d, want %d", got, exitFailure)
	}
	if called {
		t.Fatal("PostUpdate ran although the options could not be built")
	}
	if want := "peers internal post-update: no executable\n"; stderr.String() != want {
		t.Errorf("stderr = %q, want %q", stderr.String(), want)
	}
}
