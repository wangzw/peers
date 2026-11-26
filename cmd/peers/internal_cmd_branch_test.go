package main

import (
	"context"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

// TestPostUpdateCmdHelpExitsCleanly checks that the hidden command treats -h
// the way flag does: it reports success and writes nothing, because the
// internal group has no user-facing usage.
func TestPostUpdateCmdHelpExitsCleanly(t *testing.T) {
	called := false
	setPostUpdateSeams(t, nil, func(service.Options) error {
		called = true
		return nil
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"internal", "post-update", "-h"}); got != exitOK {
		t.Fatalf("run(internal post-update -h) = %d, want %d", got, exitOK)
	}
	if called {
		t.Error("PostUpdate ran for -h")
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Errorf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
	}
}

// TestPostUpdateCmdRejectsAMalformedInvocation checks that an option given
// without its value is refused with the general failure code and the
// missing-argument message, without reaching the rewrite.
func TestPostUpdateCmdRejectsAMalformedInvocation(t *testing.T) {
	called := false
	setPostUpdateSeams(t, func(bool) (service.Options, error) {
		return service.Options{}, nil
	}, func(service.Options) error {
		called = true
		return nil
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"internal", "post-update", "--level"}); got != exitFailure {
		t.Fatalf("run(internal post-update --level) = %d, want %d", got, exitFailure)
	}
	if called {
		t.Error("PostUpdate ran with a missing option value")
	}
	if want := "peers internal post-update: flag needs an argument: -level\n"; stderr.String() != want {
		t.Errorf("stderr = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
}

// TestPostUpdateCmdRejectsAnInvalidPID checks that a --pid the helper cannot
// use is refused instead of being forwarded as 0.
func TestPostUpdateCmdRejectsAnInvalidPID(t *testing.T) {
	called := false
	setPostUpdateSeams(t, func(bool) (service.Options, error) {
		return service.Options{}, nil
	}, func(service.Options) error {
		called = true
		return nil
	})

	a, _, stderr, _ := newDispatchApp("dev")
	args := []string{"internal", "post-update", "--level", "system", "--pid", "abc"}
	if got := a.run(context.Background(), args); got != exitFailure {
		t.Fatalf("run(%q) = %d, want %d", args, got, exitFailure)
	}
	if called {
		t.Error("PostUpdate ran with an invalid pid")
	}
	if want := "peers internal post-update: invalid --pid \"abc\"\n"; stderr.String() != want {
		t.Errorf("stderr = %q, want %q", stderr.String(), want)
	}
}

// TestLaunchdReloadHelpExitsCleanly checks the hidden helper's own -h handling.
func TestLaunchdReloadHelpExitsCleanly(t *testing.T) {
	called := false
	setLaunchdReload(t, func(service.Options, string, string, int, service.ExecutableID, string) error {
		called = true
		return nil
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"internal", "launchd-reload", "-h"}); got != exitOK {
		t.Fatalf("run(internal launchd-reload -h) = %d, want %d", got, exitOK)
	}
	if called {
		t.Error("the reload ran for -h")
	}
	if stdout.Len() != 0 || stderr.Len() != 0 {
		t.Errorf("stdout = %q, stderr = %q, want both empty", stdout.String(), stderr.String())
	}
}

// TestLaunchdReloadRejectsAnUnknownOption checks that a flag the helper does
// not define is a usage error rather than an attempt to reload with defaults.
func TestLaunchdReloadRejectsAnUnknownOption(t *testing.T) {
	called := false
	setLaunchdReload(t, func(service.Options, string, string, int, service.ExecutableID, string) error {
		called = true
		return nil
	})

	a, stdout, stderr, _ := newDispatchApp("dev")
	if got := a.run(context.Background(), []string{"internal", "launchd-reload", "--bogus"}); got != exitUsage {
		t.Fatalf("run(internal launchd-reload --bogus) = %d, want %d", got, exitUsage)
	}
	if called {
		t.Error("the reload ran with an unknown option")
	}
	if want := "peers internal launchd-reload: flag provided but not defined: -bogus\n"; stderr.String() != want {
		t.Errorf("stderr = %q, want %q", stderr.String(), want)
	}
	if stdout.Len() != 0 {
		t.Errorf("stdout = %q, want empty", stdout.String())
	}
	if strings.Contains(stderr.String(), "Usage:") {
		t.Errorf("stderr = %q, want no usage block for the hidden group", stderr.String())
	}
}
