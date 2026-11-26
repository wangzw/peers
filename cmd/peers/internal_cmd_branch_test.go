package main

import (
	"context"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

// TestPostUpdateCmdHelpExitsCleanly checks that internal post-update -h exits 0
// without running the post-update step or writing output.
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

// TestPostUpdateCmdRejectsAMalformedInvocation checks that an option without its
// value is refused with the missing-argument message and runs no post-update
// step.
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

// TestPostUpdateCmdRejectsAnInvalidPID checks that an invalid --pid is a
// failure and runs no post-update step.
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

// TestLaunchdReloadHelpExitsCleanly checks that launchd-reload -h exits 0
// without running the reload or writing output.
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

// TestLaunchdReloadRejectsAnUnknownOption checks that an unknown option of
// launchd-reload is a usage error and runs no reload.
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
