package main

import (
	"bytes"
	"errors"
	"os"
	"os/exec"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

// mainEntryEnv marks a re-execution of the test binary as the real entry point;
// TestMain checks it before running the tests.
const mainEntryEnv = "PEERS_TEST_MAIN_ENTRY"

func runMainEntryForTest() {
	addTestInternalCommands = func(internal *cobra.Command, _ *app) {
		internal.AddCommand(&cobra.Command{
			Use:                "panic-test",
			Hidden:             true,
			DisableFlagParsing: true,
			RunE: func(_ *cobra.Command, _ []string) error {
				panic("test-entry panic")
			},
		})
	}
	os.Args = []string{"peers", "internal", "panic-test"}
	main()
	// main exits through os.Exit, so this is only a compiler guard.
	os.Exit(exitFailure)
}

// TestMainEntryTurnsAPanicIntoExitFailure checks that the compiled entry point
// turns a panic into exitFailure rather than status 2.
func TestMainEntryTurnsAPanicIntoExitFailure(t *testing.T) {
	cmd := exec.Command(os.Args[0])
	cmd.Env = append(os.Environ(), mainEntryEnv+"=1")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr

	err := cmd.Run()
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("run the entry point: %v; stderr: %s", err, stderr.String())
	}
	if got := exitErr.ExitCode(); got != exitFailure {
		t.Fatalf("exit code = %d, want %d; stderr: %s", got, exitFailure, stderr.String())
	}
	if want := "peers: internal error: test-entry panic"; !strings.Contains(stderr.String(), want) {
		t.Errorf("stderr = %q, want it to contain %q", stderr.String(), want)
	}
}
