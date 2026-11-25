package helper

import (
	"context"
	"fmt"
	"os"
	"os/exec"
	"testing"
)

// testRoleHooks lets a platform-specific test file add roles to TestMain.
var testRoleHooks = map[string]func() int{}

// TestMain runs this test binary as a real helper child when a role is requested and otherwise runs the tests.
func TestMain(m *testing.M) {
	if role, extra, ok := helperRoleFromArgs(os.Args); ok {
		os.Exit(runHelperRole(role, extra))
	}
	switch role := os.Getenv("PEERS_TEST_HELPER_ROLE"); role {
	case "account-helper", "access-check":
		os.Exit(runHelperRole(role, nil))
	case "":
		os.Exit(m.Run())
	default:
		if hook := testRoleHooks[role]; hook != nil {
			os.Exit(hook())
		}
		os.Exit(2)
	}
}

func helperRoleFromArgs(args []string) (role string, extra []string, ok bool) {
	if len(args) < 3 || args[1] != "internal" {
		return "", nil, false
	}
	switch args[2] {
	case "account-helper", "access-check":
		return args[2], args[3:], true
	}
	return "", nil, false
}

func runHelperRole(role string, extra []string) int {
	switch role {
	case "account-helper":
		for _, arg := range extra {
			if arg == "stderr-marker" {
				fmt.Fprintln(os.Stderr, "helper stderr marker")
			}
		}
		if err := RunAccountHelper(context.Background(), os.Stdin, os.Stdout); err != nil {
			return 1
		}
		return 0
	case "access-check":
		if err := RunAccessCheck(os.Stdin, os.Stdout); err != nil {
			return 1
		}
		return 0
	default:
		return 2
	}
}

// testExecStart returns a Start function that runs the current test binary in role as the test process, with a minimal environment and no credential change.
func testExecStart(t *testing.T, role string) func(ctx context.Context, acct Account) (*Process, error) {
	t.Helper()
	home := t.TempDir()
	path := os.Getenv("PATH")
	return func(ctx context.Context, acct Account) (*Process, error) {
		cmd := exec.Command(os.Args[0])
		cmd.Env = []string{
			"PEERS_TEST_HELPER_ROLE=" + role,
			"HOME=" + home,
			"PATH=" + path,
		}
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		t.Cleanup(func() {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		})
		return &Process{
			Stdin:  stdin,
			Stdout: stdout,
			Wait:   cmd.Wait,
			Kill:   cmd.Process.Kill,
		}, nil
	}
}
