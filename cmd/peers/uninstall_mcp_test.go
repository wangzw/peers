package main

import (
	"errors"
	"os/exec"
	"slices"
	"strings"
	"testing"
)

type mcpFakeRun struct {
	output map[string]string
	errs   map[string]error
	calls  []string
}

func (f *mcpFakeRun) run(name string, args ...string) ([]byte, error) {
	key := strings.Join(append([]string{name}, args...), " ")
	f.calls = append(f.calls, key)
	if err := f.errs[key]; err != nil {
		return nil, err
	}
	if out, ok := f.output[key]; ok {
		return []byte(out), nil
	}
	return nil, errors.New("unexpected command " + key)
}

// TestRemoveMCPRegistrationRemovesMatchingCommand checks that a registration
// running the same binary is removed.
func TestRemoveMCPRegistrationRemovesMatchingCommand(t *testing.T) {
	fake := &mcpFakeRun{output: map[string]string{
		"claude mcp get peer":            "peer:\n  Scope: user\n  Command: /usr/local/bin/peers\n  Args: mcp\n",
		"claude mcp remove -s user peer": "removed\n",
	}}

	outcome, err := removeMCPRegistration(fake.run, "", "/usr/local/bin/peers")
	if err != nil {
		t.Fatalf("removeMCPRegistration: %v", err)
	}
	if outcome != "removed the peer MCP registration" {
		t.Errorf("outcome = %q", outcome)
	}
	if !slices.Contains(fake.calls, "claude mcp remove -s user peer") {
		t.Errorf("calls = %q, want the registration removed", fake.calls)
	}
}

// TestRemoveMCPRegistrationLeavesAnotherCommand checks that a registration
// pointing at another binary is left unchanged.
func TestRemoveMCPRegistrationLeavesAnotherCommand(t *testing.T) {
	fake := &mcpFakeRun{output: map[string]string{
		"claude mcp get peer": "peer:\n  Command: /opt/other/peers\n",
	}}
	outcome, err := removeMCPRegistration(fake.run, "", "/usr/local/bin/peers")
	if err != nil {
		t.Fatalf("removeMCPRegistration: %v", err)
	}
	if !strings.Contains(outcome, "/opt/other/peers") || !strings.Contains(outcome, "left unchanged") {
		t.Errorf("outcome = %q, want the other command named and left unchanged", outcome)
	}
	for _, call := range fake.calls {
		if strings.Contains(call, "mcp remove") {
			t.Errorf("removed a registration that points elsewhere: %q", call)
		}
	}
}

// TestRemoveMCPRegistrationWithoutClaude checks that a missing claude is
// reported with the manual command.
func TestRemoveMCPRegistrationWithoutClaude(t *testing.T) {
	fake := &mcpFakeRun{errs: map[string]error{
		"claude mcp get peer": exec.ErrNotFound,
	}}
	outcome, err := removeMCPRegistration(fake.run, "", "/usr/local/bin/peers")
	if err != nil {
		t.Fatalf("removeMCPRegistration: %v", err)
	}
	if !strings.Contains(outcome, "claude was not found") || !strings.Contains(outcome, "claude mcp remove -s user peer") {
		t.Errorf("outcome = %q, want the manual command", outcome)
	}
}

// TestRemoveMCPRegistrationWithoutCommandLine checks that a registration with
// no command line is left unchanged.
func TestRemoveMCPRegistrationWithoutCommandLine(t *testing.T) {
	fake := &mcpFakeRun{output: map[string]string{
		"claude mcp get peer": "peer:\n  Scope: user\n",
	}}
	outcome, err := removeMCPRegistration(fake.run, "", "/usr/local/bin/peers")
	if err != nil {
		t.Fatalf("removeMCPRegistration: %v", err)
	}
	if !strings.Contains(outcome, "left unchanged") {
		t.Errorf("outcome = %q, want left unchanged", outcome)
	}
}

func fakeLoginShell(goos, account, shell string) map[string]string {
	if goos == "darwin" {
		return map[string]string{
			"dscl . -read /Users/" + account + " UserShell": "UserShell: " + shell + "\n",
		}
	}
	return map[string]string{
		"getent passwd " + account: account + ":x:2000:2000:Bob:/home/" + account + ":" + shell + "\n",
	}
}

func useHostGOOS(t *testing.T, goos string) {
	t.Helper()
	previous := mcpHostGOOS
	mcpHostGOOS = goos
	t.Cleanup(func() { mcpHostGOOS = previous })
}

// TestRemoveMCPRegistrationForAnotherAccount checks that another account's
// registration is reached through sudo.
func TestRemoveMCPRegistrationForAnotherAccount(t *testing.T) {
	for _, goos := range []string{"linux", "darwin"} {
		t.Run(goos, func(t *testing.T) {
			useHostGOOS(t, goos)
			const (
				get    = "sudo -u bob -H /usr/local/bin/claude mcp get peer"
				remove = "sudo -u bob -H /usr/local/bin/claude mcp remove -s user peer"
			)
			output := fakeLoginShell(goos, "bob", "/bin/bash")
			output["sudo -u bob -H /bin/bash -lc command -v claude"] = "/usr/local/bin/claude\n"
			output[get] = "peer:\n  Command: /usr/local/bin/peers\n"
			output[remove] = "removed\n"
			fake := &mcpFakeRun{output: output}
			outcome, err := removeMCPRegistration(fake.run, "bob", "/usr/local/bin/peers")
			if err != nil {
				t.Fatalf("removeMCPRegistration: %v", err)
			}
			if outcome != "removed the peer MCP registration for bob" {
				t.Errorf("outcome = %q", outcome)
			}
			if !slices.Contains(fake.calls, remove) {
				t.Errorf("calls = %q, want %q", fake.calls, remove)
			}
		})
	}
}

// TestRemoveMCPRegistrationForAnotherAccountWithoutClaude checks that another
// account with no claude gets the manual command.
func TestRemoveMCPRegistrationForAnotherAccountWithoutClaude(t *testing.T) {
	for _, goos := range []string{"linux", "darwin"} {
		t.Run(goos, func(t *testing.T) {
			useHostGOOS(t, goos)
			fake := &mcpFakeRun{
				output: fakeLoginShell(goos, "bob", "/bin/sh"),
				errs: map[string]error{
					"sudo -u bob -H /bin/sh -lc command -v claude": exec.ErrNotFound,
				},
			}
			outcome, err := removeMCPRegistration(fake.run, "bob", "/usr/local/bin/peers")
			if err != nil {
				t.Fatalf("removeMCPRegistration: %v", err)
			}
			if !strings.Contains(outcome, "sudo -u bob -H claude mcp remove -s user peer") {
				t.Errorf("outcome = %q, want the manual command for bob", outcome)
			}
		})
	}
}

// TestRemoveMCPRegistrationReportsUnreadableRegistration checks that an
// unreadable registration is reported and left unchanged.
func TestRemoveMCPRegistrationReportsUnreadableRegistration(t *testing.T) {
	fake := &mcpFakeRun{errs: map[string]error{
		"claude mcp get peer": errors.New("exit status 1"),
	}}
	outcome, err := removeMCPRegistration(fake.run, "", "/usr/local/bin/peers")
	if err != nil {
		t.Fatalf("removeMCPRegistration: %v", err)
	}
	if !strings.Contains(outcome, "could not be read") {
		t.Errorf("outcome = %q, want it to say the registration was left unchanged", outcome)
	}
}

// TestRemoveMCPRegistrationReportsRemoveFailure checks that a failed removal is
// returned as an error.
func TestRemoveMCPRegistrationReportsRemoveFailure(t *testing.T) {
	fake := &mcpFakeRun{
		output: map[string]string{
			"claude mcp get peer": "peer:\n  Command: /usr/local/bin/peers\n",
		},
		errs: map[string]error{
			"claude mcp remove -s user peer": errors.New("denied"),
		},
	}
	if _, err := removeMCPRegistration(fake.run, "", "/usr/local/bin/peers"); err == nil {
		t.Fatal("removeMCPRegistration succeeded although the removal failed")
	}
}
