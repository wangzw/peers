package main

import (
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"slices"
	"strings"
)

// mcpRunner runs a command and returns its combined output.
type mcpRunner func(name string, args ...string) ([]byte, error)

// mcpRegistration is what Claude Code reports for the peer MCP server. When
// command is empty, outcome says why it is unknown.
type mcpRegistration struct {
	command string
	outcome string
	prefix  []string
}

// removeMCPRegistration removes account's "peer" registration in Claude Code
// when its command is binary. An empty account means the running account.
// outcome is one line for the output; claude not found or a registration
// pointing elsewhere is an outcome, not an error.
func removeMCPRegistration(run func(name string, args ...string) ([]byte, error), account, binary string) (outcome string, err error) {
	reg := lookupPeerMCP(run, account)
	if reg.command == "" {
		return reg.outcome, nil
	}
	if reg.command != binary {
		return fmt.Sprintf("the peer MCP registration runs %s; left unchanged", reg.command), nil
	}
	if _, err := runArgv(run, invocation(reg.prefix, "mcp", "remove", "-s", "user", "peer")); err != nil {
		return "", fmt.Errorf("mcp remove -s user peer: %w", err)
	}
	if account == "" {
		return "removed the peer MCP registration", nil
	}
	return "removed the peer MCP registration for " + account, nil
}

// lookupPeerMCP reads the peer registration. Every failure is an outcome: an
// unreadable registration must not stop the rest of an uninstall.
func lookupPeerMCP(run mcpRunner, account string) mcpRegistration {
	prefix, manual, found := claudeInvocation(run, account)
	if !found {
		return mcpRegistration{outcome: claudeNotFoundOutcome(manual)}
	}
	out, err := runArgv(run, invocation(prefix, "mcp", "get", "peer"))
	if err != nil {
		if account == "" && errors.Is(err, exec.ErrNotFound) {
			return mcpRegistration{outcome: claudeNotFoundOutcome(manual)}
		}
		return mcpRegistration{outcome: "the peer MCP registration could not be read; left unchanged"}
	}
	command := parseMCPCommand(out)
	if command == "" {
		return mcpRegistration{outcome: "the peer MCP registration does not show its command; left unchanged"}
	}
	return mcpRegistration{command: command, prefix: prefix}
}

// claudeInvocation returns how to run claude for account, the command to show
// when claude cannot be found, and whether it was found. An empty account runs
// the claude on this process's PATH; another account is reached through sudo
// and its own login shell, the way the install script registers it.
func claudeInvocation(run mcpRunner, account string) (prefix []string, manual string, found bool) {
	if account == "" {
		return []string{"claude"}, "claude mcp remove -s user peer", true
	}
	shell := loginShell(run, account)
	manual = "sudo -u " + account + " -H claude mcp remove -s user peer"
	out, err := run("sudo", "-u", account, "-H", shell, "-lc", "command -v claude")
	path := strings.TrimSpace(string(out))
	if err != nil || path == "" {
		return nil, manual, false
	}
	return []string{"sudo", "-u", account, "-H", path},
		"sudo -u " + account + " -H " + path + " mcp remove -s user peer", true
}

// mcpHostGOOS is the platform whose account database loginShell queries. Tests
// set it to check both lookups on one host; production keeps runtime.GOOS.
var mcpHostGOOS = runtime.GOOS

// loginShell returns account's login shell from the account database.
func loginShell(run mcpRunner, account string) string {
	switch mcpHostGOOS {
	case "darwin":
		if out, err := run("dscl", ".", "-read", "/Users/"+account, "UserShell"); err == nil {
			for _, line := range strings.Split(string(out), "\n") {
				if value, ok := strings.CutPrefix(strings.TrimSpace(line), "UserShell:"); ok {
					if shell := strings.TrimSpace(value); shell != "" {
						return shell
					}
				}
			}
		}
	default:
		if out, err := run("getent", "passwd", account); err == nil {
			fields := strings.Split(strings.TrimSpace(string(out)), ":")
			if len(fields) >= 7 && fields[6] != "" {
				return fields[6]
			}
		}
	}
	return "/bin/sh"
}

// parseMCPCommand returns the command line claude mcp get prints. Only the
// exact "Command:" line counts, as in the install script.
func parseMCPCommand(out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "Command:"); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

// claudeNotFoundOutcome is the message shown when claude is unavailable.
func claudeNotFoundOutcome(manual string) string {
	return "claude was not found; remove the registration manually with: " + manual
}

// invocation appends args to prefix without sharing prefix's backing array.
func invocation(prefix []string, args ...string) []string {
	return slices.Concat(prefix, args)
}

// runArgv runs argv, whose first element names the command.
func runArgv(run mcpRunner, argv []string) ([]byte, error) {
	return run(argv[0], argv[1:]...)
}
