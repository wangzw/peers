package main

import (
	"errors"
	"fmt"
	"os/exec"
	"runtime"
	"slices"
	"strings"
)

type mcpRunner func(name string, args ...string) ([]byte, error)

// mcpRegistration is what Claude Code reports for the peer MCP server; when
// command is empty, outcome says why.
type mcpRegistration struct {
	command string
	outcome string
	prefix  []string
}

// removeMCPRegistration removes account's "peer" registration in Claude Code
// when its command is binary; an empty account means the running account. A
// missing claude or a registration pointing elsewhere is reported as an
// outcome, not an error.
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

// lookupPeerMCP reads the peer registration; every failure is an outcome, so an
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

// Tests set mcpHostGOOS so the login-shell lookup runs for both platforms on
// one host.
var mcpHostGOOS = runtime.GOOS

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

func parseMCPCommand(out []byte) string {
	for _, line := range strings.Split(string(out), "\n") {
		if value, ok := strings.CutPrefix(strings.TrimSpace(line), "Command:"); ok {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func claudeNotFoundOutcome(manual string) string {
	return "claude was not found; remove the registration manually with: " + manual
}

// invocation appends args without sharing prefix's backing array.
func invocation(prefix []string, args ...string) []string {
	return slices.Concat(prefix, args)
}

func runArgv(run mcpRunner, argv []string) ([]byte, error) {
	return run(argv[0], argv[1:]...)
}
