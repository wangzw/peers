package service

import (
	"os"
	"path/filepath"
	"testing"
)

// TestServiceDomain checks that serviceDomain returns the system domain for a
// system service and gui/<uid> for a user one.
func TestServiceDomain(t *testing.T) {
	if got := serviceDomain(true, 501); got != "system" {
		t.Errorf("serviceDomain(system) = %q, want system", got)
	}
	if got := serviceDomain(false, 501); got != "gui/501" {
		t.Errorf("serviceDomain(user) = %q, want gui/501", got)
	}
}

// TestMainPIDReportsTheServiceManagerPID checks that MainPID asks the service
// manager of the requested level and returns its pid.
func TestMainPIDReportsTheServiceManagerPID(t *testing.T) {
	dir := t.TempDir()
	writeExecutable(t, filepath.Join(dir, "systemctl"),
		"#!/bin/sh\ncase \" $* \" in\n  *\" --user \"*) echo 4242 ;;\n  *) echo 5353 ;;\nesac\n")
	writeExecutable(t, filepath.Join(dir, "launchctl"),
		"#!/bin/sh\ncase \"$2\" in\n  system/*) printf 'state = running\\n\\tpid = 5353\\n' ;;\n  *) printf 'state = running\\n\\tpid = 4242\\n' ;;\nesac\n")
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))

	if pid, ok := MainPID(false); !ok || pid != 4242 {
		t.Errorf("MainPID(false) = %d, %v, want the user service's pid 4242", pid, ok)
	}
	if pid, ok := MainPID(true); !ok || pid != 5353 {
		t.Errorf("MainPID(true) = %d, %v, want the system service's pid 5353", pid, ok)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o755); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
}
