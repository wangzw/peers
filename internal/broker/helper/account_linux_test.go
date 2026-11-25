//go:build linux

package helper

import (
	"os"
	"syscall"
	"testing"
)

// TestAccountCommandParentDeathSignal checks that the helper child is killed
// when the parent dies.
func TestAccountCommandParentDeathSignal(t *testing.T) {
	cmd := accountCommand("/bin/true", nil, Account{UID: uint32(os.Geteuid())})
	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	if got := cmd.SysProcAttr.Pdeathsig; got != syscall.SIGKILL {
		t.Errorf("Pdeathsig = %v, want %v", got, syscall.SIGKILL)
	}
}
