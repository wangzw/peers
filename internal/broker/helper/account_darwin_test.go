//go:build darwin

package helper

import (
	"os"
	"slices"
	"strconv"
	"testing"
)

// TestAccountCommandDarwinDoesNotSetSupplementalGroups checks that a Darwin
// helper child gets the identity in its environment and no group table in the
// credential.
func TestAccountCommandDarwinDoesNotSetSupplementalGroups(t *testing.T) {
	other := Account{
		Name:   "other",
		UID:    uint32(os.Geteuid()) + 1,
		GID:    11,
		Groups: make([]uint32, 17),
		Home:   "/Users/other",
	}
	for i := range other.Groups {
		other.Groups[i] = uint32(100 + i)
	}

	cmd := accountCommand("/usr/bin/peers", []string{"internal", "account-helper"}, other)

	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	if cred := cmd.SysProcAttr.Credential; cred != nil {
		t.Fatalf("Credential = %+v, want nil: Darwin must not pass the account's group table to the child", cred)
	}
	wantUID := accountHelperUIDEnv + "=" + strconv.FormatUint(uint64(other.UID), 10)
	wantGID := accountHelperGIDEnv + "=" + strconv.FormatUint(uint64(other.GID), 10)
	if !slices.Contains(cmd.Env, wantUID) {
		t.Errorf("Env = %q, want it to contain %q", cmd.Env, wantUID)
	}
	if !slices.Contains(cmd.Env, wantGID) {
		t.Errorf("Env = %q, want it to contain %q", cmd.Env, wantGID)
	}
	if slices.Contains(cmd.Env, accessCheckGroupsEnv+"="+joinIDs(other.Groups)) {
		t.Error("Env carries the full group list, want the child to resolve groups through the kernel")
	}
}

// TestAccountCommandDarwinSelfKeepsPlainEnvironment checks that the Broker's
// own account gets no identity in the child's environment.
func TestAccountCommandDarwinSelfKeepsPlainEnvironment(t *testing.T) {
	self := Account{
		Name:   "self",
		UID:    uint32(os.Geteuid()),
		GID:    7,
		Groups: []uint32{7, 9},
		Home:   "/Users/self",
	}
	cmd := accountCommand("/usr/bin/peers", nil, self)
	if cred := cmd.SysProcAttr.Credential; cred != nil {
		t.Errorf("Credential = %+v, want nil for the current account", cred)
	}
	for _, entry := range cmd.Env {
		if entry == accountHelperUIDEnv+"="+strconv.FormatUint(uint64(self.UID), 10) {
			t.Errorf("Env = %q, want no identity for the current account", cmd.Env)
		}
	}
}
