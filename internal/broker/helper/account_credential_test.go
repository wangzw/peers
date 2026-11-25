//go:build !darwin

package helper

import (
	"os"
	"slices"
	"testing"
)

// TestAccountCommandOtherAccountGetsCredential checks that an account other
// than the Broker's gets the full identity in the credential.
func TestAccountCommandOtherAccountGetsCredential(t *testing.T) {
	other := Account{
		Name:   "other",
		UID:    uint32(os.Geteuid()) + 1,
		GID:    11,
		Groups: []uint32{11, 12},
		Home:   "/home/other",
	}
	cmd := accountCommand("/bin/x", nil, other)
	if cmd.SysProcAttr == nil {
		t.Fatal("SysProcAttr is nil")
	}
	cred := cmd.SysProcAttr.Credential
	if cred == nil {
		t.Fatal("Credential is nil, want the other account")
	}
	if cred.Uid != other.UID {
		t.Errorf("Credential.Uid = %d, want %d", cred.Uid, other.UID)
	}
	if cred.Gid != other.GID {
		t.Errorf("Credential.Gid = %d, want %d", cred.Gid, other.GID)
	}
	if !slices.Equal(cred.Groups, other.Groups) {
		t.Errorf("Credential.Groups = %v, want %v", cred.Groups, other.Groups)
	}
}
