package config

import (
	"os"
	"testing"
)

// TestRootOnlyWritableFor checks the owner-and-mode rule across root-owned,
// group-writable and other-owner cases.
func TestRootOnlyWritableFor(t *testing.T) {
	cases := []struct {
		name string
		uid  uint32
		mode os.FileMode
		want bool
	}{
		{"root owns a 0600 file", 0, 0o600, true},
		{"root owns a 0640 file", 0, 0o640, true},
		{"root owns a group-writable file", 0, 0o620, false},
		{"root owns an other-writable file", 0, 0o602, false},
		{"root owns a 0660 file", 0, 0o660, false},
		{"another account owns a 0600 file", 1000, 0o600, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := rootOnlyWritableFor(tc.uid, tc.mode); got != tc.want {
				t.Errorf("rootOnlyWritableFor(%d, %#o) = %v, want %v", tc.uid, tc.mode, got, tc.want)
			}
		})
	}
}
