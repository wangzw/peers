//go:build linux

package helper

import (
	"errors"
	"slices"
	"strings"
	"testing"
)

// errSwitchRejected is the failure a stubbed credential syscall reports.
var errSwitchRejected = errors.New("operation not permitted")

// switchProbe records the credential calls switchAccount makes and makes the
// step named by failAt return errSwitchRejected.
type switchProbe struct {
	failAt string
	calls  []string
	groups []int
	gid    int
	uid    int
}

// install replaces the three credential syscalls for the duration of the test.
func (p *switchProbe) install(t *testing.T) {
	t.Helper()
	origGroups, origGID, origUID := switchSetgroups, switchSetegid, switchSeteuid
	t.Cleanup(func() {
		switchSetgroups, switchSetegid, switchSeteuid = origGroups, origGID, origUID
	})
	switchSetgroups = func(groups []int) error {
		p.calls = append(p.calls, "setgroups")
		p.groups = slices.Clone(groups)
		if p.failAt == "setgroups" {
			return errSwitchRejected
		}
		return nil
	}
	switchSetegid = func(gid int) error {
		p.calls = append(p.calls, "setegid")
		p.gid = gid
		if p.failAt == "setegid" {
			return errSwitchRejected
		}
		return nil
	}
	switchSeteuid = func(uid int) error {
		p.calls = append(p.calls, "seteuid")
		p.uid = uid
		if p.failAt == "seteuid" {
			return errSwitchRejected
		}
		return nil
	}
}

// TestSwitchAccount checks that switchAccount applies groups, gid and uid in order and names the failing step.
func TestSwitchAccount(t *testing.T) {
	cases := []struct {
		name      string
		failAt    string
		errText   string
		wantCalls []string
	}{
		{"applies in order", "", "", []string{"setgroups", "setegid", "seteuid"}},
		{"groups refused", "setgroups", "setting groups [43 44]", []string{"setgroups"}},
		{"gid refused", "setegid", "setting effective gid 43", []string{"setgroups", "setegid"}},
		{"uid refused", "seteuid", "setting effective uid 42", []string{"setgroups", "setegid", "seteuid"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			probe := &switchProbe{failAt: tc.failAt}
			probe.install(t)

			err := switchAccount(42, 43, []uint32{43, 44})
			if tc.errText == "" {
				if err != nil {
					t.Fatalf("switchAccount: %v", err)
				}
			} else if err == nil || !strings.Contains(err.Error(), tc.errText) {
				t.Fatalf("switchAccount error = %v, want it to name %q", err, tc.errText)
			}
			if tc.errText != "" && !errors.Is(err, errSwitchRejected) {
				t.Errorf("switchAccount error = %v, want it to wrap the syscall failure", err)
			}
			if !slices.Equal(probe.calls, tc.wantCalls) {
				t.Errorf("credential calls = %v, want %v", probe.calls, tc.wantCalls)
			}
			if !slices.Equal(probe.groups, []int{43, 44}) {
				t.Errorf("groups passed to setgroups = %v, want [43 44]", probe.groups)
			}
			if tc.failAt != "setgroups" && probe.gid != 43 {
				t.Errorf("gid passed to setegid = %d, want 43", probe.gid)
			}
			if tc.failAt == "" || tc.failAt == "seteuid" {
				if probe.uid != 42 {
					t.Errorf("uid passed to seteuid = %d, want 42", probe.uid)
				}
			}
		})
	}
}
