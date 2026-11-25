package service

import (
	"errors"
	"slices"
	"testing"
)

// staticRun returns a Run that answers every command with one output and error.
func staticRun(out []byte, err error) func(string, ...string) ([]byte, error) {
	return func(string, ...string) ([]byte, error) { return out, err }
}

// TestEnsureAdminGroupReportsDatabaseFailures checks that an unreadable group
// or account database fails the installation.
func TestEnsureAdminGroupReportsDatabaseFailures(t *testing.T) {
	membersErr := errors.New("group database unavailable")
	accountsErr := errors.New("account database unavailable")
	lookupErr := errors.New("group lookup failed")

	t.Run("member list", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.GroupMembers = func(string) ([]string, error) { return nil, membersErr }

		var res Result
		if _, err := ensureAdminGroup(opts, &res); !errors.Is(err, membersErr) {
			t.Fatalf("ensureAdminGroup error = %v, want %v", err, membersErr)
		}
	})

	t.Run("primary group accounts", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.PrimaryGroupAccounts = func(int) ([]string, error) { return nil, accountsErr }

		var res Result
		if _, err := ensureAdminGroup(opts, &res); !errors.Is(err, accountsErr) {
			t.Fatalf("ensureAdminGroup error = %v, want %v", err, accountsErr)
		}
	})

	t.Run("second lookup after creation", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		created := false
		opts.GroupGID = func(string) (int, bool, error) {
			if created {
				return 0, false, lookupErr
			}
			return 0, false, nil
		}
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if name == "groupadd" {
				created = true
			}
			return nil, nil
		}

		var res Result
		if _, err := ensureAdminGroup(opts, &res); !errors.Is(err, lookupErr) {
			t.Fatalf("ensureAdminGroup error = %v, want %v", err, lookupErr)
		}
		if want := []string{"groupadd --system peers"}; !slices.Equal(res.Commands, want) {
			t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
		}
	})
}

// TestGroupMembersReportsQueryFailures checks the lookup errors and the
// platform check of groupMembers, none of which may be read as an empty group.
func TestGroupMembersReportsQueryFailures(t *testing.T) {
	injected := errors.New("member lookup failed")
	cases := []struct {
		name    string
		opts    func(*testing.T) Options
		want    string
		wantErr error
	}{
		{
			name: "injected lookup",
			opts: func(t *testing.T) Options {
				o := baseOptions(t)
				o.GroupMembers = func(string) ([]string, error) { return nil, injected }
				return o
			},
			wantErr: injected,
		},
		{
			name: "unsupported platform",
			opts: func(t *testing.T) Options {
				o := baseOptions(t)
				o.GOOS = "freebsd"
				o.GroupMembers = nil
				return o
			},
			want: `cannot read the members of group "peers" on platform "freebsd"`,
		},
		{
			name: "linux query failure",
			opts: func(t *testing.T) Options {
				o := baseOptions(t)
				o.GroupMembers = nil
				o.Run = staticRun([]byte("nsswitch broken"), errors.New("exit status 1"))
				return o
			},
			want: `cannot read the members of group "peers": exit status 1: nsswitch broken`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			members, err := groupMembers(tc.opts(t), "peers")
			if tc.wantErr != nil {
				if !errors.Is(err, tc.wantErr) {
					t.Fatalf("groupMembers error = %v, want %v", err, tc.wantErr)
				}
			} else if err == nil || err.Error() != tc.want {
				t.Fatalf("groupMembers error = %v, want %q", err, tc.want)
			}
			if members != nil {
				t.Errorf("members = %q, want none on failure", members)
			}
		})
	}
}

// TestGroupMembersLinuxTreatsAShortRecordAsNoMembers covers a getent line with
// no member field: the group has no supplementary members.
func TestGroupMembersLinuxTreatsAShortRecordAsNoMembers(t *testing.T) {
	opts := baseOptions(t)
	opts.GroupMembers = nil
	opts.Run = staticRun([]byte("peers:x\n"), nil)

	members, err := groupMembers(opts, "peers")
	if err != nil {
		t.Fatalf("groupMembers: %v", err)
	}
	if len(members) != 0 {
		t.Errorf("members = %q, want none", members)
	}
}

// TestOnlyMissingKeysRejectsAContinuationWithoutAKey covers a dscl line that
// continues an attribute that was never named, while a missing-key note on its
// own is accepted.
func TestOnlyMissingKeysRejectsAContinuationWithoutAKey(t *testing.T) {
	if onlyMissingKeys("No such key: NestedGroups\n  orphan value\n") {
		t.Error("onlyMissingKeys accepted a continuation line with no attribute above it")
	}
	if !onlyMissingKeys("No such key: NestedGroups\n") {
		t.Error("onlyMissingKeys rejected output that is only a missing-key note")
	}
}

// TestLookupGroupByGUIDReturnsEmptyWithoutAName covers a search that answers
// with no record name.
func TestLookupGroupByGUIDReturnsEmptyWithoutAName(t *testing.T) {
	opts := baseOptions(t)
	opts.Run = staticRun([]byte("\n\n"), nil)

	if got := lookupGroupByGUID(opts, "A1B2C3"); got != "" {
		t.Errorf("lookupGroupByGUID = %q, want an empty name", got)
	}
}

// TestLookupGroupReportsLookupFailures pins the error of each failed group
// lookup: an unsupported platform, an unreadable id, a dscl failure without
// output, and a non-numeric id.
func TestLookupGroupReportsLookupFailures(t *testing.T) {
	cases := []struct {
		name string
		opts Options
		want string
	}{
		{
			name: "unsupported platform",
			opts: Options{GOOS: "freebsd"},
			want: `cannot look up the group on platform "freebsd"`,
		},
		{
			name: "linux non-numeric id",
			opts: Options{GOOS: "linux", Run: staticRun([]byte("staff:x:nope:\n"), nil)},
			want: `cannot read group staff: group id "nope" is not a number`,
		},
		{
			name: "darwin failure without output",
			opts: Options{GOOS: "darwin", Run: staticRun(nil, errors.New("boom"))},
			want: `cannot look up group "staff": boom`,
		},
		{
			name: "darwin non-numeric id",
			opts: Options{GOOS: "darwin", Run: staticRun([]byte("PrimaryGroupID: nope\n"), nil)},
			want: `cannot read group staff: group id "nope" is not a number`,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			gid, exists, err := lookupGroup(tc.opts, "staff")
			if err == nil || err.Error() != tc.want {
				t.Fatalf("lookupGroup error = %v, want %q", err, tc.want)
			}
			if gid != 0 || exists {
				t.Errorf("lookupGroup = (%d, %v), want (0, false) on failure", gid, exists)
			}
		})
	}
}

// TestCreateAdminGroupRejectsAnUnsupportedPlatform checks that the group is not
// created with a guessed command on a platform without one.
func TestCreateAdminGroupRejectsAnUnsupportedPlatform(t *testing.T) {
	opts := Options{GOOS: "freebsd", Run: func(name string, args ...string) ([]byte, error) {
		t.Errorf("createAdminGroup ran %s", name)
		return nil, nil
	}}
	var res Result

	err := createAdminGroup(opts, &res, "peers")
	const want = `cannot create the group on platform "freebsd"`
	if err == nil || err.Error() != want {
		t.Fatalf("createAdminGroup error = %v, want %q", err, want)
	}
	if len(res.Commands) != 0 {
		t.Errorf("Result.Commands = %q, want none", res.Commands)
	}
}
