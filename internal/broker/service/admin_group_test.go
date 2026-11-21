package service

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

func TestValidAdminGroupName(t *testing.T) {
	cases := []struct {
		name string
		want bool
	}{
		{"peers", true},
		{"broker_ops-1", true},
		{"_ops", true},
		{"a", true},
		{"", false},
		{"-ops", false},
		{"Ops", false},
		{"1ops", false},
		{"op s", false},
		{"ops/1", false},
		{"ops.1", false},
	}
	for _, tc := range cases {
		if got := ValidAdminGroupName(tc.name); got != tc.want {
			t.Errorf("ValidAdminGroupName(%q) = %v, want %v", tc.name, got, tc.want)
		}
	}
}

func TestInstallSystemRejectsInvalidAdminGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.AdminGroup = "-bad"
	opts.GroupGID = nil
	opts.Run = func(name string, args ...string) ([]byte, error) {
		t.Errorf("ran %s %s before rejecting the group name", name, strings.Join(args, " "))
		return nil, nil
	}

	if _, err := Install(opts); err == nil || !strings.Contains(err.Error(), "the name is invalid") {
		t.Fatalf("Install error = %v, want the invalid name error", err)
	}
	assertNoEntries(t, opts.Root)
}

func TestAdminGroupName(t *testing.T) {
	cases := []struct {
		name string
		want string
	}{
		{"", DefaultAdminGroup},
		{"broker-ops", "broker-ops"},
	}
	for _, tc := range cases {
		if got := adminGroupName(Options{AdminGroup: tc.name}); got != tc.want {
			t.Errorf("adminGroupName(%q) = %q, want %q", tc.name, got, tc.want)
		}
	}
}

func TestEnsureAdminGroupDarwinCreatesTheGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.System = true
	opts.GroupGID = nil
	var cmds []string
	created := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch name {
		case "dscl":
			if !created {
				return []byte("No such key: PrimaryGroupID"), exitError(t, 1)
			}
			return []byte("PrimaryGroupID: 999\n"), nil
		case "dseditgroup":
			created = true
			return nil, nil
		}
		return nil, nil
	}

	var res Result
	gid, err := ensureAdminGroup(opts, &res)
	if err != nil {
		t.Fatalf("ensureAdminGroup: %v", err)
	}
	if gid != 999 {
		t.Errorf("group id = %d, want 999", gid)
	}
	wantCmds := []string{
		"dscl . -read /Groups/peers",
		"dseditgroup -o create -r peers administrators peers",
		"dscl . -read /Groups/peers",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if want := []string{"dseditgroup -o create -r peers administrators peers"}; !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
}

func TestEnsureAdminGroupDarwinUsesAnExistingGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.System = true
	opts.GroupGID = nil
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		return []byte("PrimaryGroupID: 20\n"), nil
	}

	var res Result
	gid, err := ensureAdminGroup(opts, &res)
	if err != nil {
		t.Fatalf("ensureAdminGroup: %v", err)
	}
	if gid != 20 {
		t.Errorf("group id = %d, want 20", gid)
	}
	if want := []string{"dscl . -read /Groups/peers"}; !slices.Equal(cmds, want) {
		t.Errorf("commands = %q, want %q", cmds, want)
	}
	if len(res.Commands) != 0 {
		t.Errorf("Result.Commands = %q, want no state change", res.Commands)
	}
}

func TestEnsureAdminGroupDarwinNamesTheRequestedGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.System = true
	opts.AdminGroup = "broker-ops"
	opts.GroupGID = nil
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch name {
		case "dscl":
			return []byte("No such key: PrimaryGroupID"), exitError(t, 1)
		case "dseditgroup":
			return nil, nil
		}
		return nil, nil
	}

	var res Result
	if _, err := ensureAdminGroup(opts, &res); err == nil {
		t.Fatal("ensureAdminGroup succeeded although the group was not found after creation")
	}
	want := []string{
		"dscl . -read /Groups/broker-ops",
		"dseditgroup -o create -r peers administrators broker-ops",
	}
	if !slices.Equal(cmds[:2], want) {
		t.Errorf("commands = %q, want the first two to be %q", cmds, want)
	}
}

func TestLookupGroupLinux(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GroupGID = nil
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if name != "getent" || !slices.Equal(args, []string{"group", "staff"}) {
				t.Errorf("ran %s %q, want getent group staff", name, args)
			}
			return []byte("staff:x:50:\n"), nil
		}
		gid, exists, err := lookupGroup(opts, "staff")
		if err != nil || !exists || gid != 50 {
			t.Errorf("lookupGroup = (%d, %v, %v), want (50, true, nil)", gid, exists, err)
		}
	})

	t.Run("absent", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GroupGID = nil
		opts.Run = func(string, ...string) ([]byte, error) { return []byte(""), exitError(t, 2) }
		gid, exists, err := lookupGroup(opts, "nobody")
		if err != nil || exists || gid != 0 {
			t.Errorf("lookupGroup = (%d, %v, %v), want (0, false, nil)", gid, exists, err)
		}
	})

	t.Run("failure", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GroupGID = nil
		opts.Run = func(string, ...string) ([]byte, error) { return []byte("nsswitch broken"), errors.New("exit status 1") }
		if _, _, err := lookupGroup(opts, "staff"); err == nil || !strings.Contains(err.Error(), "nsswitch broken") {
			t.Errorf("lookupGroup error = %v, want the command output", err)
		}
	})

	t.Run("unexpected output", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GroupGID = nil
		opts.Run = func(string, ...string) ([]byte, error) { return []byte("staff\n"), nil }
		if _, _, err := lookupGroup(opts, "staff"); err == nil || !strings.Contains(err.Error(), "unexpected getent output") {
			t.Errorf("lookupGroup error = %v, want the unexpected output", err)
		}
	})
}

func TestLookupGroupDarwin(t *testing.T) {
	t.Run("found", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupGID = nil
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if name != "dscl" || !slices.Equal(args, []string{".", "-read", "/Groups/staff"}) {
				t.Errorf("ran %s %q, want the dscl read", name, args)
			}
			return []byte("PrimaryGroupID: 20\n"), nil
		}
		gid, exists, err := lookupGroup(opts, "staff")
		if err != nil || !exists || gid != 20 {
			t.Errorf("lookupGroup = (%d, %v, %v), want (20, true, nil)", gid, exists, err)
		}
	})

	t.Run("absent", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupGID = nil
		opts.Run = func(string, ...string) ([]byte, error) { return []byte("No such key"), exitError(t, 1) }
		gid, exists, err := lookupGroup(opts, "nobody")
		if err != nil || exists || gid != 0 {
			t.Errorf("lookupGroup = (%d, %v, %v), want (0, false, nil)", gid, exists, err)
		}
	})

	t.Run("missing attribute", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupGID = nil
		opts.Run = func(string, ...string) ([]byte, error) { return []byte("RecordName: staff\n"), nil }
		if _, _, err := lookupGroup(opts, "staff"); err == nil || !strings.Contains(err.Error(), "no PrimaryGroupID") {
			t.Errorf("lookupGroup error = %v, want the missing attribute", err)
		}
	})
}

// TestInstallSystemRefusesAnUnexplainedGroupWithMembers reproduces the security
// defect: an existing group that the administrator never named and that peers
// never recorded creating would silently hand its members management of the
// system Broker through a newly generated configuration file.
func TestInstallSystemRefusesAnUnexplainedGroupWithMembers(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = func(string) (int, bool, error) { return 998, true, nil }
	opts.GroupMembers = func(string) ([]string, error) { return []string{"bob", "alice"}, nil }
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although an unexplained group with members would administer the system Broker")
	}
	want := `cannot use the existing group "peers" as the administration group: its members alice and bob would be able to manage the system Broker; pass --admin-group peers to accept them, or pass --admin-group with another name`
	if err.Error() != want {
		t.Errorf("Install error = %q, want %q", err, want)
	}
	if len(cmds) != 0 {
		t.Errorf("Install ran %q before refusing the group", cmds)
	}
	if _, statErr := os.Stat(filepath.Join(opts.Root, "etc/peers/config.yaml")); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("a refused installation wrote the configuration file: %v", statErr)
	}
	assertNoEntries(t, opts.Root)
}

// TestInstallSystemRefusesAnAdminGroupWithGIDZero reproduces the defect that a
// group id of 0 (root's group, such as wheel) was accepted as the
// administration group.
func TestInstallSystemRefusesAnAdminGroupWithGIDZero(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.AdminGroup = "wheel"
	opts.GroupGID = func(string) (int, bool, error) { return 0, true, nil }
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although the administration group id is 0")
	}
	want := `cannot use group "wheel" as the administration group: its group id is 0`
	if err.Error() != want {
		t.Errorf("Install error = %q, want %q", err, want)
	}
	assertNoEntries(t, opts.Root)
}

// systemInstallWithExistingGroup returns options for a system install that
// reuses an existing group with the given members and would otherwise succeed.
func systemInstallWithExistingGroup(t *testing.T, gid int, members []string) Options {
	t.Helper()
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = func(string) (int, bool, error) { return gid, true, nil }
	opts.GroupMembers = func(string) ([]string, error) { return members, nil }
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)
	return opts
}

// TestInstallSystemReusesAnExistingGroup covers the cases where an existing
// group is used without confirmation: the administrator named it, an earlier
// peers installation recorded creating it, the configuration file already
// exists, or the group lists no members.
func TestInstallSystemReusesAnExistingGroup(t *testing.T) {
	const gid = 998
	cases := []struct {
		name        string
		members     []string
		named       bool
		record      bool
		config      bool
		wantMembers []string
		wantQueried bool
	}{
		{
			name:    "named on the command line",
			members: []string{"bob", "alice"},
			named:   true,
		},
		{
			name:        "recorded by an earlier installation",
			members:     []string{"alice"},
			record:      true,
			wantMembers: []string{"alice"},
			wantQueried: true,
		},
		{
			name:    "configuration file already exists",
			members: []string{"alice"},
			config:  true,
		},
		{
			name:        "group lists no members",
			members:     nil,
			wantQueried: true,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := systemInstallWithExistingGroup(t, gid, tc.members)
			if tc.named {
				opts.AdminGroup = DefaultAdminGroup
			}
			if tc.record {
				writeAdminGroupRecord(t, opts.Root, DefaultAdminGroup)
			}
			if tc.config {
				writeExistingSystemConfig(t, opts.Root)
			}

			res, err := Install(opts)
			if err != nil {
				t.Fatalf("Install: %v", err)
			}
			if res.AdminGroupCreated {
				t.Error("AdminGroupCreated = true for a group that already existed")
			}
			if !slices.Equal(res.AdminGroupMembers, tc.wantMembers) {
				t.Errorf("AdminGroupMembers = %q, want %q", res.AdminGroupMembers, tc.wantMembers)
			}
			if res.AdminGroupMembersQueried != tc.wantQueried {
				t.Errorf("AdminGroupMembersQueried = %v, want %v", res.AdminGroupMembersQueried, tc.wantQueried)
			}
			if res.AdminGroup != DefaultAdminGroup {
				t.Errorf("AdminGroup = %q, want %q", res.AdminGroup, DefaultAdminGroup)
			}
		})
	}
}

// writeAdminGroupRecord writes the record an earlier installation leaves when
// it created the group.
func writeAdminGroupRecord(t *testing.T, root, name string) {
	t.Helper()
	path := filepath.Join(root, adminGroupRecordPath)
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("creating the record directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(name+"\n"), 0o600); err != nil {
		t.Fatalf("writing the group record: %v", err)
	}
}

// writeExistingSystemConfig writes a minimal system configuration file, standing
// in for one an earlier installation left in place.
func writeExistingSystemConfig(t *testing.T, root string) {
	t.Helper()
	path := filepath.Join(root, "etc/peers/config.yaml")
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating the configuration directory: %v", err)
	}
	if err := os.WriteFile(path, []byte("broker:\n  alias: kept\n"), 0o660); err != nil {
		t.Fatalf("writing the configuration file: %v", err)
	}
}

// TestGroupMembersLinux reads the member list from getent's fourth field.
func TestGroupMembersLinux(t *testing.T) {
	cases := []struct {
		name   string
		output string
		want   []string
	}{
		{"members", "peers:x:999:bob,alice\n", []string{"alice", "bob"}},
		{"no members", "peers:x:999:\n", nil},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.GroupMembers = nil
			var ran []string
			opts.Run = func(name string, args ...string) ([]byte, error) {
				ran = append(ran, strings.Join(append([]string{name}, args...), " "))
				return []byte(tc.output), nil
			}
			members, err := groupMembers(opts, "peers")
			if err != nil {
				t.Fatalf("groupMembers: %v", err)
			}
			if !slices.Equal(members, tc.want) {
				t.Errorf("members = %q, want %q", members, tc.want)
			}
			if want := []string{"getent group peers"}; !slices.Equal(ran, want) {
				t.Errorf("commands = %q, want %q", ran, want)
			}
		})
	}
}

// darwinGroupRecordStub answers the dscl reads groupMembersDarwin makes: the
// record read returns read and readErr, and each NestedGroups GUID in names is
// resolved to its group name. A GUID without an entry fails the search, which
// stands for a record the directory service cannot resolve.
func darwinGroupRecordStub(t *testing.T, read string, readErr error, names map[string]string) (func(string, ...string) ([]byte, error), *[]string) {
	t.Helper()
	var ran []string
	return func(name string, args ...string) ([]byte, error) {
		ran = append(ran, strings.Join(append([]string{name}, args...), " "))
		if name != "dscl" {
			t.Errorf("ran %s %q, want dscl", name, args)
			return nil, nil
		}
		switch {
		case len(args) == 5 && args[1] == "-read":
			return []byte(read), readErr
		case len(args) == 6 && args[1] == "-search":
			if resolved, ok := names[args[4]]; ok {
				return []byte(resolved + "\n"), nil
			}
			return nil, exitError(t, 1)
		}
		t.Errorf("unexpected dscl arguments %q", args)
		return nil, nil
	}, &ran
}

// TestGroupMembersDarwin reads GroupMembership and NestedGroups from dscl. A
// group without either attribute lists no members rather than failing, while a
// missing record is a query failure so the installation refuses. NestedGroups
// holds group GUIDs: each is resolved to the group's name for display, and a
// GUID that cannot be resolved is kept so its members still count.
func TestGroupMembersDarwin(t *testing.T) {
	const readCommand = "dscl . -read /Groups/peers GroupMembership NestedGroups"

	t.Run("members and nested groups", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		run, ran := darwinGroupRecordStub(t,
			"GroupMembership: bob alice\nNestedGroups: A1B2C3\n", nil,
			map[string]string{"A1B2C3": "team-a"})
		opts.Run = run
		members, err := groupMembers(opts, "peers")
		if err != nil {
			t.Fatalf("groupMembers: %v", err)
		}
		if want := []string{"alice", "bob", "team-a"}; !slices.Equal(members, want) {
			t.Errorf("members = %q, want %q", members, want)
		}
		wantRan := []string{readCommand, "dscl . -search /Groups GeneratedUID A1B2C3 RecordName"}
		if !slices.Equal(*ran, wantRan) {
			t.Errorf("commands = %q, want %q", *ran, wantRan)
		}
	})

	t.Run("continuation lines", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		run, _ := darwinGroupRecordStub(t,
			"GroupMembership: bob\n alice\nNestedGroups:\n A1B2C3\n", nil,
			map[string]string{"A1B2C3": "team-a"})
		opts.Run = run
		members, err := groupMembers(opts, "peers")
		if err != nil {
			t.Fatalf("groupMembers: %v", err)
		}
		if want := []string{"alice", "bob", "team-a"}; !slices.Equal(members, want) {
			t.Errorf("members = %q, want %q", members, want)
		}
	})

	t.Run("nested groups only", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		run, _ := darwinGroupRecordStub(t,
			"NestedGroups: A1B2C3\n", nil,
			map[string]string{"A1B2C3": "team-a"})
		opts.Run = run
		members, err := groupMembers(opts, "peers")
		if err != nil {
			t.Fatalf("groupMembers: %v", err)
		}
		if want := []string{"team-a"}; !slices.Equal(members, want) {
			t.Errorf("members = %q, want %q", members, want)
		}
	})

	t.Run("unresolved nested group keeps its guid", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		run, _ := darwinGroupRecordStub(t,
			"NestedGroups: A1B2C3\n", nil, nil)
		opts.Run = run
		members, err := groupMembers(opts, "peers")
		if err != nil {
			t.Fatalf("groupMembers: %v", err)
		}
		if want := []string{"A1B2C3"}; !slices.Equal(members, want) {
			t.Errorf("members = %q, want the unresolved GUID %q", members, want)
		}
	})

	t.Run("missing nested groups key", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		run, _ := darwinGroupRecordStub(t,
			"GroupMembership: bob\nNo such key: NestedGroups\n", exitError(t, 1), nil)
		opts.Run = run
		members, err := groupMembers(opts, "peers")
		if err != nil {
			t.Fatalf("groupMembers: %v", err)
		}
		if want := []string{"bob"}; !slices.Equal(members, want) {
			t.Errorf("members = %q, want %q", members, want)
		}
	})

	t.Run("missing nested groups key with a continuation member", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		run, _ := darwinGroupRecordStub(t,
			"GroupMembership: bob\n alice\nNo such key: NestedGroups\n", exitError(t, 1), nil)
		opts.Run = run
		members, err := groupMembers(opts, "peers")
		if err != nil {
			t.Fatalf("groupMembers: %v", err)
		}
		if want := []string{"alice", "bob"}; !slices.Equal(members, want) {
			t.Errorf("members = %q, want %q", members, want)
		}
	})

	t.Run("no members", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		run, _ := darwinGroupRecordStub(t,
			"No such key: GroupMembership\nNo such key: NestedGroups\n", exitError(t, 1), nil)
		opts.Run = run
		members, err := groupMembers(opts, "peers")
		if err != nil {
			t.Fatalf("groupMembers: %v", err)
		}
		if len(members) != 0 {
			t.Errorf("members = %q, want none", members)
		}
	})

	t.Run("record missing", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		run, _ := darwinGroupRecordStub(t,
			"eDSRecordNotFound\n", exitError(t, 1), nil)
		opts.Run = run
		if _, err := groupMembers(opts, "peers"); err == nil || !strings.Contains(err.Error(), "cannot read the members") {
			t.Errorf("groupMembers error = %v, want a query failure", err)
		}
	})

	t.Run("unexplained failure", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.GroupMembers = nil
		// A nonzero exit with no missing-key note is a query failure even when
		// part of the record could be read.
		run, _ := darwinGroupRecordStub(t,
			"GroupMembership: bob\n", exitError(t, 1), nil)
		opts.Run = run
		if _, err := groupMembers(opts, "peers"); err == nil || !strings.Contains(err.Error(), "cannot read the members") {
			t.Errorf("groupMembers error = %v, want a query failure", err)
		}
	})
}

// TestGroupMembersDarwinUsesRealDscl exercises groupMembersDarwin against the
// host's real directory service, so a macOS CI run proves the parser and the
// GUID-to-name resolution against real dscl output rather than a stub. The
// nested-group round trip needs passwordless sudo and is skipped without it.
func TestGroupMembersDarwinUsesRealDscl(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("dscl runs only on macOS")
	}
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.GroupMembers = nil
	opts.Run = func(name string, args ...string) ([]byte, error) {
		return exec.Command(name, args...).CombinedOutput()
	}

	// staff exists on every macOS installation.
	members, err := groupMembers(opts, "staff")
	if err != nil {
		t.Fatalf("groupMembers(staff): %v", err)
	}
	for _, member := range members {
		if member == "" || strings.ContainsAny(member, " \t:") {
			t.Errorf("member %q is neither an account nor a group name", member)
		}
	}

	if err := exec.Command("sudo", "-n", "true").Run(); err != nil {
		t.Log("skipping the nested group round trip: passwordless sudo is unavailable")
		return
	}
	const nested, parent = "peers-nested-probe", "peers-parent-probe"
	group := func(args ...string) {
		t.Helper()
		argv := append([]string{"-n", "dseditgroup"}, args...)
		out, err := exec.Command("sudo", argv...).CombinedOutput()
		if err != nil {
			t.Fatalf("sudo dseditgroup %s: %v: %s", strings.Join(args, " "), err, out)
		}
	}
	t.Cleanup(func() {
		_ = exec.Command("sudo", "-n", "dseditgroup", "-o", "delete", parent).Run()
		_ = exec.Command("sudo", "-n", "dseditgroup", "-o", "delete", nested).Run()
	})
	group("-o", "create", nested)
	group("-o", "create", parent)
	group("-o", "edit", "-a", nested, "-t", "group", parent)

	members, err = groupMembers(opts, parent)
	if err != nil {
		t.Fatalf("groupMembers(%s): %v", parent, err)
	}
	if !slices.Contains(members, nested) {
		t.Errorf("members of %s = %q, want the resolved nested group %q", parent, members, nested)
	}
}

// TestPrimaryGroupAccountsLinux reads the account list from getent passwd.
func TestPrimaryGroupAccountsLinux(t *testing.T) {
	opts := baseOptions(t)
	opts.PrimaryGroupAccounts = nil
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if name != "getent" || !slices.Equal(args, []string{"passwd"}) {
			t.Errorf("ran %s %q, want getent passwd", name, args)
		}
		return []byte("alice:x:1000:998:Alice:/home/alice:/bin/sh\nroot:x:0:0:root:/root:/bin/sh\n"), nil
	}
	accounts, err := primaryGroupAccounts(opts, 998)
	if err != nil {
		t.Fatalf("primaryGroupAccounts: %v", err)
	}
	if want := []string{"alice"}; !slices.Equal(accounts, want) {
		t.Errorf("accounts = %q, want %q", accounts, want)
	}
}

// TestPrimaryGroupAccountsDarwin reads the account list from dscl.
func TestPrimaryGroupAccountsDarwin(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.PrimaryGroupAccounts = nil
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if name != "dscl" || !slices.Equal(args, []string{".", "-list", "/Users", "PrimaryGroupID"}) {
			t.Errorf("ran %s %q, want the dscl account list", name, args)
		}
		return []byte("alice 998\nroot 0\n"), nil
	}
	accounts, err := primaryGroupAccounts(opts, 998)
	if err != nil {
		t.Fatalf("primaryGroupAccounts: %v", err)
	}
	if want := []string{"alice"}; !slices.Equal(accounts, want) {
		t.Errorf("accounts = %q, want %q", accounts, want)
	}
}

// TestInstallSystemCountsAPrimaryGroupAccountAsAMember reproduces the defect
// that an account whose primary group is the administration group was not
// listed as a supplementary member, so it could write the configuration file
// without the administrator ever accepting it.
func TestInstallSystemCountsAPrimaryGroupAccountAsAMember(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = func(string) (int, bool, error) { return 998, true, nil }
	opts.GroupMembers = func(string) ([]string, error) { return nil, nil }
	opts.PrimaryGroupAccounts = func(int) ([]string, error) { return []string{"alice"}, nil }
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)

	_, err := Install(opts)
	if err == nil {
		t.Fatal("Install succeeded although an account's primary group would administer the system Broker")
	}
	want := `cannot use the existing group "peers" as the administration group: its member alice would be able to manage the system Broker; pass --admin-group peers to accept them, or pass --admin-group with another name`
	if err.Error() != want {
		t.Errorf("Install error = %q, want %q", err, want)
	}
	assertNoEntries(t, opts.Root)
}

// TestInstallSystemSkipsTheMemberLookupWhenTheDecisionIsSettled checks that a
// reinstall is never blocked by a broken member lookup once the administrator
// named the group or an existing configuration file is kept.
func TestInstallSystemSkipsTheMemberLookupWhenTheDecisionIsSettled(t *testing.T) {
	cases := []struct {
		name   string
		named  bool
		config bool
	}{
		{name: "administrator named the group", named: true},
		{name: "configuration file already exists", config: true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.System = true
			opts.EUID = 0
			opts.GroupGID = func(string) (int, bool, error) { return 998, true, nil }
			calls := 0
			opts.GroupMembers = func(string) ([]string, error) {
				calls++
				return []string{"alice"}, nil
			}
			if tc.named {
				opts.AdminGroup = DefaultAdminGroup
			}
			if tc.config {
				writeExistingSystemConfig(t, opts.Root)
			}
			var cmds []string
			started := false
			opts.Run = recordCommandsUntil(&cmds, &started, "restart")
			opts.Probe = probeAfterStart(&started, 4242)

			res, err := Install(opts)
			if err != nil {
				t.Fatalf("Install: %v", err)
			}
			if calls != 0 {
				t.Errorf("groupMembers was called %d times, want 0", calls)
			}
			if res.AdminGroupMembersQueried {
				t.Error("AdminGroupMembersQueried = true, want false")
			}
			if len(res.AdminGroupMembers) != 0 {
				t.Errorf("AdminGroupMembers = %q, want none", res.AdminGroupMembers)
			}
		})
	}
}

// TestInstallSystemRefusesAGroupRecordedUnderAnotherName guards the record
// check: only a record naming this group explains its members.
func TestInstallSystemRefusesAGroupRecordedUnderAnotherName(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = func(string) (int, bool, error) { return 998, true, nil }
	opts.GroupMembers = func(string) ([]string, error) { return []string{"alice"}, nil }
	writeAdminGroupRecord(t, opts.Root, "another-group")
	var cmds []string
	started := false
	opts.Run = recordCommandsUntil(&cmds, &started, "restart")
	opts.Probe = probeAfterStart(&started, 4242)

	if _, err := Install(opts); err == nil {
		t.Fatal("Install accepted a record naming another group")
	}
	if _, statErr := os.Stat(filepath.Join(opts.Root, "etc/peers/config.yaml")); !errors.Is(statErr, os.ErrNotExist) {
		t.Errorf("a refused installation wrote the configuration file: %v", statErr)
	}
}
