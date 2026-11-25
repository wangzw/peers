package service

import (
	"errors"
	"os"
	"os/user"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"
)

// skipUnlessPermissionDenied skips a case that relies on a write or read
// permission the root account does not need.
func skipUnlessPermissionDenied(t *testing.T) {
	t.Helper()
	if os.Geteuid() == 0 {
		t.Skip("the root account bypasses the permission checked here")
	}
}

// TestRemovePath checks removal of an existing tree, a missing path, and the
// failures removePath reports.
func TestRemovePath(t *testing.T) {
	t.Run("removes a tree", func(t *testing.T) {
		opts := baseOptions(t)
		writeFileUnder(t, opts.Root, "/home/alice/.peers/config.yaml", "broker: {}\n")
		ok, err := removePath(opts, "/home/alice/.peers")
		if err != nil {
			t.Fatalf("removePath: %v", err)
		}
		if !ok {
			t.Error("removePath reported nothing removed")
		}
		if _, err := os.Stat(filepath.Join(opts.Root, "/home/alice/.peers")); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("path still exists: %v", err)
		}
	})

	t.Run("missing path", func(t *testing.T) {
		opts := baseOptions(t)
		ok, err := removePath(opts, "/home/alice/.peers")
		if err != nil {
			t.Fatalf("removePath: %v", err)
		}
		if ok {
			t.Error("removePath reported a missing path as removed")
		}
	})

	t.Run("inspection failure", func(t *testing.T) {
		opts := baseOptions(t)
		writeFileUnder(t, opts.Root, "/home/alice/.peers", "not a directory")
		ok, err := removePath(opts, "/home/alice/.peers/config.yaml")
		if err == nil || !strings.Contains(err.Error(), "cannot inspect /home/alice/.peers/config.yaml") {
			t.Fatalf("removePath error = %v, want the inspection failure", err)
		}
		if ok {
			t.Error("removePath reported a path it could not inspect as removed")
		}
	})

	t.Run("removal failure", func(t *testing.T) {
		skipUnlessPermissionDenied(t)
		opts := baseOptions(t)
		dir := filepath.Join(opts.Root, "/home/alice/.peers")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "config.yaml"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

		ok, err := removePath(opts, "/home/alice/.peers")
		if err == nil || !strings.Contains(err.Error(), "cannot remove /home/alice/.peers") {
			t.Fatalf("removePath error = %v, want the removal failure", err)
		}
		if ok {
			t.Error("removePath reported a failed removal")
		}
	})
}

// TestPruneEmptyDir checks that a missing directory and a non-empty one are
// left alone, an empty one is removed, and failures are reported.
func TestPruneEmptyDir(t *testing.T) {
	t.Run("missing", func(t *testing.T) {
		opts := baseOptions(t)
		pruned, err := pruneEmptyDir(opts, "/var/log/peers")
		if err != nil {
			t.Fatalf("pruneEmptyDir: %v", err)
		}
		if pruned {
			t.Error("pruneEmptyDir reported a missing directory pruned")
		}
	})

	t.Run("empty", func(t *testing.T) {
		opts := baseOptions(t)
		dir := filepath.Join(opts.Root, "/var/log/peers")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		pruned, err := pruneEmptyDir(opts, "/var/log/peers")
		if err != nil {
			t.Fatalf("pruneEmptyDir: %v", err)
		}
		if !pruned {
			t.Error("pruneEmptyDir reported an empty directory kept")
		}
		if _, err := os.Stat(dir); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("directory still exists: %v", err)
		}
	})

	t.Run("non-empty", func(t *testing.T) {
		opts := baseOptions(t)
		dir := filepath.Join(opts.Root, "/var/log/peers")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, "broker.log"), []byte("x"), 0o644); err != nil {
			t.Fatal(err)
		}
		pruned, err := pruneEmptyDir(opts, "/var/log/peers")
		if err != nil {
			t.Fatalf("pruneEmptyDir: %v", err)
		}
		if pruned {
			t.Error("pruneEmptyDir removed a non-empty directory")
		}
		if _, err := os.Stat(dir); err != nil {
			t.Errorf("directory was removed: %v", err)
		}
	})

	t.Run("inspection failure", func(t *testing.T) {
		opts := baseOptions(t)
		writeFileUnder(t, opts.Root, "/var/log/peers", "not a directory")
		pruned, err := pruneEmptyDir(opts, "/var/log/peers")
		if err == nil || !strings.Contains(err.Error(), "cannot inspect /var/log/peers") {
			t.Fatalf("pruneEmptyDir error = %v, want the inspection failure", err)
		}
		if pruned {
			t.Error("pruneEmptyDir reported a directory it could not inspect as pruned")
		}
	})

	t.Run("removal failure", func(t *testing.T) {
		skipUnlessPermissionDenied(t)
		opts := baseOptions(t)
		dir := filepath.Join(opts.Root, "/var/log/peers")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		logDir := filepath.Join(opts.Root, "/var/log")
		if err := os.Chmod(logDir, 0o555); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(logDir, 0o755) })

		pruned, err := pruneEmptyDir(opts, "/var/log/peers")
		if err == nil || !strings.Contains(err.Error(), "cannot remove /var/log/peers") {
			t.Fatalf("pruneEmptyDir error = %v, want the removal failure", err)
		}
		if pruned {
			t.Error("pruneEmptyDir reported a failed removal")
		}
	})
}

// TestReadAdminGroupRecord checks reading the created-group record, its
// absence, and an unreadable record.
func TestReadAdminGroupRecord(t *testing.T) {
	t.Run("reads and trims the name", func(t *testing.T) {
		opts := baseOptions(t)
		writeFileUnder(t, opts.Root, adminGroupRecordPath, " ops \n")
		name, err := readAdminGroupRecord(opts)
		if err != nil {
			t.Fatalf("readAdminGroupRecord: %v", err)
		}
		if name != "ops" {
			t.Errorf("name = %q, want ops", name)
		}
	})

	t.Run("missing record", func(t *testing.T) {
		opts := baseOptions(t)
		name, err := readAdminGroupRecord(opts)
		if err != nil {
			t.Fatalf("readAdminGroupRecord: %v", err)
		}
		if name != "" {
			t.Errorf("name = %q, want empty", name)
		}
	})

	t.Run("inspection failure", func(t *testing.T) {
		opts := baseOptions(t)
		writeFileUnder(t, opts.Root, "/var/lib/peers", "not a directory")
		name, err := readAdminGroupRecord(opts)
		if err == nil || !strings.Contains(err.Error(), "cannot read "+adminGroupRecordPath) {
			t.Fatalf("readAdminGroupRecord error = %v, want the read failure", err)
		}
		if name != "" {
			t.Errorf("name = %q, want empty on failure", name)
		}
	})
}

// TestCurrentAdminGroupName checks the group owning the configuration file and
// the failures the lookup reports.
func TestCurrentAdminGroupName(t *testing.T) {
	t.Run("missing file", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		name, gid, exists, err := currentAdminGroupName(opts)
		if err != nil {
			t.Fatalf("currentAdminGroupName: %v", err)
		}
		if name != "" || gid != -1 || exists {
			t.Errorf("currentAdminGroupName = %q, %d, %v; want empty, -1, false", name, gid, exists)
		}
	})

	t.Run("resolves the owning group", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		writeFileUnder(t, opts.Root, systemConfigPath, "broker: {}\n")
		gid := configGID(t, opts, systemConfigPath)
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if name == "getent" && len(args) == 2 && args[0] == "group" && args[1] == strconv.Itoa(gid) {
				return []byte("ops:x:" + strconv.Itoa(gid) + ":\n"), nil
			}
			return nil, errors.New("unexpected command")
		}

		name, gotGID, exists, err := currentAdminGroupName(opts)
		if err != nil {
			t.Fatalf("currentAdminGroupName: %v", err)
		}
		if name != "ops" || gotGID != gid || !exists {
			t.Errorf("currentAdminGroupName = %q, %d, %v; want ops, %d, true", name, gotGID, exists, gid)
		}
	})

	t.Run("group id has no name", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		writeFileUnder(t, opts.Root, systemConfigPath, "broker: {}\n")
		gid := configGID(t, opts, systemConfigPath)
		opts.Run = func(name string, args ...string) ([]byte, error) {
			return nil, exitError(t, 2)
		}

		name, gotGID, exists, err := currentAdminGroupName(opts)
		if err != nil {
			t.Fatalf("currentAdminGroupName: %v", err)
		}
		if name != "" || gotGID != gid || !exists {
			t.Errorf("currentAdminGroupName = %q, %d, %v; want empty, %d, true", name, gotGID, exists, gid)
		}
	})

	t.Run("lookup failure", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		writeFileUnder(t, opts.Root, systemConfigPath, "broker: {}\n")
		opts.Run = func(string, ...string) ([]byte, error) {
			return nil, errors.New("group database unavailable")
		}
		if _, _, _, err := currentAdminGroupName(opts); err == nil || !strings.Contains(err.Error(), "group database unavailable") {
			t.Fatalf("currentAdminGroupName error = %v, want the lookup failure", err)
		}
	})

	t.Run("inspection failure", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		writeFileUnder(t, opts.Root, "/etc/peers", "not a directory")
		if _, _, _, err := currentAdminGroupName(opts); err == nil || !strings.Contains(err.Error(), "cannot inspect "+systemConfigPath) {
			t.Fatalf("currentAdminGroupName error = %v, want the inspection failure", err)
		}
	})
}

// TestPurgeDataRejectsUnsupportedPlatform checks the platform check.
func TestPurgeDataRejectsUnsupportedPlatform(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "plan9"
	if _, err := PurgeData(opts); err == nil || !strings.Contains(err.Error(), "cannot purge data on platform") {
		t.Fatalf("PurgeData error = %v, want the platform check", err)
	}
}

// TestPurgeDataReportsUnreadableGroupRecord checks that a record the purge
// cannot read aborts it before anything is removed.
func TestPurgeDataReportsUnreadableGroupRecord(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	writeFileUnder(t, opts.Root, "/var/lib/peers", "not a directory")

	removed, err := PurgeData(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot read "+adminGroupRecordPath) {
		t.Fatalf("PurgeData error = %v, want the record read failure", err)
	}
	if len(removed) != 0 {
		t.Errorf("removed = %q, want nothing removed", removed)
	}
}

// TestPurgeDataReportsConfigInspectionFailure checks that a configuration
// path the purge cannot inspect aborts it.
func TestPurgeDataReportsConfigInspectionFailure(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	writeFileUnder(t, opts.Root, "/etc/peers", "not a directory")

	removed, err := PurgeData(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot inspect "+systemConfigPath) {
		t.Fatalf("PurgeData error = %v, want the inspection failure", err)
	}
	if len(removed) != 0 {
		t.Errorf("removed = %q, want nothing removed", removed)
	}
}

// TestPurgeDataReportsRecordedGroupLookupError checks that a failing lookup of
// the recorded group aborts the purge.
func TestPurgeDataReportsRecordedGroupLookupError(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	writeFileUnder(t, opts.Root, adminGroupRecordPath, "peers\n")
	opts.GroupGID = func(string) (int, bool, error) {
		return 0, false, errors.New("group database unavailable")
	}

	if _, err := PurgeData(opts); err == nil || !strings.Contains(err.Error(), "group database unavailable") {
		t.Fatalf("PurgeData error = %v, want the group lookup failure", err)
	}
}

// TestPurgeDataKeepsAnInvalidRecordedGroup checks that a record naming an
// unusable group is reported and the group is left alone.
func TestPurgeDataKeepsAnInvalidRecordedGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	writeFileUnder(t, opts.Root, adminGroupRecordPath, "bad name\n")
	var cmds []string
	opts.Run = recordCommands(&cmds)

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	if !groupKept(removed, "bad name") {
		t.Errorf("removed = %q, want a kept-group message for the invalid record", removed)
	}
	for _, entry := range removed {
		if strings.Contains(entry, "the recorded name is not a valid group name") {
			return
		}
	}
	t.Errorf("removed = %q, want the invalid-name reason", removed)
}

// TestPurgeDataKeepsGroupWhoseConfigGroupCannotBeRead checks that an
// unresolvable configuration-file group keeps the recorded group.
func TestPurgeDataKeepsGroupWhoseConfigGroupCannotBeRead(t *testing.T) {
	opts, gid := purgeGroupOptions(t)
	writeFileUnder(t, opts.Root, adminGroupRecordPath, "peers\n")
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if name == "getent" && len(args) == 2 && args[0] == "group" && args[1] == strconv.Itoa(gid) {
			return nil, exitError(t, 2)
		}
		return nil, nil
	}

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	if slices.Contains(cmds, "groupdel peers") {
		t.Errorf("commands = %q, want no group deletion", cmds)
	}
	var kept string
	for _, entry := range removed {
		if strings.HasPrefix(entry, "kept group peers:") {
			kept = entry
		}
	}
	if !strings.Contains(kept, "could not be read") {
		t.Errorf("kept = %q, want the unreadable-group reason", kept)
	}
}

// TestPurgeDataReportsGroupLookupError checks that a failing group lookup
// aborts the purge.
func TestPurgeDataReportsGroupLookupError(t *testing.T) {
	opts, gid := purgeGroupOptions(t)
	var cmds []string
	opts.Run = purgeGroupRun(&cmds, gid, "ops", "root:x:0:0:root:/root:/bin/sh\n")
	opts.GroupGID = func(string) (int, bool, error) {
		return 0, false, errors.New("group database unavailable")
	}

	removed, err := PurgeData(opts)
	if err == nil || !strings.Contains(err.Error(), "group database unavailable") {
		t.Fatalf("PurgeData error = %v, want the group lookup failure", err)
	}
	for _, entry := range removed {
		if strings.HasPrefix(entry, "group ") || strings.HasPrefix(entry, "kept group ") {
			t.Errorf("removed = %q, want no group outcome after a failed lookup", removed)
			break
		}
	}
}

// TestPurgeDataSkipsARecordedAbsentGroup checks that a recorded group missing
// from the group database leaves no trace in the result.
func TestPurgeDataSkipsARecordedAbsentGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	writeFileUnder(t, opts.Root, adminGroupRecordPath, "peers\n")
	opts.GroupGID = func(string) (int, bool, error) { return 0, false, nil }

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	for _, entry := range removed {
		if strings.HasPrefix(entry, "group ") || strings.HasPrefix(entry, "kept group ") {
			t.Errorf("removed = %q, want no group outcome for an absent group", removed)
			break
		}
	}
}

// TestPurgeDataKeepsGroupWhenPrimaryGroupLookupFails checks that an
// inconclusive primary-group check keeps the group with the reason.
func TestPurgeDataKeepsGroupWhenPrimaryGroupLookupFails(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	writeFileUnder(t, opts.Root, adminGroupRecordPath, "peers\n")
	opts.GroupGID = func(string) (int, bool, error) { return 4242, true, nil }
	opts.PrimaryGroupAccounts = func(int) ([]string, error) {
		return nil, errors.New("directory unavailable")
	}

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	var kept string
	for _, entry := range removed {
		if strings.HasPrefix(entry, "kept group peers:") {
			kept = entry
		}
	}
	if !strings.Contains(kept, "could not be checked") {
		t.Errorf("removed = %q, want the inconclusive primary-group reason", removed)
	}
}

// TestPurgeDataReportsGroupDeletionFailure checks that a failing groupdel
// aborts the purge.
func TestPurgeDataReportsGroupDeletionFailure(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	writeFileUnder(t, opts.Root, adminGroupRecordPath, "peers\n")
	opts.GroupGID = func(string) (int, bool, error) { return 4242, true, nil }
	opts.PrimaryGroupAccounts = nil
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if name == "groupdel" {
			return nil, errors.New("groupdel failed")
		}
		return nil, nil
	}

	if _, err := PurgeData(opts); err == nil || !strings.Contains(err.Error(), "cannot run groupdel peers") {
		t.Fatalf("PurgeData error = %v, want the group deletion failure", err)
	}
}

// TestPurgeDataReportsRemovePathError checks that a path the purge cannot
// inspect aborts it.
func TestPurgeDataReportsRemovePathError(t *testing.T) {
	opts := baseOptions(t)
	writeFileUnder(t, opts.Root, "/home/alice", "not a directory")

	removed, err := PurgeData(opts)
	if err == nil || !strings.Contains(err.Error(), "cannot inspect /home/alice/.peers") {
		t.Fatalf("PurgeData error = %v, want the inspection failure", err)
	}
	if len(removed) != 0 {
		t.Errorf("removed = %q, want nothing removed", removed)
	}
}

// TestPurgeDataReportsPruneError checks that a service log directory the purge
// cannot inspect aborts it.
func TestPurgeDataReportsPruneError(t *testing.T) {
	skipUnlessPermissionDenied(t)
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	dir := filepath.Join(opts.Root, systemLogDir)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o111); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })

	if _, err := PurgeData(opts); err == nil || !strings.Contains(err.Error(), "cannot inspect "+systemLogDir) {
		t.Fatalf("PurgeData error = %v, want the prune inspection failure", err)
	}
}

// TestRemoveRuntimeDirErrors checks the platform check and an unreadable
// runtime path.
func TestRemoveRuntimeDirErrors(t *testing.T) {
	t.Run("unsupported platform", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "plan9"
		if _, err := RemoveRuntimeDir(opts); err == nil || !strings.Contains(err.Error(), "cannot remove the runtime directory on platform") {
			t.Fatalf("RemoveRuntimeDir error = %v, want the platform check", err)
		}
	})

	t.Run("inspection failure", func(t *testing.T) {
		opts := baseOptions(t)
		opts.System = true
		opts.EUID = 0
		writeFileUnder(t, opts.Root, "/run", "not a directory")
		if _, err := RemoveRuntimeDir(opts); err == nil || !strings.Contains(err.Error(), "cannot inspect /run/peers") {
			t.Fatalf("RemoveRuntimeDir error = %v, want the inspection failure", err)
		}
	})
}

// TestOtherAccountBrokersBranches checks an unreadable temporary directory,
// a missing probe, and entries that name no account broker.
func TestOtherAccountBrokersBranches(t *testing.T) {
	t.Run("temporary directory cannot be read", func(t *testing.T) {
		opts := baseOptions(t)
		tmp := rooted(opts, "/tmp")
		writeFileUnder(t, opts.Root, "/tmp", "not a directory")
		if _, err := OtherAccountBrokers(opts); err == nil || !strings.Contains(err.Error(), "cannot read "+tmp) {
			t.Fatalf("OtherAccountBrokers error = %v, want the read failure naming %s", err, tmp)
		}
	})

	t.Run("no probe", func(t *testing.T) {
		opts := baseOptions(t)
		if err := os.MkdirAll(filepath.Join(opts.Root, "/tmp", "peers-1001"), 0o755); err != nil {
			t.Fatal(err)
		}
		opts.Run = getentPasswd(map[string]string{"1001": "bob"})
		opts.Probe = nil
		accounts, err := OtherAccountBrokers(opts)
		if err != nil {
			t.Fatalf("OtherAccountBrokers: %v", err)
		}
		if len(accounts) != 0 {
			t.Errorf("accounts = %q, want none", accounts)
		}

		opts.Probe = func(string) (int, bool) { return 4242, true }
		accounts, err = OtherAccountBrokers(opts)
		if err != nil {
			t.Fatalf("OtherAccountBrokers: %v", err)
		}
		if !slices.Equal(accounts, []string{"bob"}) {
			t.Errorf("accounts = %q, want bob from the listening socket", accounts)
		}
	})

	t.Run("entries that name no broker", func(t *testing.T) {
		opts := baseOptions(t)
		tmp := filepath.Join(opts.Root, "/tmp")
		if err := os.MkdirAll(tmp, 0o755); err != nil {
			t.Fatal(err)
		}
		for _, name := range []string{"other", "peers-notanumber", "1001"} {
			if err := os.WriteFile(filepath.Join(tmp, name), []byte("x"), 0o644); err != nil {
				t.Fatal(err)
			}
		}
		opts.Run = getentPasswd(map[string]string{"1001": "bob", "1002": "carol"})
		opts.Probe = func(string) (int, bool) { return 4242, true }
		accounts, err := OtherAccountBrokers(opts)
		if err != nil {
			t.Fatalf("OtherAccountBrokers: %v", err)
		}
		if len(accounts) != 0 {
			t.Errorf("accounts = %q, want none", accounts)
		}

		if err := os.MkdirAll(filepath.Join(tmp, "peers-1002"), 0o755); err != nil {
			t.Fatal(err)
		}
		accounts, err = OtherAccountBrokers(opts)
		if err != nil {
			t.Fatalf("OtherAccountBrokers: %v", err)
		}
		if !slices.Equal(accounts, []string{"carol"}) {
			t.Errorf("accounts = %q, want carol from the listening socket", accounts)
		}
	})
}

// getentPasswd answers the Linux account lookup with the given uid-to-name
// entries and no one else.
func getentPasswd(names map[string]string) func(string, ...string) ([]byte, error) {
	return func(name string, args ...string) ([]byte, error) {
		if name == "getent" && len(args) == 2 && args[0] == "passwd" {
			if account, ok := names[args[1]]; ok {
				return []byte(account + ":x:" + args[1] + ":0::/home/" + account + ":/bin/sh\n"), nil
			}
		}
		return nil, nil
	}
}

// TestAccountName checks the account database lookups and the numeric
// fallback.
func TestAccountName(t *testing.T) {
	t.Run("darwin account list", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "darwin"
		opts.Run = func(name string, args ...string) ([]byte, error) {
			if name == "dscl" && len(args) == 4 && args[0] == "." && args[1] == "-list" && args[2] == "/Users" && args[3] == "UniqueID" {
				return []byte("alice 501\nbob 600\n"), nil
			}
			return nil, errors.New("unexpected command")
		}
		if got := accountName(opts, 600); got != "bob" {
			t.Errorf("accountName(600) = %q, want bob", got)
		}
		const uid = 999999998
		if _, err := user.LookupId(strconv.Itoa(uid)); err == nil {
			t.Skipf("uid %d has an account entry", uid)
		}
		if got := accountName(opts, uid); got != strconv.Itoa(uid) {
			t.Errorf("accountName(%d) = %q, want the numeric id", uid, got)
		}
	})

	t.Run("account database fallback", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "plan9"
		opts.Run = func(string, ...string) ([]byte, error) {
			t.Fatal("ran a command on a platform without a command lookup")
			return nil, nil
		}
		uid := os.Getuid()
		want, err := user.LookupId(strconv.Itoa(uid))
		if err != nil {
			t.Skipf("the account database does not resolve uid %d: %v", uid, err)
		}
		if got := accountName(opts, uid); got != want.Username {
			t.Errorf("accountName(%d) = %q, want %q", uid, got, want.Username)
		}
	})

	t.Run("unknown account", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "plan9"
		opts.Run = func(string, ...string) ([]byte, error) {
			t.Fatal("ran a command on a platform without a command lookup")
			return nil, nil
		}
		const uid = 999999999
		if _, err := user.LookupId(strconv.Itoa(uid)); err == nil {
			t.Skipf("uid %d has an account entry", uid)
		}
		if got := accountName(opts, uid); got != strconv.Itoa(uid) {
			t.Errorf("accountName(%d) = %q, want the numeric id", uid, got)
		}
	})
}

// TestGroupNameByGID checks the Linux and macOS group lookups and their
// failures.
func TestGroupNameByGID(t *testing.T) {
	const gid = 4242
	id := strconv.Itoa(gid)
	cases := []struct {
		name    string
		goos    string
		run     func(*testing.T) func(string, ...string) ([]byte, error)
		want    string
		wantErr string
	}{
		{
			name: "linux name",
			goos: "linux",
			run: func(*testing.T) func(string, ...string) ([]byte, error) {
				return func(name string, args ...string) ([]byte, error) {
					if name == "getent" && len(args) == 2 && args[0] == "group" && args[1] == id {
						return []byte("ops:x:" + id + ":\n"), nil
					}
					return nil, errors.New("unexpected command")
				}
			},
			want: "ops",
		},
		{
			name: "linux absent",
			goos: "linux",
			run: func(t *testing.T) func(string, ...string) ([]byte, error) {
				return func(string, ...string) ([]byte, error) { return nil, exitError(t, 2) }
			},
		},
		{
			name: "linux unexpected output",
			goos: "linux",
			run: func(*testing.T) func(string, ...string) ([]byte, error) {
				return func(string, ...string) ([]byte, error) { return []byte("\n"), nil }
			},
			wantErr: "unexpected getent output",
		},
		{
			name: "linux lookup failure",
			goos: "linux",
			run: func(*testing.T) func(string, ...string) ([]byte, error) {
				return func(string, ...string) ([]byte, error) { return nil, errors.New("getent failed") }
			},
			wantErr: "cannot look up group",
		},
		{
			name: "darwin name",
			goos: "darwin",
			run: func(*testing.T) func(string, ...string) ([]byte, error) {
				return func(name string, args ...string) ([]byte, error) {
					if name == "dscl" && len(args) == 4 && args[2] == "/Groups" {
						return []byte("staff 1\nops " + id + "\n"), nil
					}
					return nil, errors.New("unexpected command")
				}
			},
			want: "ops",
		},
		{
			name: "darwin absent",
			goos: "darwin",
			run: func(*testing.T) func(string, ...string) ([]byte, error) {
				return func(string, ...string) ([]byte, error) { return []byte("staff 1\n"), nil }
			},
		},
		{
			name: "darwin lookup failure",
			goos: "darwin",
			run: func(*testing.T) func(string, ...string) ([]byte, error) {
				return func(string, ...string) ([]byte, error) { return nil, errors.New("dscl failed") }
			},
			wantErr: "cannot list groups",
		},
		{
			name: "unsupported platform",
			goos: "plan9",
			run: func(*testing.T) func(string, ...string) ([]byte, error) {
				return func(string, ...string) ([]byte, error) { return nil, nil }
			},
			wantErr: "cannot look up the group on platform",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.GOOS = tc.goos
			opts.Run = tc.run(t)
			got, err := groupNameByGID(opts, gid)
			if tc.wantErr != "" {
				if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
					t.Fatalf("groupNameByGID error = %v, want %q", err, tc.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("groupNameByGID: %v", err)
			}
			if got != tc.want {
				t.Errorf("groupNameByGID = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestDeleteAdminGroup checks the platform table and the command failure.
func TestDeleteAdminGroup(t *testing.T) {
	t.Run("command failure reports output", func(t *testing.T) {
		opts := baseOptions(t)
		opts.Run = func(name string, args ...string) ([]byte, error) {
			return []byte("groupdel: cannot remove the primary group of user 'alice'"), errors.New("exit status 8")
		}
		err := deleteAdminGroup(opts, "ops")
		if err == nil || !strings.Contains(err.Error(), "cannot run groupdel ops") {
			t.Fatalf("deleteAdminGroup error = %v, want the command failure", err)
		}
		if !strings.Contains(err.Error(), "primary group of user 'alice'") {
			t.Errorf("deleteAdminGroup error = %v, want the command output", err)
		}
	})

	t.Run("unsupported platform", func(t *testing.T) {
		opts := baseOptions(t)
		opts.GOOS = "plan9"
		if err := deleteAdminGroup(opts, "ops"); err == nil || !strings.Contains(err.Error(), "cannot delete the group on platform") {
			t.Fatalf("deleteAdminGroup error = %v, want the platform failure", err)
		}
	})
}

// TestAdminGroupDeleteCommandNamesThePlatformTool checks the command a kept
// group message names.
func TestAdminGroupDeleteCommandNamesThePlatformTool(t *testing.T) {
	cases := []struct {
		goos string
		want string
	}{
		{"linux", "groupdel ops"},
		{"darwin", "dseditgroup -o delete ops"},
	}
	for _, tc := range cases {
		opts := baseOptions(t)
		opts.GOOS = tc.goos
		if got := adminGroupDeleteCommand(opts, "ops"); got != tc.want {
			t.Errorf("adminGroupDeleteCommand(%s) = %q, want %q", tc.goos, got, tc.want)
		}
	}
}

// TestPrimaryGroupAccountsErrors checks the command failures and the platform
// check.
func TestPrimaryGroupAccountsErrors(t *testing.T) {
	cases := []struct {
		name    string
		goos    string
		wantErr string
	}{
		{"linux command failure", "linux", "cannot list accounts"},
		{"darwin command failure", "darwin", "cannot list accounts"},
		{"unsupported platform", "plan9", "cannot check the primary group on platform"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.GOOS = tc.goos
			opts.PrimaryGroupAccounts = nil
			opts.Run = func(string, ...string) ([]byte, error) {
				return nil, errors.New("directory lookup failed")
			}
			if _, err := primaryGroupAccounts(opts, 4242); err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("primaryGroupAccounts error = %v, want %q", err, tc.wantErr)
			}
		})
	}
}

// TestPrimaryGroupOfAnyAccountPropagatesError checks that an account lookup
// failure is reported rather than read as no account.
func TestPrimaryGroupOfAnyAccountPropagatesError(t *testing.T) {
	opts := baseOptions(t)
	opts.PrimaryGroupAccounts = func(int) ([]string, error) {
		return nil, errors.New("directory unavailable")
	}
	primary, err := primaryGroupOfAnyAccount(opts, 4242)
	if err == nil || !strings.Contains(err.Error(), "directory unavailable") {
		t.Fatalf("primaryGroupOfAnyAccount error = %v, want the lookup failure", err)
	}
	if primary {
		t.Error("primaryGroupOfAnyAccount reported a primary group after a failed lookup")
	}
}
