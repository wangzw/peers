package service

import (
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"syscall"
	"testing"
)

func writeFileUnder(t *testing.T, root, path, content string) string {
	t.Helper()
	rootedPath := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(rootedPath), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(rootedPath, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	return rootedPath
}

// TestServiceBinaryReadsSystemdUnits checks that ServiceBinary returns the
// ExecStart binary of user and system units, including unescaped systemd sigils.
func TestServiceBinaryReadsSystemdUnits(t *testing.T) {
	cases := []struct {
		name   string
		opts   func(t *testing.T) Options
		exe    string
		unit   func(string) (string, error)
		expect string
	}{
		{
			name: "user unit",
			opts: func(t *testing.T) Options { return baseOptions(t) },
			exe:  "/opt/peer tools/bin/peers",
			unit: SystemdUserUnit,
		},
		{
			name: "system unit",
			opts: func(t *testing.T) Options {
				opts := baseOptions(t)
				opts.System = true
				return opts
			},
			exe:  "/usr/local/bin/peers",
			unit: SystemdSystemUnit,
		},
		{
			name: "systemd sigils are unescaped",
			opts: func(t *testing.T) Options { return baseOptions(t) },
			exe:  "/opt/100%/a$b",
			unit: SystemdUserUnit,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.opts(t)
			path, err := unitPath(opts)
			if err != nil {
				t.Fatal(err)
			}
			content, err := tc.unit(tc.exe)
			if err != nil {
				t.Fatal(err)
			}
			writeFileUnder(t, opts.Root, path, content)

			got, ok, err := ServiceBinary(opts)
			if err != nil {
				t.Fatalf("ServiceBinary: %v", err)
			}
			if !ok {
				t.Fatal("ServiceBinary reported no definition")
			}
			if got != tc.exe {
				t.Errorf("ServiceBinary = %q, want %q", got, tc.exe)
			}
		})
	}
}

// TestServiceBinaryReadsLaunchdPlists checks that ServiceBinary returns the
// first ProgramArguments item of user and system plists.
func TestServiceBinaryReadsLaunchdPlists(t *testing.T) {
	cases := []struct {
		name string
		opts func(t *testing.T) Options
		exe  string
		unit func(string) (string, error)
	}{
		{
			name: "user plist",
			opts: func(t *testing.T) Options {
				opts := baseOptions(t)
				opts.GOOS = "darwin"
				opts.Home = "/Users/alice"
				return opts
			},
			exe: "/Users/alice/bin/peers",
			unit: func(exe string) (string, error) {
				return LaunchAgentPlist(exe, "/Users/alice/Library/Logs/peers/broker.log")
			},
		},
		{
			name: "system plist",
			opts: func(t *testing.T) Options {
				opts := baseOptions(t)
				opts.GOOS = "darwin"
				opts.Home = "/Users/alice"
				opts.System = true
				return opts
			},
			exe:  "/usr/local/bin/peers",
			unit: LaunchDaemonPlist,
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.opts(t)
			path, err := unitPath(opts)
			if err != nil {
				t.Fatal(err)
			}
			content, err := tc.unit(tc.exe)
			if err != nil {
				t.Fatal(err)
			}
			writeFileUnder(t, opts.Root, path, content)

			got, ok, err := ServiceBinary(opts)
			if err != nil {
				t.Fatalf("ServiceBinary: %v", err)
			}
			if !ok {
				t.Fatal("ServiceBinary reported no definition")
			}
			if got != tc.exe {
				t.Errorf("ServiceBinary = %q, want %q", got, tc.exe)
			}
		})
	}
}

// TestServiceBinaryWithoutDefinition checks that ServiceBinary reports no
// definition when no unit file exists.
func TestServiceBinaryWithoutDefinition(t *testing.T) {
	opts := baseOptions(t)
	got, ok, err := ServiceBinary(opts)
	if err != nil {
		t.Fatalf("ServiceBinary: %v", err)
	}
	if ok || got != "" {
		t.Errorf("ServiceBinary = %q, %v, want no definition", got, ok)
	}
}

// TestServiceBinaryRejectsDefinitionWithoutProgram checks that ServiceBinary
// rejects a unit without ExecStart.
func TestServiceBinaryRejectsDefinitionWithoutProgram(t *testing.T) {
	opts := baseOptions(t)
	path, err := unitPath(opts)
	if err != nil {
		t.Fatal(err)
	}
	writeFileUnder(t, opts.Root, path, "[Service]\nRestart=always\n")
	if _, _, err := ServiceBinary(opts); err == nil {
		t.Fatal("ServiceBinary accepted a definition without ExecStart")
	}
}

// TestRemoveRuntimeDir checks that RemoveRuntimeDir removes the runtime
// directory at each level and platform and reports nothing the second time.
func TestRemoveRuntimeDir(t *testing.T) {
	cases := []struct {
		name    string
		opts    func(t *testing.T) Options
		runtime string
	}{
		{
			name:    "linux user",
			opts:    func(t *testing.T) Options { return baseOptions(t) },
			runtime: "/tmp/peers-1000",
		},
		{
			name: "linux system",
			opts: func(t *testing.T) Options {
				opts := baseOptions(t)
				opts.System = true
				opts.EUID = 0
				return opts
			},
			runtime: "/run/peers",
		},
		{
			name: "darwin user",
			opts: func(t *testing.T) Options {
				opts := baseOptions(t)
				opts.GOOS = "darwin"
				opts.Home = "/Users/alice"
				return opts
			},
			runtime: "/tmp/peers-1000",
		},
		{
			name: "darwin system",
			opts: func(t *testing.T) Options {
				opts := baseOptions(t)
				opts.GOOS = "darwin"
				opts.Home = "/Users/alice"
				opts.System = true
				opts.EUID = 0
				return opts
			},
			runtime: "/var/run/peers",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := tc.opts(t)
			writeFileUnder(t, opts.Root, filepath.Join(tc.runtime, "broker.sock"), "socket")

			removed, err := RemoveRuntimeDir(opts)
			if err != nil {
				t.Fatalf("RemoveRuntimeDir: %v", err)
			}
			if !slices.Equal(removed, []string{tc.runtime}) {
				t.Errorf("removed = %q, want %q", removed, tc.runtime)
			}
			if _, err := os.Stat(filepath.Join(opts.Root, tc.runtime)); !errors.Is(err, os.ErrNotExist) {
				t.Errorf("runtime directory still exists: %v", err)
			}

			again, err := RemoveRuntimeDir(opts)
			if err != nil {
				t.Fatalf("second RemoveRuntimeDir: %v", err)
			}
			if len(again) != 0 {
				t.Errorf("second RemoveRuntimeDir removed %q, want nothing", again)
			}
		})
	}
}

// TestPurgeDataLinuxUser checks that a Linux user purge removes the data
// directory and the systemd drop-in.
func TestPurgeDataLinuxUser(t *testing.T) {
	opts := baseOptions(t)
	writeFileUnder(t, opts.Root, "/home/alice/.peers/config.yaml", "broker: {}\n")
	writeFileUnder(t, opts.Root, "/home/alice/.peers/identity.key", "key")
	dropIn := "/home/alice/.config/systemd/user/peers.service.d/proxy.conf"
	writeFileUnder(t, opts.Root, dropIn, "[Service]\n")

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	want := []string{"/home/alice/.peers", "/home/alice/.config/systemd/user/peers.service.d"}
	if !slices.Equal(removed, want) {
		t.Errorf("removed = %q, want %q", removed, want)
	}
	for _, path := range want {
		if _, err := os.Stat(filepath.Join(opts.Root, path)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s still exists: %v", path, err)
		}
	}
}

// TestPurgeDataLinuxSystem checks that a Linux system purge removes the
// configuration, data, audit log, drop-in and empty log directory, and deletes
// the recorded admin group.
func TestPurgeDataLinuxSystem(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0

	writeFileUnder(t, opts.Root, "/etc/peers/config.yaml", "broker: {}\n")
	writeFileUnder(t, opts.Root, "/var/lib/peers/identity.key", "key")
	writeFileUnder(t, opts.Root, "/var/log/peers/audit.log", "audit\n")
	writeFileUnder(t, opts.Root, "/etc/systemd/system/peers.service.d/proxy.conf", "[Service]\n")

	gid := configGID(t, opts, "/etc/peers/config.yaml")
	writeFileUnder(t, opts.Root, adminGroupRecord, "ops\n")
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if name == "getent" && slices.Equal(args, []string{"passwd"}) {
			return []byte("root:x:0:0:root:/root:/bin/sh\n"), nil
		}
		if name == "getent" && len(args) == 2 && args[0] == "group" {
			switch args[1] {
			case strconv.Itoa(gid):
				return []byte("ops:x:" + strconv.Itoa(gid) + ":\n"), nil
			case "ops":
				return []byte("ops:x:" + strconv.Itoa(gid) + ":\n"), nil
			}
		}
		return nil, nil
	}

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	want := []string{
		"/etc/peers",
		"/var/lib/peers",
		"/var/log/peers/audit.log",
		"/etc/systemd/system/peers.service.d",
		"/var/log/peers",
		"group ops",
	}
	if !slices.Equal(removed, want) {
		t.Errorf("removed = %q, want %q", removed, want)
	}
	if !slices.Contains(cmds, "groupdel ops") {
		t.Errorf("commands = %q, want groupdel ops", cmds)
	}
	for _, path := range want[:5] {
		if !strings.HasPrefix(path, "/") {
			continue
		}
		if _, err := os.Stat(filepath.Join(opts.Root, path)); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s still exists: %v", path, err)
		}
	}
}

// TestPurgeDataDarwinUser checks that a darwin user purge removes the data
// directory and the log directory.
func TestPurgeDataDarwinUser(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.Home = "/Users/alice"
	writeFileUnder(t, opts.Root, "/Users/alice/.peers/config.yaml", "broker: {}\n")
	writeFileUnder(t, opts.Root, "/Users/alice/Library/Logs/peers/broker.log", "log\n")

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	want := []string{"/Users/alice/.peers", "/Users/alice/Library/Logs/peers"}
	if !slices.Equal(removed, want) {
		t.Errorf("removed = %q, want %q", removed, want)
	}
}

// TestPurgeDataDarwinSystem checks that a darwin system purge removes the
// configuration, data and log directories and deletes the recorded admin group.
func TestPurgeDataDarwinSystem(t *testing.T) {
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.Home = "/Users/alice"
	opts.System = true
	opts.EUID = 0

	writeFileUnder(t, opts.Root, "/etc/peers/config.yaml", "broker: {}\n")
	writeFileUnder(t, opts.Root, "/var/lib/peers/identity.key", "key")
	writeFileUnder(t, opts.Root, "/var/log/peers/audit.log", "audit\n")
	writeFileUnder(t, opts.Root, "/var/log/peers/broker.log", "log\n")

	gid := configGID(t, opts, "/etc/peers/config.yaml")
	writeFileUnder(t, opts.Root, adminGroupRecord, "ops\n")
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case name == "dscl" && slices.Equal(args, []string{".", "-list", "/Groups", "PrimaryGroupID"}):
			// A second group must not share the configuration file's gid, or the
			// lookup resolves the owner to that group.
			return []byte("staff 1\nops " + strconv.Itoa(gid) + "\n"), nil
		case name == "dscl" && slices.Equal(args, []string{".", "-list", "/Users", "PrimaryGroupID"}):
			return []byte("root 0\n"), nil
		case name == "dscl" && len(args) == 3 && args[0] == "." && args[1] == "-read" && args[2] == "/Groups/ops":
			return []byte("PrimaryGroupID: " + strconv.Itoa(gid) + "\n"), nil
		}
		return nil, nil
	}

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	want := []string{"/etc/peers", "/var/lib/peers", "/var/log/peers", "group ops"}
	if !slices.Equal(removed, want) {
		t.Errorf("removed = %q, want %q", removed, want)
	}
	if !slices.Contains(cmds, "dseditgroup -o delete ops") {
		t.Errorf("commands = %q, want dseditgroup -o delete ops", cmds)
	}
}

// TestPurgeDataKeepsUnknownGroup checks that a system purge removes no group the
// group database does not know.
func TestPurgeDataKeepsUnknownGroup(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.GroupGID = func(string) (int, bool, error) { return 0, false, nil }
	opts.Run = func(string, ...string) ([]byte, error) { return nil, nil }

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	for _, path := range removed {
		if strings.HasPrefix(path, "group ") {
			t.Errorf("removed %q although the group does not exist", path)
		}
	}
}

// TestPurgeDataKeepsAnUnrecordedGroupWhenConfigIsGone checks that a purge
// without a record keeps the default group and reports why.
func TestPurgeDataKeepsAnUnrecordedGroupWhenConfigIsGone(t *testing.T) {
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		return nil, nil
	}

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	if slices.Contains(removed, "group "+DefaultAdminGroup) {
		t.Errorf("removed = %q, want the unrecorded group kept", removed)
	}
	if slices.Contains(cmds, "groupdel "+DefaultAdminGroup) {
		t.Errorf("commands = %q, want no group deletion", cmds)
	}
	if !groupKept(removed, DefaultAdminGroup) {
		t.Errorf("removed = %q, want a kept-group message naming %s", removed, DefaultAdminGroup)
	}
}

// TestOtherAccountBrokers checks that OtherAccountBrokers skips this account and
// the accounts that do not answer and lists the rest's account names sorted by
// uid.
func TestOtherAccountBrokers(t *testing.T) {
	opts := baseOptions(t)
	for _, uid := range []int{1000, 2000, 3000, 4000} {
		writeFileUnder(t, opts.Root, "/tmp/peers-"+strconv.Itoa(uid)+"/broker.sock", "socket")
	}
	opts.Probe = func(path string) (int, bool) {
		switch path {
		case "/tmp/peers-1000/broker.sock", "/tmp/peers-2000/broker.sock", "/tmp/peers-4000/broker.sock":
			return 4242, true
		}
		return 0, false
	}
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if name == "getent" && slices.Equal(args, []string{"passwd", "2000"}) {
			return []byte("bob:x:2000:2000:Bob:/home/bob:/bin/sh\n"), nil
		}
		if name == "getent" && slices.Equal(args, []string{"passwd", "4000"}) {
			return []byte("carol:x:4000:4000:Carol:/home/carol:/bin/sh\n"), nil
		}
		return nil, errors.New("no entry")
	}

	accounts, err := OtherAccountBrokers(opts)
	if err != nil {
		t.Fatalf("OtherAccountBrokers: %v", err)
	}
	if want := []string{"bob", "carol"}; !slices.Equal(accounts, want) {
		t.Errorf("accounts = %q, want %q", accounts, want)
	}
}

// TestOtherAccountBrokersWithoutTemporaryDir checks that OtherAccountBrokers
// returns an empty list and no error when /tmp does not exist.
func TestOtherAccountBrokersWithoutTemporaryDir(t *testing.T) {
	opts := baseOptions(t)
	accounts, err := OtherAccountBrokers(opts)
	if err != nil {
		t.Fatalf("OtherAccountBrokers: %v", err)
	}
	if len(accounts) != 0 {
		t.Errorf("accounts = %q, want none", accounts)
	}
}

func configGID(t *testing.T, opts Options, path string) int {
	t.Helper()
	info, err := os.Lstat(filepath.Join(opts.Root, path))
	if err != nil {
		t.Fatal(err)
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		t.Fatal("no owner information")
	}
	return int(st.Gid)
}

const adminGroupRecord = "/var/lib/peers/admin-group"

func purgeGroupOptions(t *testing.T) (Options, int) {
	t.Helper()
	opts := baseOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.PrimaryGroupAccounts = nil
	writeFileUnder(t, opts.Root, "/etc/peers/config.yaml", "broker: {}\n")
	return opts, configGID(t, opts, "/etc/peers/config.yaml")
}

func purgeGroupRun(cmds *[]string, gid int, name, passwd string) func(string, ...string) ([]byte, error) {
	return func(command string, args ...string) ([]byte, error) {
		*cmds = append(*cmds, strings.Join(append([]string{command}, args...), " "))
		switch {
		case command == "getent" && slices.Equal(args, []string{"group", strconv.Itoa(gid)}):
			return []byte(name + ":x:" + strconv.Itoa(gid) + ":\n"), nil
		case command == "getent" && slices.Equal(args, []string{"group", name}):
			return []byte(name + ":x:" + strconv.Itoa(gid) + ":\n"), nil
		case command == "getent" && slices.Equal(args, []string{"passwd"}):
			return []byte(passwd), nil
		}
		return nil, nil
	}
}

func groupKept(removed []string, name string) bool {
	for _, entry := range removed {
		if strings.HasPrefix(entry, "kept group "+name+":") {
			return true
		}
	}
	return false
}

// TestPurgeDataKeepsGroupWithoutInstallRecord checks that a purge without a
// record keeps every candidate group, including the default.
func TestPurgeDataKeepsGroupWithoutInstallRecord(t *testing.T) {
	for _, name := range []string{"root", "wheel", "staff", "sudo", "broker-ops"} {
		t.Run(name, func(t *testing.T) {
			opts, gid := purgeGroupOptions(t)
			var cmds []string
			opts.Run = purgeGroupRun(&cmds, gid, name, "root:x:0:0:root:/root:/bin/sh\n")

			removed, err := PurgeData(opts)
			if err != nil {
				t.Fatalf("PurgeData: %v", err)
			}
			if slices.Contains(cmds, "groupdel "+name) {
				t.Errorf("removed a group the installation did not record; commands = %q", cmds)
			}
			if !groupKept(removed, name) {
				t.Errorf("removed = %q, want a kept-group message", removed)
			}
		})
	}
}

// TestPurgeDataSkipsAnUnrecordedAbsentGroup checks that a purge without a record
// says nothing about a group that does not exist.
func TestPurgeDataSkipsAnUnrecordedAbsentGroup(t *testing.T) {
	cases := []struct {
		name       string
		withConfig bool
	}{
		{name: "configuration file's group", withConfig: true},
		{name: "default group"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.System = true
			opts.EUID = 0
			opts.GroupGID = nil
			if tc.withConfig {
				writeFileUnder(t, opts.Root, "/etc/peers/config.yaml", "broker: {}\n")
			}
			var cmds []string
			opts.Run = func(command string, args ...string) ([]byte, error) {
				cmds = append(cmds, strings.Join(append([]string{command}, args...), " "))
				if command == "getent" && len(args) == 2 && args[0] == "group" {
					return nil, exitError(t, 2)
				}
				return nil, nil
			}

			removed, err := PurgeData(opts)
			if err != nil {
				t.Fatalf("PurgeData: %v", err)
			}
			for _, entry := range removed {
				if strings.HasPrefix(entry, "kept group") {
					t.Errorf("removed = %q, want no kept-group message for an absent group", removed)
				}
			}
			if slices.Contains(cmds, "groupdel "+DefaultAdminGroup) {
				t.Errorf("commands = %q, want no group deletion", cmds)
			}
		})
	}
}

// TestPurgeDataKeepsUnrecordedDefaultGroup checks that a purge without a record
// keeps the default group and gives the missing-record reason.
func TestPurgeDataKeepsUnrecordedDefaultGroup(t *testing.T) {
	opts, gid := purgeGroupOptions(t)
	var cmds []string
	opts.Run = purgeGroupRun(&cmds, gid, DefaultAdminGroup, "root:x:0:0:root:/root:/bin/sh\n")

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	if slices.Contains(cmds, "groupdel "+DefaultAdminGroup) {
		t.Errorf("commands = %q, want no group deletion without a record", cmds)
	}
	if !groupKept(removed, DefaultAdminGroup) {
		t.Errorf("removed = %q, want a kept-group message naming %s", removed, DefaultAdminGroup)
	}
	var kept string
	for _, entry := range removed {
		if strings.HasPrefix(entry, "kept group "+DefaultAdminGroup+":") {
			kept = entry
		}
	}
	if !strings.Contains(kept, "did not record creating it") {
		t.Errorf("kept = %q, want the reason that the installation did not record creating the group", kept)
	}
}

// TestPurgeDataKeepsGroupWhenRecordDiffers checks that a purge keeps the recorded
// group when the configuration file belongs to another group.
func TestPurgeDataKeepsGroupWhenRecordDiffers(t *testing.T) {
	opts, gid := purgeGroupOptions(t)
	writeFileUnder(t, opts.Root, adminGroupRecord, "peers\n")
	var cmds []string
	opts.Run = purgeGroupRun(&cmds, gid, "wheel", "root:x:0:0:root:/root:/bin/sh\n")

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	if slices.Contains(cmds, "groupdel wheel") || slices.Contains(cmds, "groupdel peers") {
		t.Errorf("removed a group that does not match the record; commands = %q", cmds)
	}
	if !groupKept(removed, "peers") {
		t.Errorf("removed = %q, want a kept-group message", removed)
	}
}

// TestPurgeDataKeepsRootGroup checks that a purge keeps the recorded group when
// its gid is 0.
func TestPurgeDataKeepsRootGroup(t *testing.T) {
	opts, gid := purgeGroupOptions(t)
	writeFileUnder(t, opts.Root, adminGroupRecord, "wheel\n")
	var cmds []string
	opts.Run = purgeGroupRun(&cmds, gid, "wheel", "root:x:0:0:root:/root:/bin/sh\n")
	opts.GroupGID = func(name string) (int, bool, error) {
		if name == "wheel" {
			return 0, true, nil
		}
		return 999, true, nil
	}

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	if slices.Contains(cmds, "groupdel wheel") {
		t.Errorf("removed the root group; commands = %q", cmds)
	}
	if !groupKept(removed, "wheel") {
		t.Errorf("removed = %q, want a kept-group message", removed)
	}
}

// TestPurgeDataKeepsPrimaryGroup checks that a purge keeps the recorded group
// when it is an account's primary group.
func TestPurgeDataKeepsPrimaryGroup(t *testing.T) {
	opts, gid := purgeGroupOptions(t)
	writeFileUnder(t, opts.Root, adminGroupRecord, "peers\n")
	var cmds []string
	opts.Run = purgeGroupRun(&cmds, gid, "peers", "alice:x:1000:"+strconv.Itoa(gid)+":Alice:/home/alice:/bin/sh\n")
	opts.GroupGID = func(string) (int, bool, error) { return gid, true, nil }

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	if slices.Contains(cmds, "groupdel peers") {
		t.Errorf("removed an account's primary group; commands = %q", cmds)
	}
	if !groupKept(removed, "peers") {
		t.Errorf("removed = %q, want a kept-group message", removed)
	}
}

// TestPurgeDataDeletesRecordedGroup checks that a purge deletes the recorded
// group when every condition holds.
func TestPurgeDataDeletesRecordedGroup(t *testing.T) {
	opts, gid := purgeGroupOptions(t)
	writeFileUnder(t, opts.Root, adminGroupRecord, "peers\n")
	var cmds []string
	opts.Run = purgeGroupRun(&cmds, gid, "peers", "root:x:0:0:root:/root:/bin/sh\n")

	removed, err := PurgeData(opts)
	if err != nil {
		t.Fatalf("PurgeData: %v", err)
	}
	if !slices.Contains(cmds, "groupdel peers") {
		t.Errorf("commands = %q, want groupdel peers", cmds)
	}
	if !slices.Contains(removed, "group peers") {
		t.Errorf("removed = %q, want the group reported as removed", removed)
	}
}

// TestPurgeDataRejectsRelativeHome checks that PurgeData rejects a relative home
// directory before running a command.
func TestPurgeDataRejectsRelativeHome(t *testing.T) {
	opts := baseOptions(t)
	opts.Home = "alice"
	opts.Run = func(command string, args ...string) ([]byte, error) {
		t.Errorf("ran %s %s before rejecting the home directory", command, strings.Join(args, " "))
		return nil, nil
	}
	if _, err := PurgeData(opts); err == nil {
		t.Fatal("PurgeData accepted a relative home directory")
	}
}

// TestServiceBinaryStripsSystemdPrefixes checks that ServiceBinary strips the
// systemd prefix characters from ExecStart.
func TestServiceBinaryStripsSystemdPrefixes(t *testing.T) {
	opts := baseOptions(t)
	path, err := unitPath(opts)
	if err != nil {
		t.Fatal(err)
	}
	writeFileUnder(t, opts.Root, path, "[Service]\nExecStart=-@+/opt/peers service run\n")
	got, ok, err := ServiceBinary(opts)
	if err != nil {
		t.Fatalf("ServiceBinary: %v", err)
	}
	if !ok || got != "/opt/peers" {
		t.Errorf("ServiceBinary = %q, %v, want /opt/peers", got, ok)
	}
}
