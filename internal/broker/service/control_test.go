package service

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
)

func writeInstalledUnit(t *testing.T, opts Options) string {
	t.Helper()
	path, err := unitPath(opts)
	if err != nil {
		t.Fatalf("unitPath: %v", err)
	}
	fsPath := rooted(opts, path)
	if err := os.MkdirAll(filepath.Dir(fsPath), 0o755); err != nil {
		t.Fatalf("create %s: %v", filepath.Dir(fsPath), err)
	}
	if err := os.WriteFile(fsPath, []byte("unit\n"), 0o644); err != nil {
		t.Fatalf("write %s: %v", fsPath, err)
	}
	return path
}

func linuxControlOptions(t *testing.T) Options {
	t.Helper()
	opts := baseOptions(t)
	opts.StatusProbe = func(string) (DegradedState, bool, error) {
		return DegradedState{}, false, nil
	}
	return opts
}

// TestStartLinuxUser checks that a user Linux start runs systemctl start and
// reports the service ready at pid 4242.
func TestStartLinuxUser(t *testing.T) {
	opts := linuxControlOptions(t)
	writeInstalledUnit(t, opts)

	var cmds []string
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if name == "systemctl" && slices.Contains(args, "start") {
			started = true
		}
		if name == "systemctl" && slices.Contains(args, "show") {
			if started {
				return []byte("4242\n"), nil
			}
			return []byte("0\n"), nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) {
		if started {
			return 4242, true
		}
		return 0, false
	}

	res, err := Start(opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if want := []string{"systemctl --user start peers.service"}; !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	wantCmds := []string{
		"systemctl --user start peers.service",
		"systemctl --user show --property MainPID --value peers.service",
	}
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
	if res.Domain != "" {
		t.Errorf("Domain = %q, want empty on Linux", res.Domain)
	}
}

// TestStartLinuxSystem checks that a system Linux start runs the system
// systemctl start first and reports the service ready.
func TestStartLinuxSystem(t *testing.T) {
	opts := linuxControlOptions(t)
	opts.System = true
	opts.EUID = 0
	opts.UID = 0
	writeInstalledUnit(t, opts)

	var cmds []string
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if slices.Contains(args, "start") {
			started = true
		}
		if slices.Contains(args, "show") {
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 4242, started }

	res, err := Start(opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if want := []string{"systemctl start peers.service"}; !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	if len(cmds) == 0 || cmds[0] != "systemctl start peers.service" {
		t.Errorf("commands = %q, want the system start first", cmds)
	}
}

// TestStopLinux checks that a Linux stop runs systemctl stop and reports the
// same command.
func TestStopLinux(t *testing.T) {
	opts := linuxControlOptions(t)
	writeInstalledUnit(t, opts)

	var cmds []string
	opts.Run = recordCommands(&cmds)

	res, err := Stop(opts)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if want := []string{"systemctl --user stop peers.service"}; !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	if !slices.Equal(cmds, res.Commands) {
		t.Errorf("commands = %q, want %q", cmds, res.Commands)
	}
}

// TestRestartLinux checks that a Linux restart runs systemctl restart and then
// waits for the service to report ready.
func TestRestartLinux(t *testing.T) {
	opts := linuxControlOptions(t)
	writeInstalledUnit(t, opts)

	var cmds []string
	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		if slices.Contains(args, "restart") {
			started = true
		}
		if slices.Contains(args, "show") && started {
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 4242, started }

	res, err := Restart(opts)
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	want := []string{"systemctl --user restart peers.service"}
	if !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	wantCmds := append(want, "systemctl --user show --property MainPID --value peers.service")
	if !slices.Equal(cmds, wantCmds) {
		t.Errorf("commands = %q, want %q", cmds, wantCmds)
	}
}

// TestStartNotInstalled checks that Start at either level returns ErrNotInstalled
// naming the level before running a start, stop or restart command.
func TestStartNotInstalled(t *testing.T) {
	for _, system := range []bool{false, true} {
		t.Run(fmt.Sprintf("system=%t", system), func(t *testing.T) {
			opts := linuxControlOptions(t)
			opts.System = system
			var cmds []string
			opts.Run = func(name string, args ...string) ([]byte, error) {
				cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
				return nil, exitError(t, 1)
			}

			_, err := Start(opts)
			if !errors.Is(err, ErrNotInstalled) {
				t.Fatalf("Start error = %v, want ErrNotInstalled", err)
			}
			level := "user service"
			if system {
				level = "system service"
			}
			if !strings.Contains(err.Error(), level) {
				t.Errorf("error %q does not name the %s level", err, level)
			}
			for _, cmd := range cmds {
				for _, verb := range []string{" start", " stop", " restart"} {
					if strings.Contains(cmd, verb) {
						t.Errorf("Start ran %q before reporting the service is not installed", cmd)
					}
				}
			}
		})
	}
}

// TestStopAndRestartNotInstalled checks that Stop and Restart return
// ErrNotInstalled when no service is installed.
func TestStopAndRestartNotInstalled(t *testing.T) {
	for _, verb := range []string{"Stop", "Restart"} {
		t.Run(verb, func(t *testing.T) {
			opts := linuxControlOptions(t)
			opts.Run = func(string, ...string) ([]byte, error) { return nil, exitError(t, 1) }
			var err error
			if verb == "Stop" {
				_, err = Stop(opts)
			} else {
				_, err = Restart(opts)
			}
			if !errors.Is(err, ErrNotInstalled) {
				t.Fatalf("%s error = %v, want ErrNotInstalled", verb, err)
			}
		})
	}
}

// TestStartReadinessTimeoutKeepsTheServiceEnabled checks that a start whose
// Broker never answers reports the startup_failed reason without stopping or
// disabling the service.
func TestStartReadinessTimeoutKeepsTheServiceEnabled(t *testing.T) {
	opts := linuxControlOptions(t)
	writeInstalledUnit(t, opts)
	opts.ReadyTimeout = 20 * time.Millisecond

	var cmds []string
	opts.Run = func(name string, args ...string) ([]byte, error) {
		cmds = append(cmds, strings.Join(append([]string{name}, args...), " "))
		switch {
		case name == "systemctl" && slices.Contains(args, "show"):
			return []byte("0\n"), nil
		case name == "journalctl":
			return []byte(fmt.Sprintf(`{"time":%q,"event":"startup_failed","error":"bind: address already in use"}`,
				time.Now().UTC().Format(time.RFC3339Nano))), nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 0, false }

	res, err := Start(opts)
	if err == nil {
		t.Fatal("Start succeeded although the Broker never answered")
	}
	if !strings.Contains(err.Error(), "startup_failed record says: bind: address already in use") {
		t.Errorf("error = %q, want the startup_failed reason", err)
	}
	if want := []string{"systemctl --user start peers.service"}; !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	for _, cmd := range cmds {
		if strings.Contains(cmd, "disable") || strings.Contains(cmd, "stop") {
			t.Errorf("a failed start ran %q; it must not stop or disable the service", cmd)
		}
	}
}

// TestStartReportsDegraded checks that Start returns a *DegradedError carrying
// the state together with a complete Result.
func TestStartReportsDegraded(t *testing.T) {
	opts := linuxControlOptions(t)
	writeInstalledUnit(t, opts)

	started := false
	opts.Run = func(name string, args ...string) ([]byte, error) {
		if slices.Contains(args, "start") {
			started = true
		}
		if slices.Contains(args, "show") {
			return []byte("4242\n"), nil
		}
		return nil, nil
	}
	opts.Probe = func(string) (int, bool) { return 4242, started }
	want := DegradedState{Level: localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY, Step: "listen", Reason: "listen :17443: address already in use", Fix: "change broker.listen"}
	opts.StatusProbe = func(string) (DegradedState, bool, error) { return want, true, nil }

	res, err := Start(opts)
	var degraded *DegradedError
	if !errors.As(err, &degraded) {
		t.Fatalf("Start error = %v, want *DegradedError", err)
	}
	if degraded.State != want {
		t.Errorf("DegradedError.State = %+v, want %+v", degraded.State, want)
	}
	if wantCmds := []string{"systemctl --user start peers.service"}; !slices.Equal(res.Commands, wantCmds) {
		t.Errorf("Result.Commands = %q, want the complete result %q", res.Commands, wantCmds)
	}
}

// TestStartReportsStatusProbeError checks that a failing status probe is retried
// and reported with the matching status hint, not as a *DegradedError.
func TestStartReportsStatusProbeError(t *testing.T) {
	cases := []struct {
		name   string
		system bool
		want   string
	}{
		{"user installation", false, "check it with `peers service status`"},
		{"system installation", true, "check it with `peers service status --system`"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts := linuxControlOptions(t)
			if tc.system {
				opts.System = true
				opts.EUID = 0
				opts.UID = 0
			}
			writeInstalledUnit(t, opts)
			opts.ReadyTimeout = 150 * time.Millisecond
			started := false
			opts.Run = func(name string, args ...string) ([]byte, error) {
				if slices.Contains(args, "start") {
					started = true
				}
				if slices.Contains(args, "show") {
					return []byte("4242\n"), nil
				}
				return nil, nil
			}
			opts.Probe = func(string) (int, bool) { return 4242, started }
			probes := 0
			opts.StatusProbe = func(string) (DegradedState, bool, error) {
				probes++
				return DegradedState{}, false, errors.New("connection reset")
			}

			_, err := Start(opts)
			if err == nil {
				t.Fatal("Start error = nil, want the status probe error")
			}
			for _, want := range []string{"connection reset", "cannot read the status", tc.want} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("Start error = %q, want it to contain %q", err, want)
				}
			}
			if probes < 2 {
				t.Errorf("StatusProbe calls = %d, want it retried until the readiness deadline", probes)
			}
			var degraded *DegradedError
			if errors.As(err, &degraded) {
				t.Fatalf("Start reported *DegradedError for a probe error: %v", err)
			}
		})
	}
}

// TestStartAndRestartRetryTheStatusProbe checks that both commands retry a
// Status call that fails once instead of failing the command.
func TestStartAndRestartRetryTheStatusProbe(t *testing.T) {
	for _, command := range []string{"start", "restart"} {
		t.Run(command, func(t *testing.T) {
			opts := linuxControlOptions(t)
			writeInstalledUnit(t, opts)
			opts.ReadyTimeout = 250 * time.Millisecond
			started := false
			opts.Run = func(name string, args ...string) ([]byte, error) {
				if slices.Contains(args, command) {
					started = true
				}
				if slices.Contains(args, "show") && started {
					return []byte("4242\n"), nil
				}
				return nil, nil
			}
			opts.Probe = func(string) (int, bool) { return 4242, started }
			probes := 0
			opts.StatusProbe = func(string) (DegradedState, bool, error) {
				probes++
				if probes == 1 {
					return DegradedState{}, false, errors.New("connection reset")
				}
				return DegradedState{}, false, nil
			}

			var err error
			if command == "start" {
				_, err = Start(opts)
			} else {
				_, err = Restart(opts)
			}
			if err != nil {
				t.Fatalf("%s: %v", command, err)
			}
			if probes < 2 {
				t.Errorf("StatusProbe calls = %d, want a retry", probes)
			}
		})
	}
}

type darwinFake struct {
	t                    *testing.T
	cmds                 []string
	loaded               bool
	pid                  int
	bootstrapFailures    int
	bootstraps           int
	stillLoadedPolls     int
	remainingLoadedPolls int
	unloadedAt           int
}

func (f *darwinFake) run(name string, args ...string) ([]byte, error) {
	f.cmds = append(f.cmds, strings.Join(append([]string{name}, args...), " "))
	if name != "launchctl" || len(args) == 0 {
		return nil, nil
	}
	switch args[0] {
	case "print":
		if len(args) != 2 {
			return nil, nil
		}
		switch args[1] {
		case "gui/501", "system":
			return []byte("domain\n"), nil
		case "gui/501/" + Label, "system/" + Label:
			if f.loaded {
				return []byte(fmt.Sprintf("state = running\n\tpid = %d\n", f.pid)), nil
			}
			if f.remainingLoadedPolls > 0 {
				f.remainingLoadedPolls--
				return []byte("state = running\n"), nil
			}
			f.unloadedAt = len(f.cmds) - 1
			return []byte("Could not find service"), exitError(f.t, 113)
		default:
			return []byte("Could not find domain"), exitError(f.t, 113)
		}
	case "bootout":
		if !f.loaded {
			return []byte("No such process"), exitError(f.t, 3)
		}
		f.loaded = false
		f.pid = 0
		f.remainingLoadedPolls = f.stillLoadedPolls
		f.unloadedAt = -1
		return nil, nil
	case "enable":
		return nil, nil
	case "kickstart":
		f.loaded = true
		f.pid = 4242
		f.remainingLoadedPolls = 0
		return nil, nil
	case "bootstrap":
		f.bootstraps++
		if f.bootstrapFailures > 0 {
			f.bootstrapFailures--
			return []byte("Bootstrap failed: 5: Input/output error"), exitError(f.t, 5)
		}
		f.loaded = true
		f.pid = 4242
		f.remainingLoadedPolls = 0
		return nil, nil
	}
	return nil, nil
}

func darwinControlOptions(t *testing.T) (Options, *darwinFake) {
	t.Helper()
	opts := baseOptions(t)
	opts.GOOS = "darwin"
	opts.Home = "/Users/alice"
	opts.UID = 501
	opts.EUID = 501
	opts.Exe = "/usr/local/bin/peers"
	opts.TempDir = "/tmp"
	opts.StatusProbe = func(string) (DegradedState, bool, error) {
		return DegradedState{}, false, nil
	}
	fake := &darwinFake{t: t, unloadedAt: -1}
	opts.Run = fake.run
	opts.Probe = func(string) (int, bool) { return fake.pid, fake.loaded }
	return opts, fake
}

// TestStartDarwinBootstrapsAnUnloadedService checks that a start of an unloaded
// darwin service enables the label and bootstraps the plist.
func TestStartDarwinBootstrapsAnUnloadedService(t *testing.T) {
	opts, _ := darwinControlOptions(t)
	plist := writeInstalledUnit(t, opts)

	res, err := Start(opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	want := []string{
		"launchctl enable gui/501/" + Label,
		"launchctl bootstrap gui/501 " + plist,
	}
	if !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	if res.Domain != "gui/501" {
		t.Errorf("Domain = %q, want gui/501", res.Domain)
	}
}

// TestStartDarwinKickstartsALoadedService checks that a start of a loaded darwin
// service kickstarts the job in place.
func TestStartDarwinKickstartsALoadedService(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	writeInstalledUnit(t, opts)
	fake.loaded = true
	fake.pid = 111

	res, err := Start(opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	want := []string{"launchctl kickstart gui/501/" + Label}
	if !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	if !fake.loaded || fake.pid != 4242 {
		t.Errorf("after kickstart loaded=%t pid=%d, want loaded with pid 4242", fake.loaded, fake.pid)
	}
}

// TestStopDarwinWaitsForUnload checks that a darwin stop boots the job out,
// leaves the enabled state alone and returns with the job unloaded.
func TestStopDarwinWaitsForUnload(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	writeInstalledUnit(t, opts)
	fake.loaded = true
	fake.pid = 111

	res, err := Stop(opts)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	want := []string{"launchctl bootout gui/501/" + Label}
	if !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	if fake.loaded {
		t.Error("the job is still loaded after Stop")
	}
	for _, cmd := range res.Commands {
		if strings.Contains(cmd, "enable") || strings.Contains(cmd, "bootstrap") || strings.Contains(cmd, "disable") {
			t.Errorf("Stop ran %q; it must not change the enabled state", cmd)
		}
	}
}

// TestStopDarwinWaitsUntilLaunchdReportsUnloaded checks that Stop keeps probing
// until launchctl reports the job gone after bootout.
func TestStopDarwinWaitsUntilLaunchdReportsUnloaded(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	writeInstalledUnit(t, opts)
	fake.loaded = true
	fake.pid = 111
	fake.stillLoadedPolls = 3

	if _, err := Stop(opts); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	bootoutAt := slices.Index(fake.cmds, "launchctl bootout gui/501/"+Label)
	if bootoutAt < 0 {
		t.Fatalf("commands = %q, want a bootout", fake.cmds)
	}
	if fake.unloadedAt < 0 {
		t.Fatal("Stop returned before launchctl print reported the job unloaded")
	}
	if got := fake.unloadedAt - bootoutAt - 1; got != fake.stillLoadedPolls {
		t.Errorf("print probes reporting the job still loaded after bootout = %d, want %d; commands = %q",
			got, fake.stillLoadedPolls, fake.cmds)
	}
}

// TestStopDarwinIgnoresBootoutExitThree checks that Stop succeeds when bootout
// reports exit 3 because the job was not loaded.
func TestStopDarwinIgnoresBootoutExitThree(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	writeInstalledUnit(t, opts)
	fake.loaded = false

	res, err := Stop(opts)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if want := []string{"launchctl bootout gui/501/" + Label}; !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
}

// TestRestartDarwinReloadsThePlist checks that a darwin restart boots out, enables
// and bootstraps the plist, leaving the job loaded at pid 4242.
func TestRestartDarwinReloadsThePlist(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	plist := writeInstalledUnit(t, opts)
	fake.loaded = true
	fake.pid = 111

	res, err := Restart(opts)
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	want := []string{
		"launchctl bootout gui/501/" + Label,
		"launchctl enable gui/501/" + Label,
		"launchctl bootstrap gui/501 " + plist,
	}
	if !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
	if !fake.loaded || fake.pid != 4242 {
		t.Errorf("after restart loaded=%t pid=%d, want loaded with pid 4242", fake.loaded, fake.pid)
	}
}

// TestRestartDarwinNotLoadedSkipsBootoutExitThree checks that a restart of an
// unloaded darwin service ignores bootout's exit 3 and still enables and
// bootstraps.
func TestRestartDarwinNotLoadedSkipsBootoutExitThree(t *testing.T) {
	opts, _ := darwinControlOptions(t)
	plist := writeInstalledUnit(t, opts)

	res, err := Restart(opts)
	if err != nil {
		t.Fatalf("Restart: %v", err)
	}
	want := []string{
		"launchctl bootout gui/501/" + Label,
		"launchctl enable gui/501/" + Label,
		"launchctl bootstrap gui/501 " + plist,
	}
	if !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
}

// TestRestartDarwinBootstrapsAfterLaunchdReportsUnloaded checks that Restart
// bootstraps only after launchctl reports the old job gone.
func TestRestartDarwinBootstrapsAfterLaunchdReportsUnloaded(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	plist := writeInstalledUnit(t, opts)
	fake.loaded = true
	fake.pid = 111
	fake.stillLoadedPolls = 3

	if _, err := Restart(opts); err != nil {
		t.Fatalf("Restart: %v", err)
	}
	bootoutAt := slices.Index(fake.cmds, "launchctl bootout gui/501/"+Label)
	if bootoutAt < 0 {
		t.Fatalf("commands = %q, want a bootout", fake.cmds)
	}
	bootstrapAt := slices.Index(fake.cmds, "launchctl bootstrap gui/501 "+plist)
	if bootstrapAt < 0 {
		t.Fatalf("commands = %q, want a bootstrap", fake.cmds)
	}
	if fake.unloadedAt < 0 {
		t.Fatal("Restart bootstrapped before launchctl print reported the job unloaded")
	}
	if bootstrapAt < fake.unloadedAt {
		t.Errorf("commands = %q, want the bootstrap after the print at %d reported the job gone", fake.cmds, fake.unloadedAt)
	}
	if got := fake.unloadedAt - bootoutAt - 1; got != fake.stillLoadedPolls {
		t.Errorf("print probes reporting the job still loaded after bootout = %d, want %d; commands = %q",
			got, fake.stillLoadedPolls, fake.cmds)
	}
}

// TestRestartDarwinRefusesAMissingPlist checks that Restart refuses a loaded job
// whose plist is missing before running any service-manager command.
func TestRestartDarwinRefusesAMissingPlist(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	path := writeInstalledUnit(t, opts)
	fake.loaded = true
	fake.pid = 111
	if err := os.Remove(rooted(opts, path)); err != nil {
		t.Fatalf("remove %s: %v", path, err)
	}

	res, err := Restart(opts)
	if err == nil {
		t.Fatal("Restart succeeded although the plist is missing")
	}
	for _, want := range []string{"plist", "service install"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error = %q, want it to mention %q", err, want)
		}
	}
	if !fake.loaded || fake.pid != 111 {
		t.Errorf("after the refused restart loaded=%t pid=%d, want the untouched loaded job with pid 111", fake.loaded, fake.pid)
	}
	for _, cmd := range fake.cmds {
		if strings.Contains(cmd, "bootout") || strings.Contains(cmd, "bootstrap") || strings.Contains(cmd, "disable") {
			t.Errorf("Restart ran %q before checking the plist exists", cmd)
		}
	}
	if res.Domain != "gui/501" {
		t.Errorf("Domain = %q, want gui/501 on the failure path", res.Domain)
	}
}

// TestRestartDarwinReadinessFailureKeepsTheServiceEnabled checks that a restart
// whose Broker never answers reports the startup_failed reason without disabling
// the service.
func TestRestartDarwinReadinessFailureKeepsTheServiceEnabled(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	writeInstalledUnit(t, opts)
	fake.loaded = true
	fake.pid = 111
	opts.ReadyTimeout = 20 * time.Millisecond
	opts.Probe = func(string) (int, bool) { return 0, false }

	logPath := rooted(opts, filepath.Join(opts.Home, "Library", "Logs", "peers", "broker.log"))
	if err := os.MkdirAll(filepath.Dir(logPath), 0o700); err != nil {
		t.Fatalf("create log directory: %v", err)
	}
	// The record is written after Restart begins, because one with an earlier
	// time is ignored.
	baseRun := fake.run
	opts.Run = func(name string, args ...string) ([]byte, error) {
		out, err := baseRun(name, args...)
		if err == nil && name == "launchctl" && len(args) > 0 && args[0] == "bootstrap" {
			record := fmt.Sprintf(`{"time":%q,"event":"startup_failed","error":"bind: address already in use"}`,
				time.Now().UTC().Format(time.RFC3339Nano))
			if writeErr := os.WriteFile(logPath, []byte(record+"\n"), 0o600); writeErr != nil {
				t.Errorf("write %s: %v", logPath, writeErr)
			}
		}
		return out, err
	}

	res, err := Restart(opts)
	if err == nil {
		t.Fatal("Restart succeeded although the Broker never answered")
	}
	if !strings.Contains(err.Error(), "startup_failed record says: bind: address already in use") {
		t.Errorf("error = %q, want the startup_failed reason", err)
	}
	if res.Domain != "gui/501" {
		t.Errorf("Domain = %q, want gui/501 on the failure path", res.Domain)
	}
	for _, cmd := range fake.cmds {
		if strings.Contains(cmd, "disable") {
			t.Errorf("a failed restart ran %q; it must not disable the service", cmd)
		}
	}
}

// TestStartDarwinRetriesBootstrapExitFive checks that a bootstrap that fails with
// exit 5 once is retried.
func TestStartDarwinRetriesBootstrapExitFive(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	writeInstalledUnit(t, opts)
	fake.loaded = false
	fake.bootstrapFailures = 1

	if _, err := Start(opts); err != nil {
		t.Fatalf("Start: %v", err)
	}
	if fake.bootstraps != 2 {
		t.Errorf("bootstrap attempts = %d, want 2", fake.bootstraps)
	}
}

// TestRestartDarwinReportsDegradedWithTheFullResult checks that a degraded restart
// returns a *DegradedError with the complete Result and leaves the service
// enabled.
func TestRestartDarwinReportsDegradedWithTheFullResult(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	plist := writeInstalledUnit(t, opts)
	fake.loaded = true
	fake.pid = 111
	want := DegradedState{Level: localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY, Step: "audit", Reason: "audit log is not writable", Fix: "check the audit log path"}
	opts.StatusProbe = func(string) (DegradedState, bool, error) { return want, true, nil }

	res, err := Restart(opts)
	var degraded *DegradedError
	if !errors.As(err, &degraded) {
		t.Fatalf("Restart error = %v, want *DegradedError", err)
	}
	if degraded.State != want {
		t.Errorf("DegradedError.State = %+v, want %+v", degraded.State, want)
	}
	wantCmds := []string{
		"launchctl bootout gui/501/" + Label,
		"launchctl enable gui/501/" + Label,
		"launchctl bootstrap gui/501 " + plist,
	}
	if !slices.Equal(res.Commands, wantCmds) {
		t.Errorf("Result.Commands = %q, want the complete result %q", res.Commands, wantCmds)
	}
	if res.Domain != "gui/501" {
		t.Errorf("Domain = %q, want gui/501", res.Domain)
	}
	for _, cmd := range fake.cmds {
		if strings.Contains(cmd, "disable") {
			t.Errorf("a degraded restart ran %q; it must leave the service enabled", cmd)
		}
	}
}

// TestStartDarwinNotInstalled checks that a darwin start with no service returns
// ErrNotInstalled before running a launchctl command.
func TestStartDarwinNotInstalled(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	fake.loaded = false

	if _, err := Start(opts); !errors.Is(err, ErrNotInstalled) {
		t.Fatalf("Start error = %v, want ErrNotInstalled", err)
	}
	for _, cmd := range fake.cmds {
		if strings.Contains(cmd, "bootstrap") || strings.Contains(cmd, "kickstart") || strings.Contains(cmd, "enable") {
			t.Errorf("Start ran %q before reporting the service is not installed", cmd)
		}
	}
}

// TestStartAndRestartDarwinWithoutAGuiDomainReportTheGraphicalSession checks that
// Start and Restart without a gui domain report the missing graphical login
// session instead of installation advice.
func TestStartAndRestartDarwinWithoutAGuiDomainReportTheGraphicalSession(t *testing.T) {
	verbs := []struct {
		name string
		run  func(Options) (Result, error)
	}{
		{"Start", Start},
		{"Restart", Restart},
	}
	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			opts, fake := darwinControlOptions(t)
			opts.UID = 777
			opts.EUID = 777
			writeInstalledUnit(t, opts)

			_, err := verb.run(opts)
			if err == nil {
				t.Fatalf("%s succeeded without a gui/777 domain", verb.name)
			}
			if errors.Is(err, ErrNotInstalled) {
				t.Fatalf("%s error = %v, want a missing-domain error", verb.name, err)
			}
			for _, want := range []string{"gui/777", "graphical login session"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %q, want it to contain %q", err, want)
				}
			}
			for _, unwanted := range []string{"service install", "--system"} {
				if strings.Contains(err.Error(), unwanted) {
					t.Errorf("error = %q, want no installation wording such as %q", err, unwanted)
				}
			}
			for _, cmd := range fake.cmds {
				if strings.Contains(cmd, "bootstrap") || strings.Contains(cmd, "kickstart") || strings.Contains(cmd, "enable") || strings.Contains(cmd, "bootout") {
					t.Errorf("%s ran %q without a gui domain", verb.name, cmd)
				}
			}
		})
	}
}

// TestStopAndRestartDarwinUnloadTimeoutAdvice checks that an unload timeout names
// the command to retry instead of installation advice.
func TestStopAndRestartDarwinUnloadTimeoutAdvice(t *testing.T) {
	verbs := []struct {
		name  string
		run   func(Options) (Result, error)
		again string
	}{
		{"Stop", Stop, "run service stop again"},
		{"Restart", Restart, "run service restart again"},
	}
	for _, verb := range verbs {
		t.Run(verb.name, func(t *testing.T) {
			opts, fake := darwinControlOptions(t)
			plist := writeInstalledUnit(t, opts)
			fake.loaded = true
			fake.pid = 111
			fake.stillLoadedPolls = 1000
			opts.StopTimeout = 20 * time.Millisecond

			_, err := verb.run(opts)
			if err == nil {
				t.Fatalf("%s succeeded although launchd never reported the job unloaded", verb.name)
			}
			if !strings.Contains(err.Error(), "cannot unload the old service") {
				t.Errorf("error = %q, want the unload timeout", err)
			}
			if !strings.Contains(err.Error(), verb.again) {
				t.Errorf("error = %q, want it to say %q", err, verb.again)
			}
			if strings.Contains(err.Error(), "install again") {
				t.Errorf("error = %q, want no installation advice", err)
			}
			for _, cmd := range fake.cmds {
				if verb.name == "Restart" && strings.Contains(cmd, "bootstrap gui/501 "+plist) {
					t.Errorf("Restart ran %q while the old job was still loaded", cmd)
				}
			}
		})
	}
}

// TestStopDarwinWithoutAGuiDomainIsANoOp checks that Stop without a gui domain
// runs no service-manager command and leaves the domain empty.
func TestStopDarwinWithoutAGuiDomainIsANoOp(t *testing.T) {
	opts, fake := darwinControlOptions(t)
	opts.UID = 777
	opts.EUID = 777
	writeInstalledUnit(t, opts)

	res, err := Stop(opts)
	if err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if len(res.Commands) != 0 {
		t.Errorf("Result.Commands = %q, want no service-manager command", res.Commands)
	}
	if res.Domain != "" {
		t.Errorf("Domain = %q, want empty when no domain exists", res.Domain)
	}
	for _, cmd := range fake.cmds {
		if strings.Contains(cmd, "bootout") {
			t.Errorf("Stop ran %q without a gui domain", cmd)
		}
	}
}
