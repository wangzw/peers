package service

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
)

// TestLinuxControlCommandsReportServiceManagerFailures checks that a failed
// systemctl command stops the command and carries its output.
func TestLinuxControlCommandsReportServiceManagerFailures(t *testing.T) {
	commands := []struct {
		name string
		verb string
		run  func(Options) (Result, error)
	}{
		{"start", "start", Start},
		{"stop", "stop", Stop},
		{"restart", "restart", Restart},
	}
	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			opts := linuxControlOptions(t)
			writeInstalledUnit(t, opts)
			opts.Run = func(name string, args ...string) ([]byte, error) {
				if slices.Contains(args, tc.verb) {
					return []byte("Job failed"), exitError(t, 1)
				}
				return nil, nil
			}

			res, err := tc.run(opts)
			if err == nil {
				t.Fatalf("%s succeeded although systemctl failed", tc.name)
			}
			wantCmd := "systemctl --user " + tc.verb + " " + UnitName
			for _, want := range []string{"cannot run " + wantCmd, "Job failed"} {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("%s error = %q, want it to contain %q", tc.name, err, want)
				}
			}
			if want := []string{wantCmd}; !slices.Equal(res.Commands, want) {
				t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
			}
		})
	}
}

// TestControlCommandsRejectAnUnsupportedPlatform checks that no service-manager
// command runs on a platform the package does not support.
func TestControlCommandsRejectAnUnsupportedPlatform(t *testing.T) {
	commands := []struct {
		name string
		run  func(Options) (Result, error)
	}{
		{"start", Start},
		{"stop", Stop},
		{"restart", Restart},
	}
	for _, tc := range commands {
		t.Run(tc.name, func(t *testing.T) {
			opts := baseOptions(t)
			opts.GOOS = "windows"
			opts.Run = func(name string, args ...string) ([]byte, error) {
				t.Errorf("%s ran %s %s on an unsupported platform", tc.name, name, strings.Join(args, " "))
				return nil, nil
			}

			res, err := tc.run(opts)
			const want = `cannot run the service command on platform "windows"`
			if err == nil || err.Error() != want {
				t.Fatalf("%s error = %v, want %q", tc.name, err, want)
			}
			if len(res.Commands) != 0 {
				t.Errorf("Result.Commands = %q, want none", res.Commands)
			}
		})
	}
}

// TestStartReportsAnUninspectableInstallation checks that a unit path that
// cannot be inspected is an error rather than "not installed".
func TestStartReportsAnUninspectableInstallation(t *testing.T) {
	opts := linuxControlOptions(t)
	blocker := filepath.Join(opts.Root, "home/alice/.config")
	if err := os.MkdirAll(filepath.Dir(blocker), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(blocker, []byte("not a directory"), 0o644); err != nil {
		t.Fatal(err)
	}
	opts.Run = func(name string, args ...string) ([]byte, error) {
		t.Errorf("Start ran %s %s although the installation could not be inspected", name, strings.Join(args, " "))
		return nil, nil
	}

	_, err := Start(opts)
	if err == nil {
		t.Fatal("Start succeeded although the unit path could not be inspected")
	}
	unitPath := filepath.Join("/home/alice/.config/systemd/user", UnitName)
	if !strings.Contains(err.Error(), "cannot inspect "+unitPath) {
		t.Errorf("Start error = %q, want the failed inspection naming %s", err, unitPath)
	}
}

// TestStartDarwinSystemStartsInTheSystemDomain checks that the system daemon
// is enabled and bootstrapped in the system domain.
func TestStartDarwinSystemStartsInTheSystemDomain(t *testing.T) {
	opts, _ := darwinControlOptions(t)
	opts.System = true
	plist := writeInstalledUnit(t, opts)

	res, err := Start(opts)
	if err != nil {
		t.Fatalf("Start: %v", err)
	}
	if res.Domain != "system" {
		t.Errorf("Domain = %q, want system", res.Domain)
	}
	want := []string{
		"launchctl enable system/" + Label,
		"launchctl bootstrap system " + plist,
	}
	if !slices.Equal(res.Commands, want) {
		t.Errorf("Result.Commands = %q, want %q", res.Commands, want)
	}
}

// TestDarwinControlCommandsReportServiceManagerFailures checks that a failed
// launchctl step names the exact command and carries its output.
func TestDarwinControlCommandsReportServiceManagerFailures(t *testing.T) {
	cases := []struct {
		name     string
		run      func(Options) (Result, error)
		loaded   bool
		failVerb string
		wantCmd  func(plist string) string
	}{
		{
			name:     "start kickstart",
			run:      Start,
			loaded:   true,
			failVerb: "kickstart",
			wantCmd:  func(string) string { return "launchctl kickstart gui/501/" + Label },
		},
		{
			name:     "start enable",
			run:      Start,
			failVerb: "enable",
			wantCmd:  func(string) string { return "launchctl enable gui/501/" + Label },
		},
		{
			name:     "start bootstrap",
			run:      Start,
			failVerb: "bootstrap",
			wantCmd:  func(plist string) string { return "launchctl bootstrap gui/501 " + plist },
		},
		{
			name:     "stop bootout",
			run:      Stop,
			loaded:   true,
			failVerb: "bootout",
			wantCmd:  func(string) string { return "launchctl bootout gui/501/" + Label },
		},
		{
			name:     "restart bootout",
			run:      Restart,
			loaded:   true,
			failVerb: "bootout",
			wantCmd:  func(string) string { return "launchctl bootout gui/501/" + Label },
		},
		{
			name:     "restart bootstrap",
			run:      Restart,
			failVerb: "bootstrap",
			wantCmd:  func(plist string) string { return "launchctl bootstrap gui/501 " + plist },
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			opts, fake := darwinControlOptions(t)
			plist := writeInstalledUnit(t, opts)
			fake.loaded = tc.loaded
			if tc.loaded {
				fake.pid = 111
			}
			baseRun := opts.Run
			opts.Run = func(name string, args ...string) ([]byte, error) {
				if name == "launchctl" && len(args) > 0 && args[0] == tc.failVerb {
					return []byte("launchctl failed"), exitError(t, 1)
				}
				return baseRun(name, args...)
			}

			res, err := tc.run(opts)
			if err == nil {
				t.Fatalf("%s succeeded although %s failed", tc.name, tc.failVerb)
			}
			want := "cannot run " + tc.wantCmd(plist)
			for _, part := range []string{want, "launchctl failed"} {
				if !strings.Contains(err.Error(), part) {
					t.Errorf("%s error = %q, want it to contain %q", tc.name, err, part)
				}
			}
			if res.Domain != "gui/501" {
				t.Errorf("Domain = %q, want gui/501 on the failure path", res.Domain)
			}
		})
	}
}
