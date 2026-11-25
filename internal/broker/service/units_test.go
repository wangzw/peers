package service

import (
	"encoding/xml"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
	"testing"
)

const (
	goldenExe     = "/usr/local/bin/peers"
	goldenLogPath = "/Users/alice/Library/Logs/peers/broker.log"
)

// TestUnitsMatchGolden checks that each rendered unit matches its golden file
// byte for byte.
func TestUnitsMatchGolden(t *testing.T) {
	cases := []struct {
		name   string
		golden string
		render func() (string, error)
	}{
		{"systemd-user", "systemd-user.golden", func() (string, error) {
			return SystemdUserUnit(goldenExe)
		}},
		{"systemd-system", "systemd-system.golden", func() (string, error) {
			return SystemdSystemUnit(goldenExe)
		}},
		{"launch-agent", "launch-agent.golden", func() (string, error) {
			return LaunchAgentPlist(goldenExe, goldenLogPath)
		}},
		{"launch-daemon", "launch-daemon.golden", func() (string, error) {
			return LaunchDaemonPlist(goldenExe)
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got, err := tc.render()
			if err != nil {
				t.Fatalf("render: %v", err)
			}
			want, err := os.ReadFile(filepath.Join("testdata", tc.golden))
			if err != nil {
				t.Fatal(err)
			}
			assertEqualText(t, got, string(want))
		})
	}
}

// TestSystemdSystemUnitResourceLimits checks that only the system unit carries
// the host-wide task and descriptor caps.
func TestSystemdSystemUnitResourceLimits(t *testing.T) {
	system, err := SystemdSystemUnit(goldenExe)
	if err != nil {
		t.Fatalf("SystemdSystemUnit: %v", err)
	}
	for _, want := range []string{"TasksMax=8192", "LimitNOFILE=65536"} {
		if !strings.Contains(system, want) {
			t.Errorf("system unit does not contain %q:\n%s", want, system)
		}
	}

	user, err := SystemdUserUnit(goldenExe)
	if err != nil {
		t.Fatalf("SystemdUserUnit: %v", err)
	}
	daemon, err := LaunchDaemonPlist(goldenExe)
	if err != nil {
		t.Fatalf("LaunchDaemonPlist: %v", err)
	}
	for name, out := range map[string]string{"user unit": user, "launch daemon": daemon} {
		for _, unwanted := range []string{"TasksMax", "LimitNOFILE"} {
			if strings.Contains(out, unwanted) {
				t.Errorf("%s contains %q but must not:\n%s", name, unwanted, out)
			}
		}
	}
}

// TestSystemdExecStartEscaping checks that ExecStart doubles systemd specifiers
// and shell variables and quotes a path that needs it.
func TestSystemdExecStartEscaping(t *testing.T) {
	cases := []struct {
		exe  string
		want string
	}{
		{"/opt/my apps/agent%peer", `ExecStart="/opt/my apps/agent%%peer" service run --user --allow-degraded`},
		{"/opt/a$b/peers", `ExecStart=/opt/a$$b/peers service run --user --allow-degraded`},
	}
	for _, tc := range cases {
		out, err := SystemdUserUnit(tc.exe)
		if err != nil {
			t.Fatalf("SystemdUserUnit(%q): %v", tc.exe, err)
		}
		if got := execStartLine(t, out); got != tc.want {
			t.Errorf("ExecStart for %q:\nwant: %q\ngot:  %q", tc.exe, tc.want, got)
		}
	}
}

// TestPlistEscaping checks that the plist escapes XML metacharacters and stays
// well-formed.
func TestPlistEscaping(t *testing.T) {
	out, err := LaunchAgentPlist("/Users/a&b/bin/peers", "/tmp/peers.log")
	if err != nil {
		t.Fatalf("LaunchAgentPlist: %v", err)
	}
	const want = "<string>/Users/a&amp;b/bin/peers</string>"
	if !strings.Contains(out, want) {
		t.Errorf("plist does not contain %q:\n%s", want, out)
	}
	if strings.Contains(out, "/Users/a&b/bin/peers") {
		t.Errorf("plist contains the unescaped executable path:\n%s", out)
	}

	dec := xml.NewDecoder(strings.NewReader(out))
	for {
		_, err := dec.Token()
		if err == io.EOF {
			break
		}
		if err != nil {
			t.Fatalf("plist is not well-formed XML: %v", err)
		}
	}
}

// TestUnitsRejectRelativeOrControlChars checks that every renderer rejects a
// relative path and a control character.
func TestUnitsRejectRelativeOrControlChars(t *testing.T) {
	bad := []string{"peers", "/usr/bin/a\nb"}
	for _, exe := range bad {
		if _, err := SystemdUserUnit(exe); err == nil {
			t.Errorf("SystemdUserUnit(%q) accepted an invalid path", exe)
		}
		if _, err := SystemdSystemUnit(exe); err == nil {
			t.Errorf("SystemdSystemUnit(%q) accepted an invalid path", exe)
		}
		if _, err := LaunchAgentPlist(exe, "/tmp/peers.log"); err == nil {
			t.Errorf("LaunchAgentPlist(%q) accepted an invalid path", exe)
		}
		if _, err := LaunchDaemonPlist(exe); err == nil {
			t.Errorf("LaunchDaemonPlist(%q) accepted an invalid path", exe)
		}
	}
}

// TestPlistLint checks that plutil accepts the generated plists on macOS.
func TestPlistLint(t *testing.T) {
	if runtime.GOOS != "darwin" {
		t.Skip("plutil is only available on macOS")
	}
	plutil, err := exec.LookPath("plutil")
	if err != nil {
		t.Fatalf("plutil not found on macOS: %v", err)
	}

	logPath := filepath.Join(t.TempDir(), "broker.log")
	agent, err := LaunchAgentPlist(goldenExe, logPath)
	if err != nil {
		t.Fatalf("LaunchAgentPlist: %v", err)
	}
	daemon, err := LaunchDaemonPlist(goldenExe)
	if err != nil {
		t.Fatalf("LaunchDaemonPlist: %v", err)
	}

	dir := t.TempDir()
	for name, content := range map[string]string{"agent.plist": agent, "daemon.plist": daemon} {
		path := filepath.Join(dir, name)
		if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
			t.Fatal(err)
		}
		if out, err := exec.Command(plutil, "-lint", path).CombinedOutput(); err != nil {
			t.Errorf("plutil -lint %s: %v\n%s", name, err, out)
		}
	}
}

func execStartLine(t *testing.T, unit string) string {
	t.Helper()
	for _, line := range strings.Split(unit, "\n") {
		if strings.HasPrefix(line, "ExecStart=") {
			return line
		}
	}
	t.Fatalf("unit has no ExecStart line:\n%s", unit)
	return ""
}

func assertEqualText(t *testing.T, got, want string) {
	t.Helper()
	if got == want {
		return
	}
	wantLines := strings.Split(want, "\n")
	gotLines := strings.Split(got, "\n")
	for i := 0; i < len(wantLines) || i < len(gotLines); i++ {
		var w, g string
		if i < len(wantLines) {
			w = wantLines[i]
		}
		if i < len(gotLines) {
			g = gotLines[i]
		}
		if w != g {
			t.Fatalf("output does not match golden at line %d:\nwant: %q\ngot:  %q", i+1, w, g)
		}
	}
	t.Fatalf("output does not match golden (want %d bytes, got %d)", len(want), len(got))
}

// TestLaunchAgentPlistRejectsAnInvalidLogPath checks that the log path is
// validated like the executable and that the error names it.
func TestLaunchAgentPlistRejectsAnInvalidLogPath(t *testing.T) {
	for _, logPath := range []string{"relative.log", "/tmp/peers\n.log"} {
		out, err := LaunchAgentPlist(goldenExe, logPath)
		if err == nil {
			t.Fatalf("LaunchAgentPlist accepted the log path %q", logPath)
		}
		if out != "" {
			t.Errorf("LaunchAgentPlist returned a plist for the rejected log path %q", logPath)
		}
		if !strings.Contains(err.Error(), "log path") {
			t.Errorf("LaunchAgentPlist(%q) error = %q, want it to name the log path", logPath, err)
		}
	}
}
