package syscontainer

import (
	"fmt"
	"strings"
	"testing"
)

// TestFormatBootEvidence checks that the failure text carries the inspect State
// fields and the last lines of each captured stream.
func TestFormatBootEvidence(t *testing.T) {
	var logs strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&logs, "docker log line %d\n", i)
	}
	var journal strings.Builder
	for i := 0; i < 100; i++ {
		fmt.Fprintf(&journal, "journal line %d\n", i)
	}

	got := formatBootEvidence(bootEvidence{
		State:          `{"Status":"exited","Running":false,"OOMKilled":true,"ExitCode":137,"Error":"","StartedAt":"2026-11-19T01:00:00Z","FinishedAt":"2026-11-19T01:00:05Z"}`,
		Logs:           logs.String(),
		Processes:      "PID                 USER                TIME                COMMAND\n1 root 0:00 /sbin/init",
		SystemState:    "offline",
		Jobs:           "JOB UNIT TYPE STATE\n42 getty@tty1.service start running",
		Activating:     "dbus.service loaded activating start",
		ProcessState:   "ep_poll\nState:\tS (sleeping)\nThreads:\t1",
		Journal:        journal.String(),
		HostMemory:     "MemTotal:       527843948 kB\nMemFree:        415000000 kB\nMemAvailable:   490000000 kB",
		HostPids:       "/sys/fs/cgroup/pids.current: 812",
		Containers:     "9 running (docker ps -q)",
		JournalSystemd: "systemd[1]: Startup finished in 1s.",
	})

	for _, want := range []string{
		"Status=exited",
		"Running=false",
		"OOMKilled=true",
		"ExitCode=137",
		"docker log line 99",
		"journal line 99",
		"1 root 0:00 /sbin/init",
		"offline",
		"42 getty@tty1.service start running",
		"dbus.service loaded activating start",
		"ep_poll",
		"systemd[1]: Startup finished in 1s.",
		"MemAvailable:   490000000 kB",
		"/sys/fs/cgroup/pids.current: 812",
		"9 running (docker ps -q)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("formatBootEvidence output does not contain %q:\n%s", want, got)
		}
	}

	for _, unwanted := range []string{
		"docker log line 0\n",
		fmt.Sprintf("docker log line %d\n", 100-evidenceTailLines-1),
		"journal line 0\n",
	} {
		if strings.Contains(got, unwanted) {
			t.Errorf("formatBootEvidence output contains %q beyond the tail bound:\n%s", unwanted, got)
		}
	}
}

// TestCollectBootEvidence checks that the collection runs the diagnostic
// commands and folds every result into the failure text.
func TestCollectBootEvidence(t *testing.T) {
	const inspect = `{"Status":"exited","Running":false,"OOMKilled":false,"ExitCode":255,"Error":"","StartedAt":"2026-11-19T01:00:00Z","FinishedAt":"2026-11-19T01:00:01Z"}`
	var commands []string
	run := func(args ...string) string {
		joined := strings.Join(args, " ")
		commands = append(commands, joined)
		switch {
		case joined == "compose -p proj -f /compose.yaml ps -aq systemd":
			return "abc123"
		case strings.HasPrefix(joined, "inspect "):
			return inspect
		case strings.HasPrefix(joined, "logs "):
			return "Boot has failed."
		case strings.HasPrefix(joined, "top "):
			return "1 root /sbin/init"
		case strings.Contains(joined, "_PID=1"):
			return "systemd[1]: Finished systemd-update-utmp.service"
		case strings.Contains(joined, "journalctl"):
			return "journal tail"
		case strings.Contains(joined, "is-system-running"):
			return "initializing"
		case strings.Contains(joined, "list-jobs"):
			return "1 sysinit.target start waiting"
		case strings.Contains(joined, "list-units"):
			return "dbus.service loaded activating start"
		case strings.Contains(joined, "sh -c"):
			return "ep_poll\nState:\tS (sleeping)"
		case joined == "ps -q":
			return "c1\nc2"
		}
		return ""
	}
	got := collectBootEvidence("proj", "/compose.yaml", "systemd", run,
		func() string { return "MemAvailable: 1 kB" },
		func() string { return "/proc/sys/kernel/pid_max: 1" })

	wantCommands := []string{
		"compose -p proj -f /compose.yaml ps -aq systemd",
		"inspect --format {{json .State}} abc123",
		"logs --tail 40 abc123",
		"top abc123",
		"exec abc123 systemctl is-system-running",
		"exec abc123 systemctl list-jobs --no-pager",
		"exec abc123 systemctl list-units --all --state=activating,activating-done --no-pager",
		"exec abc123 sh -c cat /proc/1/wchan; echo; grep -E '^(State|Threads)' /proc/1/status",
		"exec abc123 journalctl -b --no-pager -n 200",
		"exec abc123 journalctl -b --no-pager -n 200 _PID=1",
		"ps -q",
	}
	for _, want := range wantCommands {
		found := false
		for _, command := range commands {
			if command == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("collectBootEvidence did not run %q; ran:\n%s", want, strings.Join(commands, "\n"))
		}
	}

	for _, want := range []string{
		"ExitCode=255",
		"Boot has failed.",
		"ep_poll",
		"initializing",
		"1 sysinit.target start waiting",
		"dbus.service loaded activating start",
		"journal tail",
		"systemd[1]: Finished systemd-update-utmp.service",
		"MemAvailable: 1 kB",
		"/proc/sys/kernel/pid_max: 1",
		"2 running (docker ps -q)",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("collectBootEvidence output does not contain %q:\n%s", want, got)
		}
	}
}

// TestFormatBootEvidenceRawState checks that a daemon error that is not JSON is
// preserved instead of being dropped by the parse.
func TestFormatBootEvidenceRawState(t *testing.T) {
	const raw = "Error response from daemon: No such container: deadbeef"
	got := formatBootEvidence(bootEvidence{State: raw})
	if !strings.Contains(got, raw) {
		t.Errorf("formatBootEvidence output does not carry the raw State text:\n%s", got)
	}
}

// TestHeadLines checks the head bound the host reads use.
func TestHeadLines(t *testing.T) {
	cases := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"shorter than bound", "a\nb", 3, "a\nb"},
		{"exactly the bound", "a\nb\nc", 3, "a\nb\nc"},
		{"longer than bound", "a\nb\nc\nd", 2, "a\nb"},
		{"trailing newline", "a\nb\n", 2, "a\nb"},
		{"empty", "", 2, ""},
		{"no lines requested", "a\n", 0, ""},
	}
	for _, tc := range cases {
		if got := headLines(tc.in, tc.n); got != tc.want {
			t.Errorf("%s: headLines(%q, %d) = %q, want %q", tc.name, tc.in, tc.n, got, tc.want)
		}
	}
}

// TestTailLines checks the bound used by every evidence stream.
func TestTailLines(t *testing.T) {
	cases := []struct {
		name string
		in   string
		n    int
		want string
	}{
		{"shorter than bound", "a\nb", 3, "a\nb"},
		{"exactly the bound", "a\nb\nc", 3, "a\nb\nc"},
		{"longer than bound", "a\nb\nc\nd", 2, "c\nd"},
		{"trailing newline", "a\nb\n", 2, "a\nb"},
		{"empty", "", 2, ""},
		{"no lines requested", "a\n", 0, ""},
	}
	for _, tc := range cases {
		if got := tailLines(tc.in, tc.n); got != tc.want {
			t.Errorf("%s: tailLines(%q, %d) = %q, want %q", tc.name, tc.in, tc.n, got, tc.want)
		}
	}
}
