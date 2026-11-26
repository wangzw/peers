// This file carries no e2e build tag so that the pure evidence formatter and
// its host-side test compile without one.

package syscontainer

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
)

// evidenceTailLines bounds each captured stream; the last lines before systemd
// gave up are the interesting ones, and an unbounded dump would bury the rest
// of the test log.
const evidenceTailLines = 40

type containerState struct {
	Status     string `json:"Status"`
	Running    bool   `json:"Running"`
	OOMKilled  bool   `json:"OOMKilled"`
	ExitCode   int    `json:"ExitCode"`
	Error      string `json:"Error"`
	StartedAt  string `json:"StartedAt"`
	FinishedAt string `json:"FinishedAt"`
}

type bootEvidence struct {
	State          string
	Logs           string
	SystemState    string
	Jobs           string
	Activating     string
	ProcessState   string
	Processes      string
	Journal        string
	JournalSystemd string
	HostMemory     string
	HostPids       string
	Containers     string
}

// parseContainerState decodes docker inspect's State JSON; ok is false when the
// input is not a JSON object, so the caller can show the daemon's error text
// instead of losing it to a parse failure.
func parseContainerState(raw string) (state containerState, ok bool) {
	raw = strings.TrimSpace(raw)
	if !strings.HasPrefix(raw, "{") {
		return containerState{}, false
	}
	if err := json.Unmarshal([]byte(raw), &state); err != nil {
		return containerState{}, false
	}
	return state, true
}

// formatBootEvidence renders the collected evidence for a test log, keeping
// docker's own State field names.
func formatBootEvidence(e bootEvidence) string {
	var b strings.Builder
	b.WriteString("systemd container boot evidence:")

	if state, ok := parseContainerState(e.State); ok {
		fmt.Fprintf(&b, "\n  State: Status=%s Running=%t OOMKilled=%t ExitCode=%d Error=%q StartedAt=%s FinishedAt=%s",
			state.Status, state.Running, state.OOMKilled, state.ExitCode, state.Error, state.StartedAt, state.FinishedAt)
	} else if text := strings.TrimSpace(e.State); text != "" {
		fmt.Fprintf(&b, "\n  State (raw): %s", text)
	} else {
		b.WriteString("\n  State (raw): (no output)")
	}

	writeEvidenceSection(&b, "docker logs", e.Logs)
	writeEvidenceSection(&b, "docker top", e.Processes)
	writeEvidenceSection(&b, "systemctl is-system-running", e.SystemState)
	writeEvidenceSection(&b, "systemctl list-jobs", e.Jobs)
	writeEvidenceSection(&b, "activating units", e.Activating)
	writeEvidenceSection(&b, "process 1 wchan/status", e.ProcessState)
	writeEvidenceSection(&b, "journalctl -b", e.Journal)
	writeEvidenceSection(&b, "journalctl -b _PID=1", e.JournalSystemd)
	writeEvidenceSection(&b, "host memory", e.HostMemory)
	writeEvidenceSection(&b, "host pids", e.HostPids)
	writeEvidenceSection(&b, "running containers", e.Containers)

	return b.String()
}

func writeEvidenceSection(b *strings.Builder, name, text string) {
	fmt.Fprintf(b, "\n  %s:", name)
	tail := tailLines(text, evidenceTailLines)
	if tail == "" {
		b.WriteString(" (no output)")
		return
	}
	for _, line := range strings.Split(tail, "\n") {
		b.WriteString("\n    ")
		b.WriteString(line)
	}
}

// collectBootEvidence runs the diagnostic commands through run and formats
// their output; the function parameters let a test inject fake command results.
func collectBootEvidence(project, composeFile, service string, run func(args ...string) string, hostMemory, hostPids func() string) string {
	var e bootEvidence

	id := strings.TrimSpace(run("compose", "-p", project, "-f", composeFile, "ps", "-aq", service))
	if id == "" {
		e.State = "docker compose ps -aq " + service + " returned no container"
		e.Logs = run("compose", "-p", project, "-f", composeFile, "ps", "-a")
	} else {
		e.State = run("inspect", "--format", "{{json .State}}", id)
		e.Logs = run("logs", "--tail", strconv.Itoa(evidenceTailLines), id)
		e.Processes = run("top", id)
		// Without --wait is-system-running reports the current state instead
		// of blocking, so a container that is still booting shows it.
		e.SystemState = run("exec", id, "systemctl", "is-system-running")
		e.Jobs = run("exec", id, "systemctl", "list-jobs", "--no-pager")
		e.Activating = run("exec", id, "systemctl", "list-units", "--all",
			"--state=activating,activating-done", "--no-pager")
		e.ProcessState = run("exec", id, "sh", "-c",
			"cat /proc/1/wchan; echo; grep -E '^(State|Threads)' /proc/1/status")
		e.Journal = run("exec", id, "journalctl", "-b", "--no-pager", "-n", "200")
		e.JournalSystemd = run("exec", id, "journalctl", "-b", "--no-pager", "-n", "200", "_PID=1")
	}

	e.HostMemory = hostMemory()
	e.HostPids = hostPids()
	running := strings.Fields(run("ps", "-q"))
	e.Containers = fmt.Sprintf("%d running (docker ps -q)", len(running))

	return formatBootEvidence(e)
}

func headLines(s string, n int) string {
	s = strings.TrimRight(s, "\n")
	if s == "" || n <= 0 {
		return ""
	}
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[:n]
	}
	return strings.Join(lines, "\n")
}

func tailLines(s string, n int) string {
	s = strings.TrimRight(s, "\n")
	if s == "" || n <= 0 {
		return ""
	}
	lines := strings.Split(s, "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}
