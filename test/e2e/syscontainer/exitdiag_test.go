package syscontainer

import (
	"slices"
	"strings"
	"testing"
	"time"
)

// TestExitDiagnosticOnlyForKilledRun checks the 137 filter and the exact events
// query, and that any other exit status runs no command and adds no text.
func TestExitDiagnosticOnlyForKilledRun(t *testing.T) {
	started := time.Date(2026, 11, 21, 10, 0, 0, 0, time.UTC)
	now := started.Add(30 * time.Second)
	const owner = "ape2e-123-456-pidns789"

	var commands []string
	run := func(args ...string) string {
		commands = append(commands, strings.Join(args, " "))
		return "2026-11-21T10:00:05Z container kill abc (image=peers-e2e, name=proj_plain_1)\n"
	}

	got := collectExitDiagnostic(137, run, owner, started, now)
	wantCommand := "events --filter label=peers.e2e.owner=ape2e-123-456-pidns789" +
		" --since 2026-11-21T10:00:00Z --until 2026-11-21T10:00:30Z"
	if !slices.Equal(commands, []string{wantCommand}) {
		t.Fatalf("collectExitDiagnostic ran %v, want [%s]", commands, wantCommand)
	}
	for _, want := range []string{"exit code 137", "container kill abc"} {
		if !strings.Contains(got, want) {
			t.Errorf("diagnostic does not contain %q:\n%s", want, got)
		}
	}

	commands = nil
	if got := collectExitDiagnostic(1, run, owner, started, now); got != "" {
		t.Errorf("collectExitDiagnostic(1) = %q, want no diagnostic", got)
	}
	if len(commands) != 0 {
		t.Errorf("collectExitDiagnostic ran %v for a non-137 exit, want no command", commands)
	}
}

// TestFormatExitDiagnosticWithoutEvents checks that an empty event stream still
// explains the exit instead of leaving a blank section.
func TestFormatExitDiagnosticWithoutEvents(t *testing.T) {
	got := formatExitDiagnostic(137, "  \n")
	if !strings.Contains(got, "(no events)") {
		t.Errorf("formatExitDiagnostic with no events = %q, want the (no events) marker", got)
	}
}
