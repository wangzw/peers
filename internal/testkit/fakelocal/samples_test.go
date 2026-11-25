package fakelocal

import (
	"bytes"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/mcpserver/termcmd"
)

// TestSamplesMatchTermcmdFormat checks that the sample texts match what the
// terminal formatter produces for the sample data.
func TestSamplesMatchTermcmdFormat(t *testing.T) {
	t.Parallel()

	var status bytes.Buffer
	if err := termcmd.WriteStatus(&status, SampleStatus()); err != nil {
		t.Fatalf("WriteStatus: %v", err)
	}
	if got := status.String(); got != SampleStatusText() {
		t.Fatalf("WriteStatus output does not match SampleStatusText:\ngot  %q\nwant %q", got, SampleStatusText())
	}

	events := SampleAuditEvents()
	if len(events) != len(sampleAuditLines) {
		t.Fatalf("SampleAuditEvents has %d events, but sampleAuditLines has %d lines", len(events), len(sampleAuditLines))
	}
	var audit strings.Builder
	for i, ev := range events {
		var line bytes.Buffer
		if err := termcmd.WriteAuditEvent(&line, ev); err != nil {
			t.Fatalf("WriteAuditEvent(%d): %v", i, err)
		}
		if got, want := line.String(), sampleAuditLines[i]; got != want {
			t.Fatalf("audit event %d line:\ngot  %q\nwant %q", i, got, want)
		}
		audit.WriteString(line.String())
	}
	if got, want := SampleAuditText(0), audit.String(); got != want {
		t.Fatalf("SampleAuditText(0):\ngot  %q\nwant %q", got, want)
	}

	if got, want := SampleAuditText(2), strings.Join(sampleAuditLines[len(sampleAuditLines)-2:], ""); got != want {
		t.Fatalf("SampleAuditText(2):\ngot  %q\nwant %q", got, want)
	}
	if got, want := SampleAuditText(uint32(len(sampleAuditLines)+1)), audit.String(); got != want {
		t.Fatalf("SampleAuditText beyond the count:\ngot  %q\nwant %q", got, want)
	}
}
