package main

import (
	"testing"

	"github.com/wangzw/peers/internal/broker"
)

// TestAgentIDMatchesBroker checks that rsprobe derives the same Agent identifier
// as the Broker for the same paths.
func TestAgentIDMatchesBroker(t *testing.T) {
	paths := []string{
		"/tmp/rs/a.sock",
		"/tmp/rs/b.sock",
		"/tmp/cc//s.sock",
		"/tmp/cc/./s.sock",
	}
	for _, path := range paths {
		id, _, err := broker.AgentIDFor(path)
		if err != nil {
			t.Fatalf("broker.AgentIDFor(%q): %v", path, err)
		}
		got, err := agentID(path)
		if err != nil {
			t.Fatalf("agentID(%q): %v", path, err)
		}
		if want := string(id); got != want {
			t.Errorf("agentID(%q) = %q, want broker's %q", path, got, want)
		}
	}
}
