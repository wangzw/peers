package harness

import (
	"strings"
	"syscall"
	"testing"
	"time"
)

// TestRedactSecretsRemovesPairingKeysAndSecretAssignments checks that redaction removes pairing keys and secret assignments and keeps ordinary lines.
func TestRedactSecretsRemovesPairingKeysAndSecretAssignments(t *testing.T) {
	text := []byte("pairing key 0000-0000-0000-0000\nPEERS_PAIRING_KEY=abc123\nSESSION_TOKEN=xyz\nan ordinary line\n")
	got := string(redactSecrets(text))
	if strings.Contains(got, "0000-0000-0000-0000") {
		t.Errorf("the pairing key survived:\n%s", got)
	}
	if strings.Contains(got, "abc123") || strings.Contains(got, "xyz") {
		t.Errorf("a secret assignment survived:\n%s", got)
	}
	if !strings.Contains(got, "an ordinary line") {
		t.Errorf("an ordinary line was removed:\n%s", got)
	}
}

// TestLocalBrokerStackIsAGoroutineDump checks that a local node's Broker stack dump carries the goroutine dump.
func TestLocalBrokerStackIsAGoroutineDump(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "stack"})
	addr := host.FreeAddr(t)
	start := host.StartBroker(t, BrokerSpec{Args: []string{"--listen", addr}})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}

	local := host.impl.(*localNode)
	stack := local.brokerStack(t)
	if !strings.Contains(stack, "SIGQUIT: quit") || !strings.Contains(stack, "goroutine ") {
		t.Fatalf("brokerStack returned no goroutine dump:\n%s", tail(stack, 20))
	}
}

// TestBrokerStackRefusesReapedBroker checks that brokerStack returns a note instead of signalling a Broker that has already been waited for.
func TestBrokerStackRefusesReapedBroker(t *testing.T) {
	host := NewLocalHost(t, LocalOptions{Name: "reaped"})
	addr := host.FreeAddr(t)
	start := host.StartBroker(t, BrokerSpec{Args: []string{"--listen", addr}})
	if !start.Running {
		t.Fatalf("the Broker did not start: exit code %d: %s", start.ExitCode, strings.TrimSpace(start.StderrLast))
	}
	local := host.impl.(*localNode)
	local.mu.Lock()
	broker := local.broker
	local.mu.Unlock()
	if broker == nil {
		t.Fatal("the started Broker has no process handle")
	}
	if err := broker.SendSignal(syscall.SIGTERM); err != nil {
		t.Fatalf("stopping the Broker: %v", err)
	}
	if _, err := broker.Wait(5 * time.Second); err != nil {
		t.Fatalf("the Broker did not exit: %v", err)
	}
	before := broker.SignalsSent()

	stack := local.brokerStack(t)
	if !strings.Contains(stack, "already been reaped") || !strings.Contains(stack, "no signal was sent") {
		t.Fatalf("brokerStack did not refuse a reaped Broker:\n%s", stack)
	}
	if got := broker.SignalsSent(); got != before {
		t.Fatalf("brokerStack sent %d signal(s) to a reaped Broker", got-before)
	}
}
