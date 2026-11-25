package broker

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// TestListenProbeCheckRejectsBusyPort checks that a busy port is rejected and the
// listener that occupied it still accepts afterwards.
func TestListenProbeCheckRejectsBusyPort(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("occupying a port: %v", err)
	}
	defer func() { _ = ln.Close() }()
	addr := ln.Addr().String()

	check := listenProbeCheck(config.ProbeListen)
	err = check(config.Value{}, config.Value{Kind: config.KindString, String: addr})
	if err == nil {
		t.Fatalf("check(%q) = nil, want an error for a busy port", addr)
	}
	for _, want := range []string{"is unavailable", addr} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("error %q does not contain %q", err, want)
		}
	}

	if err := ln.(*net.TCPListener).SetDeadline(time.Now().Add(eventTimeout)); err != nil {
		t.Fatalf("setting the accept deadline: %v", err)
	}
	accepted := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err == nil {
			_ = conn.Close()
		}
		accepted <- err
	}()
	conn, err := net.DialTimeout("tcp", addr, eventTimeout)
	if err != nil {
		t.Fatalf("dialing the occupied address after the check: %v", err)
	}
	_ = conn.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("the occupied listener no longer accepts: %v", err)
	}
}

// TestListenValidatorSameValueSkipsProbe checks that an unchanged
// broker.listen value is accepted without probing.
func TestListenValidatorSameValueSkipsProbe(t *testing.T) {
	old := config.Value{Kind: config.KindString, String: "127.0.0.1:17443"}
	newValue := config.Value{Kind: config.KindString, String: "127.0.0.1:17443"}
	check := listenProbeCheck(func(addr string) error {
		t.Fatalf("probe(%q) called for an unchanged value", addr)
		return nil
	})
	if err := check(old, newValue); err != nil {
		t.Fatalf("check(unchanged value) = %v, want nil", err)
	}
}

// TestListenProbeCheckProbesChangedValue checks that a changed value reaches
// the probe with the new value and that a successful probe accepts the change.
func TestListenProbeCheckProbesChangedValue(t *testing.T) {
	const addr = "127.0.0.1:17444"
	calls := 0
	check := listenProbeCheck(func(got string) error {
		calls++
		if got != addr {
			t.Errorf("probe(%q), want %q", got, addr)
		}
		return nil
	})
	err := check(config.Value{Kind: config.KindString, String: "127.0.0.1:17443"},
		config.Value{Kind: config.KindString, String: addr})
	if err != nil {
		t.Fatalf("check(changed value) = %v, want nil", err)
	}
	if calls != 1 {
		t.Fatalf("probe called %d times, want 1", calls)
	}
}
