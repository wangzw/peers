package broker

import (
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// TestListenProbeCheckRejectsBusyPort covers REQ-broker-config-034: an address
// that cannot be bound is rejected, and the successful probe of a busy port
// must not take the port away from its current holder.
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

	if err := ln.(*net.TCPListener).SetDeadline(time.Now().Add(3 * time.Second)); err != nil {
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
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		t.Fatalf("dialing the occupied address after the check: %v", err)
	}
	_ = conn.Close()
	if err := <-accepted; err != nil {
		t.Fatalf("the occupied listener no longer accepts: %v", err)
	}
}

// TestListenValidatorSameValueSkipsProbe covers the case where the running
// link holds the listener: setting broker.listen to the value it already has
// must succeed without probing, because a probe of the occupied address would
// fail. The probe fails the test if it is called at all.
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

// TestListenProbeCheckProbesChangedValue checks that a changed broker.listen
// reaches the probe with the new value and that a successful probe accepts the
// change.
//
// Whether config.ProbeListen closes the listener it opens is checked by
// TestProbeListenLeavesNoListener in package config, the only package that can
// observe the listener the probe opens. Re-binding a released port here would
// depend on that port staying free, and another process can take it between the
// release and the probe or between the probe and the re-bind.
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
