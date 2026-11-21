package config

import (
	"net"
	"testing"
)

// probeRecord is one listener ProbeListen opened in a test. It records the
// address the probe requested and whether the probe closed the listener.
type probeRecord struct {
	net.Listener
	requested string
	closed    bool
}

func (r *probeRecord) Close() error {
	r.closed = true
	return r.Listener.Close()
}

// recordProbeListeners replaces the listener factory ProbeListen uses with one
// that binds an ephemeral loopback port and records every listener it returns.
// A test can then assert the probe closed each one.
//
// Nothing here re-binds a released port: the probe asks for a concrete address
// (broker.listen rejects port zero), but the factory binds port zero itself and
// records the requested address instead of binding it. Choosing the port first
// and releasing it for the probe would leave the window this test exists to
// avoid, in which another process takes the port and the probe fails for a
// reason unrelated to leaving a listener behind.
func recordProbeListeners() (*[]*probeRecord, func()) {
	prev := probeListenNet
	var records []*probeRecord
	probeListenNet = func(network, requested string) (net.Listener, error) {
		ln, err := net.Listen(network, "127.0.0.1:0")
		if err != nil {
			return nil, err
		}
		rec := &probeRecord{Listener: ln, requested: requested}
		records = append(records, rec)
		return rec, nil
	}
	return &records, func() { probeListenNet = prev }
}

// C4: the broker.listen validator only probes. ProbeListen must close the
// listener it opened, so the port stays free for the real listener.
func TestProbeListenLeavesNoListener(t *testing.T) {
	const addr = "127.0.0.1:17445"
	records, restore := recordProbeListeners()
	err := ProbeListen(addr)
	restore()
	if err != nil {
		t.Fatalf("ProbeListen(%q) error = %v", addr, err)
	}
	if got := len(*records); got != 1 {
		t.Fatalf("ProbeListen opened %d listeners, want 1", got)
	}
	rec := (*records)[0]
	if rec.requested != addr {
		t.Fatalf("ProbeListen bound %q, want the requested %q", rec.requested, addr)
	}
	if !rec.closed {
		t.Fatalf("ProbeListen left its listener on %s open", rec.Addr())
	}

	// An occupied address is rejected without disturbing the listener that
	// occupies it.
	occupied, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = occupied.Close() }()
	if err := ProbeListen(occupied.Addr().String()); err == nil {
		t.Fatalf("ProbeListen(%q) accepted an occupied address", occupied.Addr())
	}

	// An empty broker.listen means "do not listen"; anything that broker.listen
	// rejects is rejected here too.
	if err := ProbeListen(""); err != nil {
		t.Fatalf("ProbeListen(\"\") error = %v, want nil for a disabled listener", err)
	}
	if err := ProbeListen("no-port"); err == nil {
		t.Fatal("ProbeListen accepted an address that broker.listen rejects")
	}
}

// REQ-broker-config-034: the broker.listen validator is a probe, so a
// successful Set leaves the port free. Running Set twice probes twice, and the
// file write that follows the probe finds no listener of ours holding the port.
func TestREQBrokerConfig034_ListenValidatorLeavesPortFree(t *testing.T) {
	c, _ := newValueCore(t, "broker:\n  listen: \":17443\"\n", "")
	installCoreForTest(t, c)

	const addr = "127.0.0.1:17444"
	Validate("broker.listen", func(old, new Value) error { return ProbeListen(new.String) })

	records, restore := recordProbeListeners()
	defer restore()
	for i := 0; i < 2; i++ {
		if _, err := Set(Actor{}, "broker.listen", stringValue(addr)); err != nil {
			t.Fatalf("Set(broker.listen) %d error = %v", i+1, err)
		}
	}
	if got := len(*records); got != 2 {
		t.Fatalf("the broker.listen validator probed %d times, want 2", got)
	}
	for i, rec := range *records {
		if rec.requested != addr {
			t.Fatalf("probe %d bound %q, want %q", i+1, rec.requested, addr)
		}
		if !rec.closed {
			t.Fatalf("probe %d left its listener on %s open", i+1, rec.Addr())
		}
	}
}
