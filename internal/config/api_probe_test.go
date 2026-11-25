package config

import (
	"net"
	"testing"
)

type probeRecord struct {
	net.Listener
	requested string
	closed    bool
}

func (r *probeRecord) Close() error {
	r.closed = true
	return r.Listener.Close()
}

// recordProbeListeners replaces ProbeListen's listener factory with one that
// binds an ephemeral loopback port and records each listener, so a test can
// assert it was closed without ever re-binding a released port.
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

// TestProbeListenLeavesNoListener checks that ProbeListen closes its listener,
// rejects an occupied address and treats an empty address as not listening.
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

// TestREQBrokerConfig034_ListenValidatorLeavesPortFree checks that a successful
// Set through the broker.listen probe leaves the port free.
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
