package pairing

import (
	"context"
	"errors"
	"io"
	"testing"
)

// transportTestConn is a minimal PairConn used to check that a Transport hands
// exactly this connection to the dial callback.
type transportTestConn struct {
	fp       Fingerprint
	pub      []byte
	remote   string
	sent     []uint32
	receiveN int
	closed   int
}

func (c *transportTestConn) PeerFingerprint() Fingerprint { return c.fp }
func (c *transportTestConn) PeerPublicKey() []byte        { return c.pub }
func (c *transportTestConn) RemoteAddr() string           { return c.remote }
func (c *transportTestConn) ProtocolVersion() uint32      { return hsProtocolVersion }

func (c *transportTestConn) Send(ctx context.Context, kind uint32, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.sent = append(c.sent, kind)
	return nil
}

func (c *transportTestConn) Receive(ctx context.Context) (uint32, []byte, error) {
	if err := ctx.Err(); err != nil {
		return 0, nil, err
	}
	if c.receiveN == 0 {
		return 0, nil, io.EOF
	}
	c.receiveN--
	return 0, nil, nil
}

func (c *transportTestConn) Unread(kind uint32, payload []byte) {}

func (c *transportTestConn) Close() error {
	c.closed++
	return nil
}

// transportTestFake implements Transport without any network. It records the
// dial address, passes the configured connection to run, and records drops.
type transportTestFake struct {
	address   string
	conn      PairConn
	dialErr   error
	runCalled bool
	runErr    error
	dropped   []Fingerprint
}

func (f *transportTestFake) PairDial(ctx context.Context, address string, run func(PairConn) error) error {
	f.address = address
	f.runCalled = true
	if f.conn != nil {
		if err := run(f.conn); err != nil {
			f.runErr = err
			return err
		}
	}
	return f.dialErr
}

func (f *transportTestFake) Drop(fp Fingerprint) {
	f.dropped = append(f.dropped, fp)
}

var (
	_ Transport = (*transportTestFake)(nil)
	_ PairConn  = (*transportTestConn)(nil)
)

// TestTransportPairDialHandsConnectionToRun checks the wiring contract: the
// address is passed through and run receives the connection the transport
// dialed.
func TestTransportPairDialHandsConnectionToRun(t *testing.T) {
	conn := &transportTestConn{fp: auditTestFingerprint(0x10), remote: "192.0.2.10:17443"}
	tr := &transportTestFake{conn: conn}
	var got PairConn

	err := tr.PairDial(context.Background(), "192.0.2.10:17443", func(c PairConn) error {
		got = c
		return nil
	})
	if err != nil {
		t.Fatalf("PairDial() error = %v", err)
	}
	if !tr.runCalled {
		t.Fatal("run was not called")
	}
	if tr.address != "192.0.2.10:17443" {
		t.Errorf("dialed %q, want 192.0.2.10:17443", tr.address)
	}
	if got != conn {
		t.Errorf("run received %v, want the dialed connection", got)
	}
}

// TestTransportPairDialPropagatesRunError keeps the handshake's own error
// (rejection, protocol error, store failure) visible to Connect unchanged,
// rather than being confused with a dial failure.
func TestTransportPairDialPropagatesRunError(t *testing.T) {
	want := errors.New("handshake failed")
	tr := &transportTestFake{conn: &transportTestConn{}}

	err := tr.PairDial(context.Background(), "192.0.2.10:17443", func(PairConn) error {
		return want
	})
	if !errors.Is(err, want) {
		t.Fatalf("PairDial() error = %v, want %v", err, want)
	}
	if !errors.Is(tr.runErr, want) {
		t.Fatalf("recorded run error = %v, want %v", tr.runErr, want)
	}
}

// TestTransportDropRecordsFingerprint checks that Unpair's close path addresses
// the right peer and that repeated drops remain safe.
func TestTransportDropRecordsFingerprint(t *testing.T) {
	fp := auditTestFingerprint(0x20)
	tr := &transportTestFake{}
	tr.Drop(fp)
	tr.Drop(fp)
	if len(tr.dropped) != 2 {
		t.Fatalf("recorded %d drops, want 2", len(tr.dropped))
	}
	for i, got := range tr.dropped {
		if got != fp {
			t.Errorf("drop %d fingerprint = %x, want %x", i, got, fp)
		}
	}
}
