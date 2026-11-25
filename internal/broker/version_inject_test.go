package broker

import (
	"context"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
)

type recordingAssembler struct {
	seen chan uint32
}

var _ Assembler = (*recordingAssembler)(nil)

func (a *recordingAssembler) Name() string { return "rec" }

func (a *recordingAssembler) Assemble(_ context.Context, _ ConfigSource, _ *Deps, asm *Assembly) ([]Component, error) {
	a.seen <- asm.LinkProtocolVersion
	return nil, nil
}

// TestRunPassesProtocolVersions checks that Run passes the local protocol
// version to Hello and the link protocol version to the assemblers.
func TestRunPassesProtocolVersions(t *testing.T) {
	rec := &recordingAssembler{seen: make(chan uint32, 1)}
	rt := newRunTest(t, func(rt *runTest) {
		rt.opts.LocalProtocolVersion = 3
		rt.opts.LinkProtocolVersion = 4
		rt.opts.Assemblers = []Assembler{rec}
	})
	rt.start()

	if got := rt.readyLine(); got != "ok\n" {
		t.Fatalf("readiness line = %q, want %q", got, "ok\n")
	}
	select {
	case got := <-rec.seen:
		if got != 4 {
			t.Fatalf("assembler saw LinkProtocolVersion %d, want 4", got)
		}
	case <-time.After(eventTimeout):
		t.Fatal("the assembler did not run")
	}

	resp, err := helloAt(t, rt.socketPath(), 3)
	if err != nil {
		t.Fatalf("Hello{3}: %v", err)
	}
	if got := resp.GetProtocolVersion(); got != 3 {
		t.Fatalf("Hello{3} answered version %d, want 3", got)
	}

	_, err = helloAt(t, rt.socketPath(), localpb.ProtocolVersion)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)
}

// TestHelloZeroVersionRejected checks that an unset protocol version is a
// mismatch, not a wildcard.
func TestHelloZeroVersionRejected(t *testing.T) {
	tb := newTestBroker(t)

	_, err := callTestHello(t, tb.client, 0)
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH)

	resp, err := callTestHello(t, tb.newClient(t), localpb.ProtocolVersion)
	if err != nil {
		t.Fatalf("Hello{%d}: %v", localpb.ProtocolVersion, err)
	}
	if got := resp.GetProtocolVersion(); got != localpb.ProtocolVersion {
		t.Fatalf("Hello{%d} answered version %d", localpb.ProtocolVersion, got)
	}
}
