package mcpserver

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/testkit/fakelocal"
)

// TestLoopOnceStopsWhenContextEndsDuringSelection stops without dialing or
// recording a status when the context ends during selection.
func TestLoopOnceStopsWhenContextEndsDuringSelection(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	st := &statusStore{}
	dialed := false
	d := loopDeps{
		selectTarget: func(context.Context) (target, error) {
			return target{}, errors.New("selection failed")
		},
		dial: func(string) (*grpc.ClientConn, localpb.LocalClient, error) {
			dialed = true
			return nil, nil, errors.New("unexpected dial")
		},
		st: st,
	}
	if stop := loopOnce(ctx, d, &loopConnBackoff{}); !stop {
		t.Fatal("loopOnce = false, want it to stop with the context")
	}
	if dialed {
		t.Fatal("loopOnce dialed after the context ended")
	}
	if got := st.Get(); got.Phase != PhaseStarting || got.Detail != "" {
		t.Fatalf("status = %+v, want the starting state unchanged", got)
	}
}

// TestLoopOnceStopsWhenContextEndsBeforeDial stops without dialing or recording
// a status when the context ends right after selection.
func TestLoopOnceStopsWhenContextEndsBeforeDial(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	st := &statusStore{}
	dialed := false
	d := loopDeps{
		selectTarget: func(context.Context) (target, error) {
			cancel()
			return target{Path: "p", Kind: kindUser}, nil
		},
		dial: func(string) (*grpc.ClientConn, localpb.LocalClient, error) {
			dialed = true
			return nil, nil, errors.New("unexpected dial")
		},
		st: st,
	}
	if stop := loopOnce(ctx, d, &loopConnBackoff{}); !stop {
		t.Fatal("loopOnce = false, want it to stop with the context")
	}
	if dialed {
		t.Fatal("loopOnce dialed after the context ended")
	}
	if got := st.Get(); got.Phase != PhaseStarting || got.Detail != "" {
		t.Fatalf("status = %+v, want the starting state unchanged", got)
	}
}

// TestLoopOnceStopsWhenDialFailsAndContextEnds records the dial failure's phase
// and detail and stops when the context ends.
func TestLoopOnceStopsWhenDialFailsAndContextEnds(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	st := &statusStore{}
	d := loopDeps{
		selectTarget: func(context.Context) (target, error) {
			return target{Path: "p", Kind: kindUser}, nil
		},
		dial: func(string) (*grpc.ClientConn, localpb.LocalClient, error) {
			cancel()
			return nil, nil, errors.New("dial failed")
		},
		probe: func(context.Context, string) probeInfo {
			return probeInfo{Outcome: probeOK, PeerUID: uint32(os.Geteuid()), PeerKnown: true}
		},
		st: st,
	}
	if stop := loopOnce(ctx, d, &loopConnBackoff{}); !stop {
		t.Fatal("loopOnce = false, want it to stop with the context")
	}
	got := st.Get()
	if got.Phase != PhaseUnavailable {
		t.Fatalf("status phase = %v, want PhaseUnavailable", got.Phase)
	}
	if want := unavailableText("dial failed"); got.Detail != want {
		t.Fatalf("status detail = %q, want %q", got.Detail, want)
	}
}

// TestHandshakeRejectionCarriesDetail passes the Broker's own sentence and code
// through.
func TestHandshakeRejectionCarriesDetail(t *testing.T) {
	const message = "another connection is being set up"
	err := fakelocal.ErrorStatus(codes.FailedPrecondition, &localpb.ErrorDetail{
		Code:    localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED,
		Message: message,
	})
	got := handshakeRejection(err)
	if got.detail != message || got.code != localpb.ErrorCode_ERROR_CODE_PERMISSION_DENIED {
		t.Fatalf("handshakeRejection = %+v, want the Broker's detail and code", got)
	}
}

// TestLoopProbeInfoUsesInspectSocket reports a missing socket for a user target
// when no probe seam is set.
func TestLoopProbeInfoUsesInspectSocket(t *testing.T) {
	path := filepath.Join(shortTempDir(t), "missing.sock")
	info, probed := loopProbeInfo(context.Background(), loopDeps{}, target{Path: path, Kind: kindUser})
	if !probed {
		t.Fatal("loopProbeInfo reported no probe for a user target")
	}
	if info.Outcome != probeMissing {
		t.Fatalf("loopProbeInfo outcome = %v, want probeMissing through inspectSocket", info.Outcome)
	}
}

// TestLoopFailureStatusForSystemTarget reports a running but unresponsive system
// Broker rather than a missing one.
func TestLoopFailureStatusForSystemTarget(t *testing.T) {
	phase, detail := loopFailureStatusForTarget(target{Path: "p", Kind: kindSystem}, "boom")
	if phase != PhaseUnavailable {
		t.Fatalf("phase = %v, want PhaseUnavailable", phase)
	}
	if !strings.Contains(detail, "running") {
		t.Fatalf("detail = %q, want the unresponsive-Broker sentence", detail)
	}
}

// TestLoopWaitDefaultsToTimeAfter checks that a nil after function waits the
// full duration on the real timer before reporting completion.
func TestLoopWaitDefaultsToTimeAfter(t *testing.T) {
	const d = 20 * time.Millisecond
	start := time.Now()
	if ok := loopWait(context.Background(), nil, d); !ok {
		t.Fatal("loopWait = false, want true after the wait")
	}
	if elapsed := time.Since(start); elapsed < d {
		t.Fatalf("loopWait returned after %v, want at least the %v it was asked to wait", elapsed, d)
	}
}
