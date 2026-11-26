package mcpserver

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/connectivity"

	"github.com/wangzw/peers/internal/broker/localpb"
)

type loopDeps struct {
	selectTarget func(ctx context.Context) (target, error)
	probe        func(ctx context.Context, path string) probeInfo
	dial         func(path string) (*grpc.ClientConn, localpb.LocalClient, error)
	hello        func(ctx context.Context, c localpb.LocalClient, version uint32) (helloResult, error)
	waitNotReady func(ctx context.Context, cc *grpc.ClientConn)
	version      uint32
	register     func(ctx context.Context, c localpb.LocalClient, first *localpb.RegisterRequest,
		st *statusStore, onComplete completeFunc, log *slog.Logger) error
	first      *localpb.RegisterRequest
	st         *statusStore
	onComplete completeFunc
	after      func(time.Duration) <-chan time.Time
	log        *slog.Logger
}

const (
	loopLostText = "the connection to the Broker was lost; reconnecting"

	loopRemedyUser   = "restart the Broker of this account with the same peers version as this MCP server, either with `peers service install` or by running `peers service run` in a terminal outside the session; this session reconnects by itself"
	loopRemedySystem = "ask the administrator to restart the Broker system service with the same peers version as this MCP server; this session reconnects by itself"
	loopRemedyTest   = "restart the Broker at %s with the same peers version as this MCP server; this session reconnects by itself"
)

func loopRemedy(t target) string {
	switch t.Kind {
	case kindSystem:
		return loopRemedySystem
	case kindTest:
		return fmt.Sprintf(loopRemedyTest, t.Path)
	default:
		return loopRemedyUser
	}
}

// ConnBackoffCap is the longest wait between reconnection attempts to a known
// socket; end-to-end tests derive their wait bounds from it.
const ConnBackoffCap = 5 * time.Second

// loopConnBackoff paces reconnection attempts and the reselect made after a
// failed selectTarget. Only a registration completion resets it; a successful
// Hello must not, or a Broker that fails every Register stream would retry at
// the initial interval forever without backing off.
type loopConnBackoff struct {
	mu   sync.Mutex
	wait time.Duration
}

func (b *loopConnBackoff) next() time.Duration {
	b.mu.Lock()
	defer b.mu.Unlock()
	switch {
	case b.wait == 0:
		b.wait = 250 * time.Millisecond
	case b.wait < ConnBackoffCap:
		b.wait *= 2
		if b.wait > ConnBackoffCap {
			b.wait = ConnBackoffCap
		}
	}
	return b.wait
}

func (b *loopConnBackoff) reset() {
	b.mu.Lock()
	b.wait = 0
	b.mu.Unlock()
}

// runLoop selects a Broker, connects to it and never starts one; after a lost
// connection it reconnects to the same target, and every iteration recovers from
// a panic so no Broker-side failure ends the process.
func runLoop(ctx context.Context, d loopDeps) {
	conn := &loopConnBackoff{}
	if d.onComplete != nil {
		complete := d.onComplete
		d.onComplete = func(ctx context.Context, c localpb.LocalClient, agentID string, autoJoin bool) {
			conn.reset()
			complete(ctx, c, agentID, autoJoin)
		}
	}
	for ctx.Err() == nil {
		if loopOnce(ctx, d, conn) {
			return
		}
	}
}

func loopOnce(ctx context.Context, d loopDeps, conn *loopConnBackoff) (stop bool) {
	defer func() {
		if r := recover(); r != nil {
			if d.log != nil {
				d.log.Error("mcpserver: recovered from a panic in the connection loop", "panic", r)
			}
			stop = !loopWait(ctx, d.after, conn.next())
		}
	}()

	t, err := d.selectTarget(ctx)
	if err != nil {
		if ctx.Err() != nil {
			return true
		}
		if errors.Is(err, errNoBroker) {
			loopSetStatus(d.st, PhaseNoBroker, "", "", nil)
		} else if mismatch, ok := peerMismatchOf(err); ok {
			loopSetStatus(d.st, PhaseUnavailable, selectionMismatchText(mismatch), "", nil)
		} else {
			loopSetStatus(d.st, PhaseUnavailable, unavailableText(err.Error()), "", nil)
		}
		return !loopWait(ctx, d.after, conn.next())
	}

	for {
		if ctx.Err() != nil {
			return true
		}

		cc, client, err := d.dial(t.Path)
		if err != nil {
			phase, detail := loopFailureStatus(ctx, d, t, err.Error())
			loopSetStatus(d.st, phase, detail, "", nil)
			if !loopReconnect(ctx, d, conn) {
				return true
			}
			continue
		}

		res, err := d.hello(ctx, client, d.version)
		if err != nil {
			probe, probed := loopProbeInfo(ctx, d, t)
			// Only an answering socket plus a connection that is not READY means
			// the Broker closed the transport during the handshake; a returned
			// gRPC status keeps the connection READY.
			closedDuringHandshake := cc.GetState() != connectivity.Ready
			_ = cc.Close()
			switch {
			case probed && probe.Outcome == probeOK && closedDuringHandshake:
				loopSetRejected(d.st, handshakeRejection(err))
			case probed:
				phase, text := loopProbeFailureStatus(t, probe, err.Error())
				loopSetStatus(d.st, phase, text, "", nil)
			default:
				phase, detail := loopFailureStatusForTarget(t, err.Error())
				loopSetStatus(d.st, phase, detail, "", nil)
			}
			if !loopReconnect(ctx, d, conn) {
				return true
			}
			continue
		}

		if !res.Compatible {
			// hello reports BrokerVersion 0 when a version_mismatch refusal
			// carries no version; do not name it then.
			detail := fmt.Sprintf("the running Broker speaks a different local protocol version; this MCP server speaks version %d",
				d.version)
			if res.BrokerVersion != 0 {
				detail = fmt.Sprintf("the running Broker speaks local protocol version %d; this MCP server speaks version %d",
					res.BrokerVersion, d.version)
			}
			loopSetStatus(d.st, PhaseVersionMismatch, detail, loopRemedy(t), nil)
			// The Broker keeps the connection open on a version mismatch, so wait
			// for it to leave READY before reconnecting.
			d.waitNotReady(ctx, cc)
			_ = cc.Close()
			if ctx.Err() != nil {
				return true
			}
			if !loopReconnect(ctx, d, conn) {
				return true
			}
			continue
		}

		d.st.Update(func(s *Status) {
			s.Phase = PhaseConnected
			s.Detail = ""
			s.Remedy = ""
			s.Client = client
			s.Conn = cc
			s.RejectCode = localpb.ErrorCode_ERROR_CODE_UNSPECIFIED
		})

		regErr := d.register(ctx, client, d.first, d.st, d.onComplete, d.log)
		_ = cc.Close()
		if ctx.Err() != nil {
			return true
		}
		if detail := errorDetail(regErr); detail != nil {
			// A Broker error detail reaches the model unchanged; it must not be
			// reported as no Broker.
			loopSetRejected(d.st, rejectedStatus(detail))
		} else {
			phase, detail := loopFailureStatus(ctx, d, t, loopLostText)
			loopSetStatus(d.st, phase, detail, "", nil)
		}
		if !loopReconnect(ctx, d, conn) {
			return true
		}
	}
}

// loopReconnect waits before the next attempt at the fixed target and never
// starts a Broker; it returns false when ctx is done.
func loopReconnect(ctx context.Context, d loopDeps, conn *loopConnBackoff) bool {
	return loopWait(ctx, d.after, conn.next())
}

type brokerRejection struct {
	detail string
	code   localpb.ErrorCode
}

func rejectedStatus(d *localpb.ErrorDetail) brokerRejection {
	return brokerRejection{detail: d.GetMessage(), code: d.GetCode()}
}

// A connection limit during the handshake closes the connection before any
// ErrorDetail travels, so name a likely cause rather than claiming the Broker
// is gone.
func handshakeRejection(err error) brokerRejection {
	if detail := errorDetail(err); detail != nil {
		return rejectedStatus(detail)
	}
	return brokerRejection{detail: textBrokerRefusedConnection, code: localpb.ErrorCode_ERROR_CODE_INTERNAL}
}

func loopProbeInfo(ctx context.Context, d loopDeps, t target) (probeInfo, bool) {
	if t.Kind != kindUser && t.Kind != kindSystem {
		return probeInfo{}, false
	}
	probe := d.probe
	if probe == nil {
		probe = inspectSocket
	}
	return probe(ctx, t.Path), true
}

func peerMismatchOf(err error) (*peerMismatch, bool) {
	var mismatch *peerMismatch
	if errors.As(err, &mismatch) {
		return mismatch, true
	}
	return nil, false
}

func selectionMismatchText(m *peerMismatch) string {
	if m.kind != kindSystem {
		return m.Error()
	}
	return m.Error() + "; " + textNoBroker
}

func loopNoBrokerStatus(t target) (Phase, string) {
	if t.Kind == kindSystem {
		return PhaseNoBroker, textNoSystemBroker
	}
	return PhaseNoBroker, ""
}

func loopProbeFailureStatus(t target, info probeInfo, detail string) (Phase, string) {
	switch info.Outcome {
	case probeMissing, probeRefused:
		return loopNoBrokerStatus(t)
	case probeWrongPeer:
		return PhaseUnavailable, peerMismatchError(t.Path, t.Kind, info.PeerUID).Error()
	case probeOK:
		if t.Kind == kindSystem {
			return PhaseUnavailable, systemBrokerUnresponsiveText(detail)
		}
		return PhaseUnavailable, unavailableText(detail)
	default: // probeDenied, probeOther
		if t.Kind == kindSystem {
			return PhaseUnavailable, systemBrokerUnreachableText(t.Path, detail)
		}
		return PhaseUnavailable, unavailableText(detail)
	}
}

func loopFailureStatusForTarget(t target, detail string) (Phase, string) {
	if t.Kind == kindSystem {
		return PhaseUnavailable, systemBrokerUnresponsiveText(detail)
	}
	return PhaseUnavailable, unavailableText(detail)
}

func loopFailureStatus(ctx context.Context, d loopDeps, t target, detail string) (Phase, string) {
	if info, probed := loopProbeInfo(ctx, d, t); probed {
		return loopProbeFailureStatus(t, info, detail)
	}
	return loopFailureStatusForTarget(t, detail)
}

func loopSetStatus(st *statusStore, phase Phase, detail, remedy string, client localpb.LocalClient) {
	st.Update(func(s *Status) {
		s.Phase = phase
		s.Detail = detail
		s.Remedy = remedy
		s.Client = client
		s.Conn = nil
		s.RejectCode = localpb.ErrorCode_ERROR_CODE_UNSPECIFIED
	})
}

func loopSetRejected(st *statusStore, r brokerRejection) {
	st.Update(func(s *Status) {
		s.Phase = PhaseBrokerRejected
		s.Detail = r.detail
		s.Remedy = ""
		s.Client = nil
		s.Conn = nil
		s.RejectCode = r.code
	})
}

func loopWait(ctx context.Context, after func(time.Duration) <-chan time.Time, d time.Duration) bool {
	if ctx.Err() != nil {
		return false
	}
	if after == nil {
		after = time.After
	}
	select {
	case <-ctx.Done():
		return false
	case <-after(d):
		return true
	}
}
