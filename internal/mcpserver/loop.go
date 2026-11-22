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

// loopDeps wires the background connection loop. Production fills every field;
// tests replace them with stubs so that no real Broker process is involved.
type loopDeps struct {
	selectTarget func(ctx context.Context) (target, error)
	probe        func(ctx context.Context, path string) probeInfo
	dial         func(path string) (*grpc.ClientConn, localpb.LocalClient, error)
	hello        func(ctx context.Context, c localpb.LocalClient, version uint32) (helloResult, error)
	waitNotReady func(ctx context.Context, cc *grpc.ClientConn)
	version      uint32 // Options.LocalProtocolVersion, 0 already replaced by localpb.ProtocolVersion
	register     func(ctx context.Context, c localpb.LocalClient, first *localpb.RegisterRequest,
		st *statusStore, onComplete completeFunc, log *slog.Logger) error
	first      *localpb.RegisterRequest
	st         *statusStore
	onComplete completeFunc
	after      func(time.Duration) <-chan time.Time
	log        *slog.Logger
}

// Texts the loop puts into Status. The remedy clauses match the sentences the
// design requires for each kind of Broker and always contain "restart the
// Broker".
const (
	loopLostText = "the connection to the Broker was lost; reconnecting"

	loopRemedyUser   = "restart the Broker of this account with the same peers version as this MCP server, either with `peers service install` or by running `peers service run` in a terminal outside the session; this session reconnects by itself"
	loopRemedySystem = "ask the administrator to restart the Broker system service with the same peers version as this MCP server; this session reconnects by itself"
	loopRemedyTest   = "restart the Broker at %s with the same peers version as this MCP server; this session reconnects by itself"
)

// loopRemedy returns the "To fix it, ..." clause for a target.
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
// socket. It is exported so end-to-end tests can derive their wait bounds from
// it.
const ConnBackoffCap = 5 * time.Second

// loopConnBackoff paces reconnection attempts to a known socket: the first
// retry after a failure waits 250ms, later ones double up to ConnBackoffCap.
// Only a registration completion resets it; a successful Hello must not, or a
// Broker that fails every Register stream would be retried without any wait. It
// is safe for concurrent use because the reset runs from the goroutine that
// handles registration_complete.
type loopConnBackoff struct {
	mu   sync.Mutex
	wait time.Duration
}

// next returns the wait before the next attempt and advances the sequence.
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

// reset restarts the sequence at 250ms.
func (b *loopConnBackoff) reset() {
	b.mu.Lock()
	b.wait = 0
	b.mu.Unlock()
}

// runLoop runs until ctx is done. It selects a Broker, connects, performs the
// version handshake and keeps the Register stream open. A lost connection
// reconnects to the same Broker and never starts one. Every iteration is
// guarded against panics, because no Broker-side failure may end this process.
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

// loopOnce performs one full pass: selection, connection and registration, or
// the wait that precedes the next attempt. It returns true when ctx is done
// and the loop must stop.
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
			// A refusal at the transport handshake closes the connection, so the
			// ClientConn is no longer READY; a Broker that answered Hello with a
			// status of its own leaves the connection usable. Probing classifies
			// the failure: only a socket answered by the account that may serve
			// this Broker is reported as running but not answering, a socket
			// served by another account is the same mismatch selection reports,
			// and a socket that could not be reached at all keeps the original
			// error without claiming the Broker is running.
			probe, probed := loopProbeInfo(ctx, d, t)
			// The connection state is what separates the two failure shapes.
			// Only an answering socket plus a connection that is not READY
			// identifies a Broker that closed the transport during the
			// handshake: a Broker that returns a gRPC status keeps the
			// connection READY, so its error is reported as before. Once the
			// transport is closed, GetState reports TRANSIENT_FAILURE or IDLE
			// depending on how far the connection got, and neither is READY, so
			// the test is "not READY" rather than one named state.
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
			// hello reports BrokerVersion 0 when the Broker refused the
			// handshake with a version_mismatch error, whose detail carries no
			// version field. Naming version 0 would claim something the Broker
			// never said, so that path describes the mismatch without it.
			detail := fmt.Sprintf("the running Broker speaks a different local protocol version; this MCP server speaks version %d",
				d.version)
			if res.BrokerVersion != 0 {
				detail = fmt.Sprintf("the running Broker speaks local protocol version %d; this MCP server speaks version %d",
					res.BrokerVersion, d.version)
			}
			loopSetStatus(d.st, PhaseVersionMismatch, detail, loopRemedy(t), nil)
			// The Broker keeps this connection open on a version mismatch, so
			// waiting for it to leave READY is the only reliable sign that the
			// administrator or the user restarted it. No requests are sent
			// while waiting.
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
			// The Broker ended the stream with an explanation of its own, such
			// as a quota refusal. Its sentence and code reach the model
			// unchanged; calling this "no Broker" would send the reader to
			// start a Broker that is running.
			loopSetRejected(d.st, rejectedStatus(detail))
		} else {
			phase, detail := loopFailureStatus(ctx, d, t, loopLostText)
			loopSetStatus(d.st, phase, detail, "", nil)
		}
		// Reconnect to the same target through the same rules as a failed dial,
		// so a Register stream that ends at once does not spin.
		if !loopReconnect(ctx, d, conn) {
			return true
		}
	}
}

// loopReconnect waits before the next attempt at the fixed target. It never
// starts a Broker; a Broker that has exited is left for the service manager or
// the user to start again. It returns false when ctx is done.
func loopReconnect(ctx context.Context, d loopDeps, conn *loopConnBackoff) bool {
	return loopWait(ctx, d.after, conn.next())
}

// brokerRejection is a refusal by a Broker that is running: the sentence and
// code the model is shown.
type brokerRejection struct {
	detail string
	code   localpb.ErrorCode
}

// rejectedStatus carries the Broker's own explanation and code from an
// ErrorDetail.
func rejectedStatus(d *localpb.ErrorDetail) brokerRejection {
	return brokerRejection{detail: d.GetMessage(), code: d.GetCode()}
}

// handshakeRejection is what a failed Hello reports once the socket is known
// to answer. A connection limit enforced during the credential handshake
// closes the connection before any ErrorDetail can travel, so the text names a
// likely cause instead of claiming the Broker is gone; an ErrorDetail the
// Broker did give is passed through as it is.
func handshakeRejection(err error) brokerRejection {
	if detail := errorDetail(err); detail != nil {
		return rejectedStatus(detail)
	}
	return brokerRejection{detail: textBrokerRefusedConnection, code: localpb.ErrorCode_ERROR_CODE_INTERNAL}
}

// loopProbeInfo probes the socket of a user or system target and returns the
// account of the process that answered it alongside the outcome. The second
// result is false for a target kind that has no local socket to probe.
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

// loopNoBrokerStatus is the no-Broker phase and text for a user or system
// target. Only a system target carries text, because its reader is sent to the
// administrator; a user target keeps the general text a session that never
// found a Broker shows.
func loopNoBrokerStatus(t target) (Phase, string) {
	if t.Kind == kindSystem {
		return PhaseNoBroker, textNoSystemBroker
	}
	return PhaseNoBroker, ""
}

// loopProbeFailureStatus chooses the phase and text for a failed attempt at t
// from the probe of its socket. Only probeOK means the account that may serve
// this Broker answered at this path, so only probeOK is reported as a running
// Broker that is not answering. A socket served by another account is the peer
// mismatch selection reports, named by peerMismatchError with the answering
// account; it is not a running Broker. A probe that could not connect at all
// (denied, or another failure) does not claim the Broker is running and keeps
// the original error.
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

// loopFailureStatusForTarget chooses the phase for a failed attempt at a target
// whose socket was not probed. A release build probes every user and system
// target, so only a test-named socket reaches it; the system branch is kept so
// a caller that skips the probe still reports a selected system Broker as
// running rather than absent.
func loopFailureStatusForTarget(t target, detail string) (Phase, string) {
	if t.Kind == kindSystem {
		return PhaseUnavailable, systemBrokerUnresponsiveText(detail)
	}
	return PhaseUnavailable, unavailableText(detail)
}

// loopFailureStatus chooses the phase for a failed attempt at t. It probes the
// socket of a user or system target and classifies the failure from the probe
// result; a target with no local socket keeps the unprobed text.
func loopFailureStatus(ctx context.Context, d loopDeps, t target, detail string) (Phase, string) {
	if info, probed := loopProbeInfo(ctx, d, t); probed {
		return loopProbeFailureStatus(t, info, detail)
	}
	return loopFailureStatusForTarget(t, detail)
}

// loopSetStatus replaces the connection status with one that has no usable
// client or connection.
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

// loopSetRejected records a refusal by a running Broker, with the code the
// model is shown beside its explanation.
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

// loopWait waits d on the injected timer and reports whether the wait
// completed. It returns false when ctx is done first.
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
