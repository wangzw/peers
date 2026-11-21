package pairing

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/protobuf/proto"
)

// TestRejectReasonWireMappingRoundTrip checks that every reason carried on the
// wire survives the local-to-protobuf mapping in both directions, that the
// expired-key reason keeps its assigned wire value, and that local-only reasons
// never map onto the wire.
func TestRejectReasonWireMappingRoundTrip(t *testing.T) {
	t.Parallel()

	wireReasons := []RejectReason{
		ReasonKeyInvalid,
		ReasonVerificationFailed,
		ReasonTryLater,
		ReasonKeyExpired,
	}
	for _, local := range wireReasons {
		pb, ok := rejectReasonToPB(local)
		if !ok {
			t.Fatalf("rejectReasonToPB(%v) = _, false; want true", local)
		}
		back, ok := rejectReasonFromPB(pb)
		if !ok {
			t.Fatalf("rejectReasonFromPB(%v) = _, false; want true", pb)
		}
		if back != local {
			t.Fatalf("round trip of %v = %v, want %v", local, back, local)
		}
	}

	pb, ok := rejectReasonToPB(ReasonKeyExpired)
	if !ok {
		t.Fatalf("rejectReasonToPB(ReasonKeyExpired) = _, false; want true")
	}
	if got := pb.Number(); got != 4 {
		t.Fatalf("KEY_EXPIRED wire number = %d, want 4", got)
	}

	for _, local := range []RejectReason{ReasonTimeout, ReasonNotConnected} {
		if _, ok := rejectReasonToPB(local); ok {
			t.Fatalf("rejectReasonToPB(%v) = _, true; want false", local)
		}
	}
}

// TestRejectErrorKeyExpired covers REQ-broker-pairing-006: a initiator that
// receives KEY_EXPIRED reports an expired key, not a protocol error.
func TestRejectErrorKeyExpired(t *testing.T) {
	t.Parallel()

	payload, err := proto.Marshal(&pairingpb.PairReject{
		Reason: pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_KEY_EXPIRED,
	})
	if err != nil {
		t.Fatalf("marshal PairReject: %v", err)
	}

	err = rejectError(payload)
	var pairingErr *PairingError
	if !errors.As(err, &pairingErr) {
		t.Fatalf("rejectError = %v (%T), want *PairingError", err, err)
	}
	if pairingErr.Reason != ReasonKeyExpired {
		t.Fatalf("Reason = %v, want %v", pairingErr.Reason, ReasonKeyExpired)
	}
	if !strings.HasPrefix(pairingErr.Detail, "key expired") {
		t.Fatalf("Detail = %q, want a %q prefix", pairingErr.Detail, "key expired")
	}
	if !strings.HasPrefix(err.Error(), "Not connected: key expired") {
		t.Fatalf("Error() = %q, want a %q prefix", err.Error(), "Not connected: key expired")
	}
	if !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("errors.Is(err, ErrPairingRejected) = false, want true")
	}
	if errors.Is(err, ErrProtocol) {
		t.Fatalf("errors.Is(err, ErrProtocol) = true, want false")
	}
}

// TestRejectErrorUnknownReasonStillProtocolError checks that an unassigned
// reason value is still treated as a protocol error rather than as an expired
// key.
func TestRejectErrorUnknownReasonStillProtocolError(t *testing.T) {
	t.Parallel()

	payload, err := proto.Marshal(&pairingpb.PairReject{
		Reason: pairingpb.PairingRejectReason(5),
	})
	if err != nil {
		t.Fatalf("marshal PairReject: %v", err)
	}

	err = rejectError(payload)
	if !errors.Is(err, ErrProtocol) {
		t.Fatalf("rejectError = %v, want a protocol error", err)
	}
}

// TestRejectAcceptorAuditsKeyExpired checks that the acceptor sends KEY_EXPIRED
// on the wire, records the "key expired" audit reason and returns a matching
// PairingError.
func TestRejectAcceptorAuditsKeyExpired(t *testing.T) {
	t.Parallel()

	clk := newHSClock()
	s := newHSSettings(t)
	p := hsNewPairing(t, s, &hsTransport{}, clk)

	fp, pub := hsKey(t)
	aConn, bConn := hsConnPairFor(fp, pub, p.identity.fingerprint(), p.identity.publicKey())

	rejectErr := make(chan error, 1)
	go func() {
		rejectErr <- p.rejectAcceptor(bConn, context.Background(), fp, ReasonKeyExpired, clk.Now())
	}()

	kind, payload, err := receiveWithTimeout(aConn)
	if err != nil {
		t.Fatalf("receive reject: %v", err)
	}
	if kind != kindPairReject {
		t.Fatalf("frame kind = %d, want PairReject", kind)
	}
	var reject pairingpb.PairReject
	if err := proto.Unmarshal(payload, &reject); err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if got := reject.GetReason(); got != pairingpb.PairingRejectReason_PAIRING_REJECT_REASON_KEY_EXPIRED {
		t.Fatalf("wire reason = %v, want KEY_EXPIRED", got)
	}

	err = awaitAccept(rejectErr)
	var pairingErr *PairingError
	if !errors.As(err, &pairingErr) {
		t.Fatalf("rejectAcceptor = %v (%T), want *PairingError", err, err)
	}
	if pairingErr.Reason != ReasonKeyExpired {
		t.Fatalf("Reason = %v, want %v", pairingErr.Reason, ReasonKeyExpired)
	}

	events := s.recorded()
	if len(events) != 1 {
		t.Fatalf("audit events = %d, want 1", len(events))
	}
	if events[0].Event != config.EventPairingFailed {
		t.Fatalf("audit event = %q, want %q", events[0].Event, config.EventPairingFailed)
	}
	if events[0].Detail != "key expired" {
		t.Fatalf("audit Detail = %q, want %q", events[0].Detail, "key expired")
	}
	if events[0].MergeKey != "" {
		t.Fatalf("audit MergeKey = %q, want empty", events[0].MergeKey)
	}
	if events[0].Peer == nil || events[0].Peer.Alias != "" {
		t.Fatalf("audit peer = %+v, want the fingerprint without the unverified alias", events[0].Peer)
	}
}
