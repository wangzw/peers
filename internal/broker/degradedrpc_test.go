package broker

import (
	"context"
	"io"
	"log/slog"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/pairing"
)

func degradedTestBroker(t *testing.T) *Broker {
	t.Helper()
	b, err := New(Deps{
		Config:    newFakeConfig(t.TempDir()),
		Topics:    newFakeTopics(),
		Logger:    NewLogger(io.Discard, new(slog.LevelVar)),
		InSession: func() bool { return false },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(b.Stop)
	return b
}

// TestBrokerStatusAndListBrokersCarryDegraded checks that Status and
// ListBrokers return the local-only degraded state.
func TestBrokerStatusAndListBrokersCarryDegraded(t *testing.T) {
	b := degradedTestBroker(t)
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
		Step:   "listen",
		Reason: "listen address :17443 is unavailable: address already in use",
		Fix:    "peers config set broker.listen <free address>",
	}
	b.setDegraded(state)

	status, err := b.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.GetDegraded() == nil || status.GetDegraded().GetReason() != state.Reason {
		t.Errorf("Status.Degraded = %+v, want the local-only state", status.GetDegraded())
	}
	if status.GetDegraded().GetLevel() != localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY {
		t.Errorf("Status.Degraded.Level = %v", status.GetDegraded().GetLevel())
	}

	list, err := b.ListBrokers(context.Background(), &localpb.ListBrokersRequest{})
	if err != nil {
		t.Fatalf("ListBrokers: %v", err)
	}
	if list.GetLocal() == nil || list.GetLocal().GetDegraded() == nil {
		t.Fatalf("ListBrokers.Local = %+v, want the local state", list.GetLocal())
	}
	if list.GetLocal().GetDegraded().GetReason() != state.Reason {
		t.Errorf("ListBrokers.Local.Degraded = %+v", list.GetLocal().GetDegraded())
	}
}

// TestBrokerNormalOmitsDegraded checks that Status and ListBrokers omit the
// degraded state on a normal Broker.
func TestBrokerNormalOmitsDegraded(t *testing.T) {
	b := degradedTestBroker(t)

	status, err := b.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.GetDegraded() != nil {
		t.Errorf("Status.Degraded = %+v, want nil", status.GetDegraded())
	}
	list, err := b.ListBrokers(context.Background(), &localpb.ListBrokersRequest{})
	if err != nil {
		t.Fatalf("ListBrokers: %v", err)
	}
	if list.GetLocal() == nil {
		t.Fatal("ListBrokers.Local is nil on a normal Broker")
	}
	if list.GetLocal().GetDegraded() != nil {
		t.Errorf("ListBrokers.Local.Degraded = %+v, want nil", list.GetLocal().GetDegraded())
	}
}

// TestCreatePairingKeyRefusedLocallyOnly checks that CreatePairingKey returns
// broker_degraded in local-only degradation but not on a normal Broker.
func TestCreatePairingKeyRefusedLocallyOnly(t *testing.T) {
	b := degradedTestBroker(t)
	state := &localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
		Step:   "listen",
		Reason: "listen address :17443 is unavailable: address already in use",
		Fix:    "peers config set broker.listen <free address>",
	}
	b.setDegraded(state)

	_, err := b.CreatePairingKey(context.Background(), &localpb.CreatePairingKeyRequest{})
	detail := errorDetailOf(t, err)
	if detail.GetCode() != localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED {
		t.Errorf("code = %v, want broker_degraded", detail.GetCode())
	}
	for _, want := range []string{":17443", "broker.listen", "peers config set"} {
		if !strings.Contains(detail.GetMessage(), want) {
			t.Errorf("message %q does not contain %q", detail.GetMessage(), want)
		}
	}

	b.setDegraded(nil)
	_, err = b.CreatePairingKey(context.Background(), &localpb.CreatePairingKeyRequest{})
	if err == nil {
		t.Fatal("CreatePairingKey succeeded without a pairing backend")
	}
	if detail := errorDetailOf(t, err); detail.GetCode() == localpb.ErrorCode_ERROR_CODE_BROKER_DEGRADED {
		t.Errorf("normal Broker answered broker_degraded: %+v", detail)
	}
}

// TestBrokerDiagnosticsStatusIsMinimal checks that diagnostics-only Status
// returns the version and the fault without agents or peers.
func TestBrokerDiagnosticsStatusIsMinimal(t *testing.T) {
	b := degradedTestBroker(t)
	b.deps.Version = "1.2.3"
	b.setDegraded(&localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: "cannot read identity.key",
	})

	status, err := b.Status(context.Background(), &localpb.StatusRequest{})
	if err != nil {
		t.Fatalf("Status: %v", err)
	}
	if status.GetCurrentVersion() != "1.2.3" {
		t.Errorf("CurrentVersion = %q", status.GetCurrentVersion())
	}
	if len(status.GetAgents()) != 0 || len(status.GetPeers()) != 0 {
		t.Errorf("diagnostics Status returned agents or peers: %+v", status)
	}
	if status.GetDegraded() == nil {
		t.Error("diagnostics Status has no degraded state")
	}
}

type fakeSendPeers struct {
	alias string
	role  pairing.Role
	known bool
}

func (f fakeSendPeers) ResolveAlias(alias string) (peerID, bool) {
	return peerID{}, f.known && alias == f.alias
}

func (f fakeSendPeers) EffectiveAlias(peerID) string { return f.alias }

func (f fakeSendPeers) Role(peerID) pairing.Role { return f.role }

// TestPeerUnreachableReasonOnlyForAcceptor checks that only an acceptor-side
// peer_unreachable gains the local-only degraded reason.
func TestPeerUnreachableReasonOnlyForAcceptor(t *testing.T) {
	b := degradedTestBroker(t)
	newUnreachable := func() error {
		return notSent(localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE, "Broker %s is unreachable", "hostA.peer")
	}
	usePeers := func(role pairing.Role, known bool) {
		b.deps.Delivery = &delivery{deps: deliveryDeps{Peers: fakeSendPeers{alias: "hostA.peer", role: role, known: known}}}
	}

	reason := "listen address :17443 is unavailable: address already in use"
	b.setDegraded(&localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
		Reason: reason,
	})

	usePeers(pairing.RoleAcceptor, true)
	if got := b.decoratePeerUnreachable(newUnreachable(), "a1@hostA.peer").Error(); !strings.HasSuffix(got, reason) {
		t.Fatalf("acceptor error %q does not end with the degraded reason %q", got, reason)
	}

	usePeers(pairing.RoleDialer, true)
	if got := b.decoratePeerUnreachable(newUnreachable(), "a1@hostA.peer").Error(); strings.Contains(got, reason) {
		t.Fatalf("dialer error gained the acceptor's degraded reason: %q", got)
	}

	usePeers(pairing.RoleAcceptor, false)
	if got := b.decoratePeerUnreachable(newUnreachable(), "a1@hostA.peer").Error(); strings.Contains(got, reason) {
		t.Fatalf("unknown alias gained the degraded reason: %q", got)
	}
	b.deps.Delivery = nil
	if got := b.decoratePeerUnreachable(newUnreachable(), "a1@hostA.peer").Error(); strings.Contains(got, reason) {
		t.Fatalf("no peer table gained the degraded reason: %q", got)
	}

	usePeers(pairing.RoleAcceptor, true)
	b.setDegraded(nil)
	if got := b.decoratePeerUnreachable(newUnreachable(), "a1@hostA.peer").Error(); strings.Contains(got, reason) {
		t.Fatalf("normal run altered the error: %q", got)
	}
	b.setDegraded(&localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY,
		Reason: reason,
	})
	if got := b.decoratePeerUnreachable(newUnreachable(), "a1@hostA.peer").Error(); strings.Contains(got, reason) {
		t.Fatalf("diagnostics-only run altered the error: %q", got)
	}

	b.setDegraded(&localpb.DegradedState{
		Level:  localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY,
		Reason: reason,
	})
	notFound := notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "no Broker is known")
	if got := b.decoratePeerUnreachable(notFound, "a1@hostA.peer").Error(); strings.Contains(got, reason) {
		t.Fatalf("not_found gained the degraded reason: %q", got)
	}
}
