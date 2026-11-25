package broker

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing"
)

// connectHandlerDone signals once the Connect handler has returned; a client
// that cancels its own context sees Canceled before the server handler
// finishes, so a test asserting on the audit log waits for this signal first.
func connectHandlerDone() (grpc.ServerOption, <-chan struct{}) {
	done := make(chan struct{})
	var once sync.Once
	opt := grpc.ChainUnaryInterceptor(func(ctx context.Context, req any, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (any, error) {
		resp, err := handler(ctx, req)
		if info.FullMethod == localpb.Local_Connect_FullMethodName {
			once.Do(func() { close(done) })
		}
		return resp, err
	})
	return opt, done
}

func waitConnectHandlerDone(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(eventTimeout):
		t.Fatal("Connect handler did not return in time")
	}
}

func callTestConnect(t *testing.T, tb *testBroker, req *localpb.ConnectRequest) (*localpb.ConnectResponse, error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), eventTimeout)
	defer cancel()
	return tb.client.Connect(ctx, req)
}

func connectPairingCalls(p *fakePairing) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.calls...)
}

func connectKeys(p *fakePairing) []string {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]string(nil), p.keys...)
}

func connectLinkCalls(l *fakePeerLinks) []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return append([]string(nil), l.calls...)
}

func connectAuditEvents(tb *testBroker) []config.Event {
	tb.cfg.mu.Lock()
	defer tb.cfg.mu.Unlock()
	return append([]config.Event(nil), tb.cfg.audits...)
}

func connectSuccessPeer() pairing.Peer {
	return pairing.Peer{
		Fingerprint: fp(7),
		Alias:       "hostA",
		LocalAlias:  "lab-a",
		Role:        pairing.RoleDialer,
	}
}

func connectFunc(peer pairing.Peer, shownHost string, err error) func(context.Context, string, string, string) (pairing.Peer, string, error) {
	return func(context.Context, string, string, string) (pairing.Peer, string, error) {
		return peer, shownHost, err
	}
}

// TestConnectPassesArgumentsThrough checks that the address, the pairing key
// and the local alias reach pairing unchanged.
func TestConnectPassesArgumentsThrough(t *testing.T) {
	p := &fakePairing{connectFn: connectFunc(connectSuccessPeer(), "", nil)}
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{
		Address:    "hostA(192.0.2.10:17443)",
		Key:        "K",
		LocalAlias: "lab-a",
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if got, want := connectKeys(p), []string{"K"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("pairing received keys %q, want %q", got, want)
	}
	want := []string{"Connect hostA(192.0.2.10:17443) lab-a"}
	if got := connectPairingCalls(p); !reflect.DeepEqual(got, want) {
		t.Fatalf("pairing calls = %q, want %q", got, want)
	}
}

// TestConnectSuccessResponse checks that a successful Connect returns the
// effective alias, the fingerprint and the shown host, and kicks the link.
func TestConnectSuccessResponse(t *testing.T) {
	p := &fakePairing{connectFn: connectFunc(connectSuccessPeer(), "hostA", nil)}
	links := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, links))

	resp, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "hostA(192.0.2.10:17443)", Key: "K"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if resp.GetAlias() != "lab-a" {
		t.Fatalf("Alias = %q, want the effective alias %q", resp.GetAlias(), "lab-a")
	}
	wantFP := fp(7)
	if !bytes.Equal(resp.GetFingerprint(), wantFP[:]) {
		t.Fatalf("Fingerprint = %x, want %x", resp.GetFingerprint(), wantFP[:])
	}
	if resp.GetShownHost() != "hostA" {
		t.Fatalf("ShownHost = %q, want %q", resp.GetShownHost(), "hostA")
	}
	if n := len(resp.GetWarnings()); n != 0 {
		t.Fatalf("Warnings = %v, want none", resp.GetWarnings())
	}
	if got, want := connectLinkCalls(links), []string{"Kick 07070707"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("link calls = %q, want %q", got, want)
	}
}

// TestConnectAddressWithoutHostHasEmptyShownHost checks that an address without
// a host yields an empty shown host.
func TestConnectAddressWithoutHostHasEmptyShownHost(t *testing.T) {
	p := &fakePairing{connectFn: connectFunc(connectSuccessPeer(), "", nil)}
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	resp, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "192.0.2.10:17443", Key: "K"})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}
	if resp.GetShownHost() != "" {
		t.Fatalf("ShownHost = %q, want empty", resp.GetShownHost())
	}
}

// TestConnectNeedsLocalAlias checks that an alias conflict returns
// needs_local_alias without an audit record or a link call.
func TestConnectNeedsLocalAlias(t *testing.T) {
	p := &fakePairing{connectFn: connectFunc(pairing.Peer{}, "", pairing.ErrNeedsLocalAlias)}
	links := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, links))

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "198.51.100.4:17443", Key: "K"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NEEDS_LOCAL_ALIAS)

	if got := connectLinkCalls(links); len(got) != 0 {
		t.Fatalf("link calls = %q, want none", got)
	}
	if events := connectAuditEvents(tb); len(events) != 0 {
		t.Fatalf("audit events = %v, want none", events)
	}
}

// TestConnectRejectedCarriesReason checks that a refusal reason reaches the
// caller and the audit record, and the key never reaches the log.
func TestConnectRejectedCarriesReason(t *testing.T) {
	const detailText = "the pairing key is invalid, expired or already used"
	const address = "198.51.100.4:17443"
	const key = "K7QF-2M9X-HT4C-PWZ3"
	p := &fakePairing{connectFn: connectFunc(pairing.Peer{}, "",
		&pairing.PairingError{Reason: pairing.ReasonKeyInvalid, Detail: detailText})}
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: address, Key: key})
	detail := wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_PAIRING_REJECTED)
	if detail.GetMessage() != detailText {
		t.Fatalf("message = %q, want %q", detail.GetMessage(), detailText)
	}

	failed := auditsOf(tb.cfg, config.EventPairingFailed)
	if len(failed) != 1 {
		t.Fatalf("pairing_failed records = %d, want 1", len(failed))
	}
	if want := "key invalid; address " + address; failed[0].Detail != want {
		t.Fatalf("Detail = %q, want %q", failed[0].Detail, want)
	}
	if strings.Contains(tb.logs.String(), key) {
		t.Fatalf("runtime log carries the pairing key: %s", tb.logs.String())
	}
}

// TestConnectUnreachable checks that an unreachable peer returns peer_unreachable
// and one pairing_failed record.
func TestConnectUnreachable(t *testing.T) {
	p := &fakePairing{connectFn: connectFunc(pairing.Peer{}, "", pairing.ErrPeerUnreachable)}
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "198.51.100.4:17443", Key: "K"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE)

	failed := auditsOf(tb.cfg, config.EventPairingFailed)
	if len(failed) != 1 {
		t.Fatalf("pairing_failed records = %d, want 1", len(failed))
	}
	if !strings.HasPrefix(failed[0].Detail, "peer unreachable") {
		t.Fatalf("Detail = %q, want a peer unreachable prefix", failed[0].Detail)
	}
}

// TestConnectInvalidAddressNotAudited checks that an invalid address returns
// invalid_argument with no audit record or link call.
func TestConnectInvalidAddressNotAudited(t *testing.T) {
	p := &fakePairing{connectFn: connectFunc(pairing.Peer{}, "", pairing.ErrInvalidAddress)}
	links := &fakePeerLinks{}
	tb := newTestBroker(t, withPeers(p, links))

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "nonsense", Key: "K"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INVALID_ARGUMENT)

	if events := connectAuditEvents(tb); len(events) != 0 {
		t.Fatalf("audit events = %v, want none", events)
	}
	if got := connectLinkCalls(links); len(got) != 0 {
		t.Fatalf("link calls = %q, want none", got)
	}
}

// TestConnectBadAgentHasNoSideEffect checks that an unknown agent id returns
// not_registered before pairing is called.
func TestConnectBadAgentHasNoSideEffect(t *testing.T) {
	p := &fakePairing{connectFn: connectFunc(connectSuccessPeer(), "", nil)}
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{
		Address: "198.51.100.4:17443",
		Key:     "K",
		AgentId: "nope",
	})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_NOT_REGISTERED)

	if got := connectPairingCalls(p); len(got) != 0 {
		t.Fatalf("pairing calls = %q, want none", got)
	}
	if events := connectAuditEvents(tb); len(events) != 0 {
		t.Fatalf("audit events = %v, want none", events)
	}
}

// TestConnectSucceededAudited checks that a successful Connect writes one
// pairing_succeeded record naming the agent, without the pairing key.
func TestConnectSucceededAudited(t *testing.T) {
	const key = "K7QF-2M9X-HT4C-PWZ3"
	p := &fakePairing{connectFn: connectFunc(connectSuccessPeer(), "hostA", nil)}
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}))
	id, address := auditRegister(t, tb, "api-worker")

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{
		Address:    "hostA(192.0.2.10:17443)",
		Key:        key,
		LocalAlias: "lab-a",
		AgentId:    id,
	})
	if err != nil {
		t.Fatalf("Connect: %v", err)
	}

	succeeded := auditsOf(tb.cfg, config.EventPairingSucceeded)
	if len(succeeded) != 1 {
		t.Fatalf("pairing_succeeded records = %d, want 1", len(succeeded))
	}
	e := succeeded[0]
	if e.Peer == nil {
		t.Fatal("Peer = nil, want the peer reference")
	}
	if e.Peer.Fingerprint != fp(7) {
		t.Fatalf("Peer.Fingerprint = %x, want %x", e.Peer.Fingerprint, fp(7))
	}
	if e.Peer.Alias != "lab-a" {
		t.Fatalf("Peer.Alias = %q, want the effective alias %q", e.Peer.Alias, "lab-a")
	}
	if e.Actor != address {
		t.Fatalf("Actor = %q, want the registered address %q", e.Actor, address)
	}
	if want := usernameFor(uint32(os.Geteuid())); e.Account != want {
		t.Fatalf("Account = %q, want %q", e.Account, want)
	}

	if events := connectAuditEvents(tb); strings.Contains(fmt.Sprintf("%+v", events), key) {
		t.Fatalf("audit events carry the pairing key: %+v", events)
	}
	if strings.Contains(tb.logs.String(), key) {
		t.Fatalf("runtime log carries the pairing key: %s", tb.logs.String())
	}
}

// TestConnectWarningsFromWarner checks that the warner's warnings are returned
// on success and not consulted on failure.
func TestConnectWarningsFromWarner(t *testing.T) {
	warn := &localpb.Warning{
		Kind:    localpb.WarningKind_WARNING_KIND_FINGERPRINT_MISMATCH,
		Message: "m",
	}

	t.Run("success", func(t *testing.T) {
		warner := &fakeWarner{warnings: []*localpb.Warning{warn}}
		p := &fakePairing{connectFn: connectFunc(connectSuccessPeer(), "hostA", nil)}
		tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}), func(d *Deps) { d.Warner = warner })

		resp, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "198.51.100.4:17443", Key: "K"})
		if err != nil {
			t.Fatalf("Connect: %v", err)
		}
		if got := resp.GetWarnings(); len(got) != 1 || !proto.Equal(got[0], warn) {
			t.Fatalf("Warnings = %v, want exactly %v", got, warn)
		}
		if len(warner.got) != 1 {
			t.Fatalf("warner saw %d peers, want 1", len(warner.got))
		}
		if warner.got[0].Fingerprint != fp(7) {
			t.Fatalf("warner peer fingerprint = %x, want %x", warner.got[0].Fingerprint, fp(7))
		}
	})

	t.Run("failure", func(t *testing.T) {
		warner := &fakeWarner{warnings: []*localpb.Warning{warn}}
		p := &fakePairing{connectFn: connectFunc(pairing.Peer{}, "", pairing.ErrPeerUnreachable)}
		tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}), func(d *Deps) { d.Warner = warner })

		_, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "198.51.100.4:17443", Key: "K"})
		wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_PEER_UNREACHABLE)
		if len(warner.got) != 0 {
			t.Fatalf("warner saw %d peers on a failed Connect, want 0", len(warner.got))
		}
	})
}

// TestConnectCanceled checks that a canceled request returns Canceled and writes
// no audit record.
func TestConnectCanceled(t *testing.T) {
	p := &fakePairing{connectFn: connectFunc(pairing.Peer{}, "", context.Canceled)}
	opt, handlerDone := connectHandlerDone()
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}), func(d *Deps) {
		d.ServerOptions = append(d.ServerOptions, opt)
	})

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "198.51.100.4:17443", Key: "K"})
	if status.Code(err) != codes.Canceled {
		t.Fatalf("gRPC code = %v, want Canceled", status.Code(err))
	}
	waitConnectHandlerDone(t, handlerDone)
	if events := connectAuditEvents(tb); len(events) != 0 {
		t.Fatalf("audit events = %v, want none", events)
	}
}

// TestConnectCancellationReachesPairing checks that a canceled request context
// reaches pairing instead of dialing on a detached context.
func TestConnectCancellationReachesPairing(t *testing.T) {
	started := make(chan struct{})
	returned := make(chan struct{})
	p := &fakePairing{connectFn: func(ctx context.Context, _, _, _ string) (pairing.Peer, string, error) {
		close(started)
		<-ctx.Done()
		close(returned)
		return pairing.Peer{}, "", ctx.Err()
	}}
	opt, handlerDone := connectHandlerDone()
	tb := newTestBroker(t, withPeers(p, &fakePeerLinks{}), func(d *Deps) {
		d.ServerOptions = append(d.ServerOptions, opt)
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errc := make(chan error, 1)
	go func() {
		_, err := tb.client.Connect(ctx, &localpb.ConnectRequest{Address: "198.51.100.4:17443", Key: "K"})
		errc <- err
	}()

	select {
	case <-started:
	case <-time.After(eventTimeout):
		t.Fatal("Connect did not reach pairing in time")
	}
	cancel()

	select {
	case <-returned:
	case <-time.After(eventTimeout):
		t.Fatal("pairing Connect did not return after cancellation; the handler did not pass the request context")
	}
	select {
	case err := <-errc:
		if status.Code(err) != codes.Canceled {
			t.Fatalf("gRPC code = %v, want Canceled", status.Code(err))
		}
	case <-time.After(eventTimeout):
		t.Fatal("client Connect did not return after cancellation")
	}
	waitConnectHandlerDone(t, handlerDone)
	if events := connectAuditEvents(tb); len(events) != 0 {
		t.Fatalf("audit events = %v, want none", events)
	}
}

// TestConnectWithoutPairing checks that Connect returns internal when no pairing
// is wired in.
func TestConnectWithoutPairing(t *testing.T) {
	tb := newTestBroker(t, func(d *Deps) { d.PeerLinks = &fakePeerLinks{} })

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "198.51.100.4:17443", Key: "K"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INTERNAL)
}

// TestConnectWithoutPeerLinks checks that Connect returns internal and does not
// call pairing when the link is missing.
func TestConnectWithoutPeerLinks(t *testing.T) {
	p := &fakePairing{}
	tb := newTestBroker(t, func(d *Deps) { d.Pairing = p })

	_, err := callTestConnect(t, tb, &localpb.ConnectRequest{Address: "198.51.100.4:17443", Key: "K"})
	wantErrorDetail(t, err, localpb.ErrorCode_ERROR_CODE_INTERNAL)
	if got := connectPairingCalls(p); len(got) != 0 {
		t.Fatalf("pairing calls = %q, want none when the link is missing", got)
	}
}
