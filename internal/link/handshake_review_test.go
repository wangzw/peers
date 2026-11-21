package link

import (
	"testing"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/link/linkpb"
	"github.com/wangzw/peers/internal/pairing"
)

// TestKnownAcceptorNonHelloFirstFrameClosesSilently checks that a known peer
// whose first frame is not Hello is closed without a Reject and without an
// audit event: the role check refuses it before the unknown-fingerprint path.
func TestKnownAcceptorNonHelloFirstFrameClosesSilently(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xF1}
	hsAddAcceptor(h.peers, peer, "peer")

	c, local, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, &linkpb.Frame{Kind: 200})

	if r := waitHSResult(t, res); r.ok {
		t.Fatal("a known peer's non-Hello first frame was installed")
	}
	if sent := local.sent(); len(sent) != 0 {
		t.Fatalf("the link sent %d frames to a known peer with a bad first frame: %+v", len(sent), sent)
	}
	if evs := h.settings.Events(); len(evs) != 0 {
		t.Fatalf("known peer's non-Hello first frame wrote audit events: %+v", evs)
	}
	hsWaitClosed(t, c)
}

// TestAcceptorMalformedHelloClosesSilently checks that a known peer's Hello
// that fails to decode closes the connection without a Reject or an audit
// event: the decode error is a protocol failure, not a peer-table decision.
func TestAcceptorMalformedHelloClosesSilently(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xF2}
	hsAddAcceptor(h.peers, peer, "peer")

	c, local, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	// A 31-byte nonce is a valid protobuf Hello but a malformed handshake.
	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer", make([]byte, nonceSize-1), nil))

	if r := waitHSResult(t, res); r.ok {
		t.Fatal("a malformed Hello was installed")
	}
	if sent := local.sent(); len(sent) != 0 {
		t.Fatalf("the link answered a malformed Hello with %d frames: %+v", len(sent), sent)
	}
	if evs := h.settings.Events(); len(evs) != 0 {
		t.Fatalf("malformed Hello wrote audit events: %+v", evs)
	}
	hsWaitClosed(t, c)
}

// TestUnknownFingerprintMalformedHelloIsAudited checks that an unknown peer is
// recognised before its Hello is decoded, so a malformed Hello still produces
// the "not a peer" Reject and a connection_rejected audit event.
func TestUnknownFingerprintMalformedHelloIsAudited(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xF3}

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "stranger", make([]byte, nonceSize-1), nil))

	rej := hsRecv(t, p)
	if rej.GetKind() != linkpb.KindReject {
		t.Fatalf("first reply kind = %d, want reject", rej.GetKind())
	}
	reason, err := decodeReject(rej)
	if err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if reason != linkpb.RejectReason_REJECT_REASON_NOT_A_PEER {
		t.Fatalf("reject reason = %v, want not a peer", reason)
	}

	if r := waitHSResult(t, res); r.ok {
		t.Fatal("unknown fingerprint with a malformed Hello was installed")
	}
	ev := h.settings.WaitEvent(t, config.EventConnectionRejected)
	if ev.Peer == nil || ev.Peer.Fingerprint != [32]byte(peer) {
		t.Fatalf("audit Peer = %+v, want fingerprint %x", ev.Peer, peer)
	}
	if evs := h.settings.Events(); len(evs) != 1 {
		t.Fatalf("audit events = %+v, want exactly one connection_rejected", evs)
	}

	h.clock.Advance(h.l.rejectLinger)
	hsWaitClosed(t, c)
}

// TestRejectedConnectionsMergeBySource checks that a rejection names the
// network source, not the certificate the stranger chose: two connections from
// one address with different certificates carry the same merge key and a
// detail naming the source. The merge key is what config groups on, so the
// stranger cannot escape the audit merge by rekeying every connection.
func TestRejectedConnectionsMergeBySource(t *testing.T) {
	h := newHandshakeHarness(t)
	const remote = "203.0.113.7:4000"
	for _, peer := range []Fingerprint{{0xB1}, {0xB2}} {
		c, _, p := h.newConnFrom(t, peer, remote)
		res := h.serve(c, nil)
		hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "stranger", hsNonce(7), nil))
		if rej := hsRecv(t, p); rej.GetKind() != linkpb.KindReject {
			t.Fatalf("first reply kind = %d, want reject", rej.GetKind())
		}
		h.clock.Advance(h.l.rejectLinger)
		hsWaitClosed(t, c)
		if r := waitHSResult(t, res); r.ok {
			t.Fatal("an unknown peer was installed")
		}
	}

	events := h.settings.Events()
	if len(events) != 2 {
		t.Fatalf("audit events = %+v, want two connection_rejected", events)
	}
	for _, ev := range events {
		if ev.Event != config.EventConnectionRejected {
			t.Errorf("audit kind = %q, want connection_rejected", ev.Event)
		}
		if ev.MergeKey != "203.0.113.7/32" {
			t.Errorf("merge key = %q, want the source prefix %q", ev.MergeKey, "203.0.113.7/32")
		}
		if ev.Detail != "not a peer from 203.0.113.7/32" {
			t.Errorf("detail = %q, want %q", ev.Detail, "not a peer from 203.0.113.7/32")
		}
		if ev.Peer == nil {
			t.Error("audit event carries no peer fingerprint")
		}
	}
}

// TestRejectedConnectionsMergeByIPv6Prefix checks the same rule inside one
// IPv6 /64: two addresses in the prefix share one merge key.
func TestRejectedConnectionsMergeByIPv6Prefix(t *testing.T) {
	h := newHandshakeHarness(t)
	event := func(remote string) config.Event {
		t.Helper()
		c, _, p := h.newConnFrom(t, Fingerprint{0xC1}, remote)
		res := h.serve(c, nil)
		hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "stranger", hsNonce(8), nil))
		hsRecv(t, p)
		h.clock.Advance(h.l.rejectLinger)
		hsWaitClosed(t, c)
		if r := waitHSResult(t, res); r.ok {
			t.Fatal("an unknown peer was installed")
		}
		return h.settings.WaitEvent(t, config.EventConnectionRejected)
	}

	first := event("[2001:db8:1:2::1]:4000")
	second := event("[2001:db8:1:2::abcd]:4000")
	const prefix = "2001:db8:1:2::/64"
	for _, ev := range []config.Event{first, second} {
		if ev.MergeKey != prefix {
			t.Errorf("merge key = %q, want %q", ev.MergeKey, prefix)
		}
		if ev.Detail != "not a peer from "+prefix {
			t.Errorf("detail = %q, want %q", ev.Detail, "not a peer from "+prefix)
		}
	}
}

// TestNonHelloFirstFrameRejectionNamesSource covers the other unknown-peer
// path: a first frame that is neither Hello nor PairRequest still reports the
// source, so both paths merge alike.
func TestNonHelloFirstFrameRejectionNamesSource(t *testing.T) {
	h := newHandshakeHarness(t)
	const remote = "203.0.113.9:4000"
	c, _, p := h.newConnFrom(t, Fingerprint{0xC2}, remote)
	res := h.serve(c, nil)
	hsSend(t, p, &linkpb.Frame{Kind: 200})
	hsRecv(t, p)
	h.clock.Advance(h.l.rejectLinger)
	hsWaitClosed(t, c)
	if r := waitHSResult(t, res); r.ok {
		t.Fatal("a connection with an unknown peer was installed")
	}
	ev := h.settings.WaitEvent(t, config.EventConnectionRejected)
	if ev.MergeKey != "203.0.113.9/32" {
		t.Errorf("merge key = %q, want %q", ev.MergeKey, "203.0.113.9/32")
	}
	if ev.Detail != "not a peer from 203.0.113.9/32" {
		t.Errorf("detail = %q, want %q", ev.Detail, "not a peer from 203.0.113.9/32")
	}
}

// TestRoleMismatchStillRejectsKnownDialer guards the role check order: a peer
// this Broker dials is still refused with a role mismatch, not with a silent
// close or a "not a peer" audit.
func TestRoleMismatchStillRejectsKnownDialer(t *testing.T) {
	h := newHandshakeHarness(t)
	peer := Fingerprint{0xF4}
	h.peers.Add(peer, pairing.RoleDialer, "peer")

	c, _, p := h.newConn(t, peer)
	res := h.serve(c, nil)

	hsSend(t, p, encodeHello(linkpb.ProtocolVersion, "peer", hsNonce(12), nil))
	rej := hsRecv(t, p)
	reason, err := decodeReject(rej)
	if err != nil {
		t.Fatalf("decode reject: %v", err)
	}
	if reason != linkpb.RejectReason_REJECT_REASON_ROLE_MISMATCH {
		t.Fatalf("reject reason = %v, want role mismatch", reason)
	}
	if r := waitHSResult(t, res); r.ok {
		t.Fatal("role mismatch was installed")
	}
	if evs := h.settings.Events(); len(evs) != 0 {
		t.Fatalf("role mismatch wrote audit events: %+v", evs)
	}
}
