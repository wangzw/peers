package pairing

// The proof helpers in this file are deliberately independent of the product
// code, so a changed formula must fail here.

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/protobuf/proto"
)

const designLookupTagLength = 8

func designPairHint(key, ni []byte, initiator Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("peers pair hint"))
	mac.Write(ni)
	mac.Write(initiator[:])
	return mac.Sum(nil)[:designLookupTagLength]
}

func designResponderProof(key, ni, nr []byte, acceptor, initiatorSeen Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("peers pair responder"))
	mac.Write(ni)
	mac.Write(nr)
	mac.Write(acceptor[:])
	mac.Write(initiatorSeen[:])
	return mac.Sum(nil)
}

func designInitiatorProof(key, ni, nr []byte, initiator, acceptorSeen Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("peers pair initiator"))
	mac.Write(ni)
	mac.Write(nr)
	mac.Write(initiator[:])
	mac.Write(acceptorSeen[:])
	return mac.Sum(nil)
}

// TestREQBrokerPairing012_InitiatorSeesDifferentAcceptorCertificate checks that
// a different acceptor certificate seen by the initiator fails the responder
// proof and commits neither side.
func TestREQBrokerPairing012_InitiatorSeesDifferentAcceptorCertificate(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "alpha"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)
	otherFP, otherPub := hsKey(t)

	aConn, bConn := hsConnPairFor(initP.identity.fingerprint(), initP.identity.publicKey(), accP.identity.fingerprint(), accP.identity.publicKey())
	aConn.fp = otherFP
	aConn.pub = otherPub
	initTr.raw = aConn
	initTr.afterRun = func(ctx context.Context, raw PairConn) error { return hsSendHello(raw) }

	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()
	_, _, connectErr := initP.Connect(context.Background(), "127.0.0.1:17443", "STATIC-KEY", "")
	assertRejectedReason(t, connectErr, ReasonVerificationFailed)
	if err := awaitAccept(acceptErr); err == nil {
		t.Fatal("Accept succeeded although the initiator saw a different acceptor certificate")
	}
	if got := initP.Peers(); len(got) != 0 {
		t.Fatalf("initiator committed after a mismatched acceptor view: %+v", got)
	}
	if got := accP.Peers(); len(got) != 0 {
		t.Fatalf("acceptor committed after a mismatched acceptor view: %+v", got)
	}
}

// TestRateLimitOrderingOneTimeKeyAfterCounterExhausted checks that a valid
// one-time key is accepted after the per-minute limit is exhausted.
func TestRateLimitOrderingOneTimeKeyAfterCounterExhausted(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if err := hsRequestAttempt(t, accP, "no-such-key", ReasonKeyInvalid); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("exhausting request error = %v", err)
	}

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	initP, tr := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, initP, accP, tr, key); connectErr != nil || acceptErr != nil {
		t.Fatalf("valid one-time key after the attempt limit: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	if !accP.IsPeer(initP.identity.fingerprint()) {
		t.Fatal("the acceptor did not commit a valid one-time key after the attempt limit")
	}
}

// TestRateLimitOrderingStaticKeyHitsAreNotCounted checks that static-key hits
// are not counted and keep working past the per-minute limit.
func TestRateLimitOrderingStaticKeyHitsAreNotCounted(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	const uses = 3
	for i := 0; i < uses; i++ {
		initP, tr := newInitiatorPairing(t, clk)
		if _, connectErr, acceptErr := pairWithHello(t, initP, accP, tr, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
			t.Fatalf("static-key use %d above the attempt limit: Connect=%v Accept=%v", i+1, connectErr, acceptErr)
		}
	}
	if got := len(accP.Peers()); got != uses {
		t.Fatalf("acceptor peers = %d, want %d", got, uses)
	}
	if got := accP.keys.attempts.total; got != 0 {
		t.Fatalf("static-key hits counted %d attempts, want 0", got)
	}
}

// TestRateLimitTryLaterIsNotCounted checks that a try-later rejection is not
// counted as an attempt.
func TestRateLimitTryLaterIsNotCounted(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonKeyInvalid, "198.51.100.5:1000"); !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("first request error = %v", err)
	}
	for i := 0; i < 3; i++ {
		if err := hsRequestAttemptFrom(t, accP, "bad-key", ReasonTryLater, "198.51.100.5:1001"); !errors.Is(err, ErrPairingRejected) {
			t.Fatalf("rate-limited request %d error = %v", i+1, err)
		}
	}
	if got := accP.keys.attempts.total; got != 1 {
		t.Fatalf("attempt total = %d, want 1: try-later rejections were counted", got)
	}
	if got := accP.keys.attempts.bySource["198.51.100.5/32"]; got != 1 {
		t.Fatalf("source attempts = %d, want 1: try-later rejections were counted", got)
	}
}

// TestRateLimitOrderingStaticKeySurvivesVerificationFailures checks that the
// static key still pairs after more verification failures than maxFailures.
func TestRateLimitOrderingStaticKeySurvivesVerificationFailures(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	accS.maxFailures = 2
	accS.maxAttempts = 10
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	for i := 0; i < accS.maxFailures+1; i++ {
		if err := hsBadConfirmAttempt(t, accP, "STATIC-KEY"); !errors.Is(err, ErrPairingRejected) {
			t.Fatalf("bad proof %d error = %v, want a rejection", i+1, err)
		}
	}

	initP, tr := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, initP, accP, tr, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
		t.Fatalf("static key after %d verification failures: Connect=%v Accept=%v", accS.maxFailures+1, connectErr, acceptErr)
	}
	if !accP.IsPeer(initP.identity.fingerprint()) {
		t.Fatal("the static key stopped working after mi verification failures")
	}
}

// TestREQBrokerPairing024_AcceptorOwnAliasConflictAssignsLocalAlias checks that
// a reported alias equal to the acceptor's own gets a distinct local alias and
// still completes.
func TestREQBrokerPairing024_AcceptorOwnAliasConflictAssignsLocalAlias(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "same"
	accS.staticKey = "STATIC-KEY"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	initFP, initPub := hsKey(t)
	aConn, bConn := hsConnPairFor(initFP, initPub, accP.identity.fingerprint(), accP.identity.publicKey())
	if err := hsCompleteAsInitiator(t, accP, aConn, bConn, initFP, "same", "STATIC-KEY"); err != nil {
		t.Fatalf("handshake: %v", err)
	}

	entry, ok := accP.peers.get(initFP)
	if !ok {
		t.Fatal("the acceptor did not commit the peer")
	}
	if entry.alias != "same" {
		t.Fatalf("reported alias = %q, want %q", entry.alias, "same")
	}
	if entry.localAlias == "" {
		t.Fatal("the acceptor assigned no local alias when the reported alias equalled its own")
	}
	if entry.localAlias == "same" {
		t.Fatalf("local alias = %q, want one distinct from the acceptor's own alias", entry.localAlias)
	}
	if got := accP.EffectiveAlias(initFP); got != entry.localAlias {
		t.Fatalf("effective alias = %q, want the local alias %q", got, entry.localAlias)
	}

	var assigned bool
	for _, e := range accS.recorded() {
		if e.Event == config.EventLocalAliasAssigned && e.Peer != nil && e.Peer.Alias == entry.localAlias {
			assigned = true
		}
	}
	if !assigned {
		t.Fatalf("no local_alias_assigned audit event for %q: %+v", entry.localAlias, accS.recorded())
	}
}

func awaitError(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(testWaitBound):
		return errors.New("scripted peer did not finish within the test wait bound")
	}
}

func connectBounded(t *testing.T, p *Pairing, localAlias ...string) (Peer, string, error) {
	t.Helper()
	alias := ""
	if len(localAlias) > 0 {
		alias = localAlias[0]
	}
	ctx, cancel := context.WithTimeout(context.Background(), testWaitBound)
	defer cancel()
	return p.Connect(ctx, "127.0.0.1:17443", "STATIC-KEY", alias)
}

func scriptedAcceptor(conn PairConn, responderFP, initiatorFP Fingerprint, responderAlias, key string, complete bool) <-chan error {
	done := make(chan error, 1)
	go func() {
		// Close on every exit, or an unexpected frame plus a fake clock leaves
		// Connect waiting forever.
		defer func() { _ = conn.Close() }()
		kind, payload, err := receiveWithTimeout(conn)
		if err != nil {
			done <- fmt.Errorf("receive PairRequest: %v", err)
			return
		}
		if kind != kindPairRequest {
			done <- fmt.Errorf("first frame = %d, want PairRequest", kind)
			return
		}
		var request pairingpb.PairRequest
		if err := proto.Unmarshal(payload, &request); err != nil {
			done <- fmt.Errorf("decode PairRequest: %v", err)
			return
		}
		ni := request.GetInitiatorNonce()
		nr := makeSeq(7, nonceLength)
		challenge := &pairingpb.PairChallenge{
			ResponderNonce: nr,
			ResponderAlias: responderAlias,
			Proof:          designResponderProof([]byte(key), ni, nr, responderFP, initiatorFP),
		}
		if err := hsSendProto(conn, kindPairChallenge, challenge); err != nil {
			done <- fmt.Errorf("send PairChallenge: %v", err)
			return
		}

		kind, payload, err = receiveWithTimeout(conn)
		if !complete {
			if err == nil {
				done <- fmt.Errorf("initiator sent frame %d after the challenge, want a close", kind)
				return
			}
			done <- nil
			return
		}
		if err != nil {
			done <- fmt.Errorf("receive PairConfirm: %v", err)
			return
		}
		if kind != kindPairConfirm {
			done <- fmt.Errorf("frame after the challenge = %d, want PairConfirm", kind)
			return
		}
		var confirm pairingpb.PairConfirm
		if err := proto.Unmarshal(payload, &confirm); err != nil {
			done <- fmt.Errorf("decode PairConfirm: %v", err)
			return
		}
		want := designInitiatorProof([]byte(key), ni, nr, initiatorFP, responderFP)
		if !hmac.Equal(want, confirm.GetProof()) {
			done <- fmt.Errorf("initiator proof = %x, want %x", confirm.GetProof(), want)
			return
		}
		if err := hsSendProto(conn, kindPairDone, &pairingpb.PairDone{}); err != nil {
			done <- fmt.Errorf("send PairDone: %v", err)
			return
		}
		kind, _, err = receiveWithTimeout(conn)
		if err != nil {
			done <- fmt.Errorf("receive link Hello: %v", err)
			return
		}
		if kind != kindHello {
			done <- fmt.Errorf("frame after PairDone = %d, want Hello", kind)
			return
		}
		done <- nil
	}()
	return done
}

// TestREQBrokerPairing004_PeerAliasEqualToOwnAliasNeedsLocalAlias checks that a
// peer reporting the initiator's own alias needs a local alias, and uses the
// supplied one.
func TestREQBrokerPairing004_PeerAliasEqualToOwnAliasNeedsLocalAlias(t *testing.T) {
	clk := newHSClock()
	initS := newHSSettings(t)
	initS.alias = "same"
	initTr := &hsTransport{}
	initP := hsNewPairing(t, initS, initTr, clk)
	initFP := initP.identity.fingerprint()
	responderFP, responderPub := hsKey(t)

	aConn, bConn := hsConnPairFor(initFP, initP.identity.publicKey(), responderFP, responderPub)
	initTr.raw = aConn
	responder := scriptedAcceptor(bConn, responderFP, initFP, "same", "STATIC-KEY", false)

	_, _, err := connectBounded(t, initP)
	if !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("Connect error = %v, want ErrNeedsLocalAlias", err)
	}
	if got := initP.Peers(); len(got) != 0 {
		t.Fatalf("initiator committed without a local alias: %+v", got)
	}
	if got := len(initP.peers.reserved); got != 0 {
		t.Fatalf("%d aliases left reserved after the refused attempt", got)
	}
	if err := awaitError(responder); err != nil {
		t.Fatalf("scripted acceptor: %v", err)
	}

	aConn, bConn = hsConnPairFor(initFP, initP.identity.publicKey(), responderFP, responderPub)
	initTr.raw = aConn
	initTr.afterRun = func(ctx context.Context, raw PairConn) error { return hsSendHello(raw) }
	responder = scriptedAcceptor(bConn, responderFP, initFP, "same", "STATIC-KEY", true)

	peer, _, connectErr := connectBounded(t, initP, "same-peer")
	if connectErr != nil {
		t.Fatalf("Connect with a local alias: %v", connectErr)
	}
	if peer.LocalAlias != "same-peer" {
		t.Fatalf("peer local alias = %q, want %q", peer.LocalAlias, "same-peer")
	}
	if got := initP.EffectiveAlias(responderFP); got != "same-peer" {
		t.Fatalf("effective alias = %q, want %q", got, "same-peer")
	}
	if err := awaitError(responder); err != nil {
		t.Fatalf("scripted acceptor: %v", err)
	}
}
