package pairing

// Tests in this file cover rules that the design states as orderings or
// inequalities rather than as observable end results: the proof formula, the
// order of the rate-limit checks, and the two sides of the alias-conflict
// rule. The proof helpers below are transcribed from
// docs/design/modules/design-pairing.md and are deliberately independent of
// the implementation, so a changed formula shows up as a mismatch.

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

// designLookupTagLength is the "first 8 bytes" in the lookup-tag formula.
const designLookupTagLength = 8

// designPairHint computes
// HMAC-SHA256(key, "peers pair hint" || ni || initiator)[:8].
func designPairHint(key, ni []byte, initiator Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("peers pair hint"))
	mac.Write(ni)
	mac.Write(initiator[:])
	return mac.Sum(nil)[:designLookupTagLength]
}

// designResponderProof computes
// HMAC-SHA256(key, "peers pair responder" || ni || nr ||
// acceptor || initiatorSeen).
func designResponderProof(key, ni, nr []byte, acceptor, initiatorSeen Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("peers pair responder"))
	mac.Write(ni)
	mac.Write(nr)
	mac.Write(acceptor[:])
	mac.Write(initiatorSeen[:])
	return mac.Sum(nil)
}

// designInitiatorProof computes
// HMAC-SHA256(key, "peers pair initiator" || ni || nr ||
// initiator || acceptorSeen).
func designInitiatorProof(key, ni, nr []byte, initiator, acceptorSeen Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte("peers pair initiator"))
	mac.Write(ni)
	mac.Write(nr)
	mac.Write(initiator[:])
	mac.Write(acceptorSeen[:])
	return mac.Sum(nil)
}

// --- the other direction of the man in the middle --------------------------

// TestREQBrokerPairing012_InitiatorSeesDifferentAcceptorCertificate covers the
// direction the existing mismatched-view tests do not: the initiator's TLS view
// reports a certificate that is not the real acceptor's. The acceptor still
// proves with its real fingerprint, so the responder proof cannot verify. With
// the acceptor fingerprint removed from both proof formulas the two views
// become indistinguishable and this pairing wrongly succeeds.
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
	// The initiator sees a different but self-consistent acceptor identity.
	// The acceptor's own view of the connection is unchanged.
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

// --- rate-limit ordering ---------------------------------------------------

// A valid one-time key is looked up before the per-minute attempt limit is
// consulted, so a minute whose counter is already exhausted still accepts it.
func TestRateLimitOrderingOneTimeKeyAfterCounterExhausted(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	// Exhaust the allowance with one request that matches no key.
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

// Static-key hits are not counted as failed attempts, so the key keeps working
// past the per-minute limit.
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

// A "try later" rejection is not itself an attempt. Counting it would let one
// source exhaust the global allowance by repeating requests the limit already
// refuses.
func TestRateLimitTryLaterIsNotCounted(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	// One counted request fills the only per-source slot.
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

// Failed mi verifications are counted against a one-time key only. The static
// key stays usable after failures exceed maxFailures.
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

// --- alias conflicts at the handshake level --------------------------------

// When the initiator's self-reported alias equals the acceptor's own alias, the
// acceptor must assign a local alias and still complete the pairing
// (REQ-broker-pairing-024).
func TestREQBrokerPairing024_AcceptorOwnAliasConflictAssignsLocalAlias(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "same"
	accS.staticKey = "STATIC-KEY"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	initFP, initPub := hsKey(t)
	aConn, bConn := hsConnPairFor(initFP, initPub, accP.identity.fingerprint(), accP.identity.publicKey())
	// Drive the initiator side by hand so its reported alias is exactly the
	// acceptor's own alias, independently of the fake initiator's own settings.
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

// awaitError waits for a scripted peer to finish, bounding the wait the same
// way awaitAccept bounds Accept.
func awaitError(ch <-chan error) error {
	select {
	case err := <-ch:
		return err
	case <-time.After(testWaitBound):
		return errors.New("scripted peer did not finish within the test wait bound")
	}
}

// connectBounded calls Connect with a real-time bound, so a scripted peer that
// stops responding turns into an assertion failure rather than a hung test.
// The static key is the key these scripted tests use.
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

// scriptedAcceptor plays the responder side of one handshake with a chosen
// self-reported alias, proving with the design formula. With complete=false it
// expects the initiator to close the connection right after the challenge, the
// reaction to needing a local alias. With complete=true it verifies the
// initiator proof, sends PairDone and consumes the link Hello.
func scriptedAcceptor(conn PairConn, responderFP, initiatorFP Fingerprint, responderAlias, key string, complete bool) <-chan error {
	done := make(chan error, 1)
	go func() {
		// Closing on every exit unblocks the initiator if this script sees a
		// frame it did not expect; an open connection plus a fake clock would
		// otherwise leave Connect waiting forever.
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

// The initiator-side counterpart: when the peer reports the initiator's own
// alias, Connect must ask for a local alias rather than pair, and the supplied
// local alias must be the effective alias. A scripted acceptor is used so the
// peer's reported alias can differ from its own settings while still matching
// the initiator's alias.
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

	// The same challenge with a supplied local alias completes under it. The
	// link Hello is the frame the transport sends after Connect's run returns.
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
