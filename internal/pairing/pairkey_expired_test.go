package pairing

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
	"google.golang.org/protobuf/proto"
)

// TestREQBrokerPairing006_NineMinutesSucceedsElevenMinutesExpired checks that a
// key works at nine minutes and answers KEY_EXPIRED at eleven without changing
// either peer table.
func TestREQBrokerPairing006_NineMinutesSucceedsElevenMinutesExpired(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	firstKey, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	first, firstTr := newInitiatorPairing(t, clk)
	clk.Advance(9 * time.Minute)
	if _, connectErr, acceptErr := pairWithHello(t, first, accP, firstTr, firstKey); connectErr != nil || acceptErr != nil {
		t.Fatalf("pair at nine minutes: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	before := accP.Peers()
	if len(before) != 1 {
		t.Fatalf("peer table after the first pairing = %d entries, want 1", len(before))
	}

	secondKey, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	clk.Advance(11 * time.Minute)
	second, secondTr := newInitiatorPairing(t, clk)
	_, _, connectErr, acceptErr := hsPair(t, second, accP, secondTr, secondKey, "", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	assertRejectedReason(t, connectErr, ReasonKeyExpired)
	var pairErr *PairingError
	if !errors.As(acceptErr, &pairErr) || pairErr.Reason != ReasonKeyExpired {
		t.Fatalf("Accept error = %v, want reason %v", acceptErr, ReasonKeyExpired)
	}
	if len(second.Peers()) != 0 {
		t.Fatalf("initiator table = %+v, want empty", second.Peers())
	}
	after := accP.Peers()
	if len(after) != len(before) || after[0].Fingerprint != before[0].Fingerprint {
		t.Fatalf("acceptor table = %+v, want the previous %+v", after, before)
	}
}

// TestREQBrokerPairing006_ExpiredRecordDroppedAfter24Hours checks that an
// expired-key record answers KEY_EXPIRED until 24 hours after expiry and is
// dropped at the cutoff.
func TestREQBrokerPairing006_ExpiredRecordDroppedAfter24Hours(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	clk.Advance(10*time.Minute + expiredKeyRetention - time.Second)
	_ = hsRequestAttempt(t, accP, key, ReasonKeyExpired)

	clk.Advance(time.Second)
	_ = hsRequestAttempt(t, accP, key, ReasonKeyInvalid)
	if got := len(accP.keys.expired); got != 0 {
		t.Fatalf("expired-key records = %d, want 0 after the cutoff", got)
	}
}

// TestREQBrokerPairing006_UnknownLookupTagStillKeyInvalid checks that an
// unknown lookup tag answers KEY_INVALID even while an expired record is held.
func TestREQBrokerPairing006_UnknownLookupTagStillKeyInvalid(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	if _, _, _, err := accP.CreateOneTimeKey(); err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	clk.Advance(11 * time.Minute)
	if _, _, _, err := accP.CreateOneTimeKey(); err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	if len(accP.keys.keys) != 1 || len(accP.keys.expired) != 1 {
		t.Fatalf("table state: %d unexpired, %d expired, want 1 and 1", len(accP.keys.keys), len(accP.keys.expired))
	}
	_ = hsRequestAttempt(t, accP, "ZZZZ-ZZZZ-ZZZZ-ZZZZ", ReasonKeyInvalid)

	freshS := newHSSettings(t)
	freshS.alias = "beta"
	freshP := hsNewPairing(t, freshS, &hsTransport{}, clk)
	_ = hsRequestAttempt(t, freshP, "ZZZZ-ZZZZ-ZZZZ-ZZZZ", ReasonKeyInvalid)
}

// TestREQBrokerPairing006_ExpiredHitsNotCounted checks that a hit on an expired
// record answers KEY_EXPIRED without counting toward the per-minute limit.
func TestREQBrokerPairing006_ExpiredHitsNotCounted(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.maxAttempts = 1
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	clk.Advance(11 * time.Minute)
	for i := 0; i < 3; i++ {
		_ = hsRequestAttempt(t, accP, key, ReasonKeyExpired)
	}
	if got := accP.keys.attempts.total; got != 0 {
		t.Fatalf("attempt counter = %d after expired hits, want 0", got)
	}

	_ = hsRequestAttempt(t, accP, "WRNG-WRNG-WRNG-WRNG", ReasonKeyInvalid)
	_ = hsRequestAttempt(t, accP, "WRNG-WRNG-WRNG-WRNG", ReasonTryLater)

	// An expired key is still answered with KEY_EXPIRED when the counter is
	// full; a lookup placed after the limit check would answer TRY_LATER.
	_ = hsRequestAttempt(t, accP, key, ReasonKeyExpired)
}

// TestREQBrokerPairing006_UsedAndDiscardedKeysAreNotExpired checks that a key
// removed by use or by the verification-failure limit becomes no expired-key
// record.
func TestREQBrokerPairing006_UsedAndDiscardedKeysAreNotExpired(t *testing.T) {
	t.Run("used", func(t *testing.T) {
		clk := newHSClock()
		accS := newHSSettings(t)
		accS.alias = "beta"
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)

		key, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey: %v", err)
		}
		first, firstTr := newInitiatorPairing(t, clk)
		if _, connectErr, acceptErr := pairWithHello(t, first, accP, firstTr, key); connectErr != nil || acceptErr != nil {
			t.Fatalf("first use: Connect=%v Accept=%v", connectErr, acceptErr)
		}

		clk.Advance(11 * time.Minute)
		_ = hsRequestAttempt(t, accP, key, ReasonKeyInvalid)
		if got := len(accP.keys.expired); got != 0 {
			t.Fatalf("expired-key records = %d for a used key, want 0", got)
		}
	})

	t.Run("verification failure", func(t *testing.T) {
		clk := newHSClock()
		accS := newHSSettings(t)
		accS.alias = "beta"
		accS.maxFailures = 1
		accP := hsNewPairing(t, accS, &hsTransport{}, clk)

		key, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey: %v", err)
		}

		initiatorFP, initiatorPub := hsKey(t)
		aConn, bConn := hsConnPairFor(initiatorFP, initiatorPub, accP.identity.fingerprint(), accP.identity.publicKey())
		acceptErr := make(chan error, 1)
		go func() { acceptErr <- accP.Accept(bConn) }()

		ni := makeSeq(0, nonceLength)
		hsManualChallenge(t, aConn, initiatorFP, ni, key)
		if err := hsSendProto(aConn, kindPairConfirm, &pairingpb.PairConfirm{Proof: make([]byte, proofLength)}); err != nil {
			t.Fatalf("send bad PairConfirm: %v", err)
		}
		if _, _, err := receiveWithTimeout(aConn); err != nil {
			t.Fatalf("receive the reply to PairConfirm: %v", err)
		}
		var pairErr *PairingError
		if err := awaitAccept(acceptErr); !errors.As(err, &pairErr) || pairErr.Reason != ReasonVerificationFailed {
			t.Fatalf("Accept error = %v, want reason %v", err, ReasonVerificationFailed)
		}

		clk.Advance(11 * time.Minute)
		_ = hsRequestAttempt(t, accP, key, ReasonKeyInvalid)
		if got := len(accP.keys.expired); got != 0 {
			t.Fatalf("expired-key records = %d for a discarded key, want 0", got)
		}
	})
}

// TestREQBrokerPairing006_ExpiredRecordLimit checks that expired-key records
// are capped at maxExpiredKeyRecords, dropping the earliest cutoff.
func TestREQBrokerPairing006_ExpiredRecordLimit(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	keys := make([]string, 0, maxExpiredKeyRecords+1)
	for i := 0; i < maxExpiredKeyRecords+1; i++ {
		key, _, _, err := accP.CreateOneTimeKey()
		if err != nil {
			t.Fatalf("CreateOneTimeKey %d: %v", i, err)
		}
		keys = append(keys, key)
		clk.Advance(time.Second)
	}
	clk.Advance(11 * time.Minute)

	_ = hsRequestAttempt(t, accP, keys[0], ReasonKeyInvalid)
	if got := len(accP.keys.expired); got != maxExpiredKeyRecords {
		t.Fatalf("expired-key records = %d, want %d", got, maxExpiredKeyRecords)
	}
	_ = hsRequestAttempt(t, accP, keys[1], ReasonKeyExpired)
	_ = hsRequestAttempt(t, accP, keys[len(keys)-1], ReasonKeyExpired)
}

// TestREQBrokerPairing006_KeyExpiresBetweenRequestAndConfirm checks that a key
// expiring between PairRequest and PairConfirm is rejected as KEY_EXPIRED.
func TestREQBrokerPairing006_KeyExpiresBetweenRequestAndConfirm(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	clk.Advance(10*time.Minute - time.Second)

	initiatorFP, initiatorPub := hsKey(t)
	aConn, bConn := hsConnPairFor(initiatorFP, initiatorPub, accP.identity.fingerprint(), accP.identity.publicKey())
	acceptErr := make(chan error, 1)
	go func() { acceptErr <- accP.Accept(bConn) }()

	ni := makeSeq(0, nonceLength)
	nr := hsManualChallenge(t, aConn, initiatorFP, ni, key)
	clk.Advance(2 * time.Second)

	proof := initiatorProof([]byte(key), ni, nr, initiatorFP, accP.identity.fingerprint())
	if err := hsSendProto(aConn, kindPairConfirm, &pairingpb.PairConfirm{Proof: proof}); err != nil {
		t.Fatalf("send PairConfirm: %v", err)
	}
	kind, payload, err := receiveWithTimeout(aConn)
	if err != nil {
		t.Fatalf("receive the reply to PairConfirm: %v", err)
	}
	if kind != kindPairReject {
		t.Fatalf("frame after PairConfirm = %d, want PairReject", kind)
	}
	var reject pairingpb.PairReject
	if err := proto.Unmarshal(payload, &reject); err != nil {
		t.Fatalf("decode PairReject: %v", err)
	}
	if got, ok := rejectReasonFromPB(reject.GetReason()); !ok || got != ReasonKeyExpired {
		t.Fatalf("reject reason = %v, want %v", reject.GetReason(), ReasonKeyExpired)
	}

	var pairErr *PairingError
	if err := awaitAccept(acceptErr); !errors.As(err, &pairErr) || pairErr.Reason != ReasonKeyExpired {
		t.Fatalf("Accept error = %v, want reason %v", err, ReasonKeyExpired)
	}
	var failed *config.Event
	for _, e := range accS.recorded() {
		if e.Event == config.EventPairingFailed {
			ev := e
			failed = &ev
		}
	}
	if failed == nil || failed.Detail != auditReasonKeyExpired {
		t.Fatalf("last pairing_failed audit = %+v, want detail %q", failed, auditReasonKeyExpired)
	}
	if len(accP.Peers()) != 0 {
		t.Fatalf("acceptor table = %+v, want empty", accP.Peers())
	}
}
