package pairing

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

func setStaticKey(s *hsSettings, key string) {
	s.mu.Lock()
	s.staticKey = key
	s.mu.Unlock()
}

func setAdvertised(s *hsSettings, addrs ...config.AdvertisedAddr) {
	s.mu.Lock()
	s.advertised = append([]config.AdvertisedAddr(nil), addrs...)
	s.mu.Unlock()
}

func pairWithHello(t *testing.T, initP, accP *Pairing, tr *hsTransport, key string) (Peer, error, error) {
	t.Helper()
	peer, _, connectErr, acceptErr := hsPair(t, initP, accP, tr, key, "", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	return peer, connectErr, acceptErr
}

func newInitiatorPairing(t *testing.T, clk *hsClock) (*Pairing, *hsTransport) {
	t.Helper()
	s := newHSSettings(t)
	s.alias = "alpha"
	tr := &hsTransport{}
	return hsNewPairing(t, s, tr, clk), tr
}

func validKeyShape(key string) bool {
	if len(key) != 19 {
		return false
	}
	groups := strings.Split(key, "-")
	if len(groups) != 4 {
		return false
	}
	for _, group := range groups {
		if len(group) != 4 {
			return false
		}
		for _, r := range group {
			if !strings.ContainsRune(crockfordAlphabet, r) {
				return false
			}
		}
	}
	return true
}

// TestOneTimeKeyEncodingZeroVector checks that ten zero bytes encode as
// 0000-0000-0000-0000.
func TestOneTimeKeyEncodingZeroVector(t *testing.T) {
	if got := encodeOneTimeKey([10]byte{}); got != "0000-0000-0000-0000" {
		t.Fatalf("encodeOneTimeKey(zero) = %q, want %q", got, "0000-0000-0000-0000")
	}
}

// TestOneTimeKeyEncodingFormat checks that encoding produces the four-group
// Crockford form deterministically.
func TestOneTimeKeyEncodingFormat(t *testing.T) {
	var ones [10]byte
	for i := range ones {
		ones[i] = 0xff
	}
	if got := encodeOneTimeKey(ones); got != "ZZZZ-ZZZZ-ZZZZ-ZZZZ" {
		t.Fatalf("encodeOneTimeKey(all ones) = %q, want %q", got, "ZZZZ-ZZZZ-ZZZZ-ZZZZ")
	}

	var mixed [10]byte
	for i := range mixed {
		mixed[i] = byte(i*17 + 3)
	}
	got := encodeOneTimeKey(mixed)
	if !validKeyShape(got) {
		t.Fatalf("encodeOneTimeKey(%x) = %q, not four groups of four Crockford chars", mixed, got)
	}
	if again := encodeOneTimeKey(mixed); again != got {
		t.Fatalf("encoding is not deterministic: %q then %q", got, again)
	}
	var other [10]byte
	other[9] = 1
	if encodeOneTimeKey(other) == got {
		t.Fatalf("distinct inputs encoded to the same key %q", got)
	}
}

// TestREQBrokerPairing005_CreateOneTimeKeyReturnsKeyAndExpiry checks that a
// created key has the one-time format, expires in ten minutes and is stored
// once.
func TestREQBrokerPairing005_CreateOneTimeKeyReturnsKeyAndExpiry(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	p := hsNewPairing(t, s, &hsTransport{}, clk)
	now := clk.Now()

	key, expires, shown, err := p.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	if !validKeyShape(key) {
		t.Fatalf("key = %q, want four groups of four Crockford Base32 characters", key)
	}
	if want := now.Add(10 * time.Minute); !expires.Equal(want) {
		t.Fatalf("expires = %v, want %v", expires, want)
	}
	if len(shown) != 0 {
		t.Fatalf("shown = %v, want none for an empty advertised list", shown)
	}
	if got := len(p.keys.keys); got != 1 || p.keys.keys[0].key != key {
		t.Fatalf("key table = %+v, want one entry for %q", p.keys.keys, key)
	}
}

// TestREQBrokerPairing005_KeyWorksForConnect checks that a one-time key
// completes a handshake, commits both sides and is consumed.
func TestREQBrokerPairing005_KeyWorksForConnect(t *testing.T) {
	clk := newHSClock()
	initP, tr := newInitiatorPairing(t, clk)
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	peer, connectErr, acceptErr := pairWithHello(t, initP, accP, tr, key)
	if connectErr != nil {
		t.Fatalf("Connect: %v", connectErr)
	}
	if acceptErr != nil {
		t.Fatalf("Accept: %v", acceptErr)
	}
	if peer.Fingerprint != accP.identity.fingerprint() {
		t.Fatalf("peer fingerprint = %s, want the acceptor", peer.Fingerprint)
	}
	if !accP.IsPeer(initP.identity.fingerprint()) || !initP.IsPeer(accP.identity.fingerprint()) {
		t.Fatal("a successful one-time-key handshake did not commit both sides")
	}
	if got := len(accP.keys.keys); got != 0 {
		t.Fatalf("%d one-time keys survived a successful use", got)
	}
}

// TestREQBrokerPairing006_KeyWorksAtNineMinutes checks that a key still works
// nine minutes after creation.
func TestREQBrokerPairing006_KeyWorksAtNineMinutes(t *testing.T) {
	clk := newHSClock()
	initP, tr := newInitiatorPairing(t, clk)
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, expires, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	clk.Advance(9 * time.Minute)
	if !clk.Now().Before(expires) {
		t.Fatalf("test clock %v is not before the expiry %v", clk.Now(), expires)
	}
	_, connectErr, acceptErr := pairWithHello(t, initP, accP, tr, key)
	if connectErr != nil {
		t.Fatalf("Connect at nine minutes: %v", connectErr)
	}
	if acceptErr != nil {
		t.Fatalf("Accept at nine minutes: %v", acceptErr)
	}
}

// TestREQBrokerPairing006_KeyExpiredAtExactlyTenMinutes checks that a key is
// already expired at the exact ten-minute instant.
func TestREQBrokerPairing006_KeyExpiredAtExactlyTenMinutes(t *testing.T) {
	clk := newHSClock()
	initP, tr := newInitiatorPairing(t, clk)
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	clk.Advance(10 * time.Minute)

	_, _, connectErr, acceptErr := hsPair(t, initP, accP, tr, key, "", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	assertRejectedReason(t, connectErr, ReasonKeyExpired)
	if !strings.Contains(connectErr.Error(), "key expired") {
		t.Fatalf("Connect error = %q, want it to mention key expired", connectErr.Error())
	}
	if acceptErr == nil {
		t.Fatal("Accept succeeded with a key at its exact expiry")
	}
	if len(initP.Peers()) != 0 || len(accP.Peers()) != 0 {
		t.Fatalf("peer tables changed at exact expiry: initiator=%+v acceptor=%+v", initP.Peers(), accP.Peers())
	}
	if got := len(accP.keys.keys); got != 0 {
		t.Fatalf("expired key remains in the table: %d entries", got)
	}
	if got := len(accP.keys.expired); got != 1 {
		t.Fatalf("expired-key records = %d, want 1", got)
	}
}

// TestREQBrokerPairing006_KeyExpiredAtElevenMinutes checks that a key is
// rejected as expired eleven minutes after creation.
func TestREQBrokerPairing006_KeyExpiredAtElevenMinutes(t *testing.T) {
	clk := newHSClock()
	initP, tr := newInitiatorPairing(t, clk)
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	clk.Advance(11 * time.Minute)

	_, _, connectErr, acceptErr := hsPair(t, initP, accP, tr, key, "", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	assertRejectedReason(t, connectErr, ReasonKeyExpired)
	if !strings.Contains(connectErr.Error(), "key expired") {
		t.Fatalf("Connect error = %q, want it to mention key expired", connectErr.Error())
	}
	if acceptErr == nil {
		t.Fatal("Accept succeeded with an expired one-time key")
	}
	if len(initP.Peers()) != 0 || len(accP.Peers()) != 0 {
		t.Fatalf("peer tables changed on an expired key: initiator=%+v acceptor=%+v", initP.Peers(), accP.Peers())
	}
	if got := len(accP.keys.keys); got != 0 {
		t.Fatalf("expired key remains in the table: %d entries", got)
	}
	if got := len(accP.keys.expired); got != 1 {
		t.Fatalf("expired-key records = %d, want 1", got)
	}
}

// TestREQBrokerPairing007_KeySucceedsOnceThenInvalid checks that a one-time key
// works once and its second use is rejected as invalid.
func TestREQBrokerPairing007_KeySucceedsOnceThenInvalid(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	key, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}

	firstInit, firstTr := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, firstInit, accP, firstTr, key); connectErr != nil || acceptErr != nil {
		t.Fatalf("first use: Connect=%v Accept=%v", connectErr, acceptErr)
	}

	secondInit, secondTr := newInitiatorPairing(t, clk)
	_, _, connectErr, acceptErr := hsPair(t, secondInit, accP, secondTr, key, "", func(ctx context.Context, raw PairConn) error {
		return hsSendHello(raw)
	})
	assertRejectedReason(t, connectErr, ReasonKeyInvalid)
	if acceptErr == nil {
		t.Fatal("Accept accepted a one-time key for a second use")
	}
	if accP.IsPeer(secondInit.identity.fingerprint()) {
		t.Fatal("the acceptor committed the second use of a one-time key")
	}
	if !accP.IsPeer(firstInit.identity.fingerprint()) {
		t.Fatal("the first successful pairing was lost")
	}
}

// TestREQBrokerPairing008_NoStaticKeyRejectsArbitraryKey checks that an
// arbitrary key is rejected when no static key is configured.
func TestREQBrokerPairing008_NoStaticKeyRejectsArbitraryKey(t *testing.T) {
	clk := newHSClock()
	initP, tr := newInitiatorPairing(t, clk)
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = ""
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	_, _, connectErr, acceptErr := hsPair(t, initP, accP, tr, "ARBITRARY-KEY", "", nil)
	assertRejectedReason(t, connectErr, ReasonKeyInvalid)
	if acceptErr == nil {
		t.Fatal("Accept accepted an arbitrary key without a static key configured")
	}
	if len(initP.Peers()) != 0 || len(accP.Peers()) != 0 {
		t.Fatalf("peer tables changed on a rejected key: initiator=%+v acceptor=%+v", initP.Peers(), accP.Peers())
	}
}

// TestREQBrokerPairing009_StaticKeyRepeatedUses checks that the static key can
// pair repeatedly.
func TestREQBrokerPairing009_StaticKeyRepeatedUses(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	for i := 0; i < 2; i++ {
		initP, tr := newInitiatorPairing(t, clk)
		if _, connectErr, acceptErr := pairWithHello(t, initP, accP, tr, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
			t.Fatalf("use %d: Connect=%v Accept=%v", i, connectErr, acceptErr)
		}
	}
	if got := len(accP.Peers()); got != 2 {
		t.Fatalf("acceptor peers = %d, want 2 after two static-key uses", got)
	}
}

// TestREQBrokerPairing009_StaticKeyNotExpired checks that the static key still
// works after eleven minutes.
func TestREQBrokerPairing009_StaticKeyNotExpired(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "STATIC-KEY"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	init1, tr1 := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, init1, accP, tr1, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
		t.Fatalf("first use: Connect=%v Accept=%v", connectErr, acceptErr)
	}

	clk.Advance(11 * time.Minute)

	init2, tr2 := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, init2, accP, tr2, "STATIC-KEY"); connectErr != nil || acceptErr != nil {
		t.Fatalf("use after eleven minutes: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	if !accP.IsPeer(init2.identity.fingerprint()) {
		t.Fatal("the static key stopped working after eleven minutes")
	}
}

// TestLookupAtRequestReadsStaticKeyOutsideTableLock checks that the static key
// is read without holding the key table lock.
func TestLookupAtRequestReadsStaticKeyOutsideTableLock(t *testing.T) {
	ni := makeSeq(1, nonceLength)
	fp, _ := hsKey(t)
	const key = "STATIC-KEY"
	now := time.Date(2026, 11, 18, 12, 0, 30, 0, time.UTC)

	t.Run("matching static key", func(t *testing.T) {
		table := &keyTable{}
		heldWhileReading := false
		staticKey := func() string {
			if table.mu.TryLock() {
				table.mu.Unlock()
			} else {
				heldWhileReading = true
			}
			return key
		}

		match, found, err := table.lookupAtRequest(ni, computeHint([]byte(key), ni, fp), fp, now, "192.0.2.1/32", 10, staticKey)
		if err != nil || !found || match.key != key {
			t.Fatalf("lookup = (%+v, found=%v, err=%v), want the static key", match, found, err)
		}
		if heldWhileReading {
			t.Fatal("lookupAtRequest read the static key while holding keys.mu")
		}
		if table.attempts.total != 0 || len(table.attempts.bySource) != 0 {
			t.Fatalf("a static-key match was counted: total=%d sources=%v", table.attempts.total, table.attempts.bySource)
		}
	})

	t.Run("unmatched key", func(t *testing.T) {
		table := &keyTable{}
		heldWhileReading := false
		staticKey := func() string {
			if table.mu.TryLock() {
				table.mu.Unlock()
			} else {
				heldWhileReading = true
			}
			return key
		}

		_, found, err := table.lookupAtRequest(ni, computeHint([]byte("OTHER-KEY"), ni, fp), fp, now, "192.0.2.1/32", 10, staticKey)
		if found || err == nil {
			t.Fatalf("lookup = (found=%v, err=%v), want a rejection", found, err)
		}
		if heldWhileReading {
			t.Fatal("lookupAtRequest read the static key while holding keys.mu")
		}
		if table.attempts.total != 1 || table.attempts.bySource["192.0.2.1/32"] != 1 {
			t.Fatalf("an unmatched request was not counted once: total=%d sources=%v", table.attempts.total, table.attempts.bySource)
		}
	})
}

// TestUndoAttemptIgnoresARolledWindow checks that undoing a tentative attempt
// leaves a later minute's window untouched.
func TestUndoAttemptIgnoresARolledWindow(t *testing.T) {
	ni := makeSeq(2, nonceLength)
	fp, _ := hsKey(t)
	const key = "STATIC-KEY"
	const source = "192.0.2.1/32"
	const other = "198.51.100.7/32"
	now := time.Date(2026, 11, 18, 12, 0, 30, 0, time.UTC)
	next := now.Truncate(time.Minute).Add(time.Minute)

	table := &keyTable{}
	staticKey := func() string {
		table.mu.Lock()
		table.attempts = attemptWindow{minute: next, total: 1, bySource: map[string]int{other: 1}}
		table.mu.Unlock()
		return key
	}

	match, found, err := table.lookupAtRequest(ni, computeHint([]byte(key), ni, fp), fp, now, source, 10, staticKey)
	if err != nil || !found || match.key != key {
		t.Fatalf("lookup = (%+v, found=%v, err=%v), want the static key", match, found, err)
	}

	table.mu.Lock()
	minute := table.attempts.minute
	total := table.attempts.total
	theOther := table.attempts.bySource[other]
	sources := len(table.attempts.bySource)
	table.mu.Unlock()
	if !minute.Equal(next) {
		t.Fatalf("window minute = %v, want %v", minute, next)
	}
	if total != 1 || theOther != 1 || sources != 1 {
		t.Fatalf("the undo changed the next minute's window: total=%d other=%d sources=%d", total, theOther, sources)
	}
}

// TestREQBrokerPairing025_OnlyOneTimeKeysAreGenerated checks that generated
// keys differ, use the one-time format and never equal the static key.
func TestREQBrokerPairing025_OnlyOneTimeKeysAreGenerated(t *testing.T) {
	clk := newHSClock()
	const static = "STATIC-KEY-UNIQUE-SENTINEL"
	accS := newHSSettings(t)
	accS.staticKey = static
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	first, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("first CreateOneTimeKey: %v", err)
	}
	second, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("second CreateOneTimeKey: %v", err)
	}
	if first == second {
		t.Fatalf("two generations produced the same key %q", first)
	}
	for _, key := range []string{first, second} {
		if !validKeyShape(key) {
			t.Fatalf("generated key %q is not in the one-time format", key)
		}
		if key == static {
			t.Fatalf("generated key equals the preset static key")
		}
	}
	if got := len(accP.keys.keys); got != 2 {
		t.Fatalf("key table has %d entries, want 2 one-time keys", got)
	}
}

// TestREQBrokerPairing025_NoReusableKeyOnDisk checks that neither the static
// key nor a generated key is written into the data directory.
func TestREQBrokerPairing025_NoReusableKeyOnDisk(t *testing.T) {
	clk := newHSClock()
	const static = "STATIC-SECRET-7Q4Z-DO-NOT-PERSIST"
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = static
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	oneTime, _, _, err := accP.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}

	initP, tr := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, initP, accP, tr, static); connectErr != nil || acceptErr != nil {
		t.Fatalf("static-key handshake: Connect=%v Accept=%v", connectErr, acceptErr)
	}

	assertDirDoesNotContain(t, accS.dataDir, static)
	assertDirDoesNotContain(t, accS.dataDir, oneTime)
}

func assertDirDoesNotContain(t *testing.T, dir, needle string) {
	t.Helper()
	err := filepath.WalkDir(dir, func(path string, entry os.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() {
			return nil
		}
		data, readErr := os.ReadFile(path)
		if readErr != nil {
			return readErr
		}
		if bytes.Contains(data, []byte(needle)) {
			return fmt.Errorf("%s contains %q", path, needle)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// TestREQBrokerPairing028_ShownAddressesOrderAndFormat checks that shown
// addresses are the direct ones, in order, in hostname(address:port) form.
func TestREQBrokerPairing028_ShownAddressesOrderAndFormat(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	s.hostname = "nodeA"
	setAdvertised(s,
		direct("192.0.2.20:17443"),
		config.AdvertisedAddr{Kind: "relay", Addr: "relay.example:17443"},
		direct(""),
		direct("[2001:db8::1]:17443"),
	)
	p := hsNewPairing(t, s, &hsTransport{}, clk)

	key, _, shown, err := p.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	want := []string{
		"nodeA(192.0.2.20:17443)",
		"nodeA([2001:db8::1]:17443)",
	}
	if len(shown) != len(want) {
		t.Fatalf("shown = %v, want %v", shown, want)
	}
	for i := range want {
		if shown[i] != want[i] {
			t.Fatalf("shown[%d] = %q, want %q", i, shown[i], want[i])
		}
	}
	if strings.Contains(key, "192.0.2.20") || strings.Contains(key, "2001:db8") {
		t.Fatalf("key %q embeds an address", key)
	}
}

// TestREQBrokerPairing028_EmptyAdvertisedWhileListening checks that a key is
// still created with an empty shown list when listening with no advertised
// address.
func TestREQBrokerPairing028_EmptyAdvertisedWhileListening(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	s.listen = "127.0.0.1:17443"
	setAdvertised(s)
	p := hsNewPairing(t, s, &hsTransport{}, clk)

	key, expires, shown, err := p.CreateOneTimeKey()
	if err != nil {
		t.Fatalf("CreateOneTimeKey: %v", err)
	}
	if key == "" || expires.IsZero() {
		t.Fatalf("key=%q expires=%v, want a generated key while listening", key, expires)
	}
	if len(shown) != 0 {
		t.Fatalf("shown = %v, want an empty list", shown)
	}
}

// TestREQBrokerPairing032_DeletedStaticKeyStopsNextHandshake checks that
// deleting the static key rejects the next handshake without removing an
// existing peer.
func TestREQBrokerPairing032_DeletedStaticKeyStopsNextHandshake(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "OLD-STATIC"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	first, firstTr := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, first, accP, firstTr, "OLD-STATIC"); connectErr != nil || acceptErr != nil {
		t.Fatalf("first handshake: Connect=%v Accept=%v", connectErr, acceptErr)
	}

	setStaticKey(accS, "")

	second, secondTr := newInitiatorPairing(t, clk)
	_, _, connectErr, acceptErr := hsPair(t, second, accP, secondTr, "OLD-STATIC", "", nil)
	assertRejectedReason(t, connectErr, ReasonKeyInvalid)
	if acceptErr == nil {
		t.Fatal("Accept succeeded after the static key was deleted")
	}
	if accP.IsPeer(second.identity.fingerprint()) {
		t.Fatal("the acceptor committed a handshake that used the deleted key")
	}
	if !accP.IsPeer(first.identity.fingerprint()) {
		t.Fatal("deleting the static key removed an existing peer")
	}
}

// TestREQBrokerPairing033_RotatedStaticKeyAcceptsOnlyNewValue checks that after
// rotation the old value is rejected and the new one pairs.
func TestREQBrokerPairing033_RotatedStaticKeyAcceptsOnlyNewValue(t *testing.T) {
	clk := newHSClock()
	accS := newHSSettings(t)
	accS.alias = "beta"
	accS.staticKey = "OLD-STATIC"
	accP := hsNewPairing(t, accS, &hsTransport{}, clk)

	first, firstTr := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, first, accP, firstTr, "OLD-STATIC"); connectErr != nil || acceptErr != nil {
		t.Fatalf("handshake with the old key: Connect=%v Accept=%v", connectErr, acceptErr)
	}

	setStaticKey(accS, "NEW-STATIC")

	stale, staleTr := newInitiatorPairing(t, clk)
	_, _, connectErr, acceptErr := hsPair(t, stale, accP, staleTr, "OLD-STATIC", "", nil)
	assertRejectedReason(t, connectErr, ReasonKeyInvalid)
	if acceptErr == nil {
		t.Fatal("Accept accepted the rotated-away static key")
	}
	if accP.IsPeer(stale.identity.fingerprint()) {
		t.Fatal("the acceptor committed a handshake that used the old key")
	}
	if !accP.IsPeer(first.identity.fingerprint()) {
		t.Fatal("rotating the static key removed an existing peer")
	}

	fresh, freshTr := newInitiatorPairing(t, clk)
	if _, connectErr, acceptErr := pairWithHello(t, fresh, accP, freshTr, "NEW-STATIC"); connectErr != nil || acceptErr != nil {
		t.Fatalf("handshake with the new key: Connect=%v Accept=%v", connectErr, acceptErr)
	}
	if !accP.IsPeer(fresh.identity.fingerprint()) {
		t.Fatal("the acceptor did not commit a handshake with the new key")
	}
}

// TestREQBrokerPairing036_CreateKeyFailsWhenNotListening checks that
// CreateOneTimeKey returns ErrNotListening and generates no key when the listen
// address is empty.
func TestREQBrokerPairing036_CreateKeyFailsWhenNotListening(t *testing.T) {
	clk := newHSClock()
	s := newHSSettings(t)
	s.listen = ""
	p := hsNewPairing(t, s, &hsTransport{}, clk)

	key, expires, shown, err := p.CreateOneTimeKey()
	if !errors.Is(err, ErrNotListening) {
		t.Fatalf("CreateOneTimeKey error = %v, want ErrNotListening", err)
	}
	if key != "" || !expires.IsZero() || shown != nil {
		t.Fatalf("CreateOneTimeKey returned key=%q expires=%v shown=%v on failure", key, expires, shown)
	}
	if got := len(p.keys.keys); got != 0 {
		t.Fatalf("%d keys were generated while not listening", got)
	}
}

func assertRejectedReason(t *testing.T, err error, want RejectReason) {
	t.Helper()
	if !errors.Is(err, ErrPairingRejected) {
		t.Fatalf("error = %v, want a pairing rejection", err)
	}
	var pairErr *PairingError
	if !errors.As(err, &pairErr) {
		t.Fatalf("error = %v, want a *PairingError", err)
	}
	if pairErr.Reason != want {
		t.Fatalf("reject reason = %v, want %v", pairErr.Reason, want)
	}
}
