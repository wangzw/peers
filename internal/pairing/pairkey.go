package pairing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	// oneTimeKeyLifetime is how long a generated pairing key stays usable. It is
	// a constant, not a configuration value.
	oneTimeKeyLifetime = 10 * time.Minute
	// oneTimeKeyBytes is the size of the random value behind a one-time key:
	// 80 bits, encoded as 16 Crockford Base32 characters.
	oneTimeKeyBytes = 10
	// crockfordAlphabet is the Crockford Base32 alphabet used for one-time keys.
	crockfordAlphabet = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	// The HMAC domain-separation labels of the pairing protocol. Changing one
	// makes pairing fail against a Broker that still uses the old value, so
	// they are kept together. None has a NUL terminator.
	pairHintLabel      = "peers pair hint"
	pairResponderLabel = "peers pair responder"
	pairInitiatorLabel = "peers pair initiator"
	// expiredKeyRetention is how long an expired one-time key is remembered so
	// the acceptor can answer KEY_EXPIRED instead of KEY_INVALID.
	expiredKeyRetention = 24 * time.Hour
	// maxExpiredKeyRecords caps the expired-key records; the records with the
	// earliest cutoff are dropped first.
	maxExpiredKeyRecords = 64
)

// expiredKey remembers a one-time key that expired by time. key is kept only
// to recompute lookup tags; it never verifies a proof or completes a
// handshake.
type expiredKey struct {
	key   string
	until time.Time // expiry + expiredKeyRetention
}

// oneTimeKey is one unexpired key and its verification failure count.
type oneTimeKey struct {
	key      string
	expires  time.Time
	failures int
}

// keyMatch is the key selected for one handshake. oneTime distinguishes a
// one-time key from the preset static key, and proof carries the initiator's
// PairConfirm proof until verification.
type keyMatch struct {
	key     string
	oneTime bool
	index   int
	proof   []byte
}

// attemptGlobalFactor multiplies the per-source allowance into the cap on all
// sources together. It is a constant, not a configuration value.
const attemptGlobalFactor = 10

// attemptWindow counts, for one minute, pairing requests that matched neither
// a one-time key nor the preset static key: per source and in total. The map
// gains an entry only when a request is counted, and the whole window is
// replaced when the minute changes, so it never holds more entries than the
// global cap.
type attemptWindow struct {
	minute   time.Time
	total    int
	bySource map[string]int
}

// keyTable holds the one-time keys, the expired-key records and the per-minute
// attempt counters. One mutex covers all three. Verification and removal of a
// one-time key happen under the same lock, so two concurrent handshakes cannot
// both consume one key.
type keyTable struct {
	mu       sync.Mutex
	keys     []oneTimeKey
	expired  []expiredKey
	attempts attemptWindow
}

// encodeOneTimeKey renders 10 random bytes as four groups of four Crockford
// Base32 characters separated by '-'. Ten zero bytes encode as
// "0000-0000-0000-0000".
func encodeOneTimeKey(raw [10]byte) string {
	var chars [16]byte
	for i := range chars {
		var v byte
		for b := 0; b < 5; b++ {
			bit := i*5 + b
			v = v<<1 | (raw[bit/8]>>uint(7-bit%8))&1
		}
		chars[i] = crockfordAlphabet[v]
	}
	out := make([]byte, 0, 19)
	for group := 0; group < 4; group++ {
		if group > 0 {
			out = append(out, '-')
		}
		out = append(out, chars[group*4:(group+1)*4]...)
	}
	return string(out)
}

// computeHint is the lookup tag the initiator sends in PairRequest. It lets the
// acceptor find the key without revealing it, and has no NUL terminator. The
// tag is bound to the initiator's fingerprint: the acceptor recomputes it with
// the fingerprint it reads from this TLS connection, so a third party that
// forwards someone else's tag presents its own certificate and matches no key.
func computeHint(key, ni []byte, initiator Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(pairHintLabel))
	mac.Write(ni)
	mac.Write(initiator[:])
	return mac.Sum(nil)[:lookupTagLength]
}

// add stores a new one-time key that expires oneTimeKeyLifetime from now, after
// dropping keys that have already expired.
func (k *keyTable) add(key string, now time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.expireLocked(now)
	k.keys = append(k.keys, oneTimeKey{key: key, expires: now.Add(oneTimeKeyLifetime)})
}

// expireLocked moves keys whose lifetime has ended into the expired-key
// records, drops records past their cutoff, and caps the records at
// maxExpiredKeyRecords by dropping the earliest cutoff first. The caller must
// hold k.mu.
func (k *keyTable) expireLocked(now time.Time) {
	kept := k.keys[:0]
	for _, key := range k.keys {
		if now.Before(key.expires) {
			kept = append(kept, key)
		} else {
			k.expired = append(k.expired, expiredKey{key: key.key, until: key.expires.Add(expiredKeyRetention)})
		}
	}
	k.keys = kept

	live := k.expired[:0]
	for _, e := range k.expired {
		if now.Before(e.until) {
			live = append(live, e)
		}
	}
	k.expired = live

	if len(k.expired) > maxExpiredKeyRecords {
		sort.SliceStable(k.expired, func(i, j int) bool {
			return k.expired[i].until.Before(k.expired[j].until)
		})
		k.expired = k.expired[len(k.expired)-maxExpiredKeyRecords:]
	}
}

// lookupAtRequest selects the key for an incoming PairRequest. It first looks
// for a matching one-time key, then the expired-key records, then checks the
// per-minute attempt limits, then the preset static key. staticKey is called
// only after both limits pass, and never while keys.mu is held: it reads
// config, and pairing must not take a config lock under a pairing lock. A
// request that matches a one-time key, hits an expired record or is refused by a
// limit never reads it. A request that matches nothing increments the source's
// counter and the total; the count is taken before the static key is read and
// undone when the static key matches. found is false on rejection; the returned
// error is a PairingError whose reason maps to the PairReject reason.
//
// initiator is the fingerprint the acceptor sees on this connection; every
// lookup tag is recomputed with it, so a tag computed by a different party
// matches no key.
//
// source is the network prefix from sourcePrefix; every address without an IP
// shares the empty string as one unknown source. A source may make
// maxAttempts unmatched requests per minute and all sources together may make
// attemptGlobalFactor times that.
//
// A hit on an expired-key record answers KEY_EXPIRED and does not touch the
// counters: the caller holds that key, and the counters exist to stop guessing.
//
// The per-key failure limit is not consulted here: recordVerificationFailure
// already discards a one-time key once its failures reach that limit, so any key
// still in the table is eligible.
func (k *keyTable) lookupAtRequest(ni, hint []byte, initiator Fingerprint, now time.Time, source string, maxAttempts int, staticKey func() string) (keyMatch, bool, error) {
	k.mu.Lock()

	k.expireLocked(now)
	for i := range k.keys {
		if hmac.Equal(computeHint([]byte(k.keys[i].key), ni, initiator), hint) {
			key := k.keys[i].key
			k.mu.Unlock()
			return keyMatch{key: key, oneTime: true, index: i}, true, nil
		}
	}

	for _, e := range k.expired {
		if hmac.Equal(computeHint([]byte(e.key), ni, initiator), hint) {
			k.mu.Unlock()
			return keyMatch{}, false, &PairingError{Reason: ReasonKeyExpired, Detail: rejectDetail(ReasonKeyExpired)}
		}
	}

	// Reset the window when the minute changes before consulting the limits;
	// otherwise a minute that reached a limit would reject forever.
	minute := now.Truncate(time.Minute)
	if !k.attempts.minute.Equal(minute) {
		k.attempts = attemptWindow{minute: minute}
	}

	if k.attempts.bySource[source] >= maxAttempts || k.attempts.total >= maxAttempts*attemptGlobalFactor {
		k.mu.Unlock()
		return keyMatch{}, false, &PairingError{Reason: ReasonTryLater, Detail: rejectDetail(ReasonTryLater)}
	}

	// Record the attempt before releasing the lock, so the limit check and the
	// count happen in one critical section and two concurrent requests cannot
	// both decide they are within the allowance. A static-key match below undoes
	// this record, because a matched request is not an attempt.
	if k.attempts.bySource == nil {
		k.attempts.bySource = make(map[string]int)
	}
	k.attempts.bySource[source]++
	k.attempts.total++
	k.mu.Unlock()

	// staticKey reads config's secret, so it must run outside keys.mu: config's
	// validators take the peer table lock while holding config's lock, and
	// nesting keys.mu before config's lock would invert that order.
	key := staticKey()
	if key != "" && hmac.Equal(computeHint([]byte(key), ni, initiator), hint) {
		k.undoAttempt(source, minute)
		return keyMatch{key: key}, true, nil
	}

	return keyMatch{}, false, &PairingError{Reason: ReasonKeyInvalid, Detail: rejectDetail(ReasonKeyInvalid)}
}

// undoAttempt removes one attempt recorded for source in the window opened at
// minute. lookupAtRequest calls it when a tentative count is followed by a
// static-key match. When the window has already rolled to a later minute, the
// record was discarded with the old window and there is nothing to undo.
func (k *keyTable) undoAttempt(source string, minute time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	if !k.attempts.minute.Equal(minute) {
		return
	}
	if k.attempts.bySource[source] > 1 {
		k.attempts.bySource[source]--
	} else {
		delete(k.attempts.bySource, source)
	}
	if k.attempts.total > 0 {
		k.attempts.total--
	}
}

// verifyAtConfirm checks the initiator's proof against the key selected at
// request time. A one-time key must still be in the table; when it expired
// during the handshake, gone is KEY_EXPIRED, and when it was removed some other
// way, gone is KEY_INVALID. In both cases the caller answers with that reason
// and counts no failure. A successful one-time key is removed atomically with
// the check. gone is 0 when the key is still usable.
func (k *keyTable) verifyAtConfirm(m keyMatch, ni, nr []byte, acceptorFP, initiatorSeen Fingerprint, staticKey string, now time.Time) (ok bool, gone RejectReason) {
	k.mu.Lock()
	defer k.mu.Unlock()

	if m.oneTime {
		k.expireLocked(now)
		idx := -1
		for i := range k.keys {
			if k.keys[i].key == m.key {
				idx = i
				break
			}
		}
		if idx < 0 {
			for _, e := range k.expired {
				if e.key == m.key {
					return false, ReasonKeyExpired
				}
			}
			return false, ReasonKeyInvalid
		}
		want := initiatorProof([]byte(m.key), ni, nr, initiatorSeen, acceptorFP)
		if !hmac.Equal(want, m.proof) {
			return false, 0
		}
		k.keys = append(k.keys[:idx], k.keys[idx+1:]...)
		return true, 0
	}

	// The preset static key must still equal its current configured value. A
	// rotated or deleted key cannot finish a handshake that started before it.
	if staticKey == "" || staticKey != m.key {
		return false, 0
	}
	want := initiatorProof([]byte(staticKey), ni, nr, initiatorSeen, acceptorFP)
	return hmac.Equal(want, m.proof), 0
}

// recordVerificationFailure counts one failed verification against a one-time
// key and discards the key once maxFailures is reached. The static key is never
// counted.
func (k *keyTable) recordVerificationFailure(m keyMatch, maxFailures int, now time.Time) {
	if !m.oneTime {
		return
	}
	k.mu.Lock()
	defer k.mu.Unlock()

	for i := range k.keys {
		if k.keys[i].key != m.key {
			continue
		}
		k.keys[i].failures++
		if maxFailures <= 0 || k.keys[i].failures >= maxFailures {
			k.keys = append(k.keys[:i], k.keys[i+1:]...)
		}
		return
	}
}

// CreateOneTimeKey generates a fresh one-time pairing key, returns its expiry
// and the display addresses of this Broker. It refuses to generate a key when no
// listen address is configured, because the peer could not dial this Broker.
func (p *Pairing) CreateOneTimeKey() (key string, expires time.Time, shown []string, err error) {
	if p == nil || p.settings == nil || p.clock == nil || p.keys == nil {
		return "", time.Time{}, nil, ErrNotOpen
	}
	listen, err := p.settings.ListenAddress()
	if err != nil {
		return "", time.Time{}, nil, fmt.Errorf("%w: %v", ErrStore, err)
	}
	if listen == "" {
		return "", time.Time{}, nil, ErrNotListening
	}

	var raw [oneTimeKeyBytes]byte
	if err := p.randomBytes(raw[:]); err != nil {
		return "", time.Time{}, nil, err
	}
	key = encodeOneTimeKey(raw)

	advertised, err := p.settings.Advertised()
	if err != nil {
		return "", time.Time{}, nil, fmt.Errorf("%w: %v", ErrStore, err)
	}
	hostname, err := p.settings.Hostname()
	if err != nil {
		return "", time.Time{}, nil, fmt.Errorf("%w: %v", ErrStore, err)
	}
	known := filterKnown(advertised)
	shown = make([]string, 0, len(known))
	for _, addr := range known {
		shown = append(shown, hostname+"("+addr.Addr+")")
	}

	now := p.clock.Now()
	expires = now.Add(oneTimeKeyLifetime)
	p.keys.add(key, now)
	return key, expires, shown, nil
}

// randomBytes fills buf from the injected random source, falling back to
// crypto/rand.
func (p *Pairing) randomBytes(buf []byte) error {
	if p.randRead != nil {
		_, err := p.randRead(buf)
		return err
	}
	_, err := rand.Read(buf)
	return err
}
