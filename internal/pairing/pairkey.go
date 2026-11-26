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
	oneTimeKeyLifetime = 10 * time.Minute
	oneTimeKeyBytes    = 10
	crockfordAlphabet  = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"
	// The HMAC domain-separation labels, kept together: changing one breaks
	// pairing with a Broker using the old value, and none has a NUL terminator.
	pairHintLabel        = "peers pair hint"
	pairResponderLabel   = "peers pair responder"
	pairInitiatorLabel   = "peers pair initiator"
	expiredKeyRetention  = 24 * time.Hour
	maxExpiredKeyRecords = 64
)

// expiredKey is kept only to recompute lookup tags; it never verifies a proof
// or completes a handshake.
type expiredKey struct {
	key   string
	until time.Time
}

type oneTimeKey struct {
	key      string
	expires  time.Time
	failures int
}

type keyMatch struct {
	key     string
	oneTime bool
	index   int
	proof   []byte
}

const attemptGlobalFactor = 10

// attemptWindow counts unmatched pairing requests per source and in total for
// one minute and is replaced when the minute changes.
type attemptWindow struct {
	minute   time.Time
	total    int
	bySource map[string]int
}

// keyTable protects the key, expired-key and attempt state with one mutex, so
// two concurrent handshakes cannot both consume one key.
type keyTable struct {
	mu       sync.Mutex
	keys     []oneTimeKey
	expired  []expiredKey
	attempts attemptWindow
}

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

// computeHint returns the lookup tag, bound to the initiator's fingerprint.
func computeHint(key, ni []byte, initiator Fingerprint) []byte {
	mac := hmac.New(sha256.New, key)
	mac.Write([]byte(pairHintLabel))
	mac.Write(ni)
	mac.Write(initiator[:])
	return mac.Sum(nil)[:lookupTagLength]
}

func (k *keyTable) add(key string, now time.Time) {
	k.mu.Lock()
	defer k.mu.Unlock()
	k.expireLocked(now)
	k.keys = append(k.keys, oneTimeKey{key: key, expires: now.Add(oneTimeKeyLifetime)})
}

// expireLocked must be called with k.mu held; it moves expired keys into the
// expired-key records and caps the records at maxExpiredKeyRecords.
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

// lookupAtRequest selects the key for an incoming PairRequest, checking
// one-time keys, expired-key records, the per-minute attempt limits and then
// the pre-provisioned static key. staticKey is called only after both limits pass and
// never with k.mu held, because it reads config. An unmatched request is
// counted before the static key is read and the count undone when it matches. A
// hit on an expired record answers KEY_EXPIRED and touches no counter.
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

	// Reset the window before consulting the limits, or an exhausted minute
	// would reject forever.
	minute := now.Truncate(time.Minute)
	if !k.attempts.minute.Equal(minute) {
		k.attempts = attemptWindow{minute: minute}
	}

	if k.attempts.bySource[source] >= maxAttempts || k.attempts.total >= maxAttempts*attemptGlobalFactor {
		k.mu.Unlock()
		return keyMatch{}, false, &PairingError{Reason: ReasonTryLater, Detail: rejectDetail(ReasonTryLater)}
	}

	// Record the attempt before releasing the lock so two concurrent requests
	// cannot both pass the limit check; a static-key match below undoes it.
	if k.attempts.bySource == nil {
		k.attempts.bySource = make(map[string]int)
	}
	k.attempts.bySource[source]++
	k.attempts.total++
	k.mu.Unlock()

	// staticKey reads config's secret, so it must run outside k.mu: taking k.mu
	// before config's lock inverts the config-then-pairing order.
	key := staticKey()
	if key != "" && hmac.Equal(computeHint([]byte(key), ni, initiator), hint) {
		k.undoAttempt(source, minute)
		return keyMatch{key: key}, true, nil
	}

	return keyMatch{}, false, &PairingError{Reason: ReasonKeyInvalid, Detail: rejectDetail(ReasonKeyInvalid)}
}

// undoAttempt removes a tentative attempt count; when the window has rolled to
// a later minute there is nothing to undo.
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

// verifyAtConfirm checks the proof against the key selected at request time,
// removing a one-time key atomically with the check. gone reports KEY_EXPIRED
// for a key expired during the handshake and KEY_INVALID for one removed
// otherwise, and is 0 when the key is still usable.
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

	// A rotated or deleted static key cannot finish a handshake that started
	// before it.
	if staticKey == "" || staticKey != m.key {
		return false, 0
	}
	want := initiatorProof([]byte(staticKey), ni, nr, initiatorSeen, acceptorFP)
	return hmac.Equal(want, m.proof), 0
}

// recordVerificationFailure counts a failed verification against a one-time key
// and discards it at maxFailures; the static key is never counted.
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

// CreateOneTimeKey generates a one-time pairing key and returns it with its
// expiry and the display spellings of this Broker's advertised direct addresses,
// each as hostname(addr). It returns ErrNotListening when no listen address is
// configured, ErrNotOpen before Open, ErrStore when the settings cannot be read,
// and the random source's error unchanged when entropy fails.
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

func (p *Pairing) randomBytes(buf []byte) error {
	if p.randRead != nil {
		_, err := p.randRead(buf)
		return err
	}
	_, err := rand.Read(buf)
	return err
}
