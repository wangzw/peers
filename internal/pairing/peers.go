package pairing

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"slices"
	"sort"
	"sync"
	"time"

	"github.com/wangzw/peers/internal/config"
)

const (
	// peersFileVersion is the version of the peers.json envelope this build
	// reads and writes. An unknown version is refused rather than guessed at.
	peersFileVersion = 1
	// peersFileMode keeps the peer table readable only by the Broker account.
	peersFileMode = 0o600
)

// peerEntry is one record in the peer table. The exported Peer view is derived
// from it.
type peerEntry struct {
	fingerprint Fingerprint
	publicKey   []byte
	alias       string // self-reported alias
	localAlias  string // empty when none was assigned
	// retiredAliases holds the effective aliases this peer gave up during the
	// current peer relationship, oldest first. They no longer name this peer,
	// but they still keep other peers from taking them over.
	retiredAliases []string
	role           Role
	pairedAt       time.Time
	acceptor       []config.AdvertisedAddr // acceptor source, dialer peers only
	card           []config.AdvertisedAddr // directory-card source, dialer peers only
	pairingAddr    string                  // address dialed during pairing, dialer peers only
	lastConnected  string                  // last address a link was established on, dialer peers only
}

// clone returns a deep copy: the exported view must never share the table's
// backing arrays.
func (e *peerEntry) clone() peerEntry {
	c := *e
	c.publicKey = append([]byte(nil), e.publicKey...)
	c.retiredAliases = append([]string(nil), e.retiredAliases...)
	c.acceptor = append([]config.AdvertisedAddr(nil), e.acceptor...)
	c.card = append([]config.AdvertisedAddr(nil), e.card...)
	return c
}

// sameEntry reports whether a and b hold the same persisted state. The
// observation paths use it to skip a peers.json rewrite when an alias, address
// list or connected address did not actually change.
func sameEntry(a, b *peerEntry) bool {
	return a.fingerprint == b.fingerprint &&
		bytes.Equal(a.publicKey, b.publicKey) &&
		a.alias == b.alias &&
		a.localAlias == b.localAlias &&
		slices.Equal(a.retiredAliases, b.retiredAliases) &&
		a.role == b.role &&
		a.pairedAt.Equal(b.pairedAt) &&
		slices.Equal(a.acceptor, b.acceptor) &&
		slices.Equal(a.card, b.card) &&
		a.pairingAddr == b.pairingAddr &&
		a.lastConnected == b.lastConnected
}

// effectiveAlias is the alias used to address this peer: the local alias when
// one was assigned, otherwise the self-reported alias.
func (e *peerEntry) effectiveAlias() string {
	if e.localAlias != "" {
		return e.localAlias
	}
	return e.alias
}

// maxRetiredAliases caps the retired aliases one peer keeps. Once a peer has
// used them all up, its effective alias is frozen as a local alias instead of
// retiring another one, so an address already held by a model never moves.
const maxRetiredAliases = 8

// applyAliasChange records the effective alias prev as retired after the entry
// moved to a different effective alias, and un-retires an alias the peer is
// returning to. When the peer already has maxRetiredAliases retired aliases, the
// change is refused instead: prev becomes the local alias, which freezes the
// effective alias in place and adds no tombstone. It returns whether the freeze
// happened, in which case localAlias now holds prev.
func (e *peerEntry) applyAliasChange(prev string) (frozen bool) {
	eff := e.effectiveAlias()
	if prev == "" || prev == eff {
		return false
	}
	if i := slices.Index(e.retiredAliases, eff); i >= 0 {
		e.retiredAliases = slices.Delete(e.retiredAliases, i, i+1)
	}
	if len(e.retiredAliases) >= maxRetiredAliases {
		e.localAlias = prev
		return true
	}
	e.retiredAliases = append(e.retiredAliases, prev)
	return false
}

// retiresAlias reports whether alias is one of the entry's retired aliases.
func (e *peerEntry) retiresAlias(alias string) bool {
	return slices.Contains(e.retiredAliases, alias)
}

// toPeer maps an internal entry to the exported view. Candidate addresses are
// part of a dialer peer's view only.
func (e *peerEntry) toPeer() Peer {
	p := Peer{
		Fingerprint: e.fingerprint,
		PublicKey:   append([]byte(nil), e.publicKey...),
		Alias:       e.alias,
		LocalAlias:  e.localAlias,
		Role:        e.role,
		PairedAt:    e.pairedAt,
	}
	if e.role == RoleDialer {
		p.Candidates = mergeCandidateList(e)
		p.LastConnected = e.lastConnected
	}
	return p
}

// peerTable is the in-memory peer table and its persistence. The mutex covers
// the map, the file write, alias conflict checks and the reservation map.
type peerTable struct {
	mu    sync.Mutex
	path  string
	ops   fileOps
	peers map[Fingerprint]*peerEntry
	// reserved maps an effective alias to the in-flight handshakes that
	// claimed it, keyed by a token unique to one handshake. A fingerprint may
	// hold several tokens on one alias when it runs concurrent handshakes;
	// releasing one token leaves the others in place.
	reserved map[string]map[uint64]Fingerprint
	// nextReservationToken issues the per-handshake reservation tokens.
	nextReservationToken uint64
	// ownAlias is the Broker's configured alias, pushed by SetOwnAlias.
	// Handshake commits recheck against this cache instead of reading config
	// while the table lock is held.
	ownAlias string
	// afterRemove is nil in production. Tests set it to interleave an update
	// between Unpair's removal and its link-drop decision.
	afterRemove func()
}

// loadPeerTable reads path and returns the table. A missing file is an empty
// table; a malformed or inconsistent file is ErrStore, never a silent reset.
func loadPeerTable(path string, ops fileOps) (*peerTable, error) {
	t := &peerTable{
		path:     path,
		ops:      ops,
		peers:    make(map[Fingerprint]*peerEntry),
		reserved: make(map[string]map[uint64]Fingerprint),
	}

	data, missing, err := readFileOrNotExist(ops, path)
	if err != nil {
		return nil, fmt.Errorf("%w: read %s: %v", ErrStore, path, err)
	}
	if missing {
		return t, nil
	}

	var file peersFile
	if err := json.Unmarshal(data, &file); err != nil {
		return nil, fmt.Errorf("%w: parse %s: %v", ErrStore, path, err)
	}
	if file.Version != peersFileVersion {
		return nil, fmt.Errorf("%w: %s has version %d, want %d", ErrStore, path, file.Version, peersFileVersion)
	}

	aliases := make(map[string]Fingerprint, len(file.Peers))
	for i := range file.Peers {
		e, err := file.Peers[i].toEntry()
		if err != nil {
			return nil, fmt.Errorf("%w: %s record %d: %v", ErrStore, path, i, err)
		}
		if _, dup := t.peers[e.fingerprint]; dup {
			return nil, fmt.Errorf("%w: %s has duplicate fingerprint %s", ErrStore, path, fingerprintHex(e.fingerprint))
		}
		eff := e.effectiveAlias()
		if other, dup := aliases[eff]; dup {
			return nil, fmt.Errorf("%w: %s has duplicate effective alias %q (%s and %s)",
				ErrStore, path, eff, fingerprintHex(other), fingerprintHex(e.fingerprint))
		}
		aliases[eff] = e.fingerprint
		t.peers[e.fingerprint] = e
	}
	// A retired alias must not collide with any effective alias, including the
	// peer's own, or with another peer's retired alias.
	retired := make(map[string]Fingerprint, len(file.Peers))
	for fp, e := range t.peers {
		for _, a := range e.retiredAliases {
			if owner, dup := aliases[a]; dup {
				return nil, fmt.Errorf("%w: %s retires alias %q, which is the effective alias of %s",
					ErrStore, path, a, fingerprintHex(owner))
			}
			if owner, dup := retired[a]; dup {
				return nil, fmt.Errorf("%w: %s has duplicate retired alias %q (%s and %s)",
					ErrStore, path, a, fingerprintHex(owner), fingerprintHex(fp))
			}
			retired[a] = fp
		}
	}
	return t, nil
}

// get returns a deep copy of one entry.
func (t *peerTable) get(fp Fingerprint) (peerEntry, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	e, ok := t.peers[fp]
	if !ok {
		return peerEntry{}, false
	}
	return e.clone(), true
}

// role returns the Broker's role for fp, or RoleUnknown when it is not a peer.
func (t *peerTable) role(fp Fingerprint) Role {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.peers[fp]; ok {
		return e.role
	}
	return RoleUnknown
}

// put inserts or replaces an entry and persists the table. The write happens
// before the in-memory update, so a failed write leaves memory unchanged.
//
// Re-pairing rules are applied here: a dialer entry keeps the existing card
// source and, when the caller supplied no new local alias, the existing local
// alias; an acceptor entry drops every candidate address. The final effective
// alias is re-checked against the table and the reservation map inside the same
// lock, so a handshake cannot commit an alias that another handshake took in the
// meantime.
func (t *peerTable) put(e peerEntry) (replaced bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	replaced, _, _, err = t.putLocked(e, 0, false)
	return replaced, err
}

// commitHandshake inserts or replaces the entry of a completed handshake. It
// performs the reservation check and the own-alias check inside the same lock
// that writes the table, so a handshake cannot commit an alias that another
// handshake took, or that the Broker itself adopted, between reserving it before
// PairDone and committing after it. token identifies this handshake's
// reservation.
//
// The own-alias recheck reads the cache that SetOwnAlias fills. It must not
// read config here: the table lock is held, and config's validators take this
// same lock while they hold config's lock.
//
// The entry's effective alias must be covered by this handshake's reservation.
// A missing or foreign reservation is ErrNeedsLocalAlias. When the effective
// alias changed after the reservation, for example because ObserveAlias assigned
// a different local alias to an existing peer, the reservation moves to the new
// effective alias and the commit continues. put keeps its unlocked-check
// semantics for direct table callers.
func (t *peerTable) commitHandshake(e peerEntry, token uint64) (replaced bool, committedAlias string, frozen bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.putLocked(e, token, true)
}

// putLocked requires t.mu to be held. handshake selects the handshake commit
// path: someone else's reservation is always rejected, and the handshake path
// additionally requires token to hold the effective alias and the effective
// alias to differ from the cached Broker alias.
func (t *peerTable) putLocked(e peerEntry, token uint64, handshake bool) (replaced bool, committedAlias string, frozen bool, err error) {
	if e.role != RoleDialer && e.role != RoleAcceptor {
		return false, "", false, fmt.Errorf("%w: invalid role %d", ErrStore, e.role)
	}
	// The stored self-reported alias is always the normalized form. The decision
	// to use it as the effective alias, or to assign a local alias instead, was
	// already made from the value as reported.
	e.alias = normalizeAlias(e.alias)

	old, existed := t.peers[e.fingerprint]
	replaced = existed
	prevEff := ""
	if replaced {
		// Re-pairing keeps the aliases this peer gave up: the relationship
		// continues, so addresses already handed out must not move.
		prevEff = old.effectiveAlias()
		e.retiredAliases = append([]string(nil), old.retiredAliases...)
		switch e.role {
		case RoleDialer:
			// Keep the directory-card source across re-pairing. The handshake
			// replaces the other sources with values from this attempt.
			e.card = append([]config.AdvertisedAddr(nil), old.card...)
			if e.localAlias == "" {
				e.localAlias = old.localAlias
			}
		case RoleAcceptor:
			e.acceptor = nil
			e.card = nil
			e.pairingAddr = ""
			e.lastConnected = ""
		}
	}

	if e.role == RoleDialer {
		e.acceptor = capKnown(e.acceptor)
		e.card = capKnown(e.card)
		if e.lastConnected == "" && e.pairingAddr != "" {
			// The initiator just completed a handshake at the pairing address,
			// so that address is by definition the last one it connected to.
			e.lastConnected = e.pairingAddr
		}
	} else {
		e.acceptor = nil
		e.card = nil
		e.pairingAddr = ""
		e.lastConnected = ""
	}

	// The effective alias may have changed through re-pairing. Retire the old
	// one, or freeze the old one as a local alias when tombstones are full.
	frozen = e.applyAliasChange(prevEff)

	eff := e.effectiveAlias()
	if eff == "" {
		return replaced, "", false, fmt.Errorf("%w: peer %s has no effective alias", ErrNeedsLocalAlias, fingerprintHex(e.fingerprint))
	}
	if handshake {
		if eff == t.ownAlias {
			return replaced, "", false, fmt.Errorf("%w: alias %q is the Broker's own alias", ErrNeedsLocalAlias, eff)
		}
		if err := t.ensureReservationLocked(eff, token, e.fingerprint, t.ownAlias); err != nil {
			return replaced, "", false, err
		}
	} else if owners, ok := t.reserved[eff]; ok {
		for _, owner := range owners {
			if owner != e.fingerprint {
				return replaced, "", false, fmt.Errorf("%w: alias %q is reserved by another handshake", ErrNeedsLocalAlias, eff)
			}
		}
	}
	for otherFP, other := range t.peers {
		if otherFP == e.fingerprint {
			continue
		}
		if other.effectiveAlias() == eff {
			return replaced, "", false, fmt.Errorf("%w: alias %q belongs to %s", ErrNeedsLocalAlias, eff, fingerprintHex(otherFP))
		}
		if other.retiresAlias(eff) {
			return replaced, "", false, fmt.Errorf("%w: alias %q was given up by %s", ErrNeedsLocalAlias, eff, fingerprintHex(otherFP))
		}
	}

	next := make(map[Fingerprint]*peerEntry, len(t.peers)+1)
	for fp, p := range t.peers {
		next[fp] = p
	}
	entry := e.clone()
	next[e.fingerprint] = &entry
	if err := t.writeLocked(next); err != nil {
		return replaced, "", false, err
	}
	t.peers = next

	// A committed entry needs no reservation. Release only this handshake's
	// token: a concurrent handshake for the same fingerprint may still hold its
	// own reservation on the same alias.
	if handshake {
		t.releaseTokenLocked(token)
	}
	return replaced, entry.effectiveAlias(), frozen, nil
}

// ensureReservationLocked makes token cover eff. When the effective alias
// changed between reserving and committing, the token moves from the alias it
// held to eff, provided eff is free. A token that is no longer reserved, or an
// eff that another peer or handshake owns, is ErrNeedsLocalAlias.
func (t *peerTable) ensureReservationLocked(eff string, token uint64, fp Fingerprint, ownAlias string) error {
	if owners, ok := t.reserved[eff]; ok && owners[token] == fp {
		return nil
	}
	if _, ok := t.tokenAliasLocked(token); !ok {
		return fmt.Errorf("%w: alias %q is not reserved by this handshake", ErrNeedsLocalAlias, eff)
	}
	if eff == ownAlias {
		return fmt.Errorf("%w: alias %q is the Broker's own alias", ErrNeedsLocalAlias, eff)
	}
	for otherFP, other := range t.peers {
		if otherFP == fp {
			continue
		}
		if other.effectiveAlias() == eff {
			return fmt.Errorf("%w: alias %q belongs to %s", ErrNeedsLocalAlias, eff, fingerprintHex(otherFP))
		}
		if other.retiresAlias(eff) {
			return fmt.Errorf("%w: alias %q was given up by %s", ErrNeedsLocalAlias, eff, fingerprintHex(otherFP))
		}
	}
	for otherToken, owner := range t.reserved[eff] {
		if owner != fp && otherToken != token {
			return fmt.Errorf("%w: alias %q is reserved by another handshake", ErrNeedsLocalAlias, eff)
		}
	}
	t.releaseTokenLocked(token)
	if t.reserved[eff] == nil {
		t.reserved[eff] = make(map[uint64]Fingerprint)
	}
	t.reserved[eff][token] = fp
	return nil
}

// remove deletes an entry and persists the table, returning the removed entry so
// the caller can tell whether a later re-pairing replaced it. A missing entry is
// a successful no-op. A failed write leaves the entry in memory and reports
// existed=true with the error.
func (t *peerTable) remove(fp Fingerprint) (removed *peerEntry, existed bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()
	entry, ok := t.peers[fp]
	if !ok {
		return nil, false, nil
	}
	next := make(map[Fingerprint]*peerEntry, len(t.peers))
	for otherFP, p := range t.peers {
		if otherFP == fp {
			continue
		}
		next[otherFP] = p
	}
	if err := t.writeLocked(next); err != nil {
		return nil, true, err
	}
	t.peers = next
	return entry, true, nil
}

// entrySuperseded reports whether a different entry has replaced the removed
// one. Unpair uses it after a removal: a re-pairing that committed in the
// meantime put a new entry in the table, and its link must not be dropped.
func (t *peerTable) entrySuperseded(fp Fingerprint, removed *peerEntry) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	current, ok := t.peers[fp]
	return ok && current != removed
}

// effectiveAlias returns the effective alias of fp, or "" when it is not a
// peer.
func (t *peerTable) effectiveAlias(fp Fingerprint) string {
	t.mu.Lock()
	defer t.mu.Unlock()
	if e, ok := t.peers[fp]; ok {
		return e.effectiveAlias()
	}
	return ""
}

// resolveAlias maps an alias to the committed peer that currently owns it.
// Aliases reserved by in-flight handshakes are not peers yet and never resolve.
// Matching is case-sensitive. A name equal to the Broker's own alias never
// resolves to a peer.
func (t *peerTable) resolveAlias(alias string) (Fingerprint, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if alias == "" || (t.ownAlias != "" && alias == t.ownAlias) {
		return Fingerprint{}, false
	}
	for fp, e := range t.peers {
		if e.effectiveAlias() == alias {
			return fp, true
		}
	}
	return Fingerprint{}, false
}

// reserveAlias claims alias for one handshake and returns its token. It is used
// before PairDone so two concurrent handshakes cannot both use one alias.
func (t *peerTable) reserveAlias(alias string, fp Fingerprint) (uint64, bool) {
	t.mu.Lock()
	defer t.mu.Unlock()
	return t.reserveAliasLocked(alias, fp)
}

// releaseToken releases the reservation held by one handshake. Releasing a
// token that is no longer reserved is a no-op.
func (t *peerTable) releaseToken(token uint64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.releaseTokenLocked(token)
}

// reserveAliasLocked requires t.mu to be held.
func (t *peerTable) reserveAliasLocked(alias string, fp Fingerprint) (uint64, bool) {
	if alias == "" {
		return 0, false
	}
	if owners, ok := t.reserved[alias]; ok {
		for _, owner := range owners {
			if owner != fp {
				return 0, false
			}
		}
	}
	for otherFP, e := range t.peers {
		if otherFP == fp {
			continue
		}
		if e.effectiveAlias() == alias || e.retiresAlias(alias) {
			return 0, false
		}
	}
	t.nextReservationToken++
	if t.nextReservationToken == 0 {
		t.nextReservationToken++
	}
	token := t.nextReservationToken
	if t.reserved[alias] == nil {
		t.reserved[alias] = make(map[uint64]Fingerprint)
	}
	t.reserved[alias][token] = fp
	return token, true
}

// releaseTokenLocked requires t.mu to be held.
func (t *peerTable) releaseTokenLocked(token uint64) {
	if token == 0 {
		return
	}
	for alias, owners := range t.reserved {
		if _, ok := owners[token]; !ok {
			continue
		}
		delete(owners, token)
		if len(owners) == 0 {
			delete(t.reserved, alias)
		}
		return
	}
}

// tokenAliasLocked returns the alias token currently reserves. It requires t.mu
// to be held.
func (t *peerTable) tokenAliasLocked(token uint64) (string, bool) {
	if token == 0 {
		return "", false
	}
	for alias, owners := range t.reserved {
		if _, ok := owners[token]; ok {
			return alias, true
		}
	}
	return "", false
}

// reservationOwnerLocked returns one fingerprint holding alias. Every holder of
// one alias is the same fingerprint. It requires t.mu to be held.
func (t *peerTable) reservationOwnerLocked(alias string) (Fingerprint, bool) {
	for _, fp := range t.reserved[alias] {
		return fp, true
	}
	return Fingerprint{}, false
}

// assignLocalAliasLocked generates a local alias for fp that does not collide
// with the Broker's own alias, another peer's effective alias, or a reserved
// alias, and reserves it for one handshake. It returns the reservation token.
// It requires t.mu to be held.
func (t *peerTable) assignLocalAliasLocked(fp Fingerprint, reported, ownAlias string) (alias string, token uint64, ok bool) {
	alias, ok = t.makeLocalAliasLocked(fp, reported, ownAlias)
	if !ok {
		return "", 0, false
	}
	token, ok = t.reserveAliasLocked(alias, fp)
	if !ok {
		return "", 0, false
	}
	return alias, token, true
}

// makeLocalAliasLocked generates a unique local alias without reserving it. It
// requires t.mu to be held.
func (t *peerTable) makeLocalAliasLocked(fp Fingerprint, reported, ownAlias string) (string, bool) {
	taken := t.takenAliasesLocked(fp, ownAlias)
	alias := assignLocalAlias(reported, fp, ownAlias, taken)
	if alias == "" {
		return "", false
	}
	return alias, true
}

// takenAliasesLocked collects every alias a new local alias must avoid. The
// entry for fp itself is excluded so a peer can keep its own alias, but every
// retired alias is included, including fp's own: an effective alias may not
// repeat one of its peer's tombstones. It requires t.mu to be held.
func (t *peerTable) takenAliasesLocked(fp Fingerprint, ownAlias string) map[string]struct{} {
	taken := make(map[string]struct{}, len(t.peers)+len(t.reserved)+1)
	if ownAlias != "" {
		taken[ownAlias] = struct{}{}
	}
	for otherFP, e := range t.peers {
		for _, retired := range e.retiredAliases {
			taken[retired] = struct{}{}
		}
		if otherFP == fp {
			continue
		}
		if a := e.effectiveAlias(); a != "" {
			taken[a] = struct{}{}
		}
	}
	for alias, owners := range t.reserved {
		for _, owner := range owners {
			if owner != fp {
				taken[alias] = struct{}{}
				break
			}
		}
	}
	return taken
}

// aliasAvailableLocked reports whether a self-reported alias may be used as the
// effective alias of fp without assigning a local alias. Other peers' retired
// aliases are taken, but fp's own are not: a peer may return to an alias it
// gave up earlier. It requires t.mu to be held.
func (t *peerTable) aliasAvailableLocked(alias string, fp Fingerprint, ownAlias string) bool {
	if !validAlias(alias) || alias == ownAlias {
		return false
	}
	for _, owner := range t.reserved[alias] {
		if owner != fp {
			return false
		}
	}
	for otherFP, e := range t.peers {
		if otherFP == fp {
			continue
		}
		if e.effectiveAlias() == alias || e.retiresAlias(alias) {
			return false
		}
	}
	return true
}

// snapshot returns the whole table sorted by effective alias and then by
// fingerprint bytes, so the exported order is deterministic.
func (t *peerTable) snapshot() []Peer {
	t.mu.Lock()
	defer t.mu.Unlock()
	out := make([]Peer, 0, len(t.peers))
	for _, e := range t.peers {
		out = append(out, e.toPeer())
	}
	sort.Slice(out, func(i, j int) bool {
		ai, aj := peerEffectiveAlias(out[i]), peerEffectiveAlias(out[j])
		if ai != aj {
			return ai < aj
		}
		return bytes.Compare(out[i].Fingerprint[:], out[j].Fingerprint[:]) < 0
	})
	return out
}

func peerEffectiveAlias(p Peer) string {
	if p.LocalAlias != "" {
		return p.LocalAlias
	}
	return p.Alias
}

// --- Pairing methods -------------------------------------------------------

// IsPeer reports whether fp is in the peer table. A zero Pairing has an empty
// table.
func (p *Pairing) IsPeer(fp Fingerprint) bool {
	if p == nil || p.peers == nil {
		return false
	}
	_, ok := p.peers.get(fp)
	return ok
}

// Role returns this Broker's role for fp, or RoleUnknown when fp is not a peer.
func (p *Pairing) Role(fp Fingerprint) Role {
	if p == nil || p.peers == nil {
		return RoleUnknown
	}
	return p.peers.role(fp)
}

// Peers returns a copy of every peer entry, sorted by effective alias and then
// fingerprint. A zero Pairing returns an empty, non-nil slice.
func (p *Pairing) Peers() []Peer {
	if p == nil || p.peers == nil {
		return []Peer{}
	}
	return p.peers.snapshot()
}

// EffectiveAlias returns the alias used to address fp, or "" when fp is not a
// peer.
func (p *Pairing) EffectiveAlias(fp Fingerprint) string {
	if p == nil || p.peers == nil {
		return ""
	}
	return p.peers.effectiveAlias(fp)
}

// ResolveAlias maps an alias to a peer fingerprint. It consults committed peers
// it. Aliases held only by in-flight handshakes do not resolve: they are not
// peers yet. It never resolves the Broker's own alias, and takes only the peer
// table lock, so it is safe to call from a config validator that holds config's
// lock.
func (p *Pairing) ResolveAlias(alias string) (Fingerprint, bool) {
	if p == nil || p.peers == nil {
		return Fingerprint{}, false
	}
	return p.peers.resolveAlias(alias)
}

// SetOwnAlias caches this Broker's configured alias. The Broker calls it when
// it wires the config subscription and again from the broker.alias subscriber,
// which runs outside config's lock. The cache is read by the handshake commit
// recheck and by ResolveAlias, so neither has to call config while holding the
// peer table lock.
func (p *Pairing) SetOwnAlias(alias string) {
	if p == nil || p.peers == nil {
		return
	}
	p.peers.setOwnAlias(alias)
}

// AliasConflict reports whether alias is already used by a committed peer or
// held by an in-flight handshake. The Broker's broker.alias validator calls it;
// it takes only the peer table lock and never calls config, so it cannot
// deadlock against a config operation that holds config's lock.
func (p *Pairing) AliasConflict(alias string) bool {
	if p == nil || p.peers == nil {
		return false
	}
	return p.peers.aliasConflict(alias)
}

// Unpair removes fp and closes any link to it. It is idempotent: a fingerprint
// that is not a peer returns nil. The link is dropped after the table lock is
// released, so a link callback cannot deadlock against the peer table. A
// re-pairing that commits between the removal and the drop leaves a new entry
// in the table; the drop is skipped so that the new link survives.
func (p *Pairing) Unpair(fp Fingerprint) error {
	if p == nil || p.peers == nil {
		return ErrNotOpen
	}
	removed, existed, err := p.peers.remove(fp)
	if err != nil {
		return err
	}
	if !existed {
		return nil
	}
	if p.peers.afterRemove != nil {
		p.peers.afterRemove()
	}
	if p.transport != nil && !p.peers.entrySuperseded(fp, removed) {
		p.transport.Drop(fp)
	}
	return nil
}

// ObserveAlias records the peer's current self-reported alias. A peer that
// already has a local alias keeps it. Otherwise the new self-reported alias
// becomes effective when it is valid and free; if it collides, a local alias is
// generated and an audit event is written. Persistence failures keep the old
// values and are logged, because the method has no error return.
func (p *Pairing) ObserveAlias(fp Fingerprint, alias string) {
	if p == nil || p.peers == nil || p.settings == nil {
		slog.Error("pairing: ObserveAlias on an unopened instance", "fingerprint", fingerprintHex(fp))
		return
	}
	localAlias, assigned, err := p.peers.observeAlias(fp, alias)
	if err != nil {
		slog.Error("pairing: persist observed alias", "fingerprint", fingerprintHex(fp), "error", err)
		return
	}
	if assigned {
		auditLocalAliasAssigned(p.settings, p.now(), fp, localAlias)
	}
}

// ObserveConnected records the address on which a link to a dialer peer was
// established. Acceptor peers store no addresses. Persistence failures keep the
// old value and are logged.
func (p *Pairing) ObserveConnected(fp Fingerprint, address string) {
	if p == nil || p.peers == nil {
		slog.Error("pairing: ObserveConnected on an unopened instance", "fingerprint", fingerprintHex(fp))
		return
	}
	if err := p.peers.observeConnected(fp, address); err != nil {
		slog.Error("pairing: persist connected address", "fingerprint", fingerprintHex(fp), "error", err)
	}
}

// observeAlias applies one alias observation and persists the result. It
// returns the generated local alias, if one was assigned. The Broker's own
// alias is read from the cache under the table lock.
func (t *peerTable) observeAlias(fp Fingerprint, alias string) (localAlias string, assigned bool, err error) {
	t.mu.Lock()
	defer t.mu.Unlock()

	old, ok := t.peers[fp]
	if !ok {
		return "", false, nil
	}
	prev := old.effectiveAlias()
	next := old.clone()
	// Store the normalized form, but keep deciding availability from the value
	// as reported: an oversized or malformed alias is still unusable and gets a
	// local alias.
	next.alias = normalizeAlias(alias)

	if next.localAlias == "" {
		if !t.aliasAvailableLocked(alias, fp, t.ownAlias) {
			generated, ok := t.makeLocalAliasLocked(fp, alias, t.ownAlias)
			if !ok {
				return "", false, fmt.Errorf("%w: cannot generate a local alias", ErrStore)
			}
			next.localAlias = generated
			localAlias = generated
			assigned = true
		}
	}
	// Adopting a different effective alias retires the old one. When the peer
	// has used up its tombstones, the old alias is frozen as a local alias
	// instead and the change is audited like any other local alias assignment.
	if next.applyAliasChange(prev) {
		localAlias = next.localAlias
		assigned = true
	}
	if sameEntry(old, &next) {
		return "", false, nil
	}

	updated := make(map[Fingerprint]*peerEntry, len(t.peers))
	for otherFP, e := range t.peers {
		if otherFP == fp {
			updated[otherFP] = &next
			continue
		}
		updated[otherFP] = e
	}
	if err := t.writeLocked(updated); err != nil {
		return "", false, err
	}
	t.peers = updated
	return localAlias, assigned, nil
}

// observeConnected stores the last connected address of a dialer peer.
func (t *peerTable) observeConnected(fp Fingerprint, address string) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	old, ok := t.peers[fp]
	if !ok || old.role != RoleDialer {
		return nil
	}
	if old.lastConnected == address {
		return nil
	}
	next := old.clone()
	next.lastConnected = address
	return t.replaceLocked(fp, &next)
}

// updateSource replaces one address source of a dialer peer, filters unknown
// kinds and caps the list. Unknown peers and acceptor peers are ignored.
func (t *peerTable) updateSource(fp Fingerprint, source AddressSource, addrs []config.AdvertisedAddr) error {
	t.mu.Lock()
	defer t.mu.Unlock()

	old, ok := t.peers[fp]
	if !ok || old.role != RoleDialer {
		return nil
	}
	next := old.clone()
	switch source {
	case SourceAcceptor:
		next.acceptor = capKnown(addrs)
	case SourceCard:
		next.card = capKnown(addrs)
	default:
		return fmt.Errorf("%w: invalid address source %d", ErrStore, source)
	}
	if sameEntry(old, &next) {
		return nil
	}
	return t.replaceLocked(fp, &next)
}

// replaceLocked persists next as the entry for fp. It requires t.mu to be held.
func (t *peerTable) replaceLocked(fp Fingerprint, next *peerEntry) error {
	updated := make(map[Fingerprint]*peerEntry, len(t.peers))
	for otherFP, e := range t.peers {
		if otherFP == fp {
			updated[otherFP] = next
			continue
		}
		updated[otherFP] = e
	}
	if err := t.writeLocked(updated); err != nil {
		return err
	}
	t.peers = updated
	return nil
}

// writeLocked serializes next and atomically replaces the table file. It
// requires t.mu to be held.
func (t *peerTable) writeLocked(next map[Fingerprint]*peerEntry) error {
	data, err := marshalPeers(next)
	if err != nil {
		return fmt.Errorf("%w: encode peer table: %v", ErrStore, err)
	}
	if err := atomicWrite(t.ops, t.path, data, peersFileMode); err != nil {
		return fmt.Errorf("%w: write %s: %v", ErrStore, t.path, err)
	}
	return nil
}

// setOwnAlias replaces the cached Broker alias under the table lock.
func (t *peerTable) setOwnAlias(alias string) {
	t.mu.Lock()
	t.ownAlias = alias
	t.mu.Unlock()
}

// aliasConflict reports whether alias is used by a committed peer or held by an
// in-flight handshake, or was given up by a peer during its current
// relationship. A retired alias is a conflict too: the Broker's own alias must
// not adopt a name another peer used to answer to. The cached own alias is not a
// conflict: the validator is checking a new value for the same key, and setting
// it to its current value must succeed.
func (t *peerTable) aliasConflict(alias string) bool {
	if alias == "" {
		return false
	}
	t.mu.Lock()
	defer t.mu.Unlock()
	if len(t.reserved[alias]) > 0 {
		return true
	}
	for _, e := range t.peers {
		if e.effectiveAlias() == alias || e.retiresAlias(alias) {
			return true
		}
	}
	return false
}

// now returns the injected clock's current time.
func (p *Pairing) now() time.Time {
	if p != nil && p.clock != nil {
		return p.clock.Now()
	}
	return time.Now()
}

// fingerprintHex renders a fingerprint as 64 lowercase hex characters without
// depending on Fingerprint.String, which another file owns.
func fingerprintHex(fp Fingerprint) string {
	return hex.EncodeToString(fp[:])
}

// --- JSON encoding ---------------------------------------------------------

// peersFile is the versioned top-level object in peers.json.
type peersFile struct {
	Version int          `json:"version"`
	Peers   []peerRecord `json:"peers"`
}

// peerRecord is the on-disk form of one entry. The field names and shapes are
// deliberately private so peers.json does not depend on config's struct layout.
type peerRecord struct {
	Fingerprint    string        `json:"fingerprint"`
	PublicKey      string        `json:"public_key"`
	Alias          string        `json:"alias"`
	LocalAlias     string        `json:"local_alias"`
	RetiredAliases []string      `json:"retired_aliases,omitempty"`
	Role           string        `json:"role"`
	PairedAt       string        `json:"paired_at"`
	Addresses      *addressesDoc `json:"addresses,omitempty"`
	LastConnected  string        `json:"last_connected,omitempty"`
}

type addressesDoc struct {
	Acceptor []addrDoc `json:"acceptor"`
	Card     []addrDoc `json:"card"`
	Pairing  string    `json:"pairing"`
}

type addrDoc struct {
	Kind string `json:"kind"`
	Addr string `json:"addr"`
}

// marshalPeers encodes the table in a deterministic order.
func marshalPeers(peers map[Fingerprint]*peerEntry) ([]byte, error) {
	fps := make([]Fingerprint, 0, len(peers))
	for fp := range peers {
		fps = append(fps, fp)
	}
	sort.Slice(fps, func(i, j int) bool {
		return bytes.Compare(fps[i][:], fps[j][:]) < 0
	})

	file := peersFile{
		Version: peersFileVersion,
		Peers:   make([]peerRecord, 0, len(fps)),
	}
	for _, fp := range fps {
		file.Peers = append(file.Peers, peers[fp].toRecord())
	}
	return json.MarshalIndent(file, "", "  ")
}

// toRecord maps an entry to its on-disk form. Candidate addresses are written
// for dialer peers only, matching the requirement that acceptor entries keep
// none.
func (e *peerEntry) toRecord() peerRecord {
	r := peerRecord{
		Fingerprint:    fingerprintHex(e.fingerprint),
		PublicKey:      base64.StdEncoding.EncodeToString(e.publicKey),
		Alias:          e.alias,
		LocalAlias:     e.localAlias,
		RetiredAliases: append([]string(nil), e.retiredAliases...),
		Role:           roleName(e.role),
		PairedAt:       e.pairedAt.UTC().Format(time.RFC3339Nano),
	}
	if e.role == RoleDialer {
		r.Addresses = &addressesDoc{
			Acceptor: addrDocs(e.acceptor),
			Card:     addrDocs(e.card),
			Pairing:  e.pairingAddr,
		}
		r.LastConnected = e.lastConnected
	}
	return r
}

func addrDocs(addrs []config.AdvertisedAddr) []addrDoc {
	out := make([]addrDoc, 0, len(addrs))
	for _, a := range addrs {
		out = append(out, addrDoc{Kind: a.Kind, Addr: a.Addr})
	}
	return out
}

// toEntry parses and validates one on-disk record.
func (r *peerRecord) toEntry() (*peerEntry, error) {
	fp, err := parseFingerprint(r.Fingerprint)
	if err != nil {
		return nil, err
	}
	pub, err := base64.StdEncoding.DecodeString(r.PublicKey)
	if err != nil {
		return nil, fmt.Errorf("public key is not base64: %v", err)
	}
	if len(pub) != 32 {
		return nil, fmt.Errorf("public key has length %d, want 32", len(pub))
	}
	if got := fingerprintOfPublicKey(pub); got != fp {
		return nil, fmt.Errorf("fingerprint %s does not match public key %s", r.Fingerprint, fingerprintHex(got))
	}
	role, err := parseRole(r.Role)
	if err != nil {
		return nil, err
	}
	pairedAt, err := time.Parse(time.RFC3339Nano, r.PairedAt)
	if err != nil {
		return nil, fmt.Errorf("paired_at is not RFC 3339: %v", err)
	}
	if len(r.RetiredAliases) > maxRetiredAliases {
		return nil, fmt.Errorf("retired_aliases has %d entries, want at most %d", len(r.RetiredAliases), maxRetiredAliases)
	}
	retired := append([]string(nil), r.RetiredAliases...)
	seen := make(map[string]struct{}, len(retired))
	for _, a := range retired {
		if !validAlias(a) {
			return nil, fmt.Errorf("retired alias %q is not a valid alias", a)
		}
		if _, dup := seen[a]; dup {
			return nil, fmt.Errorf("retired alias %q is listed twice", a)
		}
		seen[a] = struct{}{}
	}

	e := &peerEntry{
		fingerprint: fp,
		publicKey:   pub,
		// An older build may have persisted an alias that the current rule
		// rejects. Normalizing it here lets such a file load, and the next write
		// persists the normalized value.
		alias:          normalizeAlias(r.Alias),
		localAlias:     r.LocalAlias,
		retiredAliases: retired,
		role:           role,
		pairedAt:       pairedAt.UTC(),
	}
	if role == RoleDialer && r.Addresses != nil {
		e.acceptor = capKnown(addrConfigs(r.Addresses.Acceptor))
		e.card = capKnown(addrConfigs(r.Addresses.Card))
		e.pairingAddr = r.Addresses.Pairing
		e.lastConnected = r.LastConnected
	}
	return e, nil
}

func addrConfigs(docs []addrDoc) []config.AdvertisedAddr {
	out := make([]config.AdvertisedAddr, 0, len(docs))
	for _, d := range docs {
		out = append(out, config.AdvertisedAddr{Kind: d.Kind, Addr: d.Addr})
	}
	return out
}

func parseFingerprint(s string) (Fingerprint, error) {
	var fp Fingerprint
	if len(s) != 2*len(fp) {
		return fp, fmt.Errorf("fingerprint %q does not have %d hex characters", s, 2*len(fp))
	}
	raw, err := hex.DecodeString(s)
	if err != nil {
		return fp, fmt.Errorf("fingerprint %q is not hex: %v", s, err)
	}
	copy(fp[:], raw)
	return fp, nil
}

func roleName(r Role) string {
	switch r {
	case RoleDialer:
		return "dialer"
	case RoleAcceptor:
		return "acceptor"
	default:
		return "unknown"
	}
}

func parseRole(s string) (Role, error) {
	switch s {
	case "dialer":
		return RoleDialer, nil
	case "acceptor":
		return RoleAcceptor, nil
	default:
		return RoleUnknown, fmt.Errorf("role %q is neither dialer nor acceptor", s)
	}
}
