package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	nameOwnersFileName       = "name-owners.json"
	nameOwnersFileFormat     = 1
	nameOwnerRetention       = 7 * 24 * time.Hour
	nameOwnerScanInterval    = time.Hour
	nameOwnerPerAccountLimit = 1024
	nameOwnerTableLimit      = 65536
)

var errNameOwnersUnreadable = errors.New("name-owners.json is unreadable")

// key counts against the per-account limit; uid is the owner the address rules
// use.
type nameOwnerRecord struct {
	uid      uint32
	key      uint32
	lastHeld time.Time
}

type nameOwnersFile struct {
	Format  int                  `json:"format"`
	Records []nameOwnersFileItem `json:"records"`
}

type nameOwnersFileItem struct {
	Name       string  `json:"name"`
	UID        uint32  `json:"uid"`
	LastHeld   string  `json:"last_held"`
	AccountKey *uint32 `json:"account_key,omitempty"`
}

// records, warned, limit, dirty and scanTimer are guarded by Broker.mu.
type nameOwnerState struct {
	records    map[string]nameOwnerRecord
	warned     map[uint32]bool
	limit      int
	tableLimit int
	dirty      bool
	// corruptPath and writeWarned are guarded by writeMu, not Broker.mu.
	corruptPath string
	writeWarned bool

	// An empty dir disables persistence.
	dir string
	log *slog.Logger

	save chan struct{}
	done chan struct{}

	writeMu   sync.Mutex
	scanTimer Timer
}

func newNameOwnerState(log *slog.Logger, dir string) *nameOwnerState {
	return &nameOwnerState{
		records:    make(map[string]nameOwnerRecord),
		warned:     make(map[uint32]bool),
		limit:      nameOwnerPerAccountLimit,
		tableLimit: nameOwnerTableLimit,
		dir:        dir,
		log:        log,
		save:       make(chan struct{}, 1),
		done:       make(chan struct{}),
	}
}

func (s *nameOwnerState) path() string {
	return filepath.Join(s.dir, nameOwnersFileName)
}

// New calls this after the configuration is loaded, so Paths() already names
// the data directory the save loop and sweep use.
func (b *Broker) initNameOwners() error {
	b.nameOwners = newNameOwnerState(b.log, b.deps.Config.Paths().DataDir)
	if b.nameOwners.dir != "" {
		if err := b.nameOwners.load(); err != nil {
			if errors.Is(err, errNameOwnersUnreadable) {
				b.log.Warn(EvNameOwnersUnreadable, "path", b.nameOwners.path(), "error", err)
				// Keep the unreadable file: rename it aside just before the next
				// write so it can be inspected instead of being overwritten.
				b.nameOwners.corruptPath = b.nameOwners.path()
			} else {
				return wrapDataUnavailable(dataKindDataDir, err)
			}
		}
		go b.runNameOwnerSaver()
	}
	b.armNameOwnerScan()
	return nil
}

func (s *nameOwnerState) load() error {
	data, err := os.ReadFile(s.path())
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cannot read %s: %w", s.path(), err)
	}
	var f nameOwnersFile
	if err := json.Unmarshal(data, &f); err != nil {
		return fmt.Errorf("%w: %v", errNameOwnersUnreadable, err)
	}
	if f.Format != nameOwnersFileFormat {
		return fmt.Errorf("%w: format %d is unknown", errNameOwnersUnreadable, f.Format)
	}
	records := make(map[string]nameOwnerRecord, len(f.Records))
	for _, item := range f.Records {
		if item.Name == "" {
			continue
		}
		at, err := time.Parse(time.RFC3339, item.LastHeld)
		if err != nil {
			return fmt.Errorf("%w: record %q has an invalid last_held", errNameOwnersUnreadable, item.Name)
		}
		// A duplicate name keeps the last record; the next write removes it.
		key := item.UID
		if item.AccountKey != nil {
			key = *item.AccountKey
		}
		records[item.Name] = nameOwnerRecord{uid: item.UID, key: key, lastHeld: at}
	}
	s.records = records
	return nil
}

// runNameOwnerSaver writes outside Broker.mu; a change during a write leaves one
// pending signal, so the next write carries it.
func (b *Broker) runNameOwnerSaver() {
	defer close(b.nameOwners.done)
	for {
		select {
		case <-b.ctx.Done():
			return
		case <-b.nameOwners.save:
			b.flushNameOwners()
		}
	}
}

// markNameOwnersDirtyLocked records that the table changed and wakes the saver.
// Callers must hold Broker.mu.
func (b *Broker) markNameOwnersDirtyLocked() {
	if b.nameOwners == nil {
		return
	}
	b.nameOwners.dirty = true
	if b.nameOwners.dir == "" {
		return
	}
	select {
	case b.nameOwners.save <- struct{}{}:
	default:
	}
}

// flushNameOwners writes the current table unless nothing changed; it takes
// Broker.mu itself, so the caller must not hold it.
func (b *Broker) flushNameOwners() {
	s := b.nameOwners
	if s == nil {
		return
	}
	s.writeMu.Lock()
	defer s.writeMu.Unlock()

	b.mu.Lock()
	if !s.dirty {
		b.mu.Unlock()
		return
	}
	records := make(map[string]nameOwnerRecord, len(s.records))
	for name, rec := range s.records {
		records[name] = rec
	}
	held := make(map[string]bool, len(b.agents))
	for _, a := range b.agents {
		held[nameHeldKey(a.name, a.caller.UID)] = true
	}
	now := b.deps.Clock.Now()
	s.dirty = false
	b.mu.Unlock()

	data, err := marshalNameOwners(records, held, now)
	if err != nil {
		b.mu.Lock()
		s.dirty = true
		b.mu.Unlock()
		b.warnNameOwnersWriteFailure(s, err)
		return
	}

	if s.corruptPath != "" {
		if err := renameCorruptNameOwners(s.corruptPath); err != nil {
			b.mu.Lock()
			s.dirty = true
			b.mu.Unlock()
			b.warnNameOwnersWriteFailure(s, err)
			return
		}
		s.corruptPath = ""
	}

	if err := writeNameOwnersFile(s.path(), data); err != nil {
		b.mu.Lock()
		s.dirty = true
		b.mu.Unlock()
		b.warnNameOwnersWriteFailure(s, err)
		return
	}
	s.writeWarned = false
}

// warnNameOwnersWriteFailure logs one write failure per burst. Callers must
// hold s.writeMu.
func (b *Broker) warnNameOwnersWriteFailure(s *nameOwnerState, err error) {
	if s.writeWarned {
		return
	}
	s.writeWarned = true
	b.log.Warn(EvNameOwnersWriteFailed, "path", s.path(), "error", err)
}

func nameHeldKey(name string, uid uint32) string {
	return name + "\x00" + strconv.FormatUint(uint64(uid), 10)
}

// Runs outside Broker.mu, so a slow encode never blocks the agent table.
func marshalNameOwners(records map[string]nameOwnerRecord, held map[string]bool, now time.Time) ([]byte, error) {
	names := make([]string, 0, len(records))
	for name := range records {
		names = append(names, name)
	}
	sort.Strings(names)
	items := make([]nameOwnersFileItem, 0, len(names))
	for _, name := range names {
		rec := records[name]
		at := rec.lastHeld
		if held[nameHeldKey(name, rec.uid)] {
			at = now
		}
		item := nameOwnersFileItem{
			Name:     name,
			UID:      rec.uid,
			LastHeld: at.UTC().Format(time.RFC3339),
		}
		if rec.key != rec.uid {
			key := rec.key
			item.AccountKey = &key
		}
		items = append(items, item)
	}
	data, err := json.Marshal(nameOwnersFile{Format: nameOwnersFileFormat, Records: items})
	if err != nil {
		return nil, fmt.Errorf("cannot encode the name owner table: %w", err)
	}
	return data, nil
}

func renameCorruptNameOwners(path string) error {
	target := path + ".corrupt-" + time.Now().UTC().Format("20060102T150405.000000000Z")
	if err := os.Rename(path, target); err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			return nil
		}
		return fmt.Errorf("cannot rename the unreadable %s to %s: %w", path, target, err)
	}
	return nil
}

func writeNameOwnersFile(path string, data []byte) error {
	dir := filepath.Dir(path)
	f, err := os.CreateTemp(dir, "."+nameOwnersFileName+".tmp-*")
	if err != nil {
		return fmt.Errorf("cannot create a temporary file in %s: %w", dir, err)
	}
	tmp := f.Name()
	done := false
	defer func() {
		if !done {
			_ = os.Remove(tmp)
		}
	}()
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return fmt.Errorf("cannot write %s: %w", tmp, err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("cannot sync %s: %w", tmp, err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("cannot close %s: %w", tmp, err)
	}
	if err := os.Chmod(tmp, 0o600); err != nil {
		return fmt.Errorf("cannot set the mode of %s: %w", tmp, err)
	}
	if err := os.Rename(tmp, path); err != nil {
		return fmt.Errorf("cannot rename %s to %s: %w", tmp, path, err)
	}
	done = true
	d, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cannot open %s: %w", dir, err)
	}
	if err := d.Sync(); err != nil {
		_ = d.Close()
		return fmt.Errorf("cannot sync %s: %w", dir, err)
	}
	if err := d.Close(); err != nil {
		return fmt.Errorf("cannot close %s: %w", dir, err)
	}
	return nil
}

func (b *Broker) stopNameOwners() {
	if b.nameOwners == nil {
		return
	}
	b.mu.Lock()
	timer := b.nameOwners.scanTimer
	b.nameOwners.scanTimer = nil
	b.mu.Unlock()
	if timer != nil {
		timer.Stop()
	}
	if b.nameOwners.dir == "" {
		return
	}
	<-b.nameOwners.done
	b.flushNameOwners()
}

func (b *Broker) armNameOwnerScan() {
	if b.nameOwners == nil {
		return
	}
	timer := b.deps.Clock.AfterFunc(nameOwnerScanInterval, b.nameOwnerScan)
	b.mu.Lock()
	b.nameOwners.scanTimer = timer
	b.mu.Unlock()
}

// nameOwnerScan expires and refreshes name owners and recomputes refs when an
// owner expired, within a single hold of Broker.mu.
func (b *Broker) nameOwnerScan() {
	select {
	case <-b.ctx.Done():
		return
	default:
	}
	b.mu.Lock()
	now := b.deps.Clock.Now()
	changed := b.expireNameOwnersLocked(now)
	b.refreshHeldNameOwnersLocked(now)
	if changed {
		b.recomputeRefsLocked()
	}
	b.mu.Unlock()
	select {
	case <-b.ctx.Done():
		return
	default:
	}
	b.armNameOwnerScan()
}

// nameHeldLocked reports whether uid has an agent currently named name. Callers
// must hold Broker.mu.
func (b *Broker) nameHeldLocked(name string, uid uint32) bool {
	for _, a := range b.agents {
		if a.name == name && a.caller.UID == uid {
			return true
		}
	}
	return false
}

// key carries the holder's account key, so a handed-over entry keeps counting
// against the right share of the table.
type nameHolder struct {
	uid uint32
	key uint32
	seq uint64
	id  AgentID
}

// nameHoldersLocked builds the set of accounts holding each name and each
// name's earliest holder. Callers must hold Broker.mu.
func (b *Broker) nameHoldersLocked() (held map[string]map[uint32]bool, earliest map[string]nameHolder) {
	held = make(map[string]map[uint32]bool, len(b.nameOwners.records))
	earliest = make(map[string]nameHolder, len(b.nameOwners.records))
	for _, a := range b.agents {
		set := held[a.name]
		if set == nil {
			set = make(map[uint32]bool)
			held[a.name] = set
		}
		set[a.caller.UID] = true
		cur, ok := earliest[a.name]
		if !ok || a.seq < cur.seq || (a.seq == cur.seq && a.id < cur.id) {
			earliest[a.name] = nameHolder{uid: a.caller.UID, key: a.accountKey, seq: a.seq, id: a.id}
		}
	}
	return held, earliest
}

// expireNameOwnerLocked expires name's entry when no holder retains it and
// reports whether the table changed. Callers must hold Broker.mu.
func (b *Broker) expireNameOwnerLocked(name string, now time.Time) bool {
	held := make(map[uint32]bool)
	var earliest nameHolder
	found := false
	for _, a := range b.agents {
		if a.name != name {
			continue
		}
		held[a.caller.UID] = true
		if !found || a.seq < earliest.seq || (a.seq == earliest.seq && a.id < earliest.id) {
			earliest = nameHolder{uid: a.caller.UID, key: a.accountKey, seq: a.seq, id: a.id}
			found = true
		}
	}
	var next *nameHolder
	if found {
		next = &earliest
	}
	return b.expireNameOwnerUsingLocked(name, now, held, next)
}

// expireNameOwnerUsingLocked is expireNameOwnerLocked with the holders of name
// already computed. Callers must hold Broker.mu.
func (b *Broker) expireNameOwnerUsingLocked(name string, now time.Time, held map[uint32]bool, earliest *nameHolder) bool {
	rec, ok := b.nameOwners.records[name]
	if !ok {
		return false
	}
	if held[rec.uid] {
		return false
	}
	if now.Sub(rec.lastHeld) < nameOwnerRetention {
		return false
	}
	delete(b.nameOwners.records, name)
	if earliest != nil {
		b.nameOwners.records[name] = nameOwnerRecord{uid: earliest.uid, key: earliest.key, lastHeld: now}
	}
	b.markNameOwnersDirtyLocked()
	return true
}

// expireNameOwnersLocked expires every idle entry and reports whether the table
// changed. Callers must hold Broker.mu.
func (b *Broker) expireNameOwnersLocked(now time.Time) bool {
	held, earliest := b.nameHoldersLocked()
	names := make([]string, 0, len(b.nameOwners.records))
	for name := range b.nameOwners.records {
		names = append(names, name)
	}
	changed := false
	for _, name := range names {
		var next *nameHolder
		if h, ok := earliest[name]; ok {
			h := h
			next = &h
		}
		if b.expireNameOwnerUsingLocked(name, now, held[name], next) {
			changed = true
		}
	}
	if changed {
		b.refreshNameOwnerWarningsLocked()
	}
	return changed
}

// refreshHeldNameOwnersLocked moves the last-held time of every held name to
// now. Callers must hold Broker.mu.
func (b *Broker) refreshHeldNameOwnersLocked(now time.Time) {
	held, _ := b.nameHoldersLocked()
	changed := false
	for name, rec := range b.nameOwners.records {
		if !held[name][rec.uid] {
			continue
		}
		if !rec.lastHeld.Equal(now) {
			rec.lastHeld = now
			b.nameOwners.records[name] = rec
			changed = true
		}
	}
	if changed {
		b.markNameOwnersDirtyLocked()
	}
}

// refreshNameOwnerWarningsLocked re-arms the full-table warning of every account
// that has room again. Callers must hold Broker.mu.
func (b *Broker) refreshNameOwnerWarningsLocked() {
	if len(b.nameOwners.warned) == 0 {
		return
	}
	counts := make(map[uint32]int, len(b.nameOwners.warned))
	for _, rec := range b.nameOwners.records {
		counts[rec.key]++
	}
	for key := range b.nameOwners.warned {
		if counts[key] < b.nameOwners.limit {
			delete(b.nameOwners.warned, key)
		}
	}
}

// recordNameOwnerLocked records uid as name's owner and reports whether the
// table changed; key must be computed outside Broker.mu so an account lookup
// never runs under the lock. Callers must hold Broker.mu.
func (b *Broker) recordNameOwnerLocked(name string, uid, key uint32, now time.Time) bool {
	if b.nameOwners == nil {
		return false
	}
	b.expireNameOwnerLocked(name, now)
	if _, ok := b.nameOwners.records[name]; ok {
		return false
	}
	if !b.makeRoomForNameOwnerLocked(uid, key) {
		return false
	}
	b.nameOwners.records[name] = nameOwnerRecord{uid: uid, key: key, lastHeld: now}
	b.markNameOwnersDirtyLocked()
	return true
}

// makeRoomForNameOwnerLocked evicts idle entries until uid may record one more
// name. Callers must hold Broker.mu.
func (b *Broker) makeRoomForNameOwnerLocked(uid, key uint32) bool {
	held, _ := b.nameHoldersLocked()
	count := 0
	for _, rec := range b.nameOwners.records {
		if rec.key == key {
			count++
		}
	}
	if count >= b.nameOwners.limit {
		victim, ok := b.oldestIdleNameOwnerLocked(func(rec nameOwnerRecord) bool { return rec.key == key }, held)
		if !ok {
			if !b.nameOwners.warned[key] {
				b.log.Warn(EvNameOwnerTableFull, "uid", uid, "account_key", key, "limit", b.nameOwners.limit)
				b.nameOwners.warned[key] = true
			}
			return false
		}
		delete(b.nameOwners.records, victim)
	} else {
		delete(b.nameOwners.warned, key)
	}

	if b.nameOwners.tableLimit > 0 && len(b.nameOwners.records) >= b.nameOwners.tableLimit {
		if victim, ok := b.oldestIdleNameOwnerLocked(func(nameOwnerRecord) bool { return true }, held); ok {
			delete(b.nameOwners.records, victim)
		} else {
			if !b.nameOwners.warned[key] {
				b.log.Warn(EvNameOwnerTableFull, "uid", uid, "account_key", key, "limit", b.nameOwners.tableLimit)
				b.nameOwners.warned[key] = true
			}
			return false
		}
	}
	return true
}

// oldestIdleNameOwnerLocked finds the oldest idle accepted entry; the name
// breaks a same-time tie. Callers must hold Broker.mu.
func (b *Broker) oldestIdleNameOwnerLocked(accept func(nameOwnerRecord) bool, held map[string]map[uint32]bool) (string, bool) {
	var (
		victim   string
		victimAt time.Time
	)
	for name, rec := range b.nameOwners.records {
		if held[name][rec.uid] || !accept(rec) {
			continue
		}
		if victim == "" || rec.lastHeld.Before(victimAt) ||
			(rec.lastHeld.Equal(victimAt) && name < victim) {
			victim, victimAt = name, rec.lastHeld
		}
	}
	return victim, victim != ""
}

// releaseNameOwnerLocked records that uid's last agent named name stopped
// holding it. Callers must hold Broker.mu.
func (b *Broker) releaseNameOwnerLocked(name string, uid uint32, now time.Time) {
	if b.nameOwners == nil {
		return
	}
	rec, ok := b.nameOwners.records[name]
	if !ok || rec.uid != uid {
		return
	}
	if b.nameHeldLocked(name, uid) {
		return
	}
	rec.lastHeld = now
	b.nameOwners.records[name] = rec
	b.markNameOwnersDirtyLocked()
}

// nameOwnerOfLocked returns the account name belongs to, expiring an idle entry
// first. Callers must hold Broker.mu.
func (b *Broker) nameOwnerOfLocked(name string, now time.Time) (uint32, bool) {
	if b.nameOwners == nil {
		return 0, false
	}
	if b.expireNameOwnerLocked(name, now) {
		// A read cannot recompute here (the caller may be iterating the table),
		// so record that a push is owed for the next snapshot rebuild.
		b.nameOwnerPushPending = true
	}
	rec, ok := b.nameOwners.records[name]
	return rec.uid, ok
}
