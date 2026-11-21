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
	// nameOwnersFileName is the name ownership table kept in the data
	// directory.
	nameOwnersFileName = "name-owners.json"
	// nameOwnersFileFormat is the only file format this version writes and
	// reads.
	nameOwnersFileFormat = 1
	// nameOwnerRetention is how long a name keeps its owner after the owner's
	// last agent stopped holding it.
	nameOwnerRetention = 7 * 24 * time.Hour
	// nameOwnerScanInterval is how often the whole table is swept for expired
	// entries. A lookup also expires the name it asks about, so a release is
	// noticed within this interval and precisely at a query.
	nameOwnerScanInterval = time.Hour
	// nameOwnerPerAccountLimit is the number of names one account may own.
	nameOwnerPerAccountLimit = 1024
	// nameOwnerTableLimit bounds the whole table. The per-account limit bounds
	// one account, but a system Broker serves many accounts, so the table still
	// needs a total bound to keep memory and the file size finite. 65536 names
	// is far above what real sessions hold, small enough to read and write in
	// one pass, and reached only by an account churn that the oldest-idle
	// eviction below then contains.
	nameOwnerTableLimit = 65536
)

// errNameOwnersUnreadable reports that name-owners.json exists and can be read
// but cannot be used: its content does not parse, its format is unknown, or a
// last_held value is not an RFC 3339 time. The Broker logs it and starts with an
// empty table. A read failure is returned as itself so startup fails instead of
// silently replacing a file the Broker could not read.
var errNameOwnersUnreadable = errors.New("name-owners.json is unreadable")

// nameOwnerRecord is one name's ownership: the account that owns it and when
// that account last held it. lastHeld is only meaningful while the entry is
// idle; a held entry is stored with the moment it was written.
//
// key is the account this entry counts against for the per-account limit, the
// accountKeyFor result of uid. It equals uid for a real account and is
// unknownAccountKey for a uid with no account entry, so every such uid shares
// one account's share of the table instead of getting a private one. uid stays
// the owner the address rules use; key only drives the limit.
type nameOwnerRecord struct {
	uid      uint32
	key      uint32
	lastHeld time.Time
}

// nameOwnersFile is the on-disk form of the table. Records are sorted by name
// so that equal content produces equal bytes.
type nameOwnersFile struct {
	Format  int                  `json:"format"`
	Records []nameOwnersFileItem `json:"records"`
}

// nameOwnersFileItem is one record as stored on disk. LastHeld is an RFC 3339
// timestamp. AccountKey is written only when the entry counts against a key
// other than its uid, that is for a uid with no account entry; a file that
// predates the field, or an entry of a real account, omits it and is counted
// by uid.
type nameOwnersFileItem struct {
	Name       string  `json:"name"`
	UID        uint32  `json:"uid"`
	LastHeld   string  `json:"last_held"`
	AccountKey *uint32 `json:"account_key,omitempty"`
}

// nameOwnerState is the Broker's name ownership table together with the state
// of its persistence. records, warned, limit, dirty and scanTimer are guarded
// by Broker.mu; the save machinery has its own fields.
type nameOwnerState struct {
	records map[string]nameOwnerRecord
	// warned records the accounts already warned about a full table, so one
	// account logs one name_owner_table_full until it falls below the limit.
	warned map[uint32]bool
	// limit is nameOwnerPerAccountLimit in production. It is a field so unit
	// tests can exercise the limit without building a thousand entries.
	limit int
	// tableLimit is nameOwnerTableLimit in production, a field for the same
	// reason.
	tableLimit int
	dirty      bool
	// corruptPath holds the path of an unreadable name-owners file that must be
	// renamed aside before the next write. It is set at load and read by the
	// saver under writeMu.
	corruptPath string
	// writeWarned suppresses repeated write-failure warnings until a write
	// succeeds. It is guarded by writeMu.
	writeWarned bool

	// dir is the data directory. An empty dir disables persistence, which is
	// what the broker's unit tests get from their fake configuration.
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

// path returns the location of the table in the data directory.
func (s *nameOwnerState) path() string {
	return filepath.Join(s.dir, nameOwnersFileName)
}

// initNameOwners builds the table, reads it back and starts the save loop and
// the hourly sweep. It is called by New after the configuration is loaded, so
// the data directory is known. An unreadable file is logged and replaced by an
// empty table; a read failure is returned as a data-unavailable error and fails
// startup.
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

// load reads the table from disk. A missing file leaves the table empty. The
// caller decides how to treat errNameOwnersUnreadable: the Broker logs it and
// starts empty.
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
		// A duplicate name keeps the last record; a hand-edited file has no
		// better answer and the next write removes the duplicate. An entry
		// without account_key counts against its uid, which is what a real
		// account's key is; only an entry written for a uid with no account
		// entry carries the shared unknown key.
		key := item.UID
		if item.AccountKey != nil {
			key = *item.AccountKey
		}
		records[item.Name] = nameOwnerRecord{uid: item.UID, key: key, lastHeld: at}
	}
	s.records = records
	return nil
}

// runNameOwnerSaver writes the table whenever a change asks for it, until the
// Broker stops. Writes happen outside Broker.mu; a change that arrives during a
// write leaves one pending signal, so the next write carries it.
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

// flushNameOwners writes the current table unless nothing changed. It takes a
// snapshot of the records and the held names under Broker.mu in one pass, then
// sorts, encodes and writes outside it, so a huge table or a slow disk never
// blocks the agent table. A failed write leaves the table dirty so the next
// change or the shutdown flush retries it, and only the first failure of a
// burst is logged as a warning.
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
	// Build the held set in one pass over the agents instead of scanning the
	// whole agent table once per record.
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

// nameHeldKey identifies one (name, account) pair in the held set.
func nameHeldKey(name string, uid uint32) string {
	return name + "\x00" + strconv.FormatUint(uint64(uid), 10)
}

// marshalNameOwners renders the snapshot in file format, sorted by name. A held
// entry is written with now, the moment this write takes place; an idle entry
// keeps the moment its owner last held it. It runs outside Broker.mu.
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

// renameCorruptNameOwners moves an unreadable table aside so the next write does
// not overwrite it. A file that is already gone is not an error.
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

// writeNameOwnersFile writes data to a temporary file next to path, syncs it,
// renames it over path and syncs the directory. The temporary file is created
// with mode 0600.
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

// stopNameOwners stops the sweep and the save loop and writes the final table.
// It is safe to call more than once.
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

// armNameOwnerScan schedules the next hourly sweep.
func (b *Broker) armNameOwnerScan() {
	if b.nameOwners == nil {
		return
	}
	timer := b.deps.Clock.AfterFunc(nameOwnerScanInterval, b.nameOwnerScan)
	b.mu.Lock()
	b.nameOwners.scanTimer = timer
	b.mu.Unlock()
}

// nameOwnerScan sweeps expired entries and re-arms itself. It does nothing once
// the Broker is stopping.
func (b *Broker) nameOwnerScan() {
	select {
	case <-b.ctx.Done():
		return
	default:
	}
	b.mu.Lock()
	now := b.deps.Clock.Now()
	changed := b.expireNameOwnersLocked(now)
	// Refresh the last held time of every name still held, and write the table:
	// without this sweep a name held for weeks would be released on the next
	// restart, because the file still carried the moment it was first held.
	b.refreshHeldNameOwnersLocked(now)
	if changed {
		// An expired entry can hand the name to another account, which changes
		// the addresses that carry a ref; push the change through the same path
		// as every other address change.
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

// nameHolder is one account holding a name and the registration order of its
// earliest agent, used to hand an expired name to the account that registered
// first. key is the holder agent's account key, carried so the handed-over
// entry keeps counting against the right share of the table.
type nameHolder struct {
	uid uint32
	key uint32
	seq uint64
	id  AgentID
}

// nameHoldersLocked builds, in one pass over the agent table, the set of
// accounts holding each name and the earliest registered holder of that name.
// Callers must hold Broker.mu.
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

// expireNameOwnerLocked drops name's entry when it is past the retention
// period and nobody owned by it holds the name. If another account currently
// holds the name, the entry moves to the earliest registered of those agents.
// It reports whether the table changed. Callers must hold Broker.mu.
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
// already computed, so a sweep builds them once instead of scanning the agent
// table per entry.
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

// expireNameOwnersLocked sweeps every entry and clears the warning flag of each
// account that fell below its limit. It reports whether the table changed. The
// holders are computed once, so the sweep is one pass over the agents and one
// over the records instead of one agent scan per record. Callers must hold
// Broker.mu.
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

// refreshHeldNameOwnersLocked moves the last held time of every name its owner
// still holds to now and marks the table dirty, so the hourly sweep writes a
// fresh timestamp. A restart then counts the retention from the last write
// rather than from the moment the name was first registered. Callers must hold
// Broker.mu.
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

// refreshNameOwnerWarningsLocked re-arms the full-table warning of every
// account that now has room again. Callers must hold Broker.mu.
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

// recordNameOwnerLocked gives name to uid when the name has no owner, or when
// its owner's entry has expired. An unexpired entry keeps its owner. key is the
// account the entry counts against for the per-account limit, the
// accountKeyFor result of uid; the caller computes it outside Broker.mu so an
// account lookup never runs under the lock. It reports whether the table
// changed. Callers must hold Broker.mu.
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

// makeRoomForNameOwnerLocked keeps one account within its share of the table.
// The share is counted by account key, not by uid, so every uid with no account
// entry shares one share under unknownAccountKey and a rotating subuid cannot
// multiply it. When the account is at the limit and every entry it owns is
// held, it reports false and logs one warning per account. Otherwise it evicts
// the account's oldest idle entry. Callers must hold Broker.mu.
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

	// The whole table has its own bound: the per-account share limits one
	// account, but a system Broker serves many. Evict the globally oldest idle
	// entry; when every entry is held, do not record the new name, which falls
	// back to the old rule for it (no owner, so the same name may be taken by
	// another account until it is recorded). The account still counts as full
	// for the warning.
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

// oldestIdleNameOwnerLocked finds the oldest entry whose account is not holding
// the name and that accept reports true for. The name breaks a same-time tie.
// Callers must hold Broker.mu.
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
// holding it. The entry keeps its owner; only the last held time moves.
// Callers must hold Broker.mu.
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

// nameOwnerOfLocked returns the account name currently belongs to after
// expiring an idle entry that is past its retention period. Callers must hold
// Broker.mu.
func (b *Broker) nameOwnerOfLocked(name string, now time.Time) (uint32, bool) {
	if b.nameOwners == nil {
		return 0, false
	}
	if b.expireNameOwnerLocked(name, now) {
		// The read moved ownership. Resolving here cannot recompute (the caller
		// may be iterating the table), so record that a push is owed; the next
		// snapshot rebuild performs it.
		b.nameOwnerPushPending = true
	}
	rec, ok := b.nameOwners.records[name]
	return rec.uid, ok
}
