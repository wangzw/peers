package broker

import (
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/accountdb"
	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// nameOwnerTest is a Broker built only for the name ownership table: no local
// interface, a fake clock and a data directory that may be empty.
type nameOwnerTest struct {
	b     *Broker
	clock *fakeClock
	logs  *syncBuffer
	cfg   *fakeConfig
}

// nameOwnerConfig reports dataDir as the data directory on top of the fake
// configuration. The rest of ConfigSource is unchanged.
type nameOwnerConfig struct {
	*fakeConfig
	dataDir string
}

func (c nameOwnerConfig) Paths() config.PathSet {
	p := c.fakeConfig.Paths()
	p.DataDir = c.dataDir
	return p
}

func newNameOwnerTest(t *testing.T, dataDir string) *nameOwnerTest {
	t.Helper()
	return newNameOwnerTestAt(t, dataDir, time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC))
}

// newNameOwnerTestAt builds the same Broker with the clock starting at now, so
// a restart test can reboot at the moment the previous Broker stopped.
func newNameOwnerTestAt(t *testing.T, dataDir string, now time.Time) *nameOwnerTest {
	t.Helper()
	clock := newFakeClock(now)
	logs := &syncBuffer{}
	level := new(slog.LevelVar)
	level.Set(slog.LevelDebug)
	cfg := newFakeConfig(t.TempDir())
	b, err := New(Deps{
		Config:    nameOwnerConfig{fakeConfig: cfg, dataDir: dataDir},
		Topics:    newFakeTopics(),
		Clock:     clock,
		Logger:    NewLogger(logs, level),
		InSession: func() bool { return false },
	})
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	t.Cleanup(b.Stop)
	return &nameOwnerTest{b: b, clock: clock, logs: logs, cfg: cfg}
}

// addNameOwnerAgent inserts an agent into the table without going through
// registration, so a test can build a multi-account table. Callers must hold
// Broker.mu.
func addNameOwnerAgent(b *Broker, name string, uid uint32, seq uint64) AgentID {
	id := AgentID(fmt.Sprintf("%064x", seq+1))
	b.agents[id] = &agent{
		id:         id,
		name:       name,
		caller:     Caller{UID: uid},
		accountKey: uid,
		seq:        seq,
		public:     make(map[string]string),
		conns:      make(map[*regConn]struct{}),
	}
	return id
}

// removeNameOwnerAgent takes an agent out through the real unregistration path.
func removeNameOwnerAgent(b *Broker, id AgentID) {
	b.mu.Lock()
	a := b.agents[id]
	if a == nil {
		b.mu.Unlock()
		return
	}
	stops := b.unregisterLocked(a)
	b.unlockAndNotify(stops)
}

// recordNameOwner records name for a test account whose key is its uid, the way
// a real account counts.
func recordNameOwner(b *Broker, name string, uid uint32) {
	recordNameOwnerKey(b, name, uid, uid)
}

// recordNameOwnerKey records name for uid counting it against key, so a test
// can make uids with no account entry share unknownAccountKey.
func recordNameOwnerKey(b *Broker, name string, uid, key uint32) {
	b.mu.Lock()
	b.recordNameOwnerLocked(name, uid, key, b.deps.Clock.Now())
	b.mu.Unlock()
}

// seedNameOwner writes an entry directly, as a table loaded from disk would
// hold it, and marks the table dirty. A file without account_key is counted by
// uid, so the seeded record's key is its uid.
func seedNameOwner(b *Broker, name string, uid uint32, at time.Time) {
	seedNameOwnerKey(b, name, uid, uid, at)
}

// seedNameOwnerKey writes an entry that counts against key, so a test can seed
// one written for a uid with no account entry.
func seedNameOwnerKey(b *Broker, name string, uid, key uint32, at time.Time) {
	b.mu.Lock()
	b.nameOwners.records[name] = nameOwnerRecord{uid: uid, key: key, lastHeld: at}
	b.markNameOwnersDirtyLocked()
	b.mu.Unlock()
}

func nameOwnerEntry(b *Broker, name string) (uid uint32, lastHeld time.Time, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.nameOwners.records[name]
	return rec.uid, rec.lastHeld, ok
}

// nameOwnerKey returns the account key an entry counts against.
func nameOwnerKey(b *Broker, name string) (key uint32, ok bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	rec, ok := b.nameOwners.records[name]
	return rec.key, ok
}

func (tb *testBroker) nameOwnerEntry(t *testing.T, name string) (uid uint32, lastHeld time.Time, ok bool) {
	t.Helper()
	return nameOwnerEntry(tb.b, name)
}

// TestNameOwnerKeepsFirstAccount covers the rule that a second account
// registering a name after the owner left does not take it over.
func TestNameOwnerKeepsFirstAccount(t *testing.T) {
	nb := newNameOwnerTest(t, "")
	b := nb.b
	start := nb.clock.Now()

	b.mu.Lock()
	first := addNameOwnerAgent(b, "worker", 1001, 1)
	b.mu.Unlock()
	recordNameOwner(b, "worker", 1001)

	removeNameOwnerAgent(b, first)

	// uid 1001 has just left; uid 1002 registers the same name.
	b.mu.Lock()
	addNameOwnerAgent(b, "worker", 1002, 2)
	b.mu.Unlock()
	recordNameOwner(b, "worker", 1002)

	uid, lastHeld, ok := nameOwnerEntry(b, "worker")
	if !ok || uid != 1001 {
		t.Fatalf("owner = %d (present %v), want 1001", uid, ok)
	}
	if !lastHeld.Equal(start) {
		t.Fatalf("last_held = %s, want the moment the first owner left (%s)", lastHeld, start)
	}
}

// TestNameOwnerExpiresAfterSevenDays checks the exact boundary: one second
// short of seven days the first account still owns the name, and at seven days
// a query hands it to the account that holds it now.
func TestNameOwnerExpiresAfterSevenDays(t *testing.T) {
	nb := newNameOwnerTest(t, "")
	b := nb.b

	b.mu.Lock()
	first := addNameOwnerAgent(b, "worker", 1001, 1)
	b.mu.Unlock()
	recordNameOwner(b, "worker", 1001)
	removeNameOwnerAgent(b, first)

	b.mu.Lock()
	addNameOwnerAgent(b, "worker", 1002, 2)
	b.mu.Unlock()
	recordNameOwner(b, "worker", 1002)

	// The boundary is spelled out rather than taken from the constant, so a
	// change to the retention itself fails this test.
	const sevenDays = 7 * 24 * time.Hour
	nb.clock.Advance(sevenDays - time.Second)
	b.mu.Lock()
	uid, ok := b.nameOwnerOfLocked("worker", nb.clock.Now())
	b.mu.Unlock()
	if !ok || uid != 1001 {
		t.Fatalf("owner one second before the retention ended = %d (present %v), want 1001", uid, ok)
	}

	nb.clock.Advance(time.Second)
	b.mu.Lock()
	uid, ok = b.nameOwnerOfLocked("worker", nb.clock.Now())
	b.mu.Unlock()
	if !ok || uid != 1002 {
		t.Fatalf("owner at the retention boundary = %d (present %v), want 1002", uid, ok)
	}
}

// TestNameOwnerScanExpiresIdleEntry checks the hourly sweep removes an entry
// nobody holds.
func TestNameOwnerScanExpiresIdleEntry(t *testing.T) {
	nb := newNameOwnerTest(t, "")
	b := nb.b

	b.mu.Lock()
	first := addNameOwnerAgent(b, "worker", 1001, 1)
	b.mu.Unlock()
	recordNameOwner(b, "worker", 1001)
	removeNameOwnerAgent(b, first)

	nb.clock.Advance(nameOwnerRetention)

	if _, _, ok := nameOwnerEntry(b, "worker"); ok {
		t.Fatal("the sweep left an idle entry past the retention period")
	}
}

// TestNameOwnerHeldEntryDoesNotExpire checks that an entry an account still
// holds survives the sweep no matter how long the account keeps it.
func TestNameOwnerHeldEntryDoesNotExpire(t *testing.T) {
	nb := newNameOwnerTest(t, "")
	b := nb.b

	b.mu.Lock()
	addNameOwnerAgent(b, "worker", 1001, 1)
	b.mu.Unlock()
	recordNameOwner(b, "worker", 1001)

	nb.clock.Advance(2 * nameOwnerRetention)

	uid, _, ok := nameOwnerEntry(b, "worker")
	if !ok || uid != 1001 {
		t.Fatalf("owner of a held entry = %d (present %v), want 1001", uid, ok)
	}
}

// TestNameOwnerLimitEvictsOldestIdle covers the eviction order and the rule
// that a held entry is never evicted.
func TestNameOwnerLimitEvictsOldestIdle(t *testing.T) {
	nb := newNameOwnerTest(t, "")
	b := nb.b
	start := nb.clock.Now()
	b.mu.Lock()
	b.nameOwners.limit = 3
	b.mu.Unlock()

	seedNameOwner(b, "e1", 1001, start)
	seedNameOwner(b, "e2", 1001, start.Add(time.Second))
	seedNameOwner(b, "e3", 1001, start.Add(2*time.Second))

	b.mu.Lock()
	addNameOwnerAgent(b, "new", 1001, 1)
	b.mu.Unlock()
	recordNameOwner(b, "new", 1001)

	if _, _, ok := nameOwnerEntry(b, "e1"); ok {
		t.Fatal("the oldest idle entry was not evicted")
	}
	for _, name := range []string{"e2", "e3", "new"} {
		if _, _, ok := nameOwnerEntry(b, name); !ok {
			t.Fatalf("entry %q disappeared; want it kept", name)
		}
	}

	// Hold e2, then ask for a second new name: e3 is the oldest idle entry and
	// must go, while the held e2 stays.
	b.mu.Lock()
	addNameOwnerAgent(b, "e2", 1001, 2)
	b.mu.Unlock()
	b.mu.Lock()
	addNameOwnerAgent(b, "new2", 1001, 3)
	b.mu.Unlock()
	recordNameOwner(b, "new2", 1001)

	if _, _, ok := nameOwnerEntry(b, "e3"); ok {
		t.Fatal("the oldest idle entry was not evicted after e2 became held")
	}
	if _, _, ok := nameOwnerEntry(b, "e2"); !ok {
		t.Fatal("a held entry was evicted")
	}
	if _, _, ok := nameOwnerEntry(b, "new2"); !ok {
		t.Fatal("the new entry was not recorded")
	}
}

// TestNameOwnerFullTableWarnsOnce covers the case where every entry of an
// account is held: the new name is not recorded and one warning is written.
func TestNameOwnerFullTableWarnsOnce(t *testing.T) {
	nb := newNameOwnerTest(t, "")
	b := nb.b
	b.mu.Lock()
	b.nameOwners.limit = 2
	b.mu.Unlock()

	for i, name := range []string{"a", "b", "c", "d"} {
		b.mu.Lock()
		addNameOwnerAgent(b, name, 1001, uint64(i+1))
		b.mu.Unlock()
		recordNameOwner(b, name, 1001)
	}

	if _, _, ok := nameOwnerEntry(b, "a"); !ok {
		t.Fatal("held entry a was not recorded")
	}
	if _, _, ok := nameOwnerEntry(b, "b"); !ok {
		t.Fatal("held entry b was not recorded")
	}
	for _, name := range []string{"c", "d"} {
		if _, _, ok := nameOwnerEntry(b, name); ok {
			t.Fatalf("%q was recorded although every entry was held", name)
		}
	}

	var warnings int
	for _, line := range decodeLogLines(t, nb.logs.String()) {
		if line["event"] != EvNameOwnerTableFull {
			continue
		}
		warnings++
		if got := line["uid"]; got != float64(1001) {
			t.Fatalf("warning uid = %v, want 1001", got)
		}
		if got := line["limit"]; got != float64(2) {
			t.Fatalf("warning limit = %v, want 2", got)
		}
	}
	if warnings != 1 {
		t.Fatalf("table-full warnings = %d, want 1", warnings)
	}
}

// TestNameOwnerQuotaIsPerAccount checks one account cannot take another's
// share of the table.
func TestNameOwnerQuotaIsPerAccount(t *testing.T) {
	nb := newNameOwnerTest(t, "")
	b := nb.b
	b.mu.Lock()
	b.nameOwners.limit = 1
	b.mu.Unlock()

	for i, tc := range []struct {
		name string
		uid  uint32
	}{{"mine", 1001}, {"theirs", 1002}} {
		b.mu.Lock()
		addNameOwnerAgent(b, tc.name, tc.uid, uint64(i+1))
		b.mu.Unlock()
		recordNameOwner(b, tc.name, tc.uid)
	}
	if _, _, ok := nameOwnerEntry(b, "theirs"); !ok {
		t.Fatal("the second account could not record a name at its own limit")
	}

	b.mu.Lock()
	addNameOwnerAgent(b, "extra", 1001, 3)
	b.mu.Unlock()
	recordNameOwner(b, "extra", 1001)

	if _, _, ok := nameOwnerEntry(b, "extra"); ok {
		t.Fatal("the first account exceeded its limit")
	}
	if _, _, ok := nameOwnerEntry(b, "theirs"); !ok {
		t.Fatal("the first account took the second account's entry")
	}
}

// TestNameOwnerUnknownUIDsShareLimit checks that uids with no account entry
// count against one shared account key: N such uids get one account's share of
// the table between them, not N shares. Counting by uid would record every one
// of them, so the old rule fails this test.
func TestNameOwnerUnknownUIDsShareLimit(t *testing.T) {
	resetUsernameCache(t)
	lookupUsername = func(uint32) (accountdb.User, error) {
		return accountdb.User{}, accountdb.ErrNotFound
	}
	usernameNow = func() time.Time { return time.Date(2026, 11, 22, 0, 0, 0, 0, time.UTC) }

	nb := newNameOwnerTest(t, "")
	b := nb.b
	const limit = 3
	b.mu.Lock()
	b.nameOwners.limit = limit
	b.nameOwners.tableLimit = 100
	b.mu.Unlock()

	var names []string
	for i := 0; i < 10; i++ {
		uid := uint32(500001 + i)
		key, err := accountKeyFor(uid)
		if err != nil {
			t.Fatalf("accountKeyFor(%d): %v", uid, err)
		}
		if key != unknownAccountKey {
			t.Fatalf("accountKeyFor(%d) = %d, want the shared unknown key %d", uid, key, unknownAccountKey)
		}
		name := fmt.Sprintf("n%06d", uid)
		recordNameOwnerKey(b, name, uid, key)
		names = append(names, name)
	}

	// Every entry is idle, so each name past the shared share evicts the oldest
	// one and exactly the last limit names remain.
	want := append([]string(nil), names[len(names)-limit:]...)

	b.mu.Lock()
	got := make([]string, 0, len(b.nameOwners.records))
	for name, rec := range b.nameOwners.records {
		if rec.key != unknownAccountKey {
			b.mu.Unlock()
			t.Fatalf("entry %q counts against key %d, want the shared unknown key %d", name, rec.key, unknownAccountKey)
		}
		got = append(got, name)
	}
	b.mu.Unlock()
	sort.Strings(got)
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("records = %v, want the last %d names %v; the ten unknown uids did not share one account's share",
			got, limit, want)
	}
}

// TestNameOwnerPersistsUnknownAccountKey checks that an entry written for a uid
// with no account entry keeps the shared key across a restart, so its records
// still count against one share.
func TestNameOwnerPersistsUnknownAccountKey(t *testing.T) {
	dataDir := t.TempDir()
	nb := newNameOwnerTest(t, dataDir)
	seedNameOwnerKey(nb.b, "shared", 700001, unknownAccountKey, nb.clock.Now())
	nb.b.Stop()

	reloaded := newNameOwnerTest(t, dataDir)
	key, ok := nameOwnerKey(reloaded.b, "shared")
	if !ok {
		t.Fatal("the reloaded table lost the entry")
	}
	if key != unknownAccountKey {
		t.Fatalf("reloaded key = %d, want the shared unknown key %d", key, unknownAccountKey)
	}
}

// TestNameOwnerPersistsAndReloads covers the file format, its permissions, the
// stable order and reading the table back into a new Broker.
func TestNameOwnerPersistsAndReloads(t *testing.T) {
	dataDir := t.TempDir()
	nb := newNameOwnerTest(t, dataDir)
	b := nb.b
	start := nb.clock.Now()

	b.mu.Lock()
	addNameOwnerAgent(b, "alpha", 1001, 1)
	b.mu.Unlock()
	seedNameOwner(b, "alpha", 1001, start)
	seedNameOwner(b, "zulu", 1001, start)

	nb.clock.Advance(30 * time.Minute)
	writeAt := nb.clock.Now()

	// A third entry at the new instant triggers a write. The held entry must
	// be stored with the write time, the idle entry with its last held time.
	seedNameOwner(b, "yankee", 1002, writeAt)
	nb.b.Stop()

	path := filepath.Join(dataDir, nameOwnersFileName)
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat %s: %v", path, err)
	}
	if got := fi.Mode().Perm(); got != 0o600 {
		t.Fatalf("mode = %#o, want 0600", got)
	}

	var f struct {
		Format  int `json:"format"`
		Records []struct {
			Name     string `json:"name"`
			UID      uint32 `json:"uid"`
			LastHeld string `json:"last_held"`
		} `json:"records"`
	}
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("decode %s: %v\n%s", path, err, data)
	}
	if f.Format != nameOwnersFileFormat {
		t.Fatalf("format = %d, want %d", f.Format, nameOwnersFileFormat)
	}
	var names []string
	for _, rec := range f.Records {
		names = append(names, rec.Name)
	}
	wantNames := []string{"alpha", "yankee", "zulu"}
	if fmt.Sprint(names) != fmt.Sprint(wantNames) {
		t.Fatalf("records = %v, want %v", names, wantNames)
	}
	byName := make(map[string]string, len(f.Records))
	for _, rec := range f.Records {
		byName[rec.Name] = rec.LastHeld
	}
	if got, want := byName["alpha"], writeAt.Format(time.RFC3339); got != want {
		t.Fatalf("held entry last_held = %q, want the write time %q", got, want)
	}
	if got, want := byName["zulu"], start.Format(time.RFC3339); got != want {
		t.Fatalf("idle entry last_held = %q, want %q", got, want)
	}
	if got, want := byName["yankee"], writeAt.Format(time.RFC3339); got != want {
		t.Fatalf("second account last_held = %q, want %q", got, want)
	}

	// A new Broker with the same data directory reads the table back.
	reloaded := newNameOwnerTest(t, dataDir)
	for _, tc := range []struct {
		name string
		uid  uint32
		at   time.Time
	}{
		{"alpha", 1001, writeAt},
		{"zulu", 1001, start},
		{"yankee", 1002, writeAt},
	} {
		uid, lastHeld, ok := nameOwnerEntry(reloaded.b, tc.name)
		if !ok || uid != tc.uid {
			t.Fatalf("reloaded owner of %q = %d (present %v), want %d", tc.name, uid, ok, tc.uid)
		}
		if !lastHeld.Equal(tc.at) {
			t.Fatalf("reloaded last_held of %q = %s, want %s", tc.name, lastHeld, tc.at)
		}
	}
}

// TestNameOwnerUnreadableFileStartsEmpty covers a file that parses to nothing
// usable: the Broker logs a warning and starts with an empty table.
func TestNameOwnerUnreadableFileStartsEmpty(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{"not JSON", "{not json"},
		{"unknown format", `{"format":2,"records":[]}`},
		{"bad timestamp", `{"format":1,"records":[{"name":"worker","uid":1001,"last_held":"yesterday"}]}`},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			dataDir := t.TempDir()
			path := filepath.Join(dataDir, nameOwnersFileName)
			if err := os.WriteFile(path, []byte(tc.content), 0o600); err != nil {
				t.Fatalf("write %s: %v", path, err)
			}
			nb := newNameOwnerTest(t, dataDir)
			if n := len(nb.b.nameOwners.records); n != 0 {
				t.Fatalf("records = %d, want an empty table", n)
			}
			found := false
			for _, line := range decodeLogLines(t, nb.logs.String()) {
				if line["event"] == EvNameOwnersUnreadable {
					found = true
				}
			}
			if !found {
				t.Fatalf("the log has no %s event", EvNameOwnersUnreadable)
			}
		})
	}
}

// TestNameOwnerCorruptFileRenamedAside checks that an unreadable table is kept,
// renamed to name-owners.json.corrupt-<time> just before the next write, instead
// of being overwritten, and that the new file is valid.
func TestNameOwnerCorruptFileRenamedAside(t *testing.T) {
	dataDir := t.TempDir()
	path := filepath.Join(dataDir, nameOwnersFileName)
	if err := os.WriteFile(path, []byte("{not json"), 0o600); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}

	nb := newNameOwnerTest(t, dataDir)
	seedNameOwner(nb.b, "worker", 1001, nb.clock.Now())
	nb.b.Stop()

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read the rewritten %s: %v", path, err)
	}
	var f nameOwnersFile
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("the rewritten table is not valid JSON: %v\n%s", err, data)
	}

	entries, err := os.ReadDir(dataDir)
	if err != nil {
		t.Fatalf("read %s: %v", dataDir, err)
	}
	kept := ""
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), nameOwnersFileName+".corrupt-") {
			kept = e.Name()
		}
	}
	if kept == "" {
		t.Fatalf("the unreadable table was not kept under %s.corrupt-<time>: %v", nameOwnersFileName, entries)
	}
	old, err := os.ReadFile(filepath.Join(dataDir, kept))
	if err != nil {
		t.Fatalf("read %s: %v", kept, err)
	}
	if string(old) != "{not json" {
		t.Fatalf("the kept corrupt file = %q, want the original bytes", old)
	}
}

// TestNameOwnerReadFailureIsDataUnavailable checks that a read failure, as
// opposed to unparsable content, fails New with the data-unavailable class.
func TestNameOwnerReadFailureIsDataUnavailable(t *testing.T) {
	dataDir := t.TempDir()
	// A directory at the file path makes the read fail with something other
	// than fs.ErrNotExist.
	if err := os.Mkdir(filepath.Join(dataDir, nameOwnersFileName), 0o700); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	cfg := nameOwnerConfig{fakeConfig: newFakeConfig(t.TempDir()), dataDir: dataDir}
	b, err := New(Deps{
		Config:    cfg,
		Topics:    newFakeTopics(),
		Clock:     newFakeClock(time.Now()),
		Logger:    NewLogger(&syncBuffer{}, new(slog.LevelVar)),
		InSession: func() bool { return false },
	})
	if err == nil {
		b.Stop()
		t.Fatal("New succeeded with an unreadable name owner table")
	}
	if !errors.Is(err, config.ErrDataUnavailable) {
		t.Fatalf("error = %v, want the data-unavailable class", err)
	}
}

// TestNameOwnerRecordedOnRegistration checks the registration path records the
// caller's account.
func TestNameOwnerRecordedOnRegistration(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "worker"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)

	uid, _, ok := tb.nameOwnerEntry(t, "worker")
	if !ok || uid != uint32(os.Geteuid()) {
		t.Fatalf("owner = %d (present %v), want the caller's account %d", uid, ok, os.Geteuid())
	}
}

// TestNameOwnerMovesOnRename checks a rename records the new name for the same
// account and stamps the old name with the moment it stopped being held.
func TestNameOwnerMovesOnRename(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "worker"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)

	fake.Rename("api-worker")
	agentChanged(t, rs, 3*time.Second)

	euid := uint32(os.Geteuid())
	if uid, _, ok := tb.nameOwnerEntry(t, "api-worker"); !ok || uid != euid {
		t.Fatalf("owner of api-worker = %d (present %v), want %d", uid, ok, euid)
	}
	uid, lastHeld, ok := tb.nameOwnerEntry(t, "worker")
	if !ok || uid != euid {
		t.Fatalf("owner of worker = %d (present %v), want %d", uid, ok, euid)
	}
	if !lastHeld.Equal(tb.clock.Now()) {
		t.Fatalf("worker last_held = %s, want the rename time %s", lastHeld, tb.clock.Now())
	}
}

// TestNameOwnerLastHeldOnUnregister checks the last holder leaving stamps the
// entry with the moment it left.
func TestNameOwnerLastHeldOnUnregister(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "worker"})
	rs := startRegistration(t, tb, fake)
	rs.complete(3 * time.Second)

	rs.cancel()
	waitForReg(t, "the agent to unregister", func() bool { return tb.lockedAgentCount() == 0 })

	uid, lastHeld, ok := tb.nameOwnerEntry(t, "worker")
	if !ok || uid != uint32(os.Geteuid()) {
		t.Fatalf("owner = %d (present %v), want %d", uid, ok, os.Geteuid())
	}
	if !lastHeld.Equal(tb.clock.Now()) {
		t.Fatalf("last_held = %s, want the unregistration time %s", lastHeld, tb.clock.Now())
	}
}

// TestNameOwnerHeldEntrySurvivesRestart registers a name, keeps it held for
// eight days across the hourly sweeps, stops the Broker and starts a new one.
// The account that registers first after the restart must not take the name:
// the sweeps kept the last held time on disk fresh.
func TestNameOwnerHeldEntrySurvivesRestart(t *testing.T) {
	dataDir := t.TempDir()
	nb := newNameOwnerTest(t, dataDir)
	b := nb.b

	b.mu.Lock()
	addNameOwnerAgent(b, "worker", 1001, 1)
	b.mu.Unlock()
	recordNameOwner(b, "worker", 1001)
	// Write the registration-time table synchronously, so the test does not pass
	// because a still-pending first write happens to carry a fresh timestamp.
	b.flushNameOwners()

	// Advance one hour at a time so every hourly sweep runs.
	for i := 0; i < 8*24; i++ {
		nb.clock.Advance(time.Hour)
	}
	b.Stop()

	// Reboot at the moment the previous Broker stopped; uid 1002 registers
	// worker first.
	restarted := newNameOwnerTestAt(t, dataDir, nb.clock.Now())
	restarted.b.mu.Lock()
	addNameOwnerAgent(restarted.b, "worker", 1002, 5)
	restarted.b.mu.Unlock()
	recordNameOwner(restarted.b, "worker", 1002)

	uid, _, ok := nameOwnerEntry(restarted.b, "worker")
	if !ok || uid != 1001 {
		t.Fatalf("owner after the restart = %d (present %v), want the account that held it (%d)", uid, ok, 1001)
	}
}

// TestNameOwnerTableLimitEvictsOldestIdle checks the whole-table bound: with the
// table full, recording a new name evicts the globally oldest idle entry, not
// only the new account's own.
func TestNameOwnerTableLimitEvictsOldestIdle(t *testing.T) {
	nb := newNameOwnerTest(t, "")
	b := nb.b
	start := nb.clock.Now()
	b.mu.Lock()
	b.nameOwners.limit = 100
	b.nameOwners.tableLimit = 3
	b.mu.Unlock()

	seedNameOwner(b, "e1", 1001, start)
	seedNameOwner(b, "e2", 1002, start.Add(time.Second))
	seedNameOwner(b, "e3", 1001, start.Add(2*time.Second))

	b.mu.Lock()
	addNameOwnerAgent(b, "new", 1003, 1)
	b.mu.Unlock()
	recordNameOwner(b, "new", 1003)

	if _, _, ok := nameOwnerEntry(b, "e1"); ok {
		t.Fatal("the globally oldest idle entry was not evicted at the table limit")
	}
	for _, name := range []string{"e2", "e3", "new"} {
		if _, _, ok := nameOwnerEntry(b, name); !ok {
			t.Fatalf("entry %q disappeared; want it kept", name)
		}
	}
}

// TestNameOwnerExpiryOnListPushesAgentChanged checks that a listing that expires
// an idle name pushes the resulting address change instead of changing ownership
// silently.
func TestNameOwnerExpiryOnListPushesAgentChanged(t *testing.T) {
	tb := newTestBroker(t)
	fake := fakesession.New(t, fakesession.Options{Name: "worker"})
	rs := startRegistration(t, tb, fake)
	id := AgentID(rs.complete(3 * time.Second).GetAgentId())

	// A departed account owns the name, one minute short of the retention. The
	// recompute makes this agent pinned and pushes one agent_changed.
	tb.b.mu.Lock()
	tb.b.nameOwners.records["worker"] = nameOwnerRecord{
		uid:      9999,
		lastHeld: tb.clock.Now().Add(-(nameOwnerRetention - time.Minute)),
	}
	tb.b.recomputeRefsLocked()
	tb.b.mu.Unlock()

	pinned := agentChanged(t, rs, 3*time.Second)
	if !strings.Contains(pinned.GetAgentAddress(), " [") {
		t.Fatalf("the address before expiry = %q, want a ref while the departed account owns the name", pinned.GetAgentAddress())
	}

	// Move past the retention without running the hourly sweep, so the listing
	// itself must expire the entry, hand the name to the holder and push the
	// ref-free address.
	tb.clock.Advance(2 * time.Minute)
	resp := mustListAgents(t, tb, &localpb.ListAgentsRequest{AgentId: string(id)})
	if got := resp.GetSelfAddress(); got != "worker@hostA" {
		t.Fatalf("self_address after expiry = %q, want worker@hostA", got)
	}
	after := agentChanged(t, rs, 3*time.Second)
	if got := after.GetAgentAddress(); got != "worker@hostA" {
		t.Fatalf("the pushed address after expiry = %q, want worker@hostA", got)
	}
}
