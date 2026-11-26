package pairing

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

type peerTestOps struct {
	fileOps

	mu          sync.Mutex
	failWrite   bool
	failRename  bool
	failSyncDir bool
}

func (o *peerTestOps) WriteFile(path string, data []byte, mode os.FileMode) error {
	o.mu.Lock()
	fail := o.failWrite
	o.mu.Unlock()
	if fail {
		return errors.New("injected write failure")
	}
	return o.fileOps.WriteFile(path, data, mode)
}

func (o *peerTestOps) Rename(oldpath, newpath string) error {
	o.mu.Lock()
	fail := o.failRename
	o.mu.Unlock()
	if fail {
		return errors.New("injected rename failure")
	}
	return o.fileOps.Rename(oldpath, newpath)
}

func (o *peerTestOps) SyncDir(dir string) error {
	o.mu.Lock()
	fail := o.failSyncDir
	o.mu.Unlock()
	if fail {
		return errors.New("injected sync failure")
	}
	return o.fileOps.SyncDir(dir)
}

func (o *peerTestOps) set(rename, write, syncDir bool) {
	o.mu.Lock()
	o.failRename = rename
	o.failWrite = write
	o.failSyncDir = syncDir
	o.mu.Unlock()
}

type peerTestSettings struct {
	mu     sync.Mutex
	alias  string
	events []config.Event
}

func (s *peerTestSettings) DataDir() (string, error)           { return "", nil }
func (s *peerTestSettings) Hostname() (string, error)          { return "testhost", nil }
func (s *peerTestSettings) ListenAddress() (string, error)     { return "0.0.0.0:1", nil }
func (s *peerTestSettings) StaticKey() string                  { return "" }
func (s *peerTestSettings) MaxFailures() (int, error)          { return 5, nil }
func (s *peerTestSettings) MaxAttemptsPerMinute() (int, error) { return 10, nil }
func (s *peerTestSettings) Advertised() ([]config.AdvertisedAddr, error) {
	return nil, nil
}

func (s *peerTestSettings) Alias() (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.alias, nil
}

func (s *peerTestSettings) Audit(e config.Event) {
	s.mu.Lock()
	s.events = append(s.events, e)
	s.mu.Unlock()
}

func (s *peerTestSettings) recordedEvents() []config.Event {
	s.mu.Lock()
	defer s.mu.Unlock()
	return append([]config.Event(nil), s.events...)
}

type peerTestTransport struct {
	mu    sync.Mutex
	drops []Fingerprint
}

func (t *peerTestTransport) PairDial(ctx context.Context, address string, run func(PairConn) error) error {
	return errors.New("peerTestTransport: unexpected PairDial")
}

func (t *peerTestTransport) Drop(fp Fingerprint) {
	t.mu.Lock()
	t.drops = append(t.drops, fp)
	t.mu.Unlock()
}

func (t *peerTestTransport) dropped() []Fingerprint {
	t.mu.Lock()
	defer t.mu.Unlock()
	return append([]Fingerprint(nil), t.drops...)
}

func newPeerTestTable(t *testing.T) (*peerTable, string, *peerTestOps) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	ops := &peerTestOps{fileOps: realFileOps{}}
	table, err := loadPeerTable(path, ops)
	if err != nil {
		t.Fatalf("loadPeerTable: %v", err)
	}
	return table, path, ops
}

func peerTestKey(t *testing.T) (Fingerprint, []byte) {
	t.Helper()
	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return fingerprintOfPublicKey(pub), pub
}

func peerTestEntry(fp Fingerprint, pub []byte, alias string, role Role) peerEntry {
	return peerEntry{
		fingerprint: fp,
		publicKey:   append([]byte(nil), pub...),
		alias:       alias,
		role:        role,
		pairedAt:    time.Date(2026, 11, 18, 12, 0, 0, 123456789, time.UTC),
	}
}

func mustPut(t *testing.T, table *peerTable, e peerEntry) bool {
	t.Helper()
	replaced, err := table.put(e)
	if err != nil {
		t.Fatalf("put %s: %v", fingerprintHex(e.fingerprint), err)
	}
	return replaced
}

// TestREQBrokerPairing017_PeersSurviveReopen checks that role, effective alias,
// every candidate source and the last connected address survive a restart.
func TestREQBrokerPairing017_PeersSurviveReopen(t *testing.T) {
	t.Parallel()
	table, path, ops := newPeerTestTable(t)
	fp, pub := peerTestKey(t)

	entry := peerTestEntry(fp, pub, "hostA", RoleDialer)
	entry.localAlias = "hostA-0123"
	entry.acceptor = []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:17443"},
	}
	entry.card = []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "card.example:17443"},
	}
	entry.pairingAddr = "192.0.2.10:17443"
	entry.lastConnected = "192.0.2.10:17443"
	mustPut(t, table, entry)

	reopened, err := loadPeerTable(path, ops)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, ok := reopened.get(fp)
	if !ok {
		t.Fatalf("peer missing after reopen")
	}
	if got.role != RoleDialer {
		t.Errorf("role = %v, want dialer", got.role)
	}
	if got.effectiveAlias() != "hostA-0123" {
		t.Errorf("effective alias = %q, want hostA-0123", got.effectiveAlias())
	}
	if got.pairingAddr != "192.0.2.10:17443" {
		t.Errorf("pairing address = %q", got.pairingAddr)
	}
	if got.lastConnected != "192.0.2.10:17443" {
		t.Errorf("last connected = %q", got.lastConnected)
	}
	addrs, last := mergeCandidates(&got)
	want := []string{"192.0.2.10:17443", "card.example:17443"}
	if strings.Join(addrs, ",") != strings.Join(want, ",") {
		t.Errorf("candidates = %v, want %v", addrs, want)
	}
	if last != "192.0.2.10:17443" {
		t.Errorf("last connected view = %q", last)
	}
}

// TestREQBrokerPairing016_IsPeerAfterReopenWithoutKey checks that a peer is
// still a peer after a restart, without any in-memory state.
func TestREQBrokerPairing016_IsPeerAfterReopenWithoutKey(t *testing.T) {
	t.Parallel()
	table, path, ops := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleAcceptor))

	reopened, err := loadPeerTable(path, ops)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	p := &Pairing{peers: reopened}
	if !p.IsPeer(fp) {
		t.Fatalf("IsPeer = false after reopen")
	}
	if p.Role(fp) != RoleAcceptor {
		t.Fatalf("Role = %v, want acceptor", p.Role(fp))
	}
}

// TestLoadChecksLocalAlias checks that loading a stored local alias enforces the
// validAlias rule while a value equal to the Broker's own alias still loads.
func TestLoadChecksLocalAlias(t *testing.T) {
	t.Parallel()
	fp, pub := peerTestKey(t)
	base := peerRecord{
		Fingerprint: fingerprintHex(fp),
		PublicKey:   base64Std(pub),
		Alias:       "hostB",
		Role:        "acceptor",
		PairedAt:    time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
	cases := []struct {
		name       string
		localAlias string
		wantErr    string
	}{
		{"invalid alias", "bad alias", `local alias "bad alias" is not a valid alias`},
		{"the Broker's own alias", "hostA", ""},
		{"valid alias", "hostB-0123", ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			s := newHSSettings(t)
			s.alias = "hostA"
			record := base
			record.LocalAlias = tc.localAlias
			data, err := json.Marshal(peersFile{Version: peersFileVersion, Peers: []peerRecord{record}})
			if err != nil {
				t.Fatalf("marshal fixture: %v", err)
			}
			path := filepath.Join(s.dataDir, peersFileName)
			if err := os.WriteFile(path, data, peersFileMode); err != nil {
				t.Fatalf("write fixture: %v", err)
			}

			p, err := newPairing(s, &hsTransport{}, newHSClock(), realFileOps{}, rand.Read)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("open with local alias %q: %v", tc.localAlias, err)
				}
				if p == nil {
					t.Fatalf("open with local alias %q returned a nil Pairing", tc.localAlias)
				}
				if got := p.EffectiveAlias(fp); got != tc.localAlias {
					t.Fatalf("effective alias = %q, want %q", got, tc.localAlias)
				}
				return
			}
			if !errors.Is(err, ErrStore) {
				t.Fatalf("open with a local alias that is %s = %v (pairing %v), want ErrStore", tc.name, err, p)
			}
			want := fmt.Sprintf("%s: %s record 0: %s", ErrStore, path, tc.wantErr)
			if got := err.Error(); got != want {
				t.Fatalf("open error = %q, want %q", got, want)
			}
			if p != nil {
				t.Fatalf("open with a local alias that is %s returned %+v, want nil", tc.name, p)
			}
		})
	}
}

// TestREQBrokerPairing021_UnpairRemovesAndDrops checks removal persistence and
// that the link is dropped exactly once.
func TestREQBrokerPairing021_UnpairRemovesAndDrops(t *testing.T) {
	t.Parallel()
	table, path, ops := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))

	transport := &peerTestTransport{}
	p := &Pairing{peers: table, transport: transport}
	if err := p.Unpair(fp); err != nil {
		t.Fatalf("Unpair: %v", err)
	}
	if p.IsPeer(fp) {
		t.Fatalf("peer still visible after Unpair")
	}
	if got := transport.dropped(); len(got) != 1 || got[0] != fp {
		t.Fatalf("Drop calls = %v, want one for %s", got, fingerprintHex(fp))
	}

	reopened, err := loadPeerTable(path, ops)
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	if _, ok := reopened.get(fp); ok {
		t.Fatalf("removed peer reappeared after reopen")
	}
}

// TestREQBrokerPairing021_UnpairIdempotent checks that unpairing a non-peer is
// a successful no-op and does not drop a link.
func TestREQBrokerPairing021_UnpairIdempotent(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, _ := peerTestKey(t)
	transport := &peerTestTransport{}
	p := &Pairing{peers: table, transport: transport}
	if err := p.Unpair(fp); err != nil {
		t.Fatalf("first Unpair: %v", err)
	}
	if err := p.Unpair(fp); err != nil {
		t.Fatalf("second Unpair: %v", err)
	}
	if got := transport.dropped(); len(got) != 0 {
		t.Fatalf("Drop calls = %v, want none", got)
	}
}

// TestREQBrokerPairing022_IsPeerFalseAfterUnpair checks that IsPeer, Role and
// EffectiveAlias report no peer after Unpair.
func TestREQBrokerPairing022_IsPeerFalseAfterUnpair(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))
	p := &Pairing{peers: table, transport: &peerTestTransport{}}
	if !p.IsPeer(fp) {
		t.Fatalf("IsPeer = false before Unpair")
	}
	if err := p.Unpair(fp); err != nil {
		t.Fatalf("Unpair: %v", err)
	}
	if p.IsPeer(fp) {
		t.Fatalf("IsPeer = true after Unpair")
	}
	if p.Role(fp) != RoleUnknown {
		t.Fatalf("Role = %v after Unpair, want unknown", p.Role(fp))
	}
	if p.EffectiveAlias(fp) != "" {
		t.Fatalf("EffectiveAlias = %q after Unpair, want empty", p.EffectiveAlias(fp))
	}
}

// TestPeerTableWriteFailureKeepsMemory checks that a failed write leaves the
// in-memory table unchanged.
func TestPeerTableWriteFailureKeepsMemory(t *testing.T) {
	t.Parallel()
	table, _, ops := newPeerTestTable(t)
	fpA, pubA := peerTestKey(t)
	fpB, pubB := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fpA, pubA, "hostA", RoleDialer))

	ops.set(true, false, false)
	_, err := table.put(peerTestEntry(fpB, pubB, "hostB", RoleDialer))
	if !errors.Is(err, ErrStore) {
		t.Fatalf("put error = %v, want ErrStore", err)
	}
	if _, ok := table.get(fpB); ok {
		t.Fatalf("failed write leaked into memory")
	}
	if got, ok := table.get(fpA); !ok || got.alias != "hostA" {
		t.Fatalf("existing entry changed by failed write: %+v, %v", got, ok)
	}
}

// TestUnpairWriteFailureKeepsMemory checks that a failed removal leaves both the
// entry and the link alone.
func TestUnpairWriteFailureKeepsMemory(t *testing.T) {
	t.Parallel()
	table, _, ops := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))

	ops.set(true, false, false)
	transport := &peerTestTransport{}
	p := &Pairing{peers: table, transport: transport}
	if err := p.Unpair(fp); !errors.Is(err, ErrStore) {
		t.Fatalf("Unpair error = %v, want ErrStore", err)
	}
	if !p.IsPeer(fp) {
		t.Fatalf("entry removed despite write failure")
	}
	if got := transport.dropped(); len(got) != 0 {
		t.Fatalf("Drop called despite write failure: %v", got)
	}
}

// TestUnpairSkipsDropWhenRepairReplacesEntry checks that a re-pairing committed
// between removal and drop keeps its link.
func TestUnpairSkipsDropWhenRepairReplacesEntry(t *testing.T) {
	t.Parallel()

	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "old", RoleDialer))

	transport := &peerTestTransport{}
	p := &Pairing{peers: table, transport: transport}

	var repairErr error
	table.afterRemove = func() {
		_, repairErr = table.put(peerTestEntry(fp, pub, "new", RoleDialer))
	}
	if err := p.Unpair(fp); err != nil {
		t.Fatalf("Unpair: %v", err)
	}
	if repairErr != nil {
		t.Fatalf("re-pairing commit: %v", repairErr)
	}
	if got := transport.dropped(); len(got) != 0 {
		t.Fatalf("Drop called for a superseded entry: %v", got)
	}
	if !p.IsPeer(fp) {
		t.Fatal("the re-paired entry was removed")
	}
	if got := p.EffectiveAlias(fp); got != "new" {
		t.Fatalf("effective alias = %q, want new", got)
	}
}

// TestInitiateRepairKeepsCardAndLocalAlias checks that re-pairing as the dialer
// keeps the card source and the old local alias while other sources take the
// new values.
func TestInitiateRepairKeepsCardAndLocalAlias(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)

	first := peerTestEntry(fp, pub, "reported-one", RoleDialer)
	first.localAlias = "kept-1234"
	first.card = []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "card.example:17443"}}
	first.acceptor = []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "old.example:17443"}}
	first.pairingAddr = "old.example:17443"
	first.lastConnected = "old.example:17443"
	mustPut(t, table, first)

	second := peerTestEntry(fp, pub, "reported-two", RoleDialer)
	second.acceptor = []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "new.example:17443"}}
	second.pairingAddr = "new.example:17443"
	second.lastConnected = "new.example:17443"
	replaced := mustPut(t, table, second)
	if !replaced {
		t.Fatalf("re-pairing did not replace the entry")
	}

	got, _ := table.get(fp)
	if got.localAlias != "kept-1234" {
		t.Errorf("local alias = %q, want kept-1234", got.localAlias)
	}
	if got.effectiveAlias() != "kept-1234" {
		t.Errorf("effective alias = %q, want kept-1234", got.effectiveAlias())
	}
	if got.alias != "reported-two" {
		t.Errorf("self-reported alias = %q, want reported-two", got.alias)
	}
	if len(got.card) != 1 || got.card[0].Addr != "card.example:17443" {
		t.Errorf("card source = %v, want the original entry", got.card)
	}
	if len(got.acceptor) != 1 || got.acceptor[0].Addr != "new.example:17443" {
		t.Errorf("acceptor source = %v, want the new list", got.acceptor)
	}
	if got.pairingAddr != "new.example:17443" || got.lastConnected != "new.example:17443" {
		t.Errorf("pairing/last = %q/%q, want the new address", got.pairingAddr, got.lastConnected)
	}
}

// TestAcceptorRepairDropsCandidates checks that re-pairing as the acceptor
// removes every candidate address.
func TestAcceptorRepairDropsCandidates(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)

	dialer := peerTestEntry(fp, pub, "hostA", RoleDialer)
	dialer.acceptor = []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:17443"}}
	dialer.card = []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "card.example:17443"}}
	dialer.pairingAddr = "192.0.2.10:17443"
	dialer.lastConnected = "192.0.2.10:17443"
	mustPut(t, table, dialer)

	acceptor := peerTestEntry(fp, pub, "hostA", RoleAcceptor)
	mustPut(t, table, acceptor)

	got, _ := table.get(fp)
	if got.role != RoleAcceptor {
		t.Fatalf("role = %v, want acceptor", got.role)
	}
	if len(got.acceptor) != 0 || len(got.card) != 0 {
		t.Fatalf("candidates survived acceptor re-pairing: %v / %v", got.acceptor, got.card)
	}
	if got.pairingAddr != "" || got.lastConnected != "" {
		t.Fatalf("addresses survived acceptor re-pairing: %q / %q", got.pairingAddr, got.lastConnected)
	}
}

// TestPutInitialisesLastConnectedFromPairingAddress covers the normal initiator
// commit where the caller only supplies the dialed address.
func TestPutInitialisesLastConnectedFromPairingAddress(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	entry := peerTestEntry(fp, pub, "hostA", RoleDialer)
	entry.pairingAddr = "192.0.2.10:17443"
	mustPut(t, table, entry)
	got, _ := table.get(fp)
	if got.lastConnected != "192.0.2.10:17443" {
		t.Fatalf("last connected = %q, want the pairing address", got.lastConnected)
	}
}

// TestPeersJSONRoundTripFieldNames checks that peers.json is written with the
// expected field names and the alias and local alias reload.
func TestPeersJSONRoundTripFieldNames(t *testing.T) {
	t.Parallel()
	table, path, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	entry := peerTestEntry(fp, pub, "hostA", RoleDialer)
	entry.localAlias = "macbook-2"
	entry.acceptor = []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:17443"}}
	entry.card = []config.AdvertisedAddr{}
	entry.pairingAddr = "192.0.2.10:17443"
	entry.lastConnected = "192.0.2.10:17443"
	mustPut(t, table, entry)

	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read peers.json: %v", err)
	}
	var doc map[string]json.RawMessage
	if err := json.Unmarshal(data, &doc); err != nil {
		t.Fatalf("unmarshal top level: %v", err)
	}
	if string(doc["version"]) != "1" {
		t.Errorf("version = %s, want 1", doc["version"])
	}
	var records []map[string]json.RawMessage
	if err := json.Unmarshal(doc["peers"], &records); err != nil {
		t.Fatalf("unmarshal peers: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}
	for _, key := range []string{"fingerprint", "public_key", "alias", "local_alias", "role", "paired_at", "addresses", "last_connected"} {
		if _, ok := records[0][key]; !ok {
			t.Errorf("record is missing %q", key)
		}
	}
	var addrs map[string]json.RawMessage
	if err := json.Unmarshal(records[0]["addresses"], &addrs); err != nil {
		t.Fatalf("unmarshal addresses: %v", err)
	}
	for _, key := range []string{"acceptor", "card", "pairing"} {
		if _, ok := addrs[key]; !ok {
			t.Errorf("addresses is missing %q", key)
		}
	}
	var one []map[string]string
	if err := json.Unmarshal(addrs["acceptor"], &one); err != nil {
		t.Fatalf("unmarshal acceptor: %v", err)
	}
	if one[0]["kind"] != config.AdvertisedKindDirect || one[0]["addr"] != "192.0.2.10:17443" {
		t.Errorf("acceptor entry = %v", one[0])
	}

	reopened, err := loadPeerTable(path, realFileOps{})
	if err != nil {
		t.Fatalf("reload: %v", err)
	}
	got, ok := reopened.get(fp)
	if !ok || got.alias != "hostA" || got.localAlias != "macbook-2" {
		t.Fatalf("round trip lost fields: %+v, %v", got, ok)
	}
}

// TestPeersJSONUnknownFieldsIgnored checks that unknown top-level and record
// fields do not prevent loading.
func TestPeersJSONUnknownFieldsIgnored(t *testing.T) {
	t.Parallel()
	fp, pub := peerTestKey(t)
	doc := fmt.Sprintf(`{
	  "version": 1,
	  "future_top_level": true,
	  "peers": [{
	    "fingerprint": %q,
	    "public_key": %q,
	    "alias": "hostA",
	    "local_alias": "",
	    "role": "dialer",
	    "paired_at": "2026-11-18T12:00:00Z",
	    "future_record_field": {"nested": 1},
	    "addresses": {"acceptor": [], "card": [], "pairing": ""}
	  }]
	}`, fingerprintHex(fp), base64Std(pub))
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	table, err := loadPeerTable(path, realFileOps{})
	if err != nil {
		t.Fatalf("load with unknown fields: %v", err)
	}
	if got, ok := table.get(fp); !ok || got.alias != "hostA" {
		t.Fatalf("record not loaded: %+v, %v", got, ok)
	}
}

// TestPeersJSONBadRoleFailsOpen checks that an unknown role rejects the file as
// ErrStore.
func TestPeersJSONBadRoleFailsOpen(t *testing.T) {
	t.Parallel()
	assertPeersLoadFails(t, func(fp Fingerprint, pub []byte) string {
		return fmt.Sprintf(`{"version":1,"peers":[{"fingerprint":%q,"public_key":%q,"alias":"a","local_alias":"","role":"sneaky","paired_at":"2026-11-18T12:00:00Z"}]}`,
			fingerprintHex(fp), base64Std(pub))
	})
}

// TestPeersJSONFingerprintMismatchFailsOpen checks that a fingerprint not
// matching its public key rejects the file as ErrStore.
func TestPeersJSONFingerprintMismatchFailsOpen(t *testing.T) {
	t.Parallel()
	assertPeersLoadFails(t, func(fp Fingerprint, pub []byte) string {
		wrong, _ := peerTestKey(t)
		return fmt.Sprintf(`{"version":1,"peers":[{"fingerprint":%q,"public_key":%q,"alias":"a","local_alias":"","role":"acceptor","paired_at":"2026-11-18T12:00:00Z"}]}`,
			fingerprintHex(wrong), base64Std(pub))
	})
}

// TestPeersJSONDuplicateFingerprintFailsOpen checks that two records with one
// fingerprint reject the file as ErrStore.
func TestPeersJSONDuplicateFingerprintFailsOpen(t *testing.T) {
	t.Parallel()
	fp, pub := peerTestKey(t)
	record := fmt.Sprintf(`{"fingerprint":%q,"public_key":%q,"alias":"a","local_alias":"","role":"acceptor","paired_at":"2026-11-18T12:00:00Z"}`,
		fingerprintHex(fp), base64Std(pub))
	doc := fmt.Sprintf(`{"version":1,"peers":[%s,%s]}`, record, strings.Replace(record, `"alias":"a"`, `"alias":"b"`, 1))
	assertPeersDocFails(t, doc)
}

// TestPeersJSONDuplicateEffectiveAliasFailsOpen checks that two records claiming
// one effective alias reject the file as ErrStore.
func TestPeersJSONDuplicateEffectiveAliasFailsOpen(t *testing.T) {
	t.Parallel()
	fpA, pubA := peerTestKey(t)
	fpB, pubB := peerTestKey(t)
	doc := fmt.Sprintf(`{"version":1,"peers":[
	  {"fingerprint":%q,"public_key":%q,"alias":"same","local_alias":"","role":"acceptor","paired_at":"2026-11-18T12:00:00Z"},
	  {"fingerprint":%q,"public_key":%q,"alias":"other","local_alias":"same","role":"acceptor","paired_at":"2026-11-18T12:00:00Z"}
	]}`, fingerprintHex(fpA), base64Std(pubA), fingerprintHex(fpB), base64Std(pubB))
	assertPeersDocFails(t, doc)
}

// TestPeersJSONUnknownVersionFailsOpen checks that an unknown file version
// rejects the file as ErrStore.
func TestPeersJSONUnknownVersionFailsOpen(t *testing.T) {
	t.Parallel()
	assertPeersDocFails(t, `{"version":2,"peers":[]}`)
}

// TestPeersJSONMalformedFailsOpen checks that a malformed file is rejected as
// ErrStore.
func TestPeersJSONMalformedFailsOpen(t *testing.T) {
	t.Parallel()
	assertPeersDocFails(t, `not json`)
}

func assertPeersLoadFails(t *testing.T, makeDoc func(Fingerprint, []byte) string) {
	t.Helper()
	fp, pub := peerTestKey(t)
	assertPeersDocFails(t, makeDoc(fp, pub))
}

func assertPeersDocFails(t *testing.T, doc string) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	if err := os.WriteFile(path, []byte(doc), 0o600); err != nil {
		t.Fatalf("write: %v", err)
	}
	if _, err := loadPeerTable(path, realFileOps{}); !errors.Is(err, ErrStore) {
		t.Fatalf("load error = %v, want ErrStore", err)
	}
}

func base64Std(b []byte) string {
	return base64.StdEncoding.EncodeToString(b)
}

// TestPeersSortedByEffectiveAliasThenFingerprint checks that Peers sorts by
// effective alias.
func TestPeersSortedByEffectiveAliasThenFingerprint(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fpA, pubA := peerTestKey(t)
	fpB, pubB := peerTestKey(t)
	fpC, pubC := peerTestKey(t)

	entryA := peerTestEntry(fpA, pubA, "zeta", RoleAcceptor)
	mustPut(t, table, entryA)
	entryB := peerTestEntry(fpB, pubB, "same", RoleAcceptor)
	mustPut(t, table, entryB)
	entryC := peerTestEntry(fpC, pubC, "alpha", RoleAcceptor)
	mustPut(t, table, entryC)

	p := &Pairing{peers: table}
	peers := p.Peers()
	if len(peers) != 3 {
		t.Fatalf("Peers() = %d entries, want 3", len(peers))
	}
	got := []string{peers[0].Alias, peers[1].Alias, peers[2].Alias}
	want := []string{"alpha", "same", "zeta"}
	if strings.Join(got, ",") != strings.Join(want, ",") {
		t.Fatalf("order = %v, want %v", got, want)
	}
}

// TestPeersReturnsCopies checks that Peers returns buffers that do not share the
// table's.
func TestPeersReturnsCopies(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	entry := peerTestEntry(fp, pub, "hostA", RoleDialer)
	entry.acceptor = []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.10:17443"}}
	mustPut(t, table, entry)

	p := &Pairing{peers: table}
	peers := p.Peers()
	peers[0].PublicKey[0] ^= 0xff
	peers[0].Candidates[0].Addr = "tampered:1"

	got, _ := table.get(fp)
	if got.publicKey[0] == peers[0].PublicKey[0] {
		t.Fatalf("snapshot shares the public key buffer")
	}
	if got.acceptor[0].Addr == "tampered:1" {
		t.Fatalf("snapshot shares the candidate buffer")
	}
}

// TestZeroPairingAnswersEmpty checks that every query on a zero Pairing answers
// empty and Unpair returns ErrNotOpen.
func TestZeroPairingAnswersEmpty(t *testing.T) {
	t.Parallel()
	var p Pairing
	fp, _ := peerTestKey(t)
	if p.IsPeer(fp) {
		t.Errorf("IsPeer on zero value = true")
	}
	if p.Role(fp) != RoleUnknown {
		t.Errorf("Role on zero value = %v", p.Role(fp))
	}
	if got := p.Peers(); len(got) != 0 {
		t.Errorf("Peers on zero value = %v, want an empty slice", got)
	}
	if p.EffectiveAlias(fp) != "" {
		t.Errorf("EffectiveAlias on zero value = %q", p.EffectiveAlias(fp))
	}
	if _, ok := p.ResolveAlias("x"); ok {
		t.Errorf("ResolveAlias on zero value = true")
	}
	if addrs, last := p.Candidates(fp); addrs != nil || last != "" {
		t.Errorf("Candidates on zero value = %v, %q", addrs, last)
	}
	if err := p.Unpair(fp); !errors.Is(err, ErrNotOpen) {
		t.Errorf("Unpair on zero value = %v, want ErrNotOpen", err)
	}
}

// TestUnpairSnapshotsAliasBeforeRemoval checks that EffectiveAlias reports the
// alias before Unpair and empty after.
func TestUnpairSnapshotsAliasBeforeRemoval(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))
	p := &Pairing{peers: table, transport: &peerTestTransport{}}
	if got := p.EffectiveAlias(fp); got != "hostA" {
		t.Fatalf("EffectiveAlias before Unpair = %q", got)
	}
	if err := p.Unpair(fp); err != nil {
		t.Fatalf("Unpair: %v", err)
	}
	if got := p.EffectiveAlias(fp); got != "" {
		t.Fatalf("EffectiveAlias after Unpair = %q", got)
	}
}
