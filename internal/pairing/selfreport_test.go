package pairing

import (
	"encoding/json"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
)

// countingOps counts the WriteFile calls atomicWrite makes. Every rewrite of
// peers.json performs exactly one WriteFile on the temporary file, so the count
// is the number of persisted table rewrites.
type countingOps struct {
	*peerTestOps

	mu     sync.Mutex
	writes int
}

func (o *countingOps) WriteFile(path string, data []byte, mode os.FileMode) error {
	o.mu.Lock()
	o.writes++
	o.mu.Unlock()
	return o.peerTestOps.WriteFile(path, data, mode)
}

func (o *countingOps) count() int {
	o.mu.Lock()
	defer o.mu.Unlock()
	return o.writes
}

func newCountingPeerTable(t *testing.T) (*peerTable, string, *countingOps) {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	ops := &countingOps{peerTestOps: &peerTestOps{fileOps: realFileOps{}}}
	table, err := loadPeerTable(path, ops)
	if err != nil {
		t.Fatalf("loadPeerTable: %v", err)
	}
	return table, path, ops
}

// readPeersFile decodes the persisted table at path.
func readPeersFile(t *testing.T, path string) peersFile {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read peers.json: %v", err)
	}
	var file peersFile
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatalf("decode peers.json: %v", err)
	}
	return file
}

// TestOversizedSelfReportedAliasStoredNormalized reproduces the audit finding
// that a self-reported alias was persisted verbatim. A peer reporting more than
// one MiB must end up with an alias that fits the alias rule; a local alias is
// generated as before, because an oversized value is not a valid alias.
func TestOversizedSelfReportedAliasStoredNormalized(t *testing.T) {
	t.Parallel()
	table, path, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleAcceptor))

	settings := &peerTestSettings{}
	p := &Pairing{peers: table, settings: settings}
	p.ObserveAlias(fp, strings.Repeat("A", 1<<20+1))

	got, ok := table.get(fp)
	if !ok {
		t.Fatal("the observed peer disappeared from the table")
	}
	if got.localAlias == "" {
		t.Fatal("no local alias generated for an oversized self-reported alias")
	}
	file := readPeersFile(t, path)
	if len(file.Peers) != 1 {
		t.Fatalf("persisted records = %d, want 1", len(file.Peers))
	}
	if n := len(file.Peers[0].Alias); n > config.AliasMaxLen {
		t.Fatalf("persisted alias length = %d, want at most %d", n, config.AliasMaxLen)
	}
	events := settings.recordedEvents()
	if len(events) != 1 || events[0].Event != config.EventLocalAliasAssigned {
		t.Fatalf("audit events = %+v, want one local_alias_assigned", events)
	}
}

// TestPutNormalizesSelfReportedAlias covers the table write path itself. A
// handshake commit hands the reported alias to put unchanged, so put must store
// the normalized form; deleting that step would let a value outside the alias
// rule reach peers.json. The persisted record is checked directly, not the
// in-memory entry.
func TestPutNormalizesSelfReportedAlias(t *testing.T) {
	t.Parallel()
	table, path, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)

	reported := strings.Repeat("A x/", 10)
	entry := peerTestEntry(fp, pub, reported, RoleAcceptor)
	mustPut(t, table, entry)

	file := readPeersFile(t, path)
	if len(file.Peers) != 1 {
		t.Fatalf("persisted records = %d, want 1", len(file.Peers))
	}
	stored := file.Peers[0].Alias
	if n := len(stored); n > config.AliasMaxLen {
		t.Fatalf("persisted alias %q has length %d, want at most %d", stored, n, config.AliasMaxLen)
	}
	if !validAlias(stored) {
		t.Fatalf("persisted alias %q does not satisfy the alias rule", stored)
	}
	if stored == reported {
		t.Fatal("persisted alias is the raw reported value")
	}
}

// TestSelfReportedCandidateAddressLengthBound pins the 259-byte cap on one
// stored candidate address: a 253-byte domain name plus ':' and a five-digit
// port is the longest address the table accepts, and one byte more is skipped.
func TestSelfReportedCandidateAddressLengthBound(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))

	label := func(n int) string { return strings.Repeat("a", n) }
	longest := label(63) + "." + label(63) + "." + label(63) + "." + label(61) + ":17443"
	if len(longest) != 259 {
		t.Fatalf("longest test address has %d bytes, want 259", len(longest))
	}
	tooLong := label(63) + "." + label(63) + "." + label(63) + "." + label(62) + ":17443"
	if len(tooLong) != 260 {
		t.Fatalf("too-long test address has %d bytes, want 260", len(tooLong))
	}

	p := &Pairing{peers: table}
	p.ObserveAcceptorAddresses(fp, []config.AdvertisedAddr{direct(tooLong), direct(longest)})

	got, _ := table.get(fp)
	if len(got.acceptor) != 1 || got.acceptor[0].Addr != longest {
		t.Fatalf("acceptor source = %v, want only the 259-byte address", got.acceptor)
	}
}

// TestAdvertisedAddrBoundaryMatchesConfig pins the length boundary where a
// fully qualified domain name carries a trailing dot. config.ValidDialHost
// removes that dot before applying the 253-byte domain limit, so a 253-byte
// domain written with a trailing dot plus a five-digit port is a 260-byte
// address that configuration accepts. Pairing must accept the same address: at
// every boundary case both sides judge it the same way.
func TestAdvertisedAddrBoundaryMatchesConfig(t *testing.T) {
	t.Parallel()

	label := func(n int) string { return strings.Repeat("a", n) }
	domain := func(last int) string {
		return label(63) + "." + label(63) + "." + label(63) + "." + label(last)
	}
	full := domain(61) // 253 bytes, the domain limit
	over := domain(62) // 254 bytes, one past the limit

	addrs := []string{
		full + ":17443",  // 259 bytes
		full + ".:17443", // 260 bytes with the trailing dot
		over + ":17443",  // 260 bytes, domain over the limit
		over + ".:17443", // 261 bytes
	}
	for _, addr := range addrs {
		host, _, err := net.SplitHostPort(addr)
		if err != nil {
			t.Fatalf("SplitHostPort(%q): %v", addr, err)
		}
		configOK := config.ValidDialHost(host)
		pairingOK := validAdvertisedAddr(addr)
		if configOK != pairingOK {
			t.Fatalf("address of %d bytes: config accepts it = %v, pairing accepts it = %v", len(addr), configOK, pairingOK)
		}
	}

	// Pin the boundary itself: a change that rejected the trailing-dot address
	// on both sides would satisfy the agreement check above while dropping a
	// configuration this version accepts.
	if !config.ValidDialHost(full + ".") {
		t.Fatal("config rejects a 253-byte domain with a trailing dot")
	}
	if !validAdvertisedAddr(full + ".:17443") {
		t.Fatal("pairing rejects a 253-byte domain with a trailing dot and a port")
	}
}

// TestSelfReportedCandidateAddressPortDigits pins the per-byte digit check on
// the port. A string that strconv alone would still parse is not enough: every
// byte must be 0-9, so '+', '-', '_', a letter and a multibyte digit are
// rejected. A port with leading zeros is still decimal and is accepted.
func TestSelfReportedCandidateAddressPortDigits(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))

	bad := []string{
		"host.example:12a",
		"host.example:+80",
		"host.example:-80",
		"host.example:1_0",
		"host.example:８０",
	}
	const good = "host.example:00080"
	addrs := make([]config.AdvertisedAddr, 0, len(bad)+1)
	for _, a := range bad {
		addrs = append(addrs, direct(a))
	}
	addrs = append(addrs, direct(good))

	p := &Pairing{peers: table}
	p.ObserveAcceptorAddresses(fp, addrs)

	got, _ := table.get(fp)
	if len(got.acceptor) != 1 || got.acceptor[0].Addr != good {
		t.Fatalf("acceptor source = %v, want only %q", got.acceptor, good)
	}
}

// TestSelfReportedCandidateAddressesValidated reproduces the audit finding that
// acceptor- and card-sourced addresses were stored without checking them. Only
// entries that are a dialable host:port may survive; malformed entries are
// skipped like unknown kinds, and the valid entries around them are kept.
func TestSelfReportedCandidateAddressesValidated(t *testing.T) {
	t.Parallel()
	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))

	p := &Pairing{peers: table}
	bad := []config.AdvertisedAddr{
		direct("bad host:80"),
		direct("/var/run/peers.sock:80"),
		direct(strings.Repeat("a", 300) + ":80"),
		direct("example.com:70000"),
	}
	const good = "192.0.2.10:17443"
	p.ObserveAcceptorAddresses(fp, append(append([]config.AdvertisedAddr(nil), bad...), direct(good)))
	p.UpdateCardAddresses(fp, append(append([]config.AdvertisedAddr(nil), bad...), direct("card.example:17443")))

	got, _ := table.get(fp)
	if len(got.acceptor) != 1 || got.acceptor[0].Addr != good {
		t.Fatalf("acceptor source = %v, want only %q", got.acceptor, good)
	}
	if len(got.card) != 1 || got.card[0].Addr != "card.example:17443" {
		t.Fatalf("card source = %v, want only the valid entry", got.card)
	}
}

// TestUnchangedSelfReportedFieldsSkipRewrite reproduces the write amplification:
// every Hello, PeerInfo or address frame rewrote the whole table even when
// nothing changed. Repeating the same alias, address list and connected address
// must write once, not once per call.
func TestUnchangedSelfReportedFieldsSkipRewrite(t *testing.T) {
	t.Parallel()
	table, _, ops := newCountingPeerTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "hostA", RoleDialer))
	p := &Pairing{peers: table, settings: &peerTestSettings{}}

	base := ops.count()
	p.ObserveAlias(fp, "renamed")
	p.ObserveAlias(fp, "renamed")
	if got := ops.count() - base; got != 1 {
		t.Errorf("observeAlias rewrote peers.json %d times, want 1", got)
	}

	acceptor := []config.AdvertisedAddr{direct("192.0.2.10:17443")}
	base = ops.count()
	p.ObserveAcceptorAddresses(fp, acceptor)
	p.ObserveAcceptorAddresses(fp, acceptor)
	if got := ops.count() - base; got != 1 {
		t.Errorf("ObserveAcceptorAddresses rewrote peers.json %d times, want 1", got)
	}

	card := []config.AdvertisedAddr{direct("card.example:17443")}
	base = ops.count()
	p.UpdateCardAddresses(fp, card)
	p.UpdateCardAddresses(fp, card)
	if got := ops.count() - base; got != 1 {
		t.Errorf("UpdateCardAddresses rewrote peers.json %d times, want 1", got)
	}

	base = ops.count()
	p.ObserveConnected(fp, "192.0.2.10:17443")
	p.ObserveConnected(fp, "192.0.2.10:17443")
	if got := ops.count() - base; got != 1 {
		t.Errorf("ObserveConnected rewrote peers.json %d times, want 1", got)
	}
}

// TestOldPeersFileOversizedAliasLoadsNormalized reproduces the load path: a file
// written by an older build may hold an alias longer than the alias rule. That
// must not stop the Broker from starting, the value must be normalized in
// memory, and the next write must persist the normalized value.
func TestOldPeersFileOversizedAliasLoadsNormalized(t *testing.T) {
	t.Parallel()
	fp, pub := peerTestKey(t)
	rec := peerRecord{
		Fingerprint: fingerprintHex(fp),
		PublicKey:   base64Std(pub),
		Alias:       strings.Repeat("b", config.AliasMaxLen+10),
		Role:        "dialer",
		PairedAt:    time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC).Format(time.RFC3339Nano),
	}
	data, err := json.Marshal(peersFile{Version: peersFileVersion, Peers: []peerRecord{rec}})
	if err != nil {
		t.Fatalf("marshal fixture: %v", err)
	}
	dir := t.TempDir()
	path := filepath.Join(dir, "peers.json")
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatalf("write fixture: %v", err)
	}

	table, err := loadPeerTable(path, realFileOps{})
	if err != nil {
		t.Fatalf("loadPeerTable with an oversized alias: %v", err)
	}
	got, ok := table.get(fp)
	if !ok {
		t.Fatal("the peer was not loaded")
	}
	if n := len(got.alias); n > config.AliasMaxLen {
		t.Fatalf("loaded alias length = %d, want at most %d", n, config.AliasMaxLen)
	}

	if err := table.observeConnected(fp, "192.0.2.10:17443"); err != nil {
		t.Fatalf("observeConnected: %v", err)
	}
	file := readPeersFile(t, path)
	if len(file.Peers) != 1 {
		t.Fatalf("persisted records = %d, want 1", len(file.Peers))
	}
	if n := len(file.Peers[0].Alias); n > config.AliasMaxLen {
		t.Fatalf("persisted alias length = %d, want at most %d", n, config.AliasMaxLen)
	}
}
