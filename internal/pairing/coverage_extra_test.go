package pairing

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io/fs"
	"math/big"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/pairing/pairingpb"
)

// stubSettings embeds the mutating handshake fake and overrides the settings
// methods whose error returns a test needs to drive. A nil error field falls
// through to the embedded fake.
type stubSettings struct {
	*hsSettings
	aliasErr       error
	listenErr      error
	advErr         error
	hostErr        error
	maxFailuresErr error
	maxAttemptsErr error
}

func (s *stubSettings) Alias() (string, error) {
	if s.aliasErr != nil {
		return "", s.aliasErr
	}
	return s.hsSettings.Alias()
}

func (s *stubSettings) ListenAddress() (string, error) {
	if s.listenErr != nil {
		return "", s.listenErr
	}
	return s.hsSettings.ListenAddress()
}

func (s *stubSettings) Advertised() ([]config.AdvertisedAddr, error) {
	if s.advErr != nil {
		return nil, s.advErr
	}
	return s.hsSettings.Advertised()
}

func (s *stubSettings) Hostname() (string, error) {
	if s.hostErr != nil {
		return "", s.hostErr
	}
	return s.hsSettings.Hostname()
}

func (s *stubSettings) MaxFailures() (int, error) {
	if s.maxFailuresErr != nil {
		return 0, s.maxFailuresErr
	}
	return s.hsSettings.MaxFailures()
}

func (s *stubSettings) MaxAttemptsPerMinute() (int, error) {
	if s.maxAttemptsErr != nil {
		return 0, s.maxAttemptsErr
	}
	return s.hsSettings.MaxAttemptsPerMinute()
}

// sendFailConn fails every Send, standing in for a transport that went away
// between the frames of one handshake.
type sendFailConn struct {
	PairConn
	err error
}

func (c *sendFailConn) Send(context.Context, uint32, []byte) error { return c.err }

// TestRealClockNowAndTimer covers the production clock adapter: Now reports the
// wall clock, NewTimer fires, and Stop reports whether a timer was running.
func TestRealClockNowAndTimer(t *testing.T) {
	t.Parallel()

	c := realClock{}
	before := time.Now()
	got := c.Now()
	after := time.Now()
	if got.Before(before) || got.After(after) {
		t.Fatalf("Now() = %s, want a value between %s and %s", got, before, after)
	}

	fired := c.NewTimer(time.Millisecond)
	select {
	case <-fired.C():
	case <-time.After(testWaitBound):
		t.Fatal("NewTimer(1ms) did not fire")
	}

	stopped := c.NewTimer(time.Hour)
	if !stopped.Stop() {
		t.Fatal("Stop() on a running timer = false, want true")
	}
	if stopped.Stop() {
		t.Fatal("second Stop() = true, want false")
	}
	if stopped.C() == nil {
		t.Fatal("C() = nil, want the timer channel")
	}
}

// TestNewPairingSurfacesIdentityAndAliasErrors covers the two failure returns
// after the collaborators are filled: an unreadable data directory and a
// configuration error from the alias read.
func TestNewPairingSurfacesIdentityAndAliasErrors(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	notDir := filepath.Join(base, "not-a-dir")
	if err := os.WriteFile(notDir, nil, 0o600); err != nil {
		t.Fatalf("prepare file: %v", err)
	}
	s := newHSSettings(t)
	s.dataDir = notDir
	if _, err := newPairing(s, &hsTransport{}, realClock{}, realFileOps{}, nil); !errors.Is(err, ErrIdentity) {
		t.Fatalf("newPairing with a file data directory = %v, want ErrIdentity", err)
	}

	aliasS := newHSSettings(t)
	aliasS.aliasFunc = func() (string, error) { return "", errors.New("alias unavailable") }
	_, err := newPairing(aliasS, &hsTransport{}, realClock{}, realFileOps{}, nil)
	if !errors.Is(err, ErrNotOpen) {
		t.Fatalf("newPairing with an alias error = %v, want ErrNotOpen", err)
	}
	if !strings.Contains(err.Error(), "alias unavailable") {
		t.Fatalf("newPairing error = %v, want the configuration error in the message", err)
	}
}

// TestAtomicWriteFailureAndSyncErrorLogged returns the write failure and keeps
// the committed file when only the directory sync fails.
func TestAtomicWriteFailureAndSyncErrorLogged(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	failing := &peerTestOps{fileOps: realFileOps{}}
	failing.set(false, true, false)
	err := atomicWrite(failing, filepath.Join(dir, "unwritten.json"), []byte("x"), 0o600)
	if err == nil || err.Error() != "injected write failure" {
		t.Fatalf("atomicWrite with a failing WriteFile = %v, want the injected write failure", err)
	}
	if _, statErr := os.Stat(filepath.Join(dir, "unwritten.json")); !errors.Is(statErr, fs.ErrNotExist) {
		t.Fatalf("file after a failed write = %v, want it absent", statErr)
	}

	syncFail := &peerTestOps{fileOps: realFileOps{}}
	syncFail.set(false, false, true)
	committed := filepath.Join(dir, "committed.json")
	if err := atomicWrite(syncFail, committed, []byte("y"), 0o600); err != nil {
		t.Fatalf("atomicWrite with a failing SyncDir = %v, want nil", err)
	}
	data, err := os.ReadFile(committed)
	if err != nil || string(data) != "y" {
		t.Fatalf("content after a sync failure = %q, %v, want y", data, err)
	}
}

// TestRealFileOpsAndReadErrors reports the os adapter's write and sync failures
// and reads a present file, a missing path and a directory.
func TestRealFileOpsAndReadErrors(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	ops := realFileOps{}
	if err := ops.WriteFile(filepath.Join(dir, "missing", "x"), []byte("a"), 0o600); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("WriteFile into a missing directory = %v, want fs.ErrNotExist", err)
	}
	if err := ops.SyncDir(filepath.Join(dir, "missing")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("SyncDir of a missing directory = %v, want fs.ErrNotExist", err)
	}

	present := filepath.Join(dir, "f")
	if err := os.WriteFile(present, []byte("v"), 0o600); err != nil {
		t.Fatalf("prepare file: %v", err)
	}
	data, missing, err := readFileOrNotExist(ops, present)
	if err != nil || missing || string(data) != "v" {
		t.Fatalf("readFileOrNotExist(present) = %q, missing=%v, err=%v, want v, false, nil", data, missing, err)
	}
	if _, missing, err := readFileOrNotExist(ops, filepath.Join(dir, "absent")); err != nil || !missing {
		t.Fatalf("readFileOrNotExist(absent) = missing=%v, err=%v, want missing=true, err=nil", missing, err)
	}
	if _, missing, err := readFileOrNotExist(ops, dir); !errors.Is(err, syscall.EISDIR) || missing {
		t.Fatalf("readFileOrNotExist(directory) = missing=%v, err=%v, want EISDIR", missing, err)
	}
}

// TestPeerTableNilAndZeroGuards reports no conflict on a nil or zero Pairing and
// a conflict for a committed alias on an open table.
func TestPeerTableNilAndZeroGuards(t *testing.T) {
	t.Parallel()

	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "taken", RoleAcceptor))
	open := &Pairing{peers: table}
	if !open.AliasConflict("taken") {
		t.Fatal("AliasConflict on an open table = false for a committed alias, want true")
	}

	var nilP *Pairing
	nilP.SetOwnAlias("x")
	if nilP.AliasConflict("x") {
		t.Fatal("AliasConflict on a nil Pairing = true, want false")
	}
	zero := &Pairing{}
	zero.SetOwnAlias("x")
	if zero.AliasConflict("x") {
		t.Fatal("AliasConflict on a zero Pairing = true, want false")
	}
}

// TestPeerTablePutRejectsCommittedAlias covers the alias-free check: an alias
// another committed peer already answers to cannot be used.
func TestPeerTablePutRejectsCommittedAlias(t *testing.T) {
	t.Parallel()

	table, _, _ := newPeerTestTable(t)
	fpA, pubA := peerTestKey(t)
	fpB, pubB := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fpA, pubA, "taken", RoleAcceptor))

	_, err := table.put(peerTestEntry(fpB, pubB, "taken", RoleAcceptor))
	if !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("put with a committed alias = %v, want ErrNeedsLocalAlias", err)
	}
	if !strings.Contains(err.Error(), "belongs to") {
		t.Fatalf("put error = %v, want it to name the current owner", err)
	}
	if _, ok := table.get(fpB); ok {
		t.Fatal("the rejected entry was stored")
	}
}

// TestPeerTablePutRejectsEmptyEffectiveAlias checks that an entry with neither
// an alias nor a local alias is refused and leaves the table empty.
func TestPeerTablePutRejectsEmptyEffectiveAlias(t *testing.T) {
	t.Parallel()

	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)

	_, err := table.put(peerTestEntry(fp, pub, "", RoleAcceptor))
	if !errors.Is(err, ErrNeedsLocalAlias) {
		t.Fatalf("put with an empty effective alias = %v, want ErrNeedsLocalAlias", err)
	}
	if got := len(table.snapshot()); got != 0 {
		t.Fatalf("table holds %d peers after the refusal, want 0", got)
	}
}

// TestPeerTableResolveAliasRejectsOwnAlias covers the rule that the Broker's own
// alias never resolves to a peer, even when a committed entry uses it.
func TestPeerTableResolveAliasRejectsOwnAlias(t *testing.T) {
	t.Parallel()

	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "self", RoleAcceptor))
	fpOther, pubOther := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fpOther, pubOther, "other", RoleAcceptor))
	table.setOwnAlias("self")

	if got, ok := table.resolveAlias(""); ok {
		t.Fatalf("resolveAlias(\"\") = %s, want no match", got)
	}
	if got, ok := table.resolveAlias("self"); ok {
		t.Fatalf("resolveAlias(own alias) = %s, want no match", got)
	}
	if got, ok := table.resolveAlias("other"); !ok || got != fpOther {
		t.Fatalf("resolveAlias(other) = (%s, %v), want (%s, true)", got, ok, fingerprintHex(fpOther))
	}
}

// TestPeerTableReservationHelpers covers the empty-alias refusal, the no-op
// releases and the unknown-token lookups of the reservation bookkeeping.
func TestPeerTableReservationHelpers(t *testing.T) {
	t.Parallel()

	table, _, _ := newPeerTestTable(t)
	fp, _ := peerTestKey(t)

	if token, ok := table.reserveAlias("", fp); ok || token != 0 {
		t.Fatalf("reserveAlias(\"\") = (%d, %v), want (0, false)", token, ok)
	}
	table.releaseToken(0)

	token, ok := table.reserveAlias("held", fp)
	if !ok || token == 0 {
		t.Fatalf("reserveAlias(held) = (%d, %v), want a token", token, ok)
	}
	table.releaseToken(token + 1000)
	if !table.aliasConflict("held") {
		t.Fatal("releasing an unknown token dropped the live reservation")
	}
	if alias, ok := table.tokenAliasLocked(token + 1000); ok || alias != "" {
		t.Fatalf("tokenAliasLocked(unknown) = (%q, %v), want (\"\", false)", alias, ok)
	}
	table.releaseToken(token)
	if table.aliasConflict("held") {
		t.Fatal("the reservation survived its own release")
	}
}

// TestPeerTableObserveAliasUnknownPeer leaves an unknown fingerprint untouched
// and assigns a local alias to a known peer whose observed name is taken.
func TestPeerTableObserveAliasUnknownPeer(t *testing.T) {
	t.Parallel()

	table, _, _ := newPeerTestTable(t)
	unknown, _ := peerTestKey(t)
	local, assigned, err := table.observeAlias(unknown, "namesake")
	if err != nil || local != "" || assigned {
		t.Fatalf("observeAlias(unknown) = (%q, %v, %v), want (\"\", false, nil)", local, assigned, err)
	}
	if got := len(table.snapshot()); got != 0 {
		t.Fatalf("table holds %d peers, want 0", got)
	}

	fp, pub := peerTestKey(t)
	mustPut(t, table, peerTestEntry(fp, pub, "existing", RoleAcceptor))
	table.setOwnAlias("self")
	local, assigned, err = table.observeAlias(fp, "self")
	if err != nil || !assigned || local == "" {
		t.Fatalf("observeAlias(known, taken name) = (%q, %v, %v), want a generated local alias", local, assigned, err)
	}
	got, ok := table.get(fp)
	if !ok {
		t.Fatal("the observed peer is missing from the table")
	}
	if got.localAlias != local {
		t.Fatalf("stored peer local alias = %q, want %q", got.localAlias, local)
	}
}

// TestLoadPeerTableSurfacesReadError covers a read failure that is not a missing
// file: a directory in place of peers.json.
func TestLoadPeerTableSurfacesReadError(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), peersFileName)
	if err := os.Mkdir(path, 0o700); err != nil {
		t.Fatalf("prepare directory: %v", err)
	}
	_, err := loadPeerTable(path, realFileOps{})
	if !errors.Is(err, ErrStore) {
		t.Fatalf("loadPeerTable(directory) = %v, want ErrStore", err)
	}
	if !strings.Contains(err.Error(), "read") {
		t.Fatalf("loadPeerTable error = %v, want it to name the read failure", err)
	}
}

// TestPeersRecordValidationErrors covers each per-record check that runs before
// a record is accepted: fingerprint syntax, public-key encoding and length, and
// the paired-at timestamp.
func TestPeersRecordValidationErrors(t *testing.T) {
	t.Parallel()

	fp, pub := peerTestKey(t)
	validFP := fingerprintHex(fp)
	validPub := base64Std(pub)
	shortPub := base64Std(pub[:31])
	record := func(fingerprint, publicKey, pairedAt string) string {
		return fmt.Sprintf(`{"version":1,"peers":[{"fingerprint":%q,"public_key":%q,"alias":"a","local_alias":"","role":"acceptor","paired_at":%q}]}`,
			fingerprint, publicKey, pairedAt)
	}

	cases := []struct {
		name string
		doc  string
		want string
	}{
		{"fingerprint length", record("abcd", validPub, "2026-11-18T12:00:00Z"), "hex characters"},
		{"fingerprint not hex", record(strings.Repeat("z", 64), validPub, "2026-11-18T12:00:00Z"), "not hex"},
		{"public key not base64", record(validFP, "!!!!", "2026-11-18T12:00:00Z"), "not base64"},
		{"public key length", record(validFP, shortPub, "2026-11-18T12:00:00Z"), "want 32"},
		{"paired_at", record(validFP, validPub, "yesterday"), "RFC 3339"},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			path := filepath.Join(t.TempDir(), peersFileName)
			if err := os.WriteFile(path, []byte(tc.doc), 0o600); err != nil {
				t.Fatalf("write: %v", err)
			}
			_, err := loadPeerTable(path, realFileOps{})
			if !errors.Is(err, ErrStore) {
				t.Fatalf("loadPeerTable = %v, want ErrStore", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("loadPeerTable error = %v, want it to mention %q", err, tc.want)
			}
		})
	}
}

// TestLoadIdentityEmptyDataDir covers the explicit empty-directory refusal.
func TestLoadIdentityEmptyDataDir(t *testing.T) {
	t.Parallel()

	if _, err := loadIdentity("", identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("loadIdentity(\"\") = %v, want ErrIdentity", err)
	}
}

// TestLoadIdentityRecoversCertificateWithUnparsableKey covers the key-present,
// certificate-missing path when the stored key itself cannot be parsed: the
// certificate is not reissued over an unusable key.
func TestLoadIdentityRecoversCertificateWithUnparsableKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	identityWriteFile(t, dir, identityKeyFileName, []byte("not a key"), 0o600)
	if _, err := loadIdentity(dir, identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("loadIdentity with an unparsable key = %v, want ErrIdentity", err)
	}
	if _, err := os.Stat(filepath.Join(dir, identityCertFileName)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("certificate file state = %v, want it absent", err)
	}
}

// TestReadIdentityFileRejectsInspectError covers a stat failure that is not a
// missing file, here caused by a path whose parent is a regular file.
func TestReadIdentityFileRejectsInspectError(t *testing.T) {
	t.Parallel()

	base := t.TempDir()
	parent := filepath.Join(base, "file")
	if err := os.WriteFile(parent, nil, 0o600); err != nil {
		t.Fatalf("prepare file: %v", err)
	}
	_, err := readIdentityFile(filepath.Join(parent, identityKeyFileName))
	if !errors.Is(err, ErrIdentity) {
		t.Fatalf("readIdentityFile = %v, want ErrIdentity", err)
	}
	if !strings.Contains(err.Error(), "inspect") {
		t.Fatalf("readIdentityFile error = %v, want it to name the inspect failure", err)
	}
}

// TestReadIdentityFileRejectsUnreadableFile reports ErrIdentity when the file
// cannot be opened.
func TestReadIdentityFileRejectsUnreadableFile(t *testing.T) {
	t.Parallel()

	if os.Geteuid() == 0 {
		t.Skip("root can read a file with no permission bits")
	}
	dir := t.TempDir()
	path := identityWriteFile(t, dir, identityKeyFileName, []byte("secret"), 0o600)
	if err := os.Chmod(path, 0o000); err != nil {
		t.Fatalf("chmod: %v", err)
	}
	_, err := readIdentityFile(path)
	if !errors.Is(err, ErrIdentity) {
		t.Fatalf("readIdentityFile = %v, want ErrIdentity", err)
	}
	if !strings.Contains(err.Error(), "open") {
		t.Fatalf("readIdentityFile error = %v, want it to name the open failure", err)
	}
}

// rsaCertificate builds a self-signed RSA certificate, the non-Ed25519 input the
// identity parsers must reject.
func rsaCertificate(t *testing.T) (*x509.Certificate, []byte) {
	t.Helper()
	key, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject:      pkix.Name{CommonName: "rsa"},
		NotBefore:    identityTestTime(),
		NotAfter:     identityTestTime().Add(time.Hour),
	}
	der, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &key.PublicKey, key)
	if err != nil {
		t.Fatalf("create RSA certificate: %v", err)
	}
	cert, err := x509.ParseCertificate(der)
	if err != nil {
		t.Fatalf("parse RSA certificate: %v", err)
	}
	return cert, der
}

// TestParseIdentityPrivateKeyErrors covers the PEM type, the PKCS#8 parse and
// the key-type checks of the private-key parser.
func TestParseIdentityPrivateKeyErrors(t *testing.T) {
	t.Parallel()

	wrongType := pem.EncodeToMemory(&pem.Block{Type: "EC PRIVATE KEY", Bytes: []byte{1, 2, 3}})
	if _, err := parseIdentityPrivateKey(wrongType); !errors.Is(err, ErrIdentity) || !strings.Contains(err.Error(), "PEM type") {
		t.Fatalf("wrong PEM type = %v, want ErrIdentity naming the PEM type", err)
	}

	badDER := pem.EncodeToMemory(&pem.Block{Type: identityKeyPEMType, Bytes: []byte("junk")})
	if _, err := parseIdentityPrivateKey(badDER); !errors.Is(err, ErrIdentity) || !strings.Contains(err.Error(), "parse private key") {
		t.Fatalf("bad PKCS#8 = %v, want ErrIdentity naming the parse", err)
	}

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	rsaDER, err := x509.MarshalPKCS8PrivateKey(rsaKey)
	if err != nil {
		t.Fatalf("marshal RSA key: %v", err)
	}
	rsaPEM := pem.EncodeToMemory(&pem.Block{Type: identityKeyPEMType, Bytes: rsaDER})
	if _, err := parseIdentityPrivateKey(rsaPEM); !errors.Is(err, ErrIdentity) || !strings.Contains(err.Error(), "not Ed25519") {
		t.Fatalf("RSA key = %v, want ErrIdentity saying not Ed25519", err)
	}
}

// TestParseIdentityCertificateErrors covers the PEM type, the x509 parse and
// the key-type checks of the certificate parser.
func TestParseIdentityCertificateErrors(t *testing.T) {
	t.Parallel()

	_, rsaDER := rsaCertificate(t)
	wrongType := pem.EncodeToMemory(&pem.Block{Type: "TRUSTED CERTIFICATE", Bytes: rsaDER})
	if _, err := parseIdentityCertificate(wrongType); !errors.Is(err, ErrIdentity) || !strings.Contains(err.Error(), "PEM type") {
		t.Fatalf("wrong PEM type = %v, want ErrIdentity naming the PEM type", err)
	}

	badDER := pem.EncodeToMemory(&pem.Block{Type: identityCertPEMType, Bytes: []byte("junk")})
	if _, err := parseIdentityCertificate(badDER); !errors.Is(err, ErrIdentity) || !strings.Contains(err.Error(), "parse certificate") {
		t.Fatalf("bad certificate DER = %v, want ErrIdentity naming the parse", err)
	}

	rsaPEM := pem.EncodeToMemory(&pem.Block{Type: identityCertPEMType, Bytes: rsaDER})
	if _, err := parseIdentityCertificate(rsaPEM); !errors.Is(err, ErrIdentity) || !strings.Contains(err.Error(), "not Ed25519") {
		t.Fatalf("RSA certificate = %v, want ErrIdentity saying not Ed25519", err)
	}

	rsaKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatalf("generate RSA key: %v", err)
	}
	spki, err := x509.MarshalPKIXPublicKey(&rsaKey.PublicKey)
	if err != nil {
		t.Fatalf("marshal RSA public key: %v", err)
	}
	if _, err := parseEd25519PublicKeyInfo(spki); err == nil || !strings.Contains(err.Error(), "not an Ed25519 public key") {
		t.Fatalf("parseEd25519PublicKeyInfo(RSA) = %v, want a type error", err)
	}
}

// TestIdentityWriteHelpersSurfaceFilesystemErrors reports a missing directory
// from every identity write helper.
func TestIdentityWriteHelpersSurfaceFilesystemErrors(t *testing.T) {
	t.Parallel()

	missing := filepath.Join(t.TempDir(), "missing")
	keyPath := filepath.Join(missing, identityKeyFileName)
	certPath := filepath.Join(missing, identityCertFileName)

	if _, err := writeIdentityTempFile(missing, keyPath, []byte("x")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("writeIdentityTempFile in a missing directory = %v, want fs.ErrNotExist", err)
	}
	if err := writeIdentityFileExclusive(missing, keyPath, []byte("x")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("writeIdentityFileExclusive in a missing directory = %v, want fs.ErrNotExist", err)
	}
	if err := writeIdentityFileAtomic(missing, keyPath, []byte("x")); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("writeIdentityFileAtomic in a missing directory = %v, want fs.ErrNotExist", err)
	}
	if err := writeIdentityCertificate(missing, certPath, []byte{1}); !errors.Is(err, ErrIdentity) {
		t.Fatalf("writeIdentityCertificate in a missing directory = %v, want ErrIdentity", err)
	}
	if _, err := generateIdentity(missing, identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("generateIdentity in a missing directory = %v, want ErrIdentity", err)
	}

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Ed25519 key: %v", err)
	}
	if _, err := generateCertificate(missing, priv, identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("generateCertificate in a missing directory = %v, want ErrIdentity", err)
	}
	if err := syncIdentityDir(missing); !errors.Is(err, fs.ErrNotExist) {
		t.Fatalf("syncIdentityDir of a missing directory = %v, want fs.ErrNotExist", err)
	}

	dir := t.TempDir()
	written := filepath.Join(dir, "identity.crt")
	if err := writeIdentityFileAtomic(dir, written, []byte("ok")); err != nil {
		t.Fatalf("writeIdentityFileAtomic in a real directory = %v, want nil", err)
	}
	if data, err := os.ReadFile(written); err != nil || string(data) != "ok" {
		t.Fatalf("content after a successful atomic write = %q, %v, want ok", data, err)
	}
}

// TestWriteIdentityFileAtomicRenameFailure reports the rename failure over an
// existing directory and installs a file when the target is free.
func TestWriteIdentityFileAtomicRenameFailure(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	target := filepath.Join(dir, "identity.crt")
	if err := os.Mkdir(target, 0o700); err != nil {
		t.Fatalf("prepare directory target: %v", err)
	}
	if err := writeIdentityFileAtomic(dir, target, []byte("x")); !errors.Is(err, fs.ErrExist) {
		t.Fatalf("writeIdentityFileAtomic over a directory = %v, want fs.ErrExist", err)
	}

	free := filepath.Join(dir, "free.crt")
	if err := writeIdentityFileAtomic(dir, free, []byte("y")); err != nil {
		t.Fatalf("writeIdentityFileAtomic to a free path = %v, want nil", err)
	}
	if data, err := os.ReadFile(free); err != nil || string(data) != "y" {
		t.Fatalf("content after a successful rename = %q, %v, want y", data, err)
	}
}

// TestRecordVerificationFailureSkipsOtherKeys covers the key scan: a failure is
// counted against the matching one-time key only.
func TestRecordVerificationFailureSkipsOtherKeys(t *testing.T) {
	t.Parallel()

	now := identityTestTime()
	table := &keyTable{}
	table.add("first-key", now)
	table.add("second-key", now)

	table.recordVerificationFailure(keyMatch{key: "second-key", oneTime: true}, 5, now)
	if got := table.keys[0].failures; got != 0 {
		t.Fatalf("first key failures = %d, want 0", got)
	}
	if got := table.keys[1].failures; got != 1 {
		t.Fatalf("second key failures = %d, want 1", got)
	}
}

// TestCreateOneTimeKeyErrors reports ErrNotOpen, the settings errors and the
// random source error before a key is stored.
func TestCreateOneTimeKeyErrors(t *testing.T) {
	t.Parallel()

	if _, _, _, err := (&Pairing{}).CreateOneTimeKey(); !errors.Is(err, ErrNotOpen) {
		t.Fatalf("CreateOneTimeKey on a zero Pairing = %v, want ErrNotOpen", err)
	}

	clk := newHSClock()
	base := newHSSettings(t)
	tr := &hsTransport{}
	p := hsNewPairing(t, base, tr, clk)

	injected := errors.New("injected failure")
	cases := []struct {
		name string
		s    *stubSettings
		want error
	}{
		{"listen", &stubSettings{hsSettings: base, listenErr: injected}, ErrStore},
		{"advertised", &stubSettings{hsSettings: base, advErr: injected}, ErrStore},
		{"hostname", &stubSettings{hsSettings: base, hostErr: injected}, ErrStore},
	}
	for _, tc := range cases {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			p.settings = tc.s
			_, _, _, err := p.CreateOneTimeKey()
			if !errors.Is(err, tc.want) {
				t.Fatalf("CreateOneTimeKey(%s error) = %v, want %v", tc.name, err, tc.want)
			}
			if !strings.Contains(err.Error(), injected.Error()) {
				t.Fatalf("CreateOneTimeKey error = %v, want the injected message", err)
			}
		})
	}

	p.settings = base
	p.randRead = func([]byte) (int, error) { return 0, injected }
	if _, _, _, err := p.CreateOneTimeKey(); !errors.Is(err, injected) {
		t.Fatalf("CreateOneTimeKey with a failing random source = %v, want the injected error", err)
	}
}

// TestRandomBytesFallback checks that the crypto/rand path writes bytes into the
// buffer when no random seam is installed.
func TestRandomBytesFallback(t *testing.T) {
	t.Parallel()

	buf := make([]byte, 16)
	if err := (&Pairing{}).randomBytes(buf); err != nil {
		t.Fatalf("randomBytes = %v, want nil", err)
	}
	if bytes.Equal(buf, make([]byte, len(buf))) {
		t.Fatal("randomBytes left the buffer zero, want crypto/rand to fill it")
	}
}

// TestValidAdvertisedAddrRejectsParens rejects an address with parentheses and
// accepts a plain host:port.
func TestValidAdvertisedAddrRejectsParens(t *testing.T) {
	t.Parallel()

	if validAdvertisedAddr("host(1.2.3.4):1") {
		t.Fatal("validAdvertisedAddr accepted an address with parentheses")
	}
	if !validAdvertisedAddr("192.0.2.1:17443") {
		t.Fatal("validAdvertisedAddr rejected a plain host:port")
	}
}

// TestCandidatesUnknownPeer returns no candidates for an unknown fingerprint and
// the stored address for a known dialer.
func TestCandidatesUnknownPeer(t *testing.T) {
	t.Parallel()

	table, _, _ := newPeerTestTable(t)
	fp, pub := peerTestKey(t)
	entry := peerTestEntry(fp, pub, "known", RoleDialer)
	entry.pairingAddr = "192.0.2.9:17443"
	mustPut(t, table, entry)
	unknown, _ := peerTestKey(t)
	p := &Pairing{peers: table}
	if addrs, last := p.Candidates(unknown); addrs != nil || last != "" {
		t.Fatalf("Candidates(unknown) = (%v, %q), want (nil, \"\")", addrs, last)
	}
	addrs, _ := p.Candidates(fp)
	if len(addrs) != 1 || addrs[0] != "192.0.2.9:17443" {
		t.Fatalf("Candidates(known) = %v, want [192.0.2.9:17443]", addrs)
	}
}

// TestTruncatePartRuneBoundary covers the rune boundary walk: a cut that would
// split a rune backs off to the previous boundary.
func TestTruncatePartRuneBoundary(t *testing.T) {
	t.Parallel()

	if got := truncatePart("é", 1); got != "" {
		t.Fatalf("truncatePart(\"é\", 1) = %q, want an empty string", got)
	}
	if got := truncatePart("aé", 2); got != "a" {
		t.Fatalf("truncatePart(\"aé\", 2) = %q, want \"a\"", got)
	}
}

// TestVerifyRejectsMalformedSubjectPublicKeyInfo rejects a malformed key and
// accepts a valid signature.
func TestVerifyRejectsMalformedSubjectPublicKeyInfo(t *testing.T) {
	t.Parallel()

	if Verify(make([]byte, 44), []byte("data"), make([]byte, ed25519.SignatureSize)) {
		t.Fatal("Verify accepted a malformed SubjectPublicKeyInfo")
	}
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate Ed25519 key: %v", err)
	}
	if !Verify(pub, []byte("data"), ed25519.Sign(priv, []byte("data"))) {
		t.Fatal("Verify rejected a valid signature")
	}
}

// TestConnectBeforeOpen covers the collaborator check on the initiator.
func TestConnectBeforeOpen(t *testing.T) {
	t.Parallel()

	if _, _, err := (&Pairing{}).Connect(context.Background(), "127.0.0.1:17443", "key", ""); !errors.Is(err, ErrNotOpen) {
		t.Fatalf("Connect on a zero Pairing = %v, want ErrNotOpen", err)
	}
}

// TestConnectRejectsInvalidLocalAlias covers the alias syntax check, which runs
// before any dial.
func TestConnectRejectsInvalidLocalAlias(t *testing.T) {
	t.Parallel()

	p := hsNewPairing(t, newHSSettings(t), &hsTransport{}, newHSClock())
	if _, _, err := p.Connect(context.Background(), "127.0.0.1:17443", "key", "bad alias!"); !errors.Is(err, ErrInvalidAlias) {
		t.Fatalf("Connect with an invalid local alias = %v, want ErrInvalidAlias", err)
	}
}

// TestConnectSurfacesAliasAndRandomErrors covers the two collaborator errors
// before the dial: the alias read and the nonce generation.
func TestConnectSurfacesAliasAndRandomErrors(t *testing.T) {
	t.Parallel()

	clk := newHSClock()
	base := newHSSettings(t)
	tr := &hsTransport{}

	aliasP := hsNewPairing(t, base, tr, clk)
	aliasP.settings = &stubSettings{hsSettings: base, aliasErr: errors.New("alias unavailable")}
	if _, _, err := aliasP.Connect(context.Background(), "127.0.0.1:17443", "key", ""); !errors.Is(err, ErrStore) {
		t.Fatalf("Connect with an alias error = %v, want ErrStore", err)
	}

	randomP := hsNewPairing(t, newHSSettings(t), tr, clk)
	injected := errors.New("random source failed")
	randomP.randRead = func([]byte) (int, error) { return 0, injected }
	if _, _, err := randomP.Connect(context.Background(), "127.0.0.1:17443", "key", ""); !errors.Is(err, injected) {
		t.Fatalf("Connect with a failing random source = %v, want the injected error", err)
	}
	if len(tr.drops) != 0 {
		t.Fatalf("transport dropped %d peers without a handshake", len(tr.drops))
	}
}

// TestRejectDetailCoversLocalReasons returns the detail text for the local-only
// reasons.
func TestRejectDetailCoversLocalReasons(t *testing.T) {
	t.Parallel()

	cases := map[RejectReason]string{
		ReasonTimeout:      "handshake timed out",
		ReasonNotConnected: "not connected",
	}
	for reason, want := range cases {
		if got := rejectDetail(reason); got != want {
			t.Errorf("rejectDetail(%d) = %q, want %q", reason, got, want)
		}
	}
}

// TestAdvertisedConversions round-trips the direct addresses between the config
// and wire forms.
func TestAdvertisedConversions(t *testing.T) {
	t.Parallel()

	in := []config.AdvertisedAddr{
		{Kind: config.AdvertisedKindDirect, Addr: "192.0.2.1:17443"},
		{Kind: config.AdvertisedKindDirect, Addr: "[2001:db8::1]:17443"},
	}
	wire := advertisedToPB(in)
	if len(wire) != len(in) {
		t.Fatalf("advertisedToPB returned %d entries, want %d", len(wire), len(in))
	}
	for i := range in {
		if wire[i].GetKind() != in[i].Kind || wire[i].GetAddr() != in[i].Addr {
			t.Errorf("wire[%d] = %+v, want %+v", i, wire[i], in[i])
		}
	}

	back := advertisedFromPB([]*pairingpb.AdvertisedAddress{
		{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.7:17443"},
	})
	want := []config.AdvertisedAddr{{Kind: config.AdvertisedKindDirect, Addr: "198.51.100.7:17443"}}
	if !reflect.DeepEqual(back, want) {
		t.Fatalf("advertisedFromPB = %+v, want %+v", back, want)
	}
}

// TestParseConnectAddressRejectsNestedParens covers the shown-name check: a name
// that itself contains a parenthesis is rejected.
func TestParseConnectAddressRejectsNestedParens(t *testing.T) {
	t.Parallel()

	if _, _, err := parseConnectAddress("shown(extra)(127.0.0.1:17443)"); !errors.Is(err, ErrInvalidAddress) {
		t.Fatalf("parseConnectAddress with a nested parenthesis = %v, want ErrInvalidAddress", err)
	}
}

// TestAcceptBeforeOpenAndNilConn covers the two early refusals on the responder.
func TestAcceptBeforeOpenAndNilConn(t *testing.T) {
	t.Parallel()

	if err := (&Pairing{}).Accept(nil); !errors.Is(err, ErrNotOpen) {
		t.Fatalf("Accept on a zero Pairing = %v, want ErrNotOpen", err)
	}
	p := hsNewPairing(t, newHSSettings(t), &hsTransport{}, newHSClock())
	if err := p.Accept(nil); !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "nil connection") {
		t.Fatalf("Accept(nil) = %v, want ErrProtocol naming the nil connection", err)
	}
}

// TestReserveAndReleaseAliasHelpers covers the empty and own-alias refusals and
// a zero-token release.
func TestReserveAndReleaseAliasHelpers(t *testing.T) {
	t.Parallel()

	p := hsNewPairing(t, newHSSettings(t), &hsTransport{}, newHSClock())
	fp, _ := hsKey(t)

	if token, ok := p.reserveAlias(fp, "", "self"); ok || token != 0 {
		t.Fatalf("reserveAlias(\"\") = (%d, %v), want (0, false)", token, ok)
	}
	if token, ok := p.reserveAlias(fp, "self", "self"); ok || token != 0 {
		t.Fatalf("reserveAlias(own alias) = (%d, %v), want (0, false)", token, ok)
	}
	p.releaseAlias(0)

	token, ok := p.reserveAlias(fp, "chosen", "self")
	if !ok || token == 0 {
		t.Fatalf("reserveAlias(chosen) = (%d, %v), want a token", token, ok)
	}
	p.releaseAlias(token)
	if p.AliasConflict("chosen") {
		t.Fatal("the reserved alias survived its release")
	}
}

// TestSendRejectAndLingerLocalReason covers a local-only reason: it closes the
// connection without writing a frame.
func TestSendRejectAndLingerLocalReason(t *testing.T) {
	t.Parallel()

	a, b := hsConnPairFor(Fingerprint{}, make([]byte, 32), Fingerprint{}, make([]byte, 32))
	if err := sendRejectAndLinger(context.Background(), a, ReasonTimeout); err != nil {
		t.Fatalf("sendRejectAndLinger(timeout) = %v, want nil", err)
	}
	b.mu.Lock()
	closed := b.closed
	queued := len(b.queue)
	b.mu.Unlock()
	if !closed {
		t.Fatal("the connection was not closed")
	}
	if queued != 0 {
		t.Fatalf("%d frames were sent for a local-only reason, want 0", queued)
	}
}

// TestSendRejectAndLingerSendFailure covers a transport that refuses the reject
// frame; the error is returned and the connection closed.
func TestSendRejectAndLingerSendFailure(t *testing.T) {
	t.Parallel()

	a, b := hsConnPairFor(Fingerprint{}, make([]byte, 32), Fingerprint{}, make([]byte, 32))
	injected := errors.New("send failed")
	conn := &sendFailConn{PairConn: a, err: injected}
	if err := sendRejectAndLinger(context.Background(), conn, ReasonKeyInvalid); !errors.Is(err, injected) {
		t.Fatalf("sendRejectAndLinger with a failing Send = %v, want the injected error", err)
	}
	b.mu.Lock()
	closed := b.closed
	b.mu.Unlock()
	if !closed {
		t.Fatal("the connection was not closed after the failed send")
	}
}

// TestRejectErrorMalformedPayload covers a PairReject payload that cannot be
// decoded: it is a protocol error, not a rejection.
func TestRejectErrorMalformedPayload(t *testing.T) {
	t.Parallel()

	err := rejectError([]byte{0xff})
	if !errors.Is(err, ErrProtocol) || !strings.Contains(err.Error(), "cannot decode PairReject") {
		t.Fatalf("rejectError(malformed) = %v, want ErrProtocol naming the decode failure", err)
	}
}
