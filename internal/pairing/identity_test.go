package pairing

import (
	"bytes"
	"crypto/ed25519"
	"crypto/sha256"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"syscall"
	"testing"
	"time"
)

func identityTestTime() time.Time {
	return time.Date(2026, 11, 18, 12, 0, 0, 0, time.UTC)
}

func identityMustLoad(t *testing.T, dir string) *identity {
	t.Helper()
	id, err := loadIdentity(dir, identityTestTime)
	if err != nil {
		t.Fatalf("loadIdentity(%q): %v", dir, err)
	}
	return id
}

func identityReadFile(t *testing.T, path string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	return data
}

func identityParseCertificate(t *testing.T, dir string) *x509.Certificate {
	t.Helper()
	data := identityReadFile(t, filepath.Join(dir, identityCertFileName))
	block, _ := pem.Decode(data)
	if block == nil {
		t.Fatalf("%s is not PEM", identityCertFileName)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		t.Fatalf("parse certificate: %v", err)
	}
	return cert
}

func identityWriteFile(t *testing.T, dir, name string, data []byte, mode os.FileMode) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("chmod %s: %v", path, err)
	}
	return path
}

// TestREQBrokerPairing001_IdentityStableAcrossReopen checks that reopening a
// data directory loads the same identity and fingerprint.
func TestREQBrokerPairing001_IdentityStableAcrossReopen(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first := identityMustLoad(t, dir)
	keyBefore := identityReadFile(t, filepath.Join(dir, identityKeyFileName))
	certBefore := identityReadFile(t, filepath.Join(dir, identityCertFileName))
	pubBefore := first.publicKey()

	later := func() time.Time { return identityTestTime().Add(72 * time.Hour) }
	second, err := loadIdentity(dir, later)
	if err != nil {
		t.Fatalf("second loadIdentity: %v", err)
	}

	if got, want := second.fingerprint(), first.fingerprint(); got != want {
		t.Fatalf("fingerprint after reopen = %s, want %s", got, want)
	}
	if got, want := second.publicKey(), pubBefore; !bytes.Equal(got, want) {
		t.Fatalf("public key after reopen = %x, want %x", got, want)
	}
	if got := identityReadFile(t, filepath.Join(dir, identityKeyFileName)); !bytes.Equal(got, keyBefore) {
		t.Fatal("private key file changed across reopen")
	}
	if got := identityReadFile(t, filepath.Join(dir, identityCertFileName)); !bytes.Equal(got, certBefore) {
		t.Fatal("certificate file changed across reopen")
	}
	if !bytes.Equal(second.sign([]byte("same input")), first.sign([]byte("same input"))) {
		t.Fatal("signatures differ after reopen with the same key")
	}
}

func identityCertFingerprint(t *testing.T, dir string) Fingerprint {
	t.Helper()
	cert := identityParseCertificate(t, dir)
	return Fingerprint(sha256.Sum256(cert.RawSubjectPublicKeyInfo))
}

// TestIdentityFingerprintMatchesCertificatePublicKeyInfo checks that the
// identity fingerprint is the SHA-256 of the certificate's SubjectPublicKeyInfo.
func TestIdentityFingerprintMatchesCertificatePublicKeyInfo(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	id := identityMustLoad(t, dir)
	want := identityCertFingerprint(t, dir)
	if got := id.fingerprint(); got != want {
		t.Fatalf("identity fingerprint = %s, want SHA-256 of SubjectPublicKeyInfo %s", got, want)
	}

	cert := identityParseCertificate(t, dir)
	if got := Fingerprint(sha256.Sum256(cert.Raw)); got == want {
		t.Fatal("fingerprint equals SHA-256 of the whole certificate, want SubjectPublicKeyInfo only")
	}
}

// TestIdentityFilesUseOwnerOnlyPermissions checks that both identity files are
// mode 0600 and owned by the effective user.
func TestIdentityFilesUseOwnerOnlyPermissions(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	identityMustLoad(t, dir)
	for _, name := range []string{identityKeyFileName, identityCertFileName} {
		path := filepath.Join(dir, name)
		info, err := os.Stat(path)
		if err != nil {
			t.Fatalf("stat %s: %v", path, err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Errorf("%s mode = %#o, want 0600", name, got)
		}
		if sys, ok := info.Sys().(*syscall.Stat_t); ok {
			if euid := os.Geteuid(); int(sys.Uid) != euid {
				t.Errorf("%s owner = uid %d, want %d", name, int(sys.Uid), euid)
			}
		}
	}
}

// TestIdentityKeyFileIsPKCS8PEM checks that the key file is a PKCS#8 PEM
// Ed25519 key and the certificate file is a single CERTIFICATE PEM block.
func TestIdentityKeyFileIsPKCS8PEM(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	identityMustLoad(t, dir)
	block, rest := pem.Decode(identityReadFile(t, filepath.Join(dir, identityKeyFileName)))
	if block == nil {
		t.Fatal("private key file is not PEM")
	}
	if block.Type != identityKeyPEMType {
		t.Fatalf("private key PEM type = %q, want %q", block.Type, identityKeyPEMType)
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		t.Fatal("private key file has trailing data")
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		t.Fatalf("parse PKCS#8 private key: %v", err)
	}
	if _, ok := key.(ed25519.PrivateKey); !ok {
		t.Fatalf("private key type = %T, want ed25519.PrivateKey", key)
	}

	certBlock, certRest := pem.Decode(identityReadFile(t, filepath.Join(dir, identityCertFileName)))
	if certBlock == nil || certBlock.Type != identityCertPEMType {
		t.Fatalf("certificate PEM block = %v, want type %q", certBlock, identityCertPEMType)
	}
	if len(bytes.TrimSpace(certRest)) != 0 {
		t.Fatal("certificate file has trailing data")
	}
}

// TestIdentityRejectsGroupOrOtherAccess checks that loadIdentity rejects
// identity files readable by group or other.
func TestIdentityRejectsGroupOrOtherAccess(t *testing.T) {
	t.Parallel()

	for _, name := range []string{identityKeyFileName, identityCertFileName} {
		for _, mode := range []os.FileMode{0o640, 0o604, 0o660, 0o666} {
			name, mode := name, mode
			t.Run(name+"/"+mode.String(), func(t *testing.T) {
				t.Parallel()

				dir := t.TempDir()
				identityMustLoad(t, dir)
				path := filepath.Join(dir, name)
				if err := os.Chmod(path, mode); err != nil {
					t.Fatalf("chmod %s: %v", path, err)
				}
				if _, err := loadIdentity(dir, identityTestTime); !errors.Is(err, ErrIdentity) {
					t.Fatalf("loadIdentity with %s mode %#o = %v, want ErrIdentity", name, mode, err)
				}
			})
		}
	}
}

// TestIdentityRejectsSymbolicLink checks that loadIdentity rejects an identity
// file that is a symbolic link.
func TestIdentityRejectsSymbolicLink(t *testing.T) {
	t.Parallel()

	src := t.TempDir()
	identityMustLoad(t, src)

	dir := t.TempDir()
	if err := os.Symlink(filepath.Join(src, identityKeyFileName), filepath.Join(dir, identityKeyFileName)); err != nil {
		t.Fatalf("symlink: %v", err)
	}
	identityWriteFile(t, dir, identityCertFileName, identityReadFile(t, filepath.Join(src, identityCertFileName)), 0o600)

	if _, err := loadIdentity(dir, identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("loadIdentity with a symlinked key = %v, want ErrIdentity", err)
	}
}

// TestIdentityRejectsDirectoryInPlaceOfFile checks that loadIdentity rejects a
// directory where an identity file is expected.
func TestIdentityRejectsDirectoryInPlaceOfFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	if err := os.Mkdir(filepath.Join(dir, identityKeyFileName), 0o600); err != nil {
		t.Fatalf("mkdir: %v", err)
	}
	if _, err := loadIdentity(dir, identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("loadIdentity with a directory as key = %v, want ErrIdentity", err)
	}
}

// TestIdentityRegeneratesMissingCertificate checks that losing the certificate
// reissues it from the existing key without changing the identity.
func TestIdentityRegeneratesMissingCertificate(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	first := identityMustLoad(t, dir)
	keyBefore := identityReadFile(t, filepath.Join(dir, identityKeyFileName))
	certPath := filepath.Join(dir, identityCertFileName)
	if err := os.Remove(certPath); err != nil {
		t.Fatalf("remove certificate: %v", err)
	}

	second, err := loadIdentity(dir, func() time.Time { return identityTestTime().Add(time.Hour) })
	if err != nil {
		t.Fatalf("loadIdentity after certificate loss: %v", err)
	}
	if got, want := second.fingerprint(), first.fingerprint(); got != want {
		t.Fatalf("fingerprint after certificate reissue = %s, want %s", got, want)
	}
	if got := identityReadFile(t, filepath.Join(dir, identityKeyFileName)); !bytes.Equal(got, keyBefore) {
		t.Fatal("private key changed when the certificate was reissued")
	}
	if info, err := os.Stat(certPath); err != nil {
		t.Fatalf("certificate was not recreated: %v", err)
	} else if got := info.Mode().Perm(); got != 0o600 {
		t.Fatalf("reissued certificate mode = %#o, want 0600", got)
	}
}

// TestIdentityRejectsCertificateWithoutKey checks that loadIdentity rejects a
// certificate whose key is missing.
func TestIdentityRejectsCertificateWithoutKey(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	identityMustLoad(t, dir)
	if err := os.Remove(filepath.Join(dir, identityKeyFileName)); err != nil {
		t.Fatalf("remove key: %v", err)
	}
	if _, err := loadIdentity(dir, identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("loadIdentity with certificate but no key = %v, want ErrIdentity", err)
	}
}

// TestIdentityRejectsMismatchedKeyAndCertificate checks that loadIdentity
// rejects a certificate issued for a different key.
func TestIdentityRejectsMismatchedKeyAndCertificate(t *testing.T) {
	t.Parallel()

	keyDir := t.TempDir()
	certDir := t.TempDir()
	identityMustLoad(t, keyDir)
	identityMustLoad(t, certDir)

	dir := t.TempDir()
	identityWriteFile(t, dir, identityKeyFileName, identityReadFile(t, filepath.Join(keyDir, identityKeyFileName)), 0o600)
	identityWriteFile(t, dir, identityCertFileName, identityReadFile(t, filepath.Join(certDir, identityCertFileName)), 0o600)

	if _, err := loadIdentity(dir, identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("loadIdentity with a mismatched key and certificate = %v, want ErrIdentity", err)
	}
}

// TestIdentityRejectsMalformedFiles checks that loadIdentity rejects PEM
// blocks with a wrong type or trailing data and files that are not PEM.
func TestIdentityRejectsMalformedFiles(t *testing.T) {
	t.Parallel()

	validDir := t.TempDir()
	identityMustLoad(t, validDir)
	validKey := identityReadFile(t, filepath.Join(validDir, identityKeyFileName))
	validCert := identityReadFile(t, filepath.Join(validDir, identityCertFileName))

	tests := []struct {
		name string
		key  []byte
		cert []byte
	}{
		{"garbage key", []byte("not a key"), validCert},
		{"wrong key PEM type", bytes.Replace(validKey, []byte("PRIVATE KEY"), []byte("EC PRIVATE KEY"), 1), validCert},
		{"key trailing data", append(append([]byte(nil), validKey...), []byte("junk")...), validCert},
		{"garbage certificate", validKey, []byte("not a certificate")},
		{"wrong certificate PEM type", validKey, bytes.Replace(validCert, []byte("CERTIFICATE"), []byte("TRUSTED CERTIFICATE"), 1)},
		{"certificate trailing data", validKey, append(append([]byte(nil), validCert...), []byte("junk")...)},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			dir := t.TempDir()
			identityWriteFile(t, dir, identityKeyFileName, tt.key, 0o600)
			identityWriteFile(t, dir, identityCertFileName, tt.cert, 0o600)
			if _, err := loadIdentity(dir, identityTestTime); !errors.Is(err, ErrIdentity) {
				t.Fatalf("loadIdentity(%s) = %v, want ErrIdentity", tt.name, err)
			}
		})
	}
}

// TestIdentityCertificateProperties checks that the generated certificate is a
// self-signed Ed25519 leaf with no host names.
func TestIdentityCertificateProperties(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	identityMustLoad(t, dir)
	cert := identityParseCertificate(t, dir)

	if got, want := cert.Subject.CommonName, "peers"; got != want {
		t.Errorf("CommonName = %q, want %q", got, want)
	}
	if len(cert.DNSNames) != 0 || len(cert.IPAddresses) != 0 || len(cert.EmailAddresses) != 0 || len(cert.URIs) != 0 {
		t.Errorf("certificate carries host names: DNS=%v IP=%v email=%v URI=%v", cert.DNSNames, cert.IPAddresses, cert.EmailAddresses, cert.URIs)
	}
	if _, ok := cert.PublicKey.(ed25519.PublicKey); !ok {
		t.Errorf("certificate public key type = %T, want Ed25519", cert.PublicKey)
	}
	if cert.IsCA {
		t.Error("certificate is a CA, want a leaf certificate")
	}
	if !cert.BasicConstraintsValid {
		t.Error("certificate has no basic constraints")
	}
	if cert.KeyUsage&x509.KeyUsageDigitalSignature == 0 {
		t.Error("certificate lacks KeyUsageDigitalSignature")
	}
	for _, want := range []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth} {
		found := false
		for _, got := range cert.ExtKeyUsage {
			if got == want {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("certificate lacks ExtKeyUsage %v", want)
		}
	}
	if err := cert.CheckSignature(cert.SignatureAlgorithm, cert.RawTBSCertificate, cert.Signature); err != nil {
		t.Errorf("certificate is not self-signed: %v", err)
	}
	issued := identityTestTime()
	if !cert.NotBefore.Equal(issued.Add(-5 * time.Minute)) {
		t.Errorf("NotBefore = %s, want %s", cert.NotBefore, issued.Add(-5*time.Minute))
	}
	if !cert.NotAfter.Equal(issued.AddDate(100, 0, 0)) {
		t.Errorf("NotAfter = %s, want %s", cert.NotAfter, issued.AddDate(100, 0, 0))
	}
	if cert.SerialNumber == nil || cert.SerialNumber.Sign() <= 0 || cert.SerialNumber.BitLen() > 128 {
		t.Errorf("serial number = %v, want a positive 128-bit value", cert.SerialNumber)
	}
}

// TestFingerprintOfRawPublicKeyEqualsCertificateFingerprint checks that
// FingerprintOf agrees for a raw public key and a SubjectPublicKeyInfo and
// returns the zero value for other inputs.
func TestFingerprintOfRawPublicKeyEqualsCertificateFingerprint(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	id := identityMustLoad(t, dir)
	want := id.fingerprint()
	cert := identityParseCertificate(t, dir)

	if got := FingerprintOf(id.publicKey()); got != want {
		t.Fatalf("FingerprintOf(raw public key) = %s, want %s", got, want)
	}
	if got := FingerprintOf(cert.RawSubjectPublicKeyInfo); got != want {
		t.Fatalf("FingerprintOf(SubjectPublicKeyInfo) = %s, want %s", got, want)
	}
	if got := Fingerprint(sha256.Sum256(cert.Raw)); got == want {
		t.Fatal("FingerprintOf accepts the whole certificate DER as a public key")
	}

	for _, n := range []int{0, 1, ed25519.PublicKeySize - 1, ed25519.PublicKeySize + 1, 44 - 1, 44 + 1, 64} {
		if got := FingerprintOf(make([]byte, n)); got != (Fingerprint{}) {
			t.Errorf("FingerprintOf(%d zero bytes) = %s, want zero fingerprint", n, got)
		}
	}
	if got := FingerprintOf(make([]byte, 44)); got != (Fingerprint{}) {
		t.Errorf("FingerprintOf(garbage SubjectPublicKeyInfo) = %s, want zero fingerprint", got)
	}
}

// TestIdentitySignAndVerify checks that a signature verifies with the identity
// public key and that mismatched inputs are rejected.
func TestIdentitySignAndVerify(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	id := identityMustLoad(t, dir)
	cert := identityParseCertificate(t, dir)
	message := []byte("peers signature test")

	sig := id.sign(message)
	if got := len(sig); got != ed25519.SignatureSize {
		t.Fatalf("signature length = %d, want %d", got, ed25519.SignatureSize)
	}
	if !Verify(id.publicKey(), message, sig) {
		t.Fatal("Verify rejected a valid signature")
	}
	if !Verify(cert.RawSubjectPublicKeyInfo, message, sig) {
		t.Fatal("Verify rejected a valid signature with a SubjectPublicKeyInfo public key")
	}
	if !bytes.Equal(sig, id.sign(message)) {
		t.Fatal("Ed25519 signatures are nondeterministic")
	}
	if Verify(id.publicKey(), []byte("different message"), sig) {
		t.Fatal("Verify accepted a signature for a different message")
	}
	if Verify(id.publicKey(), message, append([]byte(nil), sig[:len(sig)-1]...)) {
		t.Fatal("Verify accepted a truncated signature")
	}
	if Verify(nil, message, sig) {
		t.Fatal("Verify accepted a nil public key")
	}
	if Verify(make([]byte, 31), message, sig) {
		t.Fatal("Verify accepted a 31-byte public key")
	}
	if Verify(id.publicKey(), message, nil) {
		t.Fatal("Verify accepted a nil signature")
	}

	other := identityMustLoad(t, t.TempDir())
	if Verify(other.publicKey(), message, sig) {
		t.Fatal("Verify accepted a signature under another identity's key")
	}
}

// TestIdentityPublicKeyReturnsCopy checks that mutating the returned public key
// does not change the identity.
func TestIdentityPublicKeyReturnsCopy(t *testing.T) {
	t.Parallel()

	id := identityMustLoad(t, t.TempDir())
	pub := id.publicKey()
	original := append([]byte(nil), pub...)
	pub[0] ^= 0xff
	if bytes.Equal(pub, original) {
		t.Fatal("test mutation did not change the copy")
	}
	if !bytes.Equal(id.publicKey(), original) {
		t.Fatal("mutating the returned public key changed the identity")
	}
}

// TestIdentityRejectsForeignOwner checks that a key owned by another uid is
// rejected, skipping without root to change ownership.
func TestIdentityRejectsForeignOwner(t *testing.T) {
	if os.Geteuid() != 0 {
		t.Skip("requires root to change file ownership")
	}
	t.Parallel()

	dir := t.TempDir()
	identityMustLoad(t, dir)
	path := filepath.Join(dir, identityKeyFileName)
	if err := os.Chown(path, 1, 1); err != nil {
		t.Skipf("cannot change file ownership: %v", err)
	}
	if _, err := loadIdentity(dir, identityTestTime); !errors.Is(err, ErrIdentity) {
		t.Fatalf("loadIdentity with a foreign-owned key = %v, want ErrIdentity", err)
	}
}

// TestWriteIdentityFileExclusiveInstallsCompleteFile checks that linking a
// temporary file installs the complete data with mode 0600 and leaves no
// temporary file behind.
func TestWriteIdentityFileExclusiveInstallsCompleteFile(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, identityKeyFileName)
	data := []byte("private key material")
	if err := writeIdentityFileExclusive(dir, path, data); err != nil {
		t.Fatalf("writeIdentityFileExclusive: %v", err)
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read installed file: %v", err)
	}
	if !bytes.Equal(got, data) {
		t.Fatalf("installed data = %q, want %q", got, data)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stat installed file: %v", err)
	}
	if mode := info.Mode().Perm(); mode != identityFileMode {
		t.Fatalf("installed mode = %#o, want %#o", mode, identityFileMode)
	}
	temps, err := filepath.Glob(filepath.Join(dir, "."+identityKeyFileName+"-*"))
	if err != nil {
		t.Fatalf("glob temporary files: %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("temporary files left behind: %v", temps)
	}
}

// TestWriteIdentityFileExclusiveRejectsExistingTarget checks that an existing
// target is never overwritten and the temporary file is removed.
func TestWriteIdentityFileExclusiveRejectsExistingTarget(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	path := filepath.Join(dir, identityKeyFileName)
	existing := []byte("existing key")
	if err := os.WriteFile(path, existing, 0o600); err != nil {
		t.Fatalf("seed target: %v", err)
	}
	if err := writeIdentityFileExclusive(dir, path, []byte("replacement")); err == nil {
		t.Fatal("writeIdentityFileExclusive overwrote an existing target")
	}
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read target: %v", err)
	}
	if !bytes.Equal(got, existing) {
		t.Fatalf("existing target changed to %q", got)
	}
	temps, err := filepath.Glob(filepath.Join(dir, "."+identityKeyFileName+"-*"))
	if err != nil {
		t.Fatalf("glob temporary files: %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("temporary files left behind: %v", temps)
	}
}

const identityWriteFailureEnv = "PEERS_TEST_IDENTITY_WRITE_FAILURE_DIR"

// TestGenerateIdentityWriteFailureLeavesNoPartialKey checks that a key write
// that fails partway leaves no identity.key and no temporary file.
func TestGenerateIdentityWriteFailureLeavesNoPartialKey(t *testing.T) {
	if dir := os.Getenv(identityWriteFailureEnv); dir != "" {
		identityWriteFailureChild(t, dir)
		return
	}

	dir := t.TempDir()
	cmd := exec.Command(os.Args[0], "-test.run=^TestGenerateIdentityWriteFailureLeavesNoPartialKey$")
	cmd.Env = append(os.Environ(), identityWriteFailureEnv+"="+dir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("child process failed: %v\n%s", err, out)
	}
}

func identityWriteFailureChild(t *testing.T, dir string) {
	t.Helper()

	// Go ignores SIGXFSZ, so the write returns EFBIG instead of terminating
	// the process once the file size limit is exceeded.
	var orig syscall.Rlimit
	if err := syscall.Getrlimit(syscall.RLIMIT_FSIZE, &orig); err != nil {
		t.Fatalf("get RLIMIT_FSIZE: %v", err)
	}
	// The PKCS#8 PEM private key is longer than this, so the write fails after
	// writing the limit and before the whole key reaches the file.
	if err := syscall.Setrlimit(syscall.RLIMIT_FSIZE, &syscall.Rlimit{Cur: 64, Max: orig.Max}); err != nil {
		t.Fatalf("set RLIMIT_FSIZE: %v", err)
	}
	defer func() { _ = syscall.Setrlimit(syscall.RLIMIT_FSIZE, &orig) }()

	if _, err := generateIdentity(dir, identityTestTime); err == nil {
		t.Fatal("generateIdentity succeeded although the key write could not finish")
	}
	keyPath := filepath.Join(dir, identityKeyFileName)
	if info, err := os.Stat(keyPath); err == nil {
		t.Fatalf("failed key write left %s behind with %d bytes", keyPath, info.Size())
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stat %s: %v", keyPath, err)
	}
	temps, err := filepath.Glob(filepath.Join(dir, "."+identityKeyFileName+"-*"))
	if err != nil {
		t.Fatalf("glob temporary files: %v", err)
	}
	if len(temps) != 0 {
		t.Fatalf("temporary files left behind: %v", temps)
	}
}
