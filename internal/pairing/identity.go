package pairing

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"syscall"
	"time"
)

const (
	// identityKeyFileName is the private key file inside the data directory.
	identityKeyFileName = "identity.key"
	// identityCertFileName is the self-signed certificate file inside the data
	// directory.
	identityCertFileName = "identity.crt"

	identityKeyPEMType  = "PRIVATE KEY"
	identityCertPEMType = "CERTIFICATE"

	// identityFileMode is the mode of both identity files. Only the account
	// running the Broker may read them.
	identityFileMode os.FileMode = 0o600
)

// ed25519SubjectPublicKeyInfoPrefix is the DER header that x509 wraps around a
// raw 32-byte Ed25519 public key when it builds a SubjectPublicKeyInfo. It lets
// the package derive a fingerprint from a bare public key, as directory records
// publish one.
var ed25519SubjectPublicKeyInfoPrefix = []byte{
	0x30, 0x2a, 0x30, 0x05, 0x06, 0x03, 0x2b, 0x65, 0x70, 0x03, 0x21, 0x00,
}

// identity is the local Broker's Ed25519 key pair and self-signed certificate.
// The fingerprint is derived from the certificate's SubjectPublicKeyInfo and
// therefore survives a certificate reissue with the same key.
type identity struct {
	privateKey ed25519.PrivateKey
	cert       tls.Certificate
	fp         Fingerprint
	pub        []byte
}

// loadIdentity loads the identity from dataDir, generating one on first use.
// It never replaces an existing key or a certificate that does not match its
// key: such states return ErrIdentity so a Broker cannot silently adopt a new
// identity.
func loadIdentity(dataDir string, now func() time.Time) (*identity, error) {
	if dataDir == "" {
		return nil, fmt.Errorf("%w: empty data directory", ErrIdentity)
	}
	keyPath := filepath.Join(dataDir, identityKeyFileName)
	certPath := filepath.Join(dataDir, identityCertFileName)

	keyData, keyErr := readIdentityFile(keyPath)
	if keyErr != nil && !errors.Is(keyErr, os.ErrNotExist) {
		return nil, keyErr
	}
	certData, certErr := readIdentityFile(certPath)
	if certErr != nil && !errors.Is(certErr, os.ErrNotExist) {
		return nil, certErr
	}

	keyMissing := errors.Is(keyErr, os.ErrNotExist)
	certMissing := errors.Is(certErr, os.ErrNotExist)

	switch {
	case keyMissing && certMissing:
		return generateIdentity(dataDir, now)
	case keyMissing:
		return nil, fmt.Errorf("%w: %s is present but %s is missing", ErrIdentity, certPath, keyPath)
	case certMissing:
		// A crash between writing the key and writing the certificate is
		// recoverable: the fingerprint comes from the key, so reissuing the
		// certificate keeps the identity stable.
		priv, err := parseIdentityPrivateKey(keyData)
		if err != nil {
			return nil, err
		}
		return generateCertificate(dataDir, priv, now)
	}

	priv, err := parseIdentityPrivateKey(keyData)
	if err != nil {
		return nil, err
	}
	cert, err := parseIdentityCertificate(certData)
	if err != nil {
		return nil, err
	}
	if err := checkCertificateMatchesKey(cert, priv); err != nil {
		return nil, err
	}
	return newIdentity(priv, cert), nil
}

// generateIdentity creates a fresh key pair and writes both files. The private
// key is created exclusively so a concurrent generator cannot be overwritten.
func generateIdentity(dataDir string, now func() time.Time) (*identity, error) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		return nil, fmt.Errorf("%w: generate Ed25519 key: %v", ErrIdentity, err)
	}

	certDER, err := createIdentityCertificate(priv.Public().(ed25519.PublicKey), priv, now)
	if err != nil {
		return nil, err
	}

	keyPEM, err := encodeIdentityPrivateKey(priv)
	if err != nil {
		return nil, err
	}
	keyPath := filepath.Join(dataDir, identityKeyFileName)
	if err := writeIdentityFileExclusive(dataDir, keyPath, keyPEM); err != nil {
		return nil, fmt.Errorf("%w: write %s: %v", ErrIdentity, keyPath, err)
	}
	// Make the new directory entry durable before the certificate is written:
	// a crash here must leave the key behind, not lose it to a new identity.
	if err := syncIdentityDir(dataDir); err != nil {
		return nil, fmt.Errorf("%w: sync %s: %v", ErrIdentity, dataDir, err)
	}

	return installCertificate(dataDir, priv, certDER)
}

// generateCertificate issues a new self-signed certificate for an existing
// private key and replaces identity.crt atomically.
func generateCertificate(dataDir string, priv ed25519.PrivateKey, now func() time.Time) (*identity, error) {
	certDER, err := createIdentityCertificate(priv.Public().(ed25519.PublicKey), priv, now)
	if err != nil {
		return nil, err
	}
	return installCertificate(dataDir, priv, certDER)
}

// installCertificate writes certDER as identity.crt and returns the in-memory
// identity built from it.
func installCertificate(dataDir string, priv ed25519.PrivateKey, certDER []byte) (*identity, error) {
	certPath := filepath.Join(dataDir, identityCertFileName)
	if err := writeIdentityCertificate(dataDir, certPath, certDER); err != nil {
		return nil, err
	}
	cert, err := parseIdentityCertificate(pem.EncodeToMemory(&pem.Block{
		Type:  identityCertPEMType,
		Bytes: certDER,
	}))
	if err != nil {
		return nil, err
	}
	return newIdentity(priv, cert), nil
}

// createIdentityCertificate builds the self-signed certificate. It carries no
// host names: the fingerprint is the identity, and names would only suggest a
// trust path that does not exist.
func createIdentityCertificate(pub ed25519.PublicKey, priv ed25519.PrivateKey, now func() time.Time) ([]byte, error) {
	serial, err := rand.Int(rand.Reader, new(big.Int).Lsh(big.NewInt(1), 128))
	if err != nil {
		return nil, fmt.Errorf("%w: generate certificate serial: %v", ErrIdentity, err)
	}
	issued := now()
	template := &x509.Certificate{
		SerialNumber:          serial,
		Subject:               pkix.Name{CommonName: "peers"},
		NotBefore:             issued.Add(-5 * time.Minute),
		NotAfter:              issued.AddDate(100, 0, 0),
		KeyUsage:              x509.KeyUsageDigitalSignature,
		ExtKeyUsage:           []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth, x509.ExtKeyUsageServerAuth},
		BasicConstraintsValid: true,
		IsCA:                  false,
	}
	der, err := x509.CreateCertificate(rand.Reader, template, template, pub, priv)
	if err != nil {
		return nil, fmt.Errorf("%w: create certificate: %v", ErrIdentity, err)
	}
	return der, nil
}

// newIdentity assembles the in-memory identity from a parsed key and
// certificate.
func newIdentity(priv ed25519.PrivateKey, cert *x509.Certificate) *identity {
	pub := append([]byte(nil), cert.PublicKey.(ed25519.PublicKey)...)
	return &identity{
		privateKey: priv,
		cert: tls.Certificate{
			Certificate: [][]byte{append([]byte(nil), cert.Raw...)},
			PrivateKey:  priv,
			Leaf:        cert,
		},
		fp:  fingerprintOfPublicKey(pub),
		pub: pub,
	}
}

// fingerprint returns the certificate fingerprint of this identity.
func (id *identity) fingerprint() Fingerprint {
	return id.fp
}

// publicKey returns a copy of the raw 32-byte Ed25519 public key.
func (id *identity) publicKey() []byte {
	return append([]byte(nil), id.pub...)
}

// sign signs data with the identity's Ed25519 private key.
func (id *identity) sign(data []byte) []byte {
	return ed25519.Sign(id.privateKey, data)
}

// fingerprintOfPublicKey derives a fingerprint from either a raw 32-byte
// Ed25519 public key or a DER SubjectPublicKeyInfo. Any other input yields the
// zero Fingerprint, which callers must treat as "no identity".
func fingerprintOfPublicKey(pub []byte) Fingerprint {
	switch len(pub) {
	case ed25519.PublicKeySize:
		h := sha256.New()
		h.Write(ed25519SubjectPublicKeyInfoPrefix)
		h.Write(pub)
		var fp Fingerprint
		copy(fp[:], h.Sum(nil))
		return fp
	case len(ed25519SubjectPublicKeyInfoPrefix) + ed25519.PublicKeySize:
		if _, err := parseEd25519PublicKeyInfo(pub); err != nil {
			return Fingerprint{}
		}
		return sha256.Sum256(pub)
	default:
		return Fingerprint{}
	}
}

// parseEd25519PublicKeyInfo parses a DER SubjectPublicKeyInfo and requires an
// Ed25519 key.
func parseEd25519PublicKeyInfo(der []byte) (ed25519.PublicKey, error) {
	key, err := x509.ParsePKIXPublicKey(der)
	if err != nil {
		return nil, err
	}
	pub, ok := key.(ed25519.PublicKey)
	if !ok || len(pub) != ed25519.PublicKeySize {
		return nil, errors.New("not an Ed25519 public key")
	}
	return pub, nil
}

// readIdentityFile reads one identity file after checking that it is a regular
// file owned by the effective user and inaccessible to group and other. Every
// failure is reported so that callers can tell a missing file from a rejected
// one.
func readIdentityFile(path string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil, err
		}
		return nil, fmt.Errorf("%w: inspect %s: %v", ErrIdentity, path, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return nil, fmt.Errorf("%w: %s is a symbolic link", ErrIdentity, path)
	}
	if !info.Mode().IsRegular() {
		return nil, fmt.Errorf("%w: %s is not a regular file", ErrIdentity, path)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("%w: %s is accessible by group or other (%#o)", ErrIdentity, path, info.Mode().Perm())
	}
	if sys, ok := info.Sys().(*syscall.Stat_t); ok && int(sys.Uid) != os.Geteuid() {
		return nil, fmt.Errorf("%w: %s is owned by uid %d, not %d", ErrIdentity, path, int(sys.Uid), os.Geteuid())
	}

	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("%w: open %s: %v", ErrIdentity, path, err)
	}
	defer func() { _ = f.Close() }()
	data, err := io.ReadAll(f)
	if err != nil {
		return nil, fmt.Errorf("%w: read %s: %v", ErrIdentity, path, err)
	}
	return data, nil
}

// parseIdentityPrivateKey decodes the PKCS#8 PEM private key.
func parseIdentityPrivateKey(data []byte) (ed25519.PrivateKey, error) {
	block, rest := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("%w: private key is not PEM", ErrIdentity)
	}
	if block.Type != identityKeyPEMType {
		return nil, fmt.Errorf("%w: private key PEM type is %q, want %q", ErrIdentity, block.Type, identityKeyPEMType)
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("%w: private key file has trailing data", ErrIdentity)
	}
	key, err := x509.ParsePKCS8PrivateKey(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse private key: %v", ErrIdentity, err)
	}
	priv, ok := key.(ed25519.PrivateKey)
	if !ok {
		return nil, fmt.Errorf("%w: private key is %T, not Ed25519", ErrIdentity, key)
	}
	if len(priv) != ed25519.PrivateKeySize {
		return nil, fmt.Errorf("%w: Ed25519 private key has length %d, want %d", ErrIdentity, len(priv), ed25519.PrivateKeySize)
	}
	return priv, nil
}

// parseIdentityCertificate decodes the self-signed PEM certificate and requires
// an Ed25519 public key.
func parseIdentityCertificate(data []byte) (*x509.Certificate, error) {
	block, rest := pem.Decode(data)
	if block == nil {
		return nil, fmt.Errorf("%w: certificate is not PEM", ErrIdentity)
	}
	if block.Type != identityCertPEMType {
		return nil, fmt.Errorf("%w: certificate PEM type is %q, want %q", ErrIdentity, block.Type, identityCertPEMType)
	}
	if len(bytes.TrimSpace(rest)) != 0 {
		return nil, fmt.Errorf("%w: certificate file has trailing data", ErrIdentity)
	}
	cert, err := x509.ParseCertificate(block.Bytes)
	if err != nil {
		return nil, fmt.Errorf("%w: parse certificate: %v", ErrIdentity, err)
	}
	if _, ok := cert.PublicKey.(ed25519.PublicKey); !ok {
		return nil, fmt.Errorf("%w: certificate public key is %T, not Ed25519", ErrIdentity, cert.PublicKey)
	}
	return cert, nil
}

// checkCertificateMatchesKey rejects a certificate that was not issued for the
// stored private key. Adopting such a pair would make signing and the TLS
// handshake disagree about the identity.
func checkCertificateMatchesKey(cert *x509.Certificate, priv ed25519.PrivateKey) error {
	certPub, ok := cert.PublicKey.(ed25519.PublicKey)
	if !ok {
		return fmt.Errorf("%w: certificate public key is %T, not Ed25519", ErrIdentity, cert.PublicKey)
	}
	keyPub := priv.Public().(ed25519.PublicKey)
	if !bytes.Equal(certPub, keyPub) {
		return fmt.Errorf("%w: certificate does not match private key", ErrIdentity)
	}
	return nil
}

// encodeIdentityPrivateKey encodes the key as PKCS#8 PEM.
func encodeIdentityPrivateKey(priv ed25519.PrivateKey) ([]byte, error) {
	der, err := x509.MarshalPKCS8PrivateKey(priv)
	if err != nil {
		return nil, fmt.Errorf("%w: encode private key: %v", ErrIdentity, err)
	}
	return pem.EncodeToMemory(&pem.Block{Type: identityKeyPEMType, Bytes: der}), nil
}

// writeIdentityTempFile creates a temporary file in dir with the restricted
// identity file mode and writes data to it. The caller owns the returned path
// and must link or rename it into place, then remove it.
func writeIdentityTempFile(dir, path string, data []byte) (string, error) {
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+"-*")
	if err != nil {
		return "", err
	}
	tmpPath := tmp.Name()
	remove := func() { _ = os.Remove(tmpPath) }

	if err := tmp.Chmod(identityFileMode); err != nil {
		_ = tmp.Close()
		remove()
		return "", err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		remove()
		return "", err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		remove()
		return "", err
	}
	if err := tmp.Close(); err != nil {
		remove()
		return "", err
	}
	return tmpPath, nil
}

// writeIdentityFileExclusive writes data to a temporary file in dir, fsyncs it
// and links it into place. Linking fails when path already exists, which keeps
// the exclusive-create semantics of os.O_EXCL: two generators cannot both
// install a key. The target never holds a partial file because only the link
// makes it visible, and the temporary file is removed on every path.
func writeIdentityFileExclusive(dir, path string, data []byte) error {
	tmpPath, err := writeIdentityTempFile(dir, path, data)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmpPath) }()
	return os.Link(tmpPath, path)
}

// writeIdentityFileAtomic writes data to a temporary file in dir, fsyncs it,
// renames it over path and fsyncs the directory.
func writeIdentityFileAtomic(dir, path string, data []byte) error {
	tmpPath, err := writeIdentityTempFile(dir, path, data)
	if err != nil {
		return err
	}
	defer func() { _ = os.Remove(tmpPath) }()
	if err := os.Rename(tmpPath, path); err != nil {
		return err
	}
	return syncIdentityDir(dir)
}

// writeIdentityCertificate encodes and atomically installs the certificate.
func writeIdentityCertificate(dir, path string, der []byte) error {
	pemData := pem.EncodeToMemory(&pem.Block{Type: identityCertPEMType, Bytes: der})
	if err := writeIdentityFileAtomic(dir, path, pemData); err != nil {
		return fmt.Errorf("%w: write %s: %v", ErrIdentity, path, err)
	}
	return nil
}

// syncIdentityDir fsyncs the directory so a rename survives a crash.
func syncIdentityDir(dir string) error {
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer func() { _ = d.Close() }()
	return d.Sync()
}
