package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
)

// TestApplyReplacesBinary checks that Apply replaces the target with the release
// binary and preserves its mode and owner.
func TestApplyReplacesBinary(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	archive := releaseArchive(t, version)
	checksums := checksumsFor([]byte("another platform"), "peers_0.2.0_darwin_arm64.tar.gz") +
		checksumsFor(archive, testArchiveName(version))
	srv := newApplyServer(t, version, writeBytes(archive), checksums)

	if err := Apply(context.Background(), testApplyOptions(srv, target, version)); err != nil {
		t.Fatalf("Apply() error = %v", err)
	}

	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("reading the replaced binary: %v", err)
	}
	if !bytes.Equal(got, versionScript(version)) {
		t.Errorf("target content = %q, want the staged binary", got)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat: %v", err)
	}
	if info.Mode().Perm() != before.Mode().Perm() {
		t.Errorf("target mode = %v, want the original %v", info.Mode().Perm(), before.Mode().Perm())
	}
	assertSameOwner(t, target, before)

	entries, err := os.ReadDir(filepath.Dir(target))
	if err != nil {
		t.Fatalf("reading the target directory: %v", err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), ".peers-update-") {
			t.Errorf("temporary file %q was left behind", entry.Name())
		}
	}
}

// TestApplyFailureLeavesTargetUnchanged checks that every bad archive or
// checksum leaves the target unchanged.
func TestApplyFailureLeavesTargetUnchanged(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")

	good := releaseArchive(t, version)
	corrupt := []byte("this is not a gzip archive")
	traversal := releaseArchive(t, version, tarEntry{name: "../escape", body: []byte("x")})
	symlink := tarGz(t,
		tarEntry{name: binaryName, body: versionScript(version), mode: 0o755},
		tarEntry{name: "peers-link", typeflag: tar.TypeSymlink, linkname: binaryName})
	hardlink := tarGz(t,
		tarEntry{name: binaryName, body: versionScript(version), mode: 0o755},
		tarEntry{name: "peers-link", typeflag: tar.TypeLink, linkname: binaryName})
	duplicateBinary := tarGz(t,
		tarEntry{name: binaryName, body: versionScript(version), mode: 0o755},
		tarEntry{name: binaryName, body: versionScript(version), mode: 0o755})
	wrongName := tarGz(t, tarEntry{name: "peers-other", body: versionScript(version), mode: 0o755})
	wrongVersion := releaseArchive(t, "9.9.9")

	tests := []struct {
		name         string
		writeArchive func(http.ResponseWriter) error
		checksums    string
		errContains  string
	}{
		{"checksum mismatch", writeBytes(good), checksumsFor([]byte("another archive"), testArchiveName(version)), "checksum"},
		{"corrupt archive", writeBytes(corrupt), checksumsFor(corrupt, testArchiveName(version)), "archive"},
		{"path traversal", writeBytes(traversal), checksumsFor(traversal, testArchiveName(version)), "unsafe path"},
		{"symbolic link", writeBytes(symlink), checksumsFor(symlink, testArchiveName(version)), "link"},
		{"hard link", writeBytes(hardlink), checksumsFor(hardlink, testArchiveName(version)), "link"},
		{"duplicate binary", writeBytes(duplicateBinary), checksumsFor(duplicateBinary, testArchiveName(version)), "more than one"},
		{"wrong package name", writeBytes(wrongName), checksumsFor(wrongName, testArchiveName(version)), "no peers"},
		{"wrong binary version", writeBytes(wrongVersion), checksumsFor(wrongVersion, testArchiveName(version)), "reported"},
		{"duplicate checksum", writeBytes(good), checksumsFor(good, testArchiveName(version)) + checksumsFor(good, testArchiveName(version)), "more than once"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target, before := newTarget(t, original, 0o750)
			srv := newApplyServer(t, version, tc.writeArchive, tc.checksums)

			err := Apply(context.Background(), testApplyOptions(srv, target, version))
			if err == nil {
				t.Fatal("Apply() = nil, want an error")
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Errorf("Apply() error = %v, want it to contain %q", err, tc.errContains)
			}
			assertUnchanged(t, target, original, before)
		})
	}
}

// TestApplyRejectsOversizedDownload checks that a download above the size limit
// is refused and the target is unchanged.
func TestApplyRejectsOversizedDownload(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	oversized := func(w http.ResponseWriter) error {
		w.Header().Set("Content-Length", strconv.FormatInt(maxDownloadSize+1, 10))
		_, err := io.CopyN(w, zeroReader{}, maxDownloadSize+1)
		return err
	}
	srv := newApplyServer(t, version, oversized, "")

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("Apply() error = %v, want a size-limit error", err)
	}
	assertUnchanged(t, target, original, before)
}

// TestApplyRejectsOversizedExtraction checks that an extracted binary above the
// size limit is refused and the target is unchanged.
func TestApplyRejectsOversizedExtraction(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	archive := oversizedBinaryArchive(t)
	srv := newApplyServer(t, version, writeBytes(archive), checksumsFor(archive, testArchiveName(version)))

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "limit") {
		t.Errorf("Apply() error = %v, want a size-limit error", err)
	}
	assertUnchanged(t, target, original, before)
}

// TestApplyRejectsOversizedChecksums checks that a checksums file above the size
// limit is refused and the target is unchanged.
func TestApplyRejectsOversizedChecksums(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	oversized := func(w http.ResponseWriter) error {
		_, err := w.Write(bytes.Repeat([]byte{'x'}, int(maxChecksumsSize)+1))
		return err
	}
	srv := newFakeReleaseServer(t, fakeRelease{
		version:        version,
		tagName:        "v" + version,
		writeArchive:   writeBytes(releaseArchive(t, version)),
		writeChecksums: oversized,
	})

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	want := fmt.Sprintf("download %s/download/checksums exceeds the %d-byte limit", srv.URL, maxChecksumsSize)
	if !strings.Contains(err.Error(), want) {
		t.Errorf("Apply() error = %v, want it to contain %q", err, want)
	}
	assertUnchanged(t, target, original, before)
}

// TestExpectedChecksumAcceptsAFullLine checks that a checksums line of exactly
// maxChecksumsLineSize bytes is accepted.
func TestExpectedChecksumAcceptsAFullLine(t *testing.T) {
	const version = "0.2.0"
	name := testArchiveName(version)
	sum := strings.Repeat("a", 2*sha256.Size) // 64 hex digits decode to 32 bytes of 0xaa
	line := sum + "  " + name
	line += strings.Repeat(" ", maxChecksumsLineSize-len(line))
	if len(line) != maxChecksumsLineSize {
		t.Fatalf("the test line is %d bytes, want exactly %d", len(line), maxChecksumsLineSize)
	}

	got, err := expectedChecksum([]byte(line+"\n"), name)
	if err != nil {
		t.Fatalf("expectedChecksum() error = %v, want a full-length line to be accepted", err)
	}
	want := bytes.Repeat([]byte{0xaa}, sha256.Size)
	if !bytes.Equal(got, want) {
		t.Errorf("expectedChecksum() = %x, want %x", got, want)
	}
}

// TestExpectedChecksumRejectsOverlongLine checks that a checksums line above
// maxChecksumsLineSize bytes is refused.
func TestExpectedChecksumRejectsOverlongLine(t *testing.T) {
	line := strings.Repeat("x", maxChecksumsLineSize+1)

	_, err := expectedChecksum([]byte(line+"\n"), testArchiveName("0.2.0"))
	if err == nil {
		t.Fatal("expectedChecksum() = nil, want an error")
	}
	want := fmt.Sprintf("%s has a line longer than the %d-byte limit", checksumsAsset, maxChecksumsLineSize)
	if err.Error() != want {
		t.Errorf("expectedChecksum() error = %q, want %q", err, want)
	}
}

// TestApplyRejectsReleaseMetadata checks that a wrong tag, a draft or a
// prerelease is refused and the target is unchanged.
func TestApplyRejectsReleaseMetadata(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	archive := releaseArchive(t, version)
	checksums := checksumsFor(archive, testArchiveName(version))

	tests := []struct {
		name        string
		tagName     string
		draft       bool
		prerelease  bool
		errContains string
	}{
		{"tag mismatch", "v9.9.9", false, false, "tag"},
		{"draft", "v0.2.0", true, false, "draft"},
		{"prerelease", "v0.2.0", false, true, "prerelease"},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			target, before := newTarget(t, original, 0o750)
			srv := newFakeReleaseServer(t, fakeRelease{
				version:        version,
				tagName:        tc.tagName,
				draft:          tc.draft,
				prerelease:     tc.prerelease,
				writeArchive:   writeBytes(archive),
				writeChecksums: writeBytes([]byte(checksums)),
			})

			err := Apply(context.Background(), testApplyOptions(srv, target, version))
			if err == nil {
				t.Fatal("Apply() = nil, want an error")
			}
			if !strings.Contains(err.Error(), tc.errContains) {
				t.Errorf("Apply() error = %v, want it to contain %q", err, tc.errContains)
			}
			assertUnchanged(t, target, original, before)
		})
	}
}

// TestApplyRejectsNonReleaseVersion checks that a non-release version is refused
// and the target stays unchanged.
func TestApplyRejectsNonReleaseVersion(t *testing.T) {
	for _, version := range []string{"0.2.0-rc.1", "dev", "0.1", "0.2.0+build.7"} {
		t.Run(version, func(t *testing.T) {
			original := []byte("original binary")
			target, before := newTarget(t, original, 0o750)

			err := Apply(context.Background(), ApplyOptions{
				Client:  noNetworkClient(t),
				Target:  target,
				Version: version,
				GOOS:    "linux",
				GOARCH:  "amd64",
			})
			if err == nil {
				t.Fatal("Apply() = nil, want an error")
			}
			assertUnchanged(t, target, original, before)
		})
	}
}

// TestApplyRejectsSymlinkTarget checks that a symbolic-link target is refused
// and the file it points at is unchanged.
func TestApplyRejectsSymlinkTarget(t *testing.T) {
	original := []byte("original binary")
	dir := t.TempDir()
	real := filepath.Join(dir, "real-peers")
	if err := os.WriteFile(real, original, 0o755); err != nil {
		t.Fatalf("writing the real binary: %v", err)
	}
	link := filepath.Join(dir, binaryName)
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("creating the target symlink: %v", err)
	}

	err := Apply(context.Background(), ApplyOptions{
		Client:  noNetworkClient(t),
		Target:  link,
		Version: "0.2.0",
		GOOS:    "linux",
		GOARCH:  "amd64",
	})
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "symbolic link") {
		t.Errorf("Apply() error = %v, want it to mention a symbolic link", err)
	}
	got, err := os.ReadFile(real)
	if err != nil {
		t.Fatalf("reading the real binary: %v", err)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("real binary changed: got %q, want %q", got, original)
	}
}

func oversizedBinaryArchive(t *testing.T) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	hdr := &tar.Header{Name: binaryName, Mode: 0o755, Size: maxDownloadSize + 1, Typeflag: tar.TypeReg}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("writing the archive header: %v", err)
	}
	if _, err := io.CopyN(tw, zeroReader{}, maxDownloadSize+1); err != nil {
		t.Fatalf("writing the oversized binary: %v", err)
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("closing the tar writer: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("closing the gzip writer: %v", err)
	}
	return buf.Bytes()
}

type tarEntry struct {
	name     string
	body     []byte
	mode     int64
	typeflag byte
	linkname string
}

func tarGz(t *testing.T, entries ...tarEntry) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, entry := range entries {
		mode := entry.mode
		if mode == 0 {
			mode = 0o644
		}
		typeflag := entry.typeflag
		if typeflag == 0 {
			typeflag = tar.TypeReg
		}
		hdr := &tar.Header{Name: entry.name, Mode: mode, Typeflag: typeflag, Linkname: entry.linkname}
		if typeflag == tar.TypeReg {
			hdr.Size = int64(len(entry.body))
		}
		if err := tw.WriteHeader(hdr); err != nil {
			t.Fatalf("writing the archive header: %v", err)
		}
		if hdr.Size > 0 {
			if _, err := tw.Write(entry.body); err != nil {
				t.Fatalf("writing the archive entry: %v", err)
			}
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("closing the tar writer: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("closing the gzip writer: %v", err)
	}
	return buf.Bytes()
}

func versionScript(version string) []byte {
	return []byte("#!/bin/sh\necho \"peers " + version + "\"\n")
}

func releaseArchive(t *testing.T, scriptVersion string, extra ...tarEntry) []byte {
	t.Helper()
	entries := []tarEntry{
		{name: "README.md", body: []byte("readme\n")},
		{name: binaryName, body: versionScript(scriptVersion), mode: 0o755},
	}
	entries = append(entries, extra...)
	return tarGz(t, entries...)
}

func checksumsFor(archive []byte, name string) string {
	sum := sha256.Sum256(archive)
	return fmt.Sprintf("%x  %s\n", sum, name)
}

func testArchiveName(version string) string {
	return fmt.Sprintf("%s_%s_linux_amd64.tar.gz", binaryName, version)
}

func writeBytes(b []byte) func(http.ResponseWriter) error {
	return func(w http.ResponseWriter) error {
		_, err := w.Write(b)
		return err
	}
}

type fakeRelease struct {
	version        string
	tagName        string
	draft          bool
	prerelease     bool
	writeArchive   func(http.ResponseWriter) error
	writeChecksums func(http.ResponseWriter) error
}

func newApplyServer(t *testing.T, version string, writeArchive func(http.ResponseWriter) error, checksums string) *httptest.Server {
	t.Helper()
	return newFakeReleaseServer(t, fakeRelease{
		version:        version,
		tagName:        "v" + version,
		writeArchive:   writeArchive,
		writeChecksums: writeBytes([]byte(checksums)),
	})
}

// newFakeReleaseServer serves rel over TLS so the release build's https rule
// holds.
func newFakeReleaseServer(t *testing.T, rel fakeRelease) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/wangzw/peers/releases/tags/v"+rel.version, func(w http.ResponseWriter, _ *http.Request) {
		payload := map[string]any{
			"tag_name":   rel.tagName,
			"draft":      rel.draft,
			"prerelease": rel.prerelease,
			"html_url":   srv.URL + "/releases/" + rel.tagName,
			"assets": []map[string]string{
				{"name": testArchiveName(rel.version), "browser_download_url": srv.URL + "/download/archive"},
				{"name": checksumsAsset, "browser_download_url": srv.URL + "/download/checksums"},
			},
		}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Errorf("encoding the fake release: %v", err)
		}
	})
	mux.HandleFunc("/download/archive", func(w http.ResponseWriter, _ *http.Request) {
		// A write error here usually means the client stopped reading because
		// of a size limit, which the test asserts on the Apply error instead.
		_ = rel.writeArchive(w)
	})
	mux.HandleFunc("/download/checksums", func(w http.ResponseWriter, _ *http.Request) {
		_ = rel.writeChecksums(w)
	})
	srv = httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func testApplyOptions(srv *httptest.Server, target, version string) ApplyOptions {
	return ApplyOptions{
		Target:  target,
		Version: version,
		API:     srv.URL,
		Client:  srv.Client(),
		GOOS:    "linux",
		GOARCH:  "amd64",
	}
}

func newTarget(t *testing.T, content []byte, mode os.FileMode) (string, os.FileInfo) {
	t.Helper()
	target := filepath.Join(t.TempDir(), binaryName)
	if err := os.WriteFile(target, content, mode); err != nil {
		t.Fatalf("writing the target: %v", err)
	}
	// WriteFile applies the umask, so set the mode explicitly.
	if err := os.Chmod(target, mode); err != nil {
		t.Fatalf("chmod the target: %v", err)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat the target: %v", err)
	}
	return target, info
}

func assertUnchanged(t *testing.T, target string, want []byte, before os.FileInfo) {
	t.Helper()
	got, err := os.ReadFile(target)
	if err != nil {
		t.Fatalf("reading the target after a failed Apply: %v", err)
	}
	if !bytes.Equal(got, want) {
		t.Errorf("target bytes changed after a failed Apply: got %q, want %q", got, want)
	}
	info, err := os.Stat(target)
	if err != nil {
		t.Fatalf("stat after a failed Apply: %v", err)
	}
	if info.Mode().Perm() != before.Mode().Perm() {
		t.Errorf("target mode changed after a failed Apply: got %v, want %v", info.Mode().Perm(), before.Mode().Perm())
	}
	assertSameOwner(t, target, before)
}

type zeroReader struct{}

func (zeroReader) Read(p []byte) (int, error) {
	clear(p)
	return len(p), nil
}
