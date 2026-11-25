package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"testing/iotest"
)

// roundTripFunc adapts a function to an http.RoundTripper.
type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) {
	return f(r)
}

// noNetworkClient returns a client that fails the test if the release API is
// reached.
func noNetworkClient(t *testing.T) *http.Client {
	t.Helper()
	return &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		t.Error("Apply contacted the release API although the guard under test should stop earlier")
		return nil, errors.New("network access in a test")
	})}
}

// TestApplyRejectsMissingTarget checks that an empty target path is refused
// before the release API is contacted.
func TestApplyRejectsMissingTarget(t *testing.T) {
	err := Apply(context.Background(), ApplyOptions{
		Version: "0.2.0",
		Client:  noNetworkClient(t),
		GOOS:    "linux",
		GOARCH:  "amd64",
	})
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	if want := "no target binary given"; err.Error() != want {
		t.Errorf("Apply() error = %q, want %q", err, want)
	}
}

// TestApplyRequiresTheTargetPlatform checks that either missing platform field
// is refused.
func TestApplyRequiresTheTargetPlatform(t *testing.T) {
	target, _ := newTarget(t, []byte("original binary"), 0o750)
	tests := []struct {
		name   string
		goos   string
		goarch string
	}{
		{"missing GOOS", "", "amd64"},
		{"missing GOARCH", "linux", ""},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			err := Apply(context.Background(), ApplyOptions{
				Target:  target,
				Version: "0.2.0",
				Client:  noNetworkClient(t),
				GOOS:    tc.goos,
				GOARCH:  tc.goarch,
			})
			if err == nil {
				t.Fatal("Apply() = nil, want an error")
			}
			if !strings.Contains(err.Error(), "GOOS and GOARCH are required") {
				t.Errorf("Apply() error = %v, want it to name the missing platform", err)
			}
		})
	}
}

// TestApplyReportsAnUnreadableTarget checks that a target path whose status
// cannot be read is reported instead of treated as a replacement.
func TestApplyReportsAnUnreadableTarget(t *testing.T) {
	target := filepath.Join(t.TempDir(), "absent")

	err := Apply(context.Background(), ApplyOptions{
		Target:  target,
		Version: "0.2.0",
		Client:  noNetworkClient(t),
		GOOS:    "linux",
		GOARCH:  "amd64",
	})
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	for _, want := range []string{"cannot inspect", target} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Apply() error = %v, want it to contain %q", err, want)
		}
	}
}

// TestApplyRejectsANonRegularTarget checks that a directory is refused as the
// replacement target.
func TestApplyRejectsANonRegularTarget(t *testing.T) {
	dir := t.TempDir()

	err := Apply(context.Background(), ApplyOptions{
		Target:  dir,
		Version: "0.2.0",
		Client:  noNetworkClient(t),
		GOOS:    "linux",
		GOARCH:  "amd64",
	})
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	for _, want := range []string{"not a regular file", dir} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Apply() error = %v, want it to contain %q", err, want)
		}
	}
}

// TestApplyReportsAReleaseRequestFailure checks that a failing release endpoint
// is reported with its status before any asset is fetched.
func TestApplyReportsAReleaseRequestFailure(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "missing", http.StatusNotFound)
	}))
	t.Cleanup(srv.Close)

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	for _, want := range []string{"requesting", "404 Not Found"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Apply() error = %v, want it to contain %q", err, want)
		}
	}
	assertUnchanged(t, target, original, before)
}

// TestApplyRejectsAReleaseWithoutTheArchive checks the error for a release that
// does not list the platform archive.
func TestApplyRejectsAReleaseWithoutTheArchive(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	srv := newReleaseAssetsServer(t, version, func(base string) []map[string]string {
		return []map[string]string{
			{"name": checksumsAsset, "browser_download_url": base + "/download/checksums"},
		}
	})

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	if want := testArchiveName(version); !strings.Contains(err.Error(), want) {
		t.Errorf("Apply() error = %v, want it to name the missing archive %q", err, want)
	}
	assertUnchanged(t, target, original, before)
}

// TestApplyRejectsAReleaseWithoutTheChecksums checks the error for a release
// that does not list checksums.txt.
func TestApplyRejectsAReleaseWithoutTheChecksums(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	srv := newReleaseAssetsServer(t, version, func(base string) []map[string]string {
		return []map[string]string{
			{"name": testArchiveName(version), "browser_download_url": base + "/download/archive"},
		}
	})

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	if !strings.Contains(err.Error(), checksumsAsset) {
		t.Errorf("Apply() error = %v, want it to name %q", err, checksumsAsset)
	}
	assertUnchanged(t, target, original, before)
}

// TestApplyRejectsAnHTTPChecksumsURL checks that an https archive does not
// excuse a plain HTTP checksums URL.
func TestApplyRejectsAnHTTPChecksumsURL(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	srv := newReleaseAssetsServer(t, version, func(base string) []map[string]string {
		return []map[string]string{
			{"name": testArchiveName(version), "browser_download_url": base + "/download/archive"},
			{"name": checksumsAsset, "browser_download_url": strings.Replace(base, "https://", "http://", 1) + "/download/checksums"},
		}
	})

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	for _, want := range []string{"downloads must use https", "/download/checksums"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Apply() error = %v, want it to contain %q", err, want)
		}
	}
	assertUnchanged(t, target, original, before)
}

// TestApplyRejectsABinaryThatFailsTheVersionRun checks that a staged binary
// which cannot run its --version check is reported with its stderr.
func TestApplyRejectsABinaryThatFailsTheVersionRun(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	script := []byte("#!/bin/sh\necho 'cannot start' >&2\nexit 7\n")
	archive := tarGz(t, tarEntry{name: binaryName, body: script, mode: 0o755})
	srv := newApplyServer(t, version, writeBytes(archive), checksumsFor(archive, testArchiveName(version)))

	err := Apply(context.Background(), testApplyOptions(srv, target, version))
	if err == nil {
		t.Fatal("Apply() = nil, want an error")
	}
	for _, want := range []string{"cannot run the downloaded binary", "cannot start"} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("Apply() error = %v, want it to contain %q", err, want)
		}
	}
	assertUnchanged(t, target, original, before)
}

// TestReadAtMostPropagatesAReadFailure checks that a failing reader's error is
// returned rather than reported as a size violation.
func TestReadAtMostPropagatesAReadFailure(t *testing.T) {
	cause := errors.New("read failed")

	_, err := readAtMost(iotest.ErrReader(cause), 16)
	if !errors.Is(err, cause) {
		t.Fatalf("readAtMost() error = %v, want the reader's error", err)
	}
}

// TestDownloadReportsABodyReadFailure checks that a download whose body stops
// early returns the read error with the asset URL.
func TestDownloadReportsABodyReadFailure(t *testing.T) {
	cause := errors.New("connection lost")
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: http.StatusOK,
			Status:     "200 OK",
			Header:     make(http.Header),
			Body:       io.NopCloser(iotest.ErrReader(cause)),
		}, nil
	})}

	_, err := download(context.Background(), client, "https://example.com/asset", maxDownloadSize)
	if !errors.Is(err, cause) {
		t.Fatalf("download() error = %v, want the body's read error", err)
	}
	if want := "cannot read https://example.com/asset"; !strings.Contains(err.Error(), want) {
		t.Errorf("download() error = %v, want it to contain %q", err, want)
	}
}

// TestExpectedChecksumSkipsLinesThatAreNotChecksumPairs checks that noise lines
// do not hide the archive's own entry.
func TestExpectedChecksumSkipsLinesThatAreNotChecksumPairs(t *testing.T) {
	const version = "0.2.0"
	name := testArchiveName(version)
	sum := strings.Repeat("a", 2*sha256.Size)
	checksums := "one field\nthree fields here\n" + sum + "  " + name + "\n"

	got, err := expectedChecksum([]byte(checksums), name)
	if err != nil {
		t.Fatalf("expectedChecksum() error = %v", err)
	}
	want := bytes.Repeat([]byte{0xaa}, sha256.Size)
	if !bytes.Equal(got, want) {
		t.Errorf("expectedChecksum() = %x, want %x", got, want)
	}
}

// TestExpectedChecksumRejectsAnInvalidSum checks both malformed hex and a sum
// of the wrong length.
func TestExpectedChecksumRejectsAnInvalidSum(t *testing.T) {
	name := testArchiveName("0.2.0")
	for _, sum := range []string{strings.Repeat("z", 2*sha256.Size), "abcd"} {
		_, err := expectedChecksum([]byte(sum+"  "+name+"\n"), name)
		if err == nil {
			t.Fatalf("expectedChecksum() = nil for sum %q, want an error", sum)
		}
		if !strings.Contains(err.Error(), "invalid checksum") {
			t.Errorf("expectedChecksum() error = %v, want an invalid-checksum error", err)
		}
	}
}

// TestExpectedChecksumRejectsAnArchiveItDoesNotList checks the error for a
// checksums.txt that names another archive only.
func TestExpectedChecksumRejectsAnArchiveItDoesNotList(t *testing.T) {
	name := testArchiveName("0.2.0")
	sum := strings.Repeat("a", 2*sha256.Size)

	_, err := expectedChecksum([]byte(sum+"  another_archive.tar.gz\n"), name)
	if err == nil {
		t.Fatal("expectedChecksum() = nil, want an error")
	}
	want := fmt.Sprintf("%s has no checksum for %s", checksumsAsset, name)
	if err.Error() != want {
		t.Errorf("expectedChecksum() error = %q, want %q", err, want)
	}
}

// TestExtractBinaryRejectsACorruptTAR checks that a valid gzip stream whose
// payload is not a tar stream is reported.
func TestExtractBinaryRejectsACorruptTAR(t *testing.T) {
	archive := gzipBytes(t, bytes.Repeat([]byte{0xff}, 512))

	_, err := extractBinary(archive)
	if err == nil {
		t.Fatal("extractBinary() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "cannot read the release archive") {
		t.Errorf("extractBinary() error = %v, want it to report the unreadable archive", err)
	}
}

// TestExtractBinaryRejectsATruncatedEntry checks that an entry whose body stops
// early is reported rather than silently shortened.
func TestExtractBinaryRejectsATruncatedEntry(t *testing.T) {
	var raw bytes.Buffer
	tw := tar.NewWriter(&raw)
	hdr := &tar.Header{Name: binaryName, Mode: 0o755, Size: 1024, Typeflag: tar.TypeReg}
	if err := tw.WriteHeader(hdr); err != nil {
		t.Fatalf("writing the archive header: %v", err)
	}
	if _, err := tw.Write(make([]byte, 1024)); err != nil {
		t.Fatalf("writing the archive entry: %v", err)
	}
	archive := gzipBytes(t, raw.Bytes()[:512+4])

	_, err := extractBinary(archive)
	if err == nil {
		t.Fatal("extractBinary() = nil, want an error")
	}
	if want := fmt.Sprintf("cannot read %s from the release archive", binaryName); !strings.Contains(err.Error(), want) {
		t.Errorf("extractBinary() error = %v, want it to contain %q", err, want)
	}
}

// TestExtractBinaryRejectsAnAbsoluteEntryPath checks that an entry outside the
// archive root rejects the whole archive.
func TestExtractBinaryRejectsAnAbsoluteEntryPath(t *testing.T) {
	archive := tarGz(t, tarEntry{name: "/etc/passwd", body: []byte("x")})

	_, err := extractBinary(archive)
	if err == nil {
		t.Fatal("extractBinary() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "unsafe path") {
		t.Errorf("extractBinary() error = %v, want it to report the unsafe path", err)
	}
}

// newReleaseAssetsServer serves release metadata for version whose assets are
// produced from the server's own base URL.
func newReleaseAssetsServer(t *testing.T, version string, assets func(base string) []map[string]string) *httptest.Server {
	t.Helper()
	var srv *httptest.Server
	mux := http.NewServeMux()
	mux.HandleFunc("/repos/wangzw/peers/releases/tags/v"+version, func(w http.ResponseWriter, _ *http.Request) {
		payload := map[string]any{"tag_name": "v" + version, "assets": assets(srv.URL)}
		if err := json.NewEncoder(w).Encode(payload); err != nil {
			t.Errorf("encoding the fake release: %v", err)
		}
	})
	srv = httptest.NewTLSServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

// gzipBytes compresses data with gzip.
func gzipBytes(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	if _, err := gz.Write(data); err != nil {
		t.Fatalf("compressing the archive: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("closing the gzip writer: %v", err)
	}
	return buf.Bytes()
}
