package harness

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
)

// The fake mirrors the one fixed repository and asset names the updater
// requires, so a test can hand its URL to a Broker through PEERS_TEST_UPDATE_API.
const (
	// ChecksumsAsset is the release asset holding the sha256sum lines.
	ChecksumsAsset = "checksums.txt"

	latestReleasePath = "/repos/wangzw/peers/releases/latest"
	tagReleasePrefix  = "/repos/wangzw/peers/releases/tags/"
)

// ReleaseAssetName returns the archive name the updater requires for version on
// goos/goarch. The name is part of the update protocol and must match
// internal/broker/update.
func ReleaseAssetName(version, goos, goarch string) string {
	return fmt.Sprintf("peers_%s_%s_%s.tar.gz", strings.TrimPrefix(version, "v"), goos, goarch)
}

// ReleaseTarGz packages the binary at path as peers in a gzip-compressed
// tar archive, the shape the updater extracts.
func ReleaseTarGz(tb testing.TB, binary string) []byte {
	tb.Helper()
	data, err := os.ReadFile(binary)
	if err != nil {
		tb.Fatalf("reading %s to package it as a release: %v", binary, err)
	}

	var buf bytes.Buffer
	zw := gzip.NewWriter(&buf)
	tw := tar.NewWriter(zw)
	if err := tw.WriteHeader(&tar.Header{
		Name:     "peers",
		Mode:     0o755,
		Size:     int64(len(data)),
		Typeflag: tar.TypeReg,
	}); err != nil {
		tb.Fatalf("writing the tar header: %v", err)
	}
	if _, err := tw.Write(data); err != nil {
		tb.Fatalf("writing the archive member: %v", err)
	}
	if err := tw.Close(); err != nil {
		tb.Fatalf("closing the tar writer: %v", err)
	}
	if err := zw.Close(); err != nil {
		tb.Fatalf("closing the gzip writer: %v", err)
	}
	return buf.Bytes()
}

// ChecksumLine renders one sha256sum line for the asset.
func ChecksumLine(assetName string, data []byte) []byte {
	sum := sha256.Sum256(data)
	return []byte(hex.EncodeToString(sum[:]) + "  " + assetName + "\n")
}

// Release is one release the fake GitHub API serves.
type Release struct {
	// Version is the release version without the leading v.
	Version string
	// ArchiveName names the platform archive inside Assets.
	ArchiveName string
	// Archive is the bytes the archive download returns.
	Archive []byte
	// Checksums is the bytes the checksums asset returns.
	Checksums []byte
}

// NewRelease builds version for goos/goarch from the binary at path and
// computes the checksum file the updater verifies it against.
func NewRelease(tb testing.TB, version, goos, goarch, binary string) Release {
	tb.Helper()
	archive := ReleaseTarGz(tb, binary)
	name := ReleaseAssetName(version, goos, goarch)
	return Release{
		Version:     version,
		ArchiveName: name,
		Archive:     archive,
		Checksums:   ChecksumLine(name, archive),
	}
}

// FakeGitHub serves the two endpoints the updater uses: the latest release and
// its assets. It binds 127.0.0.1 through httptest, so no external service is
// involved.
type FakeGitHub struct {
	server *httptest.Server

	mu        sync.Mutex
	release   Release
	fail      bool
	requests  int
	downloads int
}

// StartFakeGitHub starts the fake API on 127.0.0.1 and stops it when the test
// ends.
func StartFakeGitHub(tb testing.TB, release Release) *FakeGitHub {
	tb.Helper()
	f := &FakeGitHub{release: release}
	f.server = httptest.NewServer(http.HandlerFunc(f.serve))
	tb.Cleanup(f.server.Close)
	return f
}

// URL is the API base the update code receives through
// PEERS_TEST_UPDATE_API.
func (f *FakeGitHub) URL() string { return f.server.URL }

// SetRelease replaces the release the next requests see.
func (f *FakeGitHub) SetRelease(release Release) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.release = release
}

// SetFailure makes every request fail, so a Broker's check cannot succeed.
func (f *FakeGitHub) SetFailure(fail bool) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.fail = fail
}

// ResetRequests forgets the counters.
func (f *FakeGitHub) ResetRequests() {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.requests, f.downloads = 0, 0
}

// Requests returns the number of requests any endpoint received.
func (f *FakeGitHub) Requests() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.requests
}

// Downloads returns the number of asset downloads served.
func (f *FakeGitHub) Downloads() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.downloads
}

func (f *FakeGitHub) serve(w http.ResponseWriter, r *http.Request) {
	f.mu.Lock()
	f.requests++
	fail := f.fail
	release := f.release
	f.mu.Unlock()

	if fail {
		http.Error(w, "injected update check failure", http.StatusInternalServerError)
		return
	}

	switch {
	case r.URL.Path == latestReleasePath ||
		r.URL.Path == tagReleasePrefix+"v"+release.Version:
		base := "http://" + r.Host
		body := struct {
			TagName    string `json:"tag_name"`
			HTMLURL    string `json:"html_url"`
			Draft      bool   `json:"draft"`
			Prerelease bool   `json:"prerelease"`
			Assets     []struct {
				Name               string `json:"name"`
				BrowserDownloadURL string `json:"browser_download_url"`
			} `json:"assets"`
		}{
			TagName: "v" + release.Version,
			HTMLURL: "https://github.com/wangzw/peers/releases/tag/v" + release.Version,
			Assets: []struct {
				Name               string `json:"name"`
				BrowserDownloadURL string `json:"browser_download_url"`
			}{
				{Name: release.ArchiveName, BrowserDownloadURL: base + "/download/" + release.ArchiveName},
				{Name: ChecksumsAsset, BrowserDownloadURL: base + "/download/" + ChecksumsAsset},
			},
		}

		w.Header().Set("Content-Type", "application/json")
		if err := json.NewEncoder(w).Encode(body); err != nil {
			return
		}
	case strings.HasPrefix(r.URL.Path, "/download/"):
		name := strings.TrimPrefix(r.URL.Path, "/download/")
		f.mu.Lock()
		f.downloads++
		f.mu.Unlock()
		switch name {
		case release.ArchiveName:
			w.Header().Set("Content-Type", "application/gzip")
			_, _ = w.Write(release.Archive)
		case ChecksumsAsset:
			_, _ = w.Write(release.Checksums)
		default:
			http.NotFound(w, r)
		}
	default:
		http.NotFound(w, r)
	}
}
