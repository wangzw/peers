package update

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"strings"
	"time"
)

const (
	// maxDownloadSize caps one downloaded file at 100 MiB. The archive and the
	// checksum file share the cap.
	maxDownloadSize int64 = 100 * 1024 * 1024

	// binaryName is the only archive entry that is ever extracted.
	binaryName = "peers"

	// precheckTimeout bounds the staged binary's --version run.
	precheckTimeout = 30 * time.Second
)

// ApplyOptions describes one release installation.
type ApplyOptions struct {
	Target       string // the binary to replace, with symlinks already resolved
	Version      string // the version to install, without the v prefix
	API          string
	Client       *http.Client
	GOOS, GOARCH string
}

// Apply downloads the release archive for the target platform, verifies it
// against checksums.txt, checks that the new binary reports the expected
// version, and atomically replaces Target. The target file is left untouched
// on every failure.
func Apply(ctx context.Context, o ApplyOptions) error {
	version := strings.TrimPrefix(o.Version, "v")
	if !isReleaseVersion(version) {
		return fmt.Errorf("version %q is not a formal release", o.Version)
	}
	if o.Target == "" {
		return errors.New("no target binary given")
	}
	if o.GOOS == "" || o.GOARCH == "" {
		return errors.New("GOOS and GOARCH are required")
	}

	info, err := os.Lstat(o.Target)
	if err != nil {
		return fmt.Errorf("cannot inspect %s: %w", o.Target, err)
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("target %s is a symbolic link", o.Target)
	}
	if !info.Mode().IsRegular() {
		return fmt.Errorf("target %s is not a regular file", o.Target)
	}

	archiveName := fmt.Sprintf("%s_%s_%s_%s.tar.gz", binaryName, version, o.GOOS, o.GOARCH)
	tag := "v" + version
	var release struct {
		TagName    string         `json:"tag_name"`
		Draft      bool           `json:"draft"`
		Prerelease bool           `json:"prerelease"`
		Assets     []releaseAsset `json:"assets"`
	}
	if err := getJSON(ctx, o.Client, releaseURL(o.API, "tags/"+tag), &release); err != nil {
		return err
	}
	if release.Draft {
		return fmt.Errorf("release %s is a draft", tag)
	}
	if release.Prerelease {
		return fmt.Errorf("release %s is a prerelease", tag)
	}
	if release.TagName != tag {
		return fmt.Errorf("release endpoint returned tag %q, want %q", release.TagName, tag)
	}
	archiveURL, checksumsURL := assetURLs(release.Assets, archiveName)
	if archiveURL == "" {
		return fmt.Errorf("release %s has no asset %q", tag, archiveName)
	}
	if checksumsURL == "" {
		return fmt.Errorf("release %s has no asset %q", tag, checksumsAsset)
	}
	if err := checkDownloadURL(archiveURL); err != nil {
		return err
	}
	if err := checkDownloadURL(checksumsURL); err != nil {
		return err
	}

	client := downloadClient(o.Client)
	archive, err := download(ctx, client, archiveURL)
	if err != nil {
		return err
	}
	checksums, err := download(ctx, client, checksumsURL)
	if err != nil {
		return err
	}
	want, err := expectedChecksum(checksums, archiveName)
	if err != nil {
		return err
	}
	got := sha256.Sum256(archive)
	if subtle.ConstantTimeCompare(got[:], want) != 1 {
		return fmt.Errorf("checksum mismatch for %s", archiveName)
	}

	binary, err := extractBinary(archive)
	if err != nil {
		return err
	}

	// Stage the binary next to the target. The file that runs for the version
	// check is the same file that is renamed into place, so a noexec system
	// temporary directory cannot break the preflight.
	tmp, err := os.CreateTemp(filepath.Dir(o.Target), ".peers-update-*")
	if err != nil {
		return fmt.Errorf("cannot create a temporary file next to %s: %w", o.Target, err)
	}
	tmpName := tmp.Name()
	installed := false
	defer func() {
		_ = tmp.Close()
		if !installed {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(binary); err != nil {
		return fmt.Errorf("cannot write the downloaded binary: %w", err)
	}
	if err := tmp.Chmod(0o700); err != nil {
		return fmt.Errorf("making the downloaded binary executable: %w", err)
	}
	// The writer has to be closed before the file can be executed: an open
	// write descriptor makes execve fail with ETXTBSY.
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("cannot close %s: %w", tmpName, err)
	}
	if err := checkVersion(ctx, tmpName, version); err != nil {
		return err
	}

	staged, err := os.Open(tmpName)
	if err != nil {
		return fmt.Errorf("reopening %s: %w", tmpName, err)
	}
	defer func() { _ = staged.Close() }()
	if err := chownFile(staged, info); err != nil {
		return fmt.Errorf("setting the owner of %s: %w", tmpName, err)
	}
	if err := staged.Chmod(info.Mode().Perm()); err != nil {
		return fmt.Errorf("setting the mode of %s: %w", tmpName, err)
	}
	if err := staged.Sync(); err != nil {
		return fmt.Errorf("syncing %s: %w", tmpName, err)
	}
	if err := staged.Close(); err != nil {
		return fmt.Errorf("cannot close %s: %w", tmpName, err)
	}
	if err := os.Rename(tmpName, o.Target); err != nil {
		return fmt.Errorf("replacing %s: %w", o.Target, err)
	}
	installed = true
	if err := syncDir(filepath.Dir(o.Target)); err != nil {
		return err
	}
	return nil
}

// releaseAsset is one downloadable file of a release.
type releaseAsset struct {
	Name string `json:"name"`
	URL  string `json:"browser_download_url"`
}

// assetURLs returns the download URLs of the archive and the checksum file.
func assetURLs(assets []releaseAsset, archiveName string) (archiveURL, checksumsURL string) {
	for _, asset := range assets {
		switch asset.Name {
		case archiveName:
			archiveURL = asset.URL
		case checksumsAsset:
			checksumsURL = asset.URL
		}
	}
	return archiveURL, checksumsURL
}

// errTooLarge reports that a bounded read found more data than its limit.
var errTooLarge = errors.New("more data than allowed")

// readAtMost reads r up to max bytes and fails with errTooLarge when r holds
// more. It reads one byte past max so a body exactly at the limit still
// succeeds.
func readAtMost(r io.Reader, max int64) ([]byte, error) {
	data, err := io.ReadAll(io.LimitReader(r, max+1))
	if err != nil {
		return nil, err
	}
	if int64(len(data)) > max {
		return nil, errTooLarge
	}
	return data, nil
}

// download fetches one asset, refusing anything above maxDownloadSize.
func download(ctx context.Context, c *http.Client, url string) ([]byte, error) {
	resp, err := get(ctx, c, url)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	data, err := readAtMost(resp.Body, maxDownloadSize)
	if errors.Is(err, errTooLarge) {
		return nil, fmt.Errorf("download %s exceeds the %d-byte limit", url, maxDownloadSize)
	}
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", url, err)
	}
	return data, nil
}

// expectedChecksum returns the SHA-256 recorded for name in checksums.txt.
// More than one line for the same name is rejected, so a file cannot offer two
// different answers for one archive.
func expectedChecksum(checksums []byte, name string) ([]byte, error) {
	var found []byte
	for _, line := range strings.Split(string(checksums), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		// sha256sum marks binary mode with a leading asterisk.
		if strings.TrimPrefix(fields[1], "*") != name {
			continue
		}
		if found != nil {
			return nil, fmt.Errorf("%s lists %s more than once", checksumsAsset, name)
		}
		sum, err := hex.DecodeString(fields[0])
		if err != nil || len(sum) != sha256.Size {
			return nil, fmt.Errorf("%s has an invalid checksum for %s", checksumsAsset, name)
		}
		found = sum
	}
	if found == nil {
		return nil, fmt.Errorf("%s has no checksum for %s", checksumsAsset, name)
	}
	return found, nil
}

// extractBinary returns the regular file named peers from the archive.
// Path traversal, symbolic links and hard links reject the whole archive.
func extractBinary(archive []byte) ([]byte, error) {
	gz, err := gzip.NewReader(bytes.NewReader(archive))
	if err != nil {
		return nil, fmt.Errorf("cannot open the release archive: %w", err)
	}
	defer func() { _ = gz.Close() }()

	tr := tar.NewReader(gz)
	var binary []byte
	for {
		hdr, err := tr.Next()
		if err == io.EOF {
			break
		}
		if err != nil {
			return nil, fmt.Errorf("cannot read the release archive: %w", err)
		}
		if !safeArchivePath(hdr.Name) {
			return nil, fmt.Errorf("release archive contains the unsafe path %q", hdr.Name)
		}
		switch hdr.Typeflag {
		case tar.TypeSymlink, tar.TypeLink:
			return nil, fmt.Errorf("release archive contains the link %q", hdr.Name)
		case tar.TypeReg:
			if path.Clean(hdr.Name) != binaryName {
				continue
			}
			if binary != nil {
				return nil, fmt.Errorf("release archive contains more than one %s", binaryName)
			}
			data, err := readAtMost(tr, maxDownloadSize)
			if errors.Is(err, errTooLarge) {
				return nil, fmt.Errorf("%s exceeds the %d-byte limit", binaryName, maxDownloadSize)
			}
			if err != nil {
				return nil, fmt.Errorf("cannot read %s from the release archive: %w", binaryName, err)
			}
			binary = data
		}
	}
	if binary == nil {
		return nil, fmt.Errorf("release archive contains no %s", binaryName)
	}
	return binary, nil
}

// safeArchivePath reports whether a tar entry stays inside the archive root.
func safeArchivePath(name string) bool {
	if path.IsAbs(name) {
		return false
	}
	cleaned := path.Clean(name)
	return cleaned != ".." && !strings.HasPrefix(cleaned, "../")
}

// checkVersion runs the staged binary and requires it to report version
// exactly, so an archive that passed the checksum but carries another build is
// still rejected. The child gets a minimal environment and a bounded run time.
func checkVersion(ctx context.Context, binary, version string) error {
	ctx, cancel := context.WithTimeout(ctx, precheckTimeout)
	defer cancel()

	var stdout, stderr bytes.Buffer
	cmd := exec.CommandContext(ctx, binary, "--version")
	cmd.Env = precheckEnv()
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		return fmt.Errorf("cannot run the downloaded binary: %w (stderr: %s)", err, strings.TrimSpace(stderr.String()))
	}
	got := strings.TrimSuffix(stdout.String(), "\n")
	got = strings.TrimSuffix(got, "\r")
	if want := "peers " + version; got != want {
		return fmt.Errorf("downloaded binary reported %q, want %q", got, want)
	}
	return nil
}

// precheckEnv is the minimal environment the version check passes to the
// downloaded binary: PATH to resolve anything it runs, and HOME when set.
func precheckEnv() []string {
	env := []string{"PATH=" + os.Getenv("PATH")}
	if home, ok := os.LookupEnv("HOME"); ok {
		env = append(env, "HOME="+home)
	}
	return env
}

// syncDir flushes the directory entry created by the rename so the replacement
// survives a crash.
func syncDir(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return fmt.Errorf("cannot open %s to sync it: %w", dir, err)
	}
	defer func() { _ = f.Close() }()
	if err := f.Sync(); err != nil {
		return fmt.Errorf("syncing %s: %w", dir, err)
	}
	return nil
}
