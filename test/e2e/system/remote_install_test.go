//go:build e2e

package e2e

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/netip"
	"os"
	"os/exec"
	"os/user"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
	"github.com/wangzw/peers/test/e2e/syscontainer"
)

const (
	// remoteInstallLatestVersion is the release the fake service resolves as
	// latest. The binary in its archive reports this version.
	remoteInstallLatestVersion = "0.2.0"

	// remoteInstallPinnedVersion is an older release the --version case pins. It
	// differs from remoteInstallLatestVersion, so the installed version proves
	// which archive the script downloaded.
	remoteInstallPinnedVersion = "0.1.1"

	// remoteInstallUnknownVersion is not served by the fake release service.
	// Asking for it must report that the version was not found.
	remoteInstallUnknownVersion = "0.9.9"

	// remoteInstallAccount is the ordinary account that runs the user
	// install, and the SUDO_USER of the system install.
	remoteInstallAccount = "peer"

	// remoteInstallReleasePath is the path prefix the fake service serves. The
	// remote script replaces https://github.com/wangzw/peers/releases with
	// PEERS_TEST_RELEASE_BASE, so the value ends at this prefix.
	remoteInstallReleasePath = "/releases"

	// remoteInstallScriptPath is where the inner test writes the fetched remote
	// script. The two accepted invocations run it from a file: "sh <file>" for
	// the user install and "sudo sh -s -- --system" for the system installation.
	remoteInstallScriptPath = "/tmp/remote-install.sh"

	// remoteInstallFakeClaudePath is the fake Claude Code CLI. It must be in
	// /usr/local/bin because install.sh looks it up through a login shell in the
	// system case.
	remoteInstallFakeClaudePath = "/usr/local/bin/claude"

	// remoteInstallFakeClaudeDir holds the fake CLI's call log and its record of
	// the MCP servers registered so far. It lives under /tmp so the ordinary
	// account can create it without root.
	remoteInstallFakeClaudeDir = "/tmp/peers-e2e-fakeclaude"

	// remoteInstallServiceBound is the fallback for the wait for the installed
	// service to report active. Install starts the service before it returns,
	// so the success path answers at once.
	remoteInstallServiceBound = harness.EventTimeout

	// remoteInstallReleasePortEnv carries the fake release service's host port
	// into the container. The host takes the port the kernel assigns and
	// forwards it, so the two sides never derive a port independently.
	remoteInstallReleasePortEnv = "PEERS_E2E_SYSTEM_RELEASE_PORT"
)

// TestRemoteInstallUser installs through the remote script as an ordinary
// account, using "sh <script>", and checks the outcome the in-package install
// script promises for a user install: the binary lands in ~/.local/bin,
// the user service runs, and the fake Claude Code CLI receives exactly
// one "mcp add --scope user peer -- <path> mcp". A second run must succeed and
// must not add the MCP server again.
func TestREQBrokerConfig042_RemoteInstallUser(t *testing.T) {
	if !syscontainer.InContainer() {
		remoteInstallRunInContainer(t, remoteInstallReleaseOptions{})
		return
	}

	base := remoteInstallBaseURL(t)
	remoteInstallWriteFakeClaude(t)
	peerUID := syscontainer.AccountUID(t, remoteInstallAccount)
	remoteInstallStartUserManager(t, peerUID)
	env := remoteInstallPeerEnv(peerUID, base)
	script := remoteInstallDownloadScript(t, base)

	for run := 1; run <= 2; run++ {
		res := remoteInstallRun(t, remoteInstallAccount, env, "", "sh", script)
		if res.ExitCode != 0 {
			t.Fatalf("remote install run %d: exit %d\nstdout:\n%s\nstderr:\n%s",
				run, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
	}

	home := remoteInstallHome(t)
	binary := filepath.Join(home, ".local", "bin", "peers")
	remoteInstallRequireVersion(t, remoteInstallAccount, binary, remoteInstallLatestVersion)

	remoteInstallRequireActive(t, remoteInstallAccount, env, "systemctl", "--user", "is-active", "peers.service")
	syscontainer.WaitFor(t, "the user Broker socket "+syscontainer.UserSocketPath(t, remoteInstallAccount), func() bool {
		_, err := os.Stat(syscontainer.UserSocketPath(t, remoteInstallAccount))
		return err == nil
	})

	remoteInstallRequireMCPAdd(t, binary)
}

// TestREQBrokerConfig042_RemoteInstallSystem installs through the remote script
// as an ordinary account, answers the level question with y, and checks the
// system installation outcome: the packaged script reruns itself through sudo inside
// the temporary directory, the binary is root-owned in /usr/local/bin and
// reports the release version, the system service runs, and the fake
// Claude Code CLI receives one registration for SUDO_USER with the system
// binary.
func TestREQBrokerConfig042_RemoteInstallSystem(t *testing.T) {
	if !syscontainer.InContainer() {
		remoteInstallRunInContainer(t, remoteInstallReleaseOptions{})
		return
	}

	base := remoteInstallBaseURL(t)
	remoteInstallWriteFakeClaude(t)
	remoteInstallGrantSudo(t)
	env := []string{
		"PEERS_TEST_RELEASE_BASE=" + base,
		"PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
	}
	script := remoteInstallDownloadScript(t, base)

	res := runInstallScriptWithTerminal(t, remoteInstallAccount, env, remoteInstallHome(t), "y\n", "sh", script)
	if res.ExitCode != 0 {
		t.Fatalf("system remote install: exit %d\n%s", res.ExitCode, res.Output)
	}
	if !strings.Contains(res.Output, installLevelPrompt) {
		t.Errorf("the terminal did not show %q:\n%s", installLevelPrompt, res.Output)
	}

	uid, gid, mode := fileOwnerAndMode(t, releaseBinary)
	if uid != 0 || gid != 0 || mode != 0o755 {
		t.Errorf("%s owner uid=%d gid=%d mode=%#o, want root:root 0755", releaseBinary, uid, gid, mode)
	}
	remoteInstallRequireVersion(t, "root", releaseBinary, remoteInstallLatestVersion)

	active := runWithEnv(t, "root", nil, "systemctl", "is-active", "peers.service")
	if state := strings.TrimSpace(active.Stdout); active.ExitCode != 0 || state != "active" {
		t.Fatalf("systemctl is-active peers.service = %q (exit %d), want %q\nstderr: %s",
			state, active.ExitCode, "active", strings.TrimSpace(active.Stderr))
	}
	syscontainer.WaitFor(t, "the system Broker socket "+systemSocket, func() bool {
		_, err := os.Stat(systemSocket)
		return err == nil
	})

	remoteInstallRequireMCPAdd(t, releaseBinary)
}

// TestRemoteInstallPinnedVersion runs the remote script with "--version" and
// checks that the archive for the pinned release was installed, not latest.
func TestREQBrokerConfig042_RemoteInstallPinnedVersion(t *testing.T) {
	if !syscontainer.InContainer() {
		remoteInstallRunInContainer(t, remoteInstallReleaseOptions{
			extraVersions: []string{remoteInstallPinnedVersion},
		})
		return
	}

	base := remoteInstallBaseURL(t)
	remoteInstallWriteFakeClaude(t)
	peerUID := syscontainer.AccountUID(t, remoteInstallAccount)
	remoteInstallStartUserManager(t, peerUID)
	env := remoteInstallPeerEnv(peerUID, base)
	script := remoteInstallDownloadScript(t, base)

	res := remoteInstallRun(t, remoteInstallAccount, env, "", "sh", script, "--version", remoteInstallPinnedVersion)
	if res.ExitCode != 0 {
		t.Fatalf("remote install with --version %s: exit %d\nstdout:\n%s\nstderr:\n%s",
			remoteInstallPinnedVersion, res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}

	binary := filepath.Join(remoteInstallHome(t), ".local", "bin", "peers")
	remoteInstallRequireVersion(t, remoteInstallAccount, binary, remoteInstallPinnedVersion)
}

// TestRemoteInstallUnknownVersion asks for a version the release service does
// not have. The download fails, and the remote script must report that the
// version was not found, leave the host untouched and clean its temporary
// directory.
func TestREQBrokerConfig042_RemoteInstallUnknownVersion(t *testing.T) {
	if !syscontainer.InContainer() {
		remoteInstallRunInContainer(t, remoteInstallReleaseOptions{})
		return
	}

	base := remoteInstallBaseURL(t)
	peerUID := syscontainer.AccountUID(t, remoteInstallAccount)
	tmp := remoteInstallPeerTempDir(t, peerUID)
	env := remoteInstallPeerEnv(peerUID, base)
	env = append(env, "TMPDIR="+tmp)
	script := remoteInstallDownloadScript(t, base)

	res := remoteInstallRun(t, remoteInstallAccount, env, "", "sh", script, "--version", remoteInstallUnknownVersion)
	if res.ExitCode == 0 {
		t.Fatalf("remote install --version %s exited 0, want a not-found refusal\nstdout:\n%s\nstderr:\n%s",
			remoteInstallUnknownVersion, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	remoteInstallRequireNotFound(t, res, remoteInstallUnknownVersion)
	remoteInstallRequireUnchanged(t, remoteInstallHome(t), peerUID, tmp)
}

// TestRemoteInstallArchiveWithoutInstallScript serves an archive that has the
// binary but no install.sh. The remote script must report the version as not
// found instead of running a half install, and leave the host untouched.
func TestREQBrokerConfig042_RemoteInstallArchiveWithoutInstallScript(t *testing.T) {
	if !syscontainer.InContainer() {
		remoteInstallRunInContainer(t, remoteInstallReleaseOptions{omitInstallScript: true})
		return
	}

	base := remoteInstallBaseURL(t)
	peerUID := syscontainer.AccountUID(t, remoteInstallAccount)
	tmp := remoteInstallPeerTempDir(t, peerUID)
	env := remoteInstallPeerEnv(peerUID, base)
	env = append(env, "TMPDIR="+tmp)
	script := remoteInstallDownloadScript(t, base)

	res := remoteInstallRun(t, remoteInstallAccount, env, "", "sh", script)
	if res.ExitCode == 0 {
		t.Fatalf("remote install of an archive without install.sh exited 0\nstdout:\n%s\nstderr:\n%s",
			strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	remoteInstallRequireNotFound(t, res, remoteInstallLatestVersion)
	remoteInstallRequireUnchanged(t, remoteInstallHome(t), peerUID, tmp)
}

// remoteInstallRequireNotFound requires the output to name version as not
// found.
func remoteInstallRequireNotFound(t *testing.T, res syscontainer.Result, version string) {
	t.Helper()
	want := "version " + version + " not found"
	if output := res.Stdout + res.Stderr; !strings.Contains(output, want) {
		t.Errorf("the refusal does not contain %q\nstdout:\n%s\nstderr:\n%s",
			want, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
}

// TestRemoteInstallTruncatedScript removes the final call that the remote
// script wraps its body in. The truncated file defines the function but never
// runs it, so the host is untouched and the fake release service sees no
// request at all.
func TestREQBrokerConfig042_RemoteInstallTruncatedScript(t *testing.T) {
	if !syscontainer.InContainer() {
		remoteInstallRunInContainer(t, remoteInstallReleaseOptions{})
		return
	}

	base := remoteInstallBaseURL(t)
	peerUID := syscontainer.AccountUID(t, remoteInstallAccount)
	tmp := remoteInstallPeerTempDir(t, peerUID)
	env := remoteInstallPeerEnv(peerUID, base)
	env = append(env, "TMPDIR="+tmp)
	script := remoteInstallTruncateScript(t, remoteInstallDownloadScript(t, base))
	remoteInstallResetRequests(t, base)

	res := remoteInstallRun(t, remoteInstallAccount, env, "", "sh", script)
	t.Logf("the truncated remote script exited %d", res.ExitCode)
	if requests := remoteInstallServerRequests(t, base); len(requests) != 0 {
		t.Errorf("the fake release service received %v from the truncated script, want no request", requests)
	}
	remoteInstallRequireUnchanged(t, remoteInstallHome(t), peerUID, tmp)
}

// TestRemoteInstallChecksumMismatch serves a checksums.txt that disagrees with
// the archive and checks that the remote script aborts without touching the
// host: no binary, service unit, MCP registration or configuration file, and no
// leftover temporary directory.
func TestREQBrokerConfig042_RemoteInstallChecksumMismatch(t *testing.T) {
	if !syscontainer.InContainer() {
		remoteInstallRunInContainer(t, remoteInstallReleaseOptions{badChecksum: true})
		return
	}

	base := remoteInstallBaseURL(t)
	peerUID := syscontainer.AccountUID(t, remoteInstallAccount)
	tmp := remoteInstallPeerTempDir(t, peerUID)
	env := remoteInstallPeerEnv(peerUID, base)
	env = append(env, "TMPDIR="+tmp)
	script := remoteInstallDownloadScript(t, base)

	res := remoteInstallRun(t, remoteInstallAccount, env, "", "sh", script)
	if res.ExitCode == 0 {
		t.Fatalf("remote install with a mismatched checksum exited 0\nstdout:\n%s\nstderr:\n%s",
			strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	for _, want := range []string{"checksum mismatch for", "but the download hashes to"} {
		if !strings.Contains(res.Stderr, want) {
			t.Fatalf("the failure did not report the checksum mismatch with %q\nstdout:\n%s\nstderr:\n%s",
				want, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
		}
	}
	t.Logf("the mismatched checksum was refused with exit %d: %s",
		res.ExitCode, strings.TrimSpace(res.Stderr))

	remoteInstallRequireUnchanged(t, remoteInstallHome(t), peerUID, tmp)
}

// remoteInstallReleaseOptions changes what the fake release service serves.
type remoteInstallReleaseOptions struct {
	// extraVersions are served in addition to the latest release.
	extraVersions []string
	// badChecksum makes the latest release's checksums.txt wrong on purpose.
	badChecksum bool
	// omitInstallScript leaves install.sh out of every served archive.
	omitInstallScript bool
}

// remoteInstallServe starts the fake release service on the test host and
// serves the repository's scripts as release assets. It runs on the host, and
// the container reaches it through the bridge gateway of its internal network.
//
// The service must run on the host: the container has no copy of the
// repository, so the host side is where the archive is built from
// scripts/remote-install.sh and scripts/install.sh. The container fetches both
// over HTTP, which is the transport the real install command uses.
//
// The kernel assigns the port from zero, so a second suite on the same host
// gets a different one and never collides with this service. The returned port
// is forwarded to the container, which cannot derive it on its own.
func remoteInstallServe(t *testing.T, opts remoteInstallReleaseOptions) int {
	t.Helper()

	root := remoteInstallModuleRoot(t)
	remoteScript := remoteInstallReadFile(t,
		filepath.Join(root, "scripts", "remote-install.sh"), "remote install script")
	inPackageScript := remoteInstallReadFile(t,
		filepath.Join(root, "scripts", "install.sh"), "in-package install script")
	readme := remoteInstallArchiveReadme(t, root)
	license := remoteInstallReadFile(t, filepath.Join(root, "LICENSE"), "license")
	if opts.omitInstallScript {
		inPackageScript = nil
	}

	versions := append([]string{remoteInstallLatestVersion}, opts.extraVersions...)
	var releases []*remoteInstallRelease
	for _, version := range versions {
		releases = append(releases, &remoteInstallRelease{
			version: version,
			tarball: remoteInstallTarGz(t, version,
				remoteInstallBuildPeers(t, root, version),
				inPackageScript, readme, license),
		})
	}

	set := &remoteInstallReleaseSet{
		newest:       remoteInstallLatestVersion,
		remoteScript: remoteScript,
		releases:     map[string]*remoteInstallRelease{},
	}
	for _, release := range releases {
		set.releases[release.version] = release
	}
	if opts.badChecksum {
		set.badChecksumVersion = remoteInstallLatestVersion
	}

	listener, err := net.Listen("tcp", "0.0.0.0:0")
	if err != nil {
		t.Fatalf("starting the fake release service: %v", err)
	}
	port := listener.Addr().(*net.TCPAddr).Port
	server := &http.Server{Handler: set}
	t.Cleanup(func() { _ = server.Close() })
	go func() { _ = server.Serve(listener) }()
	t.Logf("fake release service listening on the host at port %d", port)
	return port
}

// remoteInstallRunInContainer starts the fake release service on the host and
// runs the test in a systemd container with the service's port forwarded. It is
// the host-side body of every remote install test.
func remoteInstallRunInContainer(t *testing.T, opts remoteInstallReleaseOptions) {
	t.Helper()
	port := remoteInstallServe(t, opts)
	syscontainer.Run(t, syscontainer.Systemd, remoteInstallReleasePortEnv+"="+strconv.Itoa(port))
}

// remoteInstallRelease is one served release.
type remoteInstallRelease struct {
	version string
	tarball []byte
}

// remoteInstallReleaseSet is the fake release service's state.
type remoteInstallReleaseSet struct {
	newest             string
	remoteScript       []byte
	releases           map[string]*remoteInstallRelease
	badChecksumVersion string

	mu       sync.Mutex
	requests []string
}

// ServeHTTP answers the paths the remote script requests. The two control
// paths let the test read and clear the request log; they are not part of the
// release tree and are not recorded.
func (s *remoteInstallReleaseSet) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/__requests":
		s.mu.Lock()
		body := strings.Join(s.requests, "\n")
		s.mu.Unlock()
		if body != "" {
			body += "\n"
		}
		_, _ = w.Write([]byte(body))
		return
	case "/__reset":
		s.mu.Lock()
		s.requests = nil
		s.mu.Unlock()
		w.WriteHeader(http.StatusNoContent)
		return
	}

	s.mu.Lock()
	s.requests = append(s.requests, r.URL.Path)
	s.mu.Unlock()

	path := strings.TrimPrefix(r.URL.Path, remoteInstallReleasePath)
	switch {
	case path == "/latest":
		http.Redirect(w, r, remoteInstallReleasePath+"/tag/v"+s.newest, http.StatusFound)
		return
	case path == "/latest/download/install.sh":
		http.Redirect(w, r, remoteInstallReleasePath+"/download/v"+s.newest+"/install.sh", http.StatusFound)
		return
	case strings.HasPrefix(path, "/tag/v"):
		version := strings.TrimPrefix(path, "/tag/v")
		if _, ok := s.releases[version]; !ok {
			http.NotFound(w, r)
			return
		}
		_, _ = fmt.Fprintf(w, "fake release page for v%s\n", version)
		return
	case strings.HasPrefix(path, "/download/v"):
		rest := strings.TrimPrefix(path, "/download/v")
		version, file, ok := strings.Cut(rest, "/")
		if !ok {
			http.NotFound(w, r)
			return
		}
		release, ok := s.releases[version]
		if !ok {
			http.NotFound(w, r)
			return
		}
		switch file {
		case "install.sh":
			_, _ = w.Write(s.remoteScript)
		case "checksums.txt":
			_, _ = w.Write(s.checksums(release))
		case remoteInstallPackageName(version):
			_, _ = w.Write(release.tarball)
		default:
			http.NotFound(w, r)
		}
		return
	default:
		http.NotFound(w, r)
	}
}

// checksums renders checksums.txt for release. The latest archive's line is
// deliberately wrong when the test asks for a mismatch.
func (s *remoteInstallReleaseSet) checksums(release *remoteInstallRelease) []byte {
	sum := sha256.Sum256(release.tarball)
	hash := hex.EncodeToString(sum[:])
	if release.version == s.badChecksumVersion {
		hash = strings.Repeat("0", sha256.Size*2)
	}
	return []byte(fmt.Sprintf("%s  %s\n", hash, remoteInstallPackageName(release.version)))
}

// remoteInstallPackageName is the archive name the remote script asks for.
func remoteInstallPackageName(version string) string {
	return fmt.Sprintf("peers_%s_linux_%s.tar.gz", version, runtime.GOARCH)
}

// remoteInstallArchiveReadme returns the README the real archive carries. The
// install-script unit adds scripts/archive-README.md; before it lands, the
// repository README is used so only the missing scripts stop the test.
func remoteInstallArchiveReadme(t *testing.T, root string) []byte {
	t.Helper()
	path := filepath.Join(root, "scripts", "archive-README.md")
	if data, err := os.ReadFile(path); err == nil {
		return data
	}
	return remoteInstallReadFile(t, filepath.Join(root, "README.md"), "archive README")
}

// remoteInstallReadFile returns path's content or fails with the unit that
// adds it.
func remoteInstallReadFile(t *testing.T, path, what string) []byte {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading the %s at %s: %v\n"+
			"The v0.1.1 install-script unit adds this file; this e2e runs after that unit merges.",
			what, path, err)
	}
	return data
}

// remoteInstallModuleRoot returns the module root through go env GOMOD.
func remoteInstallModuleRoot(t *testing.T) string {
	t.Helper()
	out, err := exec.Command("go", "env", "GOMOD").Output()
	if err != nil {
		t.Fatalf("go env GOMOD: %v", err)
	}
	gomod := strings.TrimSpace(string(out))
	if gomod == "" || gomod == os.DevNull {
		t.Fatalf("not inside a Go module")
	}
	return filepath.Dir(gomod)
}

// remoteInstallBuildPeers builds ./cmd/peers for linux with the
// release version in it.
func remoteInstallBuildPeers(t *testing.T, root, version string) []byte {
	t.Helper()
	out := filepath.Join(t.TempDir(), "peers")
	cmd := exec.Command("go", "build", "-trimpath",
		"-ldflags", "-X main.version="+version, "-o", out, "./cmd/peers")
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+runtime.GOARCH,
	)
	if combined, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("building peers %s: %v\n%s", version, err, strings.TrimSpace(string(combined)))
	}
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("reading the built peers %s: %v", version, err)
	}
	return data
}

// remoteInstallArchiveFile is one entry of a served archive.
type remoteInstallArchiveFile struct {
	name string
	mode int64
	data []byte
}

// remoteInstallTarGz builds the release archive with the same root layout the
// goreleaser archive uses. A nil installScript leaves install.sh out, for the
// archive the remote script must reject.
func remoteInstallTarGz(t *testing.T, version string, binary, installScript, readme, license []byte) []byte {
	t.Helper()
	files := []remoteInstallArchiveFile{
		{"peers", 0o755, binary},
	}
	if installScript != nil {
		files = append(files, remoteInstallArchiveFile{"install.sh", 0o755, installScript})
	}
	files = append(files,
		remoteInstallArchiveFile{"README.md", 0o644, readme},
		remoteInstallArchiveFile{"LICENSE", 0o644, license},
	)

	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	for _, file := range files {
		header := &tar.Header{
			Name:     file.name,
			Mode:     file.mode,
			Size:     int64(len(file.data)),
			ModTime:  time.Unix(0, 0),
			Typeflag: tar.TypeReg,
		}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("writing the %s header for %s: %v", file.name, version, err)
		}
		if _, err := tw.Write(file.data); err != nil {
			t.Fatalf("writing the %s entry for %s: %v", file.name, version, err)
		}
	}
	if err := tw.Close(); err != nil {
		t.Fatalf("closing the %s tar stream: %v", version, err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("closing the %s gzip stream: %v", version, err)
	}
	return buf.Bytes()
}

// remoteInstallPort returns the host port remoteInstallServe bound. The host
// forwards it through remoteInstallReleasePortEnv, so the container learns the
// kernel-assigned port instead of deriving one that two suites could share.
func remoteInstallPort(t *testing.T) int {
	t.Helper()
	raw := os.Getenv(remoteInstallReleasePortEnv)
	if raw == "" {
		t.Fatalf("%s is empty; the host must forward the fake release service's port", remoteInstallReleasePortEnv)
	}
	port, err := strconv.Atoi(raw)
	if err != nil {
		t.Fatalf("%s=%q is not a port: %v", remoteInstallReleasePortEnv, raw, err)
	}
	if port < 1 || port > 65535 {
		t.Fatalf("%s=%q is outside the port range 1-65535", remoteInstallReleasePortEnv, raw)
	}
	return port
}

// remoteInstallBaseURL is the value of PEERS_TEST_RELEASE_BASE inside the
// container: the bridge gateway and the port passed to this test.
func remoteInstallBaseURL(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("http://%s:%d%s", remoteInstallGateway(t), remoteInstallPort(t), remoteInstallReleasePath)
}

// remoteInstallGateway returns the container's bridge gateway. Run forwards
// the allocated subnet, and the gateway is its first address. When it is
// absent, the address on eth0 gives the same subnet.
func remoteInstallGateway(t *testing.T) string {
	t.Helper()
	if subnet := os.Getenv("PEERS_E2E_SYSTEM_SUBNET"); subnet != "" {
		if prefix, err := netip.ParsePrefix(subnet); err == nil {
			return prefix.Masked().Addr().Next().String()
		}
	}
	res := runWithEnv(t, "root", nil, "ip", "-4", "-o", "addr", "show", "dev", "eth0")
	if res.ExitCode != 0 {
		t.Fatalf("ip -4 -o addr show dev eth0: exit %d\n%s", res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	fields := strings.Fields(res.Stdout)
	for i, field := range fields {
		if field != "inet" || i+1 >= len(fields) {
			continue
		}
		prefix, err := netip.ParsePrefix(fields[i+1])
		if err != nil {
			continue
		}
		return prefix.Masked().Addr().Next().String()
	}
	t.Fatalf("no IPv4 address on eth0:\n%s", res.Stdout)
	return ""
}

// remoteInstallHome returns the ordinary account's home.
func remoteInstallHome(t *testing.T) string {
	t.Helper()
	u, err := user.Lookup(remoteInstallAccount)
	if err != nil {
		t.Fatalf("looking up account %q: %v", remoteInstallAccount, err)
	}
	return u.HomeDir
}

// remoteInstallPeerEnv is the environment of the user install. The
// runtime directory and the bus are what let the in-package install script's
// "service install" reach the account's user manager.
func remoteInstallPeerEnv(uid uint32, base string) []string {
	user := strconv.FormatUint(uint64(uid), 10)
	return []string{
		"PEERS_TEST_RELEASE_BASE=" + base,
		"XDG_RUNTIME_DIR=/run/user/" + user,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + user + "/bus",
		"PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
	}
}

// remoteInstallStartUserManager enables lingering and starts the account's user
// manager, then waits for its runtime directory and bus.
func remoteInstallStartUserManager(t *testing.T, uid uint32) {
	t.Helper()
	mustRun(t, "root", nil, "loginctl", "enable-linger", remoteInstallAccount)
	mustRun(t, "root", nil, "systemctl", "start", "user@"+strconv.FormatUint(uint64(uid), 10)+".service")
	waitUserManager(t, uid)
}

// remoteInstallWriteFakeClaude installs the fake Claude Code CLI that records
// every call and remembers which MCP servers were added.
func remoteInstallWriteFakeClaude(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(remoteInstallFakeClaudePath, []byte(remoteInstallFakeClaudeScript), 0o755); err != nil {
		t.Fatalf("writing %s: %v", remoteInstallFakeClaudePath, err)
	}
}

// remoteInstallGrantSudo gives the ordinary account passwordless sudo in this
// container and keeps the test hook in its environment, so the system
// case can run the same "sudo sh -s -- --system" a user would.
func remoteInstallGrantSudo(t *testing.T) {
	t.Helper()
	path := "/etc/sudoers.d/peers-e2e"
	content := strings.Join([]string{
		`Defaults env_keep += "PEERS_TEST_RELEASE_BASE"`,
		"Defaults !use_pty",
		remoteInstallAccount + " ALL=(ALL:ALL) NOPASSWD: ALL",
		"",
	}, "\n")
	if err := os.WriteFile(path, []byte(content), 0o440); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
	mustRun(t, "root", nil, "visudo", "-c")
	mustRun(t, remoteInstallAccount, nil, "sudo", "-n", "true")
}

// remoteInstallDownloadScript fetches the remote script from the fake release
// service the way the documented install command does, and returns its path.
func remoteInstallDownloadScript(t *testing.T, base string) string {
	t.Helper()
	res := runWithEnv(t, remoteInstallAccount, nil, "curl", "-fsSL",
		base+"/latest/download/install.sh", "-o", remoteInstallScriptPath)
	if res.ExitCode != 0 {
		t.Fatalf("fetching the remote install script: exit %d\nstdout:\n%s\nstderr:\n%s",
			res.ExitCode, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
	return remoteInstallScriptPath
}

// remoteInstallRun runs a command as account, optionally with stdin from a
// file, and returns its output and exit code.
func remoteInstallRun(t *testing.T, account string, env []string, stdinPath, name string, args ...string) syscontainer.Result {
	t.Helper()
	cmd := syscontainer.Command(account, env, name, args...)
	// A new session has no controlling terminal, so an install without a level
	// option exercises the no-terminal choice instead of blocking on the
	// terminal that runs the tests.
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.Setsid = true
	if stdinPath != "" {
		f, err := os.Open(stdinPath)
		if err != nil {
			t.Fatalf("opening %s for stdin: %v", stdinPath, err)
		}
		defer func() { _ = f.Close() }()
		cmd.Stdin = f
	}
	var stdout, stderr strings.Builder
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	err := cmd.Run()

	res := syscontainer.Result{Stdout: stdout.String(), Stderr: stderr.String()}
	if err == nil {
		return res
	}
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		t.Fatalf("running %s as %s: %v", name, account, err)
	}
	res.ExitCode = exitErr.ExitCode()
	return res
}

// remoteInstallPeerTempDir creates a temporary directory owned by the ordinary
// account, so the remote script's mktemp -d and its cleanup are observable.
func remoteInstallPeerTempDir(t *testing.T, uid uint32) string {
	t.Helper()
	dir, err := os.MkdirTemp("/tmp", "remote-install-tmp-")
	if err != nil {
		t.Fatalf("creating a temporary directory: %v", err)
	}
	if err := os.Chown(dir, int(uid), int(uid)); err != nil {
		t.Fatalf("chown %s: %v", dir, err)
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	return dir
}

// remoteInstallRequireUnchanged checks that a refused or inert install left no
// binary, service unit, MCP registration or configuration file, started no
// Broker, and cleaned its temporary directory.
func remoteInstallRequireUnchanged(t *testing.T, home string, peerUID uint32, tmp string) {
	t.Helper()
	for _, path := range []string{
		filepath.Join(home, ".local", "bin", "peers"),
		userUnitPath(t),
		systemUnitPath,
		filepath.Join(home, ".peers", "config.yaml"),
		remoteInstallFakeClaudeDir,
	} {
		if _, err := os.Stat(path); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s exists after the install did not complete (stat error: %v)", path, err)
		}
	}
	if procs := peerBrokers(peerUID); len(procs) != 0 {
		t.Errorf("peer Broker processes %v are running after the install did not complete", procs)
	}
	entries, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatalf("reading the temporary directory %s: %v", tmp, err)
	}
	if len(entries) != 0 {
		var names []string
		for _, entry := range entries {
			names = append(names, entry.Name())
		}
		t.Errorf("temporary directory %s still holds %v", tmp, names)
	}
}

// remoteInstallTruncateScript removes the last line of the fetched remote
// script and returns the truncated file. The script wraps its body in a
// function and calls it on the last line, so the result parses but does
// nothing.
func remoteInstallTruncateScript(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	lines := strings.Split(strings.TrimSuffix(string(data), "\n"), "\n")
	if len(lines) < 2 {
		t.Fatalf("%s has %d lines, want at least 2", path, len(lines))
	}
	truncated := strings.Join(lines[:len(lines)-1], "\n") + "\n"
	out := filepath.Join(filepath.Dir(path), "remote-install-truncated.sh")
	if err := os.WriteFile(out, []byte(truncated), 0o644); err != nil {
		t.Fatalf("writing %s: %v", out, err)
	}
	return out
}

// remoteInstallServerRootURL is the fake service's origin without the release
// path, where the control endpoints live.
func remoteInstallServerRootURL(base string) string {
	return strings.TrimSuffix(base, remoteInstallReleasePath)
}

// remoteInstallResetRequests clears the fake service's request log.
func remoteInstallResetRequests(t *testing.T, base string) {
	t.Helper()
	res := runWithEnv(t, "root", nil, "curl", "-fsSL", remoteInstallServerRootURL(base)+"/__reset")
	if res.ExitCode != 0 {
		t.Fatalf("clearing the fake release service's request log: exit %d\n%s",
			res.ExitCode, strings.TrimSpace(res.Stderr))
	}
}

// remoteInstallServerRequests returns the paths the fake service received since
// the last reset.
func remoteInstallServerRequests(t *testing.T, base string) []string {
	t.Helper()
	res := runWithEnv(t, "root", nil, "curl", "-fsSL", remoteInstallServerRootURL(base)+"/__requests")
	if res.ExitCode != 0 {
		t.Fatalf("reading the fake release service's request log: exit %d\n%s",
			res.ExitCode, strings.TrimSpace(res.Stderr))
	}
	var requests []string
	for _, line := range strings.Split(res.Stdout, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			requests = append(requests, line)
		}
	}
	return requests
}

// remoteInstallRequireVersion runs binary --version as account and requires the
// exact release version.
func remoteInstallRequireVersion(t *testing.T, account, binary, version string) {
	t.Helper()
	res := runWithEnv(t, account, nil, binary, "--version")
	want := "peers " + version
	if got := strings.TrimSpace(res.Stdout); res.ExitCode != 0 || got != want {
		t.Fatalf("%s --version as %s = %q (exit %d), want %q\nstderr: %s",
			binary, account, got, res.ExitCode, want, strings.TrimSpace(res.Stderr))
	}
}

// remoteInstallRequireActive waits for a systemctl is-active check to report
// active. Install starts the service before it returns; the wait only covers
// the short window before systemd reports the state.
func remoteInstallRequireActive(t *testing.T, account string, env []string, name string, args ...string) {
	t.Helper()
	harness.Eventually(t, remoteInstallServiceBound, "the installed service to be active", func() (bool, string) {
		res := runWithEnv(t, account, env, name, args...)
		state := strings.TrimSpace(res.Stdout)
		return res.ExitCode == 0 && state == "active",
			fmt.Sprintf("%s %s = %q (exit %d): %s",
				name, strings.Join(args, " "), state, res.ExitCode, strings.TrimSpace(res.Stderr))
	})
}

// remoteInstallRequireMCPAdd requires exactly one successful MCP registration,
// run as the ordinary account against binary, and at least one lookup before
// it.
func remoteInstallRequireMCPAdd(t *testing.T, binary string) {
	t.Helper()
	calls := remoteInstallMCPCalls(t)
	adds, gets := 0, 0
	for _, call := range calls {
		if len(call) < 3 || call[1] != "mcp" {
			continue
		}
		switch call[2] {
		case "add":
			adds++
			if call[0] != remoteInstallAccount {
				t.Errorf("mcp add ran as %q, want %q: %v", call[0], remoteInstallAccount, call)
			}
			want := "--scope user " + remoteInstallAccount + " -- " + binary + " mcp"
			if joined := strings.Join(call, " "); !strings.Contains(joined, want) {
				t.Errorf("mcp add is %q, want it to contain %q", joined, want)
			}
		case "get":
			gets++
		}
	}
	if adds != 1 {
		t.Fatalf("mcp add calls = %d, want exactly 1: %v", adds, calls)
	}
	if gets == 0 {
		t.Errorf("the install script never looked up the existing registration: %v", calls)
	}
}

// remoteInstallMCPCalls returns every fake Claude Code CLI call, one slice of
// fields per call.
func remoteInstallMCPCalls(t *testing.T) [][]string {
	t.Helper()
	entries, err := os.ReadDir(remoteInstallFakeClaudeDir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		t.Fatalf("reading %s: %v", remoteInstallFakeClaudeDir, err)
	}
	var calls [][]string
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".log") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(remoteInstallFakeClaudeDir, entry.Name()))
		if err != nil {
			t.Fatalf("reading %s: %v", entry.Name(), err)
		}
		for _, line := range strings.Split(string(data), "\n") {
			if line = strings.TrimSpace(line); line != "" {
				calls = append(calls, strings.Fields(line))
			}
		}
	}
	return calls
}

// remoteInstallFakeClaudeScript records each call as "<user> <args...>" in a
// per-user log, returns exit 0 from "mcp get" once the server is registered,
// and records a registration from "mcp add".
const remoteInstallFakeClaudeScript = `#!/bin/sh
set -u
dir=/tmp/peers-e2e-fakeclaude
mkdir -p "$dir" || exit 2
who=$(id -un 2>/dev/null || echo unknown)
{
	printf '%s' "$who"
	for arg in "$@"; do printf ' %s' "$arg"; done
	printf '\n'
} >> "$dir/$who.log" 2>/dev/null || true

if [ "${1:-}" != "mcp" ]; then
	printf 'fake claude: unsupported command: %s\n' "$*" >&2
	exit 2
fi
case "${2:-}" in
get)
	name="${3:-}"
	if [ -n "$name" ] && [ -f "$dir/$who-registered-$name" ]; then
		printf '%s: registered\n' "$name"
		exit 0
	fi
	printf 'No MCP server found with name: %s\n' "$name" >&2
	exit 1
	;;
add)
	shift 2
	name=
	while [ "$#" -gt 0 ]; do
		case "$1" in
		--scope) shift 2 ;;
		--) shift; break ;;
		-*) shift ;;
		*) name="$1"; shift ;;
		esac
	done
	if [ -z "$name" ]; then
		printf 'fake claude: mcp add without a name\n' >&2
		exit 2
	fi
	: > "$dir/$who-registered-$name"
	printf '%s: added\n' "$name"
	exit 0
	;;
esac
printf 'fake claude: unsupported mcp subcommand: %s\n' "${2:-}" >&2
exit 2
`
