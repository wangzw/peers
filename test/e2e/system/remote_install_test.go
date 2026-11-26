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
	remoteInstallLatestVersion = "0.2.0"

	// remoteInstallPinnedVersion differs from remoteInstallLatestVersion so the
	// installed version proves which archive was downloaded.
	remoteInstallPinnedVersion = "0.1.1"

	remoteInstallUnknownVersion = "0.9.9"

	remoteInstallAccount = "peer"

	// The remote script replaces https://github.com/wangzw/peers/releases with
	// PEERS_TEST_RELEASE_BASE, so the value ends at this prefix and the fake
	// server serves every path under it.
	remoteInstallReleasePath = "/releases"

	remoteInstallScriptPath = "/tmp/remote-install.sh"

	// install.sh looks the CLI up through a login shell in the system case, so it
	// must be in /usr/local/bin.
	remoteInstallFakeClaudePath = "/usr/local/bin/claude"

	// remoteInstallFakeClaudeDir lives under /tmp so the ordinary account can
	// create it without root.
	remoteInstallFakeClaudeDir = "/tmp/peers-e2e-fakeclaude"

	// remoteInstallServiceBound is the fallback for the wait for the installed
	// service to report active; install starts the service before it returns, so
	// the success path answers at once.
	remoteInstallServiceBound = harness.EventTimeout

	// The host forwards the port the kernel assigns, so the two sides never derive
	// it independently.
	remoteInstallReleasePortEnv = "PEERS_E2E_SYSTEM_RELEASE_PORT"
)

// TestREQBrokerConfig042_RemoteInstallUser checks that a user install through the
// remote script places the binary in ~/.local/bin, runs the user service, and
// registers the MCP server once, staying idempotent on a second run.
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

// TestREQBrokerConfig042_RemoteInstallSystem checks that a system install through
// the remote script reruns itself through sudo, leaves a root-owned binary in
// /usr/local/bin, runs the system service, and registers the MCP server once for
// SUDO_USER.
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

// TestREQBrokerConfig042_RemoteInstallPinnedVersion checks that --version installs
// the pinned release instead of latest.
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

// TestREQBrokerConfig042_RemoteInstallUnknownVersion checks that asking for a
// version the release service does not have fails as not found, leaves the host
// untouched, and cleans its temporary directory.
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

// TestREQBrokerConfig042_RemoteInstallArchiveWithoutInstallScript checks that an
// archive without install.sh is refused as not found instead of running a half
// install, leaving the host untouched.
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

func remoteInstallRequireNotFound(t *testing.T, res syscontainer.Result, version string) {
	t.Helper()
	want := "version " + version + " not found"
	if output := res.Stdout + res.Stderr; !strings.Contains(output, want) {
		t.Errorf("the refusal does not contain %q\nstdout:\n%s\nstderr:\n%s",
			want, strings.TrimSpace(res.Stdout), strings.TrimSpace(res.Stderr))
	}
}

// TestREQBrokerConfig042_RemoteInstallTruncatedScript checks that a remote script
// truncated before its final call does nothing and sends no request to the release
// service.
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

// TestREQBrokerConfig042_RemoteInstallChecksumMismatch checks that a checksums.txt
// that disagrees with the archive makes the remote script abort without touching
// the host.
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

type remoteInstallReleaseOptions struct {
	extraVersions     []string
	badChecksum       bool
	omitInstallScript bool
}

// remoteInstallServe starts the fake release service on the host and returns its
// kernel-assigned port, which the container reaches through the bridge gateway.
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
// reruns the test in a systemd container with the port forwarded.
func remoteInstallRunInContainer(t *testing.T, opts remoteInstallReleaseOptions) {
	t.Helper()
	port := remoteInstallServe(t, opts)
	syscontainer.Run(t, syscontainer.Systemd, remoteInstallReleasePortEnv+"="+strconv.Itoa(port))
}

type remoteInstallRelease struct {
	version string
	tarball []byte
}

type remoteInstallReleaseSet struct {
	newest             string
	remoteScript       []byte
	releases           map[string]*remoteInstallRelease
	badChecksumVersion string

	mu       sync.Mutex
	requests []string
}

// ServeHTTP answers the paths the remote script requests; /__requests and /__reset
// serve the test's request log and are not recorded.
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

func (s *remoteInstallReleaseSet) checksums(release *remoteInstallRelease) []byte {
	sum := sha256.Sum256(release.tarball)
	hash := hex.EncodeToString(sum[:])
	if release.version == s.badChecksumVersion {
		hash = strings.Repeat("0", sha256.Size*2)
	}
	return []byte(fmt.Sprintf("%s  %s\n", hash, remoteInstallPackageName(release.version)))
}

func remoteInstallPackageName(version string) string {
	return fmt.Sprintf("peers_%s_linux_%s.tar.gz", version, runtime.GOARCH)
}

func remoteInstallArchiveReadme(t *testing.T, root string) []byte {
	t.Helper()
	path := filepath.Join(root, "scripts", "archive-README.md")
	if data, err := os.ReadFile(path); err == nil {
		return data
	}
	return remoteInstallReadFile(t, filepath.Join(root, "README.md"), "archive README")
}

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

type remoteInstallArchiveFile struct {
	name string
	mode int64
	data []byte
}

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

// remoteInstallPort reads the kernel-assigned port the host forwards through
// remoteInstallReleasePortEnv.
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

func remoteInstallBaseURL(t *testing.T) string {
	t.Helper()
	return fmt.Sprintf("http://%s:%d%s", remoteInstallGateway(t), remoteInstallPort(t), remoteInstallReleasePath)
}

// remoteInstallGateway is the first address of the subnet Run allocated; when that
// subnet is absent, the prefix on eth0 gives the same subnet.
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

func remoteInstallHome(t *testing.T) string {
	t.Helper()
	u, err := user.Lookup(remoteInstallAccount)
	if err != nil {
		t.Fatalf("looking up account %q: %v", remoteInstallAccount, err)
	}
	return u.HomeDir
}

func remoteInstallPeerEnv(uid uint32, base string) []string {
	user := strconv.FormatUint(uint64(uid), 10)
	return []string{
		"PEERS_TEST_RELEASE_BASE=" + base,
		"XDG_RUNTIME_DIR=/run/user/" + user,
		"DBUS_SESSION_BUS_ADDRESS=unix:path=/run/user/" + user + "/bus",
		"PATH=/usr/local/bin:/usr/bin:/bin:/usr/sbin:/sbin",
	}
}

func remoteInstallStartUserManager(t *testing.T, uid uint32) {
	t.Helper()
	mustRun(t, "root", nil, "loginctl", "enable-linger", remoteInstallAccount)
	mustRun(t, "root", nil, "systemctl", "start", "user@"+strconv.FormatUint(uint64(uid), 10)+".service")
	waitUserManager(t, uid)
}

func remoteInstallWriteFakeClaude(t *testing.T) {
	t.Helper()
	if err := os.WriteFile(remoteInstallFakeClaudePath, []byte(remoteInstallFakeClaudeScript), 0o755); err != nil {
		t.Fatalf("writing %s: %v", remoteInstallFakeClaudePath, err)
	}
}

// remoteInstallGrantSudo keeps PEERS_TEST_RELEASE_BASE across sudo so the system
// case reaches the fake release service.
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

func remoteInstallRun(t *testing.T, account string, env []string, stdinPath, name string, args ...string) syscontainer.Result {
	t.Helper()
	cmd := syscontainer.Command(account, env, name, args...)
	// A new session has no controlling terminal, so an install without a level
	// option exercises the no-terminal choice.
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

// remoteInstallTruncateScript drops the last line, where the script calls the
// function wrapping its body, so the result parses but does nothing.
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

func remoteInstallServerRootURL(base string) string {
	return strings.TrimSuffix(base, remoteInstallReleasePath)
}

func remoteInstallResetRequests(t *testing.T, base string) {
	t.Helper()
	res := runWithEnv(t, "root", nil, "curl", "-fsSL", remoteInstallServerRootURL(base)+"/__reset")
	if res.ExitCode != 0 {
		t.Fatalf("clearing the fake release service's request log: exit %d\n%s",
			res.ExitCode, strings.TrimSpace(res.Stderr))
	}
}

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

func remoteInstallRequireVersion(t *testing.T, account, binary, version string) {
	t.Helper()
	res := runWithEnv(t, account, nil, binary, "--version")
	want := "peers " + version
	if got := strings.TrimSpace(res.Stdout); res.ExitCode != 0 || got != want {
		t.Fatalf("%s --version as %s = %q (exit %d), want %q\nstderr: %s",
			binary, account, got, res.ExitCode, want, strings.TrimSpace(res.Stderr))
	}
}

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
// per-user log and answers mcp get from the registration mcp add wrote.
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
