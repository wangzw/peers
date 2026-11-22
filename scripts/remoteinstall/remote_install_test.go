package remoteinstall

import (
	"archive/tar"
	"bytes"
	"compress/gzip"
	"crypto/sha256"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"
)

// scriptPath is the script under test, relative to this package.
const scriptPath = "../remote-install.sh"

// releaseBase is a URL that is never contacted: the fake curl and wget map
// every request below it onto releaseDir.
const releaseBase = "https://release.invalid/releases"

// systemPath is a fixed search path for the real tools the script needs. It is
// joined after the fake bin directory, so the fakes win.
var systemPath = strings.Join([]string{"/usr/bin", "/bin", "/usr/sbin", "/sbin"}, string(os.PathListSeparator))

// fixture is one release tree plus the fake command directories and the
// environment the script runs with.
type fixture struct {
	t    *testing.T
	base string
	home string

	tmpdir     string
	fakes      string
	releaseDir string

	script string

	curlLog     string
	wgetLog     string
	installArgs string
}

// newFixture builds a release tree with two versions and the fake commands.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	base := t.TempDir()
	f := &fixture{
		t:    t,
		base: base,
		home: filepath.Join(base, "home"),

		tmpdir:     filepath.Join(base, "tmp"),
		fakes:      filepath.Join(base, "fakebin"),
		releaseDir: filepath.Join(base, "release"),

		curlLog:     filepath.Join(base, "curl.log"),
		wgetLog:     filepath.Join(base, "wget.log"),
		installArgs: filepath.Join(base, "install-args"),
	}
	for _, dir := range []string{f.home, f.tmpdir, f.fakes, f.releaseDir} {
		mustMkdirAll(t, dir)
	}

	script, err := filepath.Abs(scriptPath)
	if err != nil {
		t.Fatalf("resolving %s: %v", scriptPath, err)
	}
	f.script = script

	realSha256sum := mustLookPath(t, "sha256sum")
	sha256sum := strings.ReplaceAll(fakeSha256sum, "@REAL_SHA256SUM@", strconv.Quote(realSha256sum))

	writeExecutable(t, filepath.Join(f.fakes, "uname"), fakeUname)
	writeExecutable(t, filepath.Join(f.fakes, "wget"), fakeWget)
	writeExecutable(t, filepath.Join(f.fakes, "sha256sum"), sha256sum)
	writeExecutable(t, filepath.Join(f.fakes, "curl"), fakeCurl)

	f.writeRelease("0.1.1")
	f.writeRelease("1.2.3")
	return f
}

// writeRelease publishes one version: four archives, one checksums.txt that
// names all of them, laid out the way the release download URLs expect.
func (f *fixture) writeRelease(version string) {
	f.t.Helper()
	dir := filepath.Join(f.releaseDir, "download", "v"+version)
	mustMkdirAll(f.t, dir)

	var checksums strings.Builder
	for _, osName := range []string{"linux", "darwin"} {
		for _, arch := range []string{"amd64", "arm64"} {
			name := fmt.Sprintf("peers_%s_%s_%s.tar.gz", version, osName, arch)
			data := archiveData(f.t, true)
			mustWriteFile(f.t, filepath.Join(dir, name), data, 0o644)
			sum := sha256.Sum256(data)
			if _, err := fmt.Fprintf(&checksums, "%x  %s\n", sum, name); err != nil {
				f.t.Fatalf("building checksums.txt: %v", err)
			}
		}
	}
	mustWriteFile(f.t, filepath.Join(dir, "checksums.txt"), []byte(checksums.String()), 0o644)
}

// environ returns the environment for a run. overrides replace the defaults.
func (f *fixture) environ(overrides map[string]string) []string {
	env := map[string]string{
		"HOME":                    f.home,
		"PATH":                    f.fakes + string(os.PathListSeparator) + systemPath,
		"TMPDIR":                  f.tmpdir,
		"LC_ALL":                  "C",
		"FAKE_UNAME_S":            "Linux",
		"FAKE_UNAME_M":            "x86_64",
		"FAKE_CURL_LOG":           f.curlLog,
		"FAKE_WGET_LOG":           f.wgetLog,
		"FAKE_RELEASE_BASE":       releaseBase,
		"FAKE_RELEASE_DIR":        f.releaseDir,
		"FAKE_EFFECTIVE_URL":      releaseBase + "/tag/v0.1.1",
		"FAKE_INSTALL_ARGS":       f.installArgs,
		"PEERS_TEST_RELEASE_BASE": releaseBase,
	}
	for key, value := range overrides {
		env[key] = value
	}

	keys := make([]string, 0, len(env))
	for key := range env {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	environ := make([]string, 0, len(keys))
	for _, key := range keys {
		environ = append(environ, key+"="+env[key])
	}
	return environ
}

// run executes remote-install.sh with the fixture environment.
func (f *fixture) run(overrides map[string]string, args ...string) (stdout, stderr string, code int) {
	f.t.Helper()
	return f.runScript(f.script, overrides, args...)
}

// runScript executes an arbitrary copy of the script with the fixture
// environment.
func (f *fixture) runScript(script string, overrides map[string]string, args ...string) (stdout, stderr string, code int) {
	f.t.Helper()
	cmd := exec.Command("sh", append([]string{script}, args...)...)
	cmd.Dir = f.base
	cmd.Env = f.environ(overrides)
	var out, errOut strings.Builder
	cmd.Stdout = &out
	cmd.Stderr = &errOut
	err := cmd.Run()
	if err != nil {
		var exitErr *exec.ExitError
		if !errors.As(err, &exitErr) {
			f.t.Fatalf("running %s: %v", script, err)
		}
		code = exitErr.ExitCode()
	}
	return out.String(), errOut.String(), code
}

// toolDir builds a directory that replaces PATH entirely. The named real
// commands are linked in, and each fake is written under its name. A name
// listed in both is taken from fakes.
func (f *fixture) toolDir(links []string, fakes map[string]string) string {
	f.t.Helper()
	dir := f.t.TempDir()
	for _, name := range links {
		real := mustLookPath(f.t, name)
		mustSymlink(f.t, real, filepath.Join(dir, name))
	}
	for name, content := range fakes {
		writeExecutable(f.t, filepath.Join(dir, name), content)
	}
	return dir
}

func (f *fixture) readLog(path string) string {
	f.t.Helper()
	data, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return ""
	}
	if err != nil {
		f.t.Fatalf("reading %s: %v", path, err)
	}
	return string(data)
}

// logLines returns the non-empty lines of a command log. Each fake downloader
// appends one line per invocation, so the line index identifies the call.
func logLines(text string) []string {
	text = strings.TrimSuffix(text, "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

func (f *fixture) installArgsLines() []string {
	f.t.Helper()
	data, err := os.ReadFile(f.installArgs)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		f.t.Fatalf("reading %s: %v", f.installArgs, err)
	}
	text := strings.TrimSuffix(string(data), "\n")
	if text == "" {
		return nil
	}
	return strings.Split(text, "\n")
}

// tempEntries returns what the run left in its TMPDIR.
func (f *fixture) tempEntries() []string {
	f.t.Helper()
	entries, err := os.ReadDir(f.tmpdir)
	if err != nil {
		f.t.Fatalf("reading %s: %v", f.tmpdir, err)
	}
	names := make([]string, 0, len(entries))
	for _, entry := range entries {
		names = append(names, entry.Name())
	}
	return names
}

func TestPlatformMapping(t *testing.T) {
	tests := []struct {
		name    string
		osName  string
		machine string
		archive string
	}{
		{"linux amd64", "Linux", "x86_64", "peers_0.1.1_linux_amd64.tar.gz"},
		{"linux amd64 spelled out", "Linux", "amd64", "peers_0.1.1_linux_amd64.tar.gz"},
		{"linux arm64", "Linux", "aarch64", "peers_0.1.1_linux_arm64.tar.gz"},
		{"linux arm64 spelled out", "Linux", "arm64", "peers_0.1.1_linux_arm64.tar.gz"},
		{"darwin amd64", "Darwin", "x86_64", "peers_0.1.1_darwin_amd64.tar.gz"},
		{"darwin arm64", "Darwin", "arm64", "peers_0.1.1_darwin_arm64.tar.gz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			_, stderr, code := f.run(map[string]string{
				"FAKE_UNAME_S": tt.osName,
				"FAKE_UNAME_M": tt.machine,
			}, "--version", "0.1.1")
			if code != 0 {
				t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
			}
			log := f.readLog(f.curlLog)
			want := releaseBase + "/download/v0.1.1/" + tt.archive
			if !strings.Contains(log, want) {
				t.Fatalf("curl was not asked for %s; log:\n%s", want, log)
			}
			if args := f.installArgsLines(); len(args) != 0 {
				t.Fatalf("install.sh received %q, want no arguments", args)
			}
		})
	}
}

func TestUnsupportedPlatform(t *testing.T) {
	tests := []struct {
		name    string
		osName  string
		machine string
		wantErr string
	}{
		{"operating system", "FreeBSD", "x86_64", "unsupported operating system"},
		{"architecture", "Linux", "riscv64", "unsupported architecture"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			_, stderr, code := f.run(map[string]string{
				"FAKE_UNAME_S": tt.osName,
				"FAKE_UNAME_M": tt.machine,
			}, "--version", "0.1.1")
			if code == 0 {
				t.Fatalf("exit = 0, want a failure; stderr:\n%s", stderr)
			}
			if !strings.Contains(stderr, tt.wantErr) {
				t.Fatalf("stderr does not mention %q:\n%s", tt.wantErr, stderr)
			}
			if log := f.readLog(f.curlLog); log != "" {
				t.Fatalf("curl ran for an unsupported platform; log:\n%s", log)
			}
		})
	}
}

func TestUsesWgetWhenCurlIsMissing(t *testing.T) {
	f := newFixture(t)
	dir := f.toolDir(
		[]string{"sh", "mktemp", "rm", "tar", "gzip", "sed", "awk", "tail", "tr", "cp"},
		map[string]string{
			"uname":     fakeUname,
			"wget":      fakeWget,
			"sha256sum": strings.ReplaceAll(fakeSha256sum, "@REAL_SHA256SUM@", strconv.Quote(mustLookPath(t, "sha256sum"))),
		},
	)
	_, stderr, code := f.run(map[string]string{"PATH": dir})
	if code != 0 {
		t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
	}
	calls := logLines(f.readLog(f.wgetLog))
	if len(calls) != 3 {
		t.Fatalf("wget ran %d times, want 3 (latest lookup, archive, checksums); log:\n%s",
			len(calls), f.readLog(f.wgetLog))
	}
	latest := calls[0]
	for _, want := range []string{
		"--spider",
		"--https-only",
		"--secure-protocol=TLSv1_2",
		"--server-response",
		releaseBase + "/latest",
	} {
		if !strings.Contains(latest, want) {
			t.Fatalf("the latest lookup is missing %q: %s", want, latest)
		}
	}
	downloads := []struct {
		name string
		url  string
	}{
		{"archive", releaseBase + "/download/v0.1.1/peers_0.1.1_linux_amd64.tar.gz"},
		{"checksums", releaseBase + "/download/v0.1.1/checksums.txt"},
	}
	for i, call := range calls[1:] {
		for _, want := range []string{
			"--https-only",
			"--secure-protocol=TLSv1_2",
			"--server-response",
			downloads[i].url,
		} {
			if !strings.Contains(call, want) {
				t.Fatalf("the %s download is missing %q: %s", downloads[i].name, want, call)
			}
		}
		if strings.Contains(call, "--spider") {
			t.Fatalf("the %s download used --spider: %s", downloads[i].name, call)
		}
	}
	if curl := f.readLog(f.curlLog); curl != "" {
		t.Fatalf("curl ran although it was not on PATH; log:\n%s", curl)
	}
}

// TestWgetRefusesHTTPRedirect checks that a wget download whose redirect ends
// at plain HTTP aborts the install. wget's --https-only does not stop a single
// download from following such a redirect, so the script inspects the Location
// headers of the same request and fails closed.
func TestWgetRefusesHTTPRedirect(t *testing.T) {
	f := newFixture(t)
	dir := f.toolDir(
		[]string{"sh", "mktemp", "rm", "tar", "gzip", "sed", "awk", "tail", "tr", "cp"},
		map[string]string{
			"uname":     fakeUname,
			"wget":      fakeWget,
			"sha256sum": strings.ReplaceAll(fakeSha256sum, "@REAL_SHA256SUM@", strconv.Quote(mustLookPath(t, "sha256sum"))),
		},
	)
	_, stderr, code := f.run(map[string]string{
		"PATH":                        dir,
		"FAKE_WGET_DOWNLOAD_LOCATION": "http://insecure.invalid/peers_0.1.1_linux_amd64.tar.gz",
	})
	if code == 0 {
		t.Fatalf("exit = 0, want a failure")
	}
	if !strings.Contains(stderr, "non-HTTPS") {
		t.Fatalf("stderr does not name the non-HTTPS redirect:\n%s", stderr)
	}
	f.assertNoInstallation(t)
}

func TestNoDownloader(t *testing.T) {
	f := newFixture(t)
	dir := f.toolDir([]string{"sh"}, map[string]string{"uname": fakeUname})
	_, stderr, code := f.run(map[string]string{"PATH": dir}, "--version", "0.1.1")
	if code == 0 {
		t.Fatalf("exit = 0, want a failure")
	}
	if !strings.Contains(stderr, "neither curl nor wget") {
		t.Fatalf("stderr does not name the missing downloaders:\n%s", stderr)
	}
}

func TestLatestVersionFromRedirect(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(nil)
	if code != 0 {
		t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
	}
	log := f.readLog(f.curlLog)
	if !strings.Contains(log, releaseBase+"/latest") {
		t.Fatalf("curl was not asked for %s/latest; log:\n%s", releaseBase, log)
	}
	if !strings.Contains(log, releaseBase+"/download/v0.1.1/peers_0.1.1_linux_amd64.tar.gz") {
		t.Fatalf("curl was not asked for the 0.1.1 archive; log:\n%s", log)
	}
}

// TestLatestLookupUsesStrictCurlOptions checks the first curl call, the one
// that resolves the latest release without --version. That call must carry the
// same HTTPS restrictions as the downloads; the other tests all pass --version
// and never reach it.
func TestLatestLookupUsesStrictCurlOptions(t *testing.T) {
	f := newFixture(t)
	_, stderr, code := f.run(nil)
	if code != 0 {
		t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
	}
	calls := logLines(f.readLog(f.curlLog))
	if len(calls) == 0 {
		t.Fatalf("curl did not run")
	}
	latest := calls[0]
	for _, want := range []string{"--proto =https", "--tlsv1.2", releaseBase + "/latest"} {
		if !strings.Contains(latest, want) {
			t.Fatalf("the latest lookup is missing %q: %s", want, latest)
		}
	}
}

func TestInvalidLatestRedirect(t *testing.T) {
	tests := []struct {
		name        string
		effective   string
		wantErr     string
		wantNoFetch bool
	}{
		{"no version component", releaseBase + "/tag/nightly", "cannot read a release version", true},
		{"short version", releaseBase + "/tag/v1.2", "invalid version", true},
		{"four components", releaseBase + "/tag/v1.2.3.4", "invalid version", true},
		{"empty redirect", "", "cannot read a release version", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			_, stderr, code := f.run(map[string]string{"FAKE_EFFECTIVE_URL": tt.effective})
			if code == 0 {
				t.Fatalf("exit = 0, want a failure")
			}
			if !strings.Contains(stderr, tt.wantErr) {
				t.Fatalf("stderr does not mention %q:\n%s", tt.wantErr, stderr)
			}
			if tt.wantNoFetch {
				if log := f.readLog(f.curlLog); strings.Contains(log, "/download/") {
					t.Fatalf("an archive was fetched after the version failed to resolve; log:\n%s", log)
				}
			}
		})
	}
}

func TestExplicitVersionSkipsLatest(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{"separate value", []string{"--version", "1.2.3"}},
		{"joined value", []string{"--version=1.2.3"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			_, stderr, code := f.run(nil, tt.args...)
			if code != 0 {
				t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
			}
			log := f.readLog(f.curlLog)
			if strings.Contains(log, "/latest") {
				t.Fatalf("a fixed version still asked for the latest release; log:\n%s", log)
			}
			want := releaseBase + "/download/v1.2.3/peers_1.2.3_linux_amd64.tar.gz"
			if !strings.Contains(log, want) {
				t.Fatalf("curl was not asked for %s; log:\n%s", want, log)
			}
		})
	}
}

func TestInvalidArguments(t *testing.T) {
	tests := []struct {
		name    string
		args    []string
		wantErr string
	}{
		{"missing value", []string{"--version"}, "needs a published version number"},
		{"empty value", []string{"--version", ""}, "invalid version"},
		{"empty joined value", []string{"--version="}, "invalid version"},
		{"short version", []string{"--version", "1.2"}, "invalid version"},
		{"not a version", []string{"--version", "latest"}, "invalid version"},
		{"path traversal", []string{"--version", "../0.1.1"}, "invalid version"},
		{"version after another argument", []string{"--system", "--version", "0.1.1"}, "must come before"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			_, stderr, code := f.run(nil, tt.args...)
			if code == 0 {
				t.Fatalf("exit = 0, want a failure; stderr:\n%s", stderr)
			}
			if !strings.Contains(stderr, tt.wantErr) {
				t.Fatalf("stderr does not mention %q:\n%s", tt.wantErr, stderr)
			}
			if log := f.readLog(f.curlLog); log != "" {
				t.Fatalf("curl ran for invalid arguments; log:\n%s", log)
			}
		})
	}
}

// assertNoInstallation asserts the run left no installation artifacts and no
// temporary files behind.
func (f *fixture) assertNoInstallation(t *testing.T) {
	t.Helper()
	if _, err := os.Stat(f.installArgs); err == nil {
		t.Fatalf("install.sh ran")
	} else if !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("checking whether install.sh ran: %v", err)
	}
	if entries := f.tempEntries(); len(entries) != 0 {
		t.Fatalf("temporary files were left behind: %q", entries)
	}
}

func TestMissingRelease(t *testing.T) {
	t.Run("explicit version is not published", func(t *testing.T) {
		f := newFixture(t)
		_, stderr, code := f.run(nil, "--version", "9.9.9")
		if code == 0 {
			t.Fatalf("exit = 0, want a failure")
		}
		if !strings.Contains(stderr, "version 9.9.9 not found") {
			t.Fatalf("stderr does not report the missing version:\n%s", stderr)
		}
		f.assertNoInstallation(t)
	})

	t.Run("latest resolves to an unpublished version", func(t *testing.T) {
		f := newFixture(t)
		_, stderr, code := f.run(map[string]string{
			"FAKE_EFFECTIVE_URL": releaseBase + "/tag/v9.9.9",
		})
		if code == 0 {
			t.Fatalf("exit = 0, want a failure")
		}
		if !strings.Contains(stderr, "version 9.9.9 not found") {
			t.Fatalf("stderr does not report the missing version:\n%s", stderr)
		}
		f.assertNoInstallation(t)
	})

	t.Run("archive has no install.sh", func(t *testing.T) {
		f := newFixture(t)
		dir := filepath.Join(f.releaseDir, "download", "v0.1.1")
		name := "peers_0.1.1_linux_amd64.tar.gz"
		data := archiveData(t, false)
		mustWriteFile(t, filepath.Join(dir, name), data, 0o644)
		sum := sha256.Sum256(data)
		checksums := fmt.Sprintf("%x  %s\n", sum, name)
		mustWriteFile(t, filepath.Join(dir, "checksums.txt"), []byte(checksums), 0o644)

		_, stderr, code := f.run(nil, "--version", "0.1.1")
		if code == 0 {
			t.Fatalf("exit = 0, want a failure")
		}
		if !strings.Contains(stderr, "version 0.1.1 not found") {
			t.Fatalf("stderr does not report the uninstallable archive:\n%s", stderr)
		}
		f.assertNoInstallation(t)
	})
}

func TestTruncatedScriptDoesNothing(t *testing.T) {
	f := newFixture(t)
	data, err := os.ReadFile(f.script)
	if err != nil {
		t.Fatalf("reading %s: %v", scriptPath, err)
	}
	const call = "main \"$@\"\n"
	if !strings.HasSuffix(string(data), call) {
		t.Fatalf("%s does not end with %q", scriptPath, call)
	}

	truncated := filepath.Join(t.TempDir(), "remote-install-truncated.sh")
	mustWriteFile(t, truncated, []byte(strings.TrimSuffix(string(data), call)), 0o755)

	stdout, stderr, code := f.runScript(truncated, nil, "--version", "0.1.1", "--system")
	if code != 0 {
		t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
	}
	if stdout != "" || stderr != "" {
		t.Fatalf("a truncated script produced output:\nstdout: %q\nstderr: %q", stdout, stderr)
	}
	if log := f.readLog(f.curlLog); log != "" {
		t.Fatalf("a truncated script downloaded something; curl log:\n%s", log)
	}
	if log := f.readLog(f.wgetLog); log != "" {
		t.Fatalf("a truncated script downloaded something; wget log:\n%s", log)
	}
	if _, err := os.Stat(f.installArgs); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("a truncated script ran install.sh: %v", err)
	}
	if entries := f.tempEntries(); len(entries) != 0 {
		t.Fatalf("a truncated script created temporary files: %q", entries)
	}
}

func TestChecksumMismatch(t *testing.T) {
	f := newFixture(t)
	checksumFile := filepath.Join(f.releaseDir, "download", "v0.1.1", "checksums.txt")
	bad := strings.Repeat("0", 64) + "  peers_0.1.1_linux_amd64.tar.gz\n"
	mustWriteFile(t, checksumFile, []byte(bad), 0o644)

	_, stderr, code := f.run(nil, "--version", "0.1.1")
	if code == 0 {
		t.Fatalf("exit = 0, want a failure")
	}
	for _, want := range []string{"checksum mismatch for", "checksums.txt lists", "but the download hashes to"} {
		if !strings.Contains(stderr, want) {
			t.Fatalf("stderr does not report the mismatch with %q:\n%s", want, stderr)
		}
	}
	if args := f.installArgsLines(); args != nil {
		t.Fatalf("install.sh ran after a checksum mismatch with %q", args)
	}
	if entries := f.tempEntries(); len(entries) != 0 {
		t.Fatalf("temporary files were left behind: %q", entries)
	}
}

func TestDuplicateChecksumLines(t *testing.T) {
	f := newFixture(t)
	checksumFile := filepath.Join(f.releaseDir, "download", "v0.1.1", "checksums.txt")
	original, err := os.ReadFile(checksumFile)
	if err != nil {
		t.Fatalf("reading %s: %v", checksumFile, err)
	}
	var duplicate strings.Builder
	duplicate.Write(original)
	duplicate.WriteString(strings.Repeat("1", 64) + "  peers_0.1.1_linux_amd64.tar.gz\n")
	mustWriteFile(t, checksumFile, []byte(duplicate.String()), 0o644)

	_, stderr, code := f.run(nil, "--version", "0.1.1")
	if code == 0 {
		t.Fatalf("exit = 0, want a failure")
	}
	if !strings.Contains(stderr, "exactly one line") {
		t.Fatalf("stderr does not report the ambiguous checksum file:\n%s", stderr)
	}
	if args := f.installArgsLines(); args != nil {
		t.Fatalf("install.sh ran for an ambiguous checksum file with %q", args)
	}
}

func TestMissingChecksumLine(t *testing.T) {
	f := newFixture(t)
	checksumFile := filepath.Join(f.releaseDir, "download", "v0.1.1", "checksums.txt")
	mustWriteFile(t, checksumFile, []byte(strings.Repeat("1", 64)+"  peers_0.1.1_linux_arm64.tar.gz\n"), 0o644)

	_, stderr, code := f.run(nil, "--version", "0.1.1")
	if code == 0 {
		t.Fatalf("exit = 0, want a failure")
	}
	if !strings.Contains(stderr, "exactly one line") {
		t.Fatalf("stderr does not report the missing checksum line:\n%s", stderr)
	}
	if args := f.installArgsLines(); args != nil {
		t.Fatalf("install.sh ran without a checksum line with %q", args)
	}
}

func TestPassesArgumentsToPackagedInstaller(t *testing.T) {
	f := newFixture(t)
	want := []string{"--system", "--no-mcp", "--prefix", "/opt/peers", "--no-service"}
	_, stderr, code := f.run(nil, append([]string{"--version", "0.1.1"}, want...)...)
	if code != 0 {
		t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
	}
	got := f.installArgsLines()
	if strings.Join(got, "\n") != strings.Join(want, "\n") {
		t.Fatalf("install.sh arguments = %q, want %q", got, want)
	}
}

func TestReturnsPackagedInstallerExitCode(t *testing.T) {
	f := newFixture(t)
	_, _, code := f.run(map[string]string{"FAKE_INSTALL_EXIT": "7"}, "--version", "0.1.1")
	if code != 7 {
		t.Fatalf("exit = %d, want 7", code)
	}
	if entries := f.tempEntries(); len(entries) != 0 {
		t.Fatalf("temporary files were left behind: %q", entries)
	}
}

func TestUsesShasumWhenSha256sumIsMissing(t *testing.T) {
	f := newFixture(t)
	realSha256sum := mustLookPath(t, "sha256sum")
	dir := f.toolDir(
		[]string{"sh", "mktemp", "rm", "tar", "gzip", "sed", "awk", "tail", "tr", "cp"},
		map[string]string{
			"uname":  fakeUname,
			"curl":   fakeCurl,
			"shasum": strings.ReplaceAll(fakeShasum, "@REAL_SHA256SUM@", strconv.Quote(realSha256sum)),
		},
	)
	_, stderr, code := f.run(map[string]string{"PATH": dir}, "--version", "0.1.1")
	if code != 0 {
		t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
	}
	if _, err := os.Stat(f.installArgs); err != nil {
		t.Fatalf("install.sh did not run: %v", err)
	}
}

func TestTransportRestrictions(t *testing.T) {
	t.Run("https base uses the strict curl options", func(t *testing.T) {
		f := newFixture(t)
		if _, stderr, code := f.run(nil, "--version", "0.1.1"); code != 0 {
			t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
		}
		log := f.readLog(f.curlLog)
		if !strings.Contains(log, "--proto =https") || !strings.Contains(log, "--tlsv1.2") {
			t.Fatalf("curl did not receive the strict HTTPS options; log:\n%s", log)
		}
	})

	t.Run("http test base allows plain HTTP", func(t *testing.T) {
		f := newFixture(t)
		base := "http://release.invalid/releases"
		_, stderr, code := f.run(map[string]string{
			"PEERS_TEST_RELEASE_BASE": base,
			"FAKE_RELEASE_BASE":       base,
			"FAKE_EFFECTIVE_URL":      base + "/tag/v0.1.1",
		})
		if code != 0 {
			t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
		}
		log := f.readLog(f.curlLog)
		if strings.Contains(log, "--proto =https") {
			t.Fatalf("curl was forced to HTTPS for an http test base; log:\n%s", log)
		}
		if !strings.Contains(log, base+"/download/v0.1.1/") {
			t.Fatalf("curl did not use the test base; log:\n%s", log)
		}
	})

	t.Run("no GitHub API call", func(t *testing.T) {
		f := newFixture(t)
		if _, stderr, code := f.run(nil); code != 0 {
			t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr)
		}
		log := f.readLog(f.curlLog)
		if strings.Contains(log, "api.github.com") || strings.Contains(log, "/api/") {
			t.Fatalf("the script called the GitHub API; log:\n%s", log)
		}
	})
}

// remoteInstallEventBound is the package fallback for a wait on an event a fake
// tool reports rather than a product timer, such as the download reporting that
// it started. It only decides how long a wait that never completes takes to
// fail; the success path observes the marker at once, so a larger value costs
// nothing. Thirty seconds matches the fallback the rest of the end-to-end tests
// use.
const remoteInstallEventBound = 30 * time.Second

// interruptStartTimeout bounds the wait for the fake download to report that it
// started. The fake writes the marker before it sleeps, so a healthy run
// reaches the marker at once; the bound is the package fallback and does not
// hide a script that never starts the download.
const interruptStartTimeout = remoteInstallEventBound

func TestInterruptRemovesTemporaryDirectory(t *testing.T) {
	signals := []struct {
		name   string
		signal syscall.Signal
	}{
		{"TERM", syscall.SIGTERM},
		{"INT", syscall.SIGINT},
		{"HUP", syscall.SIGHUP},
	}
	for _, tt := range signals {
		t.Run(tt.name, func(t *testing.T) {
			f := newFixture(t)
			started := filepath.Join(f.base, "curl-started")
			cmd := exec.Command("sh", f.script, "--version", "0.1.1")
			cmd.Dir = f.base
			cmd.Env = f.environ(map[string]string{
				"FAKE_CURL_SLEEP":   "30",
				"FAKE_CURL_STARTED": started,
			})
			cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true}
			if err := cmd.Start(); err != nil {
				t.Fatalf("starting remote-install.sh: %v", err)
			}

			waitForFile(t, started, interruptStartTimeout)
			if entries := f.tempEntries(); len(entries) == 0 {
				t.Fatalf("the temporary directory did not exist while the download was running")
			}
			if cmd.Process.Pid <= 1 {
				t.Fatalf("invalid process id %d: negating it would signal init", cmd.Process.Pid)
			}
			//procsig:allow signals the whole process group this test started
			if err := syscall.Kill(-cmd.Process.Pid, tt.signal); err != nil {
				t.Fatalf("sending SIG%s: %v", tt.name, err)
			}

			err := cmd.Wait()
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() == 0 {
				t.Fatalf("wait returned %v, want a non-zero exit", err)
			}
			if entries := f.tempEntries(); len(entries) != 0 {
				t.Fatalf("temporary files were left after the interrupt: %q", entries)
			}
		})
	}
}

func waitForFile(t *testing.T, path string, timeout time.Duration) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if _, err := os.Stat(path); err == nil {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", path)
}

func mustMkdirAll(t *testing.T, dir string) {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
}

func mustWriteFile(t *testing.T, path string, data []byte, mode os.FileMode) {
	t.Helper()
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("writing %s: %v", path, err)
	}
}

func writeExecutable(t *testing.T, path, content string) {
	t.Helper()
	mustWriteFile(t, path, []byte(content), 0o755)
}

func mustSymlink(t *testing.T, target, path string) {
	t.Helper()
	if err := os.Symlink(target, path); err != nil {
		t.Fatalf("linking %s to %s: %v", path, target, err)
	}
}

func mustLookPath(t *testing.T, name string) string {
	t.Helper()
	path, err := exec.LookPath(name)
	if err != nil {
		t.Fatalf("looking up %s: %v", name, err)
	}
	return path
}

// archiveData returns a tar.gz with the layout the release archives use: the
// installer next to the binary. withInstall controls whether install.sh is
// present, so a release package that lacks it can be tested.
func archiveData(t *testing.T, withInstall bool) []byte {
	t.Helper()
	var buf bytes.Buffer
	gz := gzip.NewWriter(&buf)
	tw := tar.NewWriter(gz)
	add := func(name, body string) {
		header := &tar.Header{Name: name, Mode: 0o755, Size: int64(len(body))}
		if err := tw.WriteHeader(header); err != nil {
			t.Fatalf("writing the %s header: %v", name, err)
		}
		if _, err := tw.Write([]byte(body)); err != nil {
			t.Fatalf("writing %s: %v", name, err)
		}
	}
	if withInstall {
		add("install.sh", fakePackagedInstall)
	}
	add("peers", "#!/bin/sh\nexit 0\n")
	if err := tw.Close(); err != nil {
		t.Fatalf("closing the tar writer: %v", err)
	}
	if err := gz.Close(); err != nil {
		t.Fatalf("closing the gzip writer: %v", err)
	}
	return buf.Bytes()
}

const fakePackagedInstall = `#!/bin/sh
if [ -n "${FAKE_INSTALL_ARGS:-}" ]; then
	: > "$FAKE_INSTALL_ARGS"
	for arg in "$@"; do
		printf '%s\n' "$arg" >> "$FAKE_INSTALL_ARGS"
	done
fi
exit "${FAKE_INSTALL_EXIT:-0}"
`

const fakeUname = `#!/bin/sh
case ${1:-} in
	-s) printf '%s\n' "${FAKE_UNAME_S:-Linux}" ;;
	-m) printf '%s\n' "${FAKE_UNAME_M:-x86_64}" ;;
	*) printf '%s\n' "${FAKE_UNAME_S:-Linux}" ;;
esac
`

const fakeCurl = `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_CURL_LOG"

dest=
url=
effective=0
while [ "$#" -gt 0 ]; do
	case $1 in
		-o)
			dest=$2
			shift
			;;
		-w)
			case $2 in
				*url_effective*) effective=1 ;;
			esac
			shift
			;;
		--proto)
			shift
			;;
		http://*|https://*)
			url=$1
			;;
	esac
	shift
done

if [ -n "${FAKE_CURL_EXIT:-}" ]; then
	exit "$FAKE_CURL_EXIT"
fi

if [ "$effective" -eq 1 ]; then
	printf '%s' "${FAKE_EFFECTIVE_URL:-}"
	exit 0
fi

if [ -n "${FAKE_CURL_SLEEP:-}" ]; then
	: > "$FAKE_CURL_STARTED"
	sleep "$FAKE_CURL_SLEEP"
fi

rel=${url#"$FAKE_RELEASE_BASE"/}
src=$FAKE_RELEASE_DIR/$rel
if [ ! -f "$src" ]; then
	printf 'fake curl: no fixture for %s\n' "$url" >&2
	exit 22
fi
cp "$src" "$dest"
`

const fakeWget = `#!/bin/sh
printf '%s\n' "$*" >> "$FAKE_WGET_LOG"

dest=
url=
spider=0
while [ "$#" -gt 0 ]; do
	case $1 in
		-O)
			dest=$2
			shift
			;;
		--spider)
			spider=1
			;;
		--max-redirect)
			shift
			;;
		http://*|https://*)
			url=$1
			;;
	esac
	shift
done

if [ -n "${FAKE_WGET_EXIT:-}" ]; then
	exit "$FAKE_WGET_EXIT"
fi

if [ "$spider" -eq 1 ]; then
	{
		printf '%s\n' '---response begin---'
		printf '%s\n' 'HTTP/1.1 302 Found'
		printf '  Location: %s\n' "${FAKE_EFFECTIVE_URL:-}"
		printf '%s\n' '---response end---'
	} >&2
	exit 0
fi

rel=${url#"$FAKE_RELEASE_BASE"/}
src=$FAKE_RELEASE_DIR/$rel
if [ ! -f "$src" ]; then
	printf 'fake wget: no fixture for %s\n' "$url" >&2
	exit 8
fi
if [ -n "${FAKE_WGET_DOWNLOAD_LOCATION:-}" ]; then
	{
		printf '%s\n' '---response begin---'
		printf '  Location: %s\n' "$FAKE_WGET_DOWNLOAD_LOCATION"
		printf '%s\n' '---response end---'
	} >&2
fi
cp "$src" "$dest"
`

const fakeSha256sum = `#!/bin/sh
exec @REAL_SHA256SUM@ "$@"
`

const fakeShasum = `#!/bin/sh
if [ "$1" = "-a" ] && [ "$2" = "256" ]; then
	shift 2
fi
exec @REAL_SHA256SUM@ "$@"
`
