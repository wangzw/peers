package harness

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"slices"
	"strings"
	"testing"
)

// TestImageTagCarriesAccountAndOwner checks that the image tag carries the account ids, the owner marker and the content digest.
func TestImageTagCarriesAccountAndOwner(t *testing.T) {
	current := currentOwner()
	tag := imageReference("0123456789ab", 1234, 5678, current)
	if !strings.HasPrefix(tag, "peers-e2e:ape2e-") {
		t.Fatalf("imageTag = %q, want the peers-e2e repository and owner marker", tag)
	}
	if !strings.Contains(tag, "-u1234-g5678-") {
		t.Fatalf("imageTag = %q, want the uid and gid in the tag", tag)
	}
	if !strings.HasSuffix(tag, "-0123456789ab") {
		t.Fatalf("imageTag = %q, want the content digest at the end", tag)
	}
	if imageTagOwnerGone(tag) {
		t.Fatalf("imageTagOwnerGone(%q) = true; the owner is this live process", tag)
	}

	dead := imageReference("0123456789ab", 1234, 5678, owner{pid: 999999999, start: 1, ns: current.ns})
	if !imageTagOwnerGone(dead) {
		t.Fatalf("imageTagOwnerGone(%q) = false; want the dead owner reported", dead)
	}
	if imageTagOwnerGone("peers-e2e:0123456789ab") {
		t.Fatalf("a tag without an owner marker was reported as removable")
	}
}

// TestBuildBinaryCachesAndInjectsVersion checks that a build is reused for the same parameters and that the default version is stamped into the binary.
func TestBuildBinaryCachesAndInjectsVersion(t *testing.T) {
	pkg := "./test/e2e/harness/testdata/hello"

	first := BuildPackage(t, pkg, runtime.GOOS, runtime.GOARCH)
	second := BuildPackage(t, pkg, runtime.GOOS, runtime.GOARCH)
	if first != second {
		t.Fatalf("BuildPackage returned %q then %q for the same parameters", first, second)
	}

	out, err := exec.Command(first).Output()
	if err != nil {
		t.Fatalf("running %s: %v", first, err)
	}
	if got := strings.TrimSpace(string(out)); got != "e2e" {
		t.Fatalf("%s printed %q, want %q", first, got, "e2e")
	}

	bin := BuildBinary(t, runtime.GOOS, runtime.GOARCH)
	version, err := exec.Command(bin, "--version").Output()
	if err != nil {
		t.Fatalf("running %s --version: %v", bin, err)
	}
	if got := strings.TrimSpace(string(version)); got != "peers e2e" {
		t.Fatalf("%s --version printed %q, want %q", bin, got, "peers e2e")
	}

	// With go removed from PATH only the cache can answer; a second build would fail.
	t.Setenv("PATH", t.TempDir())
	if again := BuildPackage(t, pkg, runtime.GOOS, runtime.GOARCH); again != first {
		t.Fatalf("cached BuildPackage returned %q, want %q", again, first)
	}
	if again := BuildBinary(t, runtime.GOOS, runtime.GOARCH); again != bin {
		t.Fatalf("cached BuildBinary returned %q, want %q", again, bin)
	}
}

// TestBuildBinaryVersionStampsTheNamedVersion checks that builds of one package at different versions get their own output and report that version.
func TestBuildBinaryVersionStampsTheNamedVersion(t *testing.T) {
	old := BuildBinaryVersion(t, "0.1.0", runtime.GOOS, runtime.GOARCH, "peers_testenv")
	next := BuildBinaryVersion(t, "0.1.2", runtime.GOOS, runtime.GOARCH, "peers_testenv")
	if old == next {
		t.Fatalf("BuildBinaryVersion returned the same path %q for two versions", old)
	}
	for _, tc := range []struct{ path, want string }{
		{old, "peers 0.1.0"},
		{next, "peers 0.1.2"},
	} {
		out, err := exec.Command(tc.path, "--version").Output()
		if err != nil {
			t.Fatalf("running %s --version: %v", tc.path, err)
		}
		if got := strings.TrimSpace(string(out)); got != tc.want {
			t.Fatalf("%s --version printed %q, want %q", tc.path, got, tc.want)
		}
	}

	if got := DefaultVersion; got != "e2e" {
		t.Fatalf("DefaultVersion = %q, want %q", got, "e2e")
	}
}

// TestBuildSpecCoverageFlags checks that a product build is instrumented only when coverage is on while helper and overlay builds never are.
func TestBuildSpecCoverageFlags(t *testing.T) {
	const coverDir = "/tmp/e2e-cover"
	cases := []struct {
		name     string
		spec     buildSpec
		coverDir string
		want     bool
	}{
		{
			name:     "product build with coverage is instrumented",
			spec:     productSpec("./cmd/peers", "e2e", "linux", "amd64", nil),
			coverDir: coverDir,
			want:     true,
		},
		{
			name:     "product build without coverage is not instrumented",
			spec:     productSpec("./cmd/peers", "e2e", "linux", "amd64", nil),
			coverDir: "",
			want:     false,
		},
		{
			name:     "helper build stays uninstrumented",
			spec:     helperSpec("./test/e2e/fakepeer", "e2e", "linux", "amd64", []string{"e2e"}),
			coverDir: coverDir,
			want:     false,
		},
		{
			name:     "overlay build stays uninstrumented",
			spec:     overlaySpec("./cmd/peers", "e2e", "linux", "amd64", "overlay.json", nil),
			coverDir: coverDir,
			want:     false,
		},
	}
	coverArgs := []string{"-cover", "-covermode=atomic", "-coverpkg=" + coverPackages}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			args := tc.spec.args("out", tc.coverDir)
			got := 0
			for _, arg := range args {
				if slices.Contains(coverArgs, arg) {
					got++
				}
			}
			if want := map[bool]int{true: len(coverArgs), false: 0}[tc.want]; got != want {
				t.Fatalf("build args %v carry %d coverage flags, want %d", args, got, want)
			}
		})
	}

	if !productSpec("./cmd/peers", "e2e", "linux", "amd64", nil).instrument {
		t.Error("productSpec is not instrumentable")
	}
	if helperSpec("./cmd/peers", "e2e", "linux", "amd64", nil).instrument {
		t.Error("helperSpec is instrumentable")
	}
	if overlaySpec("./cmd/peers", "e2e", "linux", "amd64", "o.json", nil).instrument {
		t.Error("overlaySpec is instrumentable")
	}
}

// TestBuildArgsWithoutCoverageAreUnchanged fixes the uninstrumented go build argv.
func TestBuildArgsWithoutCoverageAreUnchanged(t *testing.T) {
	spec := productSpec("./cmd/peers", "e2e", "linux", "amd64", []string{"peers_testenv"})
	want := []string{
		"build", "-trimpath", "-ldflags", "-X main.version=e2e",
		"-tags", "peers_testenv", "-o", "out", "./cmd/peers",
	}
	if got := spec.args("out", ""); !slices.Equal(got, want) {
		t.Fatalf("build args = %v, want %v", got, want)
	}
}

// TestValidateCoverageDir checks that an empty coverage directory is accepted and any other value must be an absolute, existing, writable directory.
func TestValidateCoverageDir(t *testing.T) {
	file := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(file, []byte("x"), 0o644); err != nil {
		t.Fatalf("writing a fixture file: %v", err)
	}
	cases := []struct {
		name    string
		dir     string
		wantErr string
	}{
		{"empty turns coverage off", "", ""},
		{"relative path", "cover", "not an absolute path"},
		{"missing directory", filepath.Join(t.TempDir(), "absent"), "no such file or directory"},
		{"regular file", file, "is not a directory"},
		{"writable directory", t.TempDir(), ""},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			err := validateCoverageDir(tc.dir)
			if tc.wantErr == "" {
				if err != nil {
					t.Fatalf("validateCoverageDir(%q) = %v, want nil", tc.dir, err)
				}
				return
			}
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("validateCoverageDir(%q) = %v, want an error containing %q", tc.dir, err, tc.wantErr)
			}
		})
	}

	dir := t.TempDir()
	if err := validateCoverageDir(dir); err != nil {
		t.Fatalf("validateCoverageDir(%q) = %v", dir, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading %s: %v", dir, err)
	}
	if len(entries) != 0 {
		t.Fatalf("validateCoverageDir left %d entries in %s", len(entries), dir)
	}
}

// TestValidateCoverageDirRejectsUnwritable checks that a directory whose mode forbids writes is reported, except for root, which can write regardless.
func TestValidateCoverageDirRejectsUnwritable(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root can write to a directory whose mode forbids it")
	}
	dir := t.TempDir()
	if err := os.Chmod(dir, 0o555); err != nil {
		t.Fatalf("chmod %s: %v", dir, err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
	err := validateCoverageDir(dir)
	if err == nil || !strings.Contains(err.Error(), "not writable") {
		t.Fatalf("validateCoverageDir(%q) = %v, want a not-writable error", dir, err)
	}
}
