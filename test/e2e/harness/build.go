// Package harness provides the building blocks shared by the end-to-end
// tests: building product binaries, waiting for asynchronous events, and
// composing Broker configuration fragments.
package harness

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
)

type buildEntry struct {
	once sync.Once
	path string
	err  error
}

var (
	buildMu    sync.Mutex
	buildCache = map[string]*buildEntry{}

	buildDirOnce sync.Once
	buildDir     string
	buildDirErr  error

	modRootOnce sync.Once
	modRoot     string
	modRootErr  error
)

// DefaultVersion is the version stamped into binaries the harness builds when the caller does not name one; it is not a release version, so an update check treats such a build as non-release.
const DefaultVersion = "e2e"

// CoverageDirEnv names the environment variable that turns on end-to-end
// coverage collection. Its value is the host directory the instrumented
// processes write their counters to.
const CoverageDirEnv = "E2E_COVER_DIR"

const coverPackages = "github.com/wangzw/peers/..."

// CoverageDir returns the directory end-to-end coverage counters are written
// to, or "" when coverage collection is off.
func CoverageDir() string { return os.Getenv(CoverageDirEnv) }

// CoverageEnabled reports whether end-to-end coverage collection is on.
func CoverageEnabled() bool { return CoverageDir() != "" }

// DownTimeout is the docker stop timeout for a teardown, in seconds. An
// instrumented process flushes its counters while it shuts down, so coverage
// collection gives it more time than the one second an ordinary teardown uses.
func DownTimeout() string {
	if CoverageEnabled() {
		return "20"
	}
	return "1"
}

// buildSpec is one go build invocation; helper and overlay builds are never
// instrumented.
type buildSpec struct {
	pkg        string
	version    string
	goos       string
	goarch     string
	tags       []string
	overlay    string
	instrument bool
}

func productSpec(pkg, version, goos, goarch string, tags []string) buildSpec {
	return buildSpec{pkg: pkg, version: version, goos: goos, goarch: goarch, tags: tags, instrument: true}
}

func helperSpec(pkg, version, goos, goarch string, tags []string) buildSpec {
	return buildSpec{pkg: pkg, version: version, goos: goos, goarch: goarch, tags: tags}
}

func overlaySpec(pkg, version, goos, goarch, overlay string, tags []string) buildSpec {
	return buildSpec{pkg: pkg, version: version, goos: goos, goarch: goarch, tags: tags, overlay: overlay}
}

func (s buildSpec) args(out, coverDir string) []string {
	args := []string{"build"}
	if s.instrument && coverDir != "" {
		args = append(args, "-cover", "-covermode=atomic", "-coverpkg="+coverPackages)
	}
	args = append(args, "-trimpath", "-ldflags", "-X main.version="+s.version)
	if len(s.tags) > 0 {
		args = append(args, "-tags", strings.Join(s.tags, ","))
	}
	if s.overlay != "" {
		args = append(args, "-overlay="+s.overlay)
	}
	return append(args, "-o", out, s.pkg)
}

// cacheKey identifies a build; the instrumentation setting is part of it so an
// instrumented and an uninstrumented build of one package never share an entry.
func (s buildSpec) cacheKey() string {
	return strings.Join([]string{
		s.pkg, s.version, s.goos, s.goarch,
		strings.Join(s.tags, ","), s.overlay, strconv.FormatBool(s.instrument),
	}, "\x00")
}

var (
	coverCheckOnce sync.Once
	coverCheckErr  error
)

func checkCoverageDir(dir string) error {
	coverCheckOnce.Do(func() {
		coverCheckErr = validateCoverageDir(dir)
	})
	return coverCheckErr
}

func validateCoverageDir(dir string) error {
	if dir == "" {
		return nil
	}
	if !filepath.IsAbs(dir) {
		return fmt.Errorf("%s %q is not an absolute path", CoverageDirEnv, dir)
	}
	info, err := os.Stat(dir)
	if err != nil {
		return fmt.Errorf("%s %q: %w", CoverageDirEnv, dir, err)
	}
	if !info.IsDir() {
		return fmt.Errorf("%s %q is not a directory", CoverageDirEnv, dir)
	}
	probe, err := os.CreateTemp(dir, ".e2e-cover-check-*")
	if err != nil {
		return fmt.Errorf("%s %q is not writable: %w", CoverageDirEnv, dir, err)
	}
	name := probe.Name()
	closeErr := probe.Close()
	removeErr := os.Remove(name)
	if closeErr != nil || removeErr != nil {
		return fmt.Errorf("%s %q: checking writability left %s: %v %v", CoverageDirEnv, dir, name, closeErr, removeErr)
	}
	return nil
}

// BuildBinary builds ./cmd/peers for goos/goarch with the default version
// and returns the path of the resulting binary. The same parameters are built
// only once per test process.
func BuildBinary(tb testing.TB, goos, goarch string, tags ...string) string {
	tb.Helper()
	return BuildBinaryVersion(tb, DefaultVersion, goos, goarch, tags...)
}

// BuildBinaryVersion is BuildBinary with version stamped into main.version.
func BuildBinaryVersion(tb testing.TB, version, goos, goarch string, tags ...string) string {
	tb.Helper()
	return build(tb, productSpec("./cmd/peers", version, goos, goarch, tags))
}

// BuildBinaryVersionOverlay is BuildBinaryVersion with a Go build overlay file,
// in the form go build's -overlay flag accepts, that substitutes source files for
// this build only.
func BuildBinaryVersionOverlay(tb testing.TB, version, goos, goarch, overlay string, tags ...string) string {
	tb.Helper()
	return build(tb, overlaySpec("./cmd/peers", version, goos, goarch, overlay, tags))
}

// BuildPackage builds pkg for goos/goarch and returns the path of the result.
// Like BuildBinary, the same parameters are built only once per test process.
func BuildPackage(tb testing.TB, pkg, goos, goarch string, tags ...string) string {
	tb.Helper()
	return build(tb, helperSpec(pkg, DefaultVersion, goos, goarch, tags))
}

// ModuleRoot returns the root directory of the Go module under test.
func ModuleRoot(tb testing.TB) string {
	tb.Helper()
	root, err := moduleRoot()
	if err != nil {
		tb.Fatalf("harness: %v", err)
	}
	return root
}

func build(tb testing.TB, spec buildSpec) string {
	key := spec.cacheKey()

	buildMu.Lock()
	entry, ok := buildCache[key]
	if !ok {
		entry = &buildEntry{}
		buildCache[key] = entry
	}
	buildMu.Unlock()

	entry.once.Do(func() {
		entry.path, entry.err = goBuild(spec)
	})
	if entry.err != nil {
		tb.Fatalf("harness: building %s for %s/%s: %v", spec.pkg, spec.goos, spec.goarch, entry.err)
	}
	return entry.path
}

func goBuild(spec buildSpec) (string, error) {
	coverDir := CoverageDir()
	if spec.instrument && coverDir != "" {
		if err := checkCoverageDir(coverDir); err != nil {
			return "", err
		}
	}

	dir, err := buildDirectory()
	if err != nil {
		return "", err
	}
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}

	out := filepath.Join(dir, outputName(spec.pkg, spec.version, spec.goos, spec.goarch, spec.tags))
	if spec.goos == "windows" && !strings.HasSuffix(out, ".exe") {
		out += ".exe"
	}

	cmd := exec.Command("go", spec.args(out, coverDir)...)
	cmd.Dir = root
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS="+spec.goos,
		"GOARCH="+spec.goarch,
	)
	combined, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("go build %s: %w\n%s", spec.pkg, err, strings.TrimSpace(string(combined)))
	}
	return out, nil
}

func outputName(pkg, version, goos, goarch string, tags []string) string {
	tagPart := "notags"
	if len(tags) > 0 {
		tagPart = strings.Join(tags, "-")
	}
	return fmt.Sprintf("%s-%s-%s-%s-%s",
		sanitize(filepath.Base(filepath.Clean(pkg))), sanitize(version), goos, goarch, sanitize(tagPart))
}

func sanitize(s string) string {
	if s == "" || s == "." {
		return "pkg"
	}
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z',
			r >= 'A' && r <= 'Z',
			r >= '0' && r <= '9',
			r == '-', r == '_', r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	return b.String()
}

func buildDirectory() (string, error) {
	buildDirOnce.Do(func() {
		buildDir, buildDirErr = os.MkdirTemp("", "ap-e2e-build-")
	})
	return buildDir, buildDirErr
}

func moduleRoot() (string, error) {
	modRootOnce.Do(func() {
		out, err := exec.Command("go", "env", "GOMOD").Output()
		if err != nil {
			modRootErr = fmt.Errorf("go env GOMOD: %w", err)
			return
		}
		gomod := strings.TrimSpace(string(out))
		if gomod == "" || gomod == os.DevNull {
			modRootErr = errors.New("not inside a Go module")
			return
		}
		modRoot = filepath.Dir(gomod)
	})
	return modRoot, modRootErr
}
