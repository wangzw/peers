//go:build e2e

// Package syscontainer runs the system end-to-end tests inside
// containers. The host test process forwards each test to a fresh container of
// the requested service, so tests that need root, several accounts, or systemd
// see the environment the system Broker is designed for.
package syscontainer

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/test/e2e/harness"
)

// Service names the compose service a test runs in.
type Service string

const (
	// Plain is a container that runs an ordinary init and starts nothing.
	Plain Service = "plain"
	// Systemd is a privileged container whose process 1 is systemd.
	Systemd Service = "systemd"
)

// inContainerEnv marks a test process that runs inside the container.
const inContainerEnv = "PEERS_E2E_IN_CONTAINER"

// sudoersCoverPath is the sudoers drop-in that keeps GOCOVERDIR across sudo. It
// is written inside the container, not bind-mounted: sudo ignores a file not
// owned by uid 0, and a host file carries the invoking uid. The name has no dot,
// so sudo's includedir reads it.
const sudoersCoverPath = "/etc/sudoers.d/e2e-cover"

// sudoersCoverContent is the drop-in's content.
const sudoersCoverContent = `Defaults env_keep += "GOCOVERDIR"`

// sudoersWriteScript writes and validates the sudoers drop-in inside a
// container. sudo refuses a file whose owner is not root or that group or
// others can write, so the script sets root ownership and mode 0440 and then
// validates the result with visudo.
const sudoersWriteScript = "mkdir -p /etc/sudoers.d && " +
	"printf '%s\\n' '" + sudoersCoverContent + "' > " + sudoersCoverPath + " && " +
	"chown root:root " + sudoersCoverPath + " && " +
	"chmod 0440 " + sudoersCoverPath + " && " +
	"visudo -cf " + sudoersCoverPath

// systemdCoverDropInName is the host file name of the systemd drop-in mounted
// into the systemd services.
const systemdCoverDropInName = "90-e2e-cover.conf"

// systemdCoverDropInPath is where the drop-in mounts inside the container.
const systemdCoverDropInPath = "/etc/systemd/system.conf.d/" + systemdCoverDropInName

// systemdCoverDropInContent puts GOCOVERDIR into the manager's environment, so
// every unit it starts inherits the variable. The [Manager] header is required:
// a drop-in without it is ignored.
const systemdCoverDropInContent = "[Manager]\nDefaultEnvironment=GOCOVERDIR=" + harness.CoverMountPath + "\n"

// expectFailEnv forwards an intentional failure request into the container. It
// is set only by the smoke test that checks failure propagation.
const expectFailEnv = "PEERS_E2E_EXPECT_FAIL"

const (
	// systemdReadyBound is the total budget for one systemd container to finish
	// booting. It covers every probe, the one rebuild and the stop before it.
	// Evidence collection after this budget is given up has its own bound and
	// does not count here.
	systemdReadyBound = 90 * time.Second

	// systemdProbeBound bounds one is-system-running --wait probe. A healthy
	// boot returns in under a second. A boot still in the initial transaction
	// blocks until this bound kills the probe, and that timeout is the first
	// moment the stall is observable, so it equals systemdBootRebuildDelay.
	systemdProbeBound = systemdBootRebuildDelay

	// systemdPollInterval separates two probes that returned without a ready
	// state and without using the probe bound.
	systemdPollInterval = 100 * time.Millisecond
)

// InContainer reports whether the process was started by an e2e container.
func InContainer() bool {
	return os.Getenv(inContainerEnv) == "1"
}

// testBinary is the host path of the test binary built by Main. Run mounts it
// into the container.
var testBinary string

// testBinaryPath is the path the container sees.
const testBinaryPath = "/opt/peers-test"

// ArtifactPath returns the path inside the container of a file staged with
// StageArtifact.
func ArtifactPath(rel string) string {
	return filepath.Join(testBinaryPath, rel)
}

// StageArtifact writes data to rel under the directory mounted into the
// container at testBinaryPath and returns the path the container sees. It lets
// a host-side test give the inner test a file from the repository, such as the
// install script the release archive carries. The mount is read-only inside
// the container, so the file must be staged before Run starts it.
func StageArtifact(t *testing.T, rel string, data []byte, mode os.FileMode) string {
	t.Helper()

	if InContainer() {
		t.Fatalf("syscontainer: StageArtifact runs on the host")
	}
	if testBinary == "" {
		t.Fatalf("syscontainer: Main was not called, so there is no mount to stage %s in", rel)
	}
	if !filepath.IsLocal(rel) {
		t.Fatalf("syscontainer: staged path %q leaves the mount", rel)
	}
	dst := filepath.Join(filepath.Dir(testBinary), rel)
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		t.Fatalf("syscontainer: creating the staged directory for %s: %v", rel, err)
	}
	if err := os.WriteFile(dst, data, mode); err != nil {
		t.Fatalf("syscontainer: staging %s: %v", rel, err)
	}
	if err := os.Chmod(dst, mode); err != nil {
		t.Fatalf("syscontainer: setting the mode of staged %s: %v", rel, err)
	}
	return ArtifactPath(rel)
}

// Main is called from a package's TestMain. On the host it builds the calling
// package's test binary once for linux, runs the tests, and removes the
// binary's directory. Inside the container it just runs the tests.
func Main(m *testing.M) int {
	if InContainer() {
		return m.Run()
	}

	path, cleanup, err := buildTestBinary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "syscontainer: %v\n", err)
		return 1
	}
	defer cleanup()
	testBinary = path
	return m.Run()
}

// buildTestBinary compiles the calling package's tests for linux. The binary is
// built without cgo so it runs in the debian-based image.
func buildTestBinary() (string, func(), error) {
	dir, err := os.Getwd()
	if err != nil {
		return "", nil, fmt.Errorf("determining the package directory: %w", err)
	}
	tmp, err := os.MkdirTemp("", "ap-e2e-systest-")
	if err != nil {
		return "", nil, fmt.Errorf("creating the test binary directory: %w", err)
	}
	cleanup := func() { _ = os.RemoveAll(tmp) }

	out := filepath.Join(tmp, filepath.Base(dir)+".test")
	cmd := exec.Command("go", "test", "-c", "-tags", "e2e", "-o", out, ".")
	cmd.Dir = dir
	cmd.Env = append(os.Environ(),
		"CGO_ENABLED=0",
		"GOOS=linux",
		"GOARCH="+runtime.GOARCH,
	)
	if combined, err := cmd.CombinedOutput(); err != nil {
		cleanup()
		return "", nil, fmt.Errorf("building the test binary: %w\n%s", err, strings.TrimSpace(string(combined)))
	}
	return out, cleanup, nil
}

var (
	systemImageOnce sync.Once
	systemImageTag  string
)

// systemImage builds the systemd-derived image once per test process and
// returns its tag. The tag is derived from the base image tag, and an existing
// image with that tag is reused.
func systemImage(t *testing.T) string {
	t.Helper()

	base := harness.Image(t)
	systemImageOnce.Do(func() {
		dir, err := composeDir()
		if err != nil {
			t.Fatalf("syscontainer: %v", err)
		}
		// The tag covers the system Dockerfile as well as the base image: a
		// change confined to the system layer must not reuse an older image.
		dockerfile, err := os.ReadFile(filepath.Join(dir, "Dockerfile"))
		if err != nil {
			t.Fatalf("syscontainer: reading the system Dockerfile: %v", err)
		}
		tag := "peers-e2e-system:" + shortHash(base+"\x00"+string(dockerfile))
		if imageExists(tag) {
			systemImageTag = tag
			return
		}

		cmd := exec.Command("docker", "build",
			"--build-arg", "BASE="+base,
			"-t", tag,
			"-f", filepath.Join(dir, "Dockerfile"),
			dir,
		)
		cmd.Env = os.Environ()
		if out, err := cmd.CombinedOutput(); err != nil {
			t.Fatalf("syscontainer: docker build of the system image: %v\n%s", err, strings.TrimSpace(string(out)))
		}
		systemImageTag = tag
	})
	if systemImageTag == "" {
		t.Fatalf("syscontainer: the system image was not built")
	}
	return systemImageTag
}

// shortHash returns the first twelve hexadecimal characters of the SHA-256 of
// s.
func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:12]
}

// imageExists reports whether the tag is in the local image store.
func imageExists(tag string) bool {
	cmd := exec.Command("docker", "image", "inspect", tag)
	cmd.Env = os.Environ()
	return cmd.Run() == nil
}

// composeDir returns the absolute path of test/e2e/compose/system.
func composeDir() (string, error) {
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "test", "e2e", "compose", "system"), nil
}

var (
	moduleRootOnce sync.Once
	moduleRootPath string
	moduleRootErr  error
)

// moduleRoot returns the root directory of the Go module.
func moduleRoot() (string, error) {
	moduleRootOnce.Do(func() {
		out, err := exec.Command("go", "env", "GOMOD").Output()
		if err != nil {
			moduleRootErr = fmt.Errorf("go env GOMOD: %w", err)
			return
		}
		gomod := strings.TrimSpace(string(out))
		if gomod == "" || gomod == os.DevNull {
			moduleRootErr = fmt.Errorf("not inside a Go module")
			return
		}
		moduleRootPath = filepath.Dir(gomod)
	})
	return moduleRootPath, moduleRootErr
}

// Run runs the current test inside a fresh container of svc and fails t when
// the inner run fails. The test must not touch the container itself. Each
// extraEnv entry is a NAME=value pair the inner test needs, such as an address
// the host chose at run time.
func Run(t *testing.T, svc Service, extraEnv ...string) {
	t.Helper()
	t.Parallel()

	if InContainer() {
		t.Fatalf("syscontainer: Run was called inside the container")
	}
	c := newTestContainer(t, extraEnv...)

	argv := []string{
		testBinaryPath + "/" + filepath.Base(testBinary),
		"-test.run", runTestFlag(t.Name()),
		"-test.v",
		"-test.count=1",
	}

	var out []byte
	var err error
	switch svc {
	case Plain:
		args := c.composeArgs("run", "--rm", "-T")
		args = append(args, c.forwardEnvArgs()...)
		args = append(args, string(Plain))
		args = append(args, plainCommand(argv)...)
		out, err = c.command(args...).CombinedOutput()
	case Systemd:
		if out, err = c.command(c.composeArgs("up", "-d", string(Systemd))...).CombinedOutput(); err != nil {
			t.Fatalf("syscontainer: starting systemd in %s: %v\n%s", c.project, err, strings.TrimSpace(string(out)))
		}
		c.waitSystemd(string(Systemd))
		c.writeSudoers(t, string(Systemd))
		args := c.composeArgs("exec", "-T")
		args = append(args, c.forwardEnvArgs()...)
		args = append(args, string(Systemd))
		args = append(args, argv...)
		out, err = c.command(args...).CombinedOutput()
	default:
		t.Fatalf("syscontainer: unknown service %q", svc)
	}

	if len(out) > 0 {
		t.Logf("syscontainer: output of %s in %s:\n%s", t.Name(), c.project, strings.TrimSpace(string(out)))
	}
	if err != nil {
		// Compose passes the inner process's exit status through. A SIGKILL
		// (137, usually the OOM killer) leaves logs that end before the kill, so
		// the daemon's events for this project are what explain the failure.
		if diagnostic := c.exitDiagnostic(err); diagnostic != "" {
			t.Fatalf("syscontainer: %s failed inside the %s container: %v\n%s", t.Name(), svc, err, diagnostic)
		}
		t.Fatalf("syscontainer: %s failed inside the %s container: %v", t.Name(), svc, err)
	}
}

// newTestContainer allocates a subnet, derives a compose project for the test,
// and builds the container handle Run and RunPairTest drive. extraEnv entries
// are added to the compose environment and forwarded to the inner process. The
// project is torn down when the test ends.
func newTestContainer(t *testing.T, extraEnv ...string) *container {
	t.Helper()
	if testBinary == "" {
		t.Fatalf("syscontainer: Main was not called, so there is no test binary to run")
	}
	if err := checkCompose(); err != nil {
		t.Fatalf("syscontainer: %v", err)
	}

	image := systemImage(t)
	sourceDir, err := composeDir()
	if err != nil {
		t.Fatalf("syscontainer: %v", err)
	}
	// Copy the compose file into the test's temporary directory so the
	// coverage override can sit next to it without touching the repository.
	composeData, err := os.ReadFile(filepath.Join(sourceDir, "compose.yaml"))
	if err != nil {
		t.Fatalf("syscontainer: reading the compose file: %v", err)
	}
	dir := t.TempDir()
	composePath := filepath.Join(dir, "compose.yaml")
	if err := os.WriteFile(composePath, composeData, 0o644); err != nil {
		t.Fatalf("syscontainer: writing %s: %v", composePath, err)
	}
	composeFiles := []string{composePath}

	// Allocate the subnet before registering down: t.Cleanup runs in reverse,
	// so the network is removed before its slot is handed to another project.
	subnet := allocSystemSubnet(t)
	project := projectName(t.Name())
	owner := ownerLabelValue()
	env := []string{
		"PEERS_E2E_SYSTEM_IMAGE=" + image,
		"PEERS_E2E_BIN=" + filepath.Dir(testBinary),
		"PEERS_E2E_SYSTEM_SUBNET=" + subnet,
		ownerEnv + "=" + owner,
	}
	env = append(env, extraEnv...)
	forward := append([]string(nil), env...)
	if harness.CoverageEnabled() {
		overridePath, forwardEntry := writeSystemCoverage(t, dir)
		composeFiles = append(composeFiles, overridePath)
		forward = append(forward, forwardEntry)
	}
	c := &container{
		t:            t,
		project:      project,
		owner:        owner,
		started:      time.Now(),
		composeFile:  composePath,
		composeFiles: composeFiles,
		// The compose file mounts this directory at /opt/peers-test.
		env: append(os.Environ(), env...),
		// The inner process needs the values; compose exec reads them from the
		// docker CLI environment, which is this build of env.
		forward: forward,
	}
	t.Cleanup(c.down)
	return c
}

// writeSystemCoverage writes the coverage override the system topology needs
// under dir. It returns the override path and the E2E_COVER_DIR entry forwarded
// to the inner test, whose value is the container path. Every service gets the
// mounted coverage directory; the two systemd services also get a read-only
// DefaultEnvironment drop-in, which is how their units inherit GOCOVERDIR.
func writeSystemCoverage(t testing.TB, dir string) (string, string) {
	t.Helper()
	dropInPath := filepath.Join(dir, systemdCoverDropInName)
	if err := os.WriteFile(dropInPath, []byte(systemdCoverDropInContent), 0o644); err != nil {
		t.Fatalf("syscontainer: writing the systemd coverage drop-in: %v", err)
	}
	mount := dropInPath + ":" + systemdCoverDropInPath + ":ro"
	extraVolumes := map[string][]string{
		string(Systemd):   {mount},
		string(pairHostB): {mount},
	}
	override, err := harness.CoverOverride(harness.CoverageDir(),
		[]string{string(Plain), string(Systemd), string(pairHostB)}, extraVolumes)
	if err != nil {
		t.Fatalf("syscontainer: building the coverage override: %v", err)
	}
	overridePath := filepath.Join(dir, "compose.cover.yaml")
	if err := os.WriteFile(overridePath, override, 0o644); err != nil {
		t.Fatalf("syscontainer: writing the coverage override: %v", err)
	}
	return overridePath, harness.CoverageDirEnv + "=" + harness.CoverMountPath
}

// pairHostA and pairHostB are the compose services a pair run starts. hostA
// carries the system Broker; hostB is its equal and carries the Broker that
// pairs from the other host.
const (
	pairHostA Service = "systemd"
	pairHostB Service = "hostB"
)

// pairInnerTestTimeout bounds one role's test binary inside its container. Even
// at every bound, one role's waits add up to about two and a half minutes: its
// startup including the MCP handshake, at most two exchange waits (72s is the
// longest) and its terminal commands. Five minutes is more than four times the
// longest exchange wait and over twice that sum, which leaves room for a loaded
// CI host while ending a role stuck in an unbounded call well before the outer
// 30-minute e2e bound (E2E_TIMEOUT in the Makefile).
const pairInnerTestTimeout = 5 * time.Minute

// RunPair is the TestMain entry point of a package that runs one of its tests
// on two hosts. Inside either pair container it runs the selected tests as the
// role the container carries. On the test host it builds the test binary and
// runs the package's tests as Main does; the test that needs both hosts calls
// RunPairTest from its body, because only a *testing.T can build the image and
// own the teardown.
//
// opts is the same value the test passes to RunPairTest. RunPair checks that
// opts.SharedDir is PairSharedMount and, inside a pair container, that the
// value the harness forwarded matches it, so a misconfigured TestMain fails
// before the test body runs.
func RunPair(m *testing.M, opts PairOptions) int {
	if opts.SharedDir != PairSharedMount {
		fmt.Fprintf(os.Stderr, "syscontainer: the pair shared directory is %q, want %q\n",
			opts.SharedDir, PairSharedMount)
		return 1
	}
	if InContainer() {
		if InPair() && PairSharedDir() != PairSharedMount {
			fmt.Fprintf(os.Stderr, "syscontainer: the pair shared directory is %q, want %q\n",
				PairSharedDir(), PairSharedMount)
			return 1
		}
		return m.Run()
	}
	return Main(m)
}

// RunPairTest runs the current test on hostA and hostB at the same time and
// fails t when either side fails. Both containers come from one compose
// project on one internal network, so hostA resolves by name as hostA and
// hostB as hostB. The two roles exchange data through opts.SharedDir, a volume
// mounted at the same path in both containers.
func RunPairTest(t *testing.T, opts PairOptions) {
	t.Helper()
	if InContainer() {
		t.Fatalf("syscontainer: RunPairTest runs on the host")
	}
	if opts.SharedDir != PairSharedMount {
		t.Fatalf("syscontainer: the pair shared directory is %q, but the compose file mounts it at %s",
			opts.SharedDir, PairSharedMount)
	}

	c := newTestContainer(t)
	up := c.command(c.composeArgs("up", "-d", string(pairHostA), string(pairHostB))...)
	if out, err := up.CombinedOutput(); err != nil {
		t.Fatalf("syscontainer: starting the pair in %s: %v\n%s", c.project, err, strings.TrimSpace(string(out)))
	}
	c.waitSystemd(string(pairHostA))
	c.waitSystemd(string(pairHostB))
	c.writeSudoers(t, string(pairHostA))
	c.writeSudoers(t, string(pairHostB))

	argv := []string{
		testBinaryPath + "/" + filepath.Base(testBinary),
		"-test.run", runTestFlag(t.Name()),
		"-test.v",
		"-test.count=1",
		"-test.timeout=" + pairInnerTestTimeout.String(),
	}

	type pairResult struct {
		role Role
		out  []byte
		err  error
	}
	hosts := []struct {
		role    Role
		service Service
	}{
		{RoleA, pairHostA},
		{RoleB, pairHostB},
	}
	results := make(chan pairResult, len(hosts))
	for _, host := range hosts {
		host := host
		go func() {
			args := c.composeArgs("exec", "-T")
			args = append(args, c.forwardEnvArgs()...)
			args = append(args, "-e", PairRoleEnv+"="+string(host.role), "-e", pairSharedEnv+"="+opts.SharedDir)
			for _, entry := range opts.ExtraEnv {
				args = append(args, "-e", entry)
			}
			args = append(args, string(host.service))
			args = append(args, argv...)
			out, err := c.command(args...).CombinedOutput()
			results <- pairResult{role: host.role, out: out, err: err}
		}()
	}

	var failed []string
	var diagnostics []string
	for range hosts {
		result := <-results
		if len(result.out) > 0 {
			t.Logf("syscontainer: output of %s on host %s:\n%s",
				t.Name(), result.role, strings.TrimSpace(string(result.out)))
		}
		if result.err != nil {
			failed = append(failed, string(result.role))
			if diagnostic := c.exitDiagnostic(result.err); diagnostic != "" {
				diagnostics = append(diagnostics, fmt.Sprintf("host %s: %s", result.role, diagnostic))
			}
		}
	}
	if len(failed) > 0 {
		if len(diagnostics) > 0 {
			t.Fatalf("syscontainer: %s failed on host %s\n%s",
				t.Name(), strings.Join(failed, ", "), strings.Join(diagnostics, "\n"))
		}
		t.Fatalf("syscontainer: %s failed on host %s", t.Name(), strings.Join(failed, ", "))
	}
}

// forwardEnvArgs returns compose flags that forward the intentional-failure
// variable and the host gateway into the container.
func (c *container) forwardEnvArgs() []string {
	var args []string
	if value, ok := os.LookupEnv(expectFailEnv); ok {
		args = append(args, "-e", expectFailEnv+"="+value)
	}
	for _, entry := range c.forward {
		args = append(args, "-e", entry)
	}
	return args
}

// container is one compose project started for one test.
type container struct {
	t       *testing.T
	project string
	// owner is this process's owner marker as stored in harness.OwnerLabel. It
	// names the docker events a failed run is diagnosed with.
	owner string
	// started is when the project was created, the start of the event window a
	// failed run is diagnosed with.
	started time.Time
	// composeFile is the copied base file, the one evidence collection reads.
	composeFile string
	// composeFiles are every file a compose command passes, the copied base
	// file and, under coverage, its override.
	composeFiles []string
	env          []string
	// forward is passed into the container with -e, one NAME=value per entry.
	forward []string
}

// command builds one docker invocation for the project.
func (c *container) command(args ...string) *exec.Cmd {
	cmd := exec.Command("docker", args...)
	cmd.Env = c.env
	return cmd
}

// composeArgs prefixes a compose command with the project name and every
// compose file, so every command sees the coverage override the start used.
func (c *container) composeArgs(args ...string) []string {
	argv := []string{"compose", "-p", c.project}
	for _, file := range c.composeFiles {
		argv = append(argv, "-f", file)
	}
	return append(argv, args...)
}

// sudoersInstall returns the command that writes the sudoers drop-in inside
// service, or nil when coverage is off. The write runs as root through compose
// exec, so sudo keeps GOCOVERDIR for the commands a test runs through it.
func (c *container) sudoersInstall(service string) []string {
	if !harness.CoverageEnabled() {
		return nil
	}
	return c.composeArgs("exec", "-T", "-u", "root", service, "sh", "-c", sudoersWriteScript)
}

// writeSudoers installs the sudoers drop-in in service when coverage is on.
func (c *container) writeSudoers(t *testing.T, service string) {
	t.Helper()
	argv := c.sudoersInstall(service)
	if argv == nil {
		return
	}
	if out, err := c.command(argv...).CombinedOutput(); err != nil {
		t.Fatalf("syscontainer: writing the coverage sudoers drop-in in %s: %v\n%s",
			service, err, strings.TrimSpace(string(out)))
	}
}

// plainCommand wraps a plain-container test command so the sudoers drop-in is
// written inside the container the run starts, before the test replaces the
// shell. A compose run has no separate exec step to write it from the host.
func plainCommand(argv []string) []string {
	if !harness.CoverageEnabled() {
		return argv
	}
	return append([]string{"sh", "-c", sudoersWriteScript + ` && exec "$@"`, "sh"}, argv...)
}

// exitDiagnostic returns the text that explains an inner run that exited with
// killedExitCode, or "" for any other failure. It asks the daemon for the
// events of this project's owner label since the project started, under its
// own bound so a hung daemon cannot delay the failure report.
func (c *container) exitDiagnostic(err error) string {
	var exitErr *exec.ExitError
	if !errors.As(err, &exitErr) {
		return ""
	}
	ctx, cancel := context.WithTimeout(context.Background(), systemdEvidenceBound)
	defer cancel()
	run := func(args ...string) string { return c.docker(ctx, args...) }
	return collectExitDiagnostic(exitErr.ExitCode(), run, c.owner, c.started, time.Now())
}

// systemdRebuildLedger records every systemd container rebuild. make e2e prints
// it after the test run, so a gate that does not pass -v still shows how often
// and why a container was rebuilt. The path is repeated in the Makefile.
const systemdRebuildLedger = "/tmp/ape2e-systemd-rebuilds.log"

// waitSystemd waits until service's systemd reports running or degraded.
// systemctl --wait blocks until the state settles, so a probe returns either a
// settled state or, when its own bound expires, the stall.
//
// A container is recreated at most once, and only for the two failure modes
// observed under the full gate; bootretry.go names them. The whole wait,
// including the probe, the rebuild and the stop before it, shares one
// systemdReadyBound context. Evidence collection after that budget is given up
// has its own bound and does not extend it.
func (c *container) waitSystemd(service string) {
	ctx, cancel := context.WithTimeout(context.Background(), systemdReadyBound)
	defer cancel()

	start := time.Now()
	rebuilds := 0
	lastState := "no probe completed"

	for {
		if ctx.Err() != nil {
			c.failSystemdBoot(service, lastState)
		}

		probe := c.probeSystemd(ctx, service)
		if probe.ready {
			return
		}
		if probe.state != "" {
			lastState = probe.state
		}
		if ctx.Err() != nil {
			c.failSystemdBoot(service, lastState)
		}

		observation := systemdBootObservation{
			rebuilds: rebuilds,
			elapsed:  time.Since(start),
		}
		switch {
		case probe.exited:
			observation.exited = true
			if code, logs, err := c.containerExit(ctx, service); err == nil {
				observation.exitCode = code
				observation.logsEmpty = strings.TrimSpace(logs) == ""
			}
		case probe.timedOut:
			observation.probeTimedOut = true
			observation.state = c.systemdCurrentState(ctx, service)
			lastState = fmt.Sprintf("probe timed out after %s, state %q", systemdProbeBound, observation.state)
		}

		if shouldRebuildSystemd(observation) {
			rebuilds++
			reason := rebuildReason(observation)
			c.recordRebuild(service, reason)
			c.t.Logf("syscontainer: %s service %s: %s; recreating the container (%d of %d)",
				c.project, service, reason, rebuilds, systemdBootRebuilds)
			// Keep the evidence of the failed boot before the container is
			// replaced.
			c.logBootEvidence(service)
			c.recreateSystemd(ctx, service)
			continue
		}
		if observation.exited {
			// A container whose process 1 is gone does not come back on its
			// own. Any exit that is not the observed 255-with-empty-logs is a
			// real failure, so report it now instead of waiting out the bound.
			c.failSystemdBoot(service, lastState)
		}

		select {
		case <-ctx.Done():
		case <-time.After(systemdPollInterval):
		}
	}
}

// rebuildReason describes the observation for the rebuild ledger and the test
// log.
func rebuildReason(o systemdBootObservation) string {
	if o.exited {
		return fmt.Sprintf("process 1 exited with code %d and docker logs is empty", o.exitCode)
	}
	return fmt.Sprintf("is-system-running --wait timed out after %s and the state is %q", systemdProbeBound, o.state)
}

// failSystemdBoot writes the evidence and ends the test at the readiness bound.
func (c *container) failSystemdBoot(service, lastState string) {
	c.logBootEvidence(service)
	c.t.Fatalf("%s to finish booting: not within %s (last state: %s)", service, systemdReadyBound, lastState)
}

// probeSystemd runs one readiness probe under the shared context. The probe's
// own bound is shorter than the readiness bound, and a probe that expires
// while the outer context is still alive is the timeout that can precede a
// rebuild.
func (c *container) probeSystemd(ctx context.Context, service string) systemdProbeResult {
	probeCtx, cancel := context.WithTimeout(ctx, systemdProbeBound)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker",
		c.composeArgs("exec", "-T", service, "systemctl", "is-system-running", "--wait")...)
	cmd.Env = c.env
	out, err := cmd.CombinedOutput()
	timedOut := probeCtx.Err() == context.DeadlineExceeded && ctx.Err() == nil
	return interpretSystemdState(string(out), err, timedOut)
}

// systemdCurrentState reads the state without --wait, which reports the current
// text instead of blocking. It confirms that a timed-out probe was looking at a
// system that was still initializing.
func (c *container) systemdCurrentState(ctx context.Context, service string) string {
	probeCtx, cancel := context.WithTimeout(ctx, systemdProbeBound)
	defer cancel()
	cmd := exec.CommandContext(probeCtx, "docker",
		c.composeArgs("exec", "-T", service, "systemctl", "is-system-running")...)
	cmd.Env = c.env
	out, err := cmd.CombinedOutput()
	text := strings.TrimSpace(string(out))
	if err != nil && text == "" {
		return ""
	}
	return text
}

// containerID returns the project's container id for service, including an
// exited one.
func (c *container) containerID(ctx context.Context, service string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", c.composeArgs("ps", "-aq", service)...)
	cmd.Env = c.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

// containerExit returns process 1's exit code and docker logs for a container
// that compose reported as not running.
func (c *container) containerExit(ctx context.Context, service string) (int, string, error) {
	id, err := c.containerID(ctx, service)
	if err != nil {
		return 0, "", err
	}
	if id == "" {
		return 0, "", fmt.Errorf("no container id")
	}
	inspect := exec.CommandContext(ctx, "docker", "inspect", "--format", "{{.State.ExitCode}}", id)
	inspect.Env = c.env
	out, err := inspect.CombinedOutput()
	if err != nil {
		return 0, "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	code, err := strconv.Atoi(strings.TrimSpace(string(out)))
	if err != nil {
		return 0, "", fmt.Errorf("parsing the exit code %q: %w", strings.TrimSpace(string(out)), err)
	}
	logsCmd := exec.CommandContext(ctx, "docker", "logs", id)
	logsCmd.Env = c.env
	logs, err := logsCmd.CombinedOutput()
	if err != nil {
		return code, "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(logs)))
	}
	return code, string(logs), nil
}

// recreateSystemd stops the container with a one-second timeout and rebuilds it
// in place, keeping the project's network. Both commands share the readiness
// context. A failure is reported rather than fatal, so the wait continues and
// the final state still explains the outcome.
func (c *container) recreateSystemd(ctx context.Context, service string) {
	if id, err := c.containerID(ctx, service); err == nil && id != "" {
		stop := exec.CommandContext(ctx, "docker", "stop", "--timeout", "1", id)
		stop.Env = c.env
		if out, err := stop.CombinedOutput(); err != nil {
			c.t.Logf("syscontainer: stopping the %s container in %s: %v\n%s",
				service, c.project, err, strings.TrimSpace(string(out)))
		}
	}
	up := exec.CommandContext(ctx, "docker", c.composeArgs("up", "-d", "--force-recreate", service)...)
	up.Env = c.env
	if out, err := up.CombinedOutput(); err != nil {
		c.t.Logf("syscontainer: recreating the %s container in %s: %v\n%s",
			service, c.project, err, strings.TrimSpace(string(out)))
	}
}

// recordRebuild appends one line to systemdRebuildLedger. The ledger is
// appended to with O_APPEND, so concurrent suites do not lose each other's
// lines.
func (c *container) recordRebuild(service, reason string) {
	line := fmt.Sprintf("%s %s %s %s\n", time.Now().UTC().Format(time.RFC3339), c.project, service, reason)
	f, err := os.OpenFile(systemdRebuildLedger, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o600)
	if err != nil {
		c.t.Logf("syscontainer: recording the rebuild: %v", err)
		return
	}
	_, writeErr := f.WriteString(line)
	closeErr := f.Close()
	if writeErr != nil || closeErr != nil {
		c.t.Logf("syscontainer: recording the rebuild: %v; closing: %v", writeErr, closeErr)
	}
}

// down tears the project down and removes the network. It is idempotent. The
// one-second stop timeout force-kills a container whose process 1 does not exit
// on the image's stop signal, so a container torn down here can end with exit
// code 137; the residue cleanup only force-removes containers whose owner
// process is already gone. Under coverage the timeout is longer, so an
// instrumented process can flush its counters while it stops.
func (c *container) down() {
	cmd := c.command(c.composeArgs("down", "-v", "--remove-orphans", "--timeout", harness.DownTimeout())...)
	if out, err := cmd.CombinedOutput(); err != nil {
		c.t.Logf("syscontainer: tearing down %s: %v\n%s", c.project, err, strings.TrimSpace(string(out)))
	}
}

// checkCompose reports whether docker and the compose plugin are usable. A
// host without them fails the test with a reason; it does not skip.
func checkCompose() error {
	if _, err := exec.LookPath("docker"); err != nil {
		return fmt.Errorf("docker is required for container scenarios: %w", err)
	}
	cmd := exec.Command("docker", "compose", "version")
	cmd.Env = os.Environ()
	if out, err := cmd.CombinedOutput(); err != nil {
		return fmt.Errorf("docker compose is required for container scenarios: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

var (
	projectNameMu   sync.Mutex
	projectNameLast string
	projectNameSeq  uint64
)

// projectName derives a compose project name from a test name. Every name
// starts with ape2e- so the residue checks find it.
func projectName(testName string) string {
	var b strings.Builder
	for _, r := range strings.ToLower(testName) {
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	slug := strings.Trim(b.String(), "-")
	if len(slug) > 32 {
		slug = strings.Trim(slug[:32], "-")
	}
	if slug == "" {
		slug = "e2e"
	}

	projectNameMu.Lock()
	defer projectNameMu.Unlock()
	for {
		name := "ape2e-" + slug + "-" + randomHex6()
		if name != projectNameLast {
			projectNameLast = name
			return name
		}
	}
}

// randomHex6 returns six random hexadecimal digits.
func randomHex6() string {
	var b [3]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	projectNameSeq++
	return fmt.Sprintf("%06x", projectNameSeq&0xffffff)
}

// WaitFor waits up to the package-wide fallback for cond to return true,
// delegating the polling to harness.Eventually. It is for a filesystem, socket
// or process condition that makes no tool call. A condition that calls a tool
// uses harness.EventuallyPoll with Session.PollCall instead: the callback
// receives the wait's deadline, so each call runs its own poll bound in full
// rather than being cut short when the wait has little time left. Longer
// non-tool waits use harness.Eventually with the sum of the relevant timers
// plus the fallback.
func WaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	harness.Eventually(t, harness.EventTimeout, what, func() (bool, string) {
		return cond(), ""
	})
}
