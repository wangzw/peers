//go:build e2e

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

// inContainerEnv is set by every compose service; its name must match the
// literal in compose/system/compose.yaml.
const inContainerEnv = "PEERS_E2E_IN_CONTAINER"

// sudoersCoverPath is the sudoers drop-in that keeps GOCOVERDIR across sudo; it
// is written inside the container because sudo ignores a file not owned by uid
// 0, and its name has no dot so sudo's includedir reads it.
const sudoersCoverPath = "/etc/sudoers.d/e2e-cover"

const sudoersCoverContent = `Defaults env_keep += "GOCOVERDIR"`

// sudoersWriteScript writes the drop-in with root ownership and mode 0440,
// which sudo requires, and validates it with visudo.
const sudoersWriteScript = "mkdir -p /etc/sudoers.d && " +
	"printf '%s\\n' '" + sudoersCoverContent + "' > " + sudoersCoverPath + " && " +
	"chown root:root " + sudoersCoverPath + " && " +
	"chmod 0440 " + sudoersCoverPath + " && " +
	"visudo -cf " + sudoersCoverPath

const systemdCoverDropInName = "90-e2e-cover.conf"

const systemdCoverDropInPath = "/etc/systemd/system.conf.d/" + systemdCoverDropInName

// systemdCoverDropInContent puts GOCOVERDIR into the manager's environment, so
// every unit it starts inherits it; the [Manager] header is required, because a
// drop-in without it is ignored.
const systemdCoverDropInContent = "[Manager]\nDefaultEnvironment=GOCOVERDIR=" + harness.CoverMountPath + "\n"

// expectFailEnv is read by the smoke test; its name must match the literal in
// test/e2e/system/harness_smoke_test.go.
const expectFailEnv = "PEERS_E2E_EXPECT_FAIL"

const (
	// systemdReadyBound is the total budget for one systemd container to finish
	// booting, covering every probe, the one rebuild and the stop before it;
	// evidence collection after this budget has its own bound.
	systemdReadyBound = 90 * time.Second

	// systemdProbeBound bounds one is-system-running --wait probe. It equals
	// systemdBootRebuildDelay because the stall becomes observable only when the
	// probe bound expires.
	systemdProbeBound = systemdBootRebuildDelay

	systemdPollInterval = 100 * time.Millisecond
)

// InContainer reports whether the process was started by an e2e container.
func InContainer() bool {
	return os.Getenv(inContainerEnv) == "1"
}

var testBinary string

// testBinaryPath must equal the container-side mount path of the test binary
// directory, /opt/peers-test in compose/system/compose.yaml.
const testBinaryPath = "/opt/peers-test"

// ArtifactPath returns the path inside the container of a file staged with
// StageArtifact.
func ArtifactPath(rel string) string {
	return filepath.Join(testBinaryPath, rel)
}

// StageArtifact writes data to rel under the directory mounted at
// testBinaryPath and returns the path the container sees; it runs on the host
// and must be called before Run starts the read-only mount.
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

// Main is called from a package's TestMain; on the host it builds the calling
// package's test binary once for linux and runs the tests, and inside the
// container it just runs the tests. It returns the tests' exit code, 1 when the
// build fails, and removes the build directory when it returns.
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

// buildTestBinary compiles the calling package's tests for linux; CGO_ENABLED=0
// keeps the binary runnable in the debian-based image.
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
// returns its tag; the tag covers the base image and the system Dockerfile, and
// an existing image with that tag is reused.
func systemImage(t *testing.T) string {
	t.Helper()

	base := harness.Image(t)
	systemImageOnce.Do(func() {
		dir, err := composeDir()
		if err != nil {
			t.Fatalf("syscontainer: %v", err)
		}
		// The tag covers the system Dockerfile as well as the base image, so a
		// change confined to the system layer builds a new image.
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

func shortHash(s string) string {
	sum := sha256.Sum256([]byte(s))
	return hex.EncodeToString(sum[:])[:12]
}

func imageExists(tag string) bool {
	cmd := exec.Command("docker", "image", "inspect", tag)
	cmd.Env = os.Environ()
	return cmd.Run() == nil
}

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
// the inner run fails. It calls t.Parallel, so a t.Setenv before Run panics.
// The test must not touch the container itself. Each extraEnv entry is a
// NAME=value pair the inner test needs, such as an address the host chose at
// run time.
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
		// Compose passes the inner process's exit status through; a SIGKILL (137,
		// usually the OOM killer) leaves logs that end before the kill, so the
		// daemon's events for this project explain the failure.
		if diagnostic := c.exitDiagnostic(err); diagnostic != "" {
			t.Fatalf("syscontainer: %s failed inside the %s container: %v\n%s", t.Name(), svc, err, diagnostic)
		}
		t.Fatalf("syscontainer: %s failed inside the %s container: %v", t.Name(), svc, err)
	}
}

// newTestContainer allocates a subnet and a compose project for one test and
// returns the handle Run and RunPairTest drive; the project is torn down when
// the test ends. extraEnv entries are added to the compose environment and
// forwarded to the inner process.
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
	// Copy the file into the test's temporary directory so the coverage
	// override can sit next to it without touching the repository.
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
		env:          append(os.Environ(), env...),
		// The inner process needs the values; compose exec reads them from the
		// docker CLI environment, which is this build of env.
		forward: forward,
	}
	t.Cleanup(c.down)
	return c
}

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

// pairHostA and pairHostB are the compose services a pair run starts; hostA
// carries the system Broker and hostB its pair.
const (
	pairHostA Service = "systemd"
	pairHostB Service = "hostB"
)

// pairInnerTestTimeout bounds the test binary of a single role inside its
// container. It must exceed the sum of that role's PairAwait bounds and stay far
// below the outer e2e bound, E2E_TIMEOUT in the Makefile.
const pairInnerTestTimeout = 5 * time.Minute

// RunPair is the TestMain entry point of a package that runs one of its tests
// on two hosts; inside a pair container it runs the selected tests as that
// host's role, and on the test host it does what Main does. opts must be the
// value the test passes to RunPairTest, with SharedDir equal to PairSharedMount;
// a mismatch returns 1.
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
// fails t when either side fails; both come from one compose project and
// exchange data through opts.SharedDir, mounted at the same path in both.
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

type container struct {
	t            *testing.T
	project      string
	owner        string
	started      time.Time
	composeFile  string
	composeFiles []string
	env          []string
	forward      []string
}

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
// service, or nil when coverage is off; it runs as root through compose exec so
// sudo keeps GOCOVERDIR for the commands a test runs through it.
func (c *container) sudoersInstall(service string) []string {
	if !harness.CoverageEnabled() {
		return nil
	}
	return c.composeArgs("exec", "-T", "-u", "root", service, "sh", "-c", sudoersWriteScript)
}

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

// plainCommand wraps a test command for a plain container so the sudoers
// drop-in is written before the test replaces the shell; a compose run has no
// separate exec step to write it from the host.
func plainCommand(argv []string) []string {
	if !harness.CoverageEnabled() {
		return argv
	}
	return append([]string{"sh", "-c", sudoersWriteScript + ` && exec "$@"`, "sh"}, argv...)
}

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

// systemdRebuildLedger records every systemd container rebuild; make e2e
// prints it after the test run, and the Makefile repeats the path.
const systemdRebuildLedger = "/tmp/ape2e-systemd-rebuilds.log"

// waitSystemd waits until service's systemd reports running or degraded. The
// whole wait, including the probe, the at-most-one rebuild and the stop before
// it, shares one systemdReadyBound context; evidence collection after that
// budget has its own bound and does not extend it.
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

func rebuildReason(o systemdBootObservation) string {
	if o.exited {
		return fmt.Sprintf("process 1 exited with code %d and docker logs is empty", o.exitCode)
	}
	return fmt.Sprintf("is-system-running --wait timed out after %s and the state is %q", systemdProbeBound, o.state)
}

func (c *container) failSystemdBoot(service, lastState string) {
	c.logBootEvidence(service)
	c.t.Fatalf("%s to finish booting: not within %s (last state: %s)", service, systemdReadyBound, lastState)
}

// probeSystemd runs one readiness probe under the shared context; a probe that
// expires while the outer context is still alive is the timeout that can
// precede a rebuild.
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

func (c *container) containerID(ctx context.Context, service string) (string, error) {
	cmd := exec.CommandContext(ctx, "docker", c.composeArgs("ps", "-aq", service)...)
	cmd.Env = c.env
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%v: %s", err, strings.TrimSpace(string(out)))
	}
	return strings.TrimSpace(string(out)), nil
}

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
// in place, keeping the project's network; both commands share the readiness
// context, and a failure is logged rather than fatal so the wait continues and
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

// recordRebuild appends one line to systemdRebuildLedger; O_APPEND keeps
// concurrent suites from losing each other's lines.
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

// down tears the project down and removes the network. The one-second stop
// timeout can end a container with exit code 137, which residue cleanup leaves
// alone because this owner is still alive; under coverage the timeout is
// longer, so an instrumented process can flush its counters while it stops.
func (c *container) down() {
	cmd := c.command(c.composeArgs("down", "-v", "--remove-orphans", "--timeout", harness.DownTimeout())...)
	if out, err := cmd.CombinedOutput(); err != nil {
		c.t.Logf("syscontainer: tearing down %s: %v\n%s", c.project, err, strings.TrimSpace(string(out)))
	}
}

// checkCompose reports whether docker and the compose plugin are usable; a host
// without them fails the test rather than skipping it.
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

func randomHex6() string {
	var b [3]byte
	if _, err := rand.Read(b[:]); err == nil {
		return hex.EncodeToString(b[:])
	}
	projectNameSeq++
	return fmt.Sprintf("%06x", projectNameSeq&0xffffff)
}

// WaitFor waits up to the package-wide fallback for cond to return true,
// delegating the polling to harness.Eventually. A condition that makes a tool
// call uses harness.EventuallyPoll with Session.PollCall instead, so each call
// runs its own poll bound in full.
func WaitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	harness.Eventually(t, harness.EventTimeout, what, func() (bool, string) {
		return cond(), ""
	})
}
