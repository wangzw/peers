package harness

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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
)

// dockerProbeBound bounds the docker availability check.
const dockerProbeBound = 20 * time.Second

var (
	imageOnce sync.Once
	imageTag  string
	imageErr  error
)

// Image builds the container image defined by test/e2e/compose/Dockerfile once
// per test process and returns its tag. The tag names the image content, the
// peer account's uid and gid, and the owner of this test process, as
// peers-e2e:ape2e-<pid>-<start>-u<uid>-g<gid>-<hash>. The owner marker
// lets a cleanup remove only images of runs that are gone, so two suites on
// one host never delete each other's image. The image holds a test build of
// peers, so a container scenario can name a Broker socket through
// PEERS_TEST_SOCKET, and the e2e-node build. The test build carries hooks,
// and each one acts only when its variable is set: PEERS_TEST_SOCKET names
// the Broker socket, PEERS_TEST_UPDATE_API points the updater at a fake
// API, PEERS_TEST_RUNTIME_ROOT and PEERS_TEST_HOME relocate the runtime
// directory and the home directory, and PEERS_TEST_INBOUND_LIMIT_INTERVAL
// lengthens the inbound quota refill interval. With none of them set, the test
// build behaves like the release build. The system Broker and service
// installation unit derives its image from it.
func Image(tb testing.TB) string {
	tb.Helper()

	// Build the two binaries outside the once, so a failing go build ends this
	// test with its own message instead of caching an empty tag for the rest
	// of the process.
	peersBinary := BuildBinary(tb, "linux", runtime.GOARCH, "peers_testenv")
	e2eNode := BuildPackage(tb, "./test/e2e/cmd/e2e-node", "linux", runtime.GOARCH)

	imageOnce.Do(func() {
		imageTag, imageErr = buildImage(peersBinary, e2eNode)
	})
	if imageErr != nil {
		tb.Fatalf("harness: %v", imageErr)
	}
	return imageTag
}

// buildImage copies the Dockerfile and the binaries into a fresh build
// context, derives the tag from their contents, and builds the image when it
// does not exist yet.
func buildImage(peersBinary, e2eNode string) (string, error) {
	if err := checkDocker(); err != nil {
		return "", fmt.Errorf("docker is required for container scenarios: %w", err)
	}

	dir, err := os.MkdirTemp("", "ap-e2e-image-")
	if err != nil {
		return "", fmt.Errorf("creating the image build context: %w", err)
	}
	defer func() { _ = os.RemoveAll(dir) }()

	dockerfilePath, err := composeSourcePath("Dockerfile")
	if err != nil {
		return "", fmt.Errorf("locating the Dockerfile: %w", err)
	}
	dockerfileSource, err := os.ReadFile(dockerfilePath)
	if err != nil {
		return "", fmt.Errorf("reading the Dockerfile: %w", err)
	}

	hash := sha256.New()
	hash.Write(dockerfileSource)
	if err := os.WriteFile(filepath.Join(dir, "Dockerfile"), dockerfileSource, 0o644); err != nil {
		return "", fmt.Errorf("writing the Dockerfile: %w", err)
	}
	for _, file := range []struct{ source, name string }{
		{peersBinary, "peers"},
		{e2eNode, "e2e-node"},
	} {
		data, err := os.ReadFile(file.source)
		if err != nil {
			return "", fmt.Errorf("reading %s: %w", file.source, err)
		}
		hash.Write(data)
		if err := os.WriteFile(filepath.Join(dir, file.name), data, 0o755); err != nil {
			return "", fmt.Errorf("adding %s to the build context: %w", file.name, err)
		}
	}

	uid, gid := imageUIDGID()
	tag := imageReference(hex.EncodeToString(hash.Sum(nil))[:12], uid, gid, currentOwner())
	if imageExists(tag) {
		return tag, nil
	}

	cmd := exec.Command("docker", "build",
		"--build-arg", "UID="+strconv.Itoa(uid),
		"--build-arg", "GID="+strconv.Itoa(gid),
		"-t", tag,
		"-f", filepath.Join(dir, "Dockerfile"),
		dir,
	)
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("docker build: %w\n%s", err, strings.TrimSpace(string(out)))
	}
	return tag, nil
}

// imageReference builds the image reference from the content digest, the peer
// account's uid and gid, and the owner marker of this test process. The tag
// component starts with "ape2e-" so the same owner parser the project and
// runtime-root names use reads it back.
func imageReference(digest string, uid, gid int, o owner) string {
	return fmt.Sprintf("peers-e2e:ape2e-%s-u%d-g%d-%s", o.String(), uid, gid, digest)
}

// imageTagOwnerGone reports whether ref carries an owner marker whose process
// is gone. A reference without the marker, including one built before the
// marker existed, has an unknown owner and is left alone.
func imageTagOwnerGone(ref string) bool {
	if i := strings.LastIndexByte(ref, ':'); i >= 0 {
		ref = ref[i+1:]
	}
	return ownerGone(ref)
}

// checkDocker reports whether the docker command works. A host without docker
// cannot run container scenarios, and the plan asks those tests to fail with a
// reason rather than skip.
func checkDocker() error {
	if _, err := exec.LookPath("docker"); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), dockerProbeBound)
	defer cancel()
	cmd := exec.CommandContext(ctx, "docker", "info")
	cmd.Env = os.Environ()
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("docker info: %w: %s", err, strings.TrimSpace(string(out)))
	}
	return nil
}

// imageExists reports whether the tag is already in the local image store.
func imageExists(tag string) bool {
	cmd := exec.Command("docker", "image", "inspect", tag)
	cmd.Env = os.Environ()
	return cmd.Run() == nil
}

// imageUIDGID returns the uid and gid of the peer account inside the image.
// They match the host test process, so files the account writes under the
// bind-mounted /ctl directory belong to the host user. A root test process
// maps to 1000.
func imageUIDGID() (int, int) {
	uid, gid := os.Getuid(), os.Getgid()
	if uid == 0 {
		uid = 1000
	}
	if gid == 0 {
		gid = 1000
	}
	return uid, gid
}

// composeSourcePath returns the path of a file under test/e2e/compose.
func composeSourcePath(name string) (string, error) {
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "test", "e2e", "compose", name), nil
}
