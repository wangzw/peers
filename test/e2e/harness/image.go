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

const dockerProbeBound = 20 * time.Second

var (
	imageOnce sync.Once
	imageTag  string
	imageErr  error
)

// Image builds the container image defined by test/e2e/compose/Dockerfile once
// per test process and returns its tag, which names the image content, the peer
// account's uid and gid, and this test process as owner.
func Image(tb testing.TB) string {
	tb.Helper()

	// Build the binaries before the once, so a failing go build fails this test
	// instead of caching an empty tag for the process.
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

func imageReference(digest string, uid, gid int, o owner) string {
	return fmt.Sprintf("peers-e2e:ape2e-%s-u%d-g%d-%s", o.String(), uid, gid, digest)
}

func imageTagOwnerGone(ref string) bool {
	if i := strings.LastIndexByte(ref, ':'); i >= 0 {
		ref = ref[i+1:]
	}
	return ownerGone(ref)
}

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

func imageExists(tag string) bool {
	cmd := exec.Command("docker", "image", "inspect", tag)
	cmd.Env = os.Environ()
	return cmd.Run() == nil
}

// imageUIDGID returns the uid and gid of the peer account inside the image: those
// of the host test process, with root mapped to 1000, so files under the
// bind-mounted /ctl directory belong to the host user.
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

func composeSourcePath(name string) (string, error) {
	root, err := moduleRoot()
	if err != nil {
		return "", err
	}
	return filepath.Join(root, "test", "e2e", "compose", name), nil
}
