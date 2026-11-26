//go:build e2e

package syscontainer

import (
	"os"
	"path/filepath"
	"testing"
)

// ReleaseArtifactPath returns the path under which a staged artifact is visible
// to the caller: inside the container the read-only mount path, on the host the
// directory next to the test binary.
func ReleaseArtifactPath(name string) string {
	if InContainer() {
		return filepath.Join(testBinaryPath, name)
	}
	return filepath.Join(filepath.Dir(testBinary), name)
}

// StageReleaseArtifact makes data available to the container under name and
// returns the path the caller uses; name must be a plain file name because
// compose mounts one directory. On the host the file is written next to the
// test binary with mode, and inside the container only its path is returned.
func StageReleaseArtifact(t testing.TB, name string, data []byte, mode os.FileMode) string {
	t.Helper()

	if name == "" || name != filepath.Base(name) || name == "." || name == ".." {
		t.Fatalf("syscontainer: artifact name %q is not a plain file name", name)
	}
	if InContainer() {
		return ReleaseArtifactPath(name)
	}
	if testBinary == "" {
		t.Fatalf("syscontainer: Main was not called, so artifacts cannot be staged")
	}

	dir := filepath.Dir(testBinary)
	tmp, err := os.CreateTemp(dir, ".stage-")
	if err != nil {
		t.Fatalf("syscontainer: staging %s: %v", name, err)
	}
	tmpName := tmp.Name()
	cleanup := func() { _ = os.Remove(tmpName) }

	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		cleanup()
		t.Fatalf("syscontainer: staging %s: %v", name, err)
	}
	if err := tmp.Close(); err != nil {
		cleanup()
		t.Fatalf("syscontainer: staging %s: %v", name, err)
	}
	if err := os.Chmod(tmpName, mode); err != nil {
		cleanup()
		t.Fatalf("syscontainer: staging %s: %v", name, err)
	}
	if err := os.Rename(tmpName, ReleaseArtifactPath(name)); err != nil {
		cleanup()
		t.Fatalf("syscontainer: staging %s: %v", name, err)
	}
	return ReleaseArtifactPath(name)
}

// CopyReleaseArtifact writes a staged artifact to path; the staged directory is
// read-only, so a test that needs a writable binary or a root-owned copy reads
// it from there and creates its own. The caller handles ownership afterwards.
func CopyReleaseArtifact(t testing.TB, name, path string, mode os.FileMode) {
	t.Helper()

	data, err := os.ReadFile(ReleaseArtifactPath(name))
	if err != nil {
		t.Fatalf("syscontainer: reading staged artifact %s: %v", name, err)
	}
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("syscontainer: creating the directory of %s: %v", path, err)
	}
	if err := os.WriteFile(path, data, mode); err != nil {
		t.Fatalf("syscontainer: writing %s: %v", path, err)
	}
	if err := os.Chmod(path, mode); err != nil {
		t.Fatalf("syscontainer: setting the mode of %s: %v", path, err)
	}
}
