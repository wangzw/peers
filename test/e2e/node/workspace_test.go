package node

import (
	"bytes"
	"errors"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func gitTestEnv(t *testing.T) []string {
	t.Helper()
	return []string{
		"HOME=" + t.TempDir(),
		"PATH=" + os.Getenv("PATH"),
		"LANG=C.UTF-8",
		"GIT_CONFIG_NOSYSTEM=1",
	}
}

func gitRemoteURL(t *testing.T, env []string, dir, name string) string {
	t.Helper()
	cmd := exec.Command("git", "-C", dir, "remote", "get-url", name)
	cmd.Env = env
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("node: git remote get-url %s in %s: %v", name, dir, err)
	}
	return strings.TrimSpace(string(out))
}

// TestGitWorkspaceOrigin checks that the git and project workspaces carry the
// requested origin and project configuration, and that a project workspace
// without an origin is not a repository.
func TestGitWorkspaceOrigin(t *testing.T) {
	env := gitTestEnv(t)
	const origin = "https://github.com/example/repo.git"

	t.Run("git workspace", func(t *testing.T) {
		root, err := GitWorkspace(filepath.Join(t.TempDir(), "repo"), origin, env)
		if err != nil {
			t.Fatalf("node: GitWorkspace: %v", err)
		}
		if got := gitRemoteURL(t, env, root, "origin"); got != origin {
			t.Fatalf("node: origin is %q, want %q", got, origin)
		}
	})

	t.Run("project workspace", func(t *testing.T) {
		const projectYAML = "topics:\n  - t1\n  - t2\nauto_join: true\n"
		root, err := ProjectWorkspace(filepath.Join(t.TempDir(), "repo"), projectYAML, origin, env)
		if err != nil {
			t.Fatalf("node: ProjectWorkspace: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(root, projectConfigName))
		if err != nil {
			t.Fatalf("node: reading %s: %v", projectConfigName, err)
		}
		if !bytes.Equal(data, []byte(projectYAML)) {
			t.Fatalf("node: %s is %q, want %q", projectConfigName, data, projectYAML)
		}
		if got := gitRemoteURL(t, env, root, "origin"); got != origin {
			t.Fatalf("node: origin is %q, want %q", got, origin)
		}
	})

	t.Run("project workspace without origin", func(t *testing.T) {
		const projectYAML = "auto_join: false\n"
		root, err := ProjectWorkspace(filepath.Join(t.TempDir(), "repo"), projectYAML, "", env)
		if err != nil {
			t.Fatalf("node: ProjectWorkspace: %v", err)
		}
		data, err := os.ReadFile(filepath.Join(root, projectConfigName))
		if err != nil {
			t.Fatalf("node: reading %s: %v", projectConfigName, err)
		}
		if !bytes.Equal(data, []byte(projectYAML)) {
			t.Fatalf("node: %s is %q, want %q", projectConfigName, data, projectYAML)
		}
		if _, err := os.Stat(filepath.Join(root, ".git")); !errors.Is(err, fs.ErrNotExist) {
			t.Fatalf("node: ProjectWorkspace without origin created a repository (stat .git: %v)", err)
		}
	})
}

// TestPlainWorkspace checks that a plain workspace is an empty directory.
func TestPlainWorkspace(t *testing.T) {
	root, err := PlainWorkspace(filepath.Join(t.TempDir(), "plain"))
	if err != nil {
		t.Fatalf("node: PlainWorkspace: %v", err)
	}
	info, err := os.Stat(root)
	if err != nil {
		t.Fatalf("node: stat %s: %v", root, err)
	}
	if !info.IsDir() {
		t.Fatalf("node: %s is not a directory", root)
	}
	entries, err := os.ReadDir(root)
	if err != nil {
		t.Fatalf("node: reading %s: %v", root, err)
	}
	if len(entries) != 0 {
		t.Fatalf("node: plain workspace is not empty: %v", entries)
	}
}
