package node

import (
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

const projectConfigName = ".peers.yaml"

// GitWorkspace creates dir when it does not exist, initialises a git repository
// there, adds origin as a remote when it is not empty, and returns the absolute
// path of the workspace. No commit is made.
//
// env is the complete environment of git; it should contain HOME pointing at an
// isolated directory. GIT_CONFIG_NOSYSTEM=1 is added when it is missing to keep
// the system-level git configuration out of the run; the user-level
// configuration is kept out only by that HOME.
func GitWorkspace(dir, origin string, env []string) (string, error) {
	root, err := prepareWorkspaceDir(dir)
	if err != nil {
		return "", err
	}
	if err := gitRun(root, env, "init", "-q"); err != nil {
		return "", err
	}
	if origin != "" {
		if err := gitRun(root, env, "remote", "add", "origin", origin); err != nil {
			return "", err
		}
	}
	return root, nil
}

// ProjectWorkspace creates dir when it does not exist and writes projectYAML
// as .peers.yaml in it. With origin it also initialises a git repository, like
// GitWorkspace; without it the directory stays plain. It returns the absolute
// path of the workspace.
func ProjectWorkspace(dir, projectYAML, origin string, env []string) (string, error) {
	root, err := prepareWorkspaceDir(dir)
	if err != nil {
		return "", err
	}
	path := filepath.Join(root, projectConfigName)
	if err := os.WriteFile(path, []byte(projectYAML), 0o644); err != nil {
		return "", fmt.Errorf("node: writing %s: %w", path, err)
	}
	if origin != "" {
		if err := gitRun(root, env, "init", "-q"); err != nil {
			return "", err
		}
		if err := gitRun(root, env, "remote", "add", "origin", origin); err != nil {
			return "", err
		}
	}
	return root, nil
}

// PlainWorkspace creates dir without a repository or a project configuration
// file and returns its absolute path.
func PlainWorkspace(dir string) (string, error) {
	return prepareWorkspaceDir(dir)
}

func prepareWorkspaceDir(dir string) (string, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return "", fmt.Errorf("node: creating workspace %s: %w", dir, err)
	}
	root, err := filepath.Abs(dir)
	if err != nil {
		return "", fmt.Errorf("node: resolving workspace %s: %w", dir, err)
	}
	return root, nil
}

func gitRun(dir string, env []string, args ...string) error {
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = withGitNoSystem(env)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		command := "git " + strings.Join(args, " ")
		if text := strings.TrimSpace(stderr.String()); text != "" {
			return fmt.Errorf("node: %s: %w: %s", command, err, text)
		}
		return fmt.Errorf("node: %s: %w", command, err)
	}
	return nil
}

// withGitNoSystem never modifies the slice passed in.
func withGitNoSystem(env []string) []string {
	for _, entry := range env {
		if strings.HasPrefix(entry, "GIT_CONFIG_NOSYSTEM=") {
			return env
		}
	}
	out := make([]string, 0, len(env)+1)
	out = append(out, env...)
	return append(out, "GIT_CONFIG_NOSYSTEM=1")
}
