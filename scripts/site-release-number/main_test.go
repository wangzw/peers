package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// newRepository creates a repository in a temporary directory with the given
// tags and returns its root.
func newRepository(t *testing.T, tags ...string) string {
	t.Helper()
	dir := t.TempDir()
	gitCommand(t, dir, "init")
	gitCommand(t, dir,
		"-c", "user.name=test",
		"-c", "user.email=test@example.com",
		"-c", "commit.gpgsign=false",
		"commit", "--allow-empty", "-m", "init")
	for _, tag := range tags {
		gitCommand(t, dir, "tag", tag)
	}
	return dir
}

// gitCommand runs one git command in dir and fails the test if it does not
// exit 0.
func gitCommand(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// newContent creates a content root in a temporary directory and writes the
// given files into it.
func newContent(t *testing.T, files map[string]string) string {
	t.Helper()
	root := t.TempDir()
	for rel, body := range files {
		path := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create directory for %s: %v", rel, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", rel, err)
		}
	}
	return root
}

// runTool runs the command over the given trees and returns the exit code and
// everything it wrote to stderr.
func runTool(t *testing.T, contentDir, repoDir string) (int, string) {
	t.Helper()
	var stderr strings.Builder
	code := run([]string{"-content", contentDir, "-repo", repoDir}, &stderr)
	return code, stderr.String()
}

// TestBareTagNumberInBothLanguages checks that a bare tag number in either
// language is reported with its path and line.
func TestBareTagNumberInBothLanguages(t *testing.T) {
	repo := newRepository(t, "v0.6.1")
	content := newContent(t, map[string]string{
		"en/docs/guides/install.md": "# Install\n\nVERSION=0.6.1\n",
		"zh/docs/guides/update.md":  "命令打印 `peers 0.6.1 is up to date`。\n",
	})

	code, stderr := runTool(t, content, repo)
	if code != 1 {
		t.Fatalf("run = %d, want 1; stderr: %s", code, stderr)
	}
	want := "en/docs/guides/install.md:3: 0.6.1\n" +
		"zh/docs/guides/update.md:1: 0.6.1\n"
	if stderr != want {
		t.Fatalf("stderr = %q, want %q", stderr, want)
	}
}

// TestFrontMatterIsScanned checks that a number in front matter is reported.
func TestFrontMatterIsScanned(t *testing.T) {
	repo := newRepository(t, "v0.6.1")
	content := newContent(t, map[string]string{
		"en/page.md": "---\ntitle: Install\nrelease: 0.6.1\n---\n",
		"zh/page.md": "---\ntitle: 安装\n---\n",
	})

	code, stderr := runTool(t, content, repo)
	if code != 1 {
		t.Fatalf("run = %d, want 1; stderr: %s", code, stderr)
	}
	want := "en/page.md:3: 0.6.1\n"
	if stderr != want {
		t.Fatalf("stderr = %q, want %q", stderr, want)
	}
}

// TestVPrefixedNumberPasses checks that a number written as history with its v
// prefix is not reported.
func TestVPrefixedNumberPasses(t *testing.T) {
	repo := newRepository(t, "v0.6.1")
	content := newContent(t, map[string]string{
		"en/page.md": "Requires v0.6.1 or newer.\n",
		"zh/page.md": "要求 v0.6.1 或更新版本。\n",
	})

	code, stderr := runTool(t, content, repo)
	if code != 0 {
		t.Fatalf("run = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

// TestNumberEqualToOneTagOnly checks that a number equal to no tag passes and
// that a prerelease tag does not put its number in the tag set.
func TestNumberEqualToOneTagOnly(t *testing.T) {
	repo := newRepository(t, "v0.6.1", "v0.7.0-rc.1")
	content := newContent(t, map[string]string{
		"en/page.md": "Version 0.6.2 and 0.7.0 are unrelated numbers.\n",
		"zh/page.md": "版本 0.6.2 和 0.7.0 与标签无关。\n",
	})

	code, stderr := runTool(t, content, repo)
	if code != 0 {
		t.Fatalf("run = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

// TestNumberInsideLongerRunIsNotAHit checks that a number inside a longer run
// of digits and dots is not reported.
func TestNumberInsideLongerRunIsNotAHit(t *testing.T) {
	repo := newRepository(t, "v0.6.1")
	content := newContent(t, map[string]string{
		"en/page.md": "10.6.1\n0.6.1.2\n1.0.6.1\n0.6.10\n",
		"zh/page.md": "10.6.1\n0.6.1.2\n1.0.6.1\n0.6.10\n",
	})

	code, stderr := runTool(t, content, repo)
	if code != 0 {
		t.Fatalf("run = %d, want 0; stderr: %s", code, stderr)
	}
	if stderr != "" {
		t.Fatalf("stderr = %q, want empty", stderr)
	}
}

// TestSentenceEndingPeriodIsAHit checks that a full stop after the number does
// not stop it being reported.
func TestSentenceEndingPeriodIsAHit(t *testing.T) {
	repo := newRepository(t, "v0.6.1")
	content := newContent(t, map[string]string{
		"en/page.md": "The installed version is 0.6.1.\n",
		"zh/page.md": "安装的版本是 0.6.1。\n",
	})

	code, stderr := runTool(t, content, repo)
	if code != 1 {
		t.Fatalf("run = %d, want 1; stderr: %s", code, stderr)
	}
	want := "en/page.md:1: 0.6.1\nzh/page.md:1: 0.6.1\n"
	if stderr != want {
		t.Fatalf("stderr = %q, want %q", stderr, want)
	}
}

// TestSortedOutputAcrossFilesAndNumbers checks that hits are sorted by path,
// line and number, and that every number on a line is reported.
func TestSortedOutputAcrossFilesAndNumbers(t *testing.T) {
	repo := newRepository(t, "v0.5.0", "v0.6.1")
	content := newContent(t, map[string]string{
		"en/a/b.md": "0.6.1\n",
		"en/a.md":   "see 0.6.2, 0.6.1 and 0.5.0\n",
		"zh/p.md":   "x\n",
	})

	code, stderr := runTool(t, content, repo)
	if code != 1 {
		t.Fatalf("run = %d, want 1; stderr: %s", code, stderr)
	}
	want := "en/a.md:1: 0.5.0\n" +
		"en/a.md:1: 0.6.1\n" +
		"en/a/b.md:1: 0.6.1\n"
	if stderr != want {
		t.Fatalf("stderr = %q, want %q", stderr, want)
	}
}

// TestNonMarkdownFileIsScanned checks that every regular file under the content
// tree is read, not only Markdown pages.
func TestNonMarkdownFileIsScanned(t *testing.T) {
	repo := newRepository(t, "v0.6.1")
	content := newContent(t, map[string]string{
		"en/data/release.yaml": "release: 0.6.1\n",
		"zh/p.md":              "x\n",
	})

	code, stderr := runTool(t, content, repo)
	if code != 1 {
		t.Fatalf("run = %d, want 1; stderr: %s", code, stderr)
	}
	want := "en/data/release.yaml:1: 0.6.1\n"
	if stderr != want {
		t.Fatalf("stderr = %q, want %q", stderr, want)
	}
}

// TestExitTwoOnInvalidInput checks that bad flags and unreadable input exit 2.
func TestExitTwoOnInvalidInput(t *testing.T) {
	content := newContent(t, map[string]string{
		"en/page.md": "VERSION=0.6.1\n",
		"zh/page.md": "VERSION=0.6.1\n",
	})
	repo := newRepository(t, "v0.6.1")
	noTags := newRepository(t)
	prereleaseOnly := newRepository(t, "v0.7.0-rc.1")
	missingLanguage := newContent(t, map[string]string{
		"en/page.md": "VERSION=0.6.1\n",
	})
	notARepository := t.TempDir()

	tests := []struct {
		name     string
		args     []string
		contains string
	}{
		{"missing content flag", []string{"-repo", repo}, "-content and -repo are required"},
		{"missing repo flag", []string{"-content", content}, "-content and -repo are required"},
		{"unexpected positional argument", []string{"-content", "c", "-repo", "r", "extra"}, "unexpected argument"},
		{"flag without value", []string{"-content", content, "-repo"}, ""},
		{"repository without release tags", []string{"-content", content, "-repo", noTags}, ""},
		{"prerelease tag only", []string{"-content", content, "-repo", prereleaseOnly}, ""},
		{"not a repository", []string{"-content", content, "-repo", notARepository}, ""},
		{"missing language directory", []string{"-content", missingLanguage, "-repo", repo}, ""},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr strings.Builder
			if code := run(tt.args, &stderr); code != 2 {
				t.Fatalf("run(%v) = %d, want 2; stderr: %s", tt.args, code, stderr.String())
			}
			if tt.contains != "" && !strings.Contains(stderr.String(), tt.contains) {
				t.Fatalf("run(%v) stderr = %q, want it to contain %q", tt.args, stderr.String(), tt.contains)
			}
		})
	}
}
