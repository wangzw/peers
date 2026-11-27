package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

// fixture is a temporary repository prepared for one test.
type fixture struct {
	dir   string
	first string // the commit before the release preparation
	head  string // the release preparation commit
}

// isolateGit keeps git away from the developer's configuration and fixes the
// commit identity, so the fixtures behave the same on every machine.
func isolateGit(t *testing.T) {
	t.Helper()
	t.Setenv("GIT_CONFIG_NOSYSTEM", "1")
	t.Setenv("GIT_CONFIG_GLOBAL", os.DevNull)
	t.Setenv("GIT_AUTHOR_NAME", "release-check")
	t.Setenv("GIT_AUTHOR_EMAIL", "release-check@example.com")
	t.Setenv("GIT_COMMITTER_NAME", "release-check")
	t.Setenv("GIT_COMMITTER_EMAIL", "release-check@example.com")
	t.Setenv("GIT_TERMINAL_PROMPT", "0")
}

// runGit runs one git command in dir and returns its trimmed output.
func runGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// newFixture creates an empty repository on main.
func newFixture(t *testing.T) *fixture {
	t.Helper()
	isolateGit(t)
	dir := t.TempDir()
	runGit(t, dir, "init", "--quiet", "--initial-branch=main")
	return &fixture{dir: dir}
}

// write writes one file in the repository.
func (f *fixture) write(t *testing.T, name, content string) {
	t.Helper()
	path := filepath.Join(f.dir, name)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		t.Fatalf("creating the directory for %s: %v", name, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("writing %s: %v", name, err)
	}
}

// commit stages everything and commits it, returning the new commit.
func (f *fixture) commit(t *testing.T, message string) string {
	t.Helper()
	runGit(t, f.dir, "add", "-A")
	runGit(t, f.dir, "commit", "--quiet", "-m", message)
	return runGit(t, f.dir, "rev-parse", "HEAD")
}

// setOriginMain points the fake origin/main ref at commit.
func (f *fixture) setOriginMain(t *testing.T, commit string) {
	t.Helper()
	runGit(t, f.dir, "update-ref", "refs/remotes/origin/main", commit)
}

// prepareWith builds a repository prepared for release v0.6.2: the first commit
// holds the previous state with a v0.6.0 tag, the second writes makefile as the
// release Makefile, and origin/main points at the second commit.
func prepareWith(t *testing.T, makefile string) *fixture {
	t.Helper()
	return prepareFiles(t, makefile, "## [0.6.2] - 2026-12-01\n\nNext release.\n")
}

// prepareFiles is prepareWith with the CHANGELOG.md written by the second commit.
func prepareFiles(t *testing.T, makefile, changelog string) *fixture {
	t.Helper()
	f := newFixture(t)
	f.write(t, "Makefile", "RELEASE_NOTES_VERSION ?= v0.6.1\n")
	f.write(t, "CHANGELOG.md", "## [0.6.1] - 2026-11-01\n\nPrevious release.\n")
	f.write(t, ".gitignore", "dist/\n")
	f.first = f.commit(t, "the previous release")
	runGit(t, f.dir, "tag", "-a", "v0.6.0", "-m", "previous release", f.first)
	f.write(t, "Makefile", makefile)
	f.write(t, "CHANGELOG.md", changelog)
	f.head = f.commit(t, "prepare v0.6.2")
	f.setOriginMain(t, f.head)
	return f
}

// prepare builds the passing fixture.
func prepare(t *testing.T) *fixture {
	t.Helper()
	return prepareWith(t, "RELEASE_NOTES_VERSION ?= v0.6.2\n")
}

// breakGitSubcommand puts a git wrapper first on PATH that fails when subcommand
// is invoked, leaving every other subcommand to the real git.
func breakGitSubcommand(t *testing.T, subcommand string) {
	t.Helper()
	real, err := exec.LookPath("git")
	if err != nil {
		t.Fatalf("looking up git: %v", err)
	}
	script := "#!/bin/sh\n" +
		"for arg in \"$@\"; do\n" +
		"\tif [ \"$arg\" = '" + subcommand + "' ]; then\n" +
		"\t\techo 'simulated failure' >&2\n" +
		"\t\texit 1\n" +
		"\tfi\n" +
		"done\n" +
		"exec '" + real + "' \"$@\"\n"
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "git"), []byte(script), 0o755); err != nil {
		t.Fatalf("writing the git wrapper: %v", err)
	}
	t.Setenv("PATH", dir+string(os.PathListSeparator)+os.Getenv("PATH"))
}

// TestRun checks the exit code and the reported problems for a passing release
// and for each precondition failing on its own.
func TestRun(t *testing.T) {
	tests := []struct {
		name       string
		setup      func(t *testing.T) *fixture
		version    string
		wantExit   int
		wantCount  int
		wantPhrase []string
	}{
		{
			name:     "passes",
			setup:    prepare,
			version:  "v0.6.2",
			wantExit: 0,
		},
		{
			name: "passes with an annotated tag at HEAD",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "tag", "-a", "v0.6.2", "-m", "release", f.head)
				return f
			},
			version:  "v0.6.2",
			wantExit: 0,
		},
		{
			name: "passes with a lightweight tag at HEAD",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "tag", "v0.6.2", f.head)
				return f
			},
			version:  "v0.6.2",
			wantExit: 0,
		},
		{
			name: "ignores tags of another form",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "tag", "-a", "v0.7", "-m", "other form", f.first)
				runGit(t, f.dir, "tag", "-a", "nightly", "-m", "other form", f.first)
				return f
			},
			version:  "v0.6.2",
			wantExit: 0,
		},
		{
			name:       "refuses a version without v",
			setup:      prepare,
			version:    "0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"not of the form vX.Y.Z"},
		},
		{
			name:       "refuses a prerelease version",
			setup:      prepare,
			version:    "v0.6.2-rc.1",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"not of the form vX.Y.Z"},
		},
		{
			name:       "refuses a leading zero",
			setup:      prepare,
			version:    "v0.6.02",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"not of the form vX.Y.Z"},
		},
		{
			name: "refuses a tag equal to the version that is not at HEAD",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "tag", "-a", "v0.6.2", "-m", "old release", f.first)
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"tag v0.6.2 already exists and does not point at HEAD"},
		},
		{
			name: "reports a tag that does not peel to a commit",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				tree := runGit(t, f.dir, "rev-parse", "HEAD^{tree}")
				runGit(t, f.dir, "tag", "v0.6.2", tree)
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"cannot resolve tag v0.6.2"},
		},
		{
			name: "refuses a version lower than an existing tag",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "tag", "-a", "v0.7.0", "-m", "newer release", f.first)
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"not higher than existing tag v0.7.0"},
		},
		{
			name: "reads the tag list despite a column configuration",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "config", "column.ui", "always")
				runGit(t, f.dir, "tag", "-a", "v0.7.0", "-m", "newer release", f.first)
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"not higher than existing tag v0.7.0"},
		},
		{
			name: "refuses a HEAD outside origin/main",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				f.setOriginMain(t, f.first)
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"HEAD is not an ancestor of refs/remotes/origin/main"},
		},
		{
			name: "refuses a missing origin/main",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "update-ref", "-d", "refs/remotes/origin/main")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"refs/remotes/origin/main does not exist"},
		},
		{
			name:       "refuses a Makefile left at the previous version",
			setup:      func(t *testing.T) *fixture { return prepareWith(t, "RELEASE_NOTES_VERSION ?= v0.6.1\n") },
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{`sets RELEASE_NOTES_VERSION to "v0.6.1", not "v0.6.2"`},
		},
		{
			name:       "refuses a Makefile without the assignment",
			setup:      func(t *testing.T) *fixture { return prepareWith(t, "VERSION ?= v0.6.2\n") },
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"has no RELEASE_NOTES_VERSION assignment"},
		},
		{
			name: "refuses a repeated RELEASE_NOTES_VERSION assignment",
			setup: func(t *testing.T) *fixture {
				return prepareWith(t, "RELEASE_NOTES_VERSION ?= v0.6.2\nRELEASE_NOTES_VERSION := v0.6.1\n")
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"assigns RELEASE_NOTES_VERSION on 2 lines"},
		},
		{
			name: "reports a Makefile that cannot be read",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "rm", "--quiet", "Makefile")
				head := f.commit(t, "drop the Makefile")
				f.setOriginMain(t, head)
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"cannot read", "Makefile"},
		},
		{
			name: "reports a CHANGELOG.md that cannot be read",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "rm", "--quiet", "CHANGELOG.md")
				head := f.commit(t, "drop the changelog")
				f.setOriginMain(t, head)
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"cannot read", "CHANGELOG.md"},
		},
		{
			name: "refuses a changelog date of TBD",
			setup: func(t *testing.T) *fixture {
				return prepareFiles(t, "RELEASE_NOTES_VERSION ?= v0.6.2\n", "## [0.6.2] - TBD\n\nNext release.\n")
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"not a YYYY-MM-DD calendar date", "TBD"},
		},
		{
			name: "refuses an impossible changelog date",
			setup: func(t *testing.T) *fixture {
				return prepareFiles(t, "RELEASE_NOTES_VERSION ?= v0.6.2\n", "## [0.6.2] - 2026-13-01\n\nNext release.\n")
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"not a YYYY-MM-DD calendar date", "2026-13-01"},
		},
		{
			name: "refuses a changelog without the version section",
			setup: func(t *testing.T) *fixture {
				return prepareFiles(t, "RELEASE_NOTES_VERSION ?= v0.6.2\n", "## [0.6.1] - 2026-11-01\n\nPrevious release.\n")
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"has no section for version 0.6.2"},
		},
		{
			name: "refuses a modified tracked file",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				f.write(t, ".gitignore", "dist/\ncoverage/\n")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"the working tree is dirty"},
		},
		{
			name: "refuses an untracked file",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				f.write(t, "notes.txt", "untracked\n")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"the working tree is dirty", "notes.txt"},
		},
		{
			name: "refuses a staged new file",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				f.write(t, "notes.txt", "staged\n")
				runGit(t, f.dir, "add", "notes.txt")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"the working tree is dirty", "notes.txt"},
		},
		{
			name: "refuses a self-ignoring untracked .gitignore",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				f.write(t, "extra/.gitignore", "*\n")
				f.write(t, "extra/f", "untracked\n")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"the working tree is dirty", "extra/.gitignore"},
		},
		{
			name: "refuses a modified skip-worktree file",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "update-index", "--skip-worktree", ".gitignore")
				f.write(t, ".gitignore", "dist/\ncoverage/\n")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"the working tree is dirty", ".gitignore"},
		},
		{
			name: "refuses a modified assume-unchanged file",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				runGit(t, f.dir, "update-index", "--assume-unchanged", ".gitignore")
				f.write(t, ".gitignore", "dist/\ncoverage/\n")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"the working tree is dirty", ".gitignore"},
		},
		{
			name: "refuses a file hidden by core.excludesFile",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				excludes := filepath.Join(t.TempDir(), "ignore")
				if err := os.WriteFile(excludes, []byte("notes.txt\n"), 0o644); err != nil {
					t.Fatalf("writing the global excludes file: %v", err)
				}
				runGit(t, f.dir, "config", "core.excludesFile", excludes)
				f.write(t, "notes.txt", "untracked\n")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"the working tree is dirty", "notes.txt"},
		},
		{
			name: "reports a repository without HEAD",
			setup: func(t *testing.T) *fixture {
				isolateGit(t)
				return &fixture{dir: t.TempDir()}
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"cannot resolve HEAD"},
		},
		{
			name: "reports a failed tag list",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				breakGitSubcommand(t, "for-each-ref")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"cannot list tags"},
		},
		{
			name: "reports a failed working tree status",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				breakGitSubcommand(t, "status")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"cannot read the working tree status"},
		},
		{
			name: "reports a failed untracked file list",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				breakGitSubcommand(t, "ls-files")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  1,
			wantPhrase: []string{"cannot list untracked files"},
		},
		{
			name: "accepts an ignored file",
			setup: func(t *testing.T) *fixture {
				f := prepare(t)
				f.write(t, "dist/output.txt", "ignored\n")
				return f
			},
			version:  "v0.6.2",
			wantExit: 0,
		},
		{
			name: "reports every failed precondition",
			setup: func(t *testing.T) *fixture {
				f := prepareWith(t, "RELEASE_NOTES_VERSION ?= v0.6.1\n")
				f.write(t, "notes.txt", "untracked\n")
				return f
			},
			version:    "v0.6.2",
			wantExit:   1,
			wantCount:  2,
			wantPhrase: []string{"sets RELEASE_NOTES_VERSION", "the working tree is dirty"},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f := tt.setup(t)
			var stderr bytes.Buffer
			exit := run([]string{"-version", tt.version, "-repo", f.dir}, &stderr)
			if exit != tt.wantExit {
				t.Fatalf("run() = %d, want %d (stderr: %s)", exit, tt.wantExit, stderr.String())
			}

			lines := nonEmptyLines(stderr.String())
			if len(lines) != tt.wantCount {
				t.Fatalf("run() reported %d lines, want %d: %q", len(lines), tt.wantCount, stderr.String())
			}
			for _, line := range lines {
				if !strings.HasPrefix(line, "release-check: ") {
					t.Errorf("line %q does not start with %q", line, "release-check: ")
				}
			}
			for _, phrase := range tt.wantPhrase {
				if !strings.Contains(stderr.String(), phrase) {
					t.Errorf("stderr %q does not contain %q", stderr.String(), phrase)
				}
			}
		})
	}
}

// TestRunBadFlags checks that a missing or unknown flag and a positional
// argument exit 2.
func TestRunBadFlags(t *testing.T) {
	tests := []struct {
		name string
		args []string
	}{
		{name: "missing version", args: nil},
		{name: "unknown flag", args: []string{"-bogus"}},
		{name: "missing flag value", args: []string{"-version"}},
		{name: "positional argument", args: []string{"-version", "v0.6.2", "extra"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var stderr bytes.Buffer
			if exit := run(tt.args, &stderr); exit != 2 {
				t.Fatalf("run(%q) = %d, want 2 (stderr: %s)", tt.args, exit, stderr.String())
			}
			if stderr.Len() == 0 {
				t.Errorf("run(%q) wrote no diagnostic to stderr, want one", tt.args)
			}
		})
	}
}

// TestReleaseNotesVersion checks that each assignment form is read, that a
// comment is stripped, that every assignment line is counted, and that a recipe
// line or a missing assignment is not.
func TestReleaseNotesVersion(t *testing.T) {
	tests := []struct {
		name  string
		text  string
		want  string
		count int
	}{
		{name: "equals", text: "RELEASE_NOTES_VERSION = v0.6.2\n", want: "v0.6.2", count: 1},
		{name: "colon equals", text: "RELEASE_NOTES_VERSION := v0.6.2\n", want: "v0.6.2", count: 1},
		{name: "question equals", text: "RELEASE_NOTES_VERSION ?= v0.6.2\n", want: "v0.6.2", count: 1},
		{name: "plus equals", text: "RELEASE_NOTES_VERSION += x\n", want: "x", count: 1},
		{name: "override", text: "override RELEASE_NOTES_VERSION := v0.6.2\n", want: "v0.6.2", count: 1},
		{name: "export", text: "export RELEASE_NOTES_VERSION ?= v0.6.2\n", want: "v0.6.2", count: 1},
		{name: "override export", text: "override export RELEASE_NOTES_VERSION = v0.6.2\n", want: "v0.6.2", count: 1},
		{name: "trailing comment", text: "RELEASE_NOTES_VERSION ?= v0.6.2 # bump\n", want: "v0.6.2", count: 1},
		{name: "trailing spaces", text: "RELEASE_NOTES_VERSION ?= v0.6.2   \n", want: "v0.6.2", count: 1},
		{name: "empty value", text: "RELEASE_NOTES_VERSION ?=\n", want: "", count: 1},
		{name: "repeated", text: "RELEASE_NOTES_VERSION ?= v0.6.2\nRELEASE_NOTES_VERSION := v0.6.1\n", want: "v0.6.2", count: 2},
		{name: "recipe line", text: "target:\n\tgo run ./scripts/release-notes -version $(RELEASE_NOTES_VERSION)\n", count: 0},
		{name: "missing", text: "VERSION ?= v0.6.2\n", count: 0},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, count := releaseNotesVersion(tt.text)
			if count != tt.count {
				t.Fatalf("releaseNotesVersion() count = %d, want %d", count, tt.count)
			}
			if got != tt.want {
				t.Errorf("releaseNotesVersion() = %q, want %q", got, tt.want)
			}
		})
	}
}

// TestParseVersion checks the accepted and refused version forms.
func TestParseVersion(t *testing.T) {
	tests := []struct {
		version string
		wantOK  bool
	}{
		{version: "v0.6.2", wantOK: true},
		{version: "v0.0.0", wantOK: true},
		{version: "v10.20.30", wantOK: true},
		{version: "v18446744073709551616.0.0", wantOK: true},
		{version: "0.6.2"},
		{version: "v0.6"},
		{version: "v0.6.2.1"},
		{version: "v0.6.2-rc.1"},
		{version: "v0.6.02"},
		{version: "v00.6.2"},
		{version: "v"},
	}

	for _, tt := range tests {
		t.Run(tt.version, func(t *testing.T) {
			_, ok := parseVersion(tt.version)
			if ok != tt.wantOK {
				t.Errorf("parseVersion(%q) ok = %t, want %t", tt.version, ok, tt.wantOK)
			}
		})
	}
}

// TestCompareReleaseVersions checks semantic ordering, including components
// longer than any fixed-width integer.
func TestCompareReleaseVersions(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{a: "v0.6.2", b: "v0.6.1", want: 1},
		{a: "v0.6.2", b: "v0.6.2", want: 0},
		{a: "v0.6.2", b: "v0.7.0", want: -1},
		{a: "v0.10.0", b: "v0.9.9", want: 1},
		{a: "v1.0.0", b: "v0.99.99", want: 1},
		{a: "v18446744073709551616.0.0", b: "v18446744073709551615.0.0", want: 1},
	}

	for _, tt := range tests {
		t.Run(tt.a+"_"+tt.b, func(t *testing.T) {
			a, ok := parseVersion(tt.a)
			if !ok {
				t.Fatalf("parseVersion(%q) refused a valid version", tt.a)
			}
			b, ok := parseVersion(tt.b)
			if !ok {
				t.Fatalf("parseVersion(%q) refused a valid version", tt.b)
			}
			if got := a.compare(b); got != tt.want {
				t.Errorf("%s compared with %s = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// nonEmptyLines splits text into lines without their line endings, dropping
// blank lines.
func nonEmptyLines(text string) []string {
	var lines []string
	for _, line := range strings.Split(text, "\n") {
		if line = strings.TrimSpace(line); line != "" {
			lines = append(lines, line)
		}
	}
	return lines
}
