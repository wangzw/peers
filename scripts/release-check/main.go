// Command release-check checks the repository state a release of the given
// version requires: the version form, the existing tags, origin/main, the
// CHANGELOG.md date, the Makefile release-notes version and a clean working
// tree. It reads git, the Makefile and CHANGELOG.md only; it never fetches and
// never writes.
package main

import (
	"bytes"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"time"
)

// versionRE is the accepted release version form. Each component is a decimal
// number without a leading zero, so two components of equal length compare
// numerically as strings.
var versionRE = regexp.MustCompile(`^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$`)

// assignmentRE matches a Makefile assignment to RELEASE_NOTES_VERSION, with one
// of the =, :=, ?= and += operators and any of the override and export prefixes.
var assignmentRE = regexp.MustCompile(`^(?:(?:override|export)\s+)*RELEASE_NOTES_VERSION\s*(\?=|:=|\+=|=)\s*(.*)$`)

// changelogHeadingRE matches the level-two version heading scripts/release-notes
// extracts: the version in brackets and the date after " - ".
var changelogHeadingRE = regexp.MustCompile(`^## \[([^\]]+)\](?: - (.*))?$`)

// originMain is the ref HEAD must be contained in.
const originMain = "refs/remotes/origin/main"

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

// run is the testable entry point. It returns 0 when every precondition holds,
// 1 when any fails, and 2 for a missing or unknown flag or a positional
// argument. Each failed precondition is reported on its own line.
func run(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("release-check", flag.ContinueOnError)
	fs.SetOutput(stderr)
	version := fs.String("version", "", "release version to check, in the form vX.Y.Z")
	repo := fs.String("repo", ".", "repository whose HEAD is checked")
	if err := fs.Parse(args); err != nil {
		// flag has already written the error and usage to stderr.
		return 2
	}
	if fs.NArg() > 0 {
		writef(stderr, "release-check: unexpected positional arguments: %s\n",
			strings.Join(fs.Args(), " "))
		return 2
	}
	if *version == "" {
		writef(stderr, "release-check: -version is required\n")
		return 2
	}

	problems := check(*repo, *version)
	for _, problem := range problems {
		writef(stderr, "release-check: %s\n", problem)
	}
	if len(problems) > 0 {
		return 1
	}
	return 0
}

// writef writes a diagnostic. A failed write leaves nothing better to report it
// to, so the error is deliberately dropped; the exit code still reports the
// failure that produced the diagnostic.
func writef(w io.Writer, format string, args ...any) {
	_, _ = fmt.Fprintf(w, format, args...)
}

// check returns one message per failed precondition. The messages carry no
// prefix; run adds it. Checks that compare against the version are skipped when
// its form is wrong, because there is no meaningful value to compare.
func check(repo, version string) []string {
	g := git{dir: repo}
	head, err := g.run("rev-parse", "--verify", "HEAD^{commit}")
	if err != nil {
		return []string{fmt.Sprintf("cannot resolve HEAD in %s: %s", repo, err)}
	}

	var problems []string
	want, valid := parseVersion(version)
	if !valid {
		problems = append(problems, fmt.Sprintf("version %q is not of the form vX.Y.Z", version))
	} else {
		problems = append(problems, tagProblems(g, head, version, want)...)
	}
	problems = append(problems, originMainProblems(g)...)
	if valid {
		problems = append(problems, makefileProblems(repo, version)...)
		problems = append(problems, changelogProblems(repo, version)...)
	}
	problems = append(problems, workingTreeProblems(g)...)
	return problems
}

// tagProblems returns the tags that VERSION is not higher than, and a tag equal
// to VERSION that does not peel to HEAD. Tags of another form are ignored.
func tagProblems(g git, head, version string, want releaseVersion) []string {
	out, err := g.run("for-each-ref", "--format=%(refname:strip=2)", "refs/tags")
	if err != nil {
		return []string{fmt.Sprintf("cannot list tags: %s", err)}
	}
	var problems []string
	for _, tag := range strings.Split(out, "\n") {
		tag = strings.TrimSpace(tag)
		if tag == "" {
			continue
		}
		got, ok := parseVersion(tag)
		if !ok {
			continue
		}
		switch got.compare(want) {
		case 1:
			problems = append(problems, fmt.Sprintf("version %s is not higher than existing tag %s", version, tag))
		case 0:
			commit, err := g.run("rev-parse", "--verify", tag+"^{commit}")
			if err != nil {
				problems = append(problems, fmt.Sprintf("cannot resolve tag %s: %s", tag, err))
				continue
			}
			if commit != head {
				problems = append(problems, fmt.Sprintf("tag %s already exists and does not point at HEAD", tag))
			}
		}
	}
	return problems
}

// originMainProblems reports a missing origin/main or a HEAD outside it.
func originMainProblems(g git) []string {
	if _, err := g.run("rev-parse", "--verify", originMain); err != nil {
		return []string{fmt.Sprintf("%s does not exist; fetch origin before checking", originMain)}
	}
	if _, err := g.run("merge-base", "--is-ancestor", "HEAD", originMain); err != nil {
		return []string{fmt.Sprintf("HEAD is not an ancestor of %s", originMain)}
	}
	return nil
}

// makefileProblems compares the value assigned to RELEASE_NOTES_VERSION in the
// Makefile text with version.
func makefileProblems(repo, version string) []string {
	path := filepath.Join(repo, "Makefile")
	data, err := os.ReadFile(path)
	if err != nil {
		return []string{fmt.Sprintf("cannot read %s: %s", path, err)}
	}
	value, count := releaseNotesVersion(string(data))
	switch {
	case count == 0:
		return []string{fmt.Sprintf("%s has no RELEASE_NOTES_VERSION assignment", path)}
	case count > 1:
		return []string{fmt.Sprintf("%s assigns RELEASE_NOTES_VERSION on %d lines; it must be assigned on exactly one line", path, count)}
	case value != version:
		return []string{fmt.Sprintf("%s sets RELEASE_NOTES_VERSION to %q, not %q", path, value, version)}
	}
	return nil
}

// releaseNotesVersion returns the value of the first RELEASE_NOTES_VERSION
// assignment in Makefile text and the number of assignment lines. Recipe lines
// and comments are not assignments.
func releaseNotesVersion(text string) (string, int) {
	value := ""
	count := 0
	for _, line := range strings.Split(text, "\n") {
		if strings.HasPrefix(line, "\t") {
			continue
		}
		m := assignmentRE.FindStringSubmatch(strings.TrimSuffix(line, "\r"))
		if m == nil {
			continue
		}
		count++
		if count == 1 {
			value = strings.TrimSpace(stripMakeComment(m[2]))
		}
	}
	return value, count
}

// stripMakeComment removes a comment from a Makefile value, honouring a
// backslash-escaped #.
func stripMakeComment(s string) string {
	for i := 0; i < len(s); i++ {
		if s[i] == '#' && (i == 0 || s[i-1] != '\\') {
			return s[:i]
		}
	}
	return s
}

// changelogProblems checks that the CHANGELOG.md section for version carries a
// valid YYYY-MM-DD calendar date.
func changelogProblems(repo, version string) []string {
	path := filepath.Join(repo, "CHANGELOG.md")
	data, err := os.ReadFile(path)
	if err != nil {
		return []string{fmt.Sprintf("cannot read %s: %s", path, err)}
	}
	want := strings.TrimPrefix(version, "v")
	text := strings.ReplaceAll(string(data), "\r\n", "\n")
	for _, line := range strings.Split(text, "\n") {
		m := changelogHeadingRE.FindStringSubmatch(line)
		if m == nil || m[1] != want {
			continue
		}
		date := strings.TrimSpace(m[2])
		if date == "" {
			return []string{fmt.Sprintf("%s has no date for version %s", path, want)}
		}
		if _, err := time.Parse("2006-01-02", date); err != nil {
			return []string{fmt.Sprintf("%s dates version %s %q, not a YYYY-MM-DD calendar date", path, want, date)}
		}
		return nil
	}
	return []string{fmt.Sprintf("%s has no section for version %s", path, want)}
}

// workingTreeProblems reports modified, staged and untracked files. Untracked
// files are filtered only by tracked .gitignore files: core.excludesFile and
// .git/info/exclude belong to one machine, and a file they hide would still be
// in the tree the tag is cut from. An untracked .gitignore is itself reported,
// because git applies its patterns while listing the other untracked files.
// Index entries marked skip-worktree or assume-unchanged are reported too: the
// marks, like the personal exclude rules, belong to one machine, and the check
// runs on content the tag does not contain.
func workingTreeProblems(g git) []string {
	status, err := g.run("status", "--porcelain", "--untracked-files=no")
	if err != nil {
		return []string{fmt.Sprintf("cannot read the working tree status: %s", err)}
	}
	untracked, err := g.run("ls-files", "--others", "--exclude-per-directory=.gitignore")
	if err != nil {
		return []string{fmt.Sprintf("cannot list untracked files: %s", err)}
	}
	ignoreFiles, err := g.run("ls-files", "--others", "--", ":(glob)**/.gitignore")
	if err != nil {
		return []string{fmt.Sprintf("cannot list untracked .gitignore files: %s", err)}
	}
	flagged, err := g.run("ls-files", "-v")
	if err != nil {
		return []string{fmt.Sprintf("cannot read the index flags: %s", err)}
	}
	var dirty []string
	seen := map[string]bool{}
	add := func(line string) {
		if line == "" || seen[line] {
			return
		}
		seen[line] = true
		dirty = append(dirty, line)
	}
	for _, out := range []string{status, untracked, ignoreFiles} {
		for _, line := range strings.Split(out, "\n") {
			add(line)
		}
	}
	for _, line := range strings.Split(flagged, "\n") {
		// git ls-files -v writes one status letter and the path: S marks
		// skip-worktree, a lowercase letter marks assume-unchanged.
		if len(line) < 3 || line[1] != ' ' {
			continue
		}
		if tag := line[0]; tag == 'S' || (tag >= 'a' && tag <= 'z') {
			add(line[2:])
		}
	}
	if len(dirty) == 0 {
		return nil
	}
	return []string{fmt.Sprintf("the working tree is dirty: %s", strings.Join(dirty, ", "))}
}

// releaseVersion is a parsed vX.Y.Z version. Its components are kept as their
// decimal text without leading zeros.
type releaseVersion struct {
	parts [3]string
}

// parseVersion parses s if it has the vX.Y.Z form.
func parseVersion(s string) (releaseVersion, bool) {
	m := versionRE.FindStringSubmatch(s)
	if m == nil {
		return releaseVersion{}, false
	}
	return releaseVersion{parts: [3]string{m[1], m[2], m[3]}}, true
}

// compare returns -1, 0 or 1 as v is lower than, equal to or higher than other.
func (v releaseVersion) compare(other releaseVersion) int {
	for i := range v.parts {
		if c := compareDecimal(v.parts[i], other.parts[i]); c != 0 {
			return c
		}
	}
	return 0
}

// compareDecimal compares two decimal numbers without leading zeros.
func compareDecimal(a, b string) int {
	if len(a) != len(b) {
		if len(a) < len(b) {
			return -1
		}
		return 1
	}
	return strings.Compare(a, b)
}

// git runs git subcommands in one repository.
type git struct {
	dir string
}

// run executes a git subcommand and returns its trimmed standard output. An
// error carries the first line of git's standard error. The caller parses the
// output by line, so the user's column configuration is turned off.
func (g git) run(args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", g.dir, "-c", "column.ui=never"}, args...)...)
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		msg := strings.TrimSpace(stderr.String())
		if msg == "" {
			msg = err.Error()
		}
		return "", errors.New(firstLine(msg))
	}
	return strings.TrimSpace(stdout.String()), nil
}

// firstLine returns the text before the first newline.
func firstLine(s string) string {
	if i := strings.IndexByte(s, '\n'); i >= 0 {
		return s[:i]
	}
	return s
}
