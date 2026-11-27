// Command site-release-number reports pages that write a release number
// without its v prefix when the number names a vX.Y.Z tag of the repository.
//
// A page that fetches the release its build installs takes the number from the
// release shortcode instead of writing it, and a release named as history keeps
// its v prefix. Every regular file under the en and zh content trees is read,
// front matter included. The check reads the working tree only.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

// run executes the check and returns the process exit code: 1 when a page
// writes a bare tag number, 2 on bad flags or unreadable input, 0 otherwise.
func run(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("site-release-number", flag.ContinueOnError)
	fs.SetOutput(stderr)
	contentDir := fs.String("content", "", "content root holding the en and zh trees")
	repoDir := fs.String("repo", "", "repository root whose tags the numbers are compared with")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "site-release-number: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *contentDir == "" || *repoDir == "" {
		_, _ = fmt.Fprintln(stderr, "site-release-number: -content and -repo are required")
		return 2
	}

	hits, err := check(*contentDir, *repoDir)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "site-release-number: %v\n", err)
		return 2
	}
	sort.Slice(hits, func(i, j int) bool {
		if hits[i].path != hits[j].path {
			return hits[i].path < hits[j].path
		}
		if hits[i].line != hits[j].line {
			return hits[i].line < hits[j].line
		}
		return hits[i].number < hits[j].number
	})
	for _, hit := range hits {
		_, _ = fmt.Fprintf(stderr, "%s:%d: %s\n", hit.path, hit.line, hit.number)
	}
	if len(hits) > 0 {
		return 1
	}
	return 0
}

// hit is one bare release number written in a page.
type hit struct {
	path   string
	line   int
	number string
}

// releaseTag matches a tag of the exact form vX.Y.Z. A prerelease tag such as
// v0.7.0-rc.1 does not match.
var releaseTag = regexp.MustCompile(`^v[0-9]+\.[0-9]+\.[0-9]+$`)

// numberRun matches a maximal run of digits and dots, so a number inside a
// longer run cannot be mistaken for a whole dotted number.
var numberRun = regexp.MustCompile(`[0-9.]+`)

// check returns every bare release number in the en and zh trees below
// contentDir that equals a tag of the repository, with paths relative to
// contentDir.
func check(contentDir, repoDir string) ([]hit, error) {
	tags, err := releaseTags(repoDir)
	if err != nil {
		return nil, err
	}
	if len(tags) == 0 {
		return nil, fmt.Errorf("no vX.Y.Z tag in %s", repoDir)
	}

	var hits []hit
	for _, language := range []string{"en", "zh"} {
		root := filepath.Join(contentDir, language)
		info, err := os.Stat(root)
		if err != nil {
			return nil, fmt.Errorf("read %s: %w", root, err)
		}
		if !info.IsDir() {
			return nil, fmt.Errorf("%s is not a directory", root)
		}
		err = filepath.WalkDir(root, func(path string, entry fs.DirEntry, err error) error {
			if err != nil {
				return err
			}
			if !entry.Type().IsRegular() {
				return nil
			}
			rel, err := filepath.Rel(contentDir, path)
			if err != nil {
				return err
			}
			found, err := checkFile(path, filepath.ToSlash(rel), tags)
			if err != nil {
				return err
			}
			hits = append(hits, found...)
			return nil
		})
		if err != nil {
			return nil, fmt.Errorf("scan %s: %w", root, err)
		}
	}
	return hits, nil
}

// releaseTags returns the numbers of the repository's vX.Y.Z tags, without the
// v prefix.
func releaseTags(repoDir string) (map[string]bool, error) {
	cmd := exec.Command("git", "-C", repoDir, "tag", "--list")
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		detail := strings.TrimSpace(stderr.String())
		if detail == "" {
			detail = err.Error()
		}
		return nil, fmt.Errorf("read tags of %s: %s", repoDir, detail)
	}

	tags := make(map[string]bool)
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if releaseTag.MatchString(line) {
			tags[strings.TrimPrefix(line, "v")] = true
		}
	}
	return tags, nil
}

// checkFile returns the bare tag numbers written in one file.
func checkFile(path, rel string, tags map[string]bool) ([]hit, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var hits []hit
	for index, line := range strings.Split(string(data), "\n") {
		for _, number := range bareNumbers(line) {
			if tags[number] {
				hits = append(hits, hit{path: rel, line: index + 1, number: number})
			}
		}
	}
	return hits, nil
}

// bareNumbers returns the whole dotted numbers in line that are not
// immediately preceded by v. A trailing sentence period is dropped, while a
// longer run such as 10.6.1 or 0.6.1.2 yields no number equal to 0.6.1.
func bareNumbers(line string) []string {
	var numbers []string
	for _, loc := range numberRun.FindAllStringIndex(line, -1) {
		start := loc[0]
		if start > 0 && line[start-1] == 'v' {
			continue
		}
		if number := strings.TrimRight(line[start:loc[1]], "."); number != "" {
			numbers = append(numbers, number)
		}
	}
	return numbers
}
