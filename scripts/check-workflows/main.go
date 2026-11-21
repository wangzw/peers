// Command check-workflows enforces the supply-chain rules the GitHub Actions
// workflows must keep.
//
// A third-party action runs inside a job that may hold a write token or the
// OAuth secret, so a mutable tag lets whoever moves the tag run other code
// there. Every uses: value must therefore name a 40-character commit, with the
// tag it was resolved from in a comment so the next reader can find the
// release. A reference to a workflow in this repository (./...) names bytes
// that the checkout already fixed and is exempt. A container reference
// (docker://) must name a sha256 digest: a tag or a bare image is mutable like
// a tag is.
//
// Composite actions under .github/actions are checked by the same rules,
// because a workflow that calls one runs the actions it names.
//
// actions/checkout leaves the job token in .git/config unless persist-credentials
// is false; a later step or a build tool that reads the git configuration could
// then use it. Every checkout step must disable it.
//
// The workflows that build the release, build the website, or run a probe with
// the OAuth secret must not restore an Actions cache: a cache entry is written
// by a different workflow and is not covered by go.sum, so a poisoned entry
// would be compiled into the released binary or executed beside the secret.
// Those workflows' setup-go steps must set cache: false.
//
// The checker reads only the files; it does not contact GitHub, so it cannot
// tell whether a commit still matches the tag in the comment. Review and the
// pinned revisions themselves carry that part.
package main

import (
	"flag"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// noCacheWorkflows lists the workflow files whose setup-go steps must disable
// the Actions cache. Each one builds a binary installed as root or runs beside
// the OAuth secret.
var noCacheWorkflows = map[string]bool{
	"release.yml":      true,
	"site.yml":         true,
	"real-session.yml": true,
}

// commitRef matches a third-party action reference pinned to a commit:
// <owner>/<repo>[/<path>]@<40 lowercase hex digits>.
var commitRef = regexp.MustCompile(`^[^@/\s]+/[^@/\s]+(?:/[^@\s]+)?@[0-9a-f]{40}$`)

// dockerDigest matches a container reference pinned by digest:
// docker://<image>@sha256:<64 lowercase hex digits>.
var dockerDigest = regexp.MustCompile(`^docker://[^@\s]+@sha256:[0-9a-f]{64}$`)

// versionComment matches the trailing comment that records the tag a commit
// was resolved from, such as "# v7" or "# v1.7.0". The comment may not be
// empty.
var versionComment = regexp.MustCompile(`^#\s+\S`)

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("check-workflows", flag.ContinueOnError)
	fs.SetOutput(stderr)
	dir := fs.String("dir", filepath.Join(".github", "workflows"), "directory holding the workflow files")
	actions := fs.String("actions", filepath.Join(".github", "actions"), "directory holding the composite actions, or an empty value to skip it")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "check-workflows: unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	problems, err := checkDir(*dir)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "check-workflows: %v\n", err)
		return 1
	}
	if *actions != "" {
		actionProblems, err := checkActions(*actions)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "check-workflows: %v\n", err)
			return 1
		}
		problems = append(problems, actionProblems...)
	}
	for _, problem := range problems {
		_, _ = fmt.Fprintln(stderr, problem)
	}
	if len(problems) > 0 {
		return 1
	}
	return 0
}

// checkDir checks every .yml and .yaml file directly under dir, in name order.
func checkDir(dir string) ([]string, error) {
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", dir, err)
	}
	var names []string
	for _, entry := range entries {
		if entry.IsDir() {
			continue
		}
		name := entry.Name()
		if strings.HasSuffix(name, ".yml") || strings.HasSuffix(name, ".yaml") {
			names = append(names, name)
		}
	}
	sort.Strings(names)

	var problems []string
	for _, name := range names {
		path := filepath.Join(dir, name)
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("cannot read %s: %w", path, err)
		}
		fileProblems, err := checkFile(path, data)
		if err != nil {
			return nil, err
		}
		problems = append(problems, fileProblems...)
	}
	return problems, nil
}

// checkActions checks every composite action below root: each action.yml or
// action.yaml at any depth. A missing root is not an error, because a
// repository need not define a composite action.
func checkActions(root string) ([]string, error) {
	var paths []string
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			if os.IsNotExist(err) {
				return nil
			}
			return err
		}
		if d.IsDir() {
			return nil
		}
		if name := d.Name(); name == "action.yml" || name == "action.yaml" {
			paths = append(paths, path)
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", root, err)
	}
	sort.Strings(paths)

	var problems []string
	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			return nil, fmt.Errorf("cannot read %s: %w", path, err)
		}
		fileProblems, err := checkFile(path, data)
		if err != nil {
			return nil, err
		}
		problems = append(problems, fileProblems...)
	}
	return problems, nil
}

// checkFile checks one workflow document. name is used in the messages; the
// cache rule is chosen by that name's base.
func checkFile(name string, data []byte) ([]string, error) {
	var doc yaml.Node
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return nil, fmt.Errorf("cannot parse %s: %w", name, err)
	}
	restricted := noCacheWorkflows[filepath.Base(name)]
	return inspect(&doc, name, restricted), nil
}

// inspect walks the document and returns one message per violation, in
// document order. A mapping that holds a uses field is a step or a reusable
// workflow call; its other fields are read from the same mapping.
func inspect(node *yaml.Node, file string, restricted bool) []string {
	if node == nil {
		return nil
	}
	var problems []string
	if node.Kind == yaml.MappingNode {
		problems = append(problems, inspectMapping(node, file, restricted)...)
	}
	for _, child := range node.Content {
		problems = append(problems, inspect(child, file, restricted)...)
	}
	return problems
}

func inspectMapping(node *yaml.Node, file string, restricted bool) []string {
	keys := map[string]*yaml.Node{}
	lines := map[string]int{}
	for i := 0; i+1 < len(node.Content); i += 2 {
		key, value := node.Content[i], node.Content[i+1]
		keys[key.Value] = value
		lines[key.Value] = key.Line
	}

	uses, ok := keys["uses"]
	if !ok {
		return nil
	}
	ref := uses.Value
	if strings.HasPrefix(ref, "./") {
		return nil
	}

	line := lines["uses"]
	if strings.HasPrefix(ref, "docker://") {
		if !dockerDigest.MatchString(ref) {
			return []string{fmt.Sprintf("%s:%d: docker:// reference %s must name a sha256 digest", file, line, ref)}
		}
		return nil
	}

	var problems []string
	if !commitRef.MatchString(ref) {
		problems = append(problems, fmt.Sprintf("%s:%d: %s is not pinned to a 40-character commit SHA", file, line, ref))
	} else if !versionComment.MatchString(strings.TrimSpace(uses.LineComment)) {
		problems = append(problems, fmt.Sprintf("%s:%d: %s has no version tag comment after the commit SHA", file, line, ref))
	}

	switch {
	case strings.HasPrefix(ref, "actions/checkout@"):
		if !hasWith(keys, "persist-credentials", "false") {
			problems = append(problems, fmt.Sprintf("%s:%d: actions/checkout must set persist-credentials: false", file, line))
		}
	case restricted && strings.HasPrefix(ref, "actions/setup-go@"):
		if !hasWith(keys, "cache", "false") {
			problems = append(problems, fmt.Sprintf("%s:%d: actions/setup-go must set cache: false in this workflow", file, line))
		}
	}
	return problems
}

// hasWith reports whether the step's with mapping sets key to value.
func hasWith(keys map[string]*yaml.Node, key, value string) bool {
	with, ok := keys["with"]
	if !ok || with.Kind != yaml.MappingNode {
		return false
	}
	for i := 0; i+1 < len(with.Content); i += 2 {
		if with.Content[i].Value == key && with.Content[i+1].Value == value {
			return true
		}
	}
	return false
}
