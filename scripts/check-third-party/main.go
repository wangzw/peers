// Command check-third-party checks the vendored source under third_party/
// against its registered upstream.
//
// third_party/UPSTREAM records, for each vendored library, the upstream module
// path and version, every file changed or added locally with the SHA-256 of its
// vendored content and the commits that introduced it, and every upstream file
// deleted locally. This command downloads each module at the recorded version
// through the Go module proxy and checksum database, so the comparison is
// against verified upstream bytes, and then compares the two trees file by
// file.
//
// A difference that the entry does not list fails the check, and so does a
// listed change that no longer exists or whose local content no longer matches
// the recorded hash. A symbolic link inside a vendored tree is an error: the
// comparison hashes regular files and would otherwise let a link stand in for
// one. The comparison reads local files only; the download is a separate step,
// so the comparison can be tested offline.
package main

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
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

// entry is one library in third_party/UPSTREAM.
type entry struct {
	Directory string
	Module    string
	Version   string
	Changed   []change
	Deleted   []string
}

// change is one local modification or addition, the SHA-256 of its vendored
// content and the commits behind it.
type change struct {
	File    string
	Hash    string
	Commits []string
}

// contentHash matches a SHA-256 written as 64 lowercase hexadecimal digits.
var contentHash = regexp.MustCompile(`^[0-9a-f]{64}$`)

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	fs := flag.NewFlagSet("check-third-party", flag.ContinueOnError)
	fs.SetOutput(stderr)
	upstreamPath := fs.String("upstream", filepath.Join("third_party", "UPSTREAM"), "registration file")
	thirdParty := fs.String("third-party", "third_party", "directory holding the vendored copies")
	if err := fs.Parse(args); err != nil {
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "check-third-party: unexpected argument %q\n", fs.Arg(0))
		return 2
	}

	entries, err := parseFile(*upstreamPath)
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "check-third-party: %v\n", err)
		return 1
	}

	status := 0
	for _, e := range entries {
		upstreamDir, err := download(e.Module, e.Version)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "check-third-party: %v\n", err)
			status = 1
			continue
		}
		localDir := filepath.Join(*thirdParty, e.Directory)
		problems, err := compare(upstreamDir, localDir, e)
		if err != nil {
			_, _ = fmt.Fprintf(stderr, "check-third-party: %v\n", err)
			status = 1
			continue
		}
		for _, problem := range problems {
			_, _ = fmt.Fprintf(stderr, "third_party/%s/%s\n", e.Directory, problem)
			status = 1
		}
	}
	return status
}

// download fetches module@version and returns the directory the Go toolchain
// unpacked it into. The proxy and checksum database verify the bytes.
func download(module, version string) (string, error) {
	spec := module + "@" + version
	cmd := exec.Command("go", "mod", "download", "-json", spec)
	out, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) && len(exitErr.Stderr) > 0 {
			return "", fmt.Errorf("cannot download %s: %s", spec, strings.TrimSpace(string(exitErr.Stderr)))
		}
		return "", fmt.Errorf("cannot download %s: %w", spec, err)
	}
	var info struct {
		Dir string
	}
	if err := json.Unmarshal(out, &info); err != nil {
		return "", fmt.Errorf("cannot read the download result for %s: %w", spec, err)
	}
	if info.Dir == "" {
		return "", fmt.Errorf("cannot download %s: the Go toolchain reported no directory", spec)
	}
	return info.Dir, nil
}

// parseFile reads and parses the registration file.
func parseFile(path string) ([]entry, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", path, err)
	}
	defer func() { _ = f.Close() }()
	return parse(f, path)
}

// parse reads entries from r. name is used in error messages.
func parse(r io.Reader, name string) ([]entry, error) {
	var entries []entry
	var current *entry
	line := 0

	finish := func() error {
		if current == nil {
			return nil
		}
		if current.Directory == "" || current.Module == "" || current.Version == "" {
			return fmt.Errorf("cannot parse %s: line %d: directory, module and version are required", name, line)
		}
		for _, e := range entries {
			if e.Directory == current.Directory {
				return fmt.Errorf("cannot parse %s: line %d: duplicate directory %q", name, line, current.Directory)
			}
		}
		entries = append(entries, *current)
		current = nil
		return nil
	}

	scanner := bufio.NewScanner(r)
	for scanner.Scan() {
		line++
		text := strings.TrimSpace(scanner.Text())
		if text == "" || strings.HasPrefix(text, "#") {
			continue
		}
		if text == "---" {
			if err := finish(); err != nil {
				return nil, err
			}
			continue
		}
		key, value, ok := strings.Cut(text, ":")
		if !ok {
			return nil, fmt.Errorf("cannot parse %s: line %d: %q is not a field", name, line, text)
		}
		key = strings.TrimSpace(key)
		value = strings.TrimSpace(value)
		if current == nil {
			current = &entry{}
		}
		switch key {
		case "directory":
			current.Directory = value
		case "module":
			current.Module = value
		case "version":
			current.Version = value
		case "changed":
			fields := strings.Fields(value)
			if len(fields) != 3 {
				return nil, fmt.Errorf("cannot parse %s: line %d: changed needs a file, its content hash and its commits", name, line)
			}
			if !contentHash.MatchString(fields[1]) {
				return nil, fmt.Errorf("cannot parse %s: line %d: changed needs a SHA-256 of 64 lowercase hexadecimal digits", name, line)
			}
			commits := strings.Split(fields[2], ",")
			for _, commit := range commits {
				if commit == "" {
					return nil, fmt.Errorf("cannot parse %s: line %d: changed has an empty commit", name, line)
				}
			}
			current.Changed = append(current.Changed, change{File: fields[0], Hash: fields[1], Commits: commits})
		case "deleted":
			if value == "" || len(strings.Fields(value)) != 1 {
				return nil, fmt.Errorf("cannot parse %s: line %d: deleted needs one file", name, line)
			}
			current.Deleted = append(current.Deleted, value)
		default:
			return nil, fmt.Errorf("cannot parse %s: line %d: unknown field %q", name, line, key)
		}
	}
	if err := scanner.Err(); err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", name, err)
	}
	if err := finish(); err != nil {
		return nil, err
	}
	if len(entries) == 0 {
		return nil, fmt.Errorf("cannot parse %s: no entries", name)
	}
	return entries, nil
}

// compare checks one vendored tree against the upstream tree and the entry that
// registers their differences. It returns one message per problem, relative to
// the vendored directory.
func compare(upstreamDir, localDir string, e entry) ([]string, error) {
	upstream, err := fileHashes(upstreamDir)
	if err != nil {
		return nil, err
	}
	local, err := fileHashes(localDir)
	if err != nil {
		return nil, err
	}

	changed := map[string]string{}
	for _, c := range e.Changed {
		changed[c.File] = c.Hash
	}
	deleted := map[string]bool{}
	for _, name := range e.Deleted {
		deleted[name] = true
	}

	var problems []string
	for name, localHash := range local {
		sum := hex.EncodeToString(localHash[:])
		upstreamHash, ok := upstream[name]
		switch {
		case !ok:
			if _, listed := changed[name]; !listed {
				problems = append(problems, name+": unregistered file")
			} else if changed[name] != sum {
				problems = append(problems, name+": content hash does not match the registration")
			}
		case upstreamHash != localHash:
			if _, listed := changed[name]; !listed {
				problems = append(problems, name+": unregistered change")
			} else if changed[name] != sum {
				problems = append(problems, name+": content hash does not match the registration")
			}
		default:
			if _, listed := changed[name]; listed {
				problems = append(problems, name+": listed as changed but unchanged")
			}
		}
	}
	for name := range upstream {
		if _, ok := local[name]; ok {
			continue
		}
		if !deleted[name] {
			problems = append(problems, name+": missing file")
		}
	}
	for name := range changed {
		if _, ok := local[name]; !ok {
			problems = append(problems, name+": listed as changed but missing")
		}
	}
	for name := range deleted {
		if _, ok := upstream[name]; !ok {
			problems = append(problems, name+": listed as deleted but absent upstream")
			continue
		}
		if _, ok := local[name]; ok {
			problems = append(problems, name+": listed as deleted but present")
		}
	}
	sort.Strings(problems)
	return problems, nil
}

// fileHashes returns the SHA-256 of every regular file below root, keyed by its
// slash-separated path relative to root. A symbolic link below root is an
// error rather than a file that is skipped, so a link cannot replace a vendored
// file without the comparison noticing.
func fileHashes(root string) (map[string][sha256.Size]byte, error) {
	hashes := map[string][sha256.Size]byte{}
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(root, path)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.Type()&fs.ModeSymlink != 0 {
			return fmt.Errorf("%s is a symbolic link", rel)
		}
		if d.IsDir() || !d.Type().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		hashes[rel] = sha256.Sum256(data)
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot read %s: %w", root, err)
	}
	return hashes, nil
}
