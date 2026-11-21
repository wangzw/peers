package main

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// hashOf returns the SHA-256 of data as 64 lowercase hexadecimal digits.
func hashOf(data string) string {
	sum := sha256.Sum256([]byte(data))
	return hex.EncodeToString(sum[:])
}

func TestParse(t *testing.T) {
	const hashA = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
	const record = `# a comment

directory: foo
module: example.com/foo
version: v1.2.3
changed: a.go ` + hashA + ` abc,def
deleted: b.go

---
directory: bar
module: example.com/bar
version: v0.1.0
`
	entries, err := parse(strings.NewReader(record), "record")
	if err != nil {
		t.Fatalf("parse = %v", err)
	}
	if len(entries) != 2 {
		t.Fatalf("entries = %d, want 2", len(entries))
	}
	foo := entries[0]
	if foo.Directory != "foo" || foo.Module != "example.com/foo" || foo.Version != "v1.2.3" {
		t.Fatalf("foo = %+v", foo)
	}
	if len(foo.Changed) != 1 || foo.Changed[0].File != "a.go" {
		t.Fatalf("foo.Changed = %+v", foo.Changed)
	}
	if foo.Changed[0].Hash != hashA {
		t.Fatalf("foo.Changed[0].Hash = %q, want %q", foo.Changed[0].Hash, hashA)
	}
	if got := strings.Join(foo.Changed[0].Commits, ","); got != "abc,def" {
		t.Fatalf("foo commits = %q", got)
	}
	if len(foo.Deleted) != 1 || foo.Deleted[0] != "b.go" {
		t.Fatalf("foo.Deleted = %q", foo.Deleted)
	}
	if entries[1].Directory != "bar" || len(entries[1].Changed) != 0 || len(entries[1].Deleted) != 0 {
		t.Fatalf("bar = %+v", entries[1])
	}
}

func TestParseErrors(t *testing.T) {
	tests := []struct {
		name   string
		record string
		want   string
	}{
		{"missing version", "directory: foo\nmodule: example.com/foo\n", "required"},
		{"unknown field", "directory: foo\nmodule: example.com/foo\nversion: v1\nfile: x\n", "unknown field"},
		{"changed without hash or commits", "directory: foo\nmodule: example.com/foo\nversion: v1\nchanged: a.go\n", "changed needs a file, its content hash and its commits"},
		{"changed with a malformed hash", "directory: foo\nmodule: example.com/foo\nversion: v1\nchanged: a.go nothex abc\n", "SHA-256"},
		{"deleted with two files", "directory: foo\nmodule: example.com/foo\nversion: v1\ndeleted: a.go b.go\n", "deleted needs one file"},
		{"duplicate directory", "directory: foo\nmodule: example.com/foo\nversion: v1\n---\ndirectory: foo\nmodule: example.com/bar\nversion: v2\n", "duplicate directory"},
		{"empty", "# nothing\n", "no entries"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, err := parse(strings.NewReader(tt.record), "record")
			if err == nil || !strings.Contains(err.Error(), tt.want) {
				t.Fatalf("parse = %v, want an error containing %q", err, tt.want)
			}
		})
	}
}

// TestParseRegistration checks that the file the repository ships parses.
func TestParseRegistration(t *testing.T) {
	entries, err := parseFile(filepath.Join("..", "..", "third_party", "UPSTREAM"))
	if err != nil {
		t.Fatalf("parseFile = %v", err)
	}
	want := []string{"claude-code-socket-transport", "fsnotify"}
	if len(entries) != len(want) {
		t.Fatalf("entries = %d, want %d", len(entries), len(want))
	}
	for i, directory := range want {
		if entries[i].Directory != directory {
			t.Errorf("entries[%d].Directory = %q, want %q", i, entries[i].Directory, directory)
		}
	}
	if len(entries[0].Changed) != 0 || len(entries[0].Deleted) != 0 {
		t.Errorf("claude-code-socket-transport has registered changes: %+v", entries[0])
	}
	if len(entries[1].Changed) != 3 {
		t.Errorf("fsnotify has %d registered changes, want 3", len(entries[1].Changed))
	}
	// The registered hash must describe the file the repository ships, so a
	// later edit to a vendored file is caught even before the upstream download.
	for _, c := range entries[1].Changed {
		if !contentHash.MatchString(c.Hash) {
			t.Errorf("%s: registered hash %q is not a SHA-256", c.File, c.Hash)
			continue
		}
		data, err := os.ReadFile(filepath.Join("..", "..", "third_party", "fsnotify", c.File))
		if err != nil {
			t.Errorf("cannot read third_party/fsnotify/%s: %v", c.File, err)
			continue
		}
		if got := hashOf(string(data)); got != c.Hash {
			t.Errorf("%s: registered hash %q, file hashes to %q", c.File, c.Hash, got)
		}
	}
}

func TestCompare(t *testing.T) {
	const upstreamDir = "testdata/upstream"

	edit := entry{Changed: []change{{File: "edit.txt", Hash: hashOf("edit local\n"), Commits: []string{"abc"}}}}
	tests := []struct {
		name   string
		entry  entry
		mutate func(*testing.T, string)
		want   []string
	}{
		{"consistent", entry{}, nil, nil},
		{"registered change", edit, overwriteEdit, nil},
		{"registered new file", entry{Changed: []change{{File: "extra.txt", Hash: hashOf("extra\n"), Commits: []string{"abc"}}}}, addExtra, nil},
		{"unregistered change", entry{}, overwriteEdit, []string{"edit.txt: unregistered change"}},
		{"extra file", entry{}, addExtra, []string{"extra.txt: unregistered file"}},
		{"missing file", entry{}, removeGone, []string{"gone.txt: missing file"}},
		{"listed as changed but unchanged", edit, nil, []string{"edit.txt: listed as changed but unchanged"}},
		{"listed as changed but missing", edit, removeEdit, []string{"edit.txt: listed as changed but missing", "edit.txt: missing file"}},
		{"content hash mismatch", entry{Changed: []change{{File: "edit.txt", Hash: hashOf("other\n"), Commits: []string{"abc"}}}}, overwriteEdit, []string{"edit.txt: content hash does not match the registration"}},
		{"registered delete", entry{Deleted: []string{"gone.txt"}}, removeGone, nil},
		{"listed as deleted but present", entry{Deleted: []string{"gone.txt"}}, nil, []string{"gone.txt: listed as deleted but present"}},
		{"listed as deleted but absent upstream", entry{Deleted: []string{"nowhere.txt"}}, nil, []string{"nowhere.txt: listed as deleted but absent upstream"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			localDir := copyTree(t, "testdata/local")
			if tt.mutate != nil {
				tt.mutate(t, localDir)
			}
			problems, err := compare(upstreamDir, localDir, tt.entry)
			if err != nil {
				t.Fatalf("compare = %v", err)
			}
			if strings.Join(problems, "\n") != strings.Join(tt.want, "\n") {
				t.Fatalf("problems = %q, want %q", problems, tt.want)
			}
		})
	}
}

// TestFileHashesRejectsSymlink checks that a symbolic link in a vendored tree
// is reported instead of being skipped.
func TestFileHashesRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "real.txt"), []byte("real\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink("real.txt", filepath.Join(dir, "link.txt")); err != nil {
		t.Fatal(err)
	}
	_, err := fileHashes(dir)
	if err == nil || !strings.Contains(err.Error(), "symbolic link") {
		t.Fatalf("fileHashes = %v, want an error naming the symbolic link", err)
	}
	if err != nil && !strings.Contains(err.Error(), "link.txt") {
		t.Errorf("fileHashes error = %v, want it to name link.txt", err)
	}
}

func overwriteEdit(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "edit.txt"), []byte("edit local\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func addExtra(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, "extra.txt"), []byte("extra\n"), 0o644); err != nil {
		t.Fatal(err)
	}
}

func removeGone(t *testing.T, dir string) {
	t.Helper()
	if err := os.Remove(filepath.Join(dir, "gone.txt")); err != nil {
		t.Fatal(err)
	}
}

func removeEdit(t *testing.T, dir string) {
	t.Helper()
	if err := os.Remove(filepath.Join(dir, "edit.txt")); err != nil {
		t.Fatal(err)
	}
}

// copyTree copies the regular files below src into a new temporary directory.
func copyTree(t *testing.T, src string) string {
	t.Helper()
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		rel, err := filepath.Rel(src, path)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatalf("copyTree(%s) = %v", src, err)
	}
	return dst
}
