package pathcmp

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func linkedPaths(t *testing.T) (link, real, fileA, fileB string) {
	t.Helper()
	root := t.TempDir()
	real = filepath.Join(root, "real")
	if err := os.Mkdir(real, 0o755); err != nil {
		t.Fatalf("creating %s: %v", real, err)
	}
	fileA = filepath.Join(real, "a")
	fileB = filepath.Join(real, "b")
	for _, name := range []string{fileA, fileB} {
		if err := os.WriteFile(name, []byte("x"), 0o600); err != nil {
			t.Fatalf("creating %s: %v", name, err)
		}
	}
	link = filepath.Join(root, "link")
	if err := os.Symlink(real, link); err != nil {
		t.Fatalf("linking %s to %s: %v", link, real, err)
	}
	return link, real, fileA, fileB
}

// TestResolveExpandsSymlinks checks that Resolve turns a path through a link
// into the link's target path.
func TestResolveExpandsSymlinks(t *testing.T) {
	link, _, fileA, _ := linkedPaths(t)

	resolvedLink, err := Resolve(filepath.Join(link, "a"))
	if err != nil {
		t.Fatalf("resolving through the link: %v", err)
	}
	resolvedReal, err := Resolve(fileA)
	if err != nil {
		t.Fatalf("resolving the real path: %v", err)
	}
	if resolvedLink != resolvedReal {
		t.Errorf("Resolve(link/a) = %q, Resolve(real/a) = %q, want equal", resolvedLink, resolvedReal)
	}
	if strings.HasPrefix(resolvedLink, link+string(os.PathSeparator)) {
		t.Errorf("Resolve(link/a) = %q, want the link expanded", resolvedLink)
	}
}

// TestSamePath checks that SamePath compares two paths after resolving links
// and reports an error for a missing path.
func TestSamePath(t *testing.T) {
	link, _, fileA, fileB := linkedPaths(t)

	tests := []struct {
		name    string
		a       string
		b       string
		want    bool
		wantErr bool
	}{
		{name: "identical", a: fileA, b: fileA, want: true},
		{name: "link and target", a: filepath.Join(link, "a"), b: fileA, want: true},
		{name: "different files", a: filepath.Join(link, "a"), b: fileB, want: false},
		{name: "missing path", a: fileA, b: filepath.Join(link, "missing"), wantErr: true},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got, err := SamePath(tc.a, tc.b)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("SamePath(%q, %q) = %v, want an error", tc.a, tc.b, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("SamePath(%q, %q): %v", tc.a, tc.b, err)
			}
			if got != tc.want {
				t.Errorf("SamePath(%q, %q) = %v, want %v", tc.a, tc.b, got, tc.want)
			}
		})
	}
}
