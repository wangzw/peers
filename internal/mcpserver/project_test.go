package mcpserver

import (
	"bytes"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeProjectFile(t *testing.T, dir, content string) string {
	t.Helper()
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("create %s: %v", dir, err)
	}
	path := filepath.Join(dir, projectConfigFile)
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatalf("write %s: %v", path, err)
	}
	return path
}

func stopSearchAt(t *testing.T, dir string) {
	t.Helper()
	old := searchStop
	searchStop = dir
	t.Cleanup(func() { searchStop = old })
}

// TestFindProjectConfig checks which .peers.yaml is found and how its topics are
// parsed.
func TestFindProjectConfig(t *testing.T) {
	t.Run("finds the file in an ancestor directory", func(t *testing.T) {
		root := t.TempDir()
		want := writeProjectFile(t, filepath.Join(root, "a"), "topics: [x]\n")
		work := filepath.Join(root, "a", "b")

		cfg, found, err := findProjectConfig(work)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want %q", cfg.Path, want)
		}
	})

	t.Run("nearest ancestor wins", func(t *testing.T) {
		root := t.TempDir()
		want := writeProjectFile(t, filepath.Join(root, "a"), "topics: [near]\n")
		writeProjectFile(t, root, "topics: [far]\n")
		work := filepath.Join(root, "a", "b")

		cfg, found, err := findProjectConfig(work)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want nearest %q", cfg.Path, want)
		}
		if !reflect.DeepEqual(cfg.Topics, []string{"near"}) {
			t.Errorf("Topics = %v, want [near]", cfg.Topics)
		}
	})

	t.Run("a directory with that name is skipped", func(t *testing.T) {
		root := t.TempDir()
		if err := os.MkdirAll(filepath.Join(root, "a", projectConfigFile), 0o755); err != nil {
			t.Fatal(err)
		}
		want := writeProjectFile(t, root, "topics: [far]\n")
		work := filepath.Join(root, "a", "b")

		cfg, found, err := findProjectConfig(work)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want %q", cfg.Path, want)
		}
	})

	t.Run("a symlink to a regular file is followed", func(t *testing.T) {
		root := t.TempDir()
		target := writeProjectFile(t, filepath.Join(root, "real"), "topics: [linked]\n")
		dir := filepath.Join(root, "a")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(dir, projectConfigFile)
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}

		cfg, found, err := findProjectConfig(filepath.Join(dir, "b"))
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.Path != link {
			t.Errorf("Path = %q, want %q", cfg.Path, link)
		}
		if want := []string{"linked"}; !reflect.DeepEqual(cfg.Topics, want) {
			t.Errorf("Topics = %v, want %v", cfg.Topics, want)
		}
	})

	t.Run("a dangling symlink is skipped", func(t *testing.T) {
		root := t.TempDir()
		want := writeProjectFile(t, root, "topics: [far]\n")
		dir := filepath.Join(root, "a")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(dir, projectConfigFile)
		if err := os.Symlink(filepath.Join(root, "missing"), link); err != nil {
			t.Fatal(err)
		}

		cfg, found, err := findProjectConfig(filepath.Join(dir, "b"))
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want %q", cfg.Path, want)
		}
	})

	t.Run("a symlink to a directory is skipped", func(t *testing.T) {
		root := t.TempDir()
		want := writeProjectFile(t, root, "topics: [far]\n")
		dir := filepath.Join(root, "a")
		target := filepath.Join(root, "target")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.MkdirAll(target, 0o755); err != nil {
			t.Fatal(err)
		}
		link := filepath.Join(dir, projectConfigFile)
		if err := os.Symlink(target, link); err != nil {
			t.Fatal(err)
		}

		cfg, found, err := findProjectConfig(filepath.Join(dir, "b"))
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want %q", cfg.Path, want)
		}
	})

	t.Run("relative directory path", func(t *testing.T) {
		root := t.TempDir()
		want := writeProjectFile(t, filepath.Join(root, "a"), "topics: [x]\n")
		t.Chdir(root)

		cfg, found, err := findProjectConfig(filepath.Join("a", "b"))
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want %q", cfg.Path, want)
		}
	})

	t.Run("drops empty strings and later duplicates", func(t *testing.T) {
		root := t.TempDir()
		writeProjectFile(t, root, "topics: [x, \"\", y, x]\n")

		cfg, found, err := findProjectConfig(root)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if want := []string{"x", "y"}; !reflect.DeepEqual(cfg.Topics, want) {
			t.Errorf("Topics = %v, want %v", cfg.Topics, want)
		}
		if !cfg.AutoJoin {
			t.Error("AutoJoin = false, want true when the key is absent")
		}
	})

	t.Run("auto_join false", func(t *testing.T) {
		root := t.TempDir()
		writeProjectFile(t, root, "auto_join: false\n")

		cfg, found, err := findProjectConfig(root)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.AutoJoin {
			t.Error("AutoJoin = true, want false")
		}
	})

	t.Run("topics with the wrong type is an error", func(t *testing.T) {
		root := t.TempDir()
		writeProjectFile(t, root, "topics: x\n")

		_, found, err := findProjectConfig(root)
		if !found {
			t.Fatal("found = false, want true")
		}
		if err == nil {
			t.Fatal("err = nil, want a parse error")
		}
	})

	t.Run("auto_join with the wrong type is an error", func(t *testing.T) {
		root := t.TempDir()
		writeProjectFile(t, root, "auto_join: maybe\n")

		_, found, err := findProjectConfig(root)
		if !found {
			t.Fatal("found = false, want true")
		}
		if err == nil {
			t.Fatal("err = nil, want a parse error")
		}
	})

	t.Run("empty file", func(t *testing.T) {
		root := t.TempDir()
		writeProjectFile(t, root, "")

		cfg, found, err := findProjectConfig(root)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if len(cfg.Topics) != 0 {
			t.Errorf("Topics = %v, want none", cfg.Topics)
		}
		if !cfg.AutoJoin {
			t.Error("AutoJoin = false, want true")
		}
	})

	t.Run("unknown keys are ignored", func(t *testing.T) {
		root := t.TempDir()
		writeProjectFile(t, root, "foo: 1\ntopics: [x]\n")

		cfg, found, err := findProjectConfig(root)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if want := []string{"x"}; !reflect.DeepEqual(cfg.Topics, want) {
			t.Errorf("Topics = %v, want %v", cfg.Topics, want)
		}
	})

	t.Run("unreadable file is an error, not a miss", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can read a file regardless of its mode")
		}
		root := t.TempDir()
		path := writeProjectFile(t, root, "topics: [x]\n")
		if err := os.Chmod(path, 0o000); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(path, 0o644) })

		_, found, err := findProjectConfig(root)
		if !found {
			t.Fatal("found = false, want true")
		}
		if err == nil {
			t.Fatal("err = nil, want a read error")
		}
	})

	t.Run("no file anywhere up the tree", func(t *testing.T) {
		root := t.TempDir()
		stopSearchAt(t, root)

		_, found, err := findProjectConfig(root)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if found {
			t.Fatal("found = true, want false")
		}
	})

	t.Run("searchStop hides a file in its parent", func(t *testing.T) {
		root := t.TempDir()
		writeProjectFile(t, root, "topics: [above]\n")
		stop := filepath.Join(root, "a")
		if err := os.MkdirAll(stop, 0o755); err != nil {
			t.Fatal(err)
		}
		stopSearchAt(t, stop)

		_, found, err := findProjectConfig(filepath.Join(stop, "b"))
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if found {
			t.Fatal("found = true, want false: the file above searchStop must not be searched")
		}
	})

	t.Run("searchStop itself is searched", func(t *testing.T) {
		root := t.TempDir()
		stop := filepath.Join(root, "a")
		want := writeProjectFile(t, stop, "topics: [here]\n")
		stopSearchAt(t, stop)

		cfg, found, err := findProjectConfig(filepath.Join(stop, "b"))
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true: searchStop itself must be searched")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want %q", cfg.Path, want)
		}
	})
}

func fakeProjectOwner(t *testing.T, uidByPath map[string]uint32) {
	t.Helper()
	old := projectOwnerUID
	projectOwnerUID = func(path string) (uint32, error) {
		if uid, ok := uidByPath[path]; ok {
			return uid, nil
		}
		return uint32(os.Geteuid()), nil
	}
	t.Cleanup(func() { projectOwnerUID = old })
}

// TestProjectConfigOwner checks that a project configuration and its directory
// are adopted only when this account owns them, and that a refusal is logged.
func TestProjectConfigOwner(t *testing.T) {
	other := uint32(os.Geteuid()) + 1

	t.Run("a file owned by another account is skipped", func(t *testing.T) {
		root := t.TempDir()
		stopSearchAt(t, root)
		want := writeProjectFile(t, root, "topics: [far]\n")
		middle := filepath.Join(root, "a")
		planted := writeProjectFile(t, middle, "topics: [planted]\n")
		fakeProjectOwner(t, map[string]uint32{planted: other})

		cfg, found, err := findProjectConfig(filepath.Join(middle, "b"))
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want the ancestor file")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want the ancestor %q", cfg.Path, want)
		}
	})

	t.Run("a directory owned by another account is skipped", func(t *testing.T) {
		root := t.TempDir()
		stopSearchAt(t, root)
		want := writeProjectFile(t, root, "topics: [far]\n")
		middle := filepath.Join(root, "a")
		writeProjectFile(t, middle, "topics: [planted]\n")
		fakeProjectOwner(t, map[string]uint32{middle: other})

		cfg, found, err := findProjectConfig(filepath.Join(middle, "b"))
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want the ancestor file")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want the ancestor %q", cfg.Path, want)
		}
	})

	t.Run("the refusal is logged", func(t *testing.T) {
		root := t.TempDir()
		stopSearchAt(t, root)
		middle := filepath.Join(root, "a")
		planted := writeProjectFile(t, middle, "topics: [planted]\n")
		fakeProjectOwner(t, map[string]uint32{planted: other})
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, nil))

		if _, found, err := findProjectConfigLogged(filepath.Join(middle, "b"), log); err != nil || found {
			t.Fatalf("findProjectConfigLogged = found %v, err %v; want absent", found, err)
		}
		if !strings.Contains(buf.String(), planted) {
			t.Errorf("log = %q, want it to name %s", buf.String(), planted)
		}
	})

	t.Run("a file and directory owned by this account are adopted", func(t *testing.T) {
		root := t.TempDir()
		want := writeProjectFile(t, root, "topics: [mine]\n")
		fakeProjectOwner(t, nil)

		cfg, found, err := findProjectConfig(root)
		if err != nil {
			t.Fatalf("findProjectConfig: %v", err)
		}
		if !found {
			t.Fatal("found = false, want true")
		}
		if cfg.Path != want {
			t.Errorf("Path = %q, want %q", cfg.Path, want)
		}
	})
}
