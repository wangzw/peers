package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"

	"github.com/wangzw/peers/scripts/site-versions/gitenv"
)

// call records one invocation the fake executor received.
type call ExecOptions

// fakeExec stands in for hugo, site-meta and lychee. The handlers may create
// the files a real build would leave behind.
type fakeExec struct {
	calls    []call
	hugo     func(call) ([]byte, error)
	siteMeta func(call) ([]byte, error)
	lychee   func(call) ([]byte, error)
}

func (f *fakeExec) Run(_ context.Context, opts ExecOptions) ([]byte, error) {
	c := call(opts)
	f.calls = append(f.calls, c)
	switch {
	case opts.Name == "hugo":
		if f.hugo != nil {
			return f.hugo(c)
		}
		return nil, writeMarkdown(flagValue(opts.Args, "--destination"))
	case opts.Name == "go" && contains(opts.Args, "./scripts/site-meta"):
		if f.siteMeta != nil {
			return f.siteMeta(c)
		}
		return nil, nil
	case opts.Name == "lychee":
		if f.lychee != nil {
			return f.lychee(c)
		}
		return nil, nil
	}
	return nil, fmt.Errorf("unexpected command %s %v", opts.Name, opts.Args)
}

// callsNamed returns the invocations of one command, in order.
func (f *fakeExec) callsNamed(name string) []call {
	var found []call
	for _, c := range f.calls {
		if c.Name == name {
			found = append(found, c)
		}
	}
	return found
}

// writeMarkdown writes the pairs of index.html and index.md a real Hugo build
// leaves behind.
func writeMarkdown(dest string) error {
	for _, dir := range []string{"", "docs"} {
		target := filepath.Join(dest, dir)
		if err := os.MkdirAll(target, 0o755); err != nil {
			return err
		}
		for _, name := range []string{"index.html", "index.md"} {
			if err := os.WriteFile(filepath.Join(target, name), []byte("page\n"), 0o644); err != nil {
				return err
			}
		}
	}
	return nil
}

// flagValue returns the value after the first occurrence of name.
func flagValue(args []string, name string) string {
	for i, arg := range args {
		if arg == name && i+1 < len(args) {
			return args[i+1]
		}
	}
	return ""
}

// newFixture creates a repository with two commits and uncommitted working
// tree changes. It returns the repository root, the working tree's site
// directory and the first commit.
func newFixture(t *testing.T) (repo, site, refCommit string) {
	t.Helper()
	repo = t.TempDir()
	writeFiles(t, repo, map[string]string{
		"docs/site/hugo.yaml":                 "baseURL: https://example.test/\n",
		"docs/site/content/en/index.md":       "ref content\n",
		"docs/site/content/zh/index.md":       "ref content zh\n",
		"docs/site/layouts/index.html":        "ref layout\n",
		"docs/site/static/robots.txt":         "ref robots\n",
		"docs/site/go.mod":                    "module example.test/site\n",
		"docs/site/public/old.html":           "committed output\n",
		"docs/site/resources/cache":           "committed cache\n",
		"docs/site/.hugo_build.lock":          "committed lock\n",
		"docs/assets/demo.mp4":                "ref asset\n",
		"main.go":                             "package main\n",
		"internal/procsig/signal.go":          "package procsig\n",
		"third_party/library/library.go":      "package library\n",
		"scripts/checksignals/checksignal.go": "package main\n",
	})
	gitRun(t, repo, "init", "-b", "main")
	gitRun(t, repo, "add", "-A")
	gitCommit(t, repo, "first")
	refCommit = gitOutput(t, repo, "rev-parse", "HEAD")
	writeFiles(t, repo, map[string]string{"docs/site/content/en/extra.md": "second commit\n"})
	gitRun(t, repo, "add", "-A")
	gitCommit(t, repo, "second")
	writeFiles(t, repo, map[string]string{
		"docs/site/layouts/index.html":  "working layout\n",
		"docs/site/content/en/index.md": "working content\n",
		"docs/site/public/stale.html":   "stale output\n",
		"docs/site/resources/cache":     "stale cache\n",
		"docs/site/.hugo_build.lock":    "lock\n",
		"docs/assets/working.mp4":       "working asset\n",
	})
	if err := os.Remove(filepath.Join(repo, "docs", "assets", "demo.mp4")); err != nil {
		t.Fatalf("remove committed asset: %v", err)
	}
	site = filepath.Join(repo, "docs", "site")
	return repo, site, refCommit
}

// writeFiles writes a map of slash-separated paths below root.
func writeFiles(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, body := range files {
		path := filepath.Join(root, filepath.FromSlash(name))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatalf("create directory for %s: %v", name, err)
		}
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatalf("write %s: %v", name, err)
		}
	}
}

// gitRun runs one git command and fails the test on a non-zero exit.
func gitRun(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
}

// gitOutput runs one git command and returns its trimmed output.
func gitOutput(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("git", append([]string{"-C", dir}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("git %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return strings.TrimSpace(string(out))
}

// gitCommit commits the index without touching the user's git configuration.
func gitCommit(t *testing.T, dir, message string) {
	t.Helper()
	gitRun(t, dir,
		"-c", "user.name=test",
		"-c", "user.email=test@example.com",
		"-c", "commit.gpgsign=false",
		"commit", "-m", message)
}

// config holds the run paths for a test.
func testConfig(t *testing.T, repo, site string) Config {
	t.Helper()
	return Config{
		Repo:   repo,
		Site:   site,
		Work:   filepath.Join(t.TempDir(), "work"),
		Public: filepath.Join(t.TempDir(), "public"),
		Jobs:   3,
	}
}

// releases returns one build per class for the first commit and the working
// tree.
func testBuilds(commit string) []Build {
	config := func(name, release string) []byte {
		return []byte("params:\n  release: \"" + release + "\"\n  name: " + name + "\n")
	}
	return []Build{
		{
			Name: "v0.6.1", Class: ClassLatest,
			Source: Source{Commit: commit}, BaseURL: "https://example.test/",
			Output: "", Config: config("v0.6.1", "0.6.1"), Release: "0.6.1",
		},
		{
			Name: "v0.6.1", Class: ClassLatestCopy,
			Source: Source{Commit: commit}, BaseURL: "https://example.test/v0.6.1/",
			Output: "v0.6.1/", Config: config("v0.6.1", "0.6.1"), Release: "0.6.1",
			CanonicalBase: "https://example.test/",
		},
		{
			Name: "v0.5.0", Class: ClassOlder,
			Source: Source{Commit: commit}, BaseURL: "https://example.test/v0.5.0/",
			Output: "v0.5.0/", Config: config("v0.5.0", "0.5.0"), Release: "0.5.0",
		},
		{
			Name: "dev", Class: ClassDev,
			Source: Source{WorkingTree: true}, BaseURL: "https://example.test/dev/",
			Output: "dev/", Config: config("dev", "0.6.1"), Release: "0.6.1",
		},
	}
}

// newResolveRepo creates a repository with a site, two release tags and a
// versions.yaml.
func newResolveRepo(t *testing.T) (repo, site string) {
	t.Helper()
	repo = t.TempDir()
	writeFiles(t, repo, map[string]string{
		"docs/site/hugo.yaml":           "baseURL: https://example.test/\ndisableKinds:\n  - taxonomy\n  - term\n",
		"docs/site/versions.yaml":       "oldest: v0.6.1\n",
		"docs/site/content/en/index.md": "content\n",
		"docs/site/content/zh/index.md": "zh content\n",
	})
	gitRun(t, repo, "init", "-b", "main")
	gitRun(t, repo, "add", "-A")
	gitCommit(t, repo, "releases")
	gitRun(t, repo, "tag", "v0.6.1")
	gitRun(t, repo, "tag", "v0.6.2")
	site = filepath.Join(repo, "docs", "site")
	return repo, site
}

// writtenConfig is the part of a per-build configuration a test reads back.
type writtenConfig struct {
	DisableKinds []string `yaml:"disableKinds"`
	Params       struct {
		Release string `yaml:"release"`
	} `yaml:"params"`
}

// TestPerBuildConfiguration checks the configuration each build gets, and that
// Hugo receives exactly the document the resolver wrote.
func TestPerBuildConfiguration(t *testing.T) {
	repo, site := newResolveRepo(t)
	builds, err := resolveBuilds(resolveConfig{Repo: repo, Site: site})
	if err != nil {
		t.Fatalf("resolveBuilds: %v", err)
	}
	if len(builds) != 4 {
		t.Fatalf("resolved %d builds, want 4", len(builds))
	}

	wantRelease := map[string]string{
		"v0.6.2/latest": "0.6.2",
		"v0.6.2/copy":   "0.6.2",
		"v0.6.1/older":  "0.6.1",
		"dev/dev":       "0.6.2",
	}
	wantKinds := []string{"taxonomy", "term", "sitemap", "robotstxt"}
	for i, build := range builds {
		var doc writtenConfig
		if err := yaml.Unmarshal(build.Config, &doc); err != nil {
			t.Fatalf("build %d configuration: %v", i, err)
		}
		labels := map[string]string{
			ClassLatest:     build.Name + "/latest",
			ClassLatestCopy: build.Name + "/copy",
			ClassOlder:      build.Name + "/older",
			ClassDev:        build.Name + "/dev",
		}
		label := labels[build.Class]
		if want := wantRelease[label]; build.Release != want || doc.Params.Release != want {
			t.Errorf("%s release = %q/%q, want %q", label, build.Release, doc.Params.Release, want)
		}
		if build.Class == ClassLatest {
			if doc.DisableKinds != nil {
				t.Errorf("%s disableKinds = %v, want none", label, doc.DisableKinds)
			}
			continue
		}
		if !reflect.DeepEqual(doc.DisableKinds, wantKinds) {
			t.Errorf("%s disableKinds = %v, want %v", label, doc.DisableKinds, wantKinds)
		}
	}

	cfg := testConfig(t, repo, site)
	var mismatches []string
	index := 0
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		parts := strings.Split(flagValue(c.Args, "--config"), ",")
		if len(parts) != 2 {
			return nil, fmt.Errorf("--config %q does not name two files", flagValue(c.Args, "--config"))
		}
		data, err := os.ReadFile(parts[1])
		if err != nil {
			return []byte(err.Error()), err
		}
		if !bytes.Equal(data, builds[index].Config) {
			mismatches = append(mismatches, buildID(index, builds[index].Name))
		}
		index++
		return nil, writeMarkdown(flagValue(c.Args, "--destination"))
	}}
	if err := Run(context.Background(), cfg, builds, Deps{Exec: fake}); err != nil {
		var check *CheckError
		if errors.As(err, &check) {
			t.Fatalf("run: %v\n%s", err, check.Output)
		}
		t.Fatalf("run: %v", err)
	}
	if len(mismatches) > 0 {
		t.Errorf("Hugo received a configuration that differs from the build's own: %v", mismatches)
	}
}

// TestWorkDirectoryIsEmptiedAndKept checks that a stale entry is gone after the
// run and that the directory itself stays.
func TestWorkDirectoryIsEmptiedAndKept(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	writeFiles(t, cfg.Work, map[string]string{"stale.txt": "old\n", "old/keep.txt": "old\n"})

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	if _, err := os.Stat(cfg.Work); err != nil {
		t.Fatalf("work directory is gone: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Work, "stale.txt")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale file survived: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Work, "old")); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("stale directory survived: %v", err)
	}
}

// TestStalePublicOutputRemoved checks that a run leaves nothing in the public
// directory that it did not assemble itself.
func TestStalePublicOutputRemoved(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	writeFiles(t, cfg.Public, map[string]string{
		"stale.html":        "stale\n",
		"v9.9.9/index.html": "stale version\n",
	})

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, rel := range []string{"stale.html", "v9.9.9/index.html"} {
		if _, err := os.Stat(filepath.Join(cfg.Public, filepath.FromSlash(rel))); !errors.Is(err, os.ErrNotExist) {
			t.Errorf("%s survived the run: %v", rel, err)
		}
	}
}

// TestCheckoutKeepsFullHistory checks that a ref build comes from a clone with
// the repository's whole history.
func TestCheckoutKeepsFullHistory(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	want := gitOutput(t, repo, "rev-list", "--all", "--count")
	worktrees := gitOutput(t, repo, "worktree", "list", "--porcelain")

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	clone := filepath.Join(cfg.Work, "refs", commit)
	if _, err := os.Stat(filepath.Join(clone, ".git")); err != nil {
		t.Fatalf("clone has no .git: %v", err)
	}
	if got := gitOutput(t, clone, "rev-list", "--all", "--count"); got != want {
		t.Fatalf("clone has %s commits, want %s", got, want)
	}
	head := gitOutput(t, clone, "rev-parse", "HEAD")
	if head != commit {
		t.Fatalf("clone is at %s, want %s", head, commit)
	}
	if got := gitOutput(t, repo, "worktree", "list", "--porcelain"); got != worktrees {
		t.Fatalf("the source repository's worktree list changed:\n%s", got)
	}
}

// TestCloneIsSparse checks that the clone holds nothing outside docs/site and
// docs/assets.
func TestCloneIsSparse(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	clone := filepath.Join(cfg.Work, "refs", commit)
	err := filepath.WalkDir(clone, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(clone, path)
		if err != nil {
			return err
		}
		if rel == "." || rel == ".git" || strings.HasPrefix(rel, ".git/") {
			return nil
		}
		slash := filepath.ToSlash(rel)
		switch slash {
		case "docs", "docs/site", "docs/assets":
			return nil
		}
		if !strings.HasPrefix(slash, "docs/site/") && !strings.HasPrefix(slash, "docs/assets/") {
			t.Errorf("clone holds %s, which is outside docs/site and docs/assets", slash)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("walk clone: %v", err)
	}
}

// TestContentFromRefAndSiteFromWorkingTree checks the build model: content from
// the ref, every other site file and the assets from the working tree.
func TestContentFromRefAndSiteFromWorkingTree(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	fake := &fakeExec{}
	type observed struct{ content, layout, source string }
	var seen []observed
	fake.hugo = func(c call) ([]byte, error) {
		source := flagValue(c.Args, "--source")
		content, err := os.ReadFile(filepath.Join(source, "content", "en", "index.md"))
		if err != nil {
			return []byte(err.Error()), err
		}
		layout, err := os.ReadFile(filepath.Join(source, "layouts", "index.html"))
		if err != nil {
			return []byte(err.Error()), err
		}
		seen = append(seen, observed{string(content), string(layout), source})
		return nil, writeMarkdown(flagValue(c.Args, "--destination"))
	}

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake}); err != nil {
		var check *CheckError
		if errors.As(err, &check) {
			t.Fatalf("run: %v\n%s", err, check.Output)
		}
		t.Fatalf("run: %v", err)
	}
	if len(seen) != 4 {
		t.Fatalf("saw %d builds, want 4", len(seen))
	}
	for i := 0; i < 3; i++ {
		if seen[i].content != "ref content\n" {
			t.Errorf("build %d content = %q, want the ref's", i, seen[i].content)
		}
		if seen[i].layout != "working layout\n" {
			t.Errorf("build %d layout = %q, want the working tree's", i, seen[i].layout)
		}
	}
	if seen[3].content != "working content\n" {
		t.Errorf("dev content = %q, want the working tree's", seen[3].content)
	}
	if seen[3].source != site {
		t.Errorf("dev source = %q, want %q", seen[3].source, site)
	}
	clone := filepath.Join(cfg.Work, "refs", commit)
	if _, err := os.Stat(filepath.Join(clone, "docs", "site", "public")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("public/ was copied from the working tree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(clone, "docs", "site", "resources")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("resources/ was copied from the working tree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(clone, "docs", "site", ".hugo_build.lock")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf(".hugo_build.lock was copied from the working tree: %v", err)
	}
	if _, err := os.Stat(filepath.Join(clone, "docs", "assets", "demo.mp4")); !errors.Is(err, os.ErrNotExist) {
		t.Errorf("ref asset survived the overlay: %v", err)
	}
	if _, err := os.Stat(filepath.Join(clone, "docs", "assets", "working.mp4")); err != nil {
		t.Errorf("working asset is missing: %v", err)
	}
}

// TestHugoEnvironmentIsScrubbed checks that every HUGO-prefixed variable, in
// any case, and the git variables that would redirect a build are gone from
// Hugo's environment, and that GOMAXPROCS carries the job count.
func TestHugoEnvironmentIsScrubbed(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	planted := []string{
		"HUGOxPARAMSxRELEASE",
		"HUGO_params_release",
		"HUGO_BASEURL",
		"HUGO_DISABLEKINDS",
		"GIT_DIR",
		"GIT_WORK_TREE",
		"GIT_INDEX_FILE",
	}
	for _, name := range planted {
		t.Setenv(name, "wrong")
	}
	t.Setenv("GOMAXPROCS", "9")
	fake := &fakeExec{}

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake}); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, c := range fake.callsNamed("hugo") {
		for _, entry := range c.Env {
			name := gitenv.Name(entry)
			for _, want := range planted {
				if name == want {
					t.Errorf("hugo env holds %s", entry)
				}
			}
		}
		gomaxprocs := 0
		for _, entry := range c.Env {
			if strings.HasPrefix(entry, "GOMAXPROCS=") {
				gomaxprocs++
				if entry != "GOMAXPROCS=3" {
					t.Errorf("hugo env holds %s, want GOMAXPROCS=3", entry)
				}
			}
		}
		if gomaxprocs != 1 {
			t.Errorf("hugo env holds GOMAXPROCS %d times, want once", gomaxprocs)
		}
	}
}

// TestHugoArguments checks the strict build invocation and the configuration
// order.
func TestHugoArguments(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	fake := &fakeExec{}

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake}); err != nil {
		t.Fatalf("run: %v", err)
	}
	calls := fake.callsNamed("hugo")
	if len(calls) != 4 {
		t.Fatalf("hugo ran %d times, want 4", len(calls))
	}
	config := flagValue(calls[0].Args, "--config")
	parts := strings.Split(config, ",")
	if len(parts) != 2 {
		t.Fatalf("--config = %q, want two files", config)
	}
	if filepath.Base(parts[0]) != "hugo.yaml" {
		t.Errorf("first config file = %q, want the site's hugo.yaml", parts[0])
	}
	if !strings.HasSuffix(parts[1], ".yaml") {
		t.Errorf("second config file = %q, want the per-build document", parts[1])
	}
	for _, flag := range []string{"--cleanDestinationDir", "--gc", "--minify", "--panicOnWarning"} {
		if !contains(calls[0].Args, flag) {
			t.Errorf("hugo args lack %s", flag)
		}
	}
	dest := flagValue(calls[0].Args, "--destination")
	if !strings.HasPrefix(dest, filepath.Join(cfg.Work, "builds")) {
		t.Errorf("destination = %q, want a directory under the work directory", dest)
	}
}

// TestSiteMetaArguments checks the -class and -canonical-base vectors per
// class.
func TestSiteMetaArguments(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	fake := &fakeExec{}

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake}); err != nil {
		t.Fatalf("run: %v", err)
	}
	calls := fake.callsNamed("go")
	if len(calls) != 4 {
		t.Fatalf("site-meta ran %d times, want 4", len(calls))
	}
	wantClass := []string{ClassLatest, ClassLatestCopy, ClassOlder, ClassDev}
	for i, c := range calls {
		if !contains(c.Args, "./scripts/site-meta") {
			t.Fatalf("call %d is not site-meta: %v", i, c.Args)
		}
		if got := flagValue(c.Args, "-p"); got != "3" {
			t.Errorf("call %d -p = %q, want 3", i, got)
		}
		if got := flagValue(c.Args, "-class"); got != wantClass[i] {
			t.Errorf("call %d class = %q, want %q", i, got, wantClass[i])
		}
		canonical := flagValue(c.Args, "-canonical-base")
		if wantClass[i] == ClassLatestCopy {
			if canonical != "https://example.test/" {
				t.Errorf("latest-copy canonical base = %q", canonical)
			}
		} else if canonical != "" {
			t.Errorf("%s passed -canonical-base %q", wantClass[i], canonical)
		}
		if c.Dir != repo {
			t.Errorf("site-meta ran in %q, want the repository root", c.Dir)
		}
	}
}

// TestLycheeArguments checks the link check over the assembled tree.
func TestLycheeArguments(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	fake := &fakeExec{}

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake}); err != nil {
		t.Fatalf("run: %v", err)
	}
	calls := fake.callsNamed("lychee")
	if len(calls) != 1 {
		t.Fatalf("lychee ran %d times, want 1", len(calls))
	}
	args := calls[0].Args
	for _, want := range []string{"--offline", "--no-progress", "--max-concurrency", "3", "--root-dir", cfg.Public} {
		if !contains(args, want) {
			t.Errorf("lychee args lack %q: %v", want, args)
		}
	}
	if got := args[len(args)-1]; got != filepath.ToSlash(filepath.Join(cfg.Public, "**", "*.html")) {
		t.Errorf("lychee glob = %q", got)
	}
}

// TestAssemblyLayout checks the deployed layout of the assembled tree.
func TestAssemblyLayout(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}}); err != nil {
		t.Fatalf("run: %v", err)
	}
	for _, rel := range []string{"index.html", "docs/index.html", "v0.6.1/index.html", "v0.5.0/index.html", "dev/index.html"} {
		if _, err := os.Stat(filepath.Join(cfg.Public, filepath.FromSlash(rel))); err != nil {
			t.Errorf("assembled tree lacks %s: %v", rel, err)
		}
	}
}

// TestAssemblyUsesEachBuildsOutput checks that every path in the assembled tree
// holds the output of the build that owns it.
func TestAssemblyUsesEachBuildsOutput(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	builds := testBuilds(commit)
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if err := writeMarkdown(dest); err != nil {
			return nil, err
		}
		page := "<html><body>" + filepath.Base(dest) + "</body></html>\n"
		return nil, os.WriteFile(filepath.Join(dest, "index.html"), []byte(page), 0o644)
	}}

	if err := Run(context.Background(), cfg, builds, Deps{Exec: fake}); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := map[string]string{
		"index.html":                          buildID(0, builds[0].Name),
		filepath.Join("v0.6.1", "index.html"): buildID(1, builds[1].Name),
		filepath.Join("v0.5.0", "index.html"): buildID(2, builds[2].Name),
		filepath.Join("dev", "index.html"):    buildID(3, builds[3].Name),
	}
	for rel, id := range want {
		data, err := os.ReadFile(filepath.Join(cfg.Public, rel))
		if err != nil {
			t.Errorf("read %s: %v", rel, err)
			continue
		}
		if !strings.Contains(string(data), id) {
			t.Errorf("%s holds %q, want the output of %s", rel, data, id)
		}
	}
}

// TestMissingMarkdownFails checks that an index.html without its index.md fails
// the index.md check.
func TestMissingMarkdownFails(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if err := writeMarkdown(dest); err != nil {
			return nil, err
		}
		return nil, os.Remove(filepath.Join(dest, "index.md"))
	}}

	err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake})
	var check *CheckError
	if !errors.As(err, &check) || check.Check != "index.md" {
		t.Fatalf("error = %v, want an index.md check failure", err)
	}
	if check.Version != "v0.6.1" {
		t.Errorf("version = %q", check.Version)
	}
	if !strings.Contains(check.Output, "index.html") {
		t.Errorf("output = %q, want the page", check.Output)
	}
}

// TestNestedRobotsTxtFails checks that a robots.txt outside the latest build
// fails assembly.
func TestNestedRobotsTxtFails(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if err := writeMarkdown(dest); err != nil {
			return nil, err
		}
		if strings.Contains(dest, "02-") {
			writeFiles(t, dest, map[string]string{"sub/robots.txt": "user-agent: *\n"})
		}
		return nil, nil
	}}

	err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake})
	assertAssemblyError(t, err, "v0.5.0", "robots.txt")
}

// TestNestedSitemapFails checks that a sitemap.xml outside the latest build
// fails assembly.
func TestNestedSitemapFails(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if err := writeMarkdown(dest); err != nil {
			return nil, err
		}
		if strings.Contains(dest, "03-") {
			writeFiles(t, dest, map[string]string{"deep/sitemap.xml": "<urlset/>\n"})
		}
		return nil, nil
	}}

	err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake})
	assertAssemblyError(t, err, "dev", "sitemap.xml")
}

// TestRootBuildCollisionFails checks that a root build emitting a version
// prefix fails assembly.
func TestRootBuildCollisionFails(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if err := writeMarkdown(dest); err != nil {
			return nil, err
		}
		if strings.Contains(dest, "00-") {
			writeFiles(t, dest, map[string]string{
				"dev/index.html": "page\n",
				"dev/index.md":   "page\n",
			})
		}
		return nil, nil
	}}

	err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake})
	assertAssemblyError(t, err, "v0.6.1", "dev")
}

// assertAssemblyError checks that err is an assembly CheckError for version
// whose output names want.
func assertAssemblyError(t *testing.T, err error, version, want string) {
	t.Helper()
	var check *CheckError
	if !errors.As(err, &check) || check.Check != "assembly" {
		t.Fatalf("error = %v, want an assembly failure", err)
	}
	if check.Version != version {
		t.Errorf("version = %q, want %q", check.Version, version)
	}
	if !strings.Contains(check.Output, want) {
		t.Errorf("output = %q, want it to name %q", check.Output, want)
	}
}

// TestWrongBaseURLCanonicalFails checks that an older build given the root
// base URL, so its pages canonicalise to the root, fails the assembled-tree
// canonical check.
func TestWrongBaseURLCanonicalFails(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	builds := testBuilds(commit)
	for i := range builds {
		if builds[i].Class == ClassOlder {
			builds[i].BaseURL = "https://example.test/"
			builds[i].Config = []byte("baseURL: https://example.test/\n")
		}
	}
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if err := writeMarkdown(dest); err != nil {
			return nil, err
		}
		if strings.Contains(dest, "02-") {
			writeFiles(t, dest, map[string]string{
				"index.html": "<html><head><link rel=\"canonical\" href=\"https://example.test/\"></head></html>\n",
			})
		}
		return nil, nil
	}}

	err := Run(context.Background(), cfg, builds, Deps{Exec: fake})
	var check *CheckError
	if !errors.As(err, &check) || check.Check != "canonical base" {
		t.Fatalf("error = %v, want a canonical base failure", err)
	}
	if check.Version != "v0.5.0" {
		t.Errorf("version = %q, want v0.5.0", check.Version)
	}
	if !strings.Contains(check.Output, "https://example.test/") {
		t.Errorf("output = %q, want the root canonical", check.Output)
	}
}

// TestDevCanonicalToRootFails checks that a dev page canonicalising to the root
// fails the assembled-tree canonical check.
func TestDevCanonicalToRootFails(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	builds := testBuilds(commit)
	index := 0
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if err := writeMarkdown(dest); err != nil {
			return nil, err
		}
		if builds[index].Class == ClassDev {
			page := "<html><head><link rel=\"canonical\" href=\"https://example.test/\"></head></html>\n"
			if err := os.WriteFile(filepath.Join(dest, "index.html"), []byte(page), 0o644); err != nil {
				return nil, err
			}
		}
		index++
		return nil, nil
	}}

	err := Run(context.Background(), cfg, builds, Deps{Exec: fake})
	var check *CheckError
	if !errors.As(err, &check) || check.Check != "canonical base" {
		t.Fatalf("error = %v, want a canonical base failure", err)
	}
	if check.Version != "dev" {
		t.Errorf("version = %q, want dev", check.Version)
	}
	if !strings.Contains(check.Output, "https://example.test/") {
		t.Errorf("output = %q, want the root canonical", check.Output)
	}
}

// TestCorrectCanonicalBasesPass checks that older and dev pages whose canonical
// starts with their own base URL pass the canonical check.
func TestCorrectCanonicalBasesPass(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	builds := testBuilds(commit)
	bases := []string{
		"https://example.test/",
		"https://example.test/",
		"https://example.test/v0.5.0/",
		"https://example.test/dev/",
	}
	index := 0
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return nil, err
		}
		page := fmt.Sprintf("<html><head><link rel=\"canonical\" href=%q></head></html>\n", bases[index])
		index++
		if err := os.WriteFile(filepath.Join(dest, "index.html"), []byte(page), 0o644); err != nil {
			return nil, err
		}
		return nil, os.WriteFile(filepath.Join(dest, "index.md"), []byte("page\n"), 0o644)
	}}

	if err := Run(context.Background(), cfg, builds, Deps{Exec: fake}); err != nil {
		t.Fatalf("run: %v", err)
	}
}

// TestValidationErrors checks the unusable inputs that make the command exit 2.
func TestValidationErrors(t *testing.T) {
	valid := testBuilds("0123456789012345678901234567890123456789")
	tests := []struct {
		name   string
		builds []Build
	}{
		{"empty", nil},
		{"no latest", valid[1:]},
		{"two latest", append([]Build{valid[0]}, valid...)},
		{"bad class", replace(valid, 0, func(b *Build) { b.Class = "newest" })},
		{"no config", replace(valid, 0, func(b *Build) { b.Config = nil })},
		{"both sources", replace(valid, 0, func(b *Build) { b.Source.WorkingTree = true })},
		{"no source", replace(valid, 0, func(b *Build) { b.Source.Commit = "" })},
		{"latest with output", replace(valid, 0, func(b *Build) { b.Output = "v0.6.1/" })},
		{"output without slash", replace(valid, 2, func(b *Build) { b.Output = "v0.5.0" })},
		{"duplicate output", replace(valid, 2, func(b *Build) { b.Output = "dev/" })},
		{"canonical on older", replace(valid, 2, func(b *Build) { b.CanonicalBase = "https://example.test/" })},
		{"latest-copy without canonical", replace(valid, 1, func(b *Build) { b.CanonicalBase = "" })},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			var usage *UsageError
			if err := validateBuilds(tc.builds); !errors.As(err, &usage) {
				t.Fatalf("validateBuilds = %v, want a UsageError", err)
			}
		})
	}
}

// TestWorkAndPublicPaths checks that a work or public directory which would
// delete the repository or site, and a work directory inside the site, are
// usage errors.
func TestWorkAndPublicPaths(t *testing.T) {
	repo, site, commit := newFixture(t)
	aboveRepo := filepath.Dir(repo)
	aboveSite := filepath.Dir(site)
	cases := []struct {
		name   string
		mutate func(*Config)
	}{
		{"work is the repository", func(c *Config) { c.Work = c.Repo }},
		{"work is the site", func(c *Config) { c.Work = c.Site }},
		{"work is above the repository", func(c *Config) { c.Work = aboveRepo }},
		{"work is above the site", func(c *Config) { c.Work = aboveSite }},
		{"work is inside the site", func(c *Config) { c.Work = filepath.Join(c.Site, "work") }},
		{"public is the repository", func(c *Config) { c.Public = c.Repo }},
		{"public is the site", func(c *Config) { c.Public = c.Site }},
		{"public is above the repository", func(c *Config) { c.Public = aboveRepo }},
		{"public is above the site", func(c *Config) { c.Public = aboveSite }},
		{"public is the work directory", func(c *Config) { c.Public = c.Work }},
		{"public contains the work directory", func(c *Config) { c.Public = filepath.Dir(c.Work) }},
		{"public is the site content", func(c *Config) { c.Public = filepath.Join(c.Site, "content") }},
		{"public is inside the site", func(c *Config) { c.Public = filepath.Join(c.Site, "build") }},
		{"public is repository internal", func(c *Config) { c.Public = filepath.Join(c.Repo, "internal") }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cfg := testConfig(t, repo, site)
			tc.mutate(&cfg)
			err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}})
			var usage *UsageError
			if !errors.As(err, &usage) {
				t.Fatalf("error = %v, want a UsageError", err)
			}
		})
	}
}

// TestPublicSiteKeepsContent checks that -public naming the site directory is a
// usage error that leaves the site's content untouched.
func TestPublicSiteKeepsContent(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	cfg.Public = site
	content := filepath.Join(site, "content", "en", "index.md")
	before, err := os.ReadFile(content)
	if err != nil {
		t.Fatalf("read site content: %v", err)
	}

	err = Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}})
	var usage *UsageError
	if !errors.As(err, &usage) {
		t.Fatalf("error = %v, want a UsageError", err)
	}
	after, err := os.ReadFile(content)
	if err != nil {
		t.Fatalf("site content is gone: %v", err)
	}
	if !bytes.Equal(before, after) {
		t.Errorf("site content changed from %q to %q", before, after)
	}
}

// TestPublicSitePublicAccepted checks that -public naming the site's own public
// output directory is accepted and assembled.
func TestPublicSitePublicAccepted(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	cfg.Public = filepath.Join(site, "public")

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}}); err != nil {
		var usage *UsageError
		if errors.As(err, &usage) {
			t.Fatalf("the site's public directory was rejected: %v", err)
		}
		t.Fatalf("run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Public, "index.html")); err != nil {
		t.Errorf("assembled tree lacks index.html: %v", err)
	}
}

// TestPublicSubdirectoryAccepted checks that -public may name a directory below
// the site's own public output directory.
func TestPublicSubdirectoryAccepted(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	cfg.Public = filepath.Join(site, "public", "nested")

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}}); err != nil {
		var usage *UsageError
		if errors.As(err, &usage) {
			t.Fatalf("a subdirectory of the site's public directory was rejected: %v", err)
		}
		t.Fatalf("run: %v", err)
	}
	if _, err := os.Stat(filepath.Join(cfg.Public, "index.html")); err != nil {
		t.Errorf("assembled tree lacks index.html: %v", err)
	}
}

// TestRepositoryBuildDirectoryAccepted checks that -work may be the repository's
// .site-build directory or a directory below it.
func TestRepositoryBuildDirectoryAccepted(t *testing.T) {
	for _, sub := range []string{"", "nested"} {
		t.Run(filepath.Join(".site-build", sub), func(t *testing.T) {
			repo, site, commit := newFixture(t)
			cfg := testConfig(t, repo, site)
			cfg.Work = filepath.Join(repo, ".site-build", sub)

			if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}}); err != nil {
				var usage *UsageError
				if errors.As(err, &usage) {
					t.Fatalf("the repository's build directory was rejected: %v", err)
				}
				t.Fatalf("run: %v", err)
			}
		})
	}
}

// replace returns a copy of builds with mutate applied to one entry.
func replace(builds []Build, index int, mutate func(*Build)) []Build {
	out := append([]Build(nil), builds...)
	mutate(&out[index])
	return out
}

// TestCommandExitCodes checks the three exit codes and the failure message.
func TestCommandExitCodes(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	builds := testBuilds(commit)
	args := []string{
		"-repo", cfg.Repo, "-site", cfg.Site,
		"-work", cfg.Work, "-public", cfg.Public, "-jobs", "3",
	}

	t.Run("success", func(t *testing.T) {
		var stderr strings.Builder
		if code := runBuilds(&stderr, cfg, builds, Deps{Exec: &fakeExec{}}); code != 0 {
			t.Fatalf("exit = %d, want 0; stderr:\n%s", code, stderr.String())
		}
	})
	t.Run("failed check", func(t *testing.T) {
		fake := &fakeExec{hugo: func(call) ([]byte, error) { return []byte("boom\n"), errors.New("exit 1") }}
		var stderr strings.Builder
		if code := runBuilds(&stderr, cfg, builds, Deps{Exec: fake}); code != 1 {
			t.Fatalf("exit = %d, want 1; stderr:\n%s", code, stderr.String())
		}
		got := stderr.String()
		if !strings.Contains(got, "site-versions: hugo build failed for v0.6.1") {
			t.Errorf("stderr lacks the failure line:\n%s", got)
		}
		if !strings.Contains(got, "boom") {
			t.Errorf("stderr lacks the check output:\n%s", got)
		}
	})
	t.Run("bad flag", func(t *testing.T) {
		var stderr strings.Builder
		if code := runWith([]string{"-nope"}, &stderr, Deps{Exec: &fakeExec{}}); code != 2 {
			t.Fatalf("exit = %d, want 2", code)
		}
	})
	t.Run("missing path", func(t *testing.T) {
		var stderr strings.Builder
		if code := runWith([]string{"-repo", cfg.Repo}, &stderr, Deps{Exec: &fakeExec{}}); code != 2 {
			t.Fatalf("exit = %d, want 2", code)
		}
	})
	t.Run("resolver error", func(t *testing.T) {
		badSite := t.TempDir()
		writeFiles(t, badSite, map[string]string{
			"hugo.yaml":     "baseURL: https://example.test/\n",
			"versions.yaml": "oldest: not-a-release\n",
		})
		badArgs := []string{"-repo", repo, "-site", badSite, "-work", cfg.Work, "-public", cfg.Public, "-jobs", "3"}
		var stderr strings.Builder
		if code := runWith(badArgs, &stderr, Deps{Exec: &fakeExec{}}); code != 2 {
			t.Fatalf("exit = %d, want 2; stderr:\n%s", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "versions.yaml") {
			t.Errorf("stderr = %q, want the resolver's error", stderr.String())
		}
	})
	t.Run("releases entry missing fields", func(t *testing.T) {
		resolveRepo, resolveSite := newResolveRepo(t)
		releases := filepath.Join(t.TempDir(), "releases.json")
		if err := os.WriteFile(releases, []byte(`[{"tagName":"v0.6.2"},{"tagName":"v0.6.1"}]`), 0o644); err != nil {
			t.Fatalf("write releases file: %v", err)
		}
		badArgs := []string{
			"-repo", resolveRepo, "-site", resolveSite,
			"-work", filepath.Join(t.TempDir(), "work"),
			"-public", filepath.Join(t.TempDir(), "public"),
			"-jobs", "3", "-releases", releases,
		}
		var stderr strings.Builder
		if code := runWith(badArgs, &stderr, Deps{Exec: &fakeExec{}}); code != 2 {
			t.Fatalf("exit = %d, want 2; stderr:\n%s", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "isDraft") {
			t.Errorf("stderr = %q, want the missing-field error", stderr.String())
		}
	})
	t.Run("as-release with releases", func(t *testing.T) {
		var stderr strings.Builder
		withBoth := append(append([]string(nil), args...), "-as-release", "v0.7.0", "-releases", "releases.json")
		if code := runWith(withBoth, &stderr, Deps{Exec: &fakeExec{}}); code != 2 {
			t.Fatalf("exit = %d, want 2", code)
		}
	})
	t.Run("missing commit", func(t *testing.T) {
		missing := testBuilds(strings.Repeat("f", 40))
		var stderr strings.Builder
		if code := runBuilds(&stderr, cfg, missing, Deps{Exec: &fakeExec{}}); code != 2 {
			t.Fatalf("exit = %d, want 2; stderr:\n%s", code, stderr.String())
		}
		if !strings.Contains(stderr.String(), "ffffffff") {
			t.Errorf("stderr = %q, want the missing commit", stderr.String())
		}
	})
}

// TestHelpExitsZero checks that -h is not an error.
func TestHelpExitsZero(t *testing.T) {
	var stderr strings.Builder
	if code := runWith([]string{"-h"}, &stderr, Deps{Exec: &fakeExec{}}); code != 0 {
		t.Fatalf("exit = %d, want 0", code)
	}
	if !strings.Contains(stderr.String(), "Usage") {
		t.Errorf("usage was not printed:\n%s", stderr.String())
	}
}

// TestBuildLogsVersionClassAndRelease checks the line every build prints before
// Hugo runs.
func TestBuildLogsVersionClassAndRelease(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	var log strings.Builder

	if err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: &fakeExec{}, Log: &log}); err != nil {
		t.Fatalf("run: %v", err)
	}
	want := []string{
		"site-versions: v0.6.1 (latest) params.release=0.6.1",
		"site-versions: v0.6.1 (latest-copy) params.release=0.6.1",
		"site-versions: v0.5.0 (older) params.release=0.5.0",
		"site-versions: dev (dev) params.release=0.6.1",
	}
	lines := strings.Split(strings.TrimSpace(log.String()), "\n")
	if len(lines) != len(want) {
		t.Fatalf("printed %d lines, want %d:\n%s", len(lines), len(want), log.String())
	}
	for i, line := range want {
		if lines[i] != line {
			t.Errorf("line %d = %q, want %q", i, lines[i], line)
		}
	}
}

// TestFailedBuildLogsItsLine checks that the failing build's line is written
// before Hugo runs, so the log ends with the build that failed rather than the
// one before it.
func TestFailedBuildLogsItsLine(t *testing.T) {
	repo, site, commit := newFixture(t)
	cfg := testConfig(t, repo, site)
	var log strings.Builder
	fake := &fakeExec{hugo: func(c call) ([]byte, error) {
		dest := flagValue(c.Args, "--destination")
		if strings.Contains(dest, "02-") {
			return []byte("hugo broke\n"), errors.New("exit 1")
		}
		return nil, writeMarkdown(dest)
	}}

	err := Run(context.Background(), cfg, testBuilds(commit), Deps{Exec: fake, Log: &log})
	var check *CheckError
	if !errors.As(err, &check) || check.Check != "hugo build" {
		t.Fatalf("error = %v, want a hugo build failure", err)
	}
	lines := strings.Split(strings.TrimSpace(log.String()), "\n")
	if log.Len() == 0 {
		t.Fatal("no build line was logged")
	}
	if last := lines[len(lines)-1]; last != "site-versions: v0.5.0 (older) params.release=0.5.0" {
		t.Errorf("last log line = %q, want the failing build's line", last)
	}
}

// TestMissingCommitIsNotCloned checks that a commit absent from the repository
// is rejected before any checkout.
func TestMissingCommitIsNotCloned(t *testing.T) {
	repo, site, _ := newFixture(t)
	cfg := testConfig(t, repo, site)
	builds := testBuilds(strings.Repeat("e", 40))
	err := Run(context.Background(), cfg, builds, Deps{Exec: &fakeExec{}})
	var usage *UsageError
	if !errors.As(err, &usage) {
		t.Fatalf("error = %v, want a UsageError", err)
	}
	if entries, err := os.ReadDir(filepath.Join(cfg.Work, "refs")); err == nil && len(entries) > 0 {
		t.Fatalf("a checkout was made for a missing commit")
	}
}

// contains reports whether values holds want.
func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}
