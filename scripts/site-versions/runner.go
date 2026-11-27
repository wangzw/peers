package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"golang.org/x/net/html"

	"github.com/wangzw/peers/scripts/site-versions/gitenv"
)

// Build is one version of the website to build and check. The version-set
// resolver fills the list; the runner consumes it.
type Build struct {
	// Name is the version name: vX.Y.Z or dev.
	Name string
	// Class is one of latest, latest-copy, older and dev.
	Class string
	// Source says where the version's content comes from.
	Source Source
	// BaseURL is the version's base URL with a trailing slash.
	BaseURL string
	// Output is the path under the assembled root: empty for latest,
	// vX.Y.Z/ for a release and dev/ for the working tree.
	Output string
	// Config is the per-build Hugo configuration document.
	Config []byte
	// Release is params.release as Config carries it.
	Release string
	// CanonicalBase is the -canonical-base value for site-meta; it is set
	// only for latest-copy.
	CanonicalBase string
}

// Source is the origin of a build's content: a commit to check out, or the
// working tree.
type Source struct {
	// Commit is the ref to check out.
	Commit string
	// WorkingTree selects the working tree instead of Commit.
	WorkingTree bool
}

// Version classes.
const (
	// ClassLatest is the build at the root of the deployed site.
	ClassLatest = "latest"
	// ClassLatestCopy is the latest release's second build under its vX.Y.Z/.
	ClassLatestCopy = "latest-copy"
	// ClassOlder is a published release other than the latest.
	ClassOlder = "older"
	// ClassDev is the working tree's build under /dev/.
	ClassDev = "dev"
)

// Config holds the paths and the job count the command was given.
type Config struct {
	// Repo is the repository root whose refs are checked out.
	Repo string
	// Site is the working tree's site directory.
	Site string
	// Work is the directory the checkouts and builds go to.
	Work string
	// Public is the assembled output directory.
	Public string
	// Jobs bounds the parallel work of Hugo and the link check.
	Jobs int
}

// Deps holds the external commands the runner cannot perform itself.
type Deps struct {
	// Exec runs Hugo, site-meta and lychee.
	Exec Execer
	// Log receives one line per build before Hugo runs; nil means standard
	// output.
	Log io.Writer
}

// Execer runs one command and returns its combined output.
type Execer interface {
	Run(ctx context.Context, opts ExecOptions) ([]byte, error)
}

// ExecOptions describes one command invocation.
type ExecOptions struct {
	// Dir is the command's working directory.
	Dir string
	// Env is the command's environment.
	Env []string
	// Name is the executable.
	Name string
	// Args are its arguments.
	Args []string
}

// UsageError reports bad flags or unusable inputs; the command exits 2.
type UsageError struct {
	// Message is written after the command name.
	Message string
}

func (e *UsageError) Error() string {
	return e.Message
}

// CheckError reports a failed check; the command exits 1.
type CheckError struct {
	// Check names the failed check.
	Check string
	// Version names the version it ran for.
	Version string
	// Output is the check's own output.
	Output string
}

func (e *CheckError) Error() string {
	if e.Version == "" {
		return e.Check + " failed"
	}
	return fmt.Sprintf("%s failed for %s", e.Check, e.Version)
}

// Run builds every version, checks each build, assembles the versions into the
// public tree and checks the internal links of the assembled tree.
func Run(ctx context.Context, cfg Config, builds []Build, deps Deps) error {
	if err := validateConfig(cfg); err != nil {
		return err
	}
	absolute, err := absolutePaths(cfg)
	if err != nil {
		return &UsageError{Message: err.Error()}
	}
	cfg = absolute
	if err := validatePaths(cfg); err != nil {
		return err
	}
	if err := validateBuilds(builds); err != nil {
		return err
	}
	if deps.Exec == nil {
		deps.Exec = commandExec{}
	}
	log := deps.Log
	if log == nil {
		log = os.Stdout
	}

	if err := emptyDir(cfg.Work); err != nil {
		return fmt.Errorf("prepare work directory: %w", err)
	}
	if err := emptyDir(cfg.Public); err != nil {
		return fmt.Errorf("prepare output directory: %w", err)
	}

	refs := newRefCheckouts(cfg)
	buildDirs := make([]string, len(builds))
	for i, build := range builds {
		id := buildID(i, build.Name)
		siteDir, err := refs.site(ctx, build.Source)
		if err != nil {
			return err
		}
		configPath := filepath.Join(cfg.Work, "configs", id+".yaml")
		if err := writeFile(configPath, build.Config); err != nil {
			return fmt.Errorf("write configuration for %s: %w", build.Name, err)
		}
		dest := filepath.Join(cfg.Work, "builds", id)
		if err := os.MkdirAll(dest, 0o755); err != nil {
			return fmt.Errorf("prepare build directory for %s: %w", build.Name, err)
		}
		buildDirs[i] = dest

		_, _ = fmt.Fprintf(log, "site-versions: %s (%s) params.release=%s\n", build.Name, build.Class, build.Release)
		if output, err := deps.Exec.Run(ctx, hugoOptions(cfg, siteDir, configPath, dest)); err != nil {
			return &CheckError{Check: "hugo build", Version: build.Name, Output: string(output)}
		}
		if missing := missingMarkdown(dest); len(missing) > 0 {
			return &CheckError{Check: "index.md", Version: build.Name, Output: strings.Join(missing, "\n")}
		}
		if output, err := deps.Exec.Run(ctx, siteMetaOptions(cfg, build, dest)); err != nil {
			return &CheckError{Check: "site-meta", Version: build.Name, Output: string(output)}
		}
	}

	if err := assemble(cfg.Public, builds, buildDirs); err != nil {
		return err
	}
	if err := checkCanonicalBases(cfg.Public, builds); err != nil {
		return err
	}
	if output, err := deps.Exec.Run(ctx, lycheeOptions(cfg)); err != nil {
		return &CheckError{Check: "lychee", Version: "assembled site", Output: string(output)}
	}
	return nil
}

func validateConfig(cfg Config) error {
	paths := []struct{ name, value string }{
		{"-repo", cfg.Repo},
		{"-site", cfg.Site},
		{"-work", cfg.Work},
		{"-public", cfg.Public},
	}
	for _, path := range paths {
		if path.value == "" {
			return &UsageError{Message: path.name + " is required"}
		}
	}
	if cfg.Jobs <= 0 {
		return &UsageError{Message: "-jobs must be positive"}
	}
	for _, dir := range []string{cfg.Repo, cfg.Site} {
		if info, err := os.Stat(dir); err != nil {
			return &UsageError{Message: fmt.Sprintf("%s: %v", dir, err)}
		} else if !info.IsDir() {
			return &UsageError{Message: dir + " is not a directory"}
		}
	}
	return nil
}

// absolutePaths resolves the command's paths against the working directory, so
// Hugo reads the configuration files by absolute path whatever it treats as
// their base.
func absolutePaths(cfg Config) (Config, error) {
	for _, path := range []*string{&cfg.Repo, &cfg.Site, &cfg.Work, &cfg.Public} {
		absolute, err := filepath.Abs(*path)
		if err != nil {
			return Config{}, err
		}
		*path = absolute
	}
	return cfg, nil
}

// validatePaths refuses a work or public directory whose emptying would delete
// the repository or the site, a work directory inside the repository but
// outside its .site-build, a public directory inside the repository but outside
// the site's own public output, a work directory inside the site, and a public
// directory that would delete the work directory.
func validatePaths(cfg Config) error {
	protected := []struct {
		name string
		dir  string
	}{
		{"the repository", cfg.Repo},
		{"the site", cfg.Site},
	}
	for _, dir := range []struct{ flag, path string }{
		{"-work", cfg.Work},
		{"-public", cfg.Public},
	} {
		for _, p := range protected {
			if dir.path == p.dir || containsPath(dir.path, p.dir) {
				return &UsageError{Message: fmt.Sprintf("%s %s would delete %s", dir.flag, dir.path, p.name)}
			}
		}
	}
	if containsPath(cfg.Site, cfg.Work) {
		return &UsageError{Message: fmt.Sprintf("-work %s is inside the site %s", cfg.Work, cfg.Site)}
	}
	workRoot := filepath.Join(cfg.Repo, ".site-build")
	if insidePath(cfg.Repo, cfg.Work) && !insidePath(workRoot, cfg.Work) {
		return &UsageError{Message: fmt.Sprintf("-work %s is inside the repository %s; only %s may hold the build", cfg.Work, cfg.Repo, workRoot)}
	}
	sitePublic := filepath.Join(cfg.Site, "public")
	if insidePath(cfg.Repo, cfg.Public) && !insidePath(sitePublic, cfg.Public) {
		return &UsageError{Message: fmt.Sprintf("-public %s is inside the repository %s; only %s may hold its output", cfg.Public, cfg.Repo, sitePublic)}
	}
	if cfg.Public == cfg.Work || containsPath(cfg.Public, cfg.Work) {
		return &UsageError{Message: fmt.Sprintf("-public %s would delete the work directory %s", cfg.Public, cfg.Work)}
	}
	return nil
}

// containsPath reports whether child lies strictly inside parent.
func containsPath(parent, child string) bool {
	rel, err := filepath.Rel(parent, child)
	if err != nil {
		return false
	}
	return rel != "." && rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator))
}

// insidePath reports whether path is dir or lies inside it.
func insidePath(dir, path string) bool {
	return path == dir || containsPath(dir, path)
}

func validateBuilds(builds []Build) error {
	if len(builds) == 0 {
		return &UsageError{Message: "the build list is empty"}
	}
	outputs := make(map[string]bool)
	latest := 0
	for _, build := range builds {
		if build.Name == "" {
			return &UsageError{Message: "a build has no version name"}
		}
		switch build.Class {
		case ClassLatest:
			latest++
		case ClassLatestCopy, ClassOlder, ClassDev:
		default:
			return &UsageError{Message: fmt.Sprintf("build %s has unknown class %q", build.Name, build.Class)}
		}
		if (build.Source.Commit == "") == !build.Source.WorkingTree {
			return &UsageError{Message: fmt.Sprintf("build %s must name a commit or the working tree", build.Name)}
		}
		if build.BaseURL == "" {
			return &UsageError{Message: fmt.Sprintf("build %s has no base URL", build.Name)}
		}
		if len(build.Config) == 0 {
			return &UsageError{Message: fmt.Sprintf("build %s has no configuration", build.Name)}
		}
		if build.Class == ClassLatestCopy {
			if build.CanonicalBase == "" {
				return &UsageError{Message: fmt.Sprintf("build %s is latest-copy but has no canonical base", build.Name)}
			}
		} else if build.CanonicalBase != "" {
			return &UsageError{Message: fmt.Sprintf("build %s has a canonical base but is %s", build.Name, build.Class)}
		}
		if build.Class == ClassLatest && build.Output != "" {
			return &UsageError{Message: fmt.Sprintf("build %s is latest but has output %q", build.Name, build.Output)}
		}
		if build.Class != ClassLatest && !validOutput(build.Output) {
			return &UsageError{Message: fmt.Sprintf("build %s has invalid output %q", build.Name, build.Output)}
		}
		if outputs[build.Output] {
			return &UsageError{Message: fmt.Sprintf("two builds share the output path %q", build.Output)}
		}
		outputs[build.Output] = true
	}
	if latest != 1 {
		return &UsageError{Message: fmt.Sprintf("the build list has %d latest builds, want 1", latest)}
	}
	return nil
}

func validOutput(output string) bool {
	if !strings.HasSuffix(output, "/") {
		return false
	}
	name := strings.TrimSuffix(output, "/")
	return name != "" && name != "." && name != ".." && !strings.Contains(name, "/") && !filepath.IsAbs(name)
}

// buildID returns the work-directory name for one build. Version names are not
// unique: the latest release is built twice.
func buildID(index int, name string) string {
	safe := strings.Map(func(r rune) rune {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '.', r == '_', r == '-':
			return r
		default:
			return '-'
		}
	}, name)
	return fmt.Sprintf("%02d-%s", index, safe)
}

// refCheckouts checks out one ref per commit and overlays the working tree's
// site files on it.
type refCheckouts struct {
	cfg  Config
	dirs map[string]string
}

func newRefCheckouts(cfg Config) *refCheckouts {
	return &refCheckouts{cfg: cfg, dirs: make(map[string]string)}
}

func (r *refCheckouts) site(ctx context.Context, source Source) (string, error) {
	if source.WorkingTree {
		return r.cfg.Site, nil
	}
	if dir, ok := r.dirs[source.Commit]; ok {
		return filepath.Join(dir, "docs", "site"), nil
	}
	dir, err := r.clone(ctx, source.Commit)
	if err != nil {
		return "", err
	}
	r.dirs[source.Commit] = dir
	return filepath.Join(dir, "docs", "site"), nil
}

// clone makes a sparse local clone and overlays the working tree's files on
// it. The clone keeps the full history, which Hugo needs for its page dates,
// and is not a worktree, which would register in the repository.
func (r *refCheckouts) clone(ctx context.Context, commit string) (string, error) {
	if out, err := runGit(ctx, r.cfg.Repo, "cat-file", "-e", commit+"^{commit}"); err != nil {
		return "", &UsageError{Message: fmt.Sprintf("commit %s: %s", commit, commandDetail(out, err))}
	}
	dir := filepath.Join(r.cfg.Work, "refs", commit)
	if err := os.MkdirAll(filepath.Dir(dir), 0o755); err != nil {
		return "", err
	}
	type step struct {
		dir  string
		args []string
	}
	steps := []step{
		{r.cfg.Repo, []string{"clone", "--no-checkout", "--local", "--", r.cfg.Repo, dir}},
		{dir, []string{"sparse-checkout", "init", "--no-cone"}},
		{dir, []string{"sparse-checkout", "set", "--no-cone", "/docs/site/", "/docs/assets/"}},
		{dir, []string{"checkout", "--detach", commit}},
	}
	for _, step := range steps {
		if out, err := runGit(ctx, step.dir, step.args...); err != nil {
			return "", &UsageError{Message: fmt.Sprintf("check out %s: %s", commit, commandDetail(out, err))}
		}
	}
	siteDir := filepath.Join(dir, "docs", "site")
	if info, err := os.Stat(siteDir); err != nil || !info.IsDir() {
		return "", &UsageError{Message: fmt.Sprintf("commit %s has no docs/site directory", commit)}
	}
	if err := overlaySite(r.cfg.Site, siteDir); err != nil {
		return "", fmt.Errorf("overlay site files on %s: %w", commit, err)
	}
	if err := overlayAssets(filepath.Join(filepath.Dir(r.cfg.Site), "assets"), filepath.Join(dir, "docs", "assets")); err != nil {
		return "", fmt.Errorf("overlay assets on %s: %w", commit, err)
	}
	return dir, nil
}

// siteFilesSkipped lists the top-level site entries a ref build does not take
// from the working tree: content comes from the ref, and the rest is build
// output.
var siteFilesSkipped = map[string]bool{
	"content":          true,
	"public":           true,
	"resources":        true,
	".hugo_build.lock": true,
}

// overlaySite replaces every top-level entry of dest except content with the
// working tree's copy from source.
func overlaySite(source, dest string) error {
	entries, err := os.ReadDir(dest)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.Name() == "content" {
			continue
		}
		if err := os.RemoveAll(filepath.Join(dest, entry.Name())); err != nil {
			return err
		}
	}
	return copyEntries(source, dest, func(name string) bool { return !siteFilesSkipped[name] })
}

func overlayAssets(source, dest string) error {
	if err := os.RemoveAll(dest); err != nil {
		return err
	}
	if _, err := os.Stat(source); errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	return copyTree(source, dest)
}

func copyEntries(source, dest string, keep func(string) bool) error {
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if !keep(entry.Name()) {
			continue
		}
		if err := copyTree(filepath.Join(source, entry.Name()), filepath.Join(dest, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func copyTree(source, dest string) error {
	info, err := os.Lstat(source)
	if err != nil {
		return err
	}
	if !info.IsDir() {
		if !info.Mode().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(source)
		if err != nil {
			return err
		}
		if err := os.MkdirAll(filepath.Dir(dest), 0o755); err != nil {
			return err
		}
		return os.WriteFile(dest, data, info.Mode().Perm())
	}
	if err := os.MkdirAll(dest, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(source)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := copyTree(filepath.Join(source, entry.Name()), filepath.Join(dest, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

func missingMarkdown(dir string) []string {
	var missing []string
	_ = filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil || entry.IsDir() || entry.Name() != "index.html" {
			return nil
		}
		markdown := strings.TrimSuffix(path, ".html") + ".md"
		if info, err := os.Stat(markdown); err != nil || info.IsDir() {
			rel, relErr := filepath.Rel(dir, path)
			if relErr != nil {
				rel = path
			}
			missing = append(missing, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(missing)
	return missing
}

func assemble(public string, builds []Build, dirs []string) error {
	root := -1
	for i, build := range builds {
		if build.Output == "" {
			root = i
		}
	}
	if root < 0 {
		return &UsageError{Message: "the build list has no latest build"}
	}
	for i, build := range builds {
		if i == root {
			continue
		}
		prefix := strings.TrimSuffix(build.Output, "/")
		if _, err := os.Lstat(filepath.Join(dirs[root], prefix)); err == nil {
			return &CheckError{
				Check:   "assembly",
				Version: builds[root].Name,
				Output:  fmt.Sprintf("%s is also the output path of %s", prefix, build.Name),
			}
		}
		bad, err := singletons(dirs[i])
		if err != nil {
			return err
		}
		if len(bad) > 0 {
			return &CheckError{
				Check:   "assembly",
				Version: build.Name,
				Output:  "robots.txt and sitemap.xml may only come from the latest build:\n" + strings.Join(bad, "\n"),
			}
		}
	}
	if err := copyTree(dirs[root], public); err != nil {
		return fmt.Errorf("assemble %s: %w", builds[root].Name, err)
	}
	for i, build := range builds {
		if i == root {
			continue
		}
		if err := copyTree(dirs[i], filepath.Join(public, strings.TrimSuffix(build.Output, "/"))); err != nil {
			return fmt.Errorf("assemble %s: %w", build.Name, err)
		}
	}
	return nil
}

func singletons(dir string) ([]string, error) {
	var found []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		if entry.Name() == "robots.txt" || entry.Name() == "sitemap.xml" {
			rel, relErr := filepath.Rel(dir, path)
			if relErr != nil {
				rel = path
			}
			found = append(found, filepath.ToSlash(rel))
		}
		return nil
	})
	sort.Strings(found)
	return found, err
}

// checkCanonicalBases verifies that every canonical URL of an older or dev
// build in the assembled tree starts with the base URL the build is served
// under. That base URL comes from the assembled layout, the root base URL plus
// the build's output path, so a build given another base URL, such as the root
// one, fails. The latest and latest-copy builds are not checked: their
// canonical points at the root by design.
func checkCanonicalBases(public string, builds []Build) error {
	root := ""
	for _, build := range builds {
		if build.Class == ClassLatest {
			root = build.BaseURL
		}
	}
	for _, build := range builds {
		if build.Class != ClassOlder && build.Class != ClassDev {
			continue
		}
		base := root + build.Output
		dir := filepath.Join(public, strings.TrimSuffix(build.Output, "/"))
		problems, err := canonicalProblems(public, dir, base)
		if err != nil {
			return err
		}
		if len(problems) > 0 {
			return &CheckError{
				Check:   "canonical base",
				Version: build.Name,
				Output:  strings.Join(problems, "\n"),
			}
		}
	}
	return nil
}

// canonicalProblems returns one message per page under dir whose canonical URL
// lies outside base.
func canonicalProblems(public, dir, base string) ([]string, error) {
	var problems []string
	err := filepath.WalkDir(dir, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || entry.Name() != "index.html" {
			return nil
		}
		canonical, err := canonicalURL(path)
		if err != nil {
			return err
		}
		if canonical == "" || strings.HasPrefix(canonical, base) {
			return nil
		}
		rel, relErr := filepath.Rel(public, path)
		if relErr != nil {
			rel = path
		}
		problems = append(problems, fmt.Sprintf("%s: canonical %q does not start with %s", filepath.ToSlash(rel), canonical, base))
		return nil
	})
	sort.Strings(problems)
	return problems, err
}

// canonicalURL returns the href of the page's canonical link, or "" when it has
// none.
func canonicalURL(path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	doc, err := html.Parse(bytes.NewReader(data))
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", path, err)
	}
	var canonical string
	var walk func(*html.Node)
	walk = func(n *html.Node) {
		if n.Type == html.ElementNode && n.Data == "link" {
			rel, href := "", ""
			for _, a := range n.Attr {
				switch a.Key {
				case "rel":
					rel = a.Val
				case "href":
					href = a.Val
				}
			}
			for _, token := range strings.Fields(rel) {
				if token == "canonical" && canonical == "" {
					canonical = href
				}
			}
		}
		for c := n.FirstChild; c != nil; c = c.NextSibling {
			walk(c)
		}
	}
	walk(doc)
	return canonical, nil
}

func hugoOptions(cfg Config, siteDir, configPath, dest string) ExecOptions {
	return ExecOptions{
		Dir:  cfg.Repo,
		Env:  hugoEnv(cfg.Jobs),
		Name: "hugo",
		Args: []string{
			"--source", siteDir,
			"--destination", dest,
			"--config", filepath.Join(siteDir, "hugo.yaml") + "," + configPath,
			"--cleanDestinationDir", "--gc", "--minify", "--panicOnWarning",
		},
	}
}

// hugoEnv drops the inherited Hugo variables that outrank the configuration
// files, the git variables that would redirect the build's git calls, and adds
// the job count.
func hugoEnv(jobs int) []string {
	env := make([]string, 0, len(os.Environ())+1)
	for _, entry := range os.Environ() {
		key := gitenv.Name(entry)
		if key == "GOMAXPROCS" || isHugoVariable(key) || gitenv.IsVariable(key) {
			continue
		}
		env = append(env, entry)
	}
	return append(env, "GOMAXPROCS="+strconv.Itoa(jobs))
}

// isHugoVariable reports whether key starts with HUGO in any case. Hugo itself
// matches the uppercase prefix only; this deliberately drops a larger set, so a
// variable Hugo reads is never inherited whatever its case.
func isHugoVariable(key string) bool {
	return len(key) >= 4 && strings.EqualFold(key[:4], "HUGO")
}

func siteMetaOptions(cfg Config, build Build, dest string) ExecOptions {
	args := []string{"run", "-p", strconv.Itoa(cfg.Jobs), "./scripts/site-meta", "-public", dest, "-class", build.Class}
	if build.Class == ClassLatestCopy {
		args = append(args, "-canonical-base", build.CanonicalBase)
	}
	return ExecOptions{Dir: cfg.Repo, Env: os.Environ(), Name: "go", Args: args}
}

func lycheeOptions(cfg Config) ExecOptions {
	return ExecOptions{
		Dir:  cfg.Repo,
		Env:  os.Environ(),
		Name: "lychee",
		Args: []string{
			"--offline", "--no-progress", "--max-concurrency", strconv.Itoa(cfg.Jobs),
			"--root-dir", cfg.Public, filepath.ToSlash(filepath.Join(cfg.Public, "**", "*.html")),
		},
	}
}

// emptyDir removes every entry inside dir and keeps dir itself.
func emptyDir(dir string) error {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return err
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if err := os.RemoveAll(filepath.Join(dir, entry.Name())); err != nil {
			return err
		}
	}
	return nil
}

// writeFile creates path's parent directory and writes data.
func writeFile(path string, data []byte) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o644)
}

// commandExec runs commands from the environment's PATH.
type commandExec struct{}

func (commandExec) Run(ctx context.Context, opts ExecOptions) ([]byte, error) {
	cmd := exec.CommandContext(ctx, opts.Name, opts.Args...)
	cmd.Dir = opts.Dir
	cmd.Env = opts.Env
	return cmd.CombinedOutput()
}

func runGit(ctx context.Context, dir string, args ...string) ([]byte, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = gitenv.Env()
	return cmd.CombinedOutput()
}

func commandDetail(output []byte, err error) string {
	if detail := strings.TrimSpace(string(output)); detail != "" {
		return detail
	}
	return err.Error()
}
