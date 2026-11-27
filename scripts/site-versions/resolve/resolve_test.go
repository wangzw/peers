package resolve

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"testing"
)

const testBaseURL = "https://example.test/"

// fixture is a temporary git repository with a site directory.
type fixture struct {
	t    *testing.T
	repo string
	site string
	shas map[string]string
}

func newFixture(t *testing.T) *fixture {
	t.Helper()
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	f := &fixture{t: t, repo: t.TempDir(), shas: map[string]string{}}
	f.git("init", "-q")
	f.git("config", "user.email", "test@example.com")
	f.git("config", "user.name", "Test")
	f.git("config", "commit.gpgsign", "false")
	f.site = filepath.Join(f.repo, "docs", "site")
	if err := os.MkdirAll(f.site, 0o755); err != nil {
		t.Fatalf("create site directory: %v", err)
	}
	f.write("docs/site/hugo.yaml", "baseURL: "+testBaseURL+"\ndisableKinds:\n  - taxonomy\n  - term\n")
	return f
}

// write creates a file under the repository.
func (f *fixture) write(rel, content string) {
	f.t.Helper()
	path := filepath.Join(f.repo, filepath.FromSlash(rel))
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		f.t.Fatalf("create directory for %s: %v", rel, err)
	}
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		f.t.Fatalf("write %s: %v", rel, err)
	}
}

// git runs a git command in the fixture repository.
func (f *fixture) git(args ...string) string {
	f.t.Helper()
	cmd := exec.Command("git", append([]string{"-C", f.repo}, args...)...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		f.t.Fatalf("git %v: %v\n%s", args, err, out)
	}
	return strings.TrimSpace(string(out))
}

// commit creates a commit labelled with a distinct file and returns its hash.
func (f *fixture) commit(label string) string {
	f.t.Helper()
	f.write("file-"+label, label)
	f.git("add", "file-"+label)
	f.git("commit", "-q", "--no-gpg-sign", "-m", label)
	sha := f.git("rev-parse", "HEAD")
	f.shas[label] = sha
	return sha
}

// tag tags the commit with the given label.
func (f *fixture) tag(name, label string) {
	f.t.Helper()
	sha, ok := f.shas[label]
	if !ok {
		f.t.Fatalf("no commit labelled %q", label)
	}
	f.git("tag", name, sha)
}

// standardVersions creates tags below, at and above the oldest, a prerelease
// tag, and an untagged commit for a refs override.
func (f *fixture) standardVersions() {
	f.t.Helper()
	f.commit("v050")
	f.tag("v0.5.0", "v050")
	f.commit("v061")
	f.tag("v0.6.1", "v061")
	f.commit("v062")
	f.tag("v0.6.2", "v062")
	f.commit("rc")
	f.tag("v0.7.0-rc1", "rc")
	f.commit("v090")
	f.tag("v0.9.0", "v090")
	f.commit("v100")
	f.tag("v0.10.0", "v100")
	f.commit("ref061")
}

// versions writes docs/site/versions.yaml.
func (f *fixture) versions(oldest string, refs map[string]string) {
	f.t.Helper()
	var b strings.Builder
	fmt.Fprintf(&b, "oldest: %s\n", oldest)
	if len(refs) > 0 {
		b.WriteString("refs:\n")
		keys := make([]string, 0, len(refs))
		for key := range refs {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			fmt.Fprintf(&b, "  %s: %s\n", key, refs[key])
		}
	}
	f.write("docs/site/versions.yaml", b.String())
}

func (f *fixture) resolve(opts Options) ([]Build, error) {
	f.t.Helper()
	opts.Repo = f.repo
	opts.Site = f.site
	return Resolve(opts)
}

func (f *fixture) mustResolve(opts Options) []Build {
	f.t.Helper()
	builds, err := f.resolve(opts)
	if err != nil {
		f.t.Fatalf("Resolve: %v", err)
	}
	return builds
}

func wantErrorContains(t *testing.T, err error, fragments ...string) {
	t.Helper()
	if err == nil {
		t.Fatalf("Resolve succeeded, want an error containing %q", fragments)
	}
	for _, fragment := range fragments {
		if !strings.Contains(err.Error(), fragment) {
			t.Fatalf("Resolve error %q does not contain %q", err, fragment)
		}
	}
}

// buildKey is the part of a build that identifies it and its place in the
// assembled tree.
type buildKey struct {
	name   string
	class  Class
	output string
}

func buildKeys(builds []Build) []buildKey {
	keys := make([]buildKey, 0, len(builds))
	for _, b := range builds {
		keys = append(keys, buildKey{b.Name, b.Class, b.OutputPath})
	}
	return keys
}

// buildFor returns the single build with the given name and class.
func buildFor(t *testing.T, builds []Build, name string, class Class) Build {
	t.Helper()
	var found []Build
	for _, b := range builds {
		if b.Name == name && b.Class == class {
			found = append(found, b)
		}
	}
	if len(found) != 1 {
		t.Fatalf("got %d builds named %s with class %s, want 1", len(found), name, class)
	}
	return found[0]
}

// TestResolveTags checks the tag version set, its semantic order, the exclusions and the refs override.
func TestResolveTags(t *testing.T) {
	f := newFixture(t)
	f.standardVersions()
	f.versions("v0.6.1", map[string]string{"v0.6.1": f.shas["ref061"]})

	builds := f.mustResolve(Options{})

	want := []buildKey{
		{"v0.10.0", ClassLatest, ""},
		{"v0.10.0", ClassLatestCopy, "v0.10.0/"},
		{"v0.9.0", ClassOlder, "v0.9.0/"},
		{"v0.6.2", ClassOlder, "v0.6.2/"},
		{"v0.6.1", ClassOlder, "v0.6.1/"},
		{"dev", ClassDev, "dev/"},
	}
	if got := buildKeys(builds); !reflect.DeepEqual(got, want) {
		t.Fatalf("builds = %v, want %v", got, want)
	}

	for _, b := range builds {
		if b.Name == "v0.7.0-rc1" || b.Name == "v0.5.0" {
			t.Errorf("build %s must not be in the set", b.Name)
		}
	}

	if got := buildFor(t, builds, "v0.10.0", ClassLatest).Source.Commit; got != f.shas["v100"] {
		t.Errorf("v0.10.0 latest source = %s, want the v0.10.0 tag commit %s", got, f.shas["v100"])
	}
	if got := buildFor(t, builds, "v0.6.2", ClassOlder).Source.Commit; got != f.shas["v062"] {
		t.Errorf("v0.6.2 source = %s, want its tag commit %s", got, f.shas["v062"])
	}

	overridden := buildFor(t, builds, "v0.6.1", ClassOlder)
	if overridden.Source.Commit == f.shas["v061"] {
		t.Errorf("v0.6.1 was built from its tag %s, want the refs override", f.shas["v061"])
	}
	if overridden.Source.Commit != f.shas["ref061"] {
		t.Errorf("v0.6.1 source = %s, want %s", overridden.Source.Commit, f.shas["ref061"])
	}

	if !buildFor(t, builds, "dev", ClassDev).Source.WorkingTree {
		t.Error("dev build must come from the working tree")
	}
}

// TestGitEnvironmentIsFiltered checks that variables which would point git at
// another repository, index or configuration do not reach the resolver's tag
// lookup.
func TestGitEnvironmentIsFiltered(t *testing.T) {
	f := newFixture(t)
	f.commit("v061")
	f.tag("v0.6.1", "v061")
	f.versions("v0.6.1", nil)

	elsewhere := t.TempDir()
	t.Setenv("GIT_DIR", filepath.Join(elsewhere, ".git"))
	t.Setenv("GIT_WORK_TREE", elsewhere)
	t.Setenv("GIT_CONFIG_COUNT", "1")
	t.Setenv("GIT_CONFIG_KEY_0", "core.bare")
	t.Setenv("GIT_CONFIG_VALUE_0", "true")

	builds := f.mustResolve(Options{})
	if got := buildFor(t, builds, "v0.6.1", ClassLatest).Source.Commit; got != f.shas["v061"] {
		t.Errorf("v0.6.1 source = %s, want the tag commit %s", got, f.shas["v061"])
	}
}

// TestVersionsFileErrors checks each versions.yaml validation error.
func TestVersionsFileErrors(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    []string
	}{
		{"unknown key", "oldest: v0.6.1\nunknown: x\n", []string{"field unknown not found"}},
		{"missing oldest", "refs: {}\n", []string{"oldest"}},
		{"malformed oldest", "oldest: 0.6.1\n", []string{"oldest"}},
		{"prerelease oldest", "oldest: v0.6.1-rc1\n", []string{"oldest"}},
		{"malformed ref key", "oldest: v0.6.1\nrefs:\n  v0.6: SHA\n", []string{"ref", "vX.Y.Z"}},
		{"ref below oldest", "oldest: v0.6.2\nrefs:\n  v0.6.1: SHA\n", []string{"below oldest"}},
		{"ref not 40-hex", "oldest: v0.6.1\nrefs:\n  v0.6.1: abc123\n", []string{"40-hex"}},
		{"ref not a commit", "oldest: v0.6.1\nrefs:\n  v0.6.1: " + strings.Repeat("a", 40) + "\n", []string{"not in the repository"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.commit("v061")
			f.tag("v0.6.1", "v061")
			f.write("docs/site/versions.yaml", strings.ReplaceAll(tc.content, "SHA", f.shas["v061"]))
			_, err := f.resolve(Options{})
			wantErrorContains(t, err, tc.want...)
		})
	}
}

// TestMissingOldestTag checks that a missing oldest tag is an error.
func TestMissingOldestTag(t *testing.T) {
	f := newFixture(t)
	f.commit("v090")
	f.tag("v0.9.0", "v090")
	f.versions("v0.6.1", nil)

	_, err := f.resolve(Options{})
	wantErrorContains(t, err, "oldest tag v0.6.1")
}

// TestResolveReleases checks the published-release set and the draft and prerelease exclusions.
func TestResolveReleases(t *testing.T) {
	f := newFixture(t)
	f.standardVersions()
	f.versions("v0.6.1", nil)
	path := filepath.Join(t.TempDir(), "releases.json")
	if err := os.WriteFile(path, []byte(`[
		{"tagName": "v0.6.1", "isDraft": false, "isPrerelease": false},
		{"tagName": "v0.6.2", "isDraft": false, "isPrerelease": false},
		{"tagName": "v0.9.0", "isDraft": true, "isPrerelease": false},
		{"tagName": "v0.10.0", "isDraft": false, "isPrerelease": true},
		{"tagName": "v0.7.0-rc1", "isDraft": false, "isPrerelease": true},
		{"tagName": "v0.5.0", "isDraft": false, "isPrerelease": false},
		{"tagName": "nightly", "isDraft": false, "isPrerelease": false}
	]`), 0o644); err != nil {
		t.Fatalf("write releases file: %v", err)
	}

	builds := f.mustResolve(Options{ReleasesFile: path})
	want := []buildKey{
		{"v0.6.2", ClassLatest, ""},
		{"v0.6.2", ClassLatestCopy, "v0.6.2/"},
		{"v0.6.1", ClassOlder, "v0.6.1/"},
		{"dev", ClassDev, "dev/"},
	}
	if got := buildKeys(builds); !reflect.DeepEqual(got, want) {
		t.Fatalf("builds = %v, want %v", got, want)
	}
	for _, b := range builds {
		switch b.Name {
		case "v0.9.0", "v0.10.0", "v0.7.0-rc1", "v0.5.0", "nightly":
			t.Errorf("build %s must not be in the set", b.Name)
		}
	}
}

// TestResolveReleasesOrderAndRefs checks the order gh release list prints
// (newest first), the latest build, and a refs override on an untagged commit.
func TestResolveReleasesOrderAndRefs(t *testing.T) {
	f := newFixture(t)
	f.standardVersions()
	f.versions("v0.6.1", map[string]string{"v0.6.1": f.shas["ref061"]})
	path := filepath.Join(t.TempDir(), "releases.json")
	if err := os.WriteFile(path, []byte(`[
		{"tagName": "v0.10.0", "isDraft": false, "isPrerelease": false},
		{"tagName": "v0.9.0", "isDraft": false, "isPrerelease": false},
		{"tagName": "v0.6.2", "isDraft": false, "isPrerelease": false},
		{"tagName": "v0.6.1", "isDraft": false, "isPrerelease": false}
	]`), 0o644); err != nil {
		t.Fatalf("write releases file: %v", err)
	}

	builds := f.mustResolve(Options{ReleasesFile: path})
	want := []buildKey{
		{"v0.10.0", ClassLatest, ""},
		{"v0.10.0", ClassLatestCopy, "v0.10.0/"},
		{"v0.9.0", ClassOlder, "v0.9.0/"},
		{"v0.6.2", ClassOlder, "v0.6.2/"},
		{"v0.6.1", ClassOlder, "v0.6.1/"},
		{"dev", ClassDev, "dev/"},
	}
	if got := buildKeys(builds); !reflect.DeepEqual(got, want) {
		t.Fatalf("builds = %v, want %v", got, want)
	}
	if got := buildFor(t, builds, "v0.10.0", ClassLatest).Source.Commit; got != f.shas["v100"] {
		t.Errorf("latest source = %s, want the v0.10.0 tag commit %s", got, f.shas["v100"])
	}
	if got := buildFor(t, builds, "v0.6.1", ClassOlder).Source.Commit; got != f.shas["ref061"] {
		t.Errorf("v0.6.1 source = %s, want the refs commit %s", got, f.shas["ref061"])
	}
}

// TestReleasesMissingFields checks that an entry without tagName, isDraft or
// isPrerelease is rejected instead of being read as published.
func TestReleasesMissingFields(t *testing.T) {
	cases := []struct {
		name    string
		content string
		want    string
	}{
		{"tagName only", `[{"tagName":"v0.6.2"},{"tagName":"v0.6.1"}]`, "isDraft"},
		{"no isPrerelease", `[{"tagName":"v0.6.1","isDraft":false}]`, "isPrerelease"},
		{"no isDraft", `[{"tagName":"v0.6.1","isPrerelease":false}]`, "isDraft"},
		{"no tagName", `[{"isDraft":false,"isPrerelease":false}]`, "tagName"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t)
			f.commit("v061")
			f.tag("v0.6.1", "v061")
			f.versions("v0.6.1", nil)
			path := filepath.Join(t.TempDir(), "releases.json")
			if err := os.WriteFile(path, []byte(tc.content), 0o644); err != nil {
				t.Fatalf("write releases file: %v", err)
			}
			_, err := f.resolve(Options{ReleasesFile: path})
			wantErrorContains(t, err, "releases file", tc.want)
		})
	}
}

// TestReleasesTagMissing checks that a published release without a tag is an error.
func TestReleasesTagMissing(t *testing.T) {
	f := newFixture(t)
	f.commit("v061")
	f.tag("v0.6.1", "v061")
	f.versions("v0.6.1", nil)
	path := filepath.Join(t.TempDir(), "releases.json")
	if err := os.WriteFile(path, []byte(`[
		{"tagName": "v0.6.1", "isDraft": false, "isPrerelease": false},
		{"tagName": "v0.8.0", "isDraft": false, "isPrerelease": false}
	]`), 0o644); err != nil {
		t.Fatalf("write releases file: %v", err)
	}

	_, err := f.resolve(Options{ReleasesFile: path})
	wantErrorContains(t, err, "v0.8.0", "no tag in the repository")
}

// TestReleasesWithAsRelease checks that -as-release and -releases cannot be combined.
func TestReleasesWithAsRelease(t *testing.T) {
	f := newFixture(t)
	f.commit("v061")
	f.tag("v0.6.1", "v061")
	f.versions("v0.6.1", nil)
	path := filepath.Join(t.TempDir(), "releases.json")
	if err := os.WriteFile(path, []byte(`[
		{"tagName": "v0.6.1", "isDraft": false, "isPrerelease": false}
	]`), 0o644); err != nil {
		t.Fatalf("write releases file: %v", err)
	}

	_, err := f.resolve(Options{ReleasesFile: path, AsRelease: "v0.7.0"})
	wantErrorContains(t, err, "-as-release cannot be used with -releases")
}

// TestAsRelease checks the working tree as a release, its replacement of a tag and the errors.
func TestAsRelease(t *testing.T) {
	t.Run("new release", func(t *testing.T) {
		f := newFixture(t)
		f.standardVersions()
		f.versions("v0.6.1", nil)

		builds := f.mustResolve(Options{AsRelease: "v0.11.0"})
		want := []buildKey{
			{"v0.11.0", ClassLatest, ""},
			{"v0.11.0", ClassLatestCopy, "v0.11.0/"},
			{"v0.10.0", ClassOlder, "v0.10.0/"},
			{"v0.9.0", ClassOlder, "v0.9.0/"},
			{"v0.6.2", ClassOlder, "v0.6.2/"},
			{"v0.6.1", ClassOlder, "v0.6.1/"},
			{"dev", ClassDev, "dev/"},
		}
		if got := buildKeys(builds); !reflect.DeepEqual(got, want) {
			t.Fatalf("builds = %v, want %v", got, want)
		}

		latest := buildFor(t, builds, "v0.11.0", ClassLatest)
		if !latest.Source.WorkingTree {
			t.Error("the newest release must be built from the working tree")
		}
		if got := latest.Config.Params.Release; got != "0.11.0" {
			t.Errorf("latest params.release = %q, want 0.11.0", got)
		}
		if got := buildFor(t, builds, "dev", ClassDev).Config.Params.Release; got != "0.11.0" {
			t.Errorf("dev params.release = %q, want the latest release 0.11.0", got)
		}
		if got := buildFor(t, builds, "v0.10.0", ClassOlder).Source.Commit; got != f.shas["v100"] {
			t.Errorf("v0.10.0 source = %s, want its tag commit %s", got, f.shas["v100"])
		}
	})

	t.Run("replaces a tag", func(t *testing.T) {
		f := newFixture(t)
		f.standardVersions()
		f.versions("v0.6.1", nil)

		builds := f.mustResolve(Options{AsRelease: "v0.10.0"})
		want := []buildKey{
			{"v0.10.0", ClassLatest, ""},
			{"v0.10.0", ClassLatestCopy, "v0.10.0/"},
			{"v0.9.0", ClassOlder, "v0.9.0/"},
			{"v0.6.2", ClassOlder, "v0.6.2/"},
			{"v0.6.1", ClassOlder, "v0.6.1/"},
			{"dev", ClassDev, "dev/"},
		}
		if got := buildKeys(builds); !reflect.DeepEqual(got, want) {
			t.Fatalf("builds = %v, want %v", got, want)
		}
		for _, b := range builds {
			if b.Name != "v0.10.0" {
				continue
			}
			if !b.Source.WorkingTree {
				t.Errorf("%s build must come from the working tree", b.Class)
			}
			if b.Source.Commit == f.shas["v100"] {
				t.Errorf("%s build kept the tag's commit %s", b.Class, f.shas["v100"])
			}
		}
	})

	t.Run("errors", func(t *testing.T) {
		cases := []struct {
			name string
			as   string
			want []string
		}{
			{"below another tag", "v0.9.0", []string{"must be higher than v0.10.0"}},
			{"equal to an older tag", "v0.6.1", []string{"must be higher than"}},
			{"missing v prefix", "0.11.0", []string{"not a vX.Y.Z"}},
			{"malformed patch", "v0.11", []string{"not a vX.Y.Z"}},
			{"prerelease", "v0.11.0-rc1", []string{"not a vX.Y.Z"}},
		}
		for _, tc := range cases {
			t.Run(tc.name, func(t *testing.T) {
				f := newFixture(t)
				f.standardVersions()
				f.versions("v0.6.1", nil)
				_, err := f.resolve(Options{AsRelease: tc.as})
				wantErrorContains(t, err, tc.want...)
			})
		}
	})
}

// TestBuildConfigs checks every params.* entry of every class and its base URL and output path.
func TestBuildConfigs(t *testing.T) {
	f := newFixture(t)
	f.commit("v061")
	f.tag("v0.6.1", "v061")
	f.commit("v090")
	f.tag("v0.9.0", "v090")
	f.commit("v100")
	f.tag("v0.10.0", "v100")
	f.versions("v0.6.1", nil)

	builds := f.mustResolve(Options{})
	root := testBaseURL
	wantVersions := []VersionEntry{
		{Name: "dev", URL: root + "dev/", Class: ClassDev},
		{Name: "v0.10.0", URL: root, Class: ClassLatest},
		{Name: "v0.9.0", URL: root + "v0.9.0/", Class: ClassOlder},
		{Name: "v0.6.1", URL: root + "v0.6.1/", Class: ClassOlder},
	}

	latest := buildFor(t, builds, "v0.10.0", ClassLatest)
	if latest.Config.BaseURL != root || latest.OutputPath != "" || latest.CanonicalBase != "" {
		t.Errorf("latest base URL/output/canonical = %q/%q/%q, want %q/\"\"/\"\"",
			latest.Config.BaseURL, latest.OutputPath, latest.CanonicalBase, root)
	}
	if got := latest.Config.Params.Release; got != "0.10.0" {
		t.Errorf("latest params.release = %q, want 0.10.0", got)
	}
	wantLatestVersion := VersionParam{
		Name:          "v0.10.0",
		Class:         ClassLatest,
		LatestBaseURL: root,
		LatestName:    "v0.10.0",
	}
	if !reflect.DeepEqual(latest.Config.Params.Version, wantLatestVersion) {
		t.Errorf("latest params.version = %+v, want %+v", latest.Config.Params.Version, wantLatestVersion)
	}
	if !reflect.DeepEqual(latest.Config.Params.Versions, wantVersions) {
		t.Errorf("latest params.versions = %+v, want %+v", latest.Config.Params.Versions, wantVersions)
	}

	latestCopy := buildFor(t, builds, "v0.10.0", ClassLatestCopy)
	if latestCopy.Config.BaseURL != root+"v0.10.0/" || latestCopy.OutputPath != "v0.10.0/" {
		t.Errorf("latest-copy base URL/output = %q/%q, want %q/v0.10.0/",
			latestCopy.Config.BaseURL, latestCopy.OutputPath, root+"v0.10.0/")
	}
	if latestCopy.CanonicalBase != root || latestCopy.Config.Params.Version.CanonicalBaseURL != root {
		t.Errorf("latest-copy canonical base = %q/%q, want %q",
			latestCopy.CanonicalBase, latestCopy.Config.Params.Version.CanonicalBaseURL, root)
	}
	wantCopyVersion := VersionParam{
		Name:             "v0.10.0",
		Class:            ClassLatestCopy,
		CanonicalBaseURL: root,
		LatestBaseURL:    root,
		LatestName:       "v0.10.0",
	}
	if !reflect.DeepEqual(latestCopy.Config.Params.Version, wantCopyVersion) {
		t.Errorf("latest-copy params.version = %+v, want %+v", latestCopy.Config.Params.Version, wantCopyVersion)
	}

	older := buildFor(t, builds, "v0.9.0", ClassOlder)
	if older.Config.BaseURL != root+"v0.9.0/" || older.OutputPath != "v0.9.0/" || older.CanonicalBase != "" {
		t.Errorf("older base URL/output/canonical = %q/%q/%q, want %q/v0.9.0/\"\"",
			older.Config.BaseURL, older.OutputPath, older.CanonicalBase, root+"v0.9.0/")
	}
	if got := older.Config.Params.Release; got != "0.9.0" {
		t.Errorf("older params.release = %q, want 0.9.0", got)
	}
	wantOlderVersion := VersionParam{
		Name:          "v0.9.0",
		Class:         ClassOlder,
		LatestBaseURL: root,
		LatestName:    "v0.10.0",
	}
	if !reflect.DeepEqual(older.Config.Params.Version, wantOlderVersion) {
		t.Errorf("older params.version = %+v, want %+v", older.Config.Params.Version, wantOlderVersion)
	}

	dev := buildFor(t, builds, "dev", ClassDev)
	if dev.Config.BaseURL != root+"dev/" || dev.OutputPath != "dev/" || dev.CanonicalBase != "" {
		t.Errorf("dev base URL/output/canonical = %q/%q/%q, want %q/dev/\"\"",
			dev.Config.BaseURL, dev.OutputPath, dev.CanonicalBase, root+"dev/")
	}
	if got := dev.Config.Params.Release; got != "0.10.0" {
		t.Errorf("dev params.release = %q, want the latest release 0.10.0", got)
	}
	wantDevVersion := VersionParam{
		Name:          "dev",
		Class:         ClassDev,
		LatestBaseURL: root,
		LatestName:    "v0.10.0",
	}
	if !reflect.DeepEqual(dev.Config.Params.Version, wantDevVersion) {
		t.Errorf("dev params.version = %+v, want %+v", dev.Config.Params.Version, wantDevVersion)
	}
	if !dev.Source.WorkingTree {
		t.Error("dev build must come from the working tree")
	}

	for _, b := range builds {
		if !reflect.DeepEqual(b.Config.Params.Versions, wantVersions) {
			t.Errorf("%s/%s params.versions = %+v, want %+v", b.Name, b.Class, b.Config.Params.Versions, wantVersions)
		}
	}
}

// TestDisableKinds checks the kinds each class disables and that the latest keeps the sitemap and robots.txt.
func TestDisableKinds(t *testing.T) {
	f := newFixture(t)
	f.commit("v061")
	f.tag("v0.6.1", "v061")
	f.commit("v090")
	f.tag("v0.9.0", "v090")
	f.versions("v0.6.1", nil)

	builds := f.mustResolve(Options{})
	want := []string{"taxonomy", "term", "sitemap", "robotstxt"}
	for _, b := range builds {
		if b.Class == ClassLatest {
			if b.Config.DisableKinds != nil {
				t.Errorf("latest disables %v, want none beyond hugo.yaml's own", b.Config.DisableKinds)
			}
			if contains(b.Config.DisableKinds, "sitemap") || contains(b.Config.DisableKinds, "robotstxt") {
				t.Errorf("latest disables %v, want sitemap and robots.txt kept", b.Config.DisableKinds)
			}
			continue
		}
		if !reflect.DeepEqual(b.Config.DisableKinds, want) {
			t.Errorf("%s/%s disableKinds = %v, want %v", b.Name, b.Class, b.Config.DisableKinds, want)
		}
	}
}

// TestBaseURLWithoutTrailingSlash checks that a base URL without a trailing slash gains one.
func TestBaseURLWithoutTrailingSlash(t *testing.T) {
	f := newFixture(t)
	f.commit("v061")
	f.tag("v0.6.1", "v061")
	f.versions("v0.6.1", nil)
	f.write("docs/site/hugo.yaml", "baseURL: https://example.test\ndisableKinds:\n  - taxonomy\n  - term\n")

	builds := f.mustResolve(Options{})
	root := testBaseURL
	latest := buildFor(t, builds, "v0.6.1", ClassLatest)
	if latest.BaseURL != root || latest.Config.BaseURL != root {
		t.Errorf("base URL = %q, want %q", latest.BaseURL, root)
	}
	if latest.Config.Params.Version.LatestBaseURL != root {
		t.Errorf("latestBaseURL = %q, want %q", latest.Config.Params.Version.LatestBaseURL, root)
	}
	if got := latest.Config.Params.Versions[0].URL; got != root+"dev/" {
		t.Errorf("dev URL = %q, want %q", got, root+"dev/")
	}
}
