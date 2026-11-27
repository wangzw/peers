// Package resolve turns the website's versions.yaml and the repository's tags
// or published releases into the list of per-version website builds.
package resolve

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/wangzw/peers/scripts/site-versions/gitenv"
)

// Class is the role a build plays in the deployed site.
type Class string

const (
	// ClassLatest is the latest release served at the root.
	ClassLatest Class = "latest"
	// ClassLatestCopy is the latest release served again under its version.
	ClassLatestCopy Class = "latest-copy"
	// ClassOlder is a release older than the latest.
	ClassOlder Class = "older"
	// ClassDev is the working tree served under /dev/.
	ClassDev Class = "dev"
)

// Source is where a build's content comes from: a commit to check out, or the
// working tree.
type Source struct {
	// WorkingTree reports whether the content is the working tree rather than
	// a commit; Commit is empty when it is true.
	WorkingTree bool
	// Commit is the commit to check out when WorkingTree is false.
	Commit string
}

// Build is one Hugo build of the site.
type Build struct {
	// Name is the version name: vX.Y.Z or dev.
	Name string
	// Class is the build's role in the deployed site.
	Class Class
	// Source is where the build's content comes from.
	Source Source
	// BaseURL is the build's site base URL, with a trailing slash.
	BaseURL string
	// OutputPath is the build's directory under the assembled root, empty for
	// the latest build.
	OutputPath string
	// Config is the per-build Hugo configuration document.
	Config Config
	// CanonicalBase is the value for site-meta's -canonical-base flag; it is
	// set for latest-copy only.
	CanonicalBase string
}

// Config is the Hugo configuration document for one build. It replaces the
// site's hugo.yaml for that build.
type Config struct {
	// BaseURL is the build's site base URL, with a trailing slash.
	BaseURL string `yaml:"baseURL" json:"baseURL"`
	// Params holds the params.* entries.
	Params Params `yaml:"params" json:"params"`
	// DisableKinds is the build's full disableKinds list, or nil to keep the
	// site's own; a non-nil list replaces that list.
	DisableKinds []string `yaml:"disableKinds,omitempty" json:"disableKinds,omitempty"`
}

// Params holds the params.* entries a build provides.
type Params struct {
	// Release is params.release: the release number without the v prefix.
	Release string `yaml:"release" json:"release"`
	// Version is params.version.
	Version VersionParam `yaml:"version" json:"version"`
	// Versions is params.versions, dev first and then releases newest first.
	Versions []VersionEntry `yaml:"versions" json:"versions"`
}

// VersionParam is params.version.
type VersionParam struct {
	// Name is vX.Y.Z or dev.
	Name string `yaml:"name" json:"name"`
	// Class is the build's class.
	Class Class `yaml:"class" json:"class"`
	// CanonicalBaseURL is the root base URL for latest-copy only, and empty
	// otherwise.
	CanonicalBaseURL string `yaml:"canonicalBaseURL" json:"canonicalBaseURL"`
	// LatestBaseURL is the root base URL.
	LatestBaseURL string `yaml:"latestBaseURL" json:"latestBaseURL"`
	// LatestName is the latest release's vX.Y.Z name.
	LatestName string `yaml:"latestName" json:"latestName"`
}

// VersionEntry is one element of params.versions.
type VersionEntry struct {
	// Name is vX.Y.Z or dev.
	Name string `yaml:"name" json:"name"`
	// URL is the version's base URL with a trailing slash; the latest
	// release's is the root base URL.
	URL string `yaml:"url" json:"url"`
	// Class is latest, older or dev.
	Class Class `yaml:"class" json:"class"`
}

// Options are the inputs to Resolve.
type Options struct {
	// Repo is the repository root. Resolve reads tags and commits from it.
	Repo string
	// Site is the directory holding hugo.yaml and versions.yaml.
	Site string
	// AsRelease builds the working tree as this release, replacing a tag of
	// the same name.
	AsRelease string
	// ReleasesFile is the JSON array gh release list --json
	// tagName,isDraft,isPrerelease writes. It switches the version set from
	// tags to published releases.
	ReleasesFile string
}

// versionsFile is docs/site/versions.yaml.
type versionsFile struct {
	Oldest string            `yaml:"oldest"`
	Refs   map[string]string `yaml:"refs"`
}

// hugoFile holds the hugo.yaml entries a build needs.
type hugoFile struct {
	BaseURL      string   `yaml:"baseURL"`
	DisableKinds []string `yaml:"disableKinds"`
}

// Resolve builds the list of website builds from the version set and the
// per-build configuration. Errors report unusable inputs.
func Resolve(opts Options) ([]Build, error) {
	if opts.Repo == "" {
		return nil, errors.New("no repository root")
	}
	if opts.Site == "" {
		return nil, errors.New("no site directory")
	}

	hugo, err := readHugoConfig(filepath.Join(opts.Site, "hugo.yaml"))
	if err != nil {
		return nil, err
	}
	versions, err := readVersionsFile(filepath.Join(opts.Site, "versions.yaml"), opts.Repo)
	if err != nil {
		return nil, err
	}

	set, err := resolveSet(opts, versions)
	if err != nil {
		return nil, err
	}
	set, err = applyAsRelease(set, opts.AsRelease)
	if err != nil {
		return nil, err
	}

	root := strings.TrimRight(hugo.BaseURL, "/") + "/"
	return buildsFor(set, root, hugo.DisableKinds), nil
}

// readHugoConfig reads the base URL and the disabled kinds of the site.
func readHugoConfig(path string) (hugoFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return hugoFile{}, fmt.Errorf("read %s: %w", path, err)
	}
	var config hugoFile
	if err := yaml.Unmarshal(data, &config); err != nil {
		return hugoFile{}, fmt.Errorf("read %s: %w", path, err)
	}
	if config.BaseURL == "" {
		return hugoFile{}, fmt.Errorf("%s: baseURL is empty", path)
	}
	return config, nil
}

// readVersionsFile reads and checks versions.yaml.
func readVersionsFile(path, repo string) (versionsFile, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return versionsFile{}, fmt.Errorf("read %s: %w", path, err)
	}
	decoder := yaml.NewDecoder(bytes.NewReader(data))
	decoder.KnownFields(true)
	var file versionsFile
	if err := decoder.Decode(&file); err != nil && !errors.Is(err, io.EOF) {
		return versionsFile{}, fmt.Errorf("read %s: %w", path, err)
	}

	oldest, ok := parseVersion(file.Oldest)
	if !ok {
		return versionsFile{}, fmt.Errorf("%s: oldest %q is not a vX.Y.Z version", path, file.Oldest)
	}
	for name, commit := range file.Refs {
		v, ok := parseVersion(name)
		if !ok {
			return versionsFile{}, fmt.Errorf("%s: ref %q is not a vX.Y.Z version", path, name)
		}
		if v.compare(oldest) < 0 {
			return versionsFile{}, fmt.Errorf("%s: ref %s is below oldest %s", path, name, file.Oldest)
		}
		if !isCommit(commit) {
			return versionsFile{}, fmt.Errorf("%s: ref %s is not a 40-hex commit", path, name)
		}
		if _, err := git(repo, "cat-file", "-e", commit+"^{commit}"); err != nil {
			return versionsFile{}, fmt.Errorf("%s: ref %s: commit %s is not in the repository", path, name, commit)
		}
	}
	return file, nil
}

// isCommit reports whether s has the form of a full SHA-1 object name.
func isCommit(s string) bool {
	if len(s) != 40 {
		return false
	}
	for _, r := range s {
		switch {
		case r >= '0' && r <= '9':
		case r >= 'a' && r <= 'f':
		case r >= 'A' && r <= 'F':
		default:
			return false
		}
	}
	return true
}

// resolveSet returns the releases to build, lowest first.
func resolveSet(opts Options, file versionsFile) ([]release, error) {
	if opts.ReleasesFile != "" {
		if opts.AsRelease != "" {
			return nil, errors.New("-as-release cannot be used with -releases")
		}
		return setFromReleases(opts.Repo, opts.ReleasesFile, file)
	}
	return setFromTags(opts.Repo, file)
}

// setFromTags returns every vX.Y.Z tag at or after the oldest, with the refs
// override of versions.yaml applied.
func setFromTags(repo string, file versionsFile) ([]release, error) {
	out, err := git(repo, "tag", "--list")
	if err != nil {
		return nil, err
	}
	oldest, _ := parseVersion(file.Oldest)
	var set []release
	for _, name := range strings.Fields(out) {
		v, ok := parseVersion(name)
		if !ok || v.compare(oldest) < 0 {
			continue
		}
		commit, err := tagCommit(repo, name)
		if err != nil {
			return nil, err
		}
		if ref, ok := file.Refs[name]; ok {
			commit = ref
		}
		set = append(set, release{name: name, ver: v, commit: commit})
	}
	sortReleases(set)
	if !hasRelease(set, file.Oldest) {
		return nil, fmt.Errorf("oldest tag %s not found", file.Oldest)
	}
	return set, nil
}

// ghRelease is one entry of the releases file. The fields are pointers so a
// missing field is an error rather than a zero value that reads as published.
type ghRelease struct {
	TagName      *string `json:"tagName"`
	IsDraft      *bool   `json:"isDraft"`
	IsPrerelease *bool   `json:"isPrerelease"`
}

// setFromReleases returns every published release whose vX.Y.Z tag exists at or
// after the oldest, with the refs override of versions.yaml applied.
func setFromReleases(repo, path string, file versionsFile) ([]release, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read releases file: %w", err)
	}
	var listed []ghRelease
	if err := json.Unmarshal(data, &listed); err != nil {
		return nil, fmt.Errorf("read releases file %s: %w", path, err)
	}

	oldest, _ := parseVersion(file.Oldest)
	seen := map[string]bool{}
	var set []release
	for i, entry := range listed {
		switch {
		case entry.TagName == nil:
			return nil, fmt.Errorf("releases file %s: entry %d has no tagName", path, i+1)
		case entry.IsDraft == nil:
			return nil, fmt.Errorf("releases file %s: entry %d (%s) has no isDraft", path, i+1, *entry.TagName)
		case entry.IsPrerelease == nil:
			return nil, fmt.Errorf("releases file %s: entry %d (%s) has no isPrerelease", path, i+1, *entry.TagName)
		}
		tag := *entry.TagName
		if *entry.IsDraft || *entry.IsPrerelease {
			continue
		}
		v, ok := parseVersion(tag)
		if !ok || v.compare(oldest) < 0 || seen[tag] {
			continue
		}
		seen[tag] = true
		commit, err := tagCommit(repo, tag)
		if err != nil {
			return nil, fmt.Errorf("published release %s has no tag in the repository", tag)
		}
		if ref, ok := file.Refs[tag]; ok {
			commit = ref
		}
		set = append(set, release{name: tag, ver: v, commit: commit})
	}
	if len(set) == 0 {
		return nil, fmt.Errorf("no published releases at or after %s", file.Oldest)
	}
	sortReleases(set)
	return set, nil
}

// applyAsRelease adds the working tree as the named release, replacing the
// build of a tag of the same name. It must be higher than every other release.
func applyAsRelease(set []release, name string) ([]release, error) {
	if name == "" {
		return set, nil
	}
	v, ok := parseVersion(name)
	if !ok {
		return nil, fmt.Errorf("-as-release %q is not a vX.Y.Z version", name)
	}
	for _, other := range set {
		if other.name == name {
			continue
		}
		if v.compare(other.ver) <= 0 {
			return nil, fmt.Errorf("-as-release %s must be higher than %s", name, other.name)
		}
	}
	for i := range set {
		if set[i].name == name {
			set[i].workingTree = true
			set[i].commit = ""
			return set, nil
		}
	}
	set = append(set, release{name: name, ver: v, workingTree: true})
	sortReleases(set)
	return set, nil
}

// buildsFor lays out the builds: the latest release at the root and as
// latest-copy, every other release under its version, and the working tree
// under /dev/.
func buildsFor(set []release, root string, hugoKinds []string) []Build {
	latest := set[len(set)-1]
	latestName := latest.name
	latestRelease := strings.TrimPrefix(latestName, "v")

	entries := make([]VersionEntry, 0, len(set)+1)
	entries = append(entries, VersionEntry{Name: "dev", URL: root + "dev/", Class: ClassDev})
	for i := len(set) - 1; i >= 0; i-- {
		entry := VersionEntry{
			Name:  set[i].name,
			URL:   versionBase(root, set[i].name),
			Class: ClassOlder,
		}
		if set[i].name == latestName {
			entry.URL = root
			entry.Class = ClassLatest
		}
		entries = append(entries, entry)
	}

	var builds []Build
	add := func(r release, class Class, baseURL, outputPath, releaseNumber, canonical string) {
		builds = append(builds, Build{
			Name:       r.name,
			Class:      class,
			Source:     sourceOf(r),
			BaseURL:    baseURL,
			OutputPath: outputPath,
			Config: Config{
				BaseURL: baseURL,
				Params: Params{
					Release: releaseNumber,
					Version: VersionParam{
						Name:             r.name,
						Class:            class,
						CanonicalBaseURL: canonical,
						LatestBaseURL:    root,
						LatestName:       latestName,
					},
					Versions: entries,
				},
				DisableKinds: disabledKinds(class, hugoKinds),
			},
			CanonicalBase: canonical,
		})
	}

	add(latest, ClassLatest, root, "", latestRelease, "")
	add(latest, ClassLatestCopy, versionBase(root, latestName), latestName+"/", latestRelease, root)
	for i := len(set) - 2; i >= 0; i-- {
		r := set[i]
		add(r, ClassOlder, versionBase(root, r.name), r.name+"/", strings.TrimPrefix(r.name, "v"), "")
	}
	builds = append(builds, Build{
		Name:       "dev",
		Class:      ClassDev,
		Source:     Source{WorkingTree: true},
		BaseURL:    root + "dev/",
		OutputPath: "dev/",
		Config: Config{
			BaseURL: root + "dev/",
			Params: Params{
				Release: latestRelease,
				Version: VersionParam{
					Name:          "dev",
					Class:         ClassDev,
					LatestBaseURL: root,
					LatestName:    latestName,
				},
				Versions: entries,
			},
			DisableKinds: disabledKinds(ClassDev, hugoKinds),
		},
	})
	return builds
}

// disabledKinds returns the kinds a build disables. A configuration file's
// list replaces the earlier one, so the site's own kinds are restated. The
// latest build keeps Hugo's sitemap and robots.txt outputs.
func disabledKinds(class Class, hugoKinds []string) []string {
	if class == ClassLatest {
		return nil
	}
	kinds := append([]string(nil), hugoKinds...)
	for _, kind := range []string{"sitemap", "robotstxt"} {
		if !contains(kinds, kind) {
			kinds = append(kinds, kind)
		}
	}
	return kinds
}

func sourceOf(r release) Source {
	if r.workingTree {
		return Source{WorkingTree: true}
	}
	return Source{Commit: r.commit}
}

func versionBase(root, name string) string {
	return root + name + "/"
}

func hasRelease(set []release, name string) bool {
	for _, r := range set {
		if r.name == name {
			return true
		}
	}
	return false
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

// tagCommit returns the commit a tag points at.
func tagCommit(repo, name string) (string, error) {
	out, err := git(repo, "rev-parse", "--verify", "refs/tags/"+name+"^{commit}")
	if err != nil {
		return "", err
	}
	return strings.TrimSpace(out), nil
}

// git runs a git command in repo and returns its standard output. Its
// environment drops the variables that would point git at another repository,
// index or configuration.
func git(repo string, args ...string) (string, error) {
	cmd := exec.Command("git", append([]string{"-C", repo}, args...)...)
	cmd.Env = gitenv.Env()
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		message := strings.TrimSpace(stderr.String())
		if message == "" {
			message = err.Error()
		}
		return "", fmt.Errorf("git %s: %s", strings.Join(args, " "), message)
	}
	return string(out), nil
}
