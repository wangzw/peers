// Command site-versions builds and checks every version of the website and
// assembles them into the tree GitHub Pages serves. It exits 0 when every check
// passes, 1 on the first failed check and 2 on bad flags or unusable inputs.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"

	"gopkg.in/yaml.v3"

	"github.com/wangzw/peers/scripts/site-versions/resolve"
)

// resolveConfig carries the inputs the version-set resolver needs.
type resolveConfig struct {
	Repo      string
	Site      string
	Releases  string
	AsRelease string
}

func main() {
	os.Exit(run(os.Args[1:], os.Stderr))
}

func run(args []string, stderr io.Writer) int {
	return runWith(args, stderr, Deps{})
}

// runWith parses the command's flags, resolves the version set and runs it.
func runWith(args []string, stderr io.Writer, deps Deps) int {
	fs := flag.NewFlagSet("site-versions", flag.ContinueOnError)
	fs.SetOutput(stderr)
	repo := fs.String("repo", "", "repository root")
	site := fs.String("site", "", "site directory")
	work := fs.String("work", "", "work directory")
	public := fs.String("public", "", "assembled output directory")
	jobs := fs.Int("jobs", 1, "parallel jobs")
	asRelease := fs.String("as-release", "", "build the working tree as this release")
	releases := fs.String("releases", "", "GitHub releases JSON file")
	if err := fs.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return 0
		}
		return 2
	}
	if fs.NArg() != 0 {
		_, _ = fmt.Fprintf(stderr, "site-versions: unexpected argument %q\n", fs.Arg(0))
		return 2
	}
	if *repo == "" || *site == "" || *work == "" || *public == "" {
		_, _ = fmt.Fprintln(stderr, "site-versions: -repo, -site, -work and -public are required")
		return 2
	}
	if *jobs <= 0 {
		_, _ = fmt.Fprintln(stderr, "site-versions: -jobs must be positive")
		return 2
	}
	if *asRelease != "" && *releases != "" {
		_, _ = fmt.Fprintln(stderr, "site-versions: -as-release and -releases are mutually exclusive")
		return 2
	}

	builds, err := resolveBuilds(resolveConfig{
		Repo:      *repo,
		Site:      *site,
		Releases:  *releases,
		AsRelease: *asRelease,
	})
	if err != nil {
		_, _ = fmt.Fprintf(stderr, "site-versions: %v\n", err)
		return 2
	}
	return runBuilds(stderr, Config{
		Repo:   *repo,
		Site:   *site,
		Work:   *work,
		Public: *public,
		Jobs:   *jobs,
	}, builds, deps)
}

// runBuilds runs a resolved build list and maps its error to the command's exit
// code.
func runBuilds(stderr io.Writer, cfg Config, builds []Build, deps Deps) int {
	err := Run(context.Background(), cfg, builds, deps)
	if err == nil {
		return 0
	}
	_, _ = fmt.Fprintf(stderr, "site-versions: %v\n", err)
	var check *CheckError
	if errors.As(err, &check) {
		if output := strings.TrimRight(check.Output, "\n"); output != "" {
			_, _ = fmt.Fprintln(stderr, output)
		}
	}
	var usage *UsageError
	if errors.As(err, &usage) {
		return 2
	}
	return 1
}

// resolveBuilds resolves the version set and converts the resolver's build list
// to the runner's.
func resolveBuilds(cfg resolveConfig) ([]Build, error) {
	resolved, err := resolve.Resolve(resolve.Options{
		Repo:         cfg.Repo,
		Site:         cfg.Site,
		AsRelease:    cfg.AsRelease,
		ReleasesFile: cfg.Releases,
	})
	if err != nil {
		return nil, err
	}
	builds := make([]Build, len(resolved))
	for i, build := range resolved {
		document, err := yaml.Marshal(build.Config)
		if err != nil {
			return nil, fmt.Errorf("build %s: marshal per-build configuration: %w", build.Name, err)
		}
		builds[i] = Build{
			Name:          build.Name,
			Class:         string(build.Class),
			Source:        Source{Commit: build.Source.Commit, WorkingTree: build.Source.WorkingTree},
			BaseURL:       build.BaseURL,
			Output:        build.OutputPath,
			Config:        document,
			Release:       build.Config.Params.Release,
			CanonicalBase: build.CanonicalBase,
		}
	}
	return builds, nil
}
