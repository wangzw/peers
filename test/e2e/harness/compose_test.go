package harness

import (
	"os"
	"regexp"
	"slices"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// TestProjectName fixes the compose project name format, which carries the
// owner marker, and requires two consecutive names to differ.
func TestProjectName(t *testing.T) {
	pattern := regexp.MustCompile(`^ape2e-[0-9]+-[0-9]+-pidns[0-9]+-[a-z0-9-]{1,32}-[0-9a-f]{6}$`)

	first := projectName("TestJourneyPairBrokers/alias conflict")
	if !pattern.MatchString(first) {
		t.Fatalf("projectName = %q, want a match for %s", first, pattern)
	}
	second := projectName("TestJourneyPairBrokers/alias conflict")
	if first == second {
		t.Fatalf("projectName returned %q twice; the names must differ", first)
	}
}

// TestComposeServices checks that every service of flat.yaml is reported, including the profiled hosts a smaller TopoFlat does not start.
func TestComposeServices(t *testing.T) {
	path, err := composeSourcePath("flat.yaml")
	if err != nil {
		t.Fatalf("locating flat.yaml: %v", err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading flat.yaml: %v", err)
	}
	got, err := composeServices(data)
	if err != nil {
		t.Fatalf("composeServices: %v", err)
	}
	want := []string{"hostA", "hostB", "hostC", "hostD"}
	if !slices.Equal(got, want) {
		t.Fatalf("composeServices = %v, want %v", got, want)
	}
}

// TestCoverOverride checks that the generated override mounts the coverage
// directory in every service and adds the extra volumes of extraVolumes.
func TestCoverOverride(t *testing.T) {
	const hostDir = "/host/cover"
	const extraMount = "/host/conf:/etc/conf:ro"
	data, err := CoverOverride(hostDir, []string{"hostA", "hostB"},
		map[string][]string{"hostB": {extraMount}})
	if err != nil {
		t.Fatalf("CoverOverride: %v", err)
	}

	var doc struct {
		Services map[string]struct {
			Environment map[string]string `yaml:"environment"`
			Volumes     []string          `yaml:"volumes"`
		} `yaml:"services"`
	}
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parsing the override %q: %v", data, err)
	}
	if len(doc.Services) != 2 {
		t.Fatalf("override declares %d services, want 2", len(doc.Services))
	}
	for _, name := range []string{"hostA", "hostB"} {
		service, ok := doc.Services[name]
		if !ok {
			t.Fatalf("override has no service %q", name)
		}
		for _, variable := range []string{CoverageDirEnv, "GOCOVERDIR"} {
			if got := service.Environment[variable]; got != CoverMountPath {
				t.Errorf("service %s: %s = %q, want %q", name, variable, got, CoverMountPath)
			}
		}
		wantVolumes := []string{hostDir + ":" + CoverMountPath}
		if name == "hostB" {
			wantVolumes = append(wantVolumes, extraMount)
		}
		if !slices.Equal(service.Volumes, wantVolumes) {
			t.Errorf("service %s: volumes = %v, want %v", name, service.Volumes, wantVolumes)
		}
	}
}

// TestComposeCommandCarriesEveryFile checks that a cluster passes each compose file it was built with.
func TestComposeCommandCarriesEveryFile(t *testing.T) {
	c := &Cluster{
		project:      "proj",
		composeFiles: []string{"/tmp/base.yaml", "/tmp/override.yaml"},
		env:          os.Environ(),
	}
	got := c.composeCommand("ps", "-aq").Args
	want := []string{
		"docker", "compose", "-p", "proj",
		"-f", "/tmp/base.yaml", "-f", "/tmp/override.yaml", "ps", "-aq",
	}
	if !slices.Equal(got, want) {
		t.Fatalf("composeCommand = %v, want %v", got, want)
	}

	c.composeFiles = []string{"/tmp/base.yaml"}
	got = c.composeCommand("down").Args
	want = []string{"docker", "compose", "-p", "proj", "-f", "/tmp/base.yaml", "down"}
	if !slices.Equal(got, want) {
		t.Fatalf("composeCommand without an override = %v, want %v", got, want)
	}
}

// TestDownTimeout checks that teardown uses one second without coverage and twenty seconds with it.
func TestDownTimeout(t *testing.T) {
	cases := []struct {
		name     string
		coverDir string
		want     string
	}{
		{"coverage off", "", "1"},
		{"coverage on", "/tmp/cover", "20"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(CoverageDirEnv, tc.coverDir)
			if got := DownTimeout(); got != tc.want {
				t.Fatalf("DownTimeout() = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestProjectNameTruncatesLongNames checks that a long or punctuation-heavy
// test name still yields a valid project name.
func TestProjectNameTruncatesLongNames(t *testing.T) {
	pattern := regexp.MustCompile(`^ape2e-[0-9]+-[0-9]+-pidns[0-9]+-[a-z0-9-]{1,32}-[0-9a-f]{6}$`)
	name := projectName(strings.Repeat("TestVeryLongName/", 20))
	if !pattern.MatchString(name) {
		t.Fatalf("projectName = %q, want a match for %s", name, pattern)
	}
}
