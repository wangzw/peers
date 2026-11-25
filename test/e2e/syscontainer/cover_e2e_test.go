//go:build e2e

package syscontainer

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/wangzw/peers/test/e2e/harness"
	"gopkg.in/yaml.v3"
)

// TestSystemCoverageOverride checks the override written for the system
// topology: every service mounts the coverage directory and names it in both
// variables, and the two systemd services mount the DefaultEnvironment drop-in.
func TestSystemCoverageOverride(t *testing.T) {
	const hostDir = "/host/cover"
	t.Setenv(harness.CoverageDirEnv, hostDir)
	dir := t.TempDir()

	overridePath, forward := writeSystemCoverage(t, dir)
	if want := harness.CoverageDirEnv + "=" + harness.CoverMountPath; forward != want {
		t.Fatalf("forwarded entry = %q, want %q", forward, want)
	}

	dropInPath := filepath.Join(dir, systemdCoverDropInName)
	dropIn, err := os.ReadFile(dropInPath)
	if err != nil {
		t.Fatalf("reading the systemd drop-in: %v", err)
	}
	const wantDropIn = "[Manager]\nDefaultEnvironment=GOCOVERDIR=/e2e-cover\n"
	if string(dropIn) != wantDropIn {
		t.Fatalf("systemd drop-in = %q, want %q", dropIn, wantDropIn)
	}

	data, err := os.ReadFile(overridePath)
	if err != nil {
		t.Fatalf("reading the override: %v", err)
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

	services := []string{string(Plain), string(Systemd), string(pairHostB)}
	if len(doc.Services) != len(services) {
		t.Fatalf("override declares %d services, want %d: %v", len(doc.Services), len(services), doc.Services)
	}
	for _, name := range services {
		service, ok := doc.Services[name]
		if !ok {
			t.Fatalf("override has no service %q", name)
		}
		for _, variable := range []string{harness.CoverageDirEnv, "GOCOVERDIR"} {
			if got := service.Environment[variable]; got != harness.CoverMountPath {
				t.Errorf("service %s: %s = %q, want %q", name, variable, got, harness.CoverMountPath)
			}
		}
		wantVolumes := []string{hostDir + ":" + harness.CoverMountPath}
		if name == string(Systemd) || name == string(pairHostB) {
			wantVolumes = append(wantVolumes, dropInPath+":"+systemdCoverDropInPath+":ro")
		}
		if !slices.Equal(service.Volumes, wantVolumes) {
			t.Errorf("service %s: volumes = %v, want %v", name, service.Volumes, wantVolumes)
		}
	}
}

// TestSudoersInstallOnlyUnderCoverage checks that the sudoers drop-in is
// written only when coverage is on, and that its command carries the content
// and the ownership, mode and validation sudo requires.
func TestSudoersInstallOnlyUnderCoverage(t *testing.T) {
	c := &container{project: "proj", composeFiles: []string{"/tmp/compose.yaml"}}

	t.Setenv(harness.CoverageDirEnv, "")
	if got := c.sudoersInstall(string(Systemd)); got != nil {
		t.Fatalf("sudoersInstall without coverage = %v, want nil", got)
	}

	t.Setenv(harness.CoverageDirEnv, "/host/cover")
	got := c.sudoersInstall(string(Systemd))
	prefix := []string{
		"compose", "-p", "proj", "-f", "/tmp/compose.yaml",
		"exec", "-T", "-u", "root", string(Systemd), "sh", "-c",
	}
	if len(got) != len(prefix)+1 || !slices.Equal(got[:len(prefix)], prefix) {
		t.Fatalf("sudoersInstall = %v, want the prefix %v and a script", got, prefix)
	}
	script := got[len(got)-1]
	for _, want := range []string{
		`Defaults env_keep += "GOCOVERDIR"`,
		sudoersCoverPath,
		"chown root:root",
		"chmod 0440",
		"visudo -cf",
	} {
		if !strings.Contains(script, want) {
			t.Errorf("sudoers script %q does not contain %q", script, want)
		}
	}
	if sudoersCoverContent != `Defaults env_keep += "GOCOVERDIR"` {
		t.Fatalf("sudoers content = %q", sudoersCoverContent)
	}
}

// TestPlainCommandWrapsOnlyUnderCoverage checks that a plain container's test
// command is wrapped so the sudoers drop-in is written before the test runs,
// and that the command is unchanged when coverage is off.
func TestPlainCommandWrapsOnlyUnderCoverage(t *testing.T) {
	argv := []string{"/opt/peers-test/system.test", "-test.run", "^TestX$"}

	t.Setenv(harness.CoverageDirEnv, "")
	if got := plainCommand(argv); !slices.Equal(got, argv) {
		t.Fatalf("plainCommand without coverage = %v, want %v", got, argv)
	}

	t.Setenv(harness.CoverageDirEnv, "/host/cover")
	got := plainCommand(argv)
	if len(got) != 4+len(argv) {
		t.Fatalf("plainCommand = %v, want a four-element wrapper around %v", got, argv)
	}
	if got[0] != "sh" || got[1] != "-c" || got[3] != "sh" {
		t.Fatalf("plainCommand = %v, want sh -c <script> sh ...", got)
	}
	for _, want := range []string{`Defaults env_keep += "GOCOVERDIR"`, `exec "$@"`} {
		if !strings.Contains(got[2], want) {
			t.Errorf("wrapper script %q does not contain %q", got[2], want)
		}
	}
	if !slices.Equal(got[4:], argv) {
		t.Fatalf("wrapper runs %v, want %v", got[4:], argv)
	}
}

// TestCommandPropagatesCoverageDir checks that a child started through Command
// gets GOCOVERDIR under coverage and that an explicit caller value wins.
func TestCommandPropagatesCoverageDir(t *testing.T) {
	t.Setenv(harness.CoverageDirEnv, "")
	if got := envValue(Command("root", nil, "true").Env, "GOCOVERDIR"); got != "" {
		t.Fatalf("GOCOVERDIR without coverage = %q, want it absent", got)
	}

	t.Setenv(harness.CoverageDirEnv, "/e2e-cover")
	if got := envValue(Command("root", nil, "true").Env, "GOCOVERDIR"); got != "/e2e-cover" {
		t.Fatalf("GOCOVERDIR under coverage = %q, want /e2e-cover", got)
	}

	explicit := []string{"GOCOVERDIR=/explicit"}
	if got := envValue(Command("root", explicit, "true").Env, "GOCOVERDIR"); got != "/explicit" {
		t.Fatalf("GOCOVERDIR with a caller value = %q, want /explicit", got)
	}
}

// envValue returns the value of name in env, or "" when it is absent.
func envValue(env []string, name string) string {
	prefix := name + "="
	for _, entry := range env {
		if strings.HasPrefix(entry, prefix) {
			return strings.TrimPrefix(entry, prefix)
		}
	}
	return ""
}
