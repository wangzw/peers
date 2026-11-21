package main

import (
	"bytes"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
)

func TestCheckDir(t *testing.T) {
	tests := []struct {
		name  string
		file  string
		want  []string
		lines []int
	}{
		{"pass", "", nil, nil},
		{"tag", "workflow.yml", []string{"is not pinned to a 40-character commit SHA"}, []int{8}},
		{"short-sha", "workflow.yml", []string{"is not pinned to a 40-character commit SHA"}, []int{8}},
		{"no-comment", "workflow.yml", []string{"has no version tag comment"}, []int{8}},
		{"checkout-credentials", "workflow.yml", []string{"actions/checkout must set persist-credentials: false"}, []int{8}},
		{"cache-true", "release.yml", []string{"actions/setup-go must set cache: false in this workflow"}, []int{11}},
		{"cache-missing", "release.yml", []string{"actions/setup-go must set cache: false in this workflow"}, []int{11}},
		{"docker-tag", "workflow.yml", []string{"docker:// reference docker://alpine:3.20 must name a sha256 digest"}, []int{8}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			problems, err := checkDir(filepath.Join("testdata", tt.name))
			if err != nil {
				t.Fatalf("checkDir = %v", err)
			}
			if len(problems) != len(tt.want) {
				t.Fatalf("checkDir returned %d problems, want %d: %q", len(problems), len(tt.want), problems)
			}
			for i, want := range tt.want {
				if !strings.Contains(problems[i], want) {
					t.Errorf("problem %d = %q, want it to contain %q", i, problems[i], want)
				}
				prefix := fmt.Sprintf("%s:%d: ", filepath.Join("testdata", tt.name, tt.file), tt.lines[i])
				if !strings.HasPrefix(problems[i], prefix) {
					t.Errorf("problem %d = %q, want it to start with %q", i, problems[i], prefix)
				}
			}
		})
	}
}

// TestCheckActions checks that a composite action is held to the same pin rule
// as a workflow step.
func TestCheckActions(t *testing.T) {
	problems, err := checkActions(filepath.Join("testdata", "actions-pass"))
	if err != nil {
		t.Fatalf("checkActions(pass) = %v", err)
	}
	if len(problems) != 0 {
		t.Fatalf("checkActions(pass) = %q, want none", problems)
	}

	problems, err = checkActions(filepath.Join("testdata", "actions-fail"))
	if err != nil {
		t.Fatalf("checkActions(fail) = %v", err)
	}
	want := filepath.Join("testdata", "actions-fail", "build", "action.yml") + ":6: bufbuild/buf-action@v1.7.0 is not pinned to a 40-character commit SHA"
	if len(problems) != 1 || problems[0] != want {
		t.Fatalf("checkActions(fail) = %q, want [%q]", problems, want)
	}
}

// TestCheckActionsMissingRoot checks that a repository without composite
// actions passes rather than failing on the absent directory.
func TestCheckActionsMissingRoot(t *testing.T) {
	problems, err := checkActions(filepath.Join("testdata", "no-such-actions"))
	if err != nil {
		t.Fatalf("checkActions(missing) = %v", err)
	}
	if len(problems) != 0 {
		t.Fatalf("checkActions(missing) = %q, want none", problems)
	}
}

// TestCacheRuleFollowsFileName checks that the same setup-go step is accepted
// in an unrestricted workflow and rejected in a restricted one, because the
// rule is chosen by the file's base name.
func TestCacheRuleFollowsFileName(t *testing.T) {
	const step = `name: t
on: push
jobs:
  build:
    runs-on: ubuntu-24.04
    steps:
      - uses: actions/setup-go@d8cbb8045d7f43706b9177cceb36c1f91a6644d5 # v7
        with:
          cache: true
`
	for name, wantFail := range map[string]bool{
		"ci.yml":           false,
		"release.yml":      true,
		"site.yml":         true,
		"real-session.yml": true,
	} {
		t.Run(name, func(t *testing.T) {
			problems, err := checkFile(name, []byte(step))
			if err != nil {
				t.Fatalf("checkFile = %v", err)
			}
			if got := len(problems) > 0; got != wantFail {
				t.Fatalf("checkFile(%s) failures = %v, want %v: %q", name, got, wantFail, problems)
			}
		})
	}
}

func TestRun(t *testing.T) {
	var stderr bytes.Buffer
	if code := run([]string{"-dir", filepath.Join("testdata", "pass"), "-actions", filepath.Join("testdata", "actions-pass")}, &stderr); code != 0 {
		t.Fatalf("run(pass) = %d, want 0 (stderr %q)", code, stderr.String())
	}
	if got := stderr.String(); got != "" {
		t.Fatalf("run(pass) wrote %q, want nothing", got)
	}

	stderr.Reset()
	if code := run([]string{"-dir", filepath.Join("testdata", "tag"), "-actions", ""}, &stderr); code != 1 {
		t.Fatalf("run(tag) = %d, want 1", code)
	}
	if got := stderr.String(); !strings.Contains(got, "is not pinned to a 40-character commit SHA") {
		t.Fatalf("run(tag) stderr = %q, want the violation", got)
	}

	stderr.Reset()
	if code := run([]string{"-dir", filepath.Join("testdata", "pass"), "-actions", filepath.Join("testdata", "actions-fail")}, &stderr); code != 1 {
		t.Fatalf("run(actions-fail) = %d, want 1", code)
	}
	if got := stderr.String(); !strings.Contains(got, "is not pinned to a 40-character commit SHA") {
		t.Fatalf("run(actions-fail) stderr = %q, want the violation", got)
	}

	stderr.Reset()
	if code := run([]string{"-dir", filepath.Join("testdata", "missing"), "-actions", ""}, &stderr); code != 1 {
		t.Fatalf("run(missing) = %d, want 1", code)
	}

	if code := run([]string{"extra"}, &stderr); code != 2 {
		t.Fatalf("run(extra argument) = %d, want 2", code)
	}
}
