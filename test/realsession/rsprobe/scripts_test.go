package main

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	realSessionWorkflowPath = "../../../.github/workflows/real-session.yml"
	probeScriptsDir         = "../probes"
)

// TestRealSessionWorkflowShape checks the parts of the workflow that decide
// when a probe runs, what it may do, and where its evidence may be uploaded.
// Both jobs are checked: the Linux job keeps the apt-installed tools, and the
// macOS job installs tmux and jq through Homebrew. Each job runs only for its
// own branch prefix, so adding the macOS job leaves the Linux triggers intact.
func TestRealSessionWorkflowShape(t *testing.T) {
	doc := loadRealSessionWorkflow(t)

	triggers := asMap(t, doc["on"], "on")
	if len(triggers) != 1 {
		t.Fatalf("triggers = %v, want only push", mapKeys(triggers))
	}
	push := asMap(t, triggers["push"], "on.push")
	if len(push) != 1 {
		t.Fatalf("push trigger = %v, want only branches", mapKeys(push))
	}
	branches := asSlice(t, push["branches"], "on.push.branches")
	wantBranches := []string{"probe/**", "probe-macos/**"}
	if len(branches) != len(wantBranches) {
		t.Fatalf("on.push.branches = %v, want %v", branches, wantBranches)
	}
	for i, want := range wantBranches {
		if got := asString(t, branches[i], "on.push.branches entry"); got != want {
			t.Errorf("on.push.branches[%d] = %q, want %q", i, got, want)
		}
	}

	permissions := asMap(t, doc["permissions"], "permissions")
	if len(permissions) != 1 || permissions["contents"] != "read" {
		t.Errorf("permissions = %v, want {contents: read}", permissions)
	}

	jobs := asMap(t, doc["jobs"], "jobs")
	if len(jobs) != 2 {
		t.Fatalf("jobs = %v, want probe and probe-macos", mapKeys(jobs))
	}

	// The macOS root is /private/tmp, not /tmp: macOS makes /tmp a symlink,
	// and Claude Code keys workspace trust by the resolved path, so a root
	// under /tmp would miss the pre-accepted entry and stop at the dialog.
	cases := []struct {
		job        string
		prefix     string
		runsOn     string
		root       string
		uploadName string
		tools      string
	}{
		{"probe", "probe/", "ubuntu-24.04", "/tmp/peers-rs", "probe-${{ env.PROBE }}", "apt-get"},
		{"probe-macos", "probe-macos/", "macos-latest", "/private/tmp/peers-rs", "probe-macos-${{ env.PROBE }}", "brew install"},
	}
	for _, tc := range cases {
		t.Run(tc.job, func(t *testing.T) {
			job := asMap(t, jobs[tc.job], "jobs."+tc.job)
			if _, ok := job["permissions"]; ok {
				t.Errorf("jobs.%s has its own permissions block; the job must rely on the workflow-level contents: read", tc.job)
			}
			if got := asString(t, job["runs-on"], "jobs."+tc.job+".runs-on"); got != tc.runsOn {
				t.Errorf("jobs.%s.runs-on = %q, want %q", tc.job, got, tc.runsOn)
			}
			wantIf := "startsWith(github.ref_name, '" + tc.prefix + "')"
			if got := asString(t, job["if"], "jobs."+tc.job+".if"); got != wantIf {
				t.Errorf("jobs.%s.if = %q, want %q", tc.job, got, wantIf)
			}

			env := asMap(t, job["env"], "jobs."+tc.job+".env")
			if _, ok := env["CLAUDE_CODE_OAUTH_TOKEN"]; ok {
				t.Errorf("jobs.%s.env contains CLAUDE_CODE_OAUTH_TOKEN; the token must be step-scoped", tc.job)
			}
			if got := asString(t, env["R"], "jobs."+tc.job+".env.R"); got != tc.root {
				t.Errorf("jobs.%s.env.R = %q, want %q", tc.job, got, tc.root)
			}

			// The token reaches exactly the steps that need it, and no other.
			steps := asSlice(t, job["steps"], "jobs."+tc.job+".steps")
			const wantToken = "${{ secrets.CLAUDE_CODE_OAUTH_TOKEN }}"
			var tokenSteps []string
			for _, raw := range steps {
				step := asMap(t, raw, "workflow step")
				rawEnv, ok := step["env"]
				if !ok {
					continue
				}
				stepEnv := asMap(t, rawEnv, "step env")
				value, ok := stepEnv["CLAUDE_CODE_OAUTH_TOKEN"]
				if !ok {
					continue
				}
				name := asString(t, step["name"], "step name")
				tokenSteps = append(tokenSteps, name)
				if value != wantToken {
					t.Errorf("%s: CLAUDE_CODE_OAUTH_TOKEN = %v, want %q", name, value, wantToken)
				}
			}
			wantTokenSteps := []string{"Prepare", "Run the probe", "Scan the evidence for the token"}
			if strings.Join(tokenSteps, ",") != strings.Join(wantTokenSteps, ",") {
				t.Errorf("%s: steps with CLAUDE_CODE_OAUTH_TOKEN = %v, want %v", tc.job, tokenSteps, wantTokenSteps)
			}

			// The setup steps touch the network and have hung before; each
			// carries a timeout so a stuck download fails the run instead of
			// holding the job until the workflow timeout.
			for name, want := range map[string]int{
				"Install tools":       10,
				"Install Claude Code": 10,
				"Build":               15,
			} {
				step := findWorkflowStepByName(t, steps, name)
				got, ok := step["timeout-minutes"].(int)
				if !ok {
					t.Errorf("%s: %s has no integer timeout-minutes; got %v", tc.job, name, step["timeout-minutes"])
					continue
				}
				if got != want {
					t.Errorf("%s: %s timeout-minutes = %d, want %d", tc.job, name, got, want)
				}
			}

			// The install step names the package manager its runner family
			// provides; the macOS job must not fall back to apt packages.
			install := findWorkflowStepByName(t, steps, "Install tools")
			installRun := asString(t, install["run"], "Install tools run")
			if !strings.Contains(installRun, tc.tools) {
				t.Errorf("%s: Install tools does not use %q", tc.job, tc.tools)
			}
			if tc.job == "probe-macos" && (strings.Contains(installRun, "apt-get") || strings.Contains(installRun, "sudo")) {
				t.Errorf("the macOS job installs packages with apt-get or sudo")
			}

			// The scan is the main defense: it runs before the upload, removes
			// the evidence on a hit, and fails when the token is empty. It runs
			// even when an earlier step failed, so a failed probe still leaves
			// evidence.
			scanIndex := findWorkflowStepIndex(t, steps, "Scan the evidence for the token")
			uploadIndex := findWorkflowStepIndexByUses(t, steps, "actions/upload-artifact@")
			if scanIndex > uploadIndex {
				t.Errorf("%s: the scan step is at index %d, after the upload at %d", tc.job, scanIndex, uploadIndex)
			}
			scan := asMap(t, steps[scanIndex], "scan step")
			if got := asString(t, scan["id"], "scan id"); got != "scan" {
				t.Errorf("%s: the scan step id = %q, want scan", tc.job, got)
			}
			const wantScanIf = "${{ !cancelled() }}"
			if got := asString(t, scan["if"], "scan if"); got != wantScanIf {
				t.Errorf("%s: the scan step if = %q, want %q", tc.job, got, wantScanIf)
			}
			scanRun := asString(t, scan["run"], "scan run")
			for _, want := range []string{
				`-z "$CLAUDE_CODE_OAUTH_TOKEN"`,
				`"$RUNNER_TEMP/build/rsprobe" scan-secret --dir "$R/out" --env CLAUDE_CODE_OAUTH_TOKEN`,
				`rm -rf "$R/out"`,
				// A scan that could not run (exit 2, for argument, environment
				// or unreadable-entry problems) fails the step like a hit, so
				// nothing is uploaded unscanned.
				`if [ "$status" -ne 0 ]; then`,
				`the evidence scan failed`,
			} {
				if !strings.Contains(scanRun, want) {
					t.Errorf("%s: the scan step does not contain %q", tc.job, want)
				}
			}

			// The upload runs only after a passing scan and only when the job
			// was not cancelled; that way a failed probe still uploads its
			// evidence, while a token hit or a cancelled run leaves nothing
			// behind.
			upload := asMap(t, steps[uploadIndex], "upload step")
			const wantUploadIf = "${{ !cancelled() && steps.scan.outcome == 'success' }}"
			if got := asString(t, upload["if"], "upload-artifact if"); got != wantUploadIf {
				t.Errorf("%s: upload-artifact if = %q, want %q", tc.job, got, wantUploadIf)
			}
			with := asMap(t, upload["with"], "upload-artifact.with")
			wantPath := tc.root + "/out/${{ env.PROBE }}/"
			if got := asString(t, with["path"], "upload-artifact.path"); got != wantPath {
				t.Errorf("%s: upload-artifact path = %q, want %q", tc.job, got, wantPath)
			}
			if got := asString(t, with["name"], "upload-artifact.name"); got != tc.uploadName {
				t.Errorf("%s: upload-artifact name = %q, want %q", tc.job, got, tc.uploadName)
			}
			if got := asString(t, with["if-no-files-found"], "upload-artifact.if-no-files-found"); got != "error" {
				t.Errorf("%s: upload-artifact if-no-files-found = %q, want error", tc.job, got)
			}
			if got, ok := with["retention-days"].(int); !ok || got != 7 {
				t.Errorf("%s: upload-artifact retention-days = %v, want 7", tc.job, with["retention-days"])
			}

			// Each job strips its own branch prefix before the shared case
			// list, so the Linux and macOS jobs accept different branch names.
			selectStep := findWorkflowStepByName(t, steps, "Select the probe")
			selectRun := asString(t, selectStep["run"], "Select the probe run")
			if !strings.Contains(selectRun, "${GITHUB_REF_NAME#"+tc.prefix+"}") {
				t.Errorf("%s: Select the probe does not strip the %q prefix", tc.job, tc.prefix)
			}
		})
	}
}

// TestProbeScriptsDoNotDumpEnvironment checks that probe scripts and the
// workflow never print whole environments or enable shell tracing.
func TestProbeScriptsDoNotDumpEnvironment(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(probeScriptsDir, "*.sh"))
	if err != nil {
		t.Fatalf("globbing probe scripts: %v", err)
	}
	paths = append(paths, realSessionWorkflowPath)

	for _, path := range paths {
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		// lib.sh reads /proc/<pid>/environ through rs_proc_vars; that one
		// helper is the reason the function exists, so lib.sh is exempt from
		// the process-environment patterns.
		allowProcEnvReads := filepath.Base(path) == "lib.sh"
		for _, finding := range environmentDumpFindings(string(data), allowProcEnvReads) {
			t.Errorf("%s dumps the environment: %q", path, finding)
		}
	}

	// The check must report a script that turns on shell tracing.
	dir := t.TempDir()
	bad := filepath.Join(dir, "bad.sh")
	if err := os.WriteFile(bad, []byte("#!/usr/bin/env bash\nset -x\n"), 0o600); err != nil {
		t.Fatalf("writing the negative control: %v", err)
	}
	data, err := os.ReadFile(bad)
	if err != nil {
		t.Fatalf("reading the negative control: %v", err)
	}
	if findings := environmentDumpFindings(string(data), false); len(findings) == 0 {
		t.Fatalf("the check does not report a script containing set -x")
	}

	// Each forbidden form is caught, while reading named variables and the
	// workflow's env blocks are not.
	for _, line := range []string{
		"set -x",
		"printenv",
		"declare -p",
		"declare -x",
		"export -p",
		"set",
		"set;",
		"export",
		"compgen -e",
		"set -o xtrace",
		"bash -x probe.sh",
		"/usr/bin/env",
		`"$R/run" /usr/bin/env >f`,
		"/proc/1/environ",
		`cat /proc/1/environ`,
		`env >"$OUT/e.txt"`,
		"env>f",
		"env",
		"cmd; env",
		"then env",
		`p="${X#y}"; env`,
	} {
		if findings := environmentDumpFindings(line, false); len(findings) == 0 {
			t.Errorf("the check does not report %q", line)
		}
	}
	// The run-wrapper form is the most likely way the token reaches the
	// evidence: env is not at the start of a command segment, so a check that
	// only matches a segment-initial env would miss it.
	t.Run("run wrapper env redirect", func(t *testing.T) {
		const line = `"$R/run" env >"$OUT/x"`
		if findings := environmentDumpFindings(line, false); len(findings) == 0 {
			t.Errorf("the check does not report %q", line)
		}
	})

	for _, line := range []string{
		"      env:",
		"env FOO=1 cmd",
		`p="${GITHUB_REF_NAME#probe/}"`,
		"cmd # set -x",
		"set -euo pipefail",
		"export FOO=1",
		"declare -r X=1",
		"compgen -c",
		"bash probe.sh",
		"#!/usr/bin/env bash",
		"environment: ok",
	} {
		if findings := environmentDumpFindings(line, false); len(findings) != 0 {
			t.Errorf("the check reports a harmless line %q as %v", line, findings)
		}
	}

	// lib.sh may read /proc/<pid>/environ, but only through the helper: the
	// same read in any other script is still reported, and /usr/bin/env is
	// reported even in lib.sh because the exemption covers environ alone.
	if findings := environmentDumpFindings("cat /proc/1/environ", true); len(findings) != 0 {
		t.Errorf("the lib.sh exemption still reports /proc/1/environ as %v", findings)
	}
	if findings := environmentDumpFindings("/usr/bin/env", true); len(findings) == 0 {
		t.Errorf("the lib.sh exemption still hides /usr/bin/env")
	}
}

// TestProbeScriptsUseTheLibraryCleanupChain checks that probe scripts register
// cleanup with rs_on_exit instead of installing their own trap. A script that
// installs its own trap replaces the library's, so a failing step would no
// longer capture the windows or run the other registered cleanups.
func TestProbeScriptsUseTheLibraryCleanupChain(t *testing.T) {
	paths, err := filepath.Glob(filepath.Join(probeScriptsDir, "*.sh"))
	if err != nil {
		t.Fatalf("globbing probe scripts: %v", err)
	}
	if len(paths) == 0 {
		t.Fatalf("no probe scripts found in %s", probeScriptsDir)
	}
	for _, path := range paths {
		if filepath.Base(path) == "lib.sh" {
			continue
		}
		data, err := os.ReadFile(path)
		if err != nil {
			t.Fatalf("reading %s: %v", path, err)
		}
		if strings.Contains(string(data), "trap ") {
			t.Errorf("%s installs its own trap; register cleanup with rs_on_exit instead", path)
		}
	}
}

// TestProbeNamesMatchScripts checks that every probe script that exists is a
// known probe, and that the workflow's case list is exactly probeNames.
//
// The reverse direction is asserted too: every name in probeNames must have a
// script under probes/, with lib.sh the only exception.
func TestProbeNamesMatchScripts(t *testing.T) {
	known := make(map[string]bool, len(probeNames))
	for _, name := range probeNames {
		if known[name] {
			t.Fatalf("probeNames lists %q twice", name)
		}
		known[name] = true
	}

	entries, err := os.ReadDir(probeScriptsDir)
	if err != nil {
		t.Fatalf("reading %s: %v", probeScriptsDir, err)
	}
	scripts := make(map[string]bool, len(probeNames))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".sh") {
			continue
		}
		name := strings.TrimSuffix(entry.Name(), ".sh")
		if name == "lib" {
			continue
		}
		scripts[name] = true
		if !known[name] {
			t.Errorf("probe script %s is not in probeNames", entry.Name())
		}
	}
	for _, name := range probeNames {
		if !scripts[name] {
			t.Errorf("probeNames lists %q but probes/%s.sh does not exist", name, name)
		}
	}

	doc := loadRealSessionWorkflow(t)
	if code, ok := unknownProbeExitCode(t, doc); !ok || code == 0 {
		t.Errorf("the case branch for an unknown probe must exit non-zero; exit code = %d (found = %v)", code, ok)
	}

	caseNames := workflowCaseNames(t, doc)
	seen := make(map[string]bool, len(caseNames))
	for _, name := range caseNames {
		if !known[name] {
			t.Errorf("the workflow case lists %q, which is not in probeNames", name)
		}
		if seen[name] {
			t.Errorf("the workflow case lists %q twice", name)
		}
		seen[name] = true
	}
	for _, name := range probeNames {
		if !seen[name] {
			t.Errorf("the workflow case does not list %q", name)
		}
	}

	// The macOS job runs a subset of the same probes. Every name it accepts
	// must be a known probe, and the two this unit runs must be present.
	macOSNames := workflowCaseNamesInJob(t, doc, "probe-macos")
	if len(macOSNames) == 0 {
		t.Errorf("jobs.probe-macos Select the probe has no case labels")
	}
	macOSSeen := make(map[string]bool, len(macOSNames))
	for _, name := range macOSNames {
		if !known[name] {
			t.Errorf("the macOS workflow case lists %q, which is not in probeNames", name)
		}
		if macOSSeen[name] {
			t.Errorf("the macOS workflow case lists %q twice", name)
		}
		macOSSeen[name] = true
	}
	for _, name := range []string{"preflight", "registry"} {
		if !macOSSeen[name] {
			t.Errorf("the macOS workflow case does not list %q", name)
		}
	}
}

// TestLibResolvesComposeFromScriptLocation checks that lib.sh finds
// test/realsession/compose.yaml from its own location rather than the caller's
// working directory. The workflow runs a probe from the repository root, but a
// developer may start one anywhere; a relative -f path then fails with "no
// configuration file provided".
func TestLibResolvesComposeFromScriptLocation(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	env := make([]string, 0, len(os.Environ())+3)
	for _, kv := range os.Environ() {
		// Drop any ambient value so a pre-fix lib.sh cannot pass by accident.
		if strings.HasPrefix(kv, "RS_COMPOSE=") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "R="+root, "PROBE=selftest")

	cmd := exec.Command("bash", "-c", `source "$1"; printf '%s' "$RS_COMPOSE"`, "bash", lib)
	cmd.Dir = root
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sourcing lib.sh from %s: %v\n%s", root, err, out)
	}
	got, err := filepath.EvalSymlinks(strings.TrimSpace(string(out)))
	if err != nil {
		t.Fatalf("resolving RS_COMPOSE %q: %v", strings.TrimSpace(string(out)), err)
	}
	want, err := filepath.Abs(filepath.Join("..", "compose.yaml"))
	if err != nil {
		t.Fatalf("resolving the expected compose path: %v", err)
	}
	want, err = filepath.EvalSymlinks(want)
	if err != nil {
		t.Fatalf("resolving the expected compose path %q: %v", want, err)
	}
	if got != want {
		t.Errorf("RS_COMPOSE = %q, want %q", got, want)
	}
}

// TestLibExportsRSRoot checks that every docker compose call a probe makes
// inherits RS_ROOT. The compose file requires it for its bind mounts, and
// Compose interpolates the file for exec and status calls as well as up, so a
// probe that sets the variable only on the first call fails at the next one.
func TestLibExportsRSRoot(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	var env []string
	for _, kv := range os.Environ() {
		// Drop any ambient value so a pre-fix lib.sh cannot pass by accident.
		if strings.HasPrefix(kv, "RS_ROOT=") {
			continue
		}
		env = append(env, kv)
	}
	env = append(env, "R="+root, "PROBE=selftest")

	// printenv is a child process, so it reports only exported variables.
	cmd := exec.Command("bash", "-c", `source "$1"; printenv RS_ROOT`, "bash", lib)
	cmd.Dir = root
	cmd.Env = env
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sourcing lib.sh: %v\n%s", err, out)
	}
	if got := strings.TrimSpace(string(out)); got != root {
		t.Errorf("exported RS_ROOT = %q, want %q", got, root)
	}
}

// TestSocketOwnerPIDDarwin checks the macOS branch of socket_owner_pid. macOS
// has no ss, so the helper asks lsof for the bound endpoint; a connected
// client's name carries a leading "->" and must not be taken for the listener.
func TestSocketOwnerPIDDarwin(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bin, "uname"), []byte("#!/bin/sh\nprintf 'Darwin\\n'\n"), 0o755); err != nil {
		t.Fatalf("writing the uname stub: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bin, "lsof"), []byte("#!/bin/sh\ncat \"$LSOF_FILE\"\n"), 0o755); err != nil {
		t.Fatalf("writing the lsof stub: %v", err)
	}

	sock := "/private/tmp/peers-501/broker.sock"
	cases := []struct {
		name string
		lsof string
		want string
	}{
		{
			name: "bound endpoint wins over a connected client",
			lsof: "p124\nn->" + sock + "\np123\nn" + sock + "\n",
			want: "123\n",
		},
		{
			name: "a connected client is not the listener",
			lsof: "p125\nn->" + sock + "\n",
			want: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			lsofFile := filepath.Join(root, "lsof.txt")
			if err := os.WriteFile(lsofFile, []byte(tc.lsof), 0o644); err != nil {
				t.Fatalf("writing the lsof output: %v", err)
			}
			cmd := exec.Command("bash", "-c", `source "$1"; socket_owner_pid "$2"`, "bash", lib, sock)
			cmd.Dir = root
			cmd.Env = []string{
				"R=" + root,
				"PROBE=selftest",
				"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
				"LSOF_FILE=" + lsofFile,
			}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("calling socket_owner_pid: %v\n%s", err, out)
			}
			if got := string(out); got != tc.want {
				t.Errorf("socket_owner_pid = %q, want %q", got, tc.want)
			}
		})
	}
}

// TestRsAcceptTrustDialogChoosesTheFocusableEntry checks how the library
// answers the workspace trust dialog. On macOS the probe root's /tmp resolves
// to /private/tmp, so the trust entry written for the literal path does not
// match and Claude Code shows the dialog with "No, exit" focused; pressing
// Enter then ends the session instead of starting it. The helper must move to
// the trust entry in that case and keep the plain Enter otherwise.
func TestRsAcceptTrustDialogChoosesTheFocusableEntry(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	record := filepath.Join(root, "keys.txt")
	stub := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '" + record + "'\n"
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(stub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}

	// The macOS dialog focuses "No, exit"; a dialog that focuses the trust
	// entry keeps the old single Enter; a dialog whose cursor is on neither
	// known option must not be answered at all.
	refusalFocused := "Quick safety check\n ❯ No, exit\n   Yes, I trust this folder\n Enter to confirm"
	trustFocused := "Do you trust the files in this folder?\n ❯ Yes, I trust this folder\n"
	unknownFocused := "Quick safety check\n   No, exit\n   Yes, I trust this folder\n"

	script := `source "$1"; rs_accept_trust_dialog w "$2"; rs_accept_trust_dialog w "$3"; rc=0; rs_accept_trust_dialog w "$4" || rc=$?; printf 'rc=%s\n' "$rc"`
	cmd := exec.Command("bash", "-c", script, "bash", lib, refusalFocused, trustFocused, unknownFocused)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_accept_trust_dialog: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "rc=2"; got != want {
		t.Errorf("unknown focus returned %q, want %q", got, want)
	}

	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("reading the recorded keys: %v", err)
	}
	want := "-L peers-rs send-keys -t w Down Enter\n-L peers-rs send-keys -t w Enter\n"
	if got := string(data); got != want {
		t.Errorf("keys sent = %q, want %q", got, want)
	}
}

// TestRsAcceptTrustDialogSurvivesABrokenPipe checks that the focus search is
// read from grep's own status when the pane writer gets SIGPIPE. bash's printf
// writes the pane line by line; grep -q exits as soon as it matches, and while
// the pane still has content to send the next write fails with EPIPE and kills
// printf with SIGPIPE. Under pipefail a plain "printf ... | grep -q" then
// returns 141, not grep's 0, and the trust dialog was left unanswered with
// "rc=2 sends=0". This pane is about 1MB, far beyond the pipe buffer, and puts
// the focused refusal on its first line, so the writer always has content left
// when grep exits. No write delay or strace is needed, so the test also runs on
// macOS. The injected-exit-2 tests cover a search that could not run; this one
// covers the broken pipe they cannot produce.
func TestRsAcceptTrustDialogSurvivesABrokenPipe(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	record := filepath.Join(root, "keys.txt")
	stub := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '" + record + "'\n"
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(stub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}

	// The focused refusal is on the first line, so grep -q matches in its
	// first read. The padding is about 1MB, far beyond the 64KB pipe buffer,
	// so printf still holds content when grep exits and its next write breaks.
	var pane strings.Builder
	pane.WriteString("Quick safety check\n ❯ No, exit\n   Yes, I trust this folder\n")
	for pane.Len() < 1<<20 {
		pane.WriteString("filler line that carries neither known option\n")
	}
	panePath := filepath.Join(root, "pane.txt")
	if err := os.WriteFile(panePath, []byte(pane.String()), 0o644); err != nil {
		t.Fatalf("writing the pane: %v", err)
	}

	// The pane is read from a file instead of passed as an argument, so the
	// fixture stays under the command-line size limit on macOS.
	cmd := exec.Command("bash", "-c", `source "$1"; pane="$(cat "$2")"; rc=0; rs_accept_trust_dialog w "$pane" || rc=$?; printf 'rc=%s\n' "$rc"`, "bash", lib, panePath)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_accept_trust_dialog: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "rc=0"; got != want {
		t.Fatalf("rs_accept_trust_dialog = %q, want %q; the writer's broken pipe must not hide grep's match", got, want)
	}

	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("reading the recorded keys: %v", err)
	}
	want := "-L peers-rs send-keys -t w Down Enter\n"
	if got := string(data); got != want {
		t.Errorf("keys sent = %q, want one Down Enter burst for the focused refusal", got)
	}
}

// TestRsDismissTrustDialogResendsWhenTheFirstBurstIsDropped checks the retry
// around the trust dialog. Claude Code draws the dialog before it reads keys,
// so a burst sent right after the first capture can be discarded and the next
// frame still shows "No, exit". The helper must resend until the frame
// changes, and report how many bursts it took so the evidence shows the retry.
func TestRsDismissTrustDialogResendsWhenTheFirstBurstIsDropped(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	counter := filepath.Join(root, "bursts")
	record := filepath.Join(root, "keys.txt")
	// The stub drops the first burst: the first two captures show the dialog,
	// and each send-keys call advances the counter.
	stub := `#!/bin/sh
case "$3" in
capture-pane)
	n=$(cat '` + counter + `' 2>/dev/null || echo 0)
	if [ "$n" -lt 2 ]; then
		printf 'Quick safety check\n ❯ No, exit\n   Yes, I trust this folder\n'
	else
		printf '❯ \n'
	fi
	;;
send-keys)
	printf '%s\n' "$*" >> '` + record + `'
	n=$(cat '` + counter + `' 2>/dev/null || echo 0)
	printf '%s' "$((n + 1))" > '` + counter + `'
	;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(stub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}

	cmd := exec.Command("bash", "-c", `source "$1"; if bursts="$(rs_dismiss_trust_dialog w 5)"; then printf 'sends=%s\n' "$bursts"; else printf 'failed after %s\n' "$bursts"; fi`, "bash", lib)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_dismiss_trust_dialog: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "sends=2"; got != want {
		t.Errorf("rs_dismiss_trust_dialog = %q, want %q", got, want)
	}

	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("reading the recorded keys: %v", err)
	}
	want := "-L peers-rs send-keys -t w Down Enter\n-L peers-rs send-keys -t w Down Enter\n"
	if got := string(data); got != want {
		t.Errorf("keys sent = %q, want two Down Enter bursts", got)
	}
}

// TestRsDismissTrustDialogSendsBeforeAnElapsedDeadline checks that a spent
// budget does not turn the helper into a no-op. The helper derives its
// deadline from bash's SECONDS, which counts wall-clock time; a zero budget
// makes the deadline already past at the loop's first check, the condition
// that made CI report "failed after 0" with no keys pressed. The helper must
// still capture the pane and send one burst before it reports the timeout.
func TestRsDismissTrustDialogSendsBeforeAnElapsedDeadline(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	record := filepath.Join(root, "keys.txt")
	stub := `#!/bin/sh
case "$3" in
capture-pane)
	printf 'Quick safety check\n ❯ No, exit\n   Yes, I trust this folder\n'
	;;
send-keys)
	printf '%s\n' "$*" >> '` + record + `'
	;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(stub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}

	cmd := exec.Command("bash", "-c", `source "$1"; if bursts="$(rs_dismiss_trust_dialog w 0)"; then printf 'sends=%s\n' "$bursts"; else printf 'failed after %s\n' "$bursts"; fi`, "bash", lib)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_dismiss_trust_dialog: %v\n%s", err, out)
	}
	got := strings.TrimSpace(string(out))
	if !strings.HasPrefix(got, "failed after ") {
		t.Fatalf("rs_dismiss_trust_dialog = %q, want rc=1 with an elapsed deadline", got)
	}
	sends, err := strconv.Atoi(strings.TrimPrefix(got, "failed after "))
	if err != nil || sends < 1 {
		t.Fatalf("rs_dismiss_trust_dialog = %q, want at least one burst; a spent budget must not skip the first send", got)
	}

	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("reading the recorded keys: %v", err)
	}
	if lines := len(strings.Split(strings.TrimSpace(string(data)), "\n")); lines != sends {
		t.Errorf("recorded %d key bursts, the helper reported %d", lines, sends)
	}
}

// TestRsDismissTrustDialogFailsWhenTheDialogNeverLeaves checks the deadline
// path: a stub keeps the dialog on screen, so the helper must stop at the
// limit and report failure instead of resending forever.
func TestRsDismissTrustDialogFailsWhenTheDialogNeverLeaves(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	record := filepath.Join(root, "keys.txt")
	stub := `#!/bin/sh
case "$3" in
capture-pane)
	printf 'Quick safety check\n ❯ No, exit\n   Yes, I trust this folder\n'
	;;
send-keys)
	printf '%s\n' "$*" >> '` + record + `'
	;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(stub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}

	cmd := exec.Command("bash", "-c", `source "$1"; rc=0; bursts="$(rs_dismiss_trust_dialog w 1)" || rc=$?; printf 'rc=%s sends=%s\n' "$rc" "$bursts"`, "bash", lib)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_dismiss_trust_dialog: %v\n%s", err, out)
	}
	got := strings.TrimSpace(string(out))
	if !strings.HasPrefix(got, "rc=1 sends=") {
		t.Fatalf("rs_dismiss_trust_dialog = %q, want rc=1 at the deadline", got)
	}
	sends, err := strconv.Atoi(strings.TrimPrefix(got, "rc=1 sends="))
	if err != nil || sends < 1 {
		t.Fatalf("sends = %q, want at least one burst before the deadline", got)
	}

	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("reading the recorded keys: %v", err)
	}
	if lines := len(strings.Split(strings.TrimSpace(string(data)), "\n")); lines != sends {
		t.Errorf("recorded %d key bursts, the helper reported %d", lines, sends)
	}
}

// TestRsDismissTrustDialogRetriesWhenTheCaptureFails checks that a failed pane
// capture is not mistaken for the dialog leaving. tmux can fail to capture,
// and its empty output used to end the wait as a successful dismissal. The
// helper must retry within the budget, report success only from a capture that
// succeeded and showed no dialog, and fail at the deadline when the capture
// keeps failing.
func TestRsDismissTrustDialogRetriesWhenTheCaptureFails(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}

	t.Run("retries and dismisses", func(t *testing.T) {
		root := t.TempDir()
		bin := filepath.Join(root, "bin")
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatalf("creating the stub directory: %v", err)
		}
		captures := filepath.Join(root, "captures")
		record := filepath.Join(root, "keys.txt")
		// The first capture fails, the second shows the dialog, and the
		// third shows the pane after the dismissal.
		stub := `#!/bin/sh
case "$3" in
capture-pane)
	n=$(cat '` + captures + `' 2>/dev/null || echo 0)
	printf '%s' "$((n + 1))" > '` + captures + `'
	if [ "$n" -eq 0 ]; then
		exit 1
	fi
	if [ "$n" -eq 1 ]; then
		printf 'Quick safety check\n ❯ No, exit\n   Yes, I trust this folder\n'
	else
		printf '❯ \n'
	fi
	;;
send-keys)
	printf '%s\n' "$*" >> '` + record + `'
	;;
esac
`
		if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(stub), 0o755); err != nil {
			t.Fatalf("writing the tmux stub: %v", err)
		}

		cmd := exec.Command("bash", "-c", `source "$1"; rc=0; bursts="$(rs_dismiss_trust_dialog w 5)" || rc=$?; printf 'rc=%s sends=%s\n' "$rc" "$bursts"`, "bash", lib)
		cmd.Dir = root
		cmd.Env = []string{
			"R=" + root,
			"PROBE=selftest",
			"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("calling rs_dismiss_trust_dialog: %v\n%s", err, out)
		}
		if got, want := strings.TrimSpace(string(out)), "rc=0 sends=1"; got != want {
			t.Fatalf("rs_dismiss_trust_dialog = %q, want %q; a failed capture must not end the wait", got, want)
		}

		data, err := os.ReadFile(record)
		if err != nil {
			t.Fatalf("reading the recorded keys: %v", err)
		}
		want := "-L peers-rs send-keys -t w Down Enter\n"
		if got := string(data); got != want {
			t.Errorf("keys sent = %q, want one Down Enter burst after the capture recovered", got)
		}
	})

	t.Run("fails when the budget is spent", func(t *testing.T) {
		root := t.TempDir()
		bin := filepath.Join(root, "bin")
		if err := os.MkdirAll(bin, 0o755); err != nil {
			t.Fatalf("creating the stub directory: %v", err)
		}
		record := filepath.Join(root, "keys.txt")
		stub := `#!/bin/sh
case "$3" in
capture-pane)
	exit 1
	;;
send-keys)
	printf '%s\n' "$*" >> '` + record + `'
	;;
esac
`
		if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(stub), 0o755); err != nil {
			t.Fatalf("writing the tmux stub: %v", err)
		}

		cmd := exec.Command("bash", "-c", `source "$1"; rc=0; bursts="$(rs_dismiss_trust_dialog w 0)" || rc=$?; printf 'rc=%s sends=%s\n' "$rc" "$bursts"`, "bash", lib)
		cmd.Dir = root
		cmd.Env = []string{
			"R=" + root,
			"PROBE=selftest",
			"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
		}
		out, err := cmd.CombinedOutput()
		if err != nil {
			t.Fatalf("calling rs_dismiss_trust_dialog: %v\n%s", err, out)
		}
		if got, want := strings.TrimSpace(string(out)), "rc=1 sends=0"; got != want {
			t.Fatalf("rs_dismiss_trust_dialog = %q, want %q; a failed capture must not report success", got, want)
		}
		if data, err := os.ReadFile(record); err == nil && len(data) > 0 {
			t.Errorf("the helper pressed keys without a successful capture: %q", data)
		}
	})
}

// realGrepPath returns the absolute path of the grep on PATH, so a stub can
// delegate to it after failing a chosen search.
func realGrepPath(t *testing.T) string {
	t.Helper()
	path, err := exec.LookPath("grep")
	if err != nil {
		t.Fatalf("finding grep: %v", err)
	}
	return path
}

// TestRsDismissTrustDialogRetriesWhenTheFocusCheckFails checks that a search
// that could not run is not read as an unknown focus. grep exits 2 when it
// cannot run, and under a loaded runner that failure made the helper report
// "rc=2 sends=0" without pressing any key, so the wait ended before the dialog
// was answered. A failed search says nothing about the cursor: the helper must
// retry within its budget, and a later search that runs must still dismiss the
// dialog with one burst.
func TestRsDismissTrustDialogRetriesWhenTheFocusCheckFails(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	record := filepath.Join(root, "keys.txt")
	sends := filepath.Join(root, "sends")
	// The dialog leaves only after a burst reaches the pane, so a retry that
	// presses nothing still sees the dialog on its next capture.
	tmuxStub := `#!/bin/sh
case "$3" in
capture-pane)
	if [ "$(cat '` + sends + `' 2>/dev/null || echo 0)" -eq 0 ]; then
		printf 'Quick safety check\n ❯ No, exit\n   Yes, I trust this folder\n'
	else
		printf '❯ \n'
	fi
	;;
send-keys)
	printf '%s\n' "$*" >> '` + record + `'
	n=$(cat '` + sends + `' 2>/dev/null || echo 0)
	printf '%s' "$((n + 1))" > '` + sends + `'
	;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(tmuxStub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}
	// Fail the first focus search with grep's "could not run" status and run
	// every other search for real.
	marker := filepath.Join(root, "failed-once")
	grepStub := `#!/bin/sh
if [ "$1" = "-qF" ] && [ ! -e '` + marker + `' ]; then
	: > '` + marker + `'
	exit 2
fi
exec '` + realGrepPath(t) + `' "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "grep"), []byte(grepStub), 0o755); err != nil {
		t.Fatalf("writing the grep stub: %v", err)
	}

	cmd := exec.Command("bash", "-c", `source "$1"; rc=0; bursts="$(rs_dismiss_trust_dialog w 5)" || rc=$?; printf 'rc=%s sends=%s\n' "$rc" "$bursts"`, "bash", lib)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_dismiss_trust_dialog: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "rc=0 sends=1"; got != want {
		t.Fatalf("rs_dismiss_trust_dialog = %q, want %q; a search that could not run must not end the wait", got, want)
	}

	data, err := os.ReadFile(record)
	if err != nil {
		t.Fatalf("reading the recorded keys: %v", err)
	}
	want := "-L peers-rs send-keys -t w Down Enter\n"
	if got := string(data); got != want {
		t.Errorf("keys sent = %q, want one Down Enter burst after the search recovered", got)
	}
}

// TestRsAcceptTrustDialogRetriesWhenTheFocusSearchFails checks the contract a
// failed search gets from rs_accept_trust_dialog: 3, meaning the cursor is
// unknown and the caller must retry, not 2, which would tell the caller the
// dialog carries no known focus and stop it from pressing any key.
func TestRsAcceptTrustDialogRetriesWhenTheFocusSearchFails(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	record := filepath.Join(root, "keys.txt")
	tmuxStub := "#!/bin/sh\nprintf '%s\\n' \"$*\" >> '" + record + "'\n"
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(tmuxStub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}
	grepStub := `#!/bin/sh
if [ "$1" = "-qF" ]; then
	exit 2
fi
exec '` + realGrepPath(t) + `' "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "grep"), []byte(grepStub), 0o755); err != nil {
		t.Fatalf("writing the grep stub: %v", err)
	}

	script := `source "$1"; rc=0; rs_accept_trust_dialog w "$2" || rc=$?; printf 'rc=%s\n' "$rc"`
	pane := "Quick safety check\n ❯ No, exit\n   Yes, I trust this folder\n"
	cmd := exec.Command("bash", "-c", script, "bash", lib, pane)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_accept_trust_dialog: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "rc=3"; got != want {
		t.Fatalf("rs_accept_trust_dialog = %q, want %q; a failed search is not an unknown focus", got, want)
	}
	if data, err := os.ReadFile(record); err == nil && len(data) > 0 {
		t.Errorf("the helper pressed keys from a failed search: %q", data)
	}
}

// TestRsWaitTurnIdleDoesNotCountAFailedSearchAsQuiet checks that a search that
// could not run is not counted as a quiet poll. Five such polls used to add up
// to an idle verdict about a pane the helper had never inspected.
func TestRsWaitTurnIdleDoesNotCountAFailedSearchAsQuiet(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	tmuxStub := `#!/bin/sh
case "$3" in
capture-pane)
	printf '❯ \n'
	;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(tmuxStub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}
	grepStub := `#!/bin/sh
if [ "$1" = "-qF" ]; then
	exit 2
fi
exec '` + realGrepPath(t) + `' "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "grep"), []byte(grepStub), 0o755); err != nil {
		t.Fatalf("writing the grep stub: %v", err)
	}

	cmd := exec.Command("bash", "-c", `source "$1"; rc=0; rs_wait_turn_idle w 2 || rc=$?; printf 'rc=%s\n' "$rc"`, "bash", lib)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_wait_turn_idle: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "rc=1"; got != want {
		t.Fatalf("rs_wait_turn_idle = %q, want %q; a search that could not run is not evidence of a quiet turn", got, want)
	}
}

// TestRsReadyCriterionDoesNotTrustAFailedDialogSearch checks that a pane is
// not called ready when the search that rules out a dialog could not run. The
// guard used to fall through on a failed search, so a criterion visible behind
// a dialog it never inspected could end the wait as ready.
func TestRsReadyCriterionDoesNotTrustAFailedDialogSearch(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	grepStub := `#!/bin/sh
if [ "$1" = "-qiE" ]; then
	exit 2
fi
exec '` + realGrepPath(t) + `' "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "grep"), []byte(grepStub), 0o755); err != nil {
		t.Fatalf("writing the grep stub: %v", err)
	}

	script := `source "$1"; rc=0; out="$(rs_ready_criterion "$2")" || rc=$?; printf 'rc=%s out=%s\n' "$rc" "$out"`
	pane := "Yes, I trust this folder\n? for shortcuts\n"
	cmd := exec.Command("bash", "-c", script, "bash", lib, pane)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_ready_criterion: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "rc=1 out="; got != want {
		t.Fatalf("rs_ready_criterion = %q, want %q; a pane could not be called ready when its dialog search could not run", got, want)
	}
}

// TestRsPaneIdleDoesNotTrustAFailedTurnSearch checks that a pane is not called
// idle when the search for the running-turn hint could not run. The retry
// around that search must not turn a failed check into an idle verdict.
func TestRsPaneIdleDoesNotTrustAFailedTurnSearch(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	tmuxStub := `#!/bin/sh
case "$3" in
capture-pane)
	printf 'esc to interrupt\nTry "\n'
	;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(tmuxStub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}
	grepStub := `#!/bin/sh
for a in "$@"; do
	if [ "$a" = "esc to interrupt" ]; then
		exit 2
	fi
done
exec '` + realGrepPath(t) + `' "$@"
`
	if err := os.WriteFile(filepath.Join(bin, "grep"), []byte(grepStub), 0o755); err != nil {
		t.Fatalf("writing the grep stub: %v", err)
	}

	cmd := exec.Command("bash", "-c", `source "$1"; rc=0; rs_pane_idle w 1 || rc=$?; printf 'rc=%s\n' "$rc"`, "bash", lib)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_pane_idle: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "rc=1"; got != want {
		t.Fatalf("rs_pane_idle = %q, want %q; a search that could not run is not evidence that the turn ended", got, want)
	}
}

// TestRsDismissTrustDialogRefusesAnUnknownFocus checks that a recognized
// dialog whose cursor is on neither known option fails immediately without
// pressing a key, so a blind Enter cannot confirm another screen.
func TestRsDismissTrustDialogRefusesAnUnknownFocus(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	record := filepath.Join(root, "keys.txt")
	stub := `#!/bin/sh
case "$3" in
capture-pane)
	printf 'Quick safety check\n   No, exit\n   Yes, I trust this folder\n'
	;;
send-keys)
	printf '%s\n' "$*" >> '` + record + `'
	;;
esac
`
	if err := os.WriteFile(filepath.Join(bin, "tmux"), []byte(stub), 0o755); err != nil {
		t.Fatalf("writing the tmux stub: %v", err)
	}

	cmd := exec.Command("bash", "-c", `source "$1"; rc=0; bursts="$(rs_dismiss_trust_dialog w 5)" || rc=$?; printf 'rc=%s sends=%s\n' "$rc" "$bursts"`, "bash", lib)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + bin + string(os.PathListSeparator) + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("calling rs_dismiss_trust_dialog: %v\n%s", err, out)
	}
	if got, want := strings.TrimSpace(string(out)), "rc=2 sends=0"; got != want {
		t.Errorf("rs_dismiss_trust_dialog = %q, want %q", got, want)
	}
	if data, err := os.ReadFile(record); err == nil && len(data) > 0 {
		t.Errorf("the helper pressed keys on an unknown focus: %q", data)
	}
}

// TestRsHostaRecvWritesArtifactSafeName checks the local receive-log name. The
// Agent name may contain characters an artifact upload rejects, such as the
// "<" in "a<b", and the log of such an Agent must still be uploadable without
// colliding with another Agent's log.
func TestRsHostaRecvWritesArtifactSafeName(t *testing.T) {
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	out := filepath.Join(root, "out", "sig")
	if err := os.MkdirAll(out, 0o755); err != nil {
		t.Fatalf("creating the output directory: %v", err)
	}
	bin := filepath.Join(root, "bin")
	if err := os.MkdirAll(bin, 0o755); err != nil {
		t.Fatalf("creating the stub directory: %v", err)
	}
	if err := os.WriteFile(filepath.Join(bin, "docker"), []byte("#!/bin/sh\nprintf 'recv-line\\n'\n"), 0o755); err != nil {
		t.Fatalf("writing the docker stub: %v", err)
	}

	cmd := exec.Command("bash", "-c", `source "$1"; rs_hosta_recv "$2"; rs_hosta_recv "$3"`, "bash", lib, "a<b", `a\b`)
	cmd.Env = []string{"R=" + root, "PROBE=sig", "PATH=" + bin + ":" + os.Getenv("PATH")}
	if data, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("calling rs_hosta_recv: %v\n%s", err, data)
	}

	entries, err := os.ReadDir(out)
	if err != nil {
		t.Fatalf("reading the output directory: %v", err)
	}
	var names []string
	for _, entry := range entries {
		name := entry.Name()
		if !strings.HasPrefix(name, "hosta-") || !strings.HasSuffix(name, ".recv.jsonl") {
			continue
		}
		if strings.ContainsAny(name, "\" :<>|*?\r\n\\") {
			t.Errorf("receive log %q contains a character an artifact upload rejects", name)
		}
		names = append(names, name)
	}
	if len(names) != 2 {
		t.Fatalf("receive logs = %v, want 2", names)
	}
	if names[0] == names[1] {
		t.Errorf("receive logs share the name %q; distinct Agents must keep distinct logs", names[0])
	}
}

// TestRsProcVarsReportsAbsence checks that rs_proc_vars distinguishes a
// missing variable from one set to the empty string. A probe treats the two
// differently, so the helper must not print both as NAME=. The helper reads
// /proc/<pid>/environ, which only Linux provides; the probe runner is Linux,
// so every other platform skips this test instead of failing on a missing
// /proc.
func TestRsProcVarsReportsAbsence(t *testing.T) {
	if runtime.GOOS != "linux" {
		t.Skip("rs_proc_vars reads /proc/<pid>/environ, which only Linux provides")
	}
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}
	root := t.TempDir()
	cmd := exec.Command("bash", "-c", `source "$1"; rs_proc_vars "$$" RS_PRESENT RS_EMPTY RS_ABSENT`, "bash", lib)
	cmd.Dir = root
	// A fixed environment makes the absent variable reliably absent; bash
	// records it in /proc/<pid>/environ exactly as given.
	cmd.Env = []string{
		"R=" + root,
		"PROBE=selftest",
		"PATH=" + os.Getenv("PATH"),
		"RS_PRESENT=value",
		"RS_EMPTY=",
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sourcing lib.sh in a controlled environment: %v\n%s", err, out)
	}
	want := "RS_PRESENT=value\nRS_EMPTY=\nRS_ABSENT absent\n"
	if string(out) != want {
		t.Errorf("rs_proc_vars output = %q, want %q", out, want)
	}
}

// TestRsWaitReplyReadsAssistantTextOnly checks the shared reply predicate:
// the prompt echo in a user record must not count as an answer, only text
// blocks count, and a transcript without assistant text is reported as its
// own result so a probe can say it fell back to the pane.
func TestRsWaitReplyReadsAssistantTextOnly(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("rs_wait_reply parses transcripts with jq")
	}
	lib, err := filepath.Abs(filepath.Join(probeScriptsDir, "lib.sh"))
	if err != nil {
		t.Fatalf("resolving the lib.sh path: %v", err)
	}

	writeTranscript := func(t *testing.T, lines ...string) string {
		t.Helper()
		root := t.TempDir()
		dir := filepath.Join(root, "cfg", "projects", "p")
		if err := os.MkdirAll(dir, 0o755); err != nil {
			t.Fatalf("creating the transcript directory: %v", err)
		}
		data := strings.Join(lines, "\n") + "\n"
		if err := os.WriteFile(filepath.Join(dir, "t.jsonl"), []byte(data), 0o600); err != nil {
			t.Fatalf("writing the transcript: %v", err)
		}
		if err := os.MkdirAll(filepath.Join(root, "out", "rsreply"), 0o755); err != nil {
			t.Fatalf("creating the output directory: %v", err)
		}
		return root
	}

	cases := []struct {
		name        string
		lines       []string
		marker      string
		wantStatus  int
		wantRecords int
	}{
		{
			name: "assistant text counts and the prompt echo does not",
			lines: []string{
				`{"type":"user","message":{"role":"user","content":"reply with rs-test-ok"}}`,
				`{"type":"assistant","message":{"content":[{"type":"text","text":"rs-test-ok"}]}}`,
				`{"type":"assistant","message":{"content":"rs-test-ok"}}`,
				`{"type":"assistant","message":{"content":[{"type":"tool_use","name":"x","input":{"text":"rs-test-ok"}}]}}`,
				`null`,
				`not json`,
			},
			marker:      "rs-test-ok",
			wantStatus:  0,
			wantRecords: 2,
		},
		{
			name: "assistant text without the marker is a miss",
			lines: []string{
				`{"type":"assistant","message":{"content":[{"type":"text","text":"something else"}]}}`,
			},
			marker:      "rs-test-ok",
			wantStatus:  1,
			wantRecords: 0,
		},
		{
			name: "no assistant text is a distinct result",
			lines: []string{
				`{"type":"user","message":{"role":"user","content":"rs-test-ok"}}`,
			},
			marker:      "rs-test-ok",
			wantStatus:  2,
			wantRecords: 0,
		},
	}

	const script = `source "$1"; status=0; rs_wait_reply "$2" reply.jsonl 1 || status=$?; printf 'status=%d\n' "$status"; printf 'records=%s\n' "$(wc -l <"$OUT/reply.jsonl" | tr -d ' ')"`
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			root := writeTranscript(t, tc.lines...)
			cmd := exec.Command("bash", "-c", script, "bash", lib, tc.marker)
			cmd.Dir = root
			cmd.Env = []string{
				"R=" + root,
				"PROBE=rsreply",
				"PATH=" + os.Getenv("PATH"),
			}
			out, err := cmd.CombinedOutput()
			if err != nil {
				t.Fatalf("running rs_wait_reply: %v\n%s", err, out)
			}
			want := "status=" + strconv.Itoa(tc.wantStatus) + "\nrecords=" + strconv.Itoa(tc.wantRecords) + "\n"
			if string(out) != want {
				t.Errorf("rs_wait_reply output = %q, want %q", out, want)
			}
		})
	}
}

// TestRatelimitWriteArrivalsCallArity checks that every rl_write_arrivals call
// passes the positional parameters its definition reads. The probe failed on
// the runner with "$3: unbound variable": the function had a label parameter
// that the call sites never supplied, and the mismatch only surfaced after the
// first burst had run.
func TestRatelimitWriteArrivalsCallArity(t *testing.T) {
	path := filepath.Join(probeScriptsDir, "ratelimit.sh")
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("reading %s: %v", path, err)
	}
	text := string(data)

	defRE := regexp.MustCompile(`(?m)^rl_write_arrivals\(\)\s*\{\s*\n\s*local\s+([^\n]*)$`)
	match := defRE.FindStringSubmatch(text)
	if match == nil {
		t.Fatalf("%s has no rl_write_arrivals function with a local declaration", path)
	}
	posRE := regexp.MustCompile(`\$([0-9]+)`)
	declared := 0
	for _, p := range posRE.FindAllStringSubmatch(match[1], -1) {
		n, err := strconv.Atoi(p[1])
		if err != nil {
			t.Fatalf("parsing positional parameter %q: %v", p[0], err)
		}
		if n > declared {
			declared = n
		}
	}
	if declared == 0 {
		t.Fatalf("rl_write_arrivals declares no positional parameters")
	}

	callRE := regexp.MustCompile(`(?m)^\s*rl_write_arrivals\s+(.+)$`)
	calls := callRE.FindAllStringSubmatch(text, -1)
	if len(calls) == 0 {
		t.Fatalf("%s never calls rl_write_arrivals", path)
	}
	wordRE := regexp.MustCompile(`\S+`)
	for _, call := range calls {
		args := wordRE.FindAllString(call[1], -1)
		if len(args) != declared {
			t.Errorf("rl_write_arrivals %q passes %d arguments, but the function declares %d", call[1], len(args), declared)
		}
	}
}

// TestRatelimitQueueCounts checks the queue-depth helper the ratelimit probe
// uses to decide that the receiver has finished processing delivered messages.
// Claude Code records enqueue, dequeue and remove operations in the session
// transcript; a positive depth means messages are still waiting, so an idle
// session is not yet quiescent.
func TestRatelimitQueueCounts(t *testing.T) {
	if _, err := exec.LookPath("jq"); err != nil {
		t.Skip("the transcript helper parses records with jq")
	}
	script, err := filepath.Abs(filepath.Join(probeScriptsDir, "ratelimit.sh"))
	if err != nil {
		t.Fatalf("resolving ratelimit.sh: %v", err)
	}
	root := t.TempDir()
	dir := filepath.Join(root, "cfg", "projects", "p")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatalf("creating the transcript directory: %v", err)
	}
	lines := []string{
		`{"type":"queue-operation","operation":"enqueue"}`,
		`{"type":"queue-operation","operation":"enqueue"}`,
		`{"type":"queue-operation","operation":"enqueue"}`,
		`{"type":"queue-operation","operation":"dequeue"}`,
		`{"type":"queue-operation","operation":"remove"}`,
		`{"type":"user","message":{"role":"user","content":"not a queue record"}}`,
	}
	if err := os.WriteFile(filepath.Join(dir, "t.jsonl"), []byte(strings.Join(lines, "\n")+"\n"), 0o600); err != nil {
		t.Fatalf("writing the transcript: %v", err)
	}

	// $0 is the probe script, so its source of lib.sh resolves next to it.
	cmd := exec.Command("bash", "-c", `source "$1"; rl_queue_counts`, script, script)
	cmd.Dir = root
	cmd.Env = []string{
		"R=" + root,
		"PROBE=ratelimit",
		"RS_SOURCE_ONLY=1",
		"PATH=" + os.Getenv("PATH"),
	}
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("sourcing ratelimit.sh: %v\n%s", err, out)
	}
	if got, want := string(out), "1 3 1 1\n"; got != want {
		t.Errorf("rl_queue_counts = %q, want %q", got, want)
	}
}

// environmentDumpFindings returns the non-comment lines that dump an
// environment or enable shell tracing. allowProcEnvReads exempts only the
// /proc/<pid>/environ read in lib.sh, whose rs_proc_vars helper needs it; any
// other environment read, including /usr/bin/env, is reported everywhere.
func environmentDumpFindings(text string, allowProcEnvReads bool) []string {
	patterns := []*regexp.Regexp{
		regexp.MustCompile(`\bset\s+-[a-zA-Z]*x\b`),
		regexp.MustCompile(`\bprintenv\b`),
		regexp.MustCompile(`declare\s+-p`),
		regexp.MustCompile(`declare\s+-[a-zA-Z]*x\b`),
		regexp.MustCompile(`export\s+-p`),
		regexp.MustCompile(`\bcompgen\s+-[a-zA-Z]*e\b`),
		regexp.MustCompile(`\bset\s+-o\s+xtrace\b`),
		regexp.MustCompile(`\bbash\s+-[a-zA-Z]*x\b`),
		regexp.MustCompile(`(^|[;&|(]\s*|\bthen\s+|\bdo\s+)set\s*($|[;&|)])`),
		regexp.MustCompile(`(^|[;&|(]\s*|\bthen\s+|\bdo\s+)export\s*($|[;&|)])`),
		// A bare env anywhere on the line is a dump when it is followed by a
		// redirection or the end of the line. The form '"$R/run" env >file'
		// is the most likely way a token reaches the evidence: env is not at
		// the start of a command segment, so the segment-relative patterns
		// above miss it.
		regexp.MustCompile(`\benv\s*(>|$)`),
		regexp.MustCompile(`/usr/bin/env\b`),
	}
	if !allowProcEnvReads {
		patterns = append(patterns, regexp.MustCompile(`\benviron\b`))
	}
	var findings []string
	for _, line := range strings.Split(text, "\n") {
		line = stripShellComment(line)
		for _, pattern := range patterns {
			if pattern.MatchString(line) {
				findings = append(findings, strings.TrimSpace(line))
				break
			}
		}
	}
	return findings
}

// stripShellComment removes a comment, which in shell and YAML starts at a
// '#' that is at the beginning of the line or follows whitespace. A '#' inside
// a word such as ${GITHUB_REF_NAME#probe/} is not a comment.
func stripShellComment(line string) string {
	for i := 0; i < len(line); i++ {
		if line[i] != '#' {
			continue
		}
		if i == 0 || line[i-1] == ' ' || line[i-1] == '\t' {
			return line[:i]
		}
	}
	return line
}

func loadRealSessionWorkflow(t *testing.T) map[string]any {
	t.Helper()
	data, err := os.ReadFile(realSessionWorkflowPath)
	if err != nil {
		t.Fatalf("reading %s: %v", realSessionWorkflowPath, err)
	}
	var doc map[string]any
	if err := yaml.Unmarshal(data, &doc); err != nil {
		t.Fatalf("parsing %s: %v", realSessionWorkflowPath, err)
	}
	return doc
}

func realSessionProbeJob(t *testing.T, doc map[string]any) map[string]any {
	t.Helper()
	jobs := asMap(t, doc["jobs"], "jobs")
	return asMap(t, jobs["probe"], "jobs.probe")
}

func findWorkflowStepByName(t *testing.T, steps []any, name string) map[string]any {
	t.Helper()
	for _, raw := range steps {
		step := asMap(t, raw, "workflow step")
		if got, _ := step["name"].(string); got == name {
			return step
		}
	}
	t.Fatalf("the workflow has no step named %q", name)
	return nil
}

// findWorkflowStepIndex returns the position of the step with this name.
func findWorkflowStepIndex(t *testing.T, steps []any, name string) int {
	t.Helper()
	for i, raw := range steps {
		step := asMap(t, raw, "workflow step")
		if got, _ := step["name"].(string); got == name {
			return i
		}
	}
	t.Fatalf("the workflow has no step named %q", name)
	return -1
}

// findWorkflowStepIndexByUses returns the position of the step using an action
// whose name starts with prefix.
func findWorkflowStepIndexByUses(t *testing.T, steps []any, prefix string) int {
	t.Helper()
	for i, raw := range steps {
		step := asMap(t, raw, "workflow step")
		if got, _ := step["uses"].(string); strings.HasPrefix(got, prefix) {
			return i
		}
	}
	t.Fatalf("the workflow has no step using %s", prefix)
	return -1
}

// unknownProbeExitCode returns the exit code of the case branch that handles
// an unknown probe. The second result is false when no such branch exists.
func unknownProbeExitCode(t *testing.T, doc map[string]any) (int, bool) {
	t.Helper()
	probe := realSessionProbeJob(t, doc)
	steps := asSlice(t, probe["steps"], "jobs.probe.steps")
	selectStep := findWorkflowStepByName(t, steps, "Select the probe")
	run := asString(t, selectStep["run"], "Select the probe run")

	var body []string
	inBranch := false
	for _, line := range strings.Split(run, "\n") {
		trimmed := strings.TrimSpace(line)
		if !inBranch {
			if !strings.HasPrefix(trimmed, "*)") {
				continue
			}
			inBranch = true
			trimmed = strings.TrimSpace(strings.TrimPrefix(trimmed, "*)"))
		}
		if end := strings.Index(trimmed, ";;"); end >= 0 {
			body = append(body, trimmed[:end])
			return exitCodeIn(strings.Join(body, "\n"))
		}
		if trimmed == "esac" {
			return exitCodeIn(strings.Join(body, "\n"))
		}
		body = append(body, trimmed)
	}
	return 0, false
}

var exitCodePattern = regexp.MustCompile(`\bexit\s+([0-9]+)\b`)

func exitCodeIn(body string) (int, bool) {
	match := exitCodePattern.FindStringSubmatch(body)
	if match == nil {
		return 0, true
	}
	code, err := strconv.Atoi(match[1])
	if err != nil {
		return 0, false
	}
	return code, true
}

func workflowCaseNames(t *testing.T, doc map[string]any) []string {
	t.Helper()
	return workflowCaseNamesInJob(t, doc, "probe")
}

// workflowCaseNamesInJob returns the labels of a job's "Select the probe" case
// list, excluding the catch-all.
func workflowCaseNamesInJob(t *testing.T, doc map[string]any, jobName string) []string {
	t.Helper()
	jobs := asMap(t, doc["jobs"], "jobs")
	job := asMap(t, jobs[jobName], "jobs."+jobName)
	steps := asSlice(t, job["steps"], "jobs."+jobName+".steps")
	selectStep := findWorkflowStepByName(t, steps, "Select the probe")
	run := asString(t, selectStep["run"], "Select the probe run")

	var names []string
	inCase := false
	for _, line := range strings.Split(run, "\n") {
		trimmed := strings.TrimSpace(line)
		switch {
		case strings.HasPrefix(trimmed, "case "):
			inCase = true
		case inCase && trimmed == "esac":
			return names
		case inCase:
			if end := strings.IndexByte(trimmed, ')'); end >= 0 {
				pattern := strings.TrimSpace(trimmed[:end])
				if pattern != "" && pattern != "*" {
					names = append(names, pattern)
				}
			}
		}
	}
	t.Fatalf("the Select the probe step has no closing esac")
	return nil
}

func asMap(t *testing.T, value any, what string) map[string]any {
	t.Helper()
	typed, ok := value.(map[string]any)
	if !ok {
		t.Fatalf("%s is %T, want a mapping", what, value)
	}
	return typed
}

func asSlice(t *testing.T, value any, what string) []any {
	t.Helper()
	typed, ok := value.([]any)
	if !ok {
		t.Fatalf("%s is %T, want a sequence", what, value)
	}
	return typed
}

func asString(t *testing.T, value any, what string) string {
	t.Helper()
	typed, ok := value.(string)
	if !ok {
		t.Fatalf("%s is %T, want a string", what, value)
	}
	return typed
}

func mapKeys(m map[string]any) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
