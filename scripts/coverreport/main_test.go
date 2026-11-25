package main

import (
	"bytes"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
)

type fixture struct {
	root   string
	inputs []string
}

func copyModule(t *testing.T) string {
	t.Helper()
	src := filepath.Join("testdata", "module")
	dst := t.TempDir()
	err := filepath.WalkDir(src, func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		rel, err := filepath.Rel(src, p)
		if err != nil {
			return err
		}
		target := filepath.Join(dst, rel)
		if d.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		data, err := os.ReadFile(p)
		if err != nil {
			return err
		}
		return os.WriteFile(target, data, 0o644)
	})
	if err != nil {
		t.Fatalf("copy fixture module: %v", err)
	}
	return dst
}

func runGo(t *testing.T, dir string, args ...string) string {
	t.Helper()
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go %s: %v\n%s", strings.Join(args, " "), err, out)
	}
	return string(out)
}

func runBinary(t *testing.T, dir, bin, coverDir string) {
	t.Helper()
	cmd := exec.Command(bin)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GOWORK=off", "GOCOVERDIR="+coverDir)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("run %s: %v\n%s", bin, err, out)
	}
}

func buildFixture(t *testing.T) *fixture {
	t.Helper()
	root := copyModule(t)
	work := t.TempDir()
	mainDir := filepath.Join(work, "main")
	if err := os.MkdirAll(mainDir, 0o755); err != nil {
		t.Fatal(err)
	}
	bin := filepath.Join(work, "peers")
	runGo(t, root, "build", "-cover", "-covermode=atomic", "-coverpkg=example.com/fx/...", "-o", bin, "./cmd/peers")
	runBinary(t, root, bin, mainDir)

	taggedDir := filepath.Join(work, "tagged")
	if err := os.MkdirAll(taggedDir, 0o755); err != nil {
		t.Fatal(err)
	}
	runGo(t, root, "test", "-count=1", "-tags", "peers_testenv", "-covermode=atomic", "-cover",
		"./internal/tagged", "-args", "-test.gocoverdir="+taggedDir)
	return &fixture{root: root, inputs: []string{mainDir, taggedDir}}
}

func (f *fixture) args(extra ...string) []string {
	return append([]string{"-i", strings.Join(f.inputs, ","), "-root", f.root}, extra...)
}

func runTool(t *testing.T, args ...string) (code int, stdout, stderr string) {
	t.Helper()
	var out, errBuf bytes.Buffer
	code = run(args, &out, &errBuf)
	return code, out.String(), errBuf.String()
}

func loadReport(t *testing.T, args ...string) *report {
	t.Helper()
	cfg, err := parseConfig(args)
	if err != nil {
		t.Fatalf("parseConfig: %v", err)
	}
	rep, err := buildReport(cfg)
	if err != nil {
		t.Fatalf("buildReport: %v", err)
	}
	return rep
}

type textRow struct {
	path       string
	statements int
	covered    int
	percent    string
	status     string
}

func parseTextReport(t *testing.T, out string) (rows []textRow, total textRow) {
	t.Helper()
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) == 0 {
		t.Fatalf("empty report")
	}
	header := strings.Fields(lines[0])
	want := []string{"package", "statements", "covered", "percent", "status"}
	if strings.Join(header, " ") != strings.Join(want, " ") {
		t.Fatalf("header = %q, want %q", header, want)
	}
	for _, line := range lines[1:] {
		f := strings.Fields(line)
		if len(f) != 5 {
			t.Fatalf("row %q has %d fields", line, len(f))
		}
		row := textRow{path: f[0], percent: f[3], status: f[4]}
		var err error
		row.statements, err = strconv.Atoi(f[1])
		if err != nil {
			t.Fatalf("row %q: %v", line, err)
		}
		row.covered, err = strconv.Atoi(f[2])
		if err != nil {
			t.Fatalf("row %q: %v", line, err)
		}
		if row.path == "total" {
			total = row
			continue
		}
		rows = append(rows, row)
	}
	return rows, total
}

func findRow(t *testing.T, rows []textRow, path string) textRow {
	t.Helper()
	for _, r := range rows {
		if r.path == path {
			return r
		}
	}
	t.Fatalf("no row for %s in %v", path, rowPaths(rows))
	return textRow{}
}

func rowPaths(rows []textRow) []string {
	var paths []string
	for _, r := range rows {
		paths = append(paths, r.path)
	}
	return paths
}

func controlStatements(t *testing.T, root, pkgDir string, tags ...string) int {
	t.Helper()
	out := filepath.Join(t.TempDir(), "cover.out")
	args := []string{"test", "-count=1", "-covermode=atomic", "-coverprofile=" + out}
	if len(tags) > 0 {
		args = append(args, "-tags", strings.Join(tags, ","))
	}
	args = append(args, "./"+pkgDir)
	runGo(t, root, args...)
	data, err := os.ReadFile(out)
	if err != nil {
		t.Fatalf("read control profile: %v", err)
	}
	total := 0
	for i, line := range strings.Split(string(data), "\n") {
		if i == 0 || strings.TrimSpace(line) == "" {
			continue
		}
		f := strings.Fields(line)
		if len(f) != 3 {
			t.Fatalf("bad control profile line %q", line)
		}
		n, err := strconv.Atoi(f[1])
		if err != nil {
			t.Fatalf("bad control profile line %q: %v", line, err)
		}
		total += n
	}
	return total
}

func TestReportFixture(t *testing.T) {
	fx := buildFixture(t)
	code, out, stderr := runTool(t, fx.args()...)
	if code != 0 {
		t.Fatalf("exit %d, stderr:\n%s", code, stderr)
	}
	if stderr != "" {
		t.Errorf("stderr: %s", stderr)
	}
	rows, total := parseTextReport(t, out)

	wantPaths := []string{
		"example.com/fx/cmd/peers",
		"example.com/fx/internal/core",
		"example.com/fx/internal/mixed",
		"example.com/fx/internal/nearonly",
		"example.com/fx/internal/pbonly",
		"example.com/fx/internal/tagged",
	}
	gotPaths := rowPaths(rows)
	sort.Strings(gotPaths)
	if strings.Join(gotPaths, "\n") != strings.Join(wantPaths, "\n") {
		t.Fatalf("paths = %v, want %v", gotPaths, wantPaths)
	}
	for _, absent := range []string{"internal/testkit", "example.com/fx/test", "scripts/support", "internal/zero", "internal/genonly"} {
		for _, p := range gotPaths {
			if strings.Contains(p, absent) {
				t.Errorf("row %s should not be in the table", p)
			}
		}
	}
	if total.path != "total" {
		t.Fatalf("missing total row")
	}
	for _, r := range append(append([]textRow(nil), rows...), total) {
		if r.status != "-" {
			t.Errorf("status for %s without thresholds = %q, want -", r.path, r.status)
		}
	}

	// Statement counts come from the profile for present packages; the
	// control runs confirm the denominator matches the toolchain.
	coreRow := findRow(t, rows, "example.com/fx/internal/core")
	if got, want := coreRow.statements, controlStatements(t, copyModule(t), "internal/core"); got != want {
		t.Errorf("core statements = %d, control = %d", got, want)
	}
	if coreRow.covered >= coreRow.statements {
		t.Errorf("core should have uncovered statements, got %d/%d", coreRow.covered, coreRow.statements)
	}
	peersRow := findRow(t, rows, "example.com/fx/cmd/peers")
	if got, want := peersRow.statements, controlStatements(t, copyModule(t), "cmd/peers"); got != want {
		t.Errorf("cmd/peers statements = %d, control = %d", got, want)
	}

	// A generated file in an otherwise counted package is excluded.
	mixedControl := copyModule(t)
	if err := os.Remove(filepath.Join(mixedControl, "internal", "mixed", "gen.go")); err != nil {
		t.Fatal(err)
	}
	mixedRow := findRow(t, rows, "example.com/fx/internal/mixed")
	if got, want := mixedRow.statements, controlStatements(t, mixedControl, "internal/mixed"); got != want {
		t.Errorf("mixed statements = %d, control without gen.go = %d", got, want)
	}

	// A *.pb.go without the header and a near-miss header are counted.
	for _, pkg := range []struct {
		name string
		path string
	}{
		{"pbonly", "internal/pbonly"},
		{"nearonly", "internal/nearonly"},
	} {
		row := findRow(t, rows, "example.com/fx/"+pkg.path)
		if got, want := row.statements, controlStatements(t, copyModule(t), pkg.path); got != want {
			t.Errorf("%s statements = %d, control = %d", pkg.name, got, want)
		}
	}

	// A file present only under peers_testenv counts, though it is not in the
	// untagged linux file list.
	taggedRow := findRow(t, rows, "example.com/fx/internal/tagged")
	untagged := controlStatements(t, copyModule(t), "internal/tagged")
	tagged := controlStatements(t, copyModule(t), "internal/tagged", "peers_testenv")
	if tagged <= untagged {
		t.Fatalf("fixture bug: tagged control %d is not above untagged %d", tagged, untagged)
	}
	if taggedRow.statements != tagged {
		t.Errorf("tagged statements = %d, want tagged control %d", taggedRow.statements, tagged)
	}

	rep := loadReport(t, fx.args()...)
	for _, b := range rep.blocks {
		switch {
		case strings.HasSuffix(b.key.file, "/gen.go"):
			t.Errorf("generated block in profile: %s", b.key.file)
		case strings.Contains(b.key.file, "/internal/testkit/"),
			strings.Contains(b.key.file, "/scripts/"),
			strings.Contains(b.key.file, "/test/"):
			t.Errorf("excluded block in profile: %s", b.key.file)
		}
	}
	found := map[string]bool{}
	for _, b := range rep.blocks {
		found[b.key.file] = true
	}
	for _, want := range []string{
		"example.com/fx/internal/pbonly/api.pb.go",
		"example.com/fx/internal/nearonly/near.go",
	} {
		if !found[want] {
			t.Errorf("block for %s missing from merged profile", want)
		}
	}

	// -o produces a profile the toolchain accepts.
	profile := filepath.Join(t.TempDir(), "coverage.txt")
	if code, _, stderr := runTool(t, fx.args("-o", profile)...); code != 0 {
		t.Fatalf("-o run: exit %d, stderr:\n%s", code, stderr)
	}
	cmd := exec.Command("go", "tool", "cover", "-func="+profile)
	cmd.Dir = fx.root
	cmd.Env = append(os.Environ(), "GOWORK=off")
	coverOut, err := cmd.CombinedOutput()
	if err != nil {
		t.Fatalf("go tool cover -func: %v\n%s", err, coverOut)
	}
	if !strings.Contains(string(coverOut), "total:") {
		t.Errorf("go tool cover -func output lacks a total line:\n%s", coverOut)
	}
}

func TestReportMarkdown(t *testing.T) {
	fx := buildFixture(t)
	code, out, stderr := runTool(t, fx.args("-format", "markdown")...)
	if code != 0 {
		t.Fatalf("exit %d, stderr:\n%s", code, stderr)
	}
	lines := strings.Split(strings.TrimRight(out, "\n"), "\n")
	if len(lines) < 3 {
		t.Fatalf("markdown output too short:\n%s", out)
	}
	if want := "| package | statements | covered | percent | status |"; lines[0] != want {
		t.Errorf("header = %q, want %q", lines[0], want)
	}
	if want := "| --- | --- | --- | --- | --- |"; lines[1] != want {
		t.Errorf("separator = %q, want %q", lines[1], want)
	}
	cells := func(line string) []string {
		parts := strings.Split(strings.Trim(line, "|"), "|")
		for i := range parts {
			parts[i] = strings.TrimSpace(parts[i])
		}
		return parts
	}
	rowRE := regexp.MustCompile("^\\| `[^`]+` \\| [0-9]+ \\| [0-9]+ \\| [0-9]+\\.[0-9] \\| (-|ok|FAIL) \\|$")
	for _, line := range lines[2 : len(lines)-1] {
		if !rowRE.MatchString(line) {
			t.Errorf("row does not match markdown shape: %q", line)
		}
	}
	totalRE := regexp.MustCompile(`^\| \*\*total\*\* \| [0-9]+ \| [0-9]+ \| [0-9]+\.[0-9] \| (-|ok|FAIL) \|$`)
	if last := lines[len(lines)-1]; !totalRE.MatchString(last) {
		t.Errorf("total line does not match markdown shape: %q", last)
	}
	rep := loadReport(t, fx.args()...)
	if got := cells(lines[len(lines)-1]); got[1] != strconv.Itoa(rep.total.statements) || got[2] != strconv.Itoa(rep.total.covered) {
		t.Errorf("total cells = %v, want statements %d covered %d", got, rep.total.statements, rep.total.covered)
	}
}

func TestThresholdComparison(t *testing.T) {
	cases := []struct {
		covered, statements int
		scaled              int
		percent             string
		pass                bool
	}{
		{8000, 10000, 800, "80.0", true},
		{7996, 10000, 800, "79.9", false},
		{10000, 10000, 1000, "100.0", true},
		{2, 3, 666, "66.6", true},
		{2, 3, 667, "66.6", false},
		{0, 0, 800, "0.0", true},
	}
	for _, tc := range cases {
		if got := percentString(tc.covered, tc.statements); got != tc.percent {
			t.Errorf("percentString(%d, %d) = %q, want %q", tc.covered, tc.statements, got, tc.percent)
		}
		if got := meetsThreshold(tc.covered, tc.statements, tc.scaled); got != tc.pass {
			t.Errorf("meetsThreshold(%d, %d, %d) = %v, want %v", tc.covered, tc.statements, tc.scaled, got, tc.pass)
		}
	}

	floor := 800
	total := 800
	rep := &report{
		packages: []packageCoverage{{path: "example.com/p", statements: 10000, covered: 8000}},
		total:    packageCoverage{statements: 10000, covered: 8000},
	}
	if got := packageStatus(rep.packages[0], nil); got != "-" {
		t.Errorf("packageStatus without a floor = %q, want -", got)
	}
	if got := totalStatus(rep, nil); got != "-" {
		t.Errorf("totalStatus without a floor = %q, want -", got)
	}
	if got := packageStatus(rep.packages[0], &floor); got != "ok" {
		t.Errorf("packageStatus at exact equality = %q, want ok", got)
	}
	if got := totalStatus(rep, &total); got != "ok" {
		t.Errorf("totalStatus at exact equality = %q, want ok", got)
	}
	if belowThreshold(rep, &config{pkg: &floor, total: &total}) {
		t.Error("belowThreshold at exact equality = true, want false")
	}
	rep.packages[0].covered = 7996
	rep.total.covered = 7996
	if got := packageStatus(rep.packages[0], &floor); got != "FAIL" {
		t.Errorf("packageStatus below floor = %q, want FAIL", got)
	}
	if got := totalStatus(rep, &total); got != "FAIL" {
		t.Errorf("totalStatus below floor = %q, want FAIL", got)
	}
	if !belowThreshold(rep, &config{pkg: &floor, total: &total}) {
		t.Error("belowThreshold below floor = false, want true")
	}
	if belowThreshold(rep, &config{}) {
		t.Error("belowThreshold without thresholds = true, want false")
	}
}

func TestParsePercent(t *testing.T) {
	valid := map[string]int{
		"0": 0, "0.0": 0, "80": 800, "80.5": 805, "100": 1000, "100.0": 1000,
	}
	for in, want := range valid {
		got, err := parsePercent(in)
		if err != nil {
			t.Errorf("parsePercent(%q): %v", in, err)
		} else if got != want {
			t.Errorf("parsePercent(%q) = %d, want %d", in, got, want)
		}
	}
	for _, in := range []string{"90.05", "101", "100.1", "-1", "", "abc", ".5", "50.", "1.23"} {
		if got, err := parsePercent(in); err == nil {
			t.Errorf("parsePercent(%q) = %d, want error", in, got)
		}
	}
}

func TestThresholdExitCodes(t *testing.T) {
	fx := buildFixture(t)

	code, out, stderr := runTool(t, fx.args()...)
	if code != 0 {
		t.Fatalf("no thresholds: exit %d, stderr:\n%s", code, stderr)
	}
	if _, total := parseTextReport(t, out); total.path != "total" {
		t.Fatal("missing total row")
	}

	code, out, stderr = runTool(t, fx.args("-total", "100")...)
	if code != 1 {
		t.Fatalf("-total 100: exit %d, want 1, stderr:\n%s", code, stderr)
	}
	if _, total := parseTextReport(t, out); total.status != "FAIL" {
		t.Errorf("-total 100: total status = %q, want FAIL", total.status)
	}

	code, out, _ = runTool(t, fx.args("-package", "100")...)
	if code != 1 {
		t.Fatalf("-package 100: exit %d, want 1", code)
	}
	failing := 0
	rows, _ := parseTextReport(t, out)
	for _, r := range rows {
		if r.status == "FAIL" {
			failing++
		}
	}
	if failing == 0 {
		t.Error("-package 100 marked no row FAIL")
	}

	code, _, _ = runTool(t, fx.args("-total", "0", "-package", "0")...)
	if code != 0 {
		t.Errorf("-total 0 -package 0: exit %d, want 0", code)
	}
}

func TestStatusShownOnlyForGivenThresholds(t *testing.T) {
	fx := buildFixture(t)
	check := func(t *testing.T, what, got string, shown bool) {
		t.Helper()
		if shown {
			if got != "ok" && got != "FAIL" {
				t.Errorf("%s status = %q, want ok or FAIL", what, got)
			}
			return
		}
		if got != "-" {
			t.Errorf("%s status = %q, want -", what, got)
		}
	}
	cases := []struct {
		name       string
		args       []string
		pkgShown   bool
		totalShown bool
	}{
		{"no thresholds", nil, false, false},
		{"package only", []string{"-package", "80"}, true, false},
		{"total only", []string{"-total", "90"}, false, true},
		{"both", []string{"-package", "80", "-total", "90"}, true, true},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			code, out, stderr := runTool(t, fx.args(tc.args...)...)
			if code == 2 {
				t.Fatalf("exit 2, stderr:\n%s", stderr)
			}
			rows, total := parseTextReport(t, out)
			if len(rows) == 0 {
				t.Fatal("no package rows")
			}
			for _, r := range rows {
				check(t, "package "+r.path, r.status, tc.pkgShown)
			}
			check(t, "total", total.status, tc.totalShown)
		})
	}
}

func TestHelpStatesStatusRule(t *testing.T) {
	code, out, stderr := runTool(t, "-h")
	if code != 2 {
		t.Fatalf("-h: exit %d, want 2", code)
	}
	if out != "" {
		t.Errorf("-h wrote to stdout: %q", out)
	}
	for _, want := range []string{
		"usage: coverreport",
		"status column shows ok or FAIL only for a threshold that is given",
		"-package sets package rows",
		"-total sets the total row",
		"-format FORMAT",
	} {
		if !strings.Contains(stderr, want) {
			t.Errorf("usage text lacks %q:\n%s", want, stderr)
		}
	}
}

func TestInvalidThresholds(t *testing.T) {
	fx := buildFixture(t)
	for _, tc := range []struct {
		flag  string
		value string
	}{
		{"-total", "90.05"},
		{"-total", "101"},
		{"-total", ""},
		{"-package", "80.55"},
		{"-package", "-1"},
		{"-package", ""},
	} {
		code, out, stderr := runTool(t, fx.args(tc.flag, tc.value)...)
		if code != 2 {
			t.Errorf("%s %s: exit %d, want 2", tc.flag, tc.value, code)
		}
		if out != "" {
			t.Errorf("%s %s printed a table", tc.flag, tc.value)
		}
		if !strings.Contains(stderr, tc.flag) {
			t.Errorf("%s %s: stderr %q does not name the flag", tc.flag, tc.value, stderr)
		}
	}
	code, _, stderr := runTool(t)
	if code != 2 || !strings.Contains(stderr, "-i") {
		t.Errorf("missing -i: exit %d, stderr %q", code, stderr)
	}
}

// TestZeroTotalStatementsFailsClosed checks that a run whose merged data
// counts no statements exits 2. The total would otherwise satisfy every
// threshold at 0 >= 0 and report a success the data does not support.
func TestZeroTotalStatementsFailsClosed(t *testing.T) {
	root := t.TempDir()
	write := func(rel, content string) {
		t.Helper()
		full := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(full), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("go.mod", "module example.com/zero\n\ngo 1.27\n")
	write("internal/zero/zero.go", "package zero\n\ntype T struct{}\n\nconst K = 1\n")
	write("cmd/peers/main.go", "package main\n\nfunc main() {}\n")

	work := t.TempDir()
	bin := filepath.Join(work, "peers")
	runGo(t, root, "build", "-cover", "-covermode=atomic", "-coverpkg=example.com/zero/...", "-o", bin, "./cmd/peers")
	coverDir := filepath.Join(work, "cover")
	if err := os.MkdirAll(coverDir, 0o755); err != nil {
		t.Fatal(err)
	}
	runBinary(t, root, bin, coverDir)

	code, out, stderr := runTool(t, "-i", coverDir, "-root", root, "-total", "0")
	if code != 2 {
		t.Fatalf("exit %d, want 2\nstdout:\n%s\nstderr:\n%s", code, out, stderr)
	}
	if out != "" {
		t.Errorf("printed a table with no statements:\n%s", out)
	}
	if !strings.Contains(stderr, "no statements") {
		t.Errorf("stderr %q does not explain the zero denominator", stderr)
	}
}

func TestInputDirectoryErrors(t *testing.T) {
	fx := buildFixture(t)
	valid := fx.inputs[0]

	entries, err := os.ReadDir(valid)
	if err != nil {
		t.Fatal(err)
	}
	var metaFile, counterFile string
	for _, e := range entries {
		switch {
		case strings.HasPrefix(e.Name(), "covmeta."):
			metaFile = e.Name()
		case strings.HasPrefix(e.Name(), "covcounters."):
			counterFile = e.Name()
		}
	}
	if metaFile == "" || counterFile == "" {
		t.Fatalf("fixture has no covdata files: %v", entries)
	}
	copyFile := func(dst, name string) {
		t.Helper()
		data, err := os.ReadFile(filepath.Join(valid, name))
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dst, name), data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	t.Run("missing", func(t *testing.T) {
		code, _, stderr := runTool(t, "-i", filepath.Join(t.TempDir(), "nope"), "-root", fx.root)
		if code != 2 || !strings.Contains(stderr, "nope") {
			t.Fatalf("exit %d, stderr %q", code, stderr)
		}
	})
	t.Run("empty", func(t *testing.T) {
		code, _, stderr := runTool(t, "-i", t.TempDir(), "-root", fx.root)
		if code != 2 || !strings.Contains(stderr, "no covmeta") {
			t.Fatalf("exit %d, stderr %q", code, stderr)
		}
	})
	t.Run("no counters", func(t *testing.T) {
		dir := t.TempDir()
		copyFile(dir, metaFile)
		code, _, stderr := runTool(t, "-i", dir, "-root", fx.root)
		if code != 2 || !strings.Contains(stderr, "no covcounters") {
			t.Fatalf("exit %d, stderr %q", code, stderr)
		}
	})
	t.Run("no meta", func(t *testing.T) {
		dir := t.TempDir()
		copyFile(dir, counterFile)
		code, _, stderr := runTool(t, "-i", dir, "-root", fx.root)
		if code != 2 || !strings.Contains(stderr, "no covmeta") {
			t.Fatalf("exit %d, stderr %q", code, stderr)
		}
	})
	t.Run("unreadable directory", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can read a directory whose mode forbids it")
		}
		dir := t.TempDir()
		copyFile(dir, metaFile)
		copyFile(dir, counterFile)
		if err := os.Chmod(dir, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o755) })
		code, _, stderr := runTool(t, "-i", dir, "-root", fx.root)
		if code != 2 || !strings.Contains(stderr, "permission denied") {
			t.Fatalf("exit %d, stderr %q", code, stderr)
		}
	})
	t.Run("unreadable file", func(t *testing.T) {
		if os.Geteuid() == 0 {
			t.Skip("root can read a file whose mode forbids it")
		}
		dir := t.TempDir()
		copyFile(dir, metaFile)
		copyFile(dir, counterFile)
		target := filepath.Join(dir, counterFile)
		if err := os.Chmod(target, 0); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(target, 0o644) })
		code, _, stderr := runTool(t, "-i", dir, "-root", fx.root)
		if code != 2 || !strings.Contains(stderr, "unreadable file") {
			t.Fatalf("exit %d, stderr %q", code, stderr)
		}
	})
}

func TestCovdataModeClash(t *testing.T) {
	fx := buildFixture(t)
	work := t.TempDir()
	bin := filepath.Join(work, "peers-set")
	runGo(t, fx.root, "build", "-cover", "-covermode=set", "-coverpkg=example.com/fx/...", "-o", bin, "./cmd/peers")
	setDir := filepath.Join(work, "data")
	if err := os.MkdirAll(setDir, 0o755); err != nil {
		t.Fatal(err)
	}
	runBinary(t, fx.root, bin, setDir)

	code, out, stderr := runTool(t, "-i", fx.inputs[0]+","+setDir, "-root", fx.root)
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if out != "" {
		t.Errorf("printed a table on a covdata error:\n%s", out)
	}
	if !strings.Contains(stderr, "counter mode clash") || !strings.Contains(stderr, "previous file had") {
		t.Errorf("stderr does not quote the covdata clash message: %q", stderr)
	}
}

const unlinkedSource = `package unlinked

func Walk(n int) int {
	x := 0
	{
		x = 1
	}
loop:
	x++
	if x < n {
		goto loop
	}
	f := func(v int) int {
		y := v
		y *= 2
		return y
	}
	x = f(x)
	defer func() {}()
	ch := make(chan int, 1)
	ch <- 1
	select {
	case v := <-ch:
		x += v
	default:
		panic("empty")
	}
	if x < 0 {
		x = 0
	} else if x == 0 {
		x = 1
	} else {
		x = 2
	}
	switch x {
	case 0:
		x++
	case 1:
		x--
	default:
		x = 3
	}
	var v interface{} = x
	switch w := v.(type) {
	case int:
		x += w
	case string:
		x += len(w)
	default:
		_ = w
	}
	for _, v := range func() []int { return []int{1, 2} }() {
		x += v
	}
outer:
	for i := 0; i < 2; i++ {
		for j := 0; j < 2; j++ {
			if i == j {
				continue outer
			}
			x += i + j
		}
	}
	return x
}
`

func TestAbsentPackageFailsClosed(t *testing.T) {
	fx := buildFixture(t)
	dir := filepath.Join(fx.root, "internal", "unlinked")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "unlinked.go"), []byte(unlinkedSource), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "unlinked_test.go"), []byte("package unlinked\n\nimport \"testing\"\n\nfunc TestWalk(t *testing.T) { _ = Walk(1) }\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	control := controlStatements(t, fx.root, "internal/unlinked")
	if control == 0 {
		t.Fatal("fixture bug: unlinked package has no control statements")
	}

	code, out, stderr := runTool(t, fx.args()...)
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if out != "" {
		t.Errorf("printed a table on a missing-data error:\n%s", out)
	}
	if !strings.Contains(stderr, "example.com/fx/internal/unlinked") {
		t.Errorf("stderr does not name the package: %q", stderr)
	}
	if want := fmt.Sprintf("%d statements", control); !strings.Contains(stderr, want) {
		t.Errorf("stderr %q does not carry the counted total %q", stderr, want)
	}
}

func TestBrokenSourceFailsClosed(t *testing.T) {
	fx := buildFixture(t)
	dir := filepath.Join(fx.root, "internal", "broken")
	if err := os.MkdirAll(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "broken.go"), []byte("package broken\n\nfunc {\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	code, out, stderr := runTool(t, fx.args()...)
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if out != "" {
		t.Errorf("printed a table on a parse error:\n%s", out)
	}
	if !strings.Contains(stderr, "broken.go") || !strings.Contains(stderr, "parse") {
		t.Errorf("stderr does not name the unparseable source: %q", stderr)
	}
}

func TestMissingProfileFileFailsClosed(t *testing.T) {
	fx := buildFixture(t)
	if err := os.Remove(filepath.Join(fx.root, "cmd", "peers", "extra.go")); err != nil {
		t.Fatal(err)
	}
	code, out, stderr := runTool(t, fx.args()...)
	if code != 2 {
		t.Fatalf("exit %d, want 2", code)
	}
	if out != "" {
		t.Errorf("printed a table when a profile file is missing:\n%s", out)
	}
	if !strings.Contains(stderr, "extra.go") || !strings.Contains(stderr, "not found on disk") {
		t.Errorf("stderr does not name the missing file: %q", stderr)
	}
}

func TestMergeBlocks(t *testing.T) {
	key := blockKey{file: "m/internal/a/a.go", startLine: 1, startCol: 1, endLine: 2, endCol: 2}
	merged, err := mergeBlocks([]block{
		{key: key, pkg: "m/internal/a", numStmt: 2, count: 1},
		{key: key, pkg: "m/internal/a", numStmt: 2, count: 0},
	})
	if err != nil {
		t.Fatalf("mergeBlocks: %v", err)
	}
	if len(merged) != 1 {
		t.Fatalf("duplicate blocks not merged: %d blocks", len(merged))
	}
	if merged[0].numStmt != 2 || merged[0].count != 1 {
		t.Errorf("merged block = %+v, want numStmt 2 count 1", merged[0])
	}

	merged, err = mergeBlocks([]block{
		{key: key, numStmt: 2, count: 5},
		{key: key, numStmt: 2, count: 3},
	})
	if err != nil {
		t.Fatalf("mergeBlocks: %v", err)
	}
	if len(merged) != 1 || merged[0].count != 5 {
		t.Errorf("merged = %+v, want one block counting 5", merged)
	}

	if _, err := mergeBlocks([]block{
		{key: key, numStmt: 2},
		{key: key, numStmt: 3},
	}); err == nil || !strings.Contains(err.Error(), "disagree on statement count") {
		t.Errorf("conflicting statement counts: err = %v, want a disagreement error", err)
	}
}

func TestParseProfileRejectsMalformedLines(t *testing.T) {
	if _, err := parseProfile(strings.NewReader("mode: atomic\nnot a profile line\n")); err == nil {
		t.Fatal("parseProfile accepted a malformed line")
	}
	blocks, err := parseProfile(strings.NewReader("mode: atomic\nm/internal/a/a.go:1.2,3.4 2 1\n"))
	if err != nil {
		t.Fatalf("parseProfile: %v", err)
	}
	if len(blocks) != 1 || blocks[0].numStmt != 2 || blocks[0].count != 1 {
		t.Fatalf("blocks = %+v", blocks)
	}
}
