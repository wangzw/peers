package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"slices"
	"strings"
	"testing"
)

// stream encodes the events as the JSON lines `go test -json` writes.
func stream(t *testing.T, events ...event) string {
	t.Helper()
	var b strings.Builder
	enc := json.NewEncoder(&b)
	for _, e := range events {
		if err := enc.Encode(e); err != nil {
			t.Fatalf("encoding event: %v", err)
		}
	}
	return b.String()
}

// raw joins JSON lines written as literals, the shape go test -json emits.
// The build-event tests use it instead of encoding an event struct, so a wrong
// JSON tag cannot make the encoder and the decoder agree on the same mistake.
func raw(lines ...string) string {
	return strings.Join(lines, "\n") + "\n"
}

func run(t *testing.T, input string) string {
	t.Helper()
	return runWithProgress(t, input, false)
}

func runWithProgress(t *testing.T, input string, progress bool) string {
	t.Helper()
	var out bytes.Buffer
	r := newReporter(&out, "test", progress)
	if err := r.read(strings.NewReader(input)); err != nil {
		t.Fatalf("read: %v", err)
	}
	r.summary()
	return out.String()
}

func TestReportListsSkippedTestsWithReasons(t *testing.T) {
	input := stream(t,
		event{Action: "run", Package: "p", Test: "TestPass"},
		event{Action: "output", Package: "p", Test: "TestPass", Output: "=== RUN   TestPass\n"},
		event{Action: "pass", Package: "p", Test: "TestPass"},
		event{Action: "run", Package: "p", Test: "TestSkip"},
		event{Action: "output", Package: "p", Test: "TestSkip", Output: "=== RUN   TestSkip\n"},
		event{Action: "output", Package: "p", Test: "TestSkip", Output: "    skip_test.go:12: no account is available\n"},
		event{Action: "output", Package: "p", Test: "TestSkip", Output: "--- SKIP: TestSkip (0.00s)\n"},
		event{Action: "skip", Package: "p", Test: "TestSkip"},
		event{Action: "run", Package: "p", Test: "TestSkip/child"},
		event{Action: "output", Package: "p", Test: "TestSkip/child", Output: "    skip_test.go:20: child reason\n"},
		event{Action: "output", Package: "p", Test: "TestSkip/child", Output: "--- SKIP: TestSkip/child (0.00s)\n"},
		event{Action: "skip", Package: "p", Test: "TestSkip/child"},
		event{Action: "fail", Package: "p", Test: "TestFail"},
		event{Action: "fail", Package: "p"},
	)

	out := run(t, input)
	for _, want := range []string{
		"test: skipped p TestSkip: skip_test.go:12: no account is available",
		"test: skipped p TestSkip/child: skip_test.go:20: child reason",
		"test: p failed (0.00s)",
		"test: 1 passed, 1 failed, 2 skipped, 1 packages failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report does not contain %q:\n%s", want, out)
		}
	}
}

func TestReportSaysWhenNothingWasSkipped(t *testing.T) {
	input := stream(t,
		event{Action: "run", Package: "p", Test: "TestPass"},
		event{Action: "pass", Package: "p", Test: "TestPass"},
		event{Action: "pass", Package: "p"},
	)

	out := run(t, input)
	if !strings.Contains(out, "test: p passed (0.00s)") {
		t.Errorf("report does not print the finished package:\n%s", out)
	}
	if !strings.Contains(out, "test: 1 passed, 0 failed, 0 skipped, 0 packages failed") {
		t.Errorf("report has the wrong summary:\n%s", out)
	}
}

func TestReportCountsPackagesWithoutTestsApartFromSkippedTests(t *testing.T) {
	input := stream(t,
		event{Action: "skip", Package: "p"},
		event{Action: "pass", Package: "ok", Test: "TestPass"},
	)

	out := run(t, input)
	for _, want := range []string{
		"test: p has no test files",
		"test: 1 passed, 0 failed, 0 skipped, 0 packages failed, 1 packages without test files",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report does not contain %q:\n%s", want, out)
		}
	}
}

func TestReportPrintsRunningTestsWithProgress(t *testing.T) {
	input := stream(t,
		event{Action: "run", Package: "p", Test: "TestSlow"},
		event{Action: "pass", Package: "p", Test: "TestSlow"},
		event{Action: "pass", Package: "p"},
	)

	quiet := run(t, input)
	if strings.Contains(quiet, "test: running") {
		t.Errorf("report prints running tests without -progress:\n%s", quiet)
	}

	out := runWithProgress(t, input, true)
	if !strings.Contains(out, "test: running p TestSlow") {
		t.Errorf("report does not print a running test with -progress:\n%s", out)
	}
}

func TestReportReprintsFailedTestOutput(t *testing.T) {
	input := stream(t,
		event{Action: "run", Package: "p", Test: "TestFail"},
		event{Action: "output", Package: "p", Test: "TestFail", Output: "=== RUN   TestFail\n"},
		event{Action: "output", Package: "p", Test: "TestFail", Output: "    fail_test.go:9: got 1, want 2\n"},
		event{Action: "output", Package: "p", Test: "TestFail", Output: "--- FAIL: TestFail (0.00s)\n"},
		event{Action: "fail", Package: "p", Test: "TestFail"},
		event{Action: "fail", Package: "p"},
	)

	out := run(t, input)
	for _, want := range []string{
		"test: failed p TestFail",
		"test:     fail_test.go:9: got 1, want 2",
		"test: 0 passed, 1 failed, 0 skipped, 1 packages failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report does not contain %q:\n%s", want, out)
		}
	}
}

func TestReportReprintsBuildFailureOfATestPackage(t *testing.T) {
	// These are the shapes `go help buildjson` documents: a build event names
	// ImportPath, and the test event that follows a failed build names that
	// ImportPath in FailedBuild rather than in Package.
	input := raw(
		`{"ImportPath":"p [p.test]","Action":"build-output","Output":"# p [p.test]\n"}`,
		`{"ImportPath":"p [p.test]","Action":"build-output","Output":"./x.go:3:1: syntax error\n"}`,
		`{"ImportPath":"p [p.test]","Action":"build-fail"}`,
		`{"Action":"fail","Package":"p","FailedBuild":"p [p.test]"}`,
	)

	out := run(t, input)
	for _, want := range []string{
		"test: failed package p [p.test]",
		"test: ./x.go:3:1: syntax error",
		"test: 0 passed, 0 failed, 0 skipped, 1 packages failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report does not contain %q:\n%s", want, out)
		}
	}
	if got := strings.Count(out, "./x.go:3:1: syntax error"); got != 1 {
		t.Errorf("compiler output printed %d times, want once:\n%s", got, out)
	}
	if strings.Contains(out, "test: p failed") {
		t.Errorf("report prints the test package twice:\n%s", out)
	}
}

func TestReportCountsANonTestBuildFailure(t *testing.T) {
	input := raw(
		`{"ImportPath":"p","Action":"build-output","Output":"./y.go:5:2: undefined: x\n"}`,
		`{"ImportPath":"p","Action":"build-fail"}`,
	)

	out := run(t, input)
	for _, want := range []string{
		"test: failed package p",
		"test: ./y.go:5:2: undefined: x",
		"test: 0 passed, 0 failed, 0 skipped, 1 packages failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report does not contain %q:\n%s", want, out)
		}
	}
}

func TestReportCountsEachDependentPackageWhoseBuildFailed(t *testing.T) {
	input := raw(
		`{"ImportPath":"p","Action":"build-output","Output":"./p.go:1:1: syntax error\n"}`,
		`{"ImportPath":"p","Action":"build-fail"}`,
		`{"Action":"fail","Package":"q","FailedBuild":"p"}`,
		`{"Action":"fail","Package":"r","FailedBuild":"p"}`,
	)

	out := run(t, input)
	for _, want := range []string{
		"test: failed package p",
		"test: q failed (0.00s)",
		"test: r failed (0.00s)",
		"test: 0 passed, 0 failed, 0 skipped, 3 packages failed",
	} {
		if !strings.Contains(out, want) {
			t.Errorf("report does not contain %q:\n%s", want, out)
		}
	}
}

// A cached package is a pass event with Elapsed 0 and the package-level line
// "ok  <import path>\t(cached)"; go test replays the recorded test events, so a
// gate that accepts it reports a pass the package never produced in this run.
func TestReportRejectsCachedResults(t *testing.T) {
	input := raw(
		`{"Action":"pass","Package":"p","Test":"TestPass","Elapsed":0.31}`,
		`{"Action":"output","Package":"p","Output":"ok  \tp\t(cached)\n"}`,
		`{"Action":"pass","Package":"p","Elapsed":0}`,
	)

	out := run(t, input)
	if !strings.Contains(out, "test: cached p: go test did not run this package") {
		t.Errorf("report does not name the cached package:\n%s", out)
	}
	if !strings.Contains(out, "1 package came from the test cache") {
		t.Errorf("report does not say a cached result is unacceptable:\n%s", out)
	}
}

func TestCachedResultsMakeTheReportExitNonzero(t *testing.T) {
	input := raw(
		`{"Action":"output","Package":"p","Output":"ok  \tp\t(cached)\n"}`,
		`{"Action":"output","Package":"q","Output":"ok  \tq\t0.21s\n"}`,
		`{"Action":"pass","Package":"q","Elapsed":0.21}`,
	)
	var out bytes.Buffer
	r := newReporter(&out, "test", false)
	if err := r.read(strings.NewReader(input)); err != nil {
		t.Fatalf("read: %v", err)
	}
	r.summary()
	if got := r.exitCode(); got != 1 {
		t.Errorf("exitCode() = %d, want 1 for a run with a cached package", got)
	}
}

func TestRealResultsLeaveTheExitCodeZero(t *testing.T) {
	var out bytes.Buffer
	r := newReporter(&out, "test", false)
	if err := r.read(strings.NewReader(stream(t,
		event{Action: "pass", Package: "p", Test: "TestPass", Elapsed: 0.5},
		event{Action: "output", Package: "p", Output: "ok  \tp\t0.51s\n"},
		event{Action: "pass", Package: "p", Elapsed: 0.51},
	))); err != nil {
		t.Fatalf("read: %v", err)
	}
	r.summary()
	if got := r.exitCode(); got != 0 {
		t.Errorf("exitCode() = %d, want 0 for a run that executed the package", got)
	}
}

func TestReportIgnoresATruncatedFinalLine(t *testing.T) {
	input := stream(t,
		event{Action: "run", Package: "p", Test: "TestSkip"},
		event{Action: "skip", Package: "p", Test: "TestSkip"},
	) + `{"Action":"output","Package":"p","Test":"TestSkip"`

	out := run(t, input)
	if !strings.Contains(out, "test: skipped p TestSkip: no reason given") {
		t.Errorf("report lost the skip before the truncated line:\n%s", out)
	}
}

func TestReportReadsAnOutputLineLargerThanFourMegabytes(t *testing.T) {
	long := strings.Repeat("x", 5<<20)
	input := stream(t,
		event{Action: "output", Package: "p", Test: "TestLong", Output: long + "\n"},
		event{Action: "pass", Package: "p", Test: "TestLong"},
	)

	out := run(t, input)
	if !strings.Contains(out, "test: 1 passed, 0 failed, 0 skipped, 0 packages failed") {
		t.Errorf("report did not survive a long line:\n%s", out)
	}
}

// slowestLines returns the slow-test lines of a report, in the order printed.
func slowestLines(out string) []string {
	var lines []string
	for _, line := range strings.Split(out, "\n") {
		if strings.HasPrefix(line, "test: slowest ") {
			lines = append(lines, line)
		}
	}
	return lines
}

func TestReportListsOnlyTopLevelTestsAsSlowest(t *testing.T) {
	input := stream(t,
		event{Action: "pass", Package: "p", Test: "TestSlow", Elapsed: 2.5},
		event{Action: "pass", Package: "p", Test: "TestSlow/child", Elapsed: 9},
		event{Action: "skip", Package: "p", Test: "TestSkip", Elapsed: 7},
	)

	want := []string{"test: slowest p TestSlow (2.50s)"}
	if got := slowestLines(run(t, input)); !slices.Equal(got, want) {
		t.Errorf("slowest tests = %q, want %q", got, want)
	}
}

func TestReportSortsSlowestTestsByElapsed(t *testing.T) {
	input := stream(t,
		event{Action: "pass", Package: "p", Test: "TestMedium", Elapsed: 2},
		event{Action: "pass", Package: "p", Test: "TestFast", Elapsed: 0.5},
		event{Action: "pass", Package: "p", Test: "TestSlow", Elapsed: 4},
	)

	want := []string{
		"test: slowest p TestSlow (4.00s)",
		"test: slowest p TestMedium (2.00s)",
		"test: slowest p TestFast (0.50s)",
	}
	if got := slowestLines(run(t, input)); !slices.Equal(got, want) {
		t.Errorf("slowest tests = %q, want %q", got, want)
	}
}

func TestReportCapsSlowestTestsPerPackage(t *testing.T) {
	total := slowestTestsPerPackage + 3
	events := make([]event, 0, total+1)
	for i := 0; i < total; i++ {
		events = append(events, event{
			Action:  "pass",
			Package: "p",
			Test:    fmt.Sprintf("TestSlow%02d", i),
			Elapsed: float64(i + 1),
		})
	}
	events = append(events, event{Action: "pass", Package: "q", Test: "TestOnly", Elapsed: 1})

	got := slowestLines(run(t, stream(t, events...)))
	if want := slowestTestsPerPackage + 1; len(got) != want {
		t.Fatalf("reported %d slowest tests, want %d:\n%s", len(got), want, strings.Join(got, "\n"))
	}
	if want := fmt.Sprintf("test: slowest p TestSlow%02d (%.2fs)", total-1, float64(total)); got[0] != want {
		t.Errorf("first slowest test = %q, want %q", got[0], want)
	}
	for _, i := range []int{0, 1, 2} {
		excluded := fmt.Sprintf("TestSlow%02d", i)
		if slices.ContainsFunc(got, func(line string) bool { return strings.Contains(line, excluded) }) {
			t.Errorf("test %s is beyond the per-package limit but was reported:\n%s", excluded, strings.Join(got, "\n"))
		}
	}
	if want := "test: slowest q TestOnly (1.00s)"; got[len(got)-1] != want {
		t.Errorf("last slowest test = %q, want %q", got[len(got)-1], want)
	}
}
