// Command testreport summarizes a `go test -json` stream as it arrives.
//
// A gate runs go test without -v, so the log of a passing run says nothing
// about tests that skipped themselves. This command prints each package as it
// finishes, each skipped test with the reason it logged, and the output of a
// failed test or build. -progress also prints each test when it starts, which
// is what makes an interrupted end-to-end run say where it was.
//
// The command keeps go test's exit status in every case but one: a package
// whose result came from the Go test cache makes it exit nonzero. A cached
// result is a recorded pass, not a run this gate produced, and a test whose
// inputs the cache does not track, such as a binary built in a subprocess, can
// hide a product change. Gates therefore run go test with -count=1, and this
// check catches a target that forgets it.
package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"slices"
	"strings"
	"syscall"
)

// slowestTestsPerPackage bounds how many top-level tests the report lists for
// one package after the summary.
const slowestTestsPerPackage = 15

// event is one line of the go test -json stream. A build event names
// ImportPath; a test event names Package, and FailedBuild on the fail event
// that follows a build failure. See `go help buildjson`.
type event struct {
	Action      string  `json:"Action"`
	Package     string  `json:"Package"`
	ImportPath  string  `json:"ImportPath"`
	Test        string  `json:"Test"`
	Output      string  `json:"Output"`
	Elapsed     float64 `json:"Elapsed"`
	FailedBuild string  `json:"FailedBuild"`
}

// testKey identifies one test, including a subtest, in one package.
type testKey struct {
	pkg  string
	test string
}

type reporter struct {
	out      io.Writer
	label    string
	progress bool

	passed         int
	failed         int
	skipped        int
	noTestPackages int

	failedPackages map[string]bool
	cachedPackages map[string]bool
	printedBuilds  map[string]bool
	testOutput     map[testKey][]string
	pkgOutput      map[string][]string
	buildOutput    map[string][]string
	testElapsed    map[testKey]float64
}

func newReporter(out io.Writer, label string, progress bool) *reporter {
	return &reporter{
		out:            out,
		label:          label,
		progress:       progress,
		failedPackages: make(map[string]bool),
		cachedPackages: make(map[string]bool),
		printedBuilds:  make(map[string]bool),
		testOutput:     make(map[testKey][]string),
		pkgOutput:      make(map[string][]string),
		buildOutput:    make(map[string][]string),
		testElapsed:    make(map[testKey]float64),
	}
}

func main() {
	label := flag.String("label", "test", "prefix for every report line")
	progress := flag.Bool("progress", false, "print each test when it starts")
	flag.Parse()
	if flag.NArg() != 1 {
		fmt.Fprintf(os.Stderr, "usage: testreport [-label name] [-progress] <go-test-json-file|->\n")
		os.Exit(2)
	}

	var rd io.Reader
	source := flag.Arg(0)
	if source == "-" {
		rd = os.Stdin
	} else {
		f, err := os.Open(source)
		if err != nil {
			fmt.Fprintf(os.Stderr, "testreport: %v\n", err)
			os.Exit(2)
		}
		defer func() { _ = f.Close() }()
		rd = f
	}

	// The gate can kill go test while this command still reads. The producer
	// receives the same signal, so this command ignores it instead of exiting:
	// it keeps reading until the pipe closes and then prints the summary of
	// what it saw. Notifying the channel replaces the default action, and the
	// goroutine drains it so a second signal cannot fall back to that default.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	go func() {
		for range signals {
		}
	}()

	r := newReporter(os.Stdout, *label, *progress)
	err := r.read(rd)
	r.summary()
	if err != nil {
		fmt.Fprintf(os.Stderr, "testreport: reading %s: %v\n", source, err)
		os.Exit(2)
	}
	if code := r.exitCode(); code != 0 {
		os.Exit(code)
	}
}

// read consumes one JSON event per line until the stream ends. ReadString has
// no line length limit, so an unusually long output line is not dropped.
func (r *reporter) read(rd io.Reader) error {
	br := bufio.NewReader(rd)
	for {
		line, err := br.ReadString('\n')
		if line != "" {
			r.consume(line)
		}
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
	}
}

func (r *reporter) consume(line string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return
	}
	var e event
	if err := json.Unmarshal([]byte(line), &e); err != nil {
		// A killed go test can leave a truncated final line; a non-JSON line
		// on the stream is equally uninformative.
		return
	}
	r.handle(e)
}

func (r *reporter) handle(e event) {
	switch e.Action {
	case "build-output":
		r.buildOutput[e.ImportPath] = append(r.buildOutput[e.ImportPath], e.Output)

	case "build-fail":
		r.failBuild(e.ImportPath)

	case "output":
		switch {
		case e.Test != "":
			k := testKey{e.Package, e.Test}
			r.testOutput[k] = append(r.testOutput[k], e.Output)
		case e.Package != "":
			r.pkgOutput[e.Package] = append(r.pkgOutput[e.Package], e.Output)
			if isCachedResult(e.Output) {
				r.cachedPackages[e.Package] = true
				r.printf("cached %s: go test did not run this package\n", e.Package)
			}
		}

	case "run":
		if r.progress && e.Test != "" {
			r.printf("running %s %s\n", e.Package, e.Test)
		}

	case "pass":
		if e.Test != "" {
			r.passed++
			r.recordElapsed(e)
		} else if e.Package != "" {
			if r.cachedPackages[e.Package] {
				r.printf("%s cached (%.2fs)\n", e.Package, e.Elapsed)
			} else {
				r.printf("%s passed (%.2fs)\n", e.Package, e.Elapsed)
			}
		}

	case "skip":
		if e.Test != "" {
			r.skipped++
			r.printf("skipped %s %s: %s\n", e.Package, e.Test, r.reason(testKey{e.Package, e.Test}))
		} else if e.Package != "" {
			r.noTestPackages++
			r.printf("%s has no test files\n", e.Package)
		}

	case "fail":
		r.failEvent(e)
	}
}

func (r *reporter) failEvent(e event) {
	if e.Test != "" {
		r.failed++
		r.recordElapsed(e)
		r.printf("failed %s %s\n", e.Package, e.Test)
		r.printTestOutput(testKey{e.Package, e.Test})
		return
	}
	if e.Package == "" {
		return
	}
	if e.FailedBuild != "" {
		r.failBuild(e.FailedBuild)
		// A dependent test package fails with FailedBuild naming the package
		// that did not compile, so it is a failed package of its own. When the
		// failing package is the build target itself, failBuild already
		// printed and counted it.
		if !samePackage(e.Package, e.FailedBuild) {
			r.failedPackages[e.Package] = true
			r.printf("%s failed (%.2fs)\n", e.Package, e.Elapsed)
		}
		return
	}
	r.failedPackages[e.Package] = true
	r.printf("%s failed (%.2fs)\n", e.Package, e.Elapsed)
	r.printLines(r.pkgOutput[e.Package])
}

// failBuild prints a build's compiler output once, when the build fails. The
// fail event that follows names the same ImportPath in FailedBuild, so the
// output is not repeated there.
func (r *reporter) failBuild(importPath string) {
	if importPath == "" || r.printedBuilds[importPath] {
		return
	}
	r.printedBuilds[importPath] = true
	r.failedPackages[importPath] = true
	r.printf("failed package %s\n", importPath)
	r.printLines(r.buildOutput[importPath])
}

func (r *reporter) printTestOutput(k testKey) {
	for _, line := range r.testOutput[k] {
		if strings.HasPrefix(strings.TrimSpace(line), "===") {
			continue
		}
		r.printf("%s", line)
	}
}

func (r *reporter) printLines(lines []string) {
	for _, line := range lines {
		r.printf("%s", line)
	}
}

func (r *reporter) summary() {
	summary := fmt.Sprintf("%d passed, %d failed, %d skipped, %d packages failed",
		r.passed, r.failed, r.skipped, len(r.failedPackages))
	if r.noTestPackages > 0 {
		summary += fmt.Sprintf(", %d packages without test files", r.noTestPackages)
	}
	r.printf("%s\n", summary)
	r.printSlowestTests()
	r.printCached()
}

// printCached names every package whose result came from the test cache. The
// packages are printed in name order so the report is comparable between runs.
func (r *reporter) printCached() {
	if len(r.cachedPackages) == 0 {
		return
	}
	pkgs := make([]string, 0, len(r.cachedPackages))
	for pkg := range r.cachedPackages {
		pkgs = append(pkgs, pkg)
	}
	slices.Sort(pkgs)
	noun := "packages"
	if len(pkgs) == 1 {
		noun = "package"
	}
	r.printf("%d %s came from the test cache: %s\n", len(pkgs), noun, strings.Join(pkgs, ", "))
}

// exitCode is nonzero when a cached result was seen. The caller exits with it
// so a pipeline that keeps go test's status still fails the gate.
func (r *reporter) exitCode() int {
	if len(r.cachedPackages) > 0 {
		return 1
	}
	return 0
}

// isCachedResult reports whether a package-level output line is the "ok
// <import path>\t(cached)" line go test writes for a cached result. Output a
// test prints is a test event and never reaches this check, but a TestMain's
// output before and after m.Run is package-level output and does, so a
// TestMain that prints this exact line still matches. The tab keeps a package
// name that ends in "(cached)" from matching.
func isCachedResult(output string) bool {
	return strings.Contains(output, "\t(cached)")
}

// recordElapsed remembers a test's duration so the report can rank it.
// Subtests are recorded too and filtered out when the list is built, because a
// parent's elapsed time already includes its children.
func (r *reporter) recordElapsed(e event) {
	r.testElapsed[testKey{e.Package, e.Test}] = e.Elapsed
}

// printSlowestTests lists, for each package, the top-level tests that took the
// longest. Only pass and fail events record a duration, so a skipped test never
// appears. Packages are printed in name order and tests from slowest to
// fastest, which keeps the report comparable between runs.
func (r *reporter) printSlowestTests() {
	byPackage := make(map[string][]testKey)
	for k := range r.testElapsed {
		if strings.Contains(k.test, "/") {
			continue
		}
		byPackage[k.pkg] = append(byPackage[k.pkg], k)
	}
	pkgs := make([]string, 0, len(byPackage))
	for pkg := range byPackage {
		pkgs = append(pkgs, pkg)
	}
	slices.Sort(pkgs)
	for _, pkg := range pkgs {
		tests := byPackage[pkg]
		slices.SortFunc(tests, func(a, b testKey) int {
			if r.testElapsed[a] != r.testElapsed[b] {
				if r.testElapsed[a] > r.testElapsed[b] {
					return -1
				}
				return 1
			}
			return strings.Compare(a.test, b.test)
		})
		if len(tests) > slowestTestsPerPackage {
			tests = tests[:slowestTestsPerPackage]
		}
		for _, k := range tests {
			r.printf("slowest %s %s (%.2fs)\n", k.pkg, k.test, r.testElapsed[k])
		}
	}
}

// reason returns the last line a skipped test logged before Go reported the
// skip. That is the message passed to t.Skip, including its file:line prefix.
func (r *reporter) reason(k testKey) string {
	reason := ""
	for _, line := range r.testOutput[k] {
		trimmed := strings.TrimSpace(line)
		if strings.HasPrefix(trimmed, "--- SKIP:") {
			break
		}
		if trimmed == "" || strings.HasPrefix(trimmed, "===") {
			continue
		}
		reason = trimmed
	}
	if reason == "" {
		return "no reason given"
	}
	return reason
}

// samePackage reports whether a test event's Package and a build event's
// ImportPath name the same package. A test binary's ImportPath is
// "<pkg> [<pkg>.test]"; a dependency that failed to build has its own path.
func samePackage(pkg, importPath string) bool {
	return pkg == importPath || strings.HasPrefix(importPath, pkg+" [")
}

func (r *reporter) printf(format string, args ...any) {
	_, _ = fmt.Fprintf(r.out, r.label+": "+format, args...)
}
