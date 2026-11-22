// Command e2e-shards reads the shard table and prints one shard's packages or
// -run regex, or checks that the table covers every package and test exactly
// once. The table is described in e2e-shards.txt.
package main

import (
	_ "embed"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"regexp"
	"sort"
	"strings"
)

//go:embed e2e-shards.txt
var tableText string

type shard struct {
	name     string
	packages []string
	tests    []string
}

func main() {
	name := flag.String("shard", "", "shard name")
	print := flag.String("print", "", "print the shard's packages or -run regex")
	check := flag.Bool("check", false, "check that the table covers every package and test exactly once")
	flag.Parse()

	shards, err := parseTable(tableText)
	if err != nil {
		fail(err)
	}

	switch {
	case *check:
		if err := runCheck(shards); err != nil {
			fail(err)
		}
	case *print != "":
		s, err := findShard(shards, *name)
		if err != nil {
			fail(err)
		}
		switch *print {
		case "packages":
			_, _ = fmt.Println(strings.Join(s.packages, " "))
		case "run":
			l := realLister{}
			expanded, err := expandShards(shards, l.packages)
			if err != nil {
				fail(err)
			}
			s, err := findShard(expanded, *name)
			if err != nil {
				fail(err)
			}
			names, err := runNamesFor(s, packageOwners(expanded), l.tests)
			if err != nil {
				fail(err)
			}
			_, _ = fmt.Println(runRegex(names))
		default:
			fail(fmt.Errorf("-print must be packages or run, not %q", *print))
		}
	default:
		fail(fmt.Errorf("one of -check or -print is required"))
	}
}

func fail(err error) {
	for _, line := range strings.Split(err.Error(), "\n") {
		_, _ = fmt.Fprintln(os.Stderr, "e2e-shards: "+line)
	}
	os.Exit(1)
}

// parseTable parses the shard table. Each non-comment line is
// "shard <name> <package>... [-- <test>...]".
func parseTable(text string) ([]shard, error) {
	var shards []shard
	seenShard := map[string]bool{}
	seenTest := map[string]string{}
	for i, line := range strings.Split(text, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 3 || fields[0] != "shard" {
			return nil, fmt.Errorf("line %d: want \"shard <name> <package>... [-- <test>...]\", got %q", i+1, line)
		}
		name := fields[1]
		if seenShard[name] {
			return nil, fmt.Errorf("line %d: shard %s is defined twice", i+1, name)
		}
		seenShard[name] = true

		rest := fields[2:]
		dash := -1
		for j, f := range rest {
			if f == "--" {
				dash = j
				break
			}
		}
		var packages, tests []string
		if dash >= 0 {
			packages, tests = rest[:dash], rest[dash+1:]
		} else {
			packages = rest
		}
		if len(packages) == 0 {
			return nil, fmt.Errorf("line %d: shard %s has no packages", i+1, name)
		}
		inLine := map[string]bool{}
		for _, p := range packages {
			if inLine[p] {
				return nil, fmt.Errorf("line %d: package %s is listed twice in shard %s", i+1, p, name)
			}
			inLine[p] = true
		}
		for _, t := range tests {
			if prev, ok := seenTest[t]; ok {
				return nil, fmt.Errorf("line %d: test %s is assigned to both %s and %s", i+1, t, prev, name)
			}
			seenTest[t] = name
		}
		shards = append(shards, shard{name: name, packages: packages, tests: tests})
	}
	if len(shards) == 0 {
		return nil, fmt.Errorf("the shard table is empty")
	}
	return shards, nil
}

func findShard(shards []shard, name string) (shard, error) {
	for _, s := range shards {
		if s.name == name {
			return s, nil
		}
	}
	return shard{}, fmt.Errorf("no shard named %q in the table", name)
}

// topPackage returns the import path of the top-level test/e2e package in
// pkgs, or "" when pkgs has none.
func topPackage(pkgs []string) string {
	for _, p := range pkgs {
		if strings.HasSuffix(p, "/test/e2e") {
			return p
		}
	}
	return ""
}

// expandShards replaces each shard's package patterns with the import paths
// they name.
func expandShards(shards []shard, packages func(...string) ([]string, error)) ([]shard, error) {
	out := make([]shard, len(shards))
	for i, s := range shards {
		ps, err := packages(s.packages...)
		if err != nil {
			return nil, err
		}
		sort.Strings(ps)
		out[i] = shard{name: s.name, packages: ps, tests: s.tests}
	}
	return out, nil
}

// packageOwners maps each package an expanded shard lists to the shards that
// list it. A package listed by one shard runs all its tests; one listed by
// several is split by test name.
func packageOwners(shards []shard) map[string][]string {
	owners := map[string][]string{}
	for _, s := range shards {
		for _, p := range s.packages {
			owners[p] = append(owners[p], s.name)
		}
	}
	return owners
}

// runNamesFor returns the top-level tests a shard's -run regex must select. A
// package the shard lists alone runs all its tests, so they are all named. A
// package listed by several shards is split, so the shard runs only the tests
// its table line names. Every named test must belong to one of the shard's
// packages: a name that belongs to none would select nothing and silently drop
// a test, so it is an error.
func runNamesFor(s shard, owners map[string][]string, testsOf func(string) ([]string, error)) ([]string, error) {
	declared := map[string]bool{}
	for _, t := range s.tests {
		declared[t] = true
	}
	found := map[string]bool{}
	names := append([]string(nil), s.tests...)
	for _, p := range s.packages {
		ts, err := testsOf(p)
		if err != nil {
			return nil, err
		}
		for _, t := range ts {
			if declared[t] {
				found[t] = true
			}
		}
		if len(owners[p]) == 1 {
			names = append(names, ts...)
		}
	}
	for _, t := range s.tests {
		if !found[t] {
			return nil, fmt.Errorf("shard %s declares %s, which is not a test of a package it includes", s.name, t)
		}
	}
	if len(names) == 0 {
		return nil, fmt.Errorf("shard %s selects no tests: every package it lists is split and its test list is empty", s.name)
	}
	return names, nil
}

// runRegex returns an anchored regexp selecting exactly names, or "" when
// there are no names (go test then runs every test).
func runRegex(names []string) string {
	seen := map[string]bool{}
	var unique []string
	for _, n := range names {
		if n == "" || seen[n] {
			continue
		}
		seen[n] = true
		unique = append(unique, n)
	}
	if len(unique) == 0 {
		return ""
	}
	sort.Strings(unique)
	quoted := make([]string, len(unique))
	for i, n := range unique {
		quoted[i] = regexp.QuoteMeta(n)
	}
	return "^(" + strings.Join(quoted, "|") + ")$"
}

func compile(names []string) *regexp.Regexp {
	expr := runRegex(names)
	if expr == "" {
		return nil
	}
	return regexp.MustCompile(expr)
}

func runCheck(shards []shard) error {
	l := realLister{}
	all, err := l.packages("./test/e2e/...")
	if err != nil {
		return err
	}
	sort.Strings(all)

	expanded, err := expandShards(shards, l.packages)
	if err != nil {
		return err
	}

	testsByPkg := map[string][]string{}
	for _, p := range all {
		ts, err := l.tests(p)
		if err != nil {
			return err
		}
		sort.Strings(ts)
		testsByPkg[p] = ts
	}

	top := topPackage(all)
	_, _ = fmt.Println("e2e-shards: packages:")
	for _, p := range all {
		_, _ = fmt.Println("e2e-shards:   " + p)
	}
	_, _ = fmt.Println("e2e-shards: top-level tests:")
	for _, t := range testsByPkg[top] {
		_, _ = fmt.Println("e2e-shards:   " + t)
	}

	if err := verify(expanded, all, testsByPkg); err != nil {
		return err
	}
	total := 0
	for _, p := range all {
		total += len(testsByPkg[p])
	}
	_, _ = fmt.Printf("e2e-shards: ok: %d packages and %d top-level tests are assigned exactly once across %d shards\n",
		len(all), total, len(expanded))
	return nil
}

// verify checks that every package is listed by at least one shard and that
// every test in the full set is selected by exactly one shard. testsByPkg holds
// every test of every package. A package listed by one shard runs all its
// tests; a package listed by several is split by test name, so leaving one of
// its tests out of every shard's test list makes the check fail rather than
// silently dropping the test from the gate.
func verify(shards []shard, all []string, testsByPkg map[string][]string) error {
	inAll := map[string]bool{}
	for _, p := range all {
		inAll[p] = true
	}
	owners := packageOwners(shards)

	var packageErrs []string
	for _, s := range shards {
		for _, p := range s.packages {
			if !inAll[p] {
				packageErrs = append(packageErrs, fmt.Sprintf("package %s in shard %s is not under ./test/e2e/...", p, s.name))
			}
		}
	}
	for _, p := range all {
		if len(owners[p]) == 0 {
			packageErrs = append(packageErrs, fmt.Sprintf("package %s is assigned to no shard", p))
		}
	}
	if len(packageErrs) > 0 {
		return fmt.Errorf("%s", strings.Join(packageErrs, "\n"))
	}

	type testKey struct{ pkg, test string }
	runs := map[testKey][]string{}
	for _, s := range shards {
		names, err := runNamesFor(s, owners, func(pkg string) ([]string, error) {
			return testsByPkg[pkg], nil
		})
		if err != nil {
			return err
		}
		re := compile(names)
		for _, p := range s.packages {
			for _, t := range testsByPkg[p] {
				if re == nil || re.MatchString(t) {
					k := testKey{p, t}
					runs[k] = append(runs[k], s.name)
				}
			}
		}
	}

	var testErrs []string
	for _, p := range all {
		for _, t := range testsByPkg[p] {
			k := testKey{p, t}
			switch len(runs[k]) {
			case 0:
				testErrs = append(testErrs, fmt.Sprintf("%s %s is assigned to no shard", p, t))
			case 1:
			default:
				testErrs = append(testErrs, fmt.Sprintf("%s %s is assigned to %s", p, t, strings.Join(runs[k], ", ")))
			}
		}
	}
	if len(testErrs) > 0 {
		return fmt.Errorf("%s", strings.Join(testErrs, "\n"))
	}
	return nil
}

type realLister struct{}

func (realLister) packages(patterns ...string) ([]string, error) {
	args := append([]string{"list", "-tags", "e2e"}, patterns...)
	out, err := exec.Command("go", args...).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("go list %s: %w: %s", strings.Join(patterns, " "), err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, fmt.Errorf("go list %s: %w", strings.Join(patterns, " "), err)
	}
	return strings.Fields(string(out)), nil
}

func (realLister) tests(pkg string) ([]string, error) {
	out, err := exec.Command("go", "test", "-tags", "e2e", "-list", "^Test", pkg).Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return nil, fmt.Errorf("go test -list %s: %w: %s", pkg, err, strings.TrimSpace(string(ee.Stderr)))
		}
		return nil, fmt.Errorf("go test -list %s: %w", pkg, err)
	}
	var names []string
	for _, line := range strings.Split(string(out), "\n") {
		line = strings.TrimSpace(line)
		if strings.HasPrefix(line, "Test") && !strings.ContainsAny(line, " \t") {
			names = append(names, line)
		}
	}
	return names, nil
}
