package main

import (
	"strings"
	"testing"
)

func fixture() ([]shard, []string, map[string][]string) {
	const (
		top    = "example.com/peers/test/e2e"
		helper = "example.com/peers/test/e2e/harness"
		system = "example.com/peers/test/e2e/system"
	)
	shards := []shard{
		{name: "top-a", packages: []string{top}, tests: []string{"TestA"}},
		{name: "top-b", packages: []string{top, helper}, tests: []string{"TestB", "TestC"}},
		{name: "system", packages: []string{system}},
	}
	all := []string{top, helper, system}
	tests := map[string][]string{
		top:    {"TestA", "TestB", "TestC"},
		helper: {"TestHelper"},
		system: {"TestSystem"},
	}
	return shards, all, tests
}

func TestParseTable(t *testing.T) {
	got, err := parseTable(`
# a comment
shard a ./test/e2e -- TestA TestB
shard b ./test/e2e/harness
`)
	if err != nil {
		t.Fatalf("parseTable: %v", err)
	}
	if len(got) != 2 {
		t.Fatalf("shards = %d, want 2", len(got))
	}
	if got[0].name != "a" || strings.Join(got[0].packages, " ") != "./test/e2e" ||
		strings.Join(got[0].tests, " ") != "TestA TestB" {
		t.Errorf("first shard = %+v", got[0])
	}
	if got[1].name != "b" || strings.Join(got[1].packages, " ") != "./test/e2e/harness" || len(got[1].tests) != 0 {
		t.Errorf("second shard = %+v", got[1])
	}
}

func TestParseTableRejectsDuplicates(t *testing.T) {
	for name, text := range map[string]string{
		"shard": "shard a ./test/e2e\nshard a ./test/e2e/system",
		"test":  "shard a ./test/e2e -- TestA\nshard b ./test/e2e -- TestA",
	} {
		t.Run(name, func(t *testing.T) {
			if _, err := parseTable(text); err == nil {
				t.Fatal("parseTable accepted a duplicate")
			}
		})
	}
}

func TestRunRegex(t *testing.T) {
	if got := runRegex(nil); got != "" {
		t.Errorf("runRegex(nil) = %q, want empty", got)
	}
	if got, want := runRegex([]string{"TestB", "TestA", "TestB"}), "^(TestA|TestB)$"; got != want {
		t.Errorf("runRegex = %q, want %q", got, want)
	}
}

// TestRunRegexExcludesLongerNameFromSplitPackage guards the split path against
// a prefix match: the package holds TestS1 and TestS10, the shard names only
// TestS1, and the -run regex it produces must not also select TestS10.
func TestRunRegexExcludesLongerNameFromSplitPackage(t *testing.T) {
	const system = "example.com/peers/test/e2e/system"
	owners := map[string][]string{system: {"system", "system-b"}}
	names, err := runNamesFor(shard{name: "system", packages: []string{system}, tests: []string{"TestS1"}}, owners, func(string) ([]string, error) {
		return []string{"TestS1", "TestS10"}, nil
	})
	if err != nil {
		t.Fatalf("runNamesFor: %v", err)
	}
	if got, want := runRegex(names), "^(TestS1)$"; got != want {
		t.Fatalf("runRegex = %q, want %q", got, want)
	}
	re := compile(names)
	if re == nil {
		t.Fatal("compile returned nil")
	}
	if !re.MatchString("TestS1") {
		t.Error("run regex does not match TestS1")
	}
	if re.MatchString("TestS10") {
		t.Error("run regex matches TestS10, which the shard did not name")
	}
}

func TestVerifyAcceptsCompleteAssignment(t *testing.T) {
	shards, all, tests := fixture()
	if err := verify(shards, all, tests); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifyRejectsUnassignedTest(t *testing.T) {
	shards, all, tests := fixture()
	shards[1].tests = []string{"TestB"}
	err := verify(shards, all, tests)
	if err == nil || !strings.Contains(err.Error(), "TestC is assigned to no shard") {
		t.Fatalf("verify error = %v, want an unassigned TestC", err)
	}
}

func TestVerifyRejectsTestAssignedTwice(t *testing.T) {
	shards, all, tests := fixture()
	shards[0].tests = []string{"TestA", "TestB"}
	err := verify(shards, all, tests)
	if err == nil || !strings.Contains(err.Error(), "TestB is assigned to top-a, top-b") {
		t.Fatalf("verify error = %v, want TestB assigned twice", err)
	}
}

func TestVerifyRejectsHelperNameCollision(t *testing.T) {
	shards, all, tests := fixture()
	tests["example.com/peers/test/e2e/harness"] = []string{"TestHelper", "TestA"}
	err := verify(shards, all, tests)
	if err == nil || !strings.Contains(err.Error(), "TestA is assigned to top-a, top-b") {
		t.Fatalf("verify error = %v, want a collision on TestA", err)
	}
}

func TestVerifyRejectsUnknownDeclaredTest(t *testing.T) {
	shards, all, tests := fixture()
	shards[0].tests = []string{"TestA", "TestMissing"}
	err := verify(shards, all, tests)
	if err == nil || !strings.Contains(err.Error(), "declares TestMissing") {
		t.Fatalf("verify error = %v, want an unknown declared test", err)
	}
}

func TestVerifyRejectsUnassignedPackage(t *testing.T) {
	shards, all, tests := fixture()
	all = append(all, "example.com/peers/test/e2e/ctl")
	tests["example.com/peers/test/e2e/ctl"] = []string{"TestCtl"}
	err := verify(shards, all, tests)
	if err == nil || !strings.Contains(err.Error(), "package example.com/peers/test/e2e/ctl is assigned to no shard") {
		t.Fatalf("verify error = %v, want an unassigned package", err)
	}
}

func TestVerifyRejectsSharedHelperPackage(t *testing.T) {
	shards, all, tests := fixture()
	shards[2].packages = []string{
		"example.com/peers/test/e2e/system",
		"example.com/peers/test/e2e/harness",
	}
	// Listing the harness in two shards splits it, so its tests must be named
	// where they run. Neither shard names TestHelper, so it drops out.
	err := verify(shards, all, tests)
	if err == nil || !strings.Contains(err.Error(), "TestHelper is assigned to no shard") {
		t.Fatalf("verify error = %v, want TestHelper assigned to no shard", err)
	}
}

// splitFixture assigns the system package to two shards by test name, the way
// a shard table splits a package to shorten the critical path.
func splitFixture() ([]shard, []string, map[string][]string) {
	const (
		top    = "example.com/peers/test/e2e"
		system = "example.com/peers/test/e2e/system"
	)
	shards := []shard{
		{name: "top-a", packages: []string{top}, tests: []string{"TestA"}},
		{name: "top-b", packages: []string{top}, tests: []string{"TestB"}},
		{name: "system", packages: []string{system}, tests: []string{"TestS1"}},
		{name: "system-b", packages: []string{system}, tests: []string{"TestS2"}},
	}
	all := []string{top, system}
	tests := map[string][]string{
		top:    {"TestA", "TestB"},
		system: {"TestS1", "TestS2", "TestS3"},
	}
	return shards, all, tests
}

func TestVerifyAcceptsSplitPackage(t *testing.T) {
	shards, all, tests := splitFixture()
	tests["example.com/peers/test/e2e/system"] = []string{"TestS1", "TestS2"}
	if err := verify(shards, all, tests); err != nil {
		t.Fatalf("verify: %v", err)
	}
}

func TestVerifyRejectsTestMissingFromSplitPackage(t *testing.T) {
	shards, all, tests := splitFixture()
	err := verify(shards, all, tests)
	if err == nil || !strings.Contains(err.Error(), "TestS3 is assigned to no shard") {
		t.Fatalf("verify error = %v, want an unassigned TestS3", err)
	}
}

func TestRunNamesForWholePackage(t *testing.T) {
	const system = "example.com/peers/test/e2e/system"
	owners := map[string][]string{system: {"system"}}
	names, err := runNamesFor(shard{name: "system", packages: []string{system}}, owners, func(pkg string) ([]string, error) {
		if pkg != system {
			t.Fatalf("runNamesFor listed %s", pkg)
		}
		return []string{"TestS1", "TestS2"}, nil
	})
	if err != nil {
		t.Fatalf("runNamesFor: %v", err)
	}
	if got := strings.Join(names, " "); got != "TestS1 TestS2" {
		t.Fatalf("names = %q, want TestS1 TestS2", got)
	}
}

func TestRunNamesForSplitPackage(t *testing.T) {
	const system = "example.com/peers/test/e2e/system"
	owners := map[string][]string{system: {"system", "system-b"}}
	names, err := runNamesFor(shard{name: "system", packages: []string{system}, tests: []string{"TestS1"}}, owners, func(string) ([]string, error) {
		return []string{"TestS1", "TestS2"}, nil
	})
	if err != nil {
		t.Fatalf("runNamesFor: %v", err)
	}
	if got := strings.Join(names, " "); got != "TestS1" {
		t.Fatalf("names = %q, want only the declared TestS1", got)
	}
}

func TestRunNamesForRejectsUnknownDeclaredTest(t *testing.T) {
	const system = "example.com/peers/test/e2e/system"
	owners := map[string][]string{system: {"system", "system-b"}}
	_, err := runNamesFor(shard{name: "system", packages: []string{system}, tests: []string{"TestMissing"}}, owners, func(string) ([]string, error) {
		return []string{"TestS1"}, nil
	})
	if err == nil || !strings.Contains(err.Error(), "declares TestMissing") {
		t.Fatalf("runNamesFor error = %v, want an unknown declared test", err)
	}
}
