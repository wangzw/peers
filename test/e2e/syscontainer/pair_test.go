package syscontainer

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

// TestPairRole checks that PairRole and InPair accept only roles A and B.
func TestPairRole(t *testing.T) {
	cases := []struct {
		value string
		want  Role
	}{
		{"", ""},
		{"A", RoleA},
		{"B", RoleB},
		{"C", ""},
		{"a", ""},
	}
	for _, tc := range cases {
		t.Setenv(PairRoleEnv, tc.value)
		if got := PairRole(); got != tc.want {
			t.Errorf("PairRole() with %s=%q = %q, want %q", PairRoleEnv, tc.value, got, tc.want)
		}
		if got := InPair(); got != (tc.want != "") {
			t.Errorf("InPair() with %s=%q = %t, want %t", PairRoleEnv, tc.value, got, tc.want != "")
		}
	}
}

// TestPeerRole checks that peerRole maps each role to the other.
func TestPeerRole(t *testing.T) {
	if got := peerRole(RoleA); got != RoleB {
		t.Errorf("peerRole(A) = %q, want %q", got, RoleB)
	}
	if got := peerRole(RoleB); got != RoleA {
		t.Errorf("peerRole(B) = %q, want %q", got, RoleA)
	}
}

// TestPairFailureName checks that the failure marker names the failing role.
func TestPairFailureName(t *testing.T) {
	if got := pairFailureName(RoleA); got != "failed-A" {
		t.Errorf("pairFailureName(A) = %q, want failed-A", got)
	}
	if got := pairFailureName(RoleB); got != "failed-B" {
		t.Errorf("pairFailureName(B) = %q, want failed-B", got)
	}
}

// TestWritePairFile checks that the write lands under the requested name and
// leaves no temporary file behind.
func TestWritePairFile(t *testing.T) {
	dir := t.TempDir()
	if err := writePairFile(dir, "pairing", []byte("hostA:17443\nkey\n")); err != nil {
		t.Fatalf("writePairFile: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "pairing"))
	if err != nil {
		t.Fatalf("reading the written file: %v", err)
	}
	if got, want := string(data), "hostA:17443\nkey\n"; got != want {
		t.Errorf("written file = %q, want %q", got, want)
	}

	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("reading the directory: %v", err)
	}
	if len(entries) != 1 {
		t.Errorf("the directory holds %d entries, want only the written file", len(entries))
	}
}

// TestWritePairFileRejectsInvalidNames checks that writePairFile refuses names
// that leave the directory and an empty directory.
func TestWritePairFileRejectsInvalidNames(t *testing.T) {
	dir := t.TempDir()
	for _, name := range []string{"", ".", "..", "a/b", "../escape"} {
		if err := writePairFile(dir, name, []byte("x")); err == nil {
			t.Errorf("writePairFile accepted the name %q", name)
		}
	}
	if err := writePairFile("", "pairing", []byte("x")); err == nil {
		t.Error("writePairFile accepted an empty directory")
	}
}

// TestPairAwaitReadsTheOtherHostsFile checks that PairAwait returns the
// contents the other host wrote.
func TestPairAwaitReadsTheOtherHostsFile(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(PairRoleEnv, string(RoleA))
	t.Setenv(pairSharedEnv, dir)
	defer PairGuard(t)()
	if err := writePairFile(dir, "connected", []byte("connected\n")); err != nil {
		t.Fatalf("writePairFile: %v", err)
	}
	if got, want := string(PairAwait(t, "connected", time.Second)), "connected\n"; got != want {
		t.Errorf("PairAwait returned %q, want %q", got, want)
	}
}

// TestPairGuardArmsAndDisarms checks that PairGuard arms the guard and disarms
// it when the returned function runs.
func TestPairGuardArmsAndDisarms(t *testing.T) {
	t.Setenv(PairRoleEnv, string(RoleA))
	t.Setenv(pairSharedEnv, t.TempDir())
	if pairGuardArmed(t) {
		t.Fatal("the test starts with the failure guard armed")
	}
	guard := PairGuard(t)
	if !pairGuardArmed(t) {
		t.Fatal("PairGuard did not arm the guard")
	}
	guard()
	if pairGuardArmed(t) {
		t.Fatal("the guard stayed armed after it ran")
	}
}

// TestPairGuardRecordsAPanic checks that the guard recovers a panic, writes the
// failure marker and panics again.
func TestPairGuardRecordsAPanic(t *testing.T) {
	dir := t.TempDir()
	t.Setenv(PairRoleEnv, string(RoleA))
	t.Setenv(pairSharedEnv, dir)

	func() {
		defer func() {
			if recovered := recover(); recovered != "boom" {
				t.Errorf("recovered %v, want the guard to panic again with boom", recovered)
			}
		}()
		func() {
			defer PairGuard(t)()
			panic("boom")
		}()
	}()

	data, err := os.ReadFile(filepath.Join(dir, pairFailureName(RoleA)))
	if err != nil {
		t.Fatalf("reading the failure marker: %v", err)
	}
	if got, want := string(data), "host A failed"; got != want {
		t.Errorf("failure marker = %q, want %q", got, want)
	}
}
