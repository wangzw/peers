package update

import (
	"bytes"
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/wangzw/peers/internal/broker/service"
)

// TestInstallRejectsInvalidLevel checks that an install refuses a level that is
// neither system nor user before it touches the file system.
func TestInstallRejectsInvalidLevel(t *testing.T) {
	err := Install(context.Background(), ApplyOptions{Client: noNetworkClient(t)}, "bogus")
	if err == nil {
		t.Fatal("Install() = nil, want an error")
	}
	if want := `service level "bogus" is not system or user`; err.Error() != want {
		t.Errorf("Install() error = %q, want %q", err, want)
	}
}

// TestInstallReportsARollbackNameItCannotReserve checks that a target whose
// directory does not exist is reported before the release is fetched.
func TestInstallReportsARollbackNameItCannotReserve(t *testing.T) {
	target := filepath.Join(t.TempDir(), "missing", binaryName)

	err := Install(context.Background(), ApplyOptions{
		Target:  target,
		Version: "0.2.0",
		Client:  noNetworkClient(t),
		GOOS:    "linux",
		GOARCH:  "amd64",
	}, "user")
	if err == nil {
		t.Fatal("Install() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "reserving a rollback name") {
		t.Errorf("Install() error = %v, want it to report the missing rollback directory", err)
	}
}

// TestInstallRestoresThePreviousBinaryWhenApplyFails checks that an Apply
// failure after the target was replaced restores the previous binary and leaves
// no rollback file.
func TestInstallRestoresThePreviousBinaryWhenApplyFails(t *testing.T) {
	const version = "0.2.0"
	original := []byte("original binary")
	target, before := newTarget(t, original, 0o750)

	archive := releaseArchive(t, version)
	srv := newApplyServer(t, version, writeBytes(archive), checksumsFor(archive, testArchiveName(version)))
	syncErr := errors.New("cannot sync the directory")
	old := syncDirAfterReplace
	syncDirAfterReplace = func(string) error { return syncErr }
	t.Cleanup(func() { syncDirAfterReplace = old })

	err := Install(context.Background(), testApplyOptions(srv, target, version), "user")
	if err == nil {
		t.Fatal("Install() = nil, want an error")
	}
	if !errors.Is(err, syncErr) {
		t.Errorf("Install() error = %v, want the directory sync failure", err)
	}
	assertUnchanged(t, target, original, before)
	assertNoRollbackFiles(t, filepath.Dir(target))
}

// TestRunPostUpdateRequiresABinary checks that a post-update run without a
// binary is refused.
func TestRunPostUpdateRequiresABinary(t *testing.T) {
	err := RunPostUpdate(context.Background(), PostUpdateOptions{Level: "user"})
	if err == nil {
		t.Fatal("RunPostUpdate() = nil, want an error")
	}
	if want := "no binary to run the post-update step"; err.Error() != want {
		t.Errorf("RunPostUpdate() error = %q, want %q", err, want)
	}
}

// TestRunPostUpdateRejectsInvalidLevel checks that the level is validated
// before the binary is started.
func TestRunPostUpdateRejectsInvalidLevel(t *testing.T) {
	binary := filepath.Join(t.TempDir(), binaryName)

	err := RunPostUpdate(context.Background(), PostUpdateOptions{Binary: binary, Level: "bogus"})
	if err == nil {
		t.Fatal("RunPostUpdate() = nil, want an error")
	}
	if want := `service level "bogus" is not system or user`; err.Error() != want {
		t.Errorf("RunPostUpdate() error = %q, want %q", err, want)
	}
}

// TestBackupTargetHardLinksTheTarget checks that the rollback name is a hard
// link to the target, so it keeps the old inode without copying it.
func TestBackupTargetHardLinksTheTarget(t *testing.T) {
	target, before := newTarget(t, []byte("original binary"), 0o750)

	backup, err := backupTarget(target)
	if err != nil {
		t.Fatalf("backupTarget() error = %v", err)
	}
	t.Cleanup(func() { _ = os.Remove(backup) })
	if !strings.HasPrefix(filepath.Base(backup), ".peers-rollback-") {
		t.Errorf("backup name = %q, want the rollback prefix", backup)
	}
	got, err := os.Stat(backup)
	if err != nil {
		t.Fatalf("stat the backup: %v", err)
	}
	if !os.SameFile(before, got) {
		t.Error("the backup is not the target's inode")
	}
}

// TestBackupTargetRejectsAMissingDirectory checks that a target in a missing
// directory is reported and leaves no reserved name behind.
func TestBackupTargetRejectsAMissingDirectory(t *testing.T) {
	target := filepath.Join(t.TempDir(), "missing", binaryName)

	_, err := backupTarget(target)
	if err == nil {
		t.Fatal("backupTarget() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "reserving a rollback name") {
		t.Errorf("backupTarget() error = %v, want it to report the reservation", err)
	}
}

// TestBackupTargetRejectsAMissingTarget checks that a target that does not
// exist is reported and the reserved name is removed again.
func TestBackupTargetRejectsAMissingTarget(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, binaryName)

	_, err := backupTarget(target)
	if err == nil {
		t.Fatal("backupTarget() = nil, want an error")
	}
	if !strings.Contains(err.Error(), "saving a rollback copy") {
		t.Errorf("backupTarget() error = %v, want it to report the hard link", err)
	}
	assertNoRollbackFiles(t, dir)
}

// TestRestoreTargetReportsAMissingBackup checks that a rollback without its
// backup file is reported and leaves the target as it was.
func TestRestoreTargetReportsAMissingBackup(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, binaryName)
	original := []byte("installed binary")
	if err := os.WriteFile(target, original, 0o755); err != nil {
		t.Fatalf("writing the target: %v", err)
	}
	backup := filepath.Join(dir, ".peers-rollback-absent")

	err := restoreTarget(backup, target)
	if err == nil {
		t.Fatal("restoreTarget() = nil, want an error")
	}
	for _, want := range []string{"restoring", target} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("restoreTarget() error = %v, want it to contain %q", err, want)
		}
	}
	got, readErr := os.ReadFile(target)
	if readErr != nil {
		t.Fatalf("reading the target: %v", readErr)
	}
	if !bytes.Equal(got, original) {
		t.Errorf("target content = %q, want the original %q", got, original)
	}
}

// TestRestoreFailureCarriesBothErrors checks that the restore failure keeps the
// original cause and names why the previous binary could not come back.
func TestRestoreFailureCarriesBothErrors(t *testing.T) {
	cause := errors.New("the post-update step failed")
	restoreErr := errors.New("rename across devices")

	err := restoreFailure(cause, restoreErr)
	if !errors.Is(err, cause) {
		t.Errorf("restoreFailure() error = %v, want it to carry the cause", err)
	}
	for _, want := range []string{"restoring the previous binary failed", restoreErr.Error()} {
		if !strings.Contains(err.Error(), want) {
			t.Errorf("restoreFailure() error = %v, want it to contain %q", err, want)
		}
	}
}

// TestRestoreAfterStepSetupFailureReportsBothOutcomes checks the two results of
// the restore that follows a failed unit guard.
func TestRestoreAfterStepSetupFailureReportsBothOutcomes(t *testing.T) {
	cause := errors.New("cannot read the unit file")
	tests := []struct {
		name        string
		dropBackup  bool
		errContains []string
		wantTarget  []byte
	}{
		{
			name:        "restore succeeds",
			errContains: []string{"the previous binary was restored"},
			wantTarget:  []byte("original binary"),
		},
		{
			name:        "restore fails",
			dropBackup:  true,
			errContains: []string{"restoring the previous binary failed", "restoring"},
			wantTarget:  []byte("new binary"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			target := filepath.Join(dir, binaryName)
			if err := os.WriteFile(target, []byte("new binary"), 0o755); err != nil {
				t.Fatalf("writing the target: %v", err)
			}
			backup := filepath.Join(dir, ".peers-rollback-1")
			if err := os.WriteFile(backup, []byte("original binary"), 0o755); err != nil {
				t.Fatalf("writing the backup: %v", err)
			}
			if tc.dropBackup {
				if err := os.Remove(backup); err != nil {
					t.Fatalf("removing the backup: %v", err)
				}
			}

			err := restoreAfterStepSetupFailure(cause, backup, target)
			if !errors.Is(err, cause) {
				t.Errorf("restoreAfterStepSetupFailure() error = %v, want it to carry the cause", err)
			}
			for _, want := range tc.errContains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %v, want it to contain %q", err, want)
				}
			}
			got, readErr := os.ReadFile(target)
			if readErr != nil {
				t.Fatalf("reading the target: %v", readErr)
			}
			if !bytes.Equal(got, tc.wantTarget) {
				t.Errorf("target content = %q, want %q", got, tc.wantTarget)
			}
		})
	}
}

// TestRollBackInstallReportsEachCombination checks the four outcomes of a
// rollback whose binary restore and unit restore can each fail.
func TestRollBackInstallReportsEachCombination(t *testing.T) {
	cause := errors.New("the post-update step failed")
	tests := []struct {
		name        string
		dropBackup  bool
		badOptions  bool
		errContains []string
		wantTarget  []byte
	}{
		{
			name:        "both restores succeed",
			errContains: []string{"previous binary was restored, and so was the service definition"},
			wantTarget:  []byte("original binary"),
		},
		{
			name:        "only the unit restore fails",
			badOptions:  true,
			errContains: []string{"previous binary was restored, but the service definition could not be restored"},
			wantTarget:  []byte("original binary"),
		},
		{
			name:        "only the binary restore fails",
			dropBackup:  true,
			errContains: []string{"restoring the previous binary failed", "the service definition was restored"},
			wantTarget:  []byte("new binary"),
		},
		{
			name:        "both restores fail",
			dropBackup:  true,
			badOptions:  true,
			errContains: []string{"restoring the previous binary failed", "could not be restored"},
			wantTarget:  []byte("new binary"),
		},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			unitGuardFixture(t)
			dir := t.TempDir()
			target := filepath.Join(dir, binaryName)
			if err := os.WriteFile(target, []byte("new binary"), 0o755); err != nil {
				t.Fatalf("writing the target: %v", err)
			}
			backup := filepath.Join(dir, ".peers-rollback-1")
			if err := os.WriteFile(backup, []byte("original binary"), 0o755); err != nil {
				t.Fatalf("writing the backup: %v", err)
			}
			if tc.dropBackup {
				if err := os.Remove(backup); err != nil {
					t.Fatalf("removing the backup: %v", err)
				}
			}

			opts := service.Options{GOOS: "windows"}
			if !tc.badOptions {
				var optsErr error
				opts, optsErr = service.PostUpdateOptions(false)
				if optsErr != nil {
					t.Fatalf("service.PostUpdateOptions() error = %v", optsErr)
				}
			}

			err := rollBackInstall(cause, backup, target, opts, service.SavedUnit{})
			if !errors.Is(err, cause) {
				t.Errorf("rollBackInstall() error = %v, want it to carry the cause", err)
			}
			for _, want := range tc.errContains {
				if !strings.Contains(err.Error(), want) {
					t.Errorf("error = %v, want it to contain %q", err, want)
				}
			}
			got, readErr := os.ReadFile(target)
			if readErr != nil {
				t.Fatalf("reading the target: %v", readErr)
			}
			if !bytes.Equal(got, tc.wantTarget) {
				t.Errorf("target content = %q, want %q", got, tc.wantTarget)
			}
		})
	}
}
