package main

import (
	"bytes"
	"encoding/base64"
	"fmt"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

// testToken is chosen so its standard and URL-safe base64 encodings differ.
const testToken = secretPrefix + "TJFiz4soCmZ9cpvnpKnA1h??~~4PmBxdU"

// otherToken shares no body bytes with testToken, so none of its encodings can
// be mistaken for a leak.
const otherToken = secretPrefix + "QqWwEeRrTtYyUuIiOoPpAaSsDdFfGgHhJjKkLlZzXxCcVvBbNnMm"

// TestTestTokenEncodingsDiffer checks the test token's standard and URL-safe
// encodings actually differ.
func TestTestTokenEncodingsDiffer(t *testing.T) {
	standard := base64.RawStdEncoding.EncodeToString([]byte(testToken))
	urlSafe := base64.RawURLEncoding.EncodeToString([]byte(testToken))
	if standard == urlSafe {
		t.Fatal("the standard and URL-safe encodings of testToken are identical")
	}
	for _, want := range []string{"+", "/"} {
		if !strings.Contains(standard, want) {
			t.Fatalf("the standard encoding of testToken does not contain %q", want)
		}
	}
}

func encodeAt(token string, offset int, urlSafe, embedded bool) string {
	encoding := base64.RawStdEncoding
	if urlSafe {
		encoding = base64.RawURLEncoding
	}
	if !embedded {
		return encoding.EncodeToString([]byte(token[offset:]))
	}
	prefixLen := (3 - offset%3) % 3
	if prefixLen == 0 {
		prefixLen = 3
	}
	blob := append(bytes.Repeat([]byte("P"), prefixLen), token...)
	blob = append(blob, []byte("TAIL")...)
	return encoding.EncodeToString(blob)
}

func scanOne(t *testing.T, content, token string) (int, error) {
	t.Helper()
	dir := t.TempDir()
	if err := os.WriteFile(filepath.Join(dir, "evidence.txt"), []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
	hits, err := scanSecretDir(dir, token)
	return len(hits), err
}

// TestScanSecretDir checks that the whole token, a split token and a truncated
// fragment are found but another token is not.
func TestScanSecretDir(t *testing.T) {
	body := strings.TrimPrefix(testToken, secretPrefix)
	tests := []struct {
		name    string
		content string
		wantHit bool
	}{
		{"the whole token", "token=" + testToken + "\n", true},
		{"a token split by a newline", "before " + testToken[:20] + "\n" + testToken[20:] + " after\n", true},
		{"a truncated fragment", "prefix " + body[4:4+fragmentLength] + " suffix\n", true},
		{"a different token", otherToken, false},
		{"a file without the token", "nothing to see here\n", false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hits, err := scanOne(t, tt.content, testToken)
			if err != nil {
				t.Fatalf("scanSecretDir = %v", err)
			}
			if got := hits == 1; got != tt.wantHit {
				t.Fatalf("hits = %d, want hit %v", hits, tt.wantHit)
			}
		})
	}
}

// TestScanSecretDirEncodings checks both base64 alphabets at each alignment,
// standalone and embedded, with a hit and a miss for each combination.
func TestScanSecretDirEncodings(t *testing.T) {
	encodings := []struct {
		name    string
		urlSafe bool
	}{
		{"standard", false},
		{"url-safe", true},
	}
	layouts := []struct {
		name     string
		embedded bool
	}{
		{"standalone", false},
		{"embedded", true},
	}
	for _, encoding := range encodings {
		for offset := 0; offset < 3; offset++ {
			for _, layout := range layouts {
				name := fmt.Sprintf("%s/offset-%d/%s", encoding.name, offset, layout.name)
				t.Run(name+"/hit", func(t *testing.T) {
					content := encodeAt(testToken, offset, encoding.urlSafe, layout.embedded)
					hits, err := scanOne(t, content, testToken)
					if err != nil {
						t.Fatalf("scanSecretDir = %v", err)
					}
					if hits != 1 {
						t.Fatalf("hits = %d, want 1 for the encoding %q", hits, content)
					}
				})
				t.Run(name+"/miss", func(t *testing.T) {
					content := encodeAt(otherToken, offset, encoding.urlSafe, layout.embedded)
					hits, err := scanOne(t, content, testToken)
					if err != nil {
						t.Fatalf("scanSecretDir = %v", err)
					}
					if hits != 0 {
						t.Fatalf("hits = %d, want 0 for the encoding %q", hits, content)
					}
				})
			}
		}
	}
}

// TestScanSecretDirURLEncoding checks the query and path URL encodings are
// found and another token's are not.
func TestScanSecretDirURLEncoding(t *testing.T) {
	const token = secretPrefix + "a+b/c=d e"
	tests := []struct {
		name    string
		content string
		wantHit bool
	}{
		{"query escape", url.QueryEscape(token), true},
		{"path escape", url.PathEscape(token), true},
		{"query escape of another token", url.QueryEscape(secretPrefix + "f+g/h=i j"), false},
		{"path escape of another token", url.PathEscape(secretPrefix + "f+g/h=i j"), false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hits, err := scanOne(t, tt.content, token)
			if err != nil {
				t.Fatalf("scanSecretDir = %v", err)
			}
			if got := hits == 1; got != tt.wantHit {
				t.Fatalf("hits = %d, want hit %v", hits, tt.wantHit)
			}
		})
	}
}

// TestScanSecretRejectsSymlinks checks that a non-regular entry is a scan
// failure (exit 2), not something skipped.
func TestScanSecretRejectsSymlinks(t *testing.T) {
	const env = "RSPROBE_TEST_TOKEN"
	t.Setenv(env, testToken)
	base := t.TempDir()
	dir := filepath.Join(base, "evidence")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	target := filepath.Join(base, "outside.txt")
	if err := os.WriteFile(target, []byte(testToken), 0o644); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(target, filepath.Join(dir, "link.txt")); err != nil {
		t.Skipf("cannot create a symbolic link: %v", err)
	}

	if _, err := scanSecretDir(dir, testToken); err == nil {
		t.Fatal("scanSecretDir = nil error, want an error for a symbolic link")
	} else if !strings.Contains(err.Error(), "not a regular file") {
		t.Fatalf("scanSecretDir = %v, want it to name the non-regular entry", err)
	}

	var stdout, stderr bytes.Buffer
	err := runScanSecret([]string{"--dir", dir, "--env", env}, &stdout)
	if code := report(err, &stderr); code != 2 {
		t.Fatalf("report(symlink) = %d, want 2 (stderr %q)", code, stderr.String())
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want nothing", stdout.String())
	}
}

// TestScanSecretCommand checks the hit, clean, missing-directory and
// argument-error exit codes.
func TestScanSecretCommand(t *testing.T) {
	const env = "RSPROBE_TEST_TOKEN"
	t.Setenv(env, testToken)

	hitDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(hitDir, "evidence.txt"), []byte(testToken), 0o644); err != nil {
		t.Fatal(err)
	}
	cleanDir := t.TempDir()
	if err := os.WriteFile(filepath.Join(cleanDir, "evidence.txt"), []byte("clean\n"), 0o644); err != nil {
		t.Fatal(err)
	}

	var stdout, stderr bytes.Buffer
	err := runScanSecret([]string{"--dir", hitDir, "--env", env}, &stdout)
	if err == nil {
		t.Fatal("runScanSecret(hit) = nil, want an error")
	}
	if code := report(err, &stderr); code != 1 {
		t.Fatalf("report(hit) = %d, want 1 (stderr %q)", code, stderr.String())
	}
	if got := stdout.String(); !strings.Contains(got, "evidence.txt") {
		t.Fatalf("stdout = %q, want the hit path", got)
	}
	if strings.Contains(stdout.String(), testToken) {
		t.Fatalf("stdout contains the token: %q", stdout.String())
	}

	stdout.Reset()
	stderr.Reset()
	if err := runScanSecret([]string{"--dir", cleanDir, "--env", env}, &stdout); err != nil {
		t.Fatalf("runScanSecret(clean) = %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout = %q, want nothing", stdout.String())
	}

	if code := dispatch([]string{"scan-secret", "--dir", cleanDir, "--env", env}, &stderr); code != 0 {
		t.Fatalf("dispatch(clean) = %d, want 0 (stderr %q)", code, stderr.String())
	}

	err = runScanSecret([]string{"--dir", filepath.Join(cleanDir, "missing"), "--env", env}, &stdout)
	if code := report(err, &stderr); code != 2 {
		t.Fatalf("report(missing directory) = %d, want 2", code)
	}

	// An argument problem or an empty token means the scan could not run, which
	// is a failure (2), never a hit (1) and never a clean result.
	for _, tc := range []struct {
		name string
		args []string
	}{
		{"no --dir", []string{"--env", env}},
		{"no --env", []string{"--dir", cleanDir}},
		{"unexpected argument", []string{"--dir", cleanDir, "--env", env, "extra"}},
		{"unknown flag", []string{"--dir", cleanDir, "--env", env, "--unknown"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			stdout.Reset()
			stderr.Reset()
			err := runScanSecret(tc.args, &stdout)
			if code := report(err, &stderr); code != 2 {
				t.Fatalf("report(%s) = %d, want 2 (stderr %q)", tc.name, code, stderr.String())
			}
			if stdout.Len() != 0 {
				t.Fatalf("stdout = %q, want nothing", stdout.String())
			}
		})
	}

	t.Setenv(env, "")
	if code := dispatch([]string{"scan-secret", "--dir", cleanDir, "--env", env}, &stderr); code != 2 {
		t.Fatalf("dispatch(empty token) = %d, want 2", code)
	}
}
