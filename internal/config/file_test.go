package config

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestConfigFileRoundTripPreservesCommentsAndOrder checks that parsing and
// re-encoding keeps comments, key order and unrelated keys.
func TestConfigFileRoundTripPreservesCommentsAndOrder(t *testing.T) {
	body := `# head comment
broker:
  # alias comment
  alias: old  # inline comment
  public: true

send:
  request_timeout: 20s

custom:
  keep: me
`
	doc, err := parseConfigFile([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.setKey("broker.alias", stringValue("new-alias")); err != nil {
		t.Fatal(err)
	}
	out, err := doc.bytes()
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	for _, want := range []string{"# head comment", "# alias comment", "# inline comment", "custom:", "keep: me"} {
		if !strings.Contains(text, want) {
			t.Errorf("output lost %q:\n%s", want, text)
		}
	}
	if strings.Index(text, "public:") > strings.Index(text, "send:") {
		t.Errorf("key order changed:\n%s", text)
	}
	back, err := parseConfigFile(out)
	if err != nil {
		t.Fatal(err)
	}
	if got := back.values["broker.alias"].String; got != "new-alias" {
		t.Errorf("alias = %q, want new-alias", got)
	}
	if !back.values["broker.public"].Bool {
		t.Error("public was not preserved")
	}
}

// TestConfigFilePreservesBlockSequenceStyle checks that a block sequence keeps
// its style when another key is set.
func TestConfigFilePreservesBlockSequenceStyle(t *testing.T) {
	body := `broker:
  advertise_extra:
    - a.example
    - b.example
  listen: ":17443"
`
	doc, err := parseConfigFile([]byte(body))
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.setKey("broker.public", boolValue(true)); err != nil {
		t.Fatal(err)
	}
	out, err := doc.bytes()
	if err != nil {
		t.Fatal(err)
	}
	text := string(out)
	if !strings.Contains(text, "- a.example") || !strings.Contains(text, "- b.example") {
		t.Errorf("block sequence style changed:\n%s", text)
	}
}

// TestConfigFileCommentOnlyFirstSet checks that a comment-only file keeps its
// comments when the first key is set.
func TestConfigFileCommentOnlyFirstSet(t *testing.T) {
	doc, err := parseConfigFile([]byte("# keep this\n\n"))
	if err != nil {
		t.Fatal(err)
	}
	if err := doc.setKey("broker.alias", stringValue("hostA-17443")); err != nil {
		t.Fatal(err)
	}
	out, err := doc.bytes()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(out), "# keep this") {
		t.Errorf("comment-only header lost:\n%s", string(out))
	}
	back, err := parseConfigFile(out)
	if err != nil {
		t.Fatalf("encoded file does not parse: %v\n%s", err, out)
	}
	if back.values["broker.alias"].String != "hostA-17443" {
		t.Errorf("alias = %q", back.values["broker.alias"].String)
	}
}

func splitFileLines(s string) []string {
	return strings.Split(strings.TrimSuffix(s, "\n"), "\n")
}

// TestConfigFileSetUncommentsPlaceholderInPlace checks that setting a key that
// exists only as an inactive example line replaces that line in place without
// changing the line count or any other byte.
func TestConfigFileSetUncommentsPlaceholderInPlace(t *testing.T) {
	before := string(Template(ModeUser))
	c, path := newValueCore(t, before, "")
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.advertise", stringValue("198.51.100.7:17443"))
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}

	want := splitFileLines(before)
	replaced := -1
	for i, line := range want {
		if line == "  # advertise: 203.0.113.5:17443" {
			want[i] = "  advertise: 198.51.100.7:17443"
			replaced = i
			break
		}
	}
	if replaced < 0 {
		t.Fatal("template has no inactive advertise example")
	}
	got := splitFileLines(string(after))
	if len(got) != len(want) {
		t.Fatalf("line count changed from %d to %d:\n%s", len(want), len(got), after)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("line %d changed:\n want %q\n got  %q", i+1, want[i], got[i])
		}
	}
	if strings.Contains(string(after), "# advertise:") {
		t.Errorf("the inactive example line survived:\n%s", after)
	}
	if n := strings.Count(string(after), "\n  advertise: "); n != 1 {
		t.Errorf("effective advertise lines = %d, want 1:\n%s", n, after)
	}
}

// TestConfigFileSetAppendsWhenNoPlaceholder checks that a key is appended when
// its section has no matching placeholder line and the existing comment is kept.
func TestConfigFileSetAppendsWhenNoPlaceholder(t *testing.T) {
	body := "pairing:\n  # a comment that is not a placeholder\n  max_failures: 5\n"
	c, path := newValueCore(t, body, "")
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "pairing.max_attempts_per_minute", intValue(3))
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseConfigFile(after)
	if err != nil {
		t.Fatal(err)
	}
	if got := doc.values["pairing.max_attempts_per_minute"].Int; got != 3 {
		t.Errorf("pairing.max_attempts_per_minute = %d, want 3:\n%s", got, after)
	}
	if !strings.Contains(string(after), "# a comment that is not a placeholder") {
		t.Errorf("the existing comment was lost:\n%s", after)
	}
}

// TestConfigFileSetUpdatesEffectiveKeyInPlace checks that setting an existing
// key updates it in place and keeps the comment above it.
func TestConfigFileSetUpdatesEffectiveKeyInPlace(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  # public comment\n  public: false\n", "")
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.public", boolValue(true))
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(after), "public: true") {
		t.Errorf("public was not updated:\n%s", after)
	}
	if !strings.Contains(string(after), "# public comment") {
		t.Errorf("the comment above the key was lost:\n%s", after)
	}
	if n := strings.Count(string(after), "public:"); n != 1 {
		t.Errorf("public appears %d times, want 1:\n%s", n, after)
	}
}

// TestConfigFileSetCreatesMissingSection checks that setting a key whose
// section is absent creates the section.
func TestConfigFileSetCreatesMissingSection(t *testing.T) {
	c, path := newValueCore(t, "log:\n  level: info\n", "")
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "broker.public", boolValue(true))
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	doc, err := parseConfigFile(after)
	if err != nil {
		t.Fatal(err)
	}
	if !doc.values["broker.public"].Bool {
		t.Errorf("broker.public is missing after Set:\n%s", after)
	}
}

// TestConfigFileSetIgnoresPlaceholderInAnotherSection checks that the search
// for an inactive example line stays inside the key's own section.
func TestConfigFileSetIgnoresPlaceholderInAnotherSection(t *testing.T) {
	body := "send:\n  # level: debug\n  request_timeout: 20s\n"
	c, path := newValueCore(t, body, "")
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "log.level", stringValue("warn"))
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(after), "  # level: debug") {
		t.Errorf("the placeholder in the send section was consumed:\n%s", after)
	}
	doc, err := parseConfigFile(after)
	if err != nil {
		t.Fatal(err)
	}
	if got := doc.values["log.level"].String; got != "warn" {
		t.Errorf("log.level = %q, want warn:\n%s", got, after)
	}
}

// TestConfigFileSetPlaceholderSearchStopsAtSectionEnd checks that the
// placeholder search stops at the end of the key's own section.
func TestConfigFileSetPlaceholderSearchStopsAtSectionEnd(t *testing.T) {
	body := []byte("log:\n  level: info\nsend:\n  # level: debug\n  request_timeout: 20s\n")
	spec, _ := lookupKey("log.level")
	got, ok := replacePlaceholderLine(body, "log", "level", spec, stringValue("warn"))
	if ok {
		t.Fatalf("replacePlaceholderLine crossed the section end:\n%s", got)
	}
}

// TestConfigFileSetIgnoresExplanationComment checks that a description comment
// mentioning "<leaf>:" in the middle of its text is not mistaken for a
// commented placeholder line.
func TestConfigFileSetIgnoresExplanationComment(t *testing.T) {
	body := "log:\n  # See level: below for the runtime log level.\n  request_timeout: 20s\n"
	c, path := newValueCore(t, body, "")
	c.mu.Lock()
	_, err := c.setLocked(Actor{}, "log.level", stringValue("warn"))
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(after), "See level: below") {
		t.Errorf("the explanation comment was replaced:\n%s", after)
	}
	doc, err := parseConfigFile(after)
	if err != nil {
		t.Fatal(err)
	}
	if got := doc.values["log.level"].String; got != "warn" {
		t.Errorf("log.level = %q, want warn:\n%s", got, after)
	}
}

// TestConfigFileParseErrors checks that malformed, duplicate-key,
// multi-document and alias configurations are refused.
func TestConfigFileParseErrors(t *testing.T) {
	cases := []string{
		"broker: [\n",
		"broker: plain\n",
		"broker:\n  alias: a\n  alias: b\n",
		"broker:\n  alias: a\n---\nbroker:\n  alias: b\n",
		"base: &b\n  alias: a\nbroker:\n  <<: *b\n",
	}
	for _, body := range cases {
		if _, err := parseConfigFile([]byte(body)); err == nil {
			t.Errorf("expected a parse error for:\n%s", body)
		}
	}
}

// TestConfigFileCreateAndChmod checks that a new configuration file is created
// with mode 0600.
func TestConfigFileCreateAndChmod(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := writeConfigFile(path, []byte("broker:\n  alias: a\n"), ModeUser); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if perm := info.Mode().Perm(); perm != 0o600 {
		t.Errorf("mode = %o, want 600", perm)
	}
}

// TestConfigFileInPlacePreservesMetadata checks that an in-place rewrite keeps
// the inode, owner and mode.
func TestConfigFileInPlacePreservesMetadata(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte("broker:\n  alias: old\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	wantSys := before.Sys().(*syscall.Stat_t)
	if err := writeConfigFile(path, []byte("broker:\n  alias: new\n"), ModeUser); err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	gotSys := after.Sys().(*syscall.Stat_t)
	if gotSys.Ino != wantSys.Ino {
		t.Errorf("inode changed: %d -> %d", wantSys.Ino, gotSys.Ino)
	}
	if gotSys.Uid != wantSys.Uid || gotSys.Gid != wantSys.Gid {
		t.Errorf("owner changed: %d:%d -> %d:%d", wantSys.Uid, wantSys.Gid, gotSys.Uid, gotSys.Gid)
	}
	if perm := after.Mode().Perm(); perm != 0o640 {
		t.Errorf("mode = %o, want 640", perm)
	}
}

// TestREQBrokerConfig002_SetPreservesFileMetadata checks that Set keeps the
// configuration file's inode, owner and mode.
func TestREQBrokerConfig002_SetPreservesFileMetadata(t *testing.T) {
	c, path := newValueCore(t, "broker:\n  alias: old\n", "")
	if err := os.Chmod(path, 0o640); err != nil {
		t.Fatal(err)
	}
	before, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	wantSys := before.Sys().(*syscall.Stat_t)
	c.mu.Lock()
	_, err = c.setLocked(Actor{}, "broker.public", boolValue(true))
	c.mu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	after, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	gotSys := after.Sys().(*syscall.Stat_t)
	if gotSys.Ino != wantSys.Ino || gotSys.Uid != wantSys.Uid || gotSys.Gid != wantSys.Gid {
		t.Errorf("metadata changed: inode %d->%d uid %d->%d gid %d->%d",
			wantSys.Ino, gotSys.Ino, wantSys.Uid, gotSys.Uid, wantSys.Gid, gotSys.Gid)
	}
	if perm := after.Mode().Perm(); perm != 0o640 {
		t.Errorf("mode = %o, want 640", perm)
	}
}

// TestConfigFileWriteFailure checks that writing under a non-directory path
// returns an error.
func TestConfigFileWriteFailure(t *testing.T) {
	dir := t.TempDir()
	blocker := filepath.Join(dir, "not-a-dir")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	err := writeConfigFile(filepath.Join(blocker, "config.yaml"), []byte("x: y\n"), ModeUser)
	if err == nil {
		t.Fatal("expected a write error")
	}
}

// TestConfigFileNodeFor checks that nodeFor finds a present key's node and
// returns nil for an absent key.
func TestConfigFileNodeFor(t *testing.T) {
	doc, err := parseConfigFile([]byte("broker:\n  alias: a\n"))
	if err != nil {
		t.Fatal(err)
	}
	if n := doc.nodeFor("broker.alias"); n == nil || n.Value != "a" {
		t.Fatalf("nodeFor(alias) = %+v", n)
	}
	if n := doc.nodeFor("broker.public"); n != nil {
		t.Fatalf("nodeFor(public) = %+v, want nil", n)
	}
}
