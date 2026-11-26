package mcpserver

import (
	"bytes"
	"context"
	"errors"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func defaultTopicsLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func defaultTopicsFailReader(t *testing.T) originReader {
	t.Helper()
	return func(context.Context, string) (string, error) {
		t.Errorf("readOrigin must not be called")
		return "", errors.New("unexpected call")
	}
}

func automaticForTest(ctx context.Context, dir string, brokerAutoJoin bool, readOrigin originReader, log *slog.Logger) []string {
	return automaticTopics(lookupDefaultTopics(ctx, dir, readOrigin, log), brokerAutoJoin)
}

func sessionForTest(ctx context.Context, dir string, readOrigin originReader, log *slog.Logger) []string {
	return lookupDefaultTopics(ctx, dir, readOrigin, log).topics
}

func defaultTopicsEqual(got, want []string) bool {
	if len(got) != len(want) {
		return false
	}
	for i := range got {
		if got[i] != want[i] {
			return false
		}
	}
	return true
}

// TestAutomaticTopics checks that automatic joining applies the Broker and
// project switches to the default topics.
func TestAutomaticTopics(t *testing.T) {
	ctx := context.Background()

	t.Run("broker switched automatic joining off", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "topics: [a]\n")
		got := automaticForTest(ctx, dir, false, defaultTopicsFailReader(t), defaultTopicsLogger())
		if !defaultTopicsEqual(got, nil) {
			t.Fatalf("defaultTopics = %v, want none", got)
		}
	})

	t.Run("project file switched automatic joining off", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "auto_join: false\ntopics: [a]\n")
		got := automaticForTest(ctx, dir, true, defaultTopicsFailReader(t), defaultTopicsLogger())
		if !defaultTopicsEqual(got, nil) {
			t.Fatalf("defaultTopics = %v, want none", got)
		}
	})

	t.Run("project file names the topics", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "topics: [a, b]\n")
		got := automaticForTest(ctx, dir, true, defaultTopicsFailReader(t), defaultTopicsLogger())
		if !defaultTopicsEqual(got, []string{"a", "b"}) {
			t.Fatalf("defaultTopics = %v, want [a b]", got)
		}
	})

	t.Run("git origin when no project file exists", func(t *testing.T) {
		dir := t.TempDir()
		readOrigin := func(context.Context, string) (string, error) {
			return "https://github.com/o/r", nil
		}
		got := automaticForTest(ctx, dir, true, readOrigin, defaultTopicsLogger())
		if !defaultTopicsEqual(got, []string{"github.com/o/r"}) {
			t.Fatalf("defaultTopics = %v, want [github.com/o/r]", got)
		}
	})

	t.Run("project file without topics falls through to the origin", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "auto_join: true\n")
		readOrigin := func(context.Context, string) (string, error) {
			return "git@github.com:o/r.git", nil
		}
		got := automaticForTest(ctx, dir, true, readOrigin, defaultTopicsLogger())
		if !defaultTopicsEqual(got, []string{"github.com/o/r"}) {
			t.Fatalf("defaultTopics = %v, want [github.com/o/r]", got)
		}
	})

	t.Run("no git origin", func(t *testing.T) {
		dir := t.TempDir()
		readOrigin := func(context.Context, string) (string, error) {
			return "", errors.New("not a git repository")
		}
		got := automaticForTest(ctx, dir, true, readOrigin, defaultTopicsLogger())
		if !defaultTopicsEqual(got, nil) {
			t.Fatalf("defaultTopics = %v, want none", got)
		}
	})

	t.Run("origin is a local path", func(t *testing.T) {
		dir := t.TempDir()
		readOrigin := func(context.Context, string) (string, error) {
			return "/srv/repo.git", nil
		}
		got := automaticForTest(ctx, dir, true, readOrigin, defaultTopicsLogger())
		if !defaultTopicsEqual(got, nil) {
			t.Fatalf("defaultTopics = %v, want none", got)
		}
	})

	t.Run("invalid project file logs and joins nothing", func(t *testing.T) {
		dir := t.TempDir()
		path := writeProjectFile(t, dir, "topics: x\n")
		var buf bytes.Buffer
		log := slog.New(slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug}))
		got := automaticForTest(ctx, dir, true, defaultTopicsFailReader(t), log)
		if !defaultTopicsEqual(got, nil) {
			t.Fatalf("defaultTopics = %v, want none", got)
		}
		text := buf.String()
		if !strings.Contains(text, "level=ERROR") {
			t.Fatalf("log = %q, want an error line", text)
		}
		if !strings.Contains(text, path) {
			t.Fatalf("log = %q, want the project file path %q", text, path)
		}
	})
}

// TestNamelessDefaultTopics checks that a nameless topic call still uses the
// project topics or the origin when the project file sets auto_join to false.
func TestNamelessDefaultTopics(t *testing.T) {
	ctx := context.Background()

	t.Run("project auto_join off still names its topics", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "auto_join: false\ntopics: [a, b]\n")
		got := sessionForTest(ctx, dir, defaultTopicsFailReader(t), defaultTopicsLogger())
		if !defaultTopicsEqual(got, []string{"a", "b"}) {
			t.Fatalf("a nameless call got %v, want [a b]", got)
		}
	})

	t.Run("project auto_join off falls through to the origin", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "auto_join: false\n")
		readOrigin := func(context.Context, string) (string, error) {
			return "https://github.com/o/r", nil
		}
		got := sessionForTest(ctx, dir, readOrigin, defaultTopicsLogger())
		if !defaultTopicsEqual(got, []string{"github.com/o/r"}) {
			t.Fatalf("a nameless call got %v, want [github.com/o/r]", got)
		}
	})

	t.Run("project topics are used before the origin", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "topics: [a]\n")
		got := sessionForTest(ctx, dir, defaultTopicsFailReader(t), defaultTopicsLogger())
		if !defaultTopicsEqual(got, []string{"a"}) {
			t.Fatalf("a nameless call got %v, want [a]", got)
		}
	})

	t.Run("invalid project file yields no topic", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "topics: x\n")
		got := sessionForTest(ctx, dir, defaultTopicsFailReader(t), defaultTopicsLogger())
		if !defaultTopicsEqual(got, nil) {
			t.Fatalf("a nameless call got %v, want none", got)
		}
	})

	t.Run("matches automatic joining when both switches are on", func(t *testing.T) {
		dir := t.TempDir()
		writeProjectFile(t, dir, "topics: [a, b]\n")
		auto := automaticForTest(ctx, dir, true, defaultTopicsFailReader(t), defaultTopicsLogger())
		session := sessionForTest(ctx, dir, defaultTopicsFailReader(t), defaultTopicsLogger())
		if !defaultTopicsEqual(auto, session) {
			t.Fatalf("automatic joining got %v, a nameless call got %v, want the same list", auto, session)
		}
	})
}

// TestLookupDefaultTopicsSettled checks that a timed-out or cancelled origin
// lookup stays unsettled while another failure settles the answer.
func TestLookupDefaultTopicsSettled(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name    string
		err     error
		settled bool
	}{
		{"timeout", context.DeadlineExceeded, false},
		{"cancelled", context.Canceled, false},
		{"no origin", errors.New("not a git repository"), true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir := t.TempDir()
			readOrigin := func(context.Context, string) (string, error) { return "", tc.err }
			got := lookupDefaultTopics(ctx, dir, readOrigin, defaultTopicsLogger())
			if got.settled != tc.settled {
				t.Fatalf("settled = %v, want %v", got.settled, tc.settled)
			}
			if len(got.topics) != 0 {
				t.Fatalf("topics = %v, want none", got.topics)
			}
		})
	}
}

// TestSessionTopicList checks that a settled computation is cached and an
// unsettled one is computed again.
func TestSessionTopicList(t *testing.T) {
	t.Run("computes once and keeps the result", func(t *testing.T) {
		calls := 0
		list := &sessionTopicList{compute: func(context.Context) defaultLookup {
			calls++
			return defaultLookup{topics: []string{"a"}, settled: true}
		}}
		for i := 0; i < 3; i++ {
			if got := list.get(context.Background()); !defaultTopicsEqual(got, []string{"a"}) {
				t.Fatalf("call %d = %v, want [a]", i, got)
			}
		}
		if calls != 1 {
			t.Fatalf("compute ran %d times, want 1", calls)
		}
	})

	t.Run("does not keep a computation whose context ended", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		calls := 0
		list := &sessionTopicList{compute: func(context.Context) defaultLookup {
			calls++
			return defaultLookup{settled: false}
		}}
		list.get(ctx)
		if calls != 1 {
			t.Fatalf("compute ran %d times, want 1", calls)
		}
		list.get(context.Background())
		if calls != 2 {
			t.Fatalf("compute ran %d times after an interrupted call, want 2", calls)
		}
	})

	t.Run("does not keep a result that did not settle", func(t *testing.T) {
		calls := 0
		list := &sessionTopicList{compute: func(context.Context) defaultLookup {
			calls++
			if calls == 1 {
				return defaultLookup{settled: false}
			}
			return defaultLookup{topics: []string{"a"}, settled: true}
		}}
		if got := list.get(context.Background()); len(got) != 0 {
			t.Fatalf("first get = %v, want none", got)
		}
		if got := list.get(context.Background()); !defaultTopicsEqual(got, []string{"a"}) {
			t.Fatalf("second get = %v, want [a]", got)
		}
		if calls != 2 {
			t.Fatalf("compute ran %d times, want 2", calls)
		}
		if got := list.get(context.Background()); !defaultTopicsEqual(got, []string{"a"}) {
			t.Fatalf("third get = %v, want the settled [a]", got)
		}
		if calls != 2 {
			t.Fatalf("compute ran %d times after a settled result, want 2", calls)
		}
	})
}

func gitOriginGit(t *testing.T, dir string, args ...string) {
	t.Helper()
	full := append([]string{"-C", dir}, args...)
	cmd := exec.Command("git", full...)
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// TestGitOrigin checks that gitOrigin reads the origin from a subdirectory and
// fails outside a repository without reading the global configuration.
func TestGitOrigin(t *testing.T) {
	if _, err := exec.LookPath("git"); err != nil {
		t.Skip("git is not installed")
	}
	ctx := context.Background()

	repo := t.TempDir()
	gitOriginGit(t, repo, "init")
	gitOriginGit(t, repo, "remote", "add", "origin", "git@h:o/r.git")
	sub := filepath.Join(repo, "a", "b")
	if err := os.MkdirAll(sub, 0o755); err != nil {
		t.Fatalf("create %s: %v", sub, err)
	}
	got, err := gitOrigin(ctx, sub)
	if err != nil {
		t.Fatalf("gitOrigin in a subdirectory: %v", err)
	}
	if got != "git@h:o/r.git" {
		t.Fatalf("gitOrigin = %q, want %q", got, "git@h:o/r.git")
	}

	notRepo := t.TempDir()
	if _, err := gitOrigin(ctx, notRepo); err == nil {
		t.Fatal("gitOrigin outside a repository must fail")
	}

	global := filepath.Join(t.TempDir(), "gitconfig")
	if err := os.WriteFile(global, []byte("[remote \"origin\"]\n\turl = x\n"), 0o644); err != nil {
		t.Fatalf("write global config: %v", err)
	}
	t.Setenv("GIT_CONFIG_GLOBAL", global)
	if _, err := gitOrigin(ctx, notRepo); err == nil {
		t.Fatal("gitOrigin must not read remote.origin.url from the global config")
	}
}
