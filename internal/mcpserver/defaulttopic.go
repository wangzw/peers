package mcpserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os/exec"
	"strings"
	"sync"
	"time"
)

// gitOriginTimeout bounds the git command that reads the origin URL.
const gitOriginTimeout = 5 * time.Second

// originReader reads the git origin URL of the repository that contains dir.
// An error means there is no usable origin, for example because dir is not in a
// git repository or the repository has none.
type originReader func(ctx context.Context, dir string) (string, error)

// gitOrigin runs `git -C dir config --local --get remote.origin.url` with a 5s
// deadline. It reports an error when the directory is not a git repository,
// when the repository has no origin, when git is not installed, or when the
// command takes too long.
//
// The --local flag is deliberate: without it git would also read
// remote.origin.url from the user's global configuration, which is not a
// property of the repository.
func gitOrigin(ctx context.Context, dir string) (string, error) {
	ctx, cancel := context.WithTimeout(ctx, gitOriginTimeout)
	defer cancel()

	cmd := exec.CommandContext(ctx, "git", "-C", dir, "config", "--local", "--get", "remote.origin.url")
	var stdout, stderr bytes.Buffer
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr
	if err := cmd.Run(); err != nil {
		detail := ctx.Err()
		if detail == nil {
			if text := strings.TrimSpace(stderr.String()); text != "" {
				detail = fmt.Errorf("%w: %s", err, text)
			} else {
				detail = err
			}
		}
		return "", fmt.Errorf("cannot read the git origin in %s: %w", dir, detail)
	}
	url := strings.TrimSpace(stdout.String())
	if url == "" {
		return "", fmt.Errorf("cannot read the git origin in %s: empty remote.origin.url", dir)
	}
	return url, nil
}

// defaultLookup is one read of the project configuration and the git origin.
// topics holds the result of steps 3 to 5 of the design's "默认 topic 的确定";
// projectAutoJoinOff records step 2. settled is false when the lookup failed
// for a reason that does not decide the answer, such as a git command that
// timed out; a result that is not settled must not be cached.
type defaultLookup struct {
	topics             []string
	projectAutoJoinOff bool
	settled            bool
}

// automaticTopics applies the two automatic-joining switches to a lookup: the
// Broker's setting is step 1 of the design's "默认 topic 的确定" and the project
// file's setting is step 2. A nameless topic call skips this function, because
// an explicit request ignores both switches.
func automaticTopics(res defaultLookup, brokerAutoJoin bool) []string {
	if !brokerAutoJoin || res.projectAutoJoinOff {
		return nil
	}
	return res.topics
}

// lookupDefaultTopics reads the project configuration and, when it names no
// topics, the git origin. It records step 2 of the design's "默认 topic 的
// 确定" separately from the topics of steps 3 to 5, because automatic joining
// honors the switch while a nameless topic call ignores it.
func lookupDefaultTopics(ctx context.Context, dir string, readOrigin originReader, log *slog.Logger) defaultLookup {
	if log == nil {
		log = slog.Default()
	}
	cfg, found, err := findProjectConfigLogged(dir, log)
	if err != nil {
		log.Error("cannot read the project configuration; not joining any default topic",
			"path", cfg.Path, "error", err)
		// A file that exists but cannot be read or parsed decides the answer:
		// the design makes such a project join nothing.
		return defaultLookup{settled: true}
	}
	out := defaultLookup{projectAutoJoinOff: found && !cfg.AutoJoin}
	if found && len(cfg.Topics) > 0 {
		out.topics = cfg.Topics
		out.settled = true
		return out
	}

	raw, err := readOrigin(ctx, dir)
	if err != nil {
		// Being outside a git repository or having no origin is ordinary, so
		// this is a debug message rather than an error.
		log.Debug("no usable git origin; not joining a default topic", "dir", dir, "error", err)
		// A timed-out or cancelled git command leaves the question open: it may
		// have an origin that the command did not get to report. Any other
		// error means there is no origin to find.
		out.settled = !errors.Is(err, context.DeadlineExceeded) && !errors.Is(err, context.Canceled)
		return out
	}
	topic, ok := normalizeRemote(raw)
	if !ok {
		log.Debug("the git origin does not name a remote repository; not joining a default topic",
			"dir", dir, "origin", raw)
		out.settled = true
		return out
	}
	out.topics = []string{topic}
	out.settled = true
	return out
}

// sessionTopicList computes the session's default topics at most once and keeps
// the result for the life of the process. The first registration and every
// nameless join_topic or leave_topic read the same result, so a topic that was
// joined automatically can always be left by name later; leaving a topic does
// not remove it from the list.
type sessionTopicList struct {
	mu sync.Mutex
	// result.settled marks a cached answer; a lookup that did not settle is
	// never stored.
	result  defaultLookup
	compute func(context.Context) defaultLookup
}

// lookup returns the session's default topics and the switches that apply to
// automatic joining, computing them on the first call. A computation whose
// context ended, or one that did not settle the answer, is returned but not
// kept: the git lookup may have failed because the call was cancelled or timed
// out, and a later call should be able to determine the list again.
func (s *sessionTopicList) lookup(ctx context.Context) defaultLookup {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.result.settled {
		return s.result
	}
	got := s.compute(ctx)
	if ctx.Err() != nil || !got.settled {
		return got
	}
	s.result = got
	return got
}

// get returns the session's default topics.
func (s *sessionTopicList) get(ctx context.Context) []string {
	return s.lookup(ctx).topics
}
