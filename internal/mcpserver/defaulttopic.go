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

const gitOriginTimeout = 5 * time.Second

type originReader func(ctx context.Context, dir string) (string, error)

// gitOrigin reads the local remote.origin.url; --local keeps a global one out of
// the result.
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

type defaultLookup struct {
	topics             []string
	projectAutoJoinOff bool
	// settled is false when the lookup failed for a reason that does not decide
	// the answer, such as a git command that timed out; such a result must not
	// be cached.
	settled bool
}

func automaticTopics(res defaultLookup, brokerAutoJoin bool) []string {
	if !brokerAutoJoin || res.projectAutoJoinOff {
		return nil
	}
	return res.topics
}

func lookupDefaultTopics(ctx context.Context, dir string, readOrigin originReader, log *slog.Logger) defaultLookup {
	if log == nil {
		log = slog.Default()
	}
	cfg, found, err := findProjectConfigLogged(dir, log)
	if err != nil {
		log.Error("cannot read the project configuration; not joining any default topic",
			"path", cfg.Path, "error", err)
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
		log.Debug("no usable git origin; not joining a default topic", "dir", dir, "error", err)
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

// sessionTopicList caches a settled result for the process's life; leaving a
// topic does not remove it from the list.
type sessionTopicList struct {
	mu      sync.Mutex
	result  defaultLookup
	compute func(context.Context) defaultLookup
}

// lookup returns the session's default topics, computing them on the first
// call; a computation whose context ended or did not settle is returned but
// not kept.
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

func (s *sessionTopicList) get(ctx context.Context) []string {
	return s.lookup(ctx).topics
}
