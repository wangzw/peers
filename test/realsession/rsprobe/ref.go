package main

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

func init() {
	register("ref", cmdRef)
}

// errBadSocketPath marks a path broker.AgentIDFor rejects: empty, relative, or
// containing a dot-dot element.
var errBadSocketPath = errors.New("inbox socket path must be absolute and must not contain a dot-dot element")

// cmdRef implements the ref subcommand; a bad path is a usage error, so it
// exits 2 rather than the generic 1.
func cmdRef(args []string) error {
	return runRefCommand(args, os.Stdout, os.Stderr, os.Exit)
}

// runRefCommand exists so tests can observe the exit code without ending the
// test process.
func runRefCommand(args []string, stdout, stderr io.Writer, exit func(int)) error {
	if err := runRef(args, stdout); err != nil {
		_, _ = fmt.Fprintf(stderr, "rsprobe: %v\n", err)
		exit(2)
	}
	return nil
}

// agentID derives the same identifier as broker.AgentIDFor: it rejects the same
// paths and hashes "session:" plus the cleaned path.
func agentID(socketPath string) (string, error) {
	if socketPath == "" || !filepath.IsAbs(socketPath) {
		return "", errBadSocketPath
	}
	for _, elem := range strings.Split(socketPath, "/") {
		if elem == ".." {
			return "", errBadSocketPath
		}
	}
	sum := sha256.Sum256([]byte("session:" + filepath.Clean(socketPath)))
	return hex.EncodeToString(sum[:]), nil
}

// shortRefs returns each id's disambiguator: the shortest unique prefix, 6 to
// 12 characters, or 12 when the first 12 collide.
func shortRefs(ids []string) map[string]string {
	refs := make(map[string]string, len(ids))
	for _, id := range ids {
		limit := len(id)
		if limit > 12 {
			limit = 12
		}
		length := limit
		for l := 6; l <= limit; l++ {
			collides := false
			for _, other := range ids {
				if other == id || len(other) < l {
					continue
				}
				if id[:l] == other[:l] {
					collides = true
					break
				}
			}
			if !collides {
				length = l
				break
			}
		}
		refs[id] = id[:length]
	}
	return refs
}

// runRef prints one line per path; every path is validated before anything is
// printed, so one bad argument yields no partial output.
func runRef(args []string, stdout io.Writer) error {
	if len(args) == 0 {
		return errors.New("usage: rsprobe ref <socket path>")
	}
	ids := make([]string, len(args))
	for i, path := range args {
		id, err := agentID(path)
		if err != nil {
			return fmt.Errorf("%s: %w", path, err)
		}
		ids[i] = id
	}
	refs := shortRefs(ids)
	for i, path := range args {
		head := ids[i]
		if len(head) > 12 {
			head = head[:12]
		}
		if _, err := fmt.Fprintf(stdout, "%s %s %s\n", path, head, refs[ids[i]]); err != nil {
			return err
		}
	}
	return nil
}
