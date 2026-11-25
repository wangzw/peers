package main

import (
	"bytes"
	"encoding/base64"
	"errors"
	"flag"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path/filepath"
	"strings"
)

// secretPrefix is the fixed prefix of a Claude Code OAuth token; fragments are
// taken after it.
const secretPrefix = "sk-ant-oat01-"

// fragmentLength is the shortest run of token bytes searched for: long enough
// not to collide with ordinary text, short enough to catch a token split by a
// line break or field boundary.
const fragmentLength = 16

func init() {
	register("scan-secret", cmdScanSecret)
}

func cmdScanSecret(args []string) error {
	return runScanSecret(args, os.Stdout)
}

// runScanSecret scans a directory for the token: a hit prints each file and
// exits 1, a clean tree exits 0. A scan that could not run exits 2, so the
// caller never mistakes it for a clean tree.
func runScanSecret(args []string, stdout io.Writer) error {
	flags := flag.NewFlagSet("scan-secret", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	dir := flags.String("dir", "", "directory to scan")
	env := flags.String("env", "", "environment variable holding the token")
	if err := flags.Parse(args); err != nil {
		return &exitError{code: 2, err: err}
	}
	if flags.NArg() > 0 {
		return &exitError{code: 2, err: fmt.Errorf("unexpected argument %q", flags.Arg(0))}
	}
	if *dir == "" {
		return &exitError{code: 2, err: errors.New("--dir is required")}
	}
	if *env == "" {
		return &exitError{code: 2, err: errors.New("--env is required")}
	}
	token := os.Getenv(*env)
	if token == "" {
		return &exitError{code: 2, err: fmt.Errorf("the environment variable %s is empty or unset", *env)}
	}

	hits, err := scanSecretDir(*dir, token)
	if err != nil {
		return &exitError{code: 2, err: err}
	}
	if len(hits) == 0 {
		return nil
	}
	for _, hit := range hits {
		_, _ = fmt.Fprintln(stdout, hit)
	}
	return &exitError{code: 1, err: errors.New("the directory contains the token")}
}

// scanSecretDir returns the regular files containing the token or one of its
// encodings; a non-regular entry is an error, not skipped, so the caller fails
// closed rather than uploads it unchecked.
func scanSecretDir(dir, token string) ([]string, error) {
	patterns := secretPatterns(token)
	var hits []string
	err := filepath.WalkDir(dir, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		if !d.Type().IsRegular() {
			return fmt.Errorf("%s is not a regular file", path)
		}
		data, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		for _, pattern := range patterns {
			if bytes.Contains(data, pattern) {
				hits = append(hits, path)
				break
			}
		}
		return nil
	})
	if err != nil {
		return nil, fmt.Errorf("cannot scan %s: %w", dir, err)
	}
	return hits, nil
}

// secretPatterns returns the byte strings whose presence means the token
// leaked: the whole token, every fragmentLength-byte fragment after the prefix,
// the standard and URL-safe base64 encodings at each of the three byte
// alignments without padding, and the query and path URL encodings.
func secretPatterns(token string) [][]byte {
	seen := map[string]bool{}
	var patterns [][]byte
	add := func(pattern []byte) {
		if len(pattern) == 0 || seen[string(pattern)] {
			return
		}
		seen[string(pattern)] = true
		patterns = append(patterns, pattern)
	}

	add([]byte(token))

	body := strings.TrimPrefix(token, secretPrefix)
	for i := 0; i+fragmentLength <= len(body); i++ {
		add([]byte(body[i : i+fragmentLength]))
	}

	for offset := 0; offset < 3 && offset < len(token); offset++ {
		raw := []byte(token[offset:])
		for _, encoding := range []*base64.Encoding{base64.RawStdEncoding, base64.RawURLEncoding} {
			encoded := encoding.EncodeToString(raw)
			// Only complete groups of three bytes encode the same regardless of
			// what surrounds them; a trailing partial group merges with whatever
			// follows the token, so keep a multiple of four characters.
			add([]byte(encoded[:len(encoded)/4*4]))
		}
	}

	add([]byte(url.QueryEscape(token)))
	add([]byte(url.PathEscape(token)))
	return patterns
}
