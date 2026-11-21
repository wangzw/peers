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

// secretPrefix is the fixed prefix of a Claude Code OAuth token. Fragments are
// taken after it, so a token split or truncated at any point still matches
// through the bytes that remain.
const secretPrefix = "sk-ant-oat01-"

// fragmentLength is the length of the shortest run of token bytes searched for.
// Sixteen bytes are long enough not to collide with ordinary text and short
// enough to catch a token cut by a line break, a log truncation or a field
// boundary.
const fragmentLength = 16

func init() {
	register("scan-secret", cmdScanSecret)
}

func cmdScanSecret(args []string) error {
	return runScanSecret(args, os.Stdout)
}

// runScanSecret implements "rsprobe scan-secret --dir D --env VAR". A hit
// prints each matching file and returns exit code 1; a clean tree returns 0.
// An argument or environment problem, or a scan that cannot complete because a
// directory or file cannot be read, returns 2: the caller must treat a scan
// that did not run as a failure, never as a clean tree.
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

// scanSecretDir returns the regular files below dir that contain the token or
// any transformation of it in secretPatterns. A symbolic link or any other
// non-regular entry is an error, not something to skip: an entry the scan
// cannot read is evidence the scan did not cover, so the caller must fail
// closed rather than upload it unchecked.
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

// secretPatterns returns the byte strings whose presence in a file means the
// token leaked. Besides the whole token they cover
//
//   - every fragmentLength-byte run after the fixed prefix, which catches a
//     token wrapped by a line break or cut short;
//   - the standard and URL-safe base64 encodings of the token at each of the
//     three byte alignments, without padding, which catches the token embedded
//     in another value;
//   - the query and path URL encodings.
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
			// Only the groups of three whole bytes encode the same way no
			// matter what surrounds them. A trailing partial group is merged
			// with whatever follows the token in the enclosing value, so keep
			// just the complete groups, a multiple of four characters.
			add([]byte(encoded[:len(encoded)/4*4]))
		}
	}

	add([]byte(url.QueryEscape(token)))
	add([]byte(url.PathEscape(token)))
	return patterns
}
