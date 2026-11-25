//go:build linux

package accountdb

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode"
)

const getentTimeout = 5 * time.Second

// getentWait is the bound getentQuery applies; production uses getentTimeout
// and tests shorten it to exercise the timeout path.
var getentWait = getentTimeout

const getentOutputLimit = 64 << 10

// getentWaitDelay bounds how long Wait waits for the output pipes after the
// process exits or the context is done; a lingering grandchild that inherited
// the pipe must not defeat the timeout.
const getentWaitDelay = 2 * time.Second

// getentPipeWait is the pipe wait execGetent applies; production uses
// getentWaitDelay, and LookupBound reads that constant rather than this test
// seam.
var getentPipeWait = getentWaitDelay

// LookupBound is the upper bound one account lookup can take in production:
// one getent invocation plus the pipe wait after a timeout. Callers sizing a
// wait around a lookup derive it from this.
const LookupBound = getentTimeout + getentWaitDelay

// getentPaths are the absolute paths probed for getent, in order; PATH is
// never consulted, so a Broker running as root cannot execute a getent found
// through the environment.
var getentPaths = []string{"/usr/bin/getent", "/bin/getent"}

type getentResult struct {
	stdout   []byte
	stderr   []byte
	exitCode int
}

// runGetent is the executor seam; production uses execGetent and tests replace
// it.
var runGetent = execGetent

var errGetentMissing = errors.New("getent executable not found")

var errGetentTimeout = errors.New("getent timed out")

var errGetentOutput = errors.New("getent output limit exceeded")

// cappedBuffer keeps at most limit bytes and records whether more arrived.
// Beyond the limit it keeps draining so the child cannot block on a full pipe,
// reporting the overflow after Wait.
type cappedBuffer struct {
	buf      bytes.Buffer
	limit    int
	overflow bool
}

func (w *cappedBuffer) Write(p []byte) (int, error) {
	if !w.overflow && w.buf.Len()+len(p) > w.limit {
		w.overflow = true
	}
	if w.overflow {
		return len(p), nil
	}
	return w.buf.Write(p)
}

var (
	lookupUID   = getentLookupUID
	lookupName  = getentLookupName
	lookupGroup = getentLookupGroup
	groupIDs    = getentGroupIDs
)

func getentLookupUID(uid uint32) (User, error) {
	id := strconv.FormatUint(uint64(uid), 10)
	out, err := getentQuery("passwd", "--", id)
	if err == nil {
		u, parseErr := parsePasswd(out)
		switch {
		case parseErr != nil:
			err = parseErr
		case u.UID != uid:
			err = fmt.Errorf("getent returned uid %d for uid %d", u.UID, uid)
		default:
			return u, nil
		}
	}
	return lookupResult("uid "+id, err, func() (User, error) { return osUserLookupUID(uid) })
}

func getentLookupName(name string) (User, error) {
	if err := validateName(name); err != nil {
		return User{}, err
	}
	out, err := getentQuery("passwd", "--", name)
	if err == nil {
		u, parseErr := parsePasswd(out)
		switch {
		case parseErr != nil:
			err = parseErr
		case u.Name != name:
			err = fmt.Errorf("getent returned name %q for name %q", u.Name, name)
		default:
			return u, nil
		}
	}
	return lookupResult("name "+name, err, func() (User, error) { return osUserLookupName(name) })
}

func getentLookupGroup(name string) (uint32, error) {
	if err := validateName(name); err != nil {
		return 0, err
	}
	out, err := getentQuery("group", "--", name)
	if err == nil {
		gotName, gid, parseErr := parseGroup(out)
		switch {
		case parseErr != nil:
			err = parseErr
		case gotName != name:
			err = fmt.Errorf("getent returned group %q for group %q", gotName, name)
		default:
			return gid, nil
		}
	}
	return lookupResult("group "+name, err, func() (uint32, error) { return osUserLookupGroup(name) })
}

func getentGroupIDs(u User) ([]uint32, error) {
	if err := validateName(u.Name); err != nil {
		return nil, err
	}
	out, err := getentQuery("initgroups", "--", u.Name)
	if err == nil {
		ids, parseErr := parseInitgroups(out, u.Name)
		if parseErr == nil {
			return normalizeGroupIDs(u.GID, ids), nil
		}
		err = parseErr
	}
	if errors.Is(err, ErrNotFound) {
		return nil, ErrNotFound
	}
	warnFallback("initgroups "+u.Name, err)
	// The local source may answer for a different account with the same name;
	// osUserGroupIDs checks the uid, and a failed check or lookup stays a
	// temporary failure, never ErrNotFound.
	if ids, fallbackErr := osUserGroupIDs(u); fallbackErr == nil {
		return ids, nil
	}
	return nil, fmt.Errorf("%w: %w", errUnavailable, err)
}

func lookupResult[T any](request string, err error, fallback func() (T, error)) (T, error) {
	var zero T
	if errors.Is(err, ErrNotFound) {
		return zero, ErrNotFound
	}
	warnFallback(request, err)
	value, fallbackErr := fallback()
	if fallbackErr == nil {
		return value, nil
	}
	return zero, unavailableAfterFallback(err, fallbackErr)
}

func unavailableAfterFallback(transient, fallbackErr error) error {
	if errors.Is(fallbackErr, ErrNotFound) {
		return fmt.Errorf("%w: %v", errUnavailable, transient)
	}
	return fallbackErr
}

// getentQuery runs getent with args and returns its standard output. The
// caller passes "--" before a name or uid so a key starting with a dash cannot
// become an option.
func getentQuery(args ...string) ([]byte, error) {
	path, ok := getentExecutable()
	if !ok {
		return nil, errGetentMissing
	}
	ctx, cancel := context.WithTimeout(context.Background(), getentWait)
	defer cancel()

	res, err := runGetent(ctx, path, args)
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) || ctx.Err() != nil {
			return nil, fmt.Errorf("%w after %s", errGetentTimeout, getentWait)
		}
		return nil, fmt.Errorf("running %s: %w", path, err)
	}
	switch res.exitCode {
	case 0:
		return res.stdout, nil
	case 2:
		// getent uses exit status 2 for "no such key".
		return nil, ErrNotFound
	default:
		message := strings.TrimSpace(string(res.stderr))
		if message == "" {
			message = strings.TrimSpace(string(res.stdout))
		}
		return nil, fmt.Errorf("getent %s exited with status %d: %s",
			strings.Join(args, " "), res.exitCode, message)
	}
}

func getentExecutable() (string, bool) {
	for _, path := range getentPaths {
		if !strings.HasPrefix(path, "/") {
			continue
		}
		fi, err := os.Stat(path)
		if err != nil || fi.IsDir() || fi.Mode().Perm()&0o111 == 0 {
			continue
		}
		return path, true
	}
	return "", false
}

func execGetent(ctx context.Context, path string, args []string) (getentResult, error) {
	cmd := exec.CommandContext(ctx, path, args...)
	cmd.Env = []string{"LANG=C"}
	cmd.WaitDelay = getentPipeWait
	stdout := cappedBuffer{limit: getentOutputLimit}
	stderr := cappedBuffer{limit: getentOutputLimit}
	cmd.Stdout = &stdout
	cmd.Stderr = &stderr

	err := cmd.Run()
	res := getentResult{stdout: stdout.buf.Bytes(), stderr: stderr.buf.Bytes()}
	// A killed process yields an *exec.ExitError with status -1, so the
	// context is checked first to report a timeout as one.
	if ctx.Err() != nil {
		return res, ctx.Err()
	}
	if stdout.overflow || stderr.overflow {
		return res, fmt.Errorf("%w (limit %d bytes)", errGetentOutput, getentOutputLimit)
	}
	if err == nil {
		return res, nil
	}
	var exitErr *exec.ExitError
	if errors.As(err, &exitErr) {
		res.exitCode = exitErr.ExitCode()
		return res, nil
	}
	return res, err
}

// errUnavailable reports a lookup failure other than a missing entry: the
// database could not be read or an answer could not be trusted, and callers
// must treat it as a failed check, not a missing account.
var errUnavailable = errors.New("account database unavailable")

func validateName(name string) error {
	if name == "" {
		return errors.New("cannot use an empty account name")
	}
	if strings.ContainsAny(name, ":\x00") || strings.IndexFunc(name, unicode.IsSpace) >= 0 {
		return fmt.Errorf("cannot use account name %q: it is invalid", name)
	}
	return nil
}

func oneLine(out []byte) (string, error) {
	s := string(out)
	line := strings.TrimSuffix(s, "\n")
	if line == "" {
		return "", errors.New("empty output")
	}
	if strings.ContainsAny(line, "\r\n") {
		return "", fmt.Errorf("output has %d lines, want one", strings.Count(line, "\n")+1)
	}
	return line, nil
}

func parsePasswd(out []byte) (User, error) {
	line, err := oneLine(out)
	if err != nil {
		return User{}, fmt.Errorf("passwd entry: %w", err)
	}
	fields := strings.Split(line, ":")
	if len(fields) != 7 {
		return User{}, fmt.Errorf("passwd entry has %d fields, want 7", len(fields))
	}
	if err := validateName(fields[0]); err != nil {
		return User{}, err
	}
	uid, err := parseDecimal32(fields[2])
	if err != nil {
		return User{}, fmt.Errorf("passwd entry uid: %w", err)
	}
	gid, err := parseDecimal32(fields[3])
	if err != nil {
		return User{}, fmt.Errorf("passwd entry gid: %w", err)
	}
	home := fields[5]
	if !filepath.IsAbs(home) {
		return User{}, fmt.Errorf("passwd entry home %q is not absolute", home)
	}
	return User{Name: fields[0], UID: uid, GID: gid, Home: home}, nil
}

func parseGroup(out []byte) (string, uint32, error) {
	line, err := oneLine(out)
	if err != nil {
		return "", 0, fmt.Errorf("group entry: %w", err)
	}
	fields := strings.Split(line, ":")
	if len(fields) != 4 {
		return "", 0, fmt.Errorf("group entry has %d fields, want 4", len(fields))
	}
	name := fields[0]
	if err := validateName(name); err != nil {
		return "", 0, err
	}
	gid, err := parseDecimal32(fields[2])
	if err != nil {
		return "", 0, fmt.Errorf("group entry gid: %w", err)
	}
	return name, gid, nil
}

func parseInitgroups(out []byte, name string) ([]uint32, error) {
	line, err := oneLine(out)
	if err != nil {
		return nil, fmt.Errorf("initgroups entry: %w", err)
	}
	fields := strings.Fields(line)
	if len(fields) == 0 || fields[0] != name {
		return nil, fmt.Errorf("initgroups entry does not start with %q", name)
	}
	ids := make([]uint32, 0, len(fields)-1)
	for _, field := range fields[1:] {
		id, err := parseDecimal32(field)
		if err != nil {
			return nil, fmt.Errorf("initgroups entry: %w", err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// warnFallback records that the preferred lookup failed and os/user is used
// instead.
func warnFallback(request string, err error) {
	slog.Warn("accountdb_lookup_fallback", "request", request, "error", err.Error())
}
