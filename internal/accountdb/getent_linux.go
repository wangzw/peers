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

// getentTimeout bounds one getent invocation from start to result. The account
// database can sit behind a network service, so an unbounded call could hold a
// Broker request forever.
const getentTimeout = 5 * time.Second

// getentWait is the bound getentQuery applies. Production uses getentTimeout;
// tests shorten it to exercise the timeout path.
var getentWait = getentTimeout

// getentOutputLimit bounds the stdout and stderr kept from one getent run. A
// broken NSS module must not be able to grow the Broker's memory.
const getentOutputLimit = 64 << 10

// getentWaitDelay bounds how long Wait waits for the output pipes after the
// process exits or the context is done. A lingering grandchild that inherited
// the pipe must not defeat the timeout.
const getentWaitDelay = 2 * time.Second

// getentPipeWait is the pipe wait execGetent applies. Production uses
// getentWaitDelay; tests shorten it so the lingering-pipe test proves the bound
// without spending the full two seconds under -race. LookupBound keeps the
// production value, so callers sizing a wait are unaffected.
var getentPipeWait = getentWaitDelay

// LookupBound is the upper bound one account lookup can take in production:
// one getent invocation plus the pipe wait after a timeout. Callers sizing a
// wait around a startup step that performs a lookup derive it from this.
const LookupBound = getentTimeout + getentWaitDelay

// getentPaths are the absolute paths probed for getent, in order. PATH is
// never consulted: a Broker that runs as root must not execute a getent found
// through an environment variable. Tests replace the list with a fake script.
var getentPaths = []string{"/usr/bin/getent", "/bin/getent"}

// getentResult is the outcome of one getent process.
type getentResult struct {
	stdout   []byte
	stderr   []byte
	exitCode int
}

// runGetent is the executor seam. Tests use it to observe or replace how a
// getent process is started; production always uses execGetent.
var runGetent = execGetent

// errGetentMissing reports that no getent executable was found at an absolute
// path.
var errGetentMissing = errors.New("getent executable not found")

// errGetentTimeout reports that getent exceeded getentWait.
var errGetentTimeout = errors.New("getent timed out")

// errGetentOutput reports that getent produced more output than
// getentOutputLimit.
var errGetentOutput = errors.New("getent output limit exceeded")

// cappedBuffer keeps at most limit bytes and records whether more arrived.
// Beyond the limit it keeps draining, so the child cannot block on a full
// pipe; the overflow is reported after Wait.
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

// The exported functions dispatch through these variables so the platform
// files can pick the implementation at compile time.
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
	if errors.Is(err, ErrNotFound) {
		return User{}, ErrNotFound
	}
	warnFallback("uid "+id, err)
	u, fallbackErr := osUserLookupUID(uid)
	if fallbackErr == nil {
		return u, nil
	}
	return User{}, unavailableAfterFallback(err, fallbackErr)
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
	if errors.Is(err, ErrNotFound) {
		return User{}, ErrNotFound
	}
	warnFallback("name "+name, err)
	u, fallbackErr := osUserLookupName(name)
	if fallbackErr == nil {
		return u, nil
	}
	return User{}, unavailableAfterFallback(err, fallbackErr)
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
	if errors.Is(err, ErrNotFound) {
		return 0, ErrNotFound
	}
	warnFallback("group "+name, err)
	gid, fallbackErr := osUserLookupGroup(name)
	if fallbackErr == nil {
		return gid, nil
	}
	return 0, unavailableAfterFallback(err, fallbackErr)
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
	// The local source may answer for a different account with the same name.
	// osUserGroupIDs checks the uid; when that check or the lookup itself fails,
	// the temporary failure is reported, never turned into ErrNotFound.
	if ids, fallbackErr := osUserGroupIDs(u); fallbackErr == nil {
		return ids, nil
	}
	return nil, fmt.Errorf("%w: %w", errUnavailable, err)
}

// unavailableAfterFallback reports the original transient failure when the
// local fallback has no entry either. A temporary failure must not be
// reported as a missing account.
func unavailableAfterFallback(transient, fallbackErr error) error {
	if errors.Is(fallbackErr, ErrNotFound) {
		return fmt.Errorf("%w: %v", errUnavailable, transient)
	}
	return fallbackErr
}

// getentQuery runs getent with args and returns its standard output. The
// caller supplies "--" before an account name or uid so a key that starts with
// a dash cannot become an option.
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

// getentExecutable returns the first executable getent at an absolute path.
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

// execGetent runs path with args and an environment of exactly LANG=C. getent
// is started directly, never through a shell.
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
	// A killed process yields an *exec.ExitError with status -1. Check the
	// context first so a timeout is reported as one instead of as an exit
	// status.
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

// errUnavailable reports that a lookup failed for a reason other than a
// missing entry: the database could not be read, or an answer could not be
// trusted. Callers must treat it as a failed check, not as a missing account.
var errUnavailable = errors.New("account database unavailable")

// validateName rejects names that must never reach a subprocess argument.
// Colon and whitespace are refused because they cannot occur in a sane account
// name and would change the meaning of a colon-separated or space-separated
// entry; NUL cannot be passed to exec at all.
func validateName(name string) error {
	if name == "" {
		return errors.New("cannot use an empty account name")
	}
	if strings.ContainsAny(name, ":\x00") || strings.IndexFunc(name, unicode.IsSpace) >= 0 {
		return fmt.Errorf("cannot use account name %q: it is invalid", name)
	}
	return nil
}

// oneLine returns the single non-empty line of out. Everything else is an
// error: a lookup answering with several entries is not trusted, because the
// first line might not correspond to the request.
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

// parsePasswd parses one getent passwd entry.
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

// parseGroup parses one getent group entry and returns its name and gid.
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

// parseInitgroups parses the single line of getent initgroups. The first field
// is the account name; the remaining fields are decimal gids.
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
// instead. The design requires this warning: falling back can make an account
// that the C library resolves invisible to the Broker.
func warnFallback(request string, err error) {
	slog.Warn("accountdb_lookup_fallback", "request", request, "error", err.Error())
}
