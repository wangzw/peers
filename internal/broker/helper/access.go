package helper

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"strings"
	"sync"
	"time"

	"golang.org/x/sys/unix"

	"github.com/wangzw/peers/internal/broker/helperpb"
)

// The access-check process execs as root and receives the identity it must
// check as in its environment. These are not part of the wire protocol: the
// switch has to happen before the child reads any caller-controlled byte, and
// the Broker constructs the environment itself.
const (
	accessCheckUIDEnv    = "PEERS_ACCESS_CHECK_UID"
	accessCheckGIDEnv    = "PEERS_ACCESS_CHECK_GID"
	accessCheckGroupsEnv = "PEERS_ACCESS_CHECK_GROUPS"
)

// Access is the permission a caller asks for when reading or writing the
// Broker configuration file.
type Access uint8

const (
	AccessRead Access = iota + 1
	AccessWrite
)

// defaultAccessCheckTimeout bounds one permission check from start to result,
// including its waits for a check slot.
const defaultAccessCheckTimeout = 5 * time.Second

// Defaults of AccessChecker.MaxConcurrent and AccessChecker.MaxPerAccount, and
// how often one account's access_check_busy record is written.
const (
	defaultMaxConcurrentChecks = 8
	defaultMaxChecksPerAccount = 2
	accessBusyLogInterval      = time.Minute
)

// RunAccessCheck is the body of `peers internal access-check`: it first
// drops its effective identity to the account named by the Broker, then reads
// one AccessCheckRequest from stdin, checks it as that account, writes one
// AccessCheckResponse to stdout and returns.
func RunAccessCheck(stdin io.Reader, stdout io.Writer) error {
	if err := enterAccountFromEnv(); err != nil {
		// Report the failure in the framed response before exiting. The
		// process's stderr is discarded by the Broker, so the response is the
		// only path for the errno to reach the Broker log. The response is
		// never allowed: it carries the error and the process exits with a
		// failure.
		resp := &helperpb.AccessCheckResponse{
			ProtocolVersion: helperpb.ProtocolVersion,
			Uid:             uint32(os.Geteuid()),
			Allowed:         false,
			Error:           err.Error(),
		}
		if writeErr := newMsgWriter(stdout).write(resp); writeErr != nil {
			return fmt.Errorf("access-check: %w (reporting it: %v)", err, writeErr)
		}
		return fmt.Errorf("access-check: %w", err)
	}

	var req helperpb.AccessCheckRequest
	if err := newMsgReader(stdin).read(&req); err != nil {
		return fmt.Errorf("access-check: reading request: %w", err)
	}

	resp := &helperpb.AccessCheckResponse{
		ProtocolVersion: helperpb.ProtocolVersion,
		// The check runs as the effective identity, which is the account the
		// Broker asked about; the real and saved uid stay root.
		Uid:     uint32(os.Geteuid()),
		Allowed: false,
	}
	switch {
	case req.GetProtocolVersion() != helperpb.ProtocolVersion:
		resp.Error = fmt.Sprintf("unsupported protocol version %d", req.GetProtocolVersion())
	case req.GetAccess() == helperpb.Access_ACCESS_UNSPECIFIED:
		resp.Error = "unspecified access"
	default:
		mode := uint32(unix.R_OK)
		if req.GetAccess() == helperpb.Access_ACCESS_WRITE {
			mode = uint32(unix.W_OK)
		}
		// The process switched its effective identity above, and faccessat2
		// judges that identity while still applying the ACL. A kernel that does
		// not implement faccessat2 must surface as a failed check: falling back
		// to the permission bits would ignore the ACL.
		switch err := accessCheckSyscall(req.GetPath(), mode); {
		case err == nil:
			resp.Allowed = true
		case isAccessDenial(err):
			// The account lacks the requested permission. That is a result of
			// the check, not a failure of it.
		default:
			resp.Error = err.Error()
		}
	}

	if err := newMsgWriter(stdout).write(resp); err != nil {
		return fmt.Errorf("access-check: writing response: %w", err)
	}
	return nil
}

// accessCheckSyscall performs the access check on path for mode as the
// process's effective identity. On Linux it calls faccessat2 directly, which
// applies the effective identity and the ACL and never falls back to the
// permission bits. Tests replace it to simulate a kernel without faccessat2.
var accessCheckSyscall = faccessat2

// isAccessDenial reports whether err is the access check saying "no" rather
// than the check itself failing. EPERM and ENOSYS are deliberately absent: a
// kernel or sandbox that refuses faccessat2 must be reported as a failed check,
// not as a denial judged from the permission bits.
func isAccessDenial(err error) bool {
	return errors.Is(err, unix.EACCES) ||
		errors.Is(err, unix.ENOENT) ||
		errors.Is(err, unix.ENOTDIR) ||
		errors.Is(err, unix.ELOOP) ||
		errors.Is(err, unix.EROFS)
}

// enterAccountFromEnv switches the process to the account named by the Broker.
// Without the identity variables the process is already the account: that is
// how in-process unit tests call RunAccessCheck. Membership is replaced, not
// extended, and only the effective ids change, so the real and saved uid stay
// root and the account cannot signal or inspect the process.
func enterAccountFromEnv() error {
	uid, gid, present, err := lookupIdentityEnv(accessCheckUIDEnv, accessCheckGIDEnv)
	if err != nil {
		return err
	}
	if !present {
		return nil
	}

	groups, err := parseIDList(os.Getenv(accessCheckGroupsEnv))
	if err != nil {
		return fmt.Errorf("account groups: %w", err)
	}
	return becomeAccount(uid, gid, groups)
}

// parseIDList parses the comma-separated group ids the Broker passes in the
// environment. An empty value is an empty list.
func parseIDList(value string) ([]uint32, error) {
	if value == "" {
		return nil, nil
	}
	fields := strings.Split(value, ",")
	ids := make([]uint32, 0, len(fields))
	for _, field := range fields {
		id, err := parseID(field)
		if err != nil {
			return nil, fmt.Errorf("%q: %w", field, err)
		}
		ids = append(ids, id)
	}
	return ids, nil
}

// becomeAccount makes the process check as uid. A check whose target identity
// is already the process's effective identity needs no switch; this also
// covers the root access check on macOS, where the account database can list
// more groups than Darwin's NGROUPS limit. Otherwise the switch is
// platform-specific: Linux replaces the supplementary groups, while Darwin
// lets the kernel resolve them through memberd. The calls require the process
// to be root, which the Broker arranges by execing it without a credential
// change. The real and saved uid are deliberately left alone.
func becomeAccount(uid, gid uint32, groups []uint32) error {
	if uint32(os.Geteuid()) == uid && uint32(os.Getegid()) == gid {
		return nil
	}
	return switchAccount(uid, gid, groups)
}

// AccessChecker decides configuration file permissions by running one
// access-check process for the calling account. The process execs as root and
// checks with only its effective identity set to the account, so the account
// never owns it. Results are never cached.
type AccessChecker struct {
	Exe     string                                                    // required unless Start is set
	Lookup  func(uid uint32) (Account, error)                         // default LookupAccount
	Timeout time.Duration                                             // default 5s
	Start   func(ctx context.Context, acct Account) (*Process, error) // default: accountCommand(Exe, []string{"internal", "access-check"}, acct) with pipes
	Logger  *slog.Logger

	// MaxConcurrent is the most permission checks that may run at once across
	// all accounts. Zero takes the default of 8.
	MaxConcurrent int
	// MaxPerAccount is the most permission checks that may run at once for one
	// uid. Zero takes the default of 2, so one account cannot take every slot.
	MaxPerAccount int

	// mu guards the lazily created slots and the times of the per-uid
	// access_check_busy records. now is a test seam; nil means time.Now.
	mu     sync.Mutex
	slots  *accessSlots
	busyAt map[uint32]time.Time
	now    func() time.Time
}

// Check reports whether the account uid may access path. It spawns one process
// per call. The wait for a check slot and the check itself share the Timeout
// budget; when no slot frees up in time the check fails instead of running.
func (c *AccessChecker) Check(ctx context.Context, uid uint32, path string, need Access) (bool, error) {
	log := c.Logger
	if log == nil {
		log = slog.Default()
	}

	timeout := c.Timeout
	if timeout <= 0 {
		timeout = defaultAccessCheckTimeout
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	// Take the account's slot first and the global slot second, so one account
	// can never hold more than its own share of the global slots while it
	// waits. Both slots are returned after the process has been reaped.
	slots := c.slotSet()
	if err := slots.acquireAccount(ctx, uid, c.maxPerAccount()); err != nil {
		c.logBusy(log, uid)
		return false, fmt.Errorf("cannot check access for uid %d: no check slot became free within %s", uid, humanDuration(timeout))
	}
	defer slots.releaseAccount(uid)
	if err := slots.acquireGlobal(ctx, c.maxConcurrent()); err != nil {
		c.logBusy(log, uid)
		return false, fmt.Errorf("cannot check access for uid %d: no check slot became free within %s", uid, humanDuration(timeout))
	}
	defer slots.releaseGlobal()

	lookup := c.Lookup
	if lookup == nil {
		lookup = LookupAccount
	}
	acct, err := lookup(uid)
	if err != nil {
		if errors.Is(err, ErrNoAccount) {
			log.Warn("access_check_unknown_account", "uid", uid)
			return false, nil
		}
		return false, fmt.Errorf("cannot check access for uid %d: %w", uid, err)
	}

	start := c.Start
	if start == nil {
		if c.Exe == "" {
			return false, errors.New("cannot check access: no executable is configured")
		}
		start = accessCheckStart(c.Exe)
	}

	proc, err := start(ctx, acct)
	if err != nil {
		return false, fmt.Errorf("cannot check access for uid %d: starting the process: %w", uid, err)
	}

	req := &helperpb.AccessCheckRequest{
		ProtocolVersion: helperpb.ProtocolVersion,
		Path:            path,
	}
	switch need {
	case AccessRead:
		req.Access = helperpb.Access_ACCESS_READ
	case AccessWrite:
		req.Access = helperpb.Access_ACCESS_WRITE
	}
	if err := newMsgWriter(proc.Stdin).write(req); err != nil {
		stopProcess(proc)
		return false, fmt.Errorf("cannot check access for uid %d: sending the request: %w", uid, err)
	}
	// The child reads exactly one request; closing stdin tells it no more
	// requests follow. A close error will surface as a read error below.
	_ = proc.Stdin.Close()

	type response struct {
		msg *helperpb.AccessCheckResponse
		err error
	}
	// The read blocks on a pipe, which a context cannot interrupt, so it runs
	// in its own goroutine and the select below abandons it on timeout. The
	// channel is buffered so the goroutine can finish after Check returns.
	read := make(chan response, 1)
	go func() {
		var msg helperpb.AccessCheckResponse
		err := newMsgReader(proc.Stdout).read(&msg)
		read <- response{msg: &msg, err: err}
	}()

	var res response
	select {
	case res = <-read:
	case <-ctx.Done():
		stopProcess(proc)
		return false, fmt.Errorf("cannot check access for uid %d: waiting for the check process: %w", uid, ctx.Err())
	}
	if res.err != nil {
		stopProcess(proc)
		return false, fmt.Errorf("cannot check access for uid %d: reading the result: %w", uid, res.err)
	}

	// The child exits after writing its result. Reap it before returning so no
	// zombie is left behind, but report a bad answer ahead of a wait error: the
	// answer explains what went wrong.
	waitErr := proc.Wait()
	if got := res.msg.GetProtocolVersion(); got != helperpb.ProtocolVersion {
		return false, fmt.Errorf("cannot check access for uid %d: the check process reported protocol version %d", uid, got)
	}
	// Check the reported error before the uid: a failed identity switch is
	// reported with the process's current effective uid, and that errno must
	// reach the Broker log instead of being replaced by a uid mismatch. An
	// error response is never allowed, whatever its Allowed field says.
	if msg := res.msg.GetError(); msg != "" {
		log.Warn("access_check_failed", "uid", uid, "error", msg)
		return false, fmt.Errorf("cannot check access for uid %d: %s", uid, msg)
	}
	if got := res.msg.GetUid(); got != uid {
		return false, fmt.Errorf("cannot check access for uid %d: the check process ran as uid %d", uid, got)
	}
	if waitErr != nil {
		return false, fmt.Errorf("cannot check access for uid %d: the check process failed: %w", uid, waitErr)
	}
	return res.msg.GetAllowed(), nil
}

// maxConcurrent returns the global check limit, defaulting a zero value.
func (c *AccessChecker) maxConcurrent() int {
	if c.MaxConcurrent > 0 {
		return c.MaxConcurrent
	}
	return defaultMaxConcurrentChecks
}

// maxPerAccount returns the per-account check limit, defaulting a zero value.
func (c *AccessChecker) maxPerAccount() int {
	if c.MaxPerAccount > 0 {
		return c.MaxPerAccount
	}
	return defaultMaxChecksPerAccount
}

// slotSet returns the checker's slot set, creating it on first use.
func (c *AccessChecker) slotSet() *accessSlots {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.slots == nil {
		c.slots = &accessSlots{perAccount: make(map[uint32]int), changed: make(chan struct{})}
	}
	return c.slots
}

// logBusy records that uid could not take a check slot, at most once per
// minute. The window keeps a caller that retries a full slot set from flooding
// the runtime log.
func (c *AccessChecker) logBusy(log *slog.Logger, uid uint32) {
	now := time.Now
	if c.now != nil {
		now = c.now
	}
	t := now()

	c.mu.Lock()
	if c.busyAt == nil {
		c.busyAt = make(map[uint32]time.Time)
	}
	if last, ok := c.busyAt[uid]; ok && t.Sub(last) < accessBusyLogInterval {
		c.mu.Unlock()
		return
	}
	// Drop records whose minute has passed so the map tracks only the accounts
	// that are busy now.
	for other, at := range c.busyAt {
		if t.Sub(at) >= accessBusyLogInterval {
			delete(c.busyAt, other)
		}
	}
	c.busyAt[uid] = t
	c.mu.Unlock()

	log.Warn("access_check_busy", "uid", uid)
}

// humanDuration renders d as the error and log lines read it, such as "5
// seconds", rather than as the time.Duration String method writes it. Any part
// of a second rounds up, so a short budget still reads as a whole second.
func humanDuration(d time.Duration) string {
	if d < time.Second {
		return "1 second"
	}
	seconds := int64(d / time.Second)
	if d%time.Second != 0 {
		seconds++
	}
	if seconds == 1 {
		return "1 second"
	}
	return fmt.Sprintf("%d seconds", seconds)
}

// accessSlots bounds how many permission checks run at once. A check takes a
// slot for its account, then one of the global slots, and returns both after
// its process has been reaped. Every state change closes changed and installs
// a fresh channel, waking every waiter to test its condition again.
type accessSlots struct {
	mu         sync.Mutex
	global     int
	perAccount map[uint32]int
	changed    chan struct{}
}

// acquireAccount takes one of the account's slots, waiting until ctx is done.
func (s *accessSlots) acquireAccount(ctx context.Context, uid uint32, limit int) error {
	for {
		s.mu.Lock()
		if s.perAccount[uid] < limit {
			s.perAccount[uid]++
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// acquireGlobal takes one of the global slots, waiting until ctx is done.
func (s *accessSlots) acquireGlobal(ctx context.Context, limit int) error {
	for {
		s.mu.Lock()
		if s.global < limit {
			s.global++
			s.mu.Unlock()
			return nil
		}
		changed := s.changed
		s.mu.Unlock()
		select {
		case <-changed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

// releaseAccount returns the account's slot and wakes the waiters.
func (s *accessSlots) releaseAccount(uid uint32) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.perAccount[uid] > 1 {
		s.perAccount[uid]--
	} else {
		delete(s.perAccount, uid)
	}
	s.broadcastLocked()
}

// releaseGlobal returns a global slot and wakes the waiters.
func (s *accessSlots) releaseGlobal() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.global > 0 {
		s.global--
	}
	s.broadcastLocked()
}

// broadcastLocked wakes every waiter. The caller must hold s.mu.
func (s *accessSlots) broadcastLocked() {
	close(s.changed)
	s.changed = make(chan struct{})
}

// accessCheckStart returns the default Start function for AccessChecker: it
// runs the access-check subcommand of exe as root with the account identity,
// and the subcommand drops only its effective identity.
func accessCheckStart(exe string) func(context.Context, Account) (*Process, error) {
	return func(_ context.Context, acct Account) (*Process, error) {
		cmd := accessCheckCommand(exe, acct)
		stdin, err := cmd.StdinPipe()
		if err != nil {
			return nil, err
		}
		stdout, err := cmd.StdoutPipe()
		if err != nil {
			return nil, err
		}
		if err := cmd.Start(); err != nil {
			return nil, err
		}
		return &Process{
			Stdin:  stdin,
			Stdout: stdout,
			Wait:   cmd.Wait,
			Kill:   cmd.Process.Kill,
		}, nil
	}
}

// stopProcess kills and reaps proc, ignoring failures on either step.
func stopProcess(proc *Process) {
	if proc.Kill != nil {
		_ = proc.Kill()
	}
	if proc.Wait != nil {
		_ = proc.Wait()
	}
}
