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
	"github.com/wangzw/peers/internal/broker/lookupgate"
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
// how often one account's access_check_busy record is written. The account
// database lookup bound is process-wide and lives in lookupgate.
const (
	defaultMaxConcurrentChecks = 8
	defaultMaxChecksPerAccount = 2
	accessBusyLogInterval      = time.Minute
)

// accessAccountIdleTTL is how long an account's check slots are kept after its
// last check returned. The table holds only accounts that exist and were
// checked recently, so a long-running Broker does not keep one entry per
// account ever seen.
const accessAccountIdleTTL = 5 * time.Minute

// maxAccessAccounts bounds the account slot table in addition to the idle
// cleanup. An entry with a check running or waiting is never evicted; when
// every entry is in use the check is refused rather than growing the table.
const maxAccessAccounts = 4096

// unknownCheckAccount is the check-slot key shared by every uid with no
// account database entry. A normal account can own a subuid range, so a
// rootless container can check from tens of thousands of uids that resolve to
// no account; they share one account's slots instead of each getting a private
// quota. Such a check is still denied, because there is no account to run the
// process as.
const unknownCheckAccount = ^uint32(0)

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
	// account. Zero takes the default of 2, so one account cannot take every
	// slot. An account is the bucket every uid without an account entry shares.
	MaxPerAccount int

	// mu guards the lazily created slots, the busy log throttle and the shared
	// unknown-account log window. now is a test seam; nil means time.Now.
	mu    sync.Mutex
	slots *accessSlots
	busy  logThrottle
	// unknownLogAt is when access_check_unknown_account was last written.
	// Every uid with no account entry shares this one window, so the throttle
	// holds no state per uid.
	unknownLogAt time.Time
	now          func() time.Time
}

// logThrottle writes at most one record per key within
// accessBusyLogInterval. A client that keeps retrying must not be able to
// flood the runtime log with records about the same account.
type logThrottle struct {
	at map[uint32]time.Time
}

// allow reports whether a record for key may be written at t, and records the
// time. Records older than the interval are dropped so the map holds only the
// keys that were logged recently.
func (l *logThrottle) allow(key uint32, t time.Time) bool {
	if l.at == nil {
		l.at = make(map[uint32]time.Time)
	}
	if last, ok := l.at[key]; ok && t.Sub(last) < accessBusyLogInterval {
		return false
	}
	for other, at := range l.at {
		if t.Sub(at) >= accessBusyLogInterval {
			delete(l.at, other)
		}
	}
	l.at[key] = t
	return true
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

	// Resolve the account before taking a slot. Only "no such account" is a
	// decision: that uid shares unknownCheckAccount's slots with every other
	// such uid, so a rootless container's rotating subuid cannot multiply the
	// quota. A lookup failure or timeout refuses the check (fail closed), and
	// creates no per-uid slot entry.
	lookup := c.Lookup
	if lookup == nil {
		lookup = LookupAccount
	}
	acct, lookupErr := c.lookupAccount(ctx, lookup, uid)
	if lookupErr != nil && !errors.Is(lookupErr, ErrNoAccount) {
		return false, fmt.Errorf("cannot check access for uid %d: %w", uid, lookupErr)
	}
	key := uid
	if errors.Is(lookupErr, ErrNoAccount) {
		key = unknownCheckAccount
	}

	// Take the account's slot first and the global slot second, so one account
	// can never hold more than its own share of the global slots while it
	// waits. Both slots are returned after the process has been reaped.
	slots := c.slotSet()
	entry, err := slots.acquireAccount(ctx, key)
	if err != nil {
		c.logBusy(log, key, uid)
		if errors.Is(err, errAccessSlotTableFull) {
			return false, fmt.Errorf("cannot check access for uid %d: %w", uid, err)
		}
		return false, fmt.Errorf("cannot check access for uid %d: %w within %s", uid, ErrNoCheckSlot, humanDuration(timeout))
	}
	// Release the exact entry the slot was taken from. Looking the key up again
	// would find a replacement if the table swept and rebuilt it meanwhile, and
	// would take that entry's slot instead of returning this one.
	defer slots.releaseAccount(entry)
	if err := slots.acquireGlobal(ctx); err != nil {
		c.logBusy(log, key, uid)
		return false, fmt.Errorf("cannot check access for uid %d: %w within %s", uid, ErrNoCheckSlot, humanDuration(timeout))
	}
	defer slots.releaseGlobal()

	if lookupErr != nil {
		// The only lookup error left is ErrNoAccount: there is no account to
		// run the process as, so the check is denied and not a failure.
		c.logUnknownAccount(log, uid)
		return false, nil
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

// lookupAccount runs one account database lookup under the process-wide
// getent gate, waiting until ctx is done. The broker package resolves caller
// names through the same gate, so one process never runs more than
// lookupgate.Concurrency getent processes in total.
func (c *AccessChecker) lookupAccount(ctx context.Context, lookup func(uint32) (Account, error), uid uint32) (Account, error) {
	release, err := lookupgate.Acquire(ctx)
	if err != nil {
		return Account{}, err
	}
	defer release()
	return lookup(uid)
}

// nowFunc returns the checker's clock, defaulting to time.Now.
func (c *AccessChecker) nowFunc() func() time.Time {
	if c.now != nil {
		return c.now
	}
	return time.Now
}

// slotSet returns the checker's slot set, creating it on first use.
func (c *AccessChecker) slotSet() *accessSlots {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.slots == nil {
		c.slots = newAccessSlots(c.maxConcurrent(), c.maxPerAccount(), c.nowFunc())
	}
	return c.slots
}

// logBusy records that the account key could not take a check slot, at most
// once per minute. uid is the caller named in the record; a uid without an
// account entry shares the unknown bucket's window with the other such uids,
// so a rotating subuid cannot flood the runtime log.
func (c *AccessChecker) logBusy(log *slog.Logger, key, uid uint32) {
	if !c.allowLog(&c.busy, key) {
		return
	}
	log.Warn("access_check_busy", "uid", uid)
}

// logUnknownAccount records that a check ran for a uid with no account entry.
// Every such uid shares one window, so at most one record is written a minute
// in total: a rootless container may check from a rotating subuid on every
// connection, and the denial itself is already the answer. The throttle holds
// one timestamp rather than one entry per uid, so it cannot grow with the uids
// that check.
func (c *AccessChecker) logUnknownAccount(log *slog.Logger, uid uint32) {
	if !c.allowUnknownAccountLog() {
		return
	}
	log.Warn("access_check_unknown_account", "uid", uid)
}

// allowUnknownAccountLog reports whether the shared unknown-account record may
// be written now, and records the time when it may.
func (c *AccessChecker) allowUnknownAccountLog() bool {
	now := c.nowFunc()
	c.mu.Lock()
	defer c.mu.Unlock()
	t := now()
	if !c.unknownLogAt.IsZero() && t.Sub(c.unknownLogAt) < accessBusyLogInterval {
		return false
	}
	c.unknownLogAt = t
	return true
}

// allowLog applies the checker's clock and lock to one log throttle.
func (c *AccessChecker) allowLog(t *logThrottle, key uint32) bool {
	now := c.nowFunc()
	c.mu.Lock()
	defer c.mu.Unlock()
	return t.allow(key, now())
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

// ErrNoCheckSlot reports that a permission check could not take a slot before
// its budget ran out. The Broker is running and too many checks are in
// progress, which is a quota refusal rather than a broken check: the Broker
// maps it to quota_exceeded and tells the caller to retry shortly.
var ErrNoCheckSlot = errors.New("no check slot became free")

// errAccessSlotTableFull reports that the account slot table reached its bound
// with every entry in use. The check is refused (fail closed) rather than
// growing the table.
var errAccessSlotTableFull = errors.New("the account check slot table is full")

// accessAccountSlot is one account's semaphore and the time its last check
// returned, which the idle cleanup uses.
type accessAccountSlot struct {
	slots  chan struct{}
	idleAt time.Time
}

// accessSlots bounds how many permission checks run at once. A check takes a
// slot for its account, then one of the global slots, and returns both after
// its process has been reaped. Each limit is a buffered channel used as a
// semaphore: a send takes a slot and a receive returns it, so returning one
// slot wakes exactly one waiter instead of every waiter at once.
//
// The account table holds only accounts that exist (a failed lookup never
// creates an entry, and every uid with no account entry shares
// unknownCheckAccount), drops entries idle for accessAccountIdleTTL, and is
// capped at maxAccessAccounts.
type accessSlots struct {
	mu       sync.Mutex
	global   chan struct{}
	accounts map[uint32]*accessAccountSlot
	perLimit int
	now      func() time.Time
}

func newAccessSlots(globalLimit, perAccountLimit int, now func() time.Time) *accessSlots {
	if now == nil {
		now = time.Now
	}
	return &accessSlots{
		global:   make(chan struct{}, globalLimit),
		accounts: make(map[uint32]*accessAccountSlot),
		perLimit: perAccountLimit,
		now:      now,
	}
}

// acquireAccount takes one of the account key's slots, waiting until ctx is
// done, and returns the entry the slot was taken from. The caller must pass
// that entry to releaseAccount: the key could name a different entry by then
// if the table swept the old one and a new check rebuilt it.
func (s *accessSlots) acquireAccount(ctx context.Context, key uint32) (*accessAccountSlot, error) {
	entry, ok := s.accountSlots(key)
	if !ok {
		return nil, errAccessSlotTableFull
	}
	select {
	case entry.slots <- struct{}{}:
		return entry, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

// acquireGlobal takes one of the global slots, waiting until ctx is done.
func (s *accessSlots) acquireGlobal(ctx context.Context) error {
	select {
	case s.global <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// releaseAccount returns a slot to the entry it was taken from and marks that
// entry idle when no check holds it any more. It takes the entry pointer rather
// than the key: a key lookup here would find a replacement if the table swept
// and rebuilt the entry, and would consume the replacement's slot instead of
// returning the original one.
func (s *accessSlots) releaseAccount(entry *accessAccountSlot) {
	if entry == nil {
		return
	}
	<-entry.slots
	s.mu.Lock()
	if len(entry.slots) == 0 {
		entry.idleAt = s.now()
	}
	s.mu.Unlock()
}

// releaseGlobal returns a global slot.
func (s *accessSlots) releaseGlobal() {
	<-s.global
}

// accountSlots returns the semaphore of one account bucket, creating it on
// first use. An unknown uid is counted under unknownCheckAccount, so the map
// holds one entry per account rather than one per uid. It reports false when
// the table is full and no idle entry can be dropped.
func (s *accessSlots) accountSlots(key uint32) (*accessAccountSlot, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	now := s.now()
	s.sweepLocked(now)
	if entry, ok := s.accounts[key]; ok {
		return entry, true
	}
	if len(s.accounts) >= maxAccessAccounts {
		s.evictOldestIdleLocked()
		if len(s.accounts) >= maxAccessAccounts {
			return nil, false
		}
	}
	entry := &accessAccountSlot{slots: make(chan struct{}, s.perLimit), idleAt: now}
	s.accounts[key] = entry
	return entry, true
}

// sweepLocked drops entries that no check holds and that have been idle for
// accessAccountIdleTTL. The caller must hold s.mu.
func (s *accessSlots) sweepLocked(now time.Time) {
	for key, entry := range s.accounts {
		if len(entry.slots) == 0 && now.Sub(entry.idleAt) >= accessAccountIdleTTL {
			delete(s.accounts, key)
		}
	}
}

// evictOldestIdleLocked drops the longest-idle entry that no check holds. It
// is the last resort when the table reaches maxAccessAccounts. The caller must
// hold s.mu.
func (s *accessSlots) evictOldestIdleLocked() {
	var (
		oldestKey uint32
		oldestAt  time.Time
		found     bool
	)
	for key, entry := range s.accounts {
		if len(entry.slots) != 0 {
			continue
		}
		if !found || entry.idleAt.Before(oldestAt) {
			oldestKey, oldestAt, found = key, entry.idleAt, true
		}
	}
	if found {
		delete(s.accounts, oldestKey)
	}
}

// accessCheckStart returns the default Start function for AccessChecker: it
// runs the access-check subcommand of exe as root with the account identity,
// and the subcommand drops only its effective identity.
func accessCheckStart(exe string) func(context.Context, Account) (*Process, error) {
	return func(_ context.Context, acct Account) (*Process, error) {
		cmd := accessCheckCommand(exe, acct)
		stdinR, stdinW, err := helperPipe()
		if err != nil {
			return nil, err
		}
		cmd.Stdin = stdinR
		stdoutR, stdoutW, err := helperPipe()
		if err != nil {
			_ = stdinR.Close()
			_ = stdinW.Close()
			return nil, err
		}
		cmd.Stdout = stdoutW
		if err := cmd.Start(); err != nil {
			_ = stdinR.Close()
			_ = stdinW.Close()
			_ = stdoutR.Close()
			_ = stdoutW.Close()
			return nil, err
		}
		_ = stdinR.Close()
		_ = stdoutW.Close()
		return &Process{
			Stdin:  stdinW,
			Stdout: stdoutR,
			Wait: func() error {
				err := cmd.Wait()
				_ = stdinW.Close()
				_ = stdoutR.Close()
				return err
			},
			Kill: cmd.Process.Kill,
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
