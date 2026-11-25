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

// The identity reaches the access-check process in its environment, not on the
// wire, because the switch must happen before the child reads any
// caller-controlled byte.
const (
	accessCheckUIDEnv    = "PEERS_ACCESS_CHECK_UID"
	accessCheckGIDEnv    = "PEERS_ACCESS_CHECK_GID"
	accessCheckGroupsEnv = "PEERS_ACCESS_CHECK_GROUPS"
)

// Access is the permission a caller asks for when reading or writing the
// Broker configuration file.
type Access uint8

// AccessRead and AccessWrite are the two permissions an access check asks for.
const (
	AccessRead Access = iota + 1
	AccessWrite
)

const defaultAccessCheckTimeout = 5 * time.Second

const (
	defaultMaxConcurrentChecks = 8
	defaultMaxChecksPerAccount = 2
	accessBusyLogInterval      = time.Minute
)

const accessAccountIdleTTL = 5 * time.Minute

const maxAccessAccounts = 4096

const unknownCheckAccount = ^uint32(0)

// RunAccessCheck is the body of `peers internal access-check`: it drops its
// effective identity to the account named in its environment, reads one
// AccessCheckRequest from stdin, checks it as that account and writes one
// AccessCheckResponse to stdout. A failed switch is reported in the response,
// which is never allowed.
func RunAccessCheck(stdin io.Reader, stdout io.Writer) error {
	if err := enterAccountFromEnv(); err != nil {
		// The Broker discards this process's stderr, so the failure must travel
		// in the response to reach its log.
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
		Uid:             uint32(os.Geteuid()),
		Allowed:         false,
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
		switch err := accessCheckSyscall(req.GetPath(), mode); {
		case err == nil:
			resp.Allowed = true
		case isAccessDenial(err):
		default:
			resp.Error = err.Error()
		}
	}

	if err := newMsgWriter(stdout).write(resp); err != nil {
		return fmt.Errorf("access-check: writing response: %w", err)
	}
	return nil
}

// accessCheckSyscall judges the effective identity and never falls back to the
// permission bits; tests replace it to simulate a kernel without faccessat2.
var accessCheckSyscall = faccessat2

// isAccessDenial reports whether err is the check saying no rather than the
// check failing; EPERM and ENOSYS are deliberately absent, because a refused
// faccessat2 must fail the check instead of being read as a denial.
func isAccessDenial(err error) bool {
	return errors.Is(err, unix.EACCES) ||
		errors.Is(err, unix.ENOENT) ||
		errors.Is(err, unix.ENOTDIR) ||
		errors.Is(err, unix.ELOOP) ||
		errors.Is(err, unix.EROFS)
}

// enterAccountFromEnv switches to the account named in the environment;
// membership is replaced, not extended, and only the effective ids change, so
// the account can neither signal nor inspect the process.
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

// becomeAccount switches to uid and skips the switch only when both the
// effective uid and effective gid already match, so a check is never decided
// under another account's effective identity. The real and saved uid are left
// alone.
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
	// Exe is the executable to run; required unless Start is set.
	Exe string
	// Lookup resolves an account; nil takes LookupAccount.
	Lookup func(uid uint32) (Account, error)
	// Timeout bounds one check, including its wait for a slot; zero takes 5s.
	Timeout time.Duration
	// Start launches one check process for acct; nil runs Exe with the
	// access-check subcommand and its pipes.
	Start func(ctx context.Context, acct Account) (*Process, error)
	// Logger receives the check's warnings; nil takes slog.Default().
	Logger *slog.Logger

	// MaxConcurrent is the most permission checks that may run at once across
	// all accounts. Zero takes the default of 8.
	MaxConcurrent int
	// MaxPerAccount is the most permission checks that may run at once for one
	// account. Zero takes the default of 2, so one account cannot take every
	// slot.
	MaxPerAccount int

	// mu guards slots, busy and unknownLogAt; now is a test seam, and nil means
	// time.Now.
	mu           sync.Mutex
	slots        *accessSlots
	busy         logThrottle
	unknownLogAt time.Time
	now          func() time.Time
}

type logThrottle struct {
	at map[uint32]time.Time
}

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

// Check reports whether the account uid may access path, running one process
// per call. The wait for a check slot and the check itself share the Timeout
// budget; when no slot frees up in time it returns an error wrapping
// ErrNoCheckSlot, and a uid with no account entry is denied without a process.
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

	// Resolve the account before taking a slot, so a failed lookup creates no
	// entry and only a definite no-account shares unknownCheckAccount.
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

	// Take the account slot before the global slot, so a waiting check cannot
	// hold more than its account's share. Both are returned after the process
	// has been reaped.
	slots := c.slotSet()
	entry, err := slots.acquireAccount(ctx, key)
	if err != nil {
		c.logBusy(log, key, uid)
		if errors.Is(err, errAccessSlotTableFull) {
			return false, fmt.Errorf("cannot check access for uid %d: %w", uid, err)
		}
		return false, fmt.Errorf("cannot check access for uid %d: %w within %s", uid, ErrNoCheckSlot, humanDuration(timeout))
	}
	// Release the entry the slot was taken from: a key lookup could find a
	// replacement if the table swept and rebuilt it meanwhile.
	defer slots.releaseAccount(entry)
	if err := slots.acquireGlobal(ctx); err != nil {
		c.logBusy(log, key, uid)
		return false, fmt.Errorf("cannot check access for uid %d: %w within %s", uid, ErrNoCheckSlot, humanDuration(timeout))
	}
	defer slots.releaseGlobal()

	if lookupErr != nil {
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
	// The child reads exactly one request; a close error surfaces as a read
	// error below.
	_ = proc.Stdin.Close()

	type response struct {
		msg *helperpb.AccessCheckResponse
		err error
	}
	// A pipe read cannot be interrupted by ctx, so it runs in its own goroutine
	// and is abandoned on timeout; the channel is buffered so the goroutine can
	// finish after Check returns.
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

	// Reap the child before returning, and report a bad answer ahead of the wait
	// error, because the answer says what went wrong.
	waitErr := proc.Wait()
	if got := res.msg.GetProtocolVersion(); got != helperpb.ProtocolVersion {
		return false, fmt.Errorf("cannot check access for uid %d: the check process reported protocol version %d", uid, got)
	}
	// Check the reported error before the uid: a failed switch carries the
	// process's current effective uid, and its errno must reach the log. An
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

func (c *AccessChecker) maxConcurrent() int {
	if c.MaxConcurrent > 0 {
		return c.MaxConcurrent
	}
	return defaultMaxConcurrentChecks
}

func (c *AccessChecker) maxPerAccount() int {
	if c.MaxPerAccount > 0 {
		return c.MaxPerAccount
	}
	return defaultMaxChecksPerAccount
}

// lookupAccount runs one account lookup under the process-wide getent gate,
// which the Broker also uses for caller names.
func (c *AccessChecker) lookupAccount(ctx context.Context, lookup func(uint32) (Account, error), uid uint32) (Account, error) {
	release, err := lookupgate.Acquire(ctx)
	if err != nil {
		return Account{}, err
	}
	defer release()
	return lookup(uid)
}

func (c *AccessChecker) nowFunc() func() time.Time {
	if c.now != nil {
		return c.now
	}
	return time.Now
}

func (c *AccessChecker) slotSet() *accessSlots {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.slots == nil {
		c.slots = newAccessSlots(c.maxConcurrent(), c.maxPerAccount(), c.nowFunc())
	}
	return c.slots
}

func (c *AccessChecker) logBusy(log *slog.Logger, key, uid uint32) {
	if !c.allowLog(&c.busy, key) {
		return
	}
	log.Warn("access_check_busy", "uid", uid)
}

func (c *AccessChecker) logUnknownAccount(log *slog.Logger, uid uint32) {
	if !c.allowUnknownAccountLog() {
		return
	}
	log.Warn("access_check_unknown_account", "uid", uid)
}

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

func (c *AccessChecker) allowLog(t *logThrottle, key uint32) bool {
	now := c.nowFunc()
	c.mu.Lock()
	defer c.mu.Unlock()
	return t.allow(key, now())
}

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
// its budget ran out; the Broker maps it to quota_exceeded.
var ErrNoCheckSlot = errors.New("no check slot became free")

var errAccessSlotTableFull = errors.New("the account check slot table is full")

type accessAccountSlot struct {
	slots  chan struct{}
	idleAt time.Time
}

// accessSlots bounds how many permission checks run at once: one slot for the
// account, then one global slot, both returned after the process is reaped.
// The account table drops entries idle for accessAccountIdleTTL and is capped
// at maxAccessAccounts.
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

// acquireAccount takes one of key's slots, waiting until ctx is done, and
// returns the entry it came from; the caller must pass that entry to
// releaseAccount, because a sweep can replace it under the same key.
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

func (s *accessSlots) acquireGlobal(ctx context.Context) error {
	select {
	case s.global <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

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

func (s *accessSlots) releaseGlobal() {
	<-s.global
}

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

// sweepLocked drops entries idle for accessAccountIdleTTL that no check holds;
// the caller must hold s.mu.
func (s *accessSlots) sweepLocked(now time.Time) {
	for key, entry := range s.accounts {
		if len(entry.slots) == 0 && now.Sub(entry.idleAt) >= accessAccountIdleTTL {
			delete(s.accounts, key)
		}
	}
}

// evictOldestIdleLocked drops the longest-idle entry no check holds, the last
// resort at maxAccessAccounts; the caller must hold s.mu.
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

func stopProcess(proc *Process) {
	if proc.Kill != nil {
		_ = proc.Kill()
	}
	if proc.Wait != nil {
		_ = proc.Wait()
	}
}
