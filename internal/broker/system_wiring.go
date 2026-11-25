package broker

import (
	"context"
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"syscall"
	"time"

	"github.com/wangzw/peers/internal/broker/helper"
)

// helperPool is the part of *helper.Pool used by the system Broker.
type helperPool interface {
	Acquire(ctx context.Context, uid uint32) (*helper.Backend, func(), error)
	Close() error
}

// helperAccess is the part of *helper.AccessChecker used by the system Broker.
type helperAccess interface {
	Check(ctx context.Context, uid uint32, path string, need helper.Access) (bool, error)
}

type systemRouter struct{ pool helperPool }

func (r systemRouter) Acquire(ctx context.Context, uid uint32) (SessionBackend, func(), error) {
	b, release, err := r.pool.Acquire(ctx, uid)
	if err != nil {
		return nil, nil, err
	}
	// The uid lets a watch request be checked against the directory's owner
	// before it reaches the helper.
	return ownedBackend{SessionBackend: b, uid: uid}, release, nil
}

// configDirVerifier is implemented by a backend that proves a configuration
// directory belongs to its account before a watch subscribes.
type configDirVerifier interface {
	verifyConfigDir(ctx context.Context, configDir string) error
}

// configDirVerifyTimeout bounds the ownership check of a configuration
// directory. The check stats a caller-chosen path as root, so it must not hold
// Broker.mu or run unbounded.
const configDirVerifyTimeout = time.Second

// configDirCheckSet allows at most one unfinished ownership check per account.
// A check abandoned at its timeout keeps its slot until its goroutine returns,
// so a stuck stat cannot accumulate goroutines.
type configDirCheckSet struct {
	mu   sync.Mutex
	busy map[uint32]chan struct{}
}

func (s *configDirCheckSet) begin(ctx context.Context, uid uint32) error {
	for {
		s.mu.Lock()
		if s.busy == nil {
			s.busy = make(map[uint32]chan struct{})
		}
		if _, ok := s.busy[uid]; !ok {
			s.busy[uid] = make(chan struct{})
			s.mu.Unlock()
			return nil
		}
		freed := s.busy[uid]
		s.mu.Unlock()
		select {
		case <-freed:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (s *configDirCheckSet) end(uid uint32) {
	s.mu.Lock()
	if freed, ok := s.busy[uid]; ok {
		close(freed)
		delete(s.busy, uid)
	}
	s.mu.Unlock()
}

// verifyConfigDirBounded runs the verifier in its own goroutine with one
// timeout shared by the slot wait and the check; a stat that never returns is
// abandoned.
func verifyConfigDirBounded(ctx context.Context, uid uint32, checks *configDirCheckSet, v configDirVerifier, configDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(ctx, configDirVerifyTimeout)
	defer cancel()
	if err := checks.begin(ctx, uid); err != nil {
		return fmt.Errorf("checking configuration directory %s: %w", configDir, err)
	}
	done := make(chan error, 1)
	go func() {
		defer checks.end(uid)
		done <- v.verifyConfigDir(ctx, configDir)
	}()
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		return fmt.Errorf("checking configuration directory %s: %w", configDir, ctx.Err())
	}
}

type ownedBackend struct {
	SessionBackend
	uid uint32
	// stat is os.Stat; a test replaces it to hold the check on a stat that never
	// returns.
	stat func(string) (fs.FileInfo, error)
}

var (
	_ SessionBackend    = ownedBackend{}
	_ configDirVerifier = ownedBackend{}
)

func (b ownedBackend) statConfigDir(path string) (fs.FileInfo, error) {
	if b.stat != nil {
		return b.stat(path)
	}
	return os.Stat(path)
}

// verifyConfigDir reports whether configDir belongs to the account. An empty
// value is the account's default, resolved from the account database rather
// than the Broker's home, and a directory that does not exist yet has no
// conflicting owner.
func (b ownedBackend) verifyConfigDir(ctx context.Context, configDir string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	dir := configDir
	if dir == "" {
		// Only the default directory needs the account database: a relative
		// path is resolved from the filesystem root because the helper runs
		// with cwd /, and an absolute path is already complete.
		acct, err := helper.LookupAccount(b.uid)
		if err != nil {
			return err
		}
		dir = filepath.Join(acct.Home, ".claude")
	}
	if !filepath.IsAbs(dir) {
		// The helper runs with cwd /, so a relative configuration directory is
		// relative to the filesystem root there.
		dir = filepath.Join("/", dir)
	}
	dir = filepath.Clean(dir)

	fi, err := b.statConfigDir(dir)
	if errors.Is(err, fs.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return fmt.Errorf("configuration directory %s has no owner information", dir)
	}
	if st.Uid != b.uid {
		return fmt.Errorf("configuration directory %s belongs to uid %d, not %d", dir, st.Uid, b.uid)
	}
	return nil
}

type systemChecker struct{ c helperAccess }

func (s systemChecker) Check(ctx context.Context, who Caller, path string, need Access) (bool, error) {
	switch need {
	case AccessRead:
		return s.c.Check(ctx, who.UID, path, helper.AccessRead)
	case AccessWrite:
		return s.c.Check(ctx, who.UID, path, helper.AccessWrite)
	case AccessNone:
		return true, nil
	default:
		return false, fmt.Errorf("unknown access level %d", need)
	}
}

// newHelperPool is a test seam; production uses helper.NewPool.
var newHelperPool = helper.NewPool

// The system Broker allows this many permission checks at once, with at most
// accessChecksPerAccount for one account.
const (
	accessChecksConcurrent = 8
	accessChecksPerAccount = 2
)

// newSystemDeps builds the router and checker of a system Broker and a close
// function. The pool uses ctx's cancellation to recognize helper exits that
// belong to a normal stop.
func newSystemDeps(ctx context.Context, exe string, logger *slog.Logger) (SessionRouter, accessChecker, func() error) {
	pool := newHelperPool(helper.PoolOptions{Exe: exe, Logger: logger, Stopping: ctx.Done()})
	ac := &helper.AccessChecker{
		Exe:           exe,
		Logger:        logger,
		MaxConcurrent: accessChecksConcurrent,
		MaxPerAccount: accessChecksPerAccount,
	}
	return systemRouter{pool: pool}, systemChecker{c: ac}, pool.Close
}

// systemAssembler runs first when RunOptions.System is true, so asm.Access is
// filled before configWiring reads it.
type systemAssembler struct {
	executable func() (string, error)                                                             // nil: os.Executable
	newDeps    func(exe string, logger *slog.Logger) (SessionRouter, accessChecker, func() error) // nil: newSystemDeps
}

var _ Assembler = systemAssembler{}

func (systemAssembler) Name() string { return "system" }

func (a systemAssembler) Assemble(ctx context.Context, _ ConfigSource, deps *Deps, asm *Assembly) ([]Component, error) {
	executable := a.executable
	if executable == nil {
		executable = os.Executable
	}
	exe, err := executable()
	if err != nil {
		return nil, fmt.Errorf("locating the peers executable: %w", err)
	}
	newDeps := a.newDeps
	if newDeps == nil {
		newDeps = func(exe string, logger *slog.Logger) (SessionRouter, accessChecker, func() error) {
			return newSystemDeps(ctx, exe, logger)
		}
	}
	router, checker, closeFn := newDeps(exe, deps.Logger)
	deps.Sessions = router
	deps.AllowCaller = func(Caller) bool { return true }
	asm.Access = checker
	return []Component{&systemComponent{close: closeFn}}, nil
}

// systemComponent closes the helper pool after the local interface and the other
// components have stopped: the system assembler runs first, so its component
// starts first and stops last.
type systemComponent struct {
	close func() error
	once  sync.Once
}

var _ Component = (*systemComponent)(nil)

func (*systemComponent) Name() string { return "system" }

func (*systemComponent) Start(context.Context, *Broker) error { return nil }

func (c *systemComponent) Stop() {
	c.once.Do(func() {
		if c.close != nil {
			_ = c.close()
		}
	})
}

// assemblersFor returns opts.Assemblers (DefaultAssemblers when nil) with a
// system assembler prepended when opts.System is true, and does not modify
// opts.Assemblers.
func assemblersFor(opts RunOptions) []Assembler {
	list := opts.Assemblers
	if list == nil {
		list = DefaultAssemblers()
	}
	if opts.System {
		return append([]Assembler{systemAssembler{}}, list...)
	}
	return list
}

var (
	_ helperPool     = (*helper.Pool)(nil)
	_ helperAccess   = (*helper.AccessChecker)(nil)
	_ SessionBackend = (*helper.Backend)(nil)
	_ SessionRouter  = systemRouter{}
	_ accessChecker  = systemChecker{}
)
