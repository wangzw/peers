package broker

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"golang.org/x/sys/unix"
	"google.golang.org/grpc"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
	"github.com/wangzw/peers/internal/inbox"
)

// The startup failures listed in design-broker.md. Run wraps each with the
// concrete cause, so cmd can pick an exit code with errors.Is while the
// readiness pipe gets the full text as one line.
var (
	// ErrNotRoot is returned by Run when --system is used without root.
	ErrNotRoot           = errors.New("the system Broker must run as root")
	ErrInSession         = errors.New("the Broker must not run inside the process tree of a Claude Code session")
	ErrRuntimeDir        = errors.New("the runtime directory is not usable")
	ErrInvalidConfig     = errors.New("the configuration is invalid")
	ErrInvalidListen     = errors.New("the --listen address is invalid")
	ErrListenUnavailable = errors.New("the listen address is unavailable")
	ErrLockLost          = errors.New("the account lock file was replaced; exiting so that only one Broker runs")
)

// evLocalServeFailed records an error returned by the local gRPC server. It is
// not a startup failure: the Broker keeps running, and only the listener that
// failed stops accepting.
const evLocalServeFailed = "local_serve_failed"

// errSystemWiring reports a system run whose access wiring is incomplete. The
// system form must never start without the checker the system assembler built.
func errSystemWiring(detail string) error {
	return fmt.Errorf("system mode access wiring is incomplete: %s", detail)
}

// runtimeTimestampRefresher sets the account lock, the runtime directory and
// the local socket to the same moment, so a /tmp cleanup rule that deletes old
// files does not remove the running Broker's lock. Paths are set without
// following a symbolic link. Each target logs its failure only when it starts
// failing and logs the recovery when it succeeds again, so a long-running
// failure does not fill the log. A failure never stops the Broker: the files
// still exist, and the next tick tries again.
type runtimeTimestampRefresher struct {
	lockFailed bool
	dirFailed  bool
	sockFailed bool
}

func (r *runtimeTimestampRefresher) refresh(lock *AccountLock, lockPath, runtimeDir, sockPath string, now time.Time, touch func(*AccountLock, time.Time) error, logger *slog.Logger) {
	if err := touch(lock, now); err != nil {
		if !r.lockFailed {
			logger.Warn(EvAccountLockTouchFailed, "path", lockPath, "error", err)
		}
		r.lockFailed = true
	} else if r.lockFailed {
		logger.Info(EvAccountLockTouchRecovered, "path", lockPath)
		r.lockFailed = false
	}
	r.refreshPath(runtimeDir, EvRuntimeDirTouchFailed, EvRuntimeDirTouchRecovered, &r.dirFailed, now, logger)
	r.refreshPath(sockPath, EvLocalSocketTouchFailed, EvLocalSocketTouchRecovered, &r.sockFailed, now, logger)
}

func (r *runtimeTimestampRefresher) refreshPath(path, failedEvent, recoveredEvent string, failed *bool, now time.Time, logger *slog.Logger) {
	if path == "" {
		return
	}
	ts := []unix.Timespec{unix.NsecToTimespec(now.UnixNano()), unix.NsecToTimespec(now.UnixNano())}
	if err := unix.UtimesNanoAt(unix.AT_FDCWD, path, ts, unix.AT_SYMLINK_NOFOLLOW); err != nil {
		if !*failed {
			logger.Warn(failedEvent, "path", path, "error", err)
		}
		*failed = true
		return
	}
	if *failed {
		logger.Info(recoveredEvent, "path", path)
		*failed = false
	}
}

// RunOptions is the process entry point's input to Run. The exported fields
// mirror the command line, except Ready, which is a test seam for reading the
// startup result. checkTick and afterCheck exist only so tests can drive the
// periodic runtime-directory review without waiting a minute.
type RunOptions struct {
	System               bool
	Listen               string
	AllowDegraded        bool
	Ready                io.WriteCloser // tests only; nil means no readiness pipe
	LogOutput            io.Writer
	Version              string
	LocalProtocolVersion uint32      // 0: localpb.ProtocolVersion
	LinkProtocolVersion  uint32      // 0: linkpb.ProtocolVersion
	Assemblers           []Assembler // nil: DefaultAssemblers()
	Components           []Component
	Deps                 Deps

	checkTick  <-chan time.Time // tests only; nil: time.NewTicker(time.Minute)
	afterCheck func()           // tests only; called after every periodic check
	// touchLock replaces AccountLock.Touch in tests that need the periodic
	// refresh to fail without disturbing the lock itself.
	touchLock func(*AccountLock, time.Time) error

	// afterUnsuppress runs in the recovery path after the config component's
	// own rebind is enabled again and before broker.listen is read once more.
	// Tests use it to deliver a change in exactly that window.
	afterUnsuppress func()

	// afterListenFailure runs after a local-only retry failure reports its
	// state. Tests use it to read the address and the still-bound address
	// without depending on polling timing.
	afterListenFailure func(addr, bound string)
}

// liveBroker is a fully started Broker (normal or local only) that Run now
// serves until shutdown.
type liveBroker struct {
	b        *Broker
	sock     *LocalSocket
	started  []Component
	asm      *Assembly
	cfg      ConfigSource
	deps     Deps
	degraded *localpb.DegradedState // nil when the listen address is bound
}

// Run starts a Broker and blocks until ctx is cancelled or a periodic check
// fails. The startup sequence is Load, the assemblers, New, the component Start
// methods, binding the link listener, binding the local socket, writing "ok",
// starting link, and the AfterReady methods; a failure at any point is written
// to the readiness pipe as one line and unwinds everything acquired so far.
//
// With AllowDegraded, a data-directory failure after the account lock enters
// diagnostics only: the process keeps the lock, serves a reduced local
// interface and retries the startup steps. A listen bind failure enters local
// only: the full local interface runs and the bind is retried. Every other
// failure after the lock stays fatal, and no failure before the lock is ever
// degraded.
func Run(ctx context.Context, opts RunOptions) error {
	// Step 0: defaults. The logger is created here rather than inside New
	// because the assemblers log through deps.Logger and they run before New.
	deps := opts.Deps

	level := new(slog.LevelVar)
	level.Set(slog.LevelInfo)
	if deps.Logger == nil {
		out := opts.LogOutput
		if out == nil {
			out = os.Stderr
		}
		deps.Logger = NewLogger(out, level)
		slog.SetDefault(deps.Logger)
	}
	if deps.Clock == nil {
		deps.Clock = SystemClock()
	}
	if deps.Config == nil {
		deps.Config = PackageConfig()
	}
	if deps.InSession == nil {
		deps.InSession = inbox.InSession
	}
	deps.LocalProtocolVersion = opts.LocalProtocolVersion
	deps.Version = opts.Version
	deps.System = opts.System
	// RequestShutdown cancels runCtx rather than the caller's context: an
	// ApplyUpdate success then takes the same shutdown path as a signal, and
	// Run returns nil so the command exits with its normal code.
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	deps.RequestShutdown = stopRun
	baseDeps := deps
	cfg := deps.Config

	writeFailure := func(err error) {
		deps.Logger.Error(EvStartupFailed, "error", err.Error())
		writeReadyLine(opts.Ready, err.Error())
	}

	// Step 1: the system form requires root. The steps below then use the
	// system paths and the accounts on this host, so this check stays before
	// the account lock and the configuration load.
	if opts.System {
		geteuid := baseDeps.Geteuid
		if geteuid == nil {
			geteuid = os.Geteuid
		}
		if euid := geteuid(); euid != 0 {
			err := fmt.Errorf("%w: --system requires root (euid %d)", ErrNotRoot, euid)
			writeFailure(err)
			return err
		}
	}

	// Step 2: never run inside a Claude Code session's process tree.
	if baseDeps.InSession() {
		err := fmt.Errorf("%w: CLAUDE_CODE_MESSAGING_SOCKET is set", ErrInSession)
		writeFailure(err)
		return err
	}

	// Step 3: prepare the runtime directory and take the account lock. The
	// lock is taken before Load so that a second instance fails before it
	// touches any configuration.
	if err := cfg.CheckRuntimeDir(); err != nil {
		err = fmt.Errorf("%w: %v", ErrRuntimeDir, err)
		writeFailure(err)
		return err
	}
	lockPath := cfg.AccountLock()
	lock, err := AcquireAccountLock(lockPath)
	if err != nil {
		writeFailure(err)
		return err
	}

	// startOnce runs the post-lock startup sequence. It undoes everything it
	// acquired before returning a failure, except the account lock, which the
	// caller owns. reuseSock is non-nil only when a diagnostics-only run is
	// retrying and keeps its bound local socket.
	startOnce := func(reuseSock *LocalSocket, fallback config.PathSet, fallbackGroup, fallbackMode string) (*liveBroker, *startupFailure) {
		d := baseDeps
		asm := &Assembly{System: opts.System}
		var (
			started    []Component
			assembled  []Component
			runnerRan  bool
			loaded     bool
			startPhase bool
			sock       = reuseSock
			b          *Broker
		)

		stopComponents := func(comps []Component) {
			for i := len(comps) - 1; i >= 0; i-- {
				comps[i].Stop()
			}
		}
		cleanup := func() {
			if d.Delivery != nil {
				d.Delivery.stop()
			}
			if asm.Runner != nil && runnerRan {
				asm.Runner.Close()
			}
			if startPhase {
				stopComponents(started)
			} else {
				stopComponents(assembled)
			}
			if sock != nil && reuseSock == nil {
				_ = sock.Close(true)
				sock = nil
			}
			if loaded {
				cfg.Close()
			}
		}
		paths := fallback
		group, mode := fallbackGroup, fallbackMode
		failWith := func(step string, err error) *startupFailure {
			cleanup()
			return &startupFailure{step: step, err: err, system: opts.System, paths: paths, socketGroup: group, socketMode: mode}
		}

		// Step 4: load the configuration and follow log.level.
		cfgMode := config.ModeUser
		if opts.System {
			cfgMode = config.ModeSystem
		}
		if err := cfg.Load(cfgMode, opts.Listen); err != nil {
			sf := &startupFailure{step: "config", err: err, system: opts.System, paths: fallback, socketGroup: fallbackGroup, socketMode: fallbackMode}
			if de, ok := err.(*config.DataUnavailableError); ok {
				sf.paths, sf.socketGroup, sf.socketMode = de.Paths, de.LocalSocketGroup, de.LocalSocketMode
			}
			// The cause is wrapped, not flattened, so a degraded retry can
			// still tell an invalid value from a parse or read failure.
			switch {
			case errors.Is(err, config.ErrDataUnavailable):
				// The data-directory class is the only one the caller may
				// degrade. Without --allow-degraded it keeps the old
				// configuration-failure shape and exit code.
				if !opts.AllowDegraded {
					sf.err = fmt.Errorf("%w: %w", ErrInvalidConfig, err)
				}
			case opts.Listen != "" && errors.Is(err, config.ErrInvalidValue) && strings.Contains(err.Error(), "broker.listen"):
				sf.err = fmt.Errorf("%w: %w", ErrInvalidListen, err)
			default:
				sf.err = fmt.Errorf("%w: %w", ErrInvalidConfig, err)
			}
			cleanup()
			return nil, sf
		}
		loaded = true
		paths = cfg.Paths()
		if opts.System {
			if v, err := cfg.Get("broker.local_socket_group"); err == nil {
				group = v.String
			}
			if v, err := cfg.Get("broker.local_socket_mode"); err == nil {
				mode = v.String
			}
		}

		// SetConfig reads this to know whether the run started with --listen;
		// set it only after Load accepted the value.
		d.ListenOverride = opts.Listen

		if v, err := cfg.Get("log.level"); err == nil && v.Kind == config.KindString {
			if lv, err := ParseLogLevel(v.String); err == nil {
				level.Set(lv)
			}
		}
		cfg.Subscribe("log.level", func(_, newVal config.Value) {
			if newVal.Kind != config.KindString {
				return
			}
			lv, err := ParseLogLevel(newVal.String)
			if err != nil {
				// Keep the current level for an invalid value.
				return
			}
			level.Set(lv)
			d.Logger.Info(EvLogLevelChanged, "level", newVal.String)
		})

		// System mode: the runtime directory and the local socket are shared by
		// every account, so their group and mode come from the configuration. The
		// group is checked before anything binds, and both keys are watched so a
		// change is applied to the socket that is already there.
		sockPath := filepath.Join(paths.RuntimeDir, "broker.sock")
		var applySocketPerms func() error
		if opts.System {
			applySocketPerms = func() error {
				g, err := cfg.Get("broker.local_socket_group")
				if err != nil {
					return err
				}
				m, err := cfg.Get("broker.local_socket_mode")
				if err != nil {
					return err
				}
				return applyLocalSocketPerms(sockPath, paths.RuntimeDir, g.String, m.String)
			}
			cfg.Validate("broker.local_socket_group", validateSocketGroup)
			if err := checkSocketGroupAtStartup(cfg); err != nil {
				return nil, failWith("config", err)
			}
			watchSocketPerms(cfg, applySocketPerms, d.Logger)
		}

		// Step 5: run the assemblers, then append the options' components.
		assemblers := assemblersFor(opts)
		asm.LinkProtocolVersion = opts.LinkProtocolVersion
		for _, a := range assemblers {
			comps, err := a.Assemble(ctx, cfg, &d, asm)
			if err != nil {
				return nil, failWith("assemblers", fmt.Errorf("%s: %w", a.Name(), err))
			}
			assembled = append(assembled, comps...)
		}

		// System mode fails closed: without the config assembler there is no access
		// interceptor at all, and a checker other than the one the system assembler
		// built would let a caller choose the decision. Neither may start.
		if opts.System {
			var wiringErr error
			switch {
			case !asm.configAssembled:
				wiringErr = errSystemWiring("no config assembler ran; every local call would be unguarded")
			case asm.Access == nil:
				wiringErr = errSystemWiring("the system assembler installed no access checker")
			case asm.effectiveAccess != asm.Access:
				wiringErr = errSystemWiring("the effective access checker is not the system assembler's")
			}
			if wiringErr != nil {
				return nil, failWith("assemblers", wiringErr)
			}
		}
		components := make([]Component, 0, len(assembled)+len(opts.Components))
		components = append(components, assembled...)
		components = append(components, opts.Components...)

		// Step 6: construct the Broker from the (possibly modified) Deps.
		nb, err := New(d)
		if err != nil {
			return nil, failWith("broker", err)
		}
		b = nb

		// Step 7: start the components in order. If one fails, the failing
		// component and every component not started yet are stopped as well, so
		// a retry does not inherit goroutines or open files they already built.
		startPhase = true
		pending := append([]Component(nil), components...)
		for len(pending) > 0 {
			c := pending[0]
			if err := c.Start(ctx, b); err != nil {
				startErr := fmt.Errorf("%s: %w", c.Name(), err)
				stopComponents(started)
				stopComponents(pending)
				started = nil
				pending = nil
				return nil, failWith("components", startErr)
			}
			started = append(started, c)
			pending = pending[1:]
		}

		// Step 8: bind the link listener, using the effective broker.listen. An
		// empty address skips this step. With AllowDegraded a failure here does
		// not abort startup: the local interface runs and the bind is retried.
		var listenState *localpb.DegradedState
		if asm.Runner != nil {
			v, err := cfg.Get("broker.listen")
			if err != nil {
				return nil, failWith("config", fmt.Errorf("%w: %v", ErrInvalidConfig, err))
			}
			runnerRan = true
			if v.String != "" {
				// The privileged-port rule is a listen rule. It is checked
				// here, not in Load, so a refusal enters local-only degraded
				// mode and the foreground exit code is the listen one.
				err := cfg.CheckLowPort()
				if err == nil {
					err = asm.Runner.Listen(v.String)
				}
				if err != nil {
					wrapped := fmt.Errorf("%w: %s: %w", ErrListenUnavailable, v.String, err)
					if !opts.AllowDegraded {
						return nil, failWith("listen", wrapped)
					}
					listenState = degradedStateFor(&startupFailure{
						step: "listen", err: wrapped, cause: err, addr: v.String, system: opts.System,
						paths: paths, socketGroup: group, socketMode: mode,
					})
					d.Logger.Error(EvStartupFailed, "error", wrapped.Error())
					d.Logger.Info(EvDegradedEntered,
						"level", localpb.DegradedLevelName(listenState.Level),
						"step", listenState.Step,
						"reason", listenState.Reason,
					)
				}
			}
			if listenState == nil {
				// Tell the components that forward broker.listen what is bound, so
				// a change delivered before their AfterReady is reconciled against
				// it.
				for _, c := range started {
					if r, ok := c.(listenBoundReporter); ok {
						r.listenBound(v.String)
					}
				}
			}
		}

		// Step 9: bind the local socket. Only the lock holder reaches this
		// point. A diagnostics-only retry reuses the socket it already bound.
		if sock == nil {
			s, err := BindLocalSocket(sockPath, 0o600)
			if err != nil {
				return nil, failWith("local socket", fmt.Errorf("%w: %v", ErrRuntimeDir, err))
			}
			sock = s
		}
		if opts.System && applySocketPerms != nil {
			if err := applySocketPerms(); err != nil {
				if reuseSock == nil {
					_ = sock.Close(true)
					sock = nil
				}
				return nil, failWith("local socket", fmt.Errorf("%w: %v", ErrInvalidConfig, err))
			}
		}

		return &liveBroker{
			b:        b,
			sock:     sock,
			started:  started,
			asm:      asm,
			cfg:      cfg,
			deps:     d,
			degraded: listenState,
		}, nil
	}

	// serveLive runs a fully started Broker until shutdown.
	serveLive := func(live *liveBroker) error {
		b := live.b
		sock := live.sock
		started := live.started
		asm := live.asm
		liveCfg := live.cfg
		liveDeps := live.deps
		paths := liveCfg.Paths()
		sockPath := filepath.Join(paths.RuntimeDir, "broker.sock")

		if live.degraded != nil {
			_ = b.setDegraded(live.degraded)
		}
		// Step 10: the readiness pipe, when a test supplies one, reports success
		// only after both listeners exist. The local service is served from here on.
		writeReadyLine(opts.Ready, "ok")
		deps.Logger.Info(EvBrokerStarted, "socket", sockPath, "pid", os.Getpid(), "version", opts.Version)
		go serveLocal(b, sock.Listener(), deps.Logger)

		// Step 11: start link after the local socket is ready. A local-only
		// Broker still starts link: it may be the dialing side of a peer.
		if asm.Runner != nil {
			asm.Runner.Start()
		}

		// A local-only Broker retries the bind with backoff and reacts to a
		// broker.listen change. While the retry owns binding, the config
		// component must not bind the same address from its subscription.
		var hasRebinder bool
		for _, c := range started {
			if _, ok := c.(interface{ rebind(string) error }); ok {
				hasRebinder = true
				break
			}
		}
		// rebindListeners goes through the component that owns the link's
		// listener, so its bound-address check deduplicates the rebind and its
		// lock serializes it with the config subscription. Without such a
		// component (unit tests), bind the runner directly.
		rebindListeners := func(address string) error {
			if hasRebinder {
				for _, c := range started {
					if r, ok := c.(interface{ rebind(string) error }); ok {
						if err := r.rebind(address); err != nil {
							return err
						}
					}
				}
				return nil
			}
			return asm.Runner.Listen(address)
		}
		var retrier *listenRetrier
		if live.degraded != nil {
			suppressListenRebind(started, true)
			retrier = newListenRetrier(
				func(addr string) error {
					if err := liveCfg.CheckLowPort(); err != nil {
						return err
					}
					return asm.Runner.Listen(addr)
				},
				func() (string, error) {
					if opts.Listen != "" {
						return opts.Listen, nil
					}
					v, err := liveCfg.Get("broker.listen")
					if err != nil {
						return "", err
					}
					return v.String, nil
				},
				deps.Clock,
				deps.Logger,
				func(addr string) (string, error) {
					// Enable the component's own rebinding first and only then
					// read broker.listen once more. A change that arrives while
					// suppression is being lifted is therefore either handled
					// by the subscription or seen by this read; the component's
					// bound-address check makes the two paths idempotent.
					for _, c := range started {
						if r, ok := c.(listenBoundReporter); ok {
							r.listenBound(addr)
						}
					}
					suppressListenRebind(started, false)
					if opts.Listen == "" {
						if opts.afterUnsuppress != nil {
							opts.afterUnsuppress()
						}
						v, err := liveCfg.Get("broker.listen")
						if err != nil {
							suppressListenRebind(started, true)
							return addr, err
						}
						if hasRebinder {
							if err := rebindListeners(v.String); err != nil {
								suppressListenRebind(started, true)
								return v.String, err
							}
						} else if v.String != addr {
							if err := rebindListeners(v.String); err != nil {
								suppressListenRebind(started, true)
								return v.String, err
							}
						}
						addr = v.String
					}
					b.setDegraded(nil)
					deps.Logger.Info(EvDegradedLeft, "listen_address", addr)
					return addr, nil
				},
				func(addr, bound string, err error) {
					logListenRebindFailure(deps.Logger, addr, err)
					// The retry owns the reported failure while local only: a
					// later attempt can fail on a different address or reason.
					state := degradedStateFor(&startupFailure{
						step: "listen", addr: addr, cause: err, system: opts.System,
						err:   fmt.Errorf("%w: %s: %w", ErrListenUnavailable, addr, err),
						paths: paths,
					})
					if bound != "" && bound != addr {
						// link.Listen binds the new address before closing the
						// old one, so a failed rebind leaves the old listener
						// serving and the report must not hide it.
						state.Reason += fmt.Sprintf("; the previous address %s is still bound", bound)
					}
					if b.setDegraded(state) {
						deps.Logger.Info(EvDegradedReasonChanged,
							"level", localpb.DegradedLevelName(state.Level),
							"step", state.Step,
							"reason", state.Reason,
						)
					}
					if opts.afterListenFailure != nil {
						opts.afterListenFailure(addr, bound)
					}
				},
			)
			retrier.start()
			if opts.Listen == "" {
				liveCfg.Subscribe("broker.listen", func(_, _ config.Value) { retrier.retryNow() })
			}
		}

		shutdown := func() {
			deps.Logger.Info(EvBrokerStopping)
			if retrier != nil {
				retrier.stop()
			}
			// Cancel received-request processing before link Close waits for its
			// handlers; a blocked inbox write must not hold shutdown open.
			if liveDeps.Delivery != nil {
				liveDeps.Delivery.stop()
			}
			if asm.Runner != nil {
				asm.Runner.Close()
			}
			b.Stop()
			for i := len(started) - 1; i >= 0; i-- {
				started[i].Stop()
			}
			_ = sock.Close(true)
			liveCfg.Close()
			lock.Release()
		}

		// Step 12: components that need the ready socket and link run now.
		for _, c := range started {
			pc, ok := c.(PostReadyComponent)
			if !ok {
				continue
			}
			if err := pc.AfterReady(ctx); err != nil {
				deps.Logger.Error("after_ready_failed", "component", c.Name(), "error", err)
				shutdown()
				return err
			}
		}

		// Step 13: start the periodic update check. A non-release build never
		// checks, so the initial delay is not even armed. The check reads the
		// configuration on every period and keeps its result in memory only.
		if isReleaseVersion(deps.Version) {
			b.startUpdateChecks(ctx)
		}

		// Step 14: run until cancellation, checking the lock and the local socket
		// once per tick.
		tick := opts.checkTick
		if tick == nil {
			ticker := time.NewTicker(time.Minute)
			defer ticker.Stop()
			tick = ticker.C
		}
		refresher := &runtimeTimestampRefresher{}
		for {
			select {
			case <-runCtx.Done():
				shutdown()
				return nil
			case <-tick:
				if !lock.StillHeld() {
					deps.Logger.Error(EvLockLost)
					shutdown()
					return ErrLockLost
				}
				if !sock.Present() {
					ns, err := BindLocalSocket(sockPath, 0o600)
					if err != nil {
						deps.Logger.Warn(EvLocalSocketRebindFailed, "error", err)
					} else {
						if opts.System {
							if err := applySocketPermsFor(liveCfg, sockPath, paths.RuntimeDir); err != nil {
								deps.Logger.Error(evLocalSocketPermsFailed, "error", err.Error())
							}
						}
						old := sock
						sock = ns
						go serveLocal(b, ns.Listener(), deps.Logger)
						_ = old.Close(false)
						deps.Logger.Info(EvLocalSocketRebound, "socket", sockPath)
					}
				}
				touch := opts.touchLock
				if touch == nil {
					touch = func(l *AccountLock, now time.Time) error { return l.Touch(now) }
				}
				refresher.refresh(lock, lockPath, paths.RuntimeDir, sockPath, deps.Clock.Now(), touch, deps.Logger)
				if opts.afterCheck != nil {
					opts.afterCheck()
				}
			}
		}
	}

	// The first startup attempt.
	live, sf := startOnce(nil, config.PathSet{}, "", "")
	if sf == nil {
		return serveLive(live)
	}
	if !opts.AllowDegraded || !errors.Is(sf.err, config.ErrDataUnavailable) {
		writeFailure(sf.err)
		lock.Release()
		return sf.err
	}

	// Diagnostics only: bind the local socket if the failed attempt did not
	// have one, serve the reduced interface, and retry from Load.
	var (
		diagSock     *LocalSocket
		diagSvc      *degradedService
		diagStarted  bool
		diagSockPath string
		diagPaths    = sf.paths
		diagGroup    = sf.socketGroup
		diagMode     = sf.socketMode
		diagState    degradedHolder
	)
	fatalDegraded := func(err error) error {
		writeFailure(err)
		if diagSvc != nil {
			diagSvc.Stop()
		}
		if diagSock != nil {
			_ = diagSock.Close(true)
		}
		lock.Release()
		return err
	}

	startDiagService := func(ln net.Listener) (*degradedService, error) {
		geteuid := baseDeps.Geteuid
		if geteuid == nil {
			geteuid = os.Geteuid
		}
		readPeerCred := baseDeps.ReadPeerCred
		allow := func(c Caller) bool { return c.UID == uint32(geteuid()) }
		if opts.System {
			allow = func(Caller) bool { return true }
		}
		return startDegradedService(
			ln, diagState.get, opts.Version, baseDeps.LocalProtocolVersion,
			readPeerCred, allow, deps.Logger,
		)
	}
	updateDiagConfig := func(sf *startupFailure) {
		if sf.paths.RuntimeDir != "" {
			diagPaths = sf.paths
		}
		if sf.socketGroup != "" {
			diagGroup = sf.socketGroup
		}
		if sf.socketMode != "" {
			diagMode = sf.socketMode
		}
	}

	enterDiagnostics := func(sf *startupFailure) error {
		state := degradedStateFor(sf)
		changed := diagState.set(state)
		switch {
		case changed && !diagStarted:
			deps.Logger.Error(EvStartupFailed, "error", sf.err.Error())
			deps.Logger.Info(EvDegradedEntered,
				"level", localpb.DegradedLevelName(state.Level),
				"step", state.Step,
				"reason", state.Reason,
			)
		case changed:
			// Already serving diagnostics only: a retry that fails for a new
			// reason updates the report instead of announcing another entry.
			deps.Logger.Info(EvDegradedReasonChanged,
				"level", localpb.DegradedLevelName(state.Level),
				"step", state.Step,
				"reason", state.Reason,
			)
		}
		updateDiagConfig(sf)
		sockPath := deriveSockPath(diagPaths)
		if sockPath == "" {
			return fmt.Errorf("%w: no runtime directory is known", ErrRuntimeDir)
		}
		if diagSock != nil {
			// A retry can change the configured group or mode; apply it to the
			// socket that is already serving.
			if opts.System {
				if err := applyLocalSocketPerms(diagSockPath, diagPaths.RuntimeDir, diagGroup, diagMode); err != nil {
					deps.Logger.Error(evLocalSocketPermsFailed, "error", err.Error())
				}
			}
			return nil
		}
		s, err := BindLocalSocket(sockPath, 0o600)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrRuntimeDir, err)
		}
		if opts.System {
			if err := applyLocalSocketPerms(sockPath, diagPaths.RuntimeDir, diagGroup, diagMode); err != nil {
				_ = s.Close(true)
				return fmt.Errorf("%w: %v", ErrInvalidConfig, err)
			}
		}
		svc, err := startDiagService(s.Listener())
		if err != nil {
			_ = s.Close(true)
			return err
		}
		diagSock, diagSvc, diagSockPath = s, svc, sockPath
		diagStarted = true
		return nil
	}

	// rebindDiagSocket mirrors the full service's periodic check: if something
	// removed the socket file, bind it again and serve the reduced interface on
	// it, so the next retry finds a working local interface.
	rebindDiagSocket := func() {
		if diagSock == nil || diagSock.Present() {
			return
		}
		sockPath := deriveSockPath(diagPaths)
		if sockPath == "" {
			return
		}
		s, err := BindLocalSocket(sockPath, 0o600)
		if err != nil {
			deps.Logger.Warn(EvLocalSocketRebindFailed, "error", err)
			return
		}
		if opts.System {
			if err := applyLocalSocketPerms(sockPath, diagPaths.RuntimeDir, diagGroup, diagMode); err != nil {
				deps.Logger.Error(evLocalSocketPermsFailed, "error", err.Error())
			}
		}
		svc, err := startDiagService(s.Listener())
		if err != nil {
			_ = s.Close(true)
			deps.Logger.Warn(EvLocalSocketRebindFailed, "error", err)
			return
		}
		oldSvc, oldSock := diagSvc, diagSock
		diagSock, diagSvc, diagSockPath = s, svc, sockPath
		oldSvc.Stop()
		_ = oldSock.Close(false)
		deps.Logger.Info(EvLocalSocketRebound, "socket", sockPath)
	}

	if err := enterDiagnostics(sf); err != nil {
		return fatalDegraded(err)
	}

	watch := startConfigDirWatch(diagPaths.ConfigFile, deps.Logger)
	tick := opts.checkTick
	if tick == nil {
		ticker := time.NewTicker(time.Minute)
		defer ticker.Stop()
		tick = ticker.C
	}
	refresher := &runtimeTimestampRefresher{}
	for {
		select {
		case <-runCtx.Done():
			watch.stopNow()
			diagSvc.Stop()
			_ = diagSock.Close(true)
			lock.Release()
			return nil
		case <-tick:
			if !lock.StillHeld() {
				watch.stopNow()
				diagSvc.Stop()
				_ = diagSock.Close(true)
				lock.Release()
				return ErrLockLost
			}
			rebindDiagSocket()
			touch := opts.touchLock
			if touch == nil {
				touch = func(l *AccountLock, now time.Time) error { return l.Touch(now) }
			}
			refresher.refresh(lock, lockPath, diagPaths.RuntimeDir, deriveSockPath(diagPaths), deps.Clock.Now(), touch, deps.Logger)
		case <-watch.events:
		}

		live, sf := startOnce(diagSock, diagPaths, diagGroup, diagMode)
		if sf == nil {
			watch.stopNow()
			diagSvc.Stop()
			diagSvc = nil
			if live.degraded == nil {
				deps.Logger.Info(EvDegradedLeft)
			}
			// Entering local only logs degraded_entered with the final level;
			// a fieldless degraded_left here would hide which level remains.
			return serveLive(live)
		}
		// A retry failure never exits while diagnostics only; a configuration
		// that has become unparsable keeps the loop and updates the reason.
		if err := enterDiagnostics(sf); err != nil {
			return fatalDegraded(err)
		}
	}
}

// applySocketPermsFor reapplies the local socket group and mode after a
// rebind. It reads the live configuration.
func applySocketPermsFor(cfg ConfigSource, sockPath, runtimeDir string) error {
	group, err := cfg.Get("broker.local_socket_group")
	if err != nil {
		return err
	}
	mode, err := cfg.Get("broker.local_socket_mode")
	if err != nil {
		return err
	}
	return applyLocalSocketPerms(sockPath, runtimeDir, group.String, mode.String)
}

// suppressListenRebind toggles the config component's own broker.listen
// rebinding. Run's listen retry owns the address while local only.
func suppressListenRebind(components []Component, suppress bool) {
	for _, c := range components {
		if s, ok := c.(interface{ suppressListenRebind(bool) }); ok {
			s.suppressListenRebind(suppress)
		}
	}
}

// serveLocal serves the local interface and records an unexpected error. A
// closed listener, from Stop or from a rebind, is expected and ignored.
func serveLocal(b *Broker, ln net.Listener, log *slog.Logger) {
	err := b.Serve(ln)
	if err == nil || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, net.ErrClosed) {
		return
	}
	log.Warn(evLocalServeFailed, "error", err)
}

// writeReadyLine writes one line to the readiness pipe and closes it. A nil
// pipe is a no-op; only tests supply one.
func writeReadyLine(w io.WriteCloser, text string) {
	if w == nil {
		return
	}
	_, _ = io.WriteString(w, strings.ReplaceAll(text, "\n", " ")+"\n")
	_ = w.Close()
}
