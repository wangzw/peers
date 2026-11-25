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

// Run's startup failures, wrapped with the concrete cause so the command can
// pick an exit code with errors.Is and the readiness pipe gets the full text on
// one line.
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

const evLocalServeFailed = "local_serve_failed"

func errSystemWiring(detail string) error {
	return fmt.Errorf("system mode access wiring is incomplete: %s", detail)
}

// Refreshing the lock file's timestamp keeps a cleanup rule that deletes old
// files from removing a live lock.
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
// mirror the command line.
type RunOptions struct {
	// System runs the system form.
	System bool
	// Listen is the --listen address; empty means use broker.listen.
	Listen string
	// AllowDegraded permits a degraded run on a recoverable startup failure.
	AllowDegraded bool
	Ready         io.WriteCloser // tests only; nil means no readiness pipe
	// LogOutput receives the run log; nil means standard error.
	LogOutput io.Writer
	// Version is the version this process reports.
	Version              string
	LocalProtocolVersion uint32      // 0: localpb.ProtocolVersion
	LinkProtocolVersion  uint32      // 0: linkpb.ProtocolVersion
	Assemblers           []Assembler // nil: DefaultAssemblers()
	// Components are started after the assemblers' components.
	Components []Component
	// Deps are the dependencies passed to New.
	Deps Deps

	checkTick          <-chan time.Time // tests only; nil: time.NewTicker(time.Minute)
	afterCheck         func()
	touchLock          func(*AccountLock, time.Time) error
	afterUnsuppress    func()
	afterListenFailure func(addr, bound string)
}

type liveBroker struct {
	b        *Broker
	sock     *LocalSocket
	started  []Component
	asm      *Assembly
	cfg      ConfigSource
	deps     Deps
	degraded *localpb.DegradedState // nil when the listen address is bound
}

func otherAccountLockPaths(candidates []string, own string) []string {
	own = filepath.Clean(own)
	var others []string
	for _, path := range candidates {
		if filepath.Clean(path) != own {
			others = append(others, path)
		}
	}
	return others
}

// Run starts a Broker and blocks until ctx is cancelled or a periodic check
// fails. With AllowDegraded, a data-directory failure after the account lock
// enters diagnostics only and a listen bind failure enters local only; every
// other failure after the lock is fatal, and no failure before the lock is
// degraded.
//
// The startup sequence is Load, the assemblers, New, the component Start
// methods, binding the link listener, binding the local socket, writing "ok",
// starting link, and the AfterReady methods; a failure at any point is written
// to the readiness pipe as one line and unwinds everything acquired so far.
func Run(ctx context.Context, opts RunOptions) error {
	// The logger is created here rather than inside New because the assemblers
	// log through deps.Logger and run before New.
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
	// RequestShutdown cancels runCtx so an ApplyUpdate success takes the same
	// shutdown path as a signal and Run returns nil.
	runCtx, stopRun := context.WithCancel(ctx)
	defer stopRun()
	deps.RequestShutdown = stopRun
	baseDeps := deps
	cfg := deps.Config

	writeFailure := func(err error) {
		deps.Logger.Error(EvStartupFailed, "error", err.Error())
		writeReadyLine(opts.Ready, err.Error())
	}

	// The root check must precede the account lock and the configuration load,
	// so neither runs for a non-root --system run.
	if opts.System {
		if euid := effectiveGeteuid(baseDeps)(); euid != 0 {
			err := fmt.Errorf("%w: --system requires root (euid %d)", ErrNotRoot, euid)
			writeFailure(err)
			return err
		}
	}

	if baseDeps.InSession() {
		err := fmt.Errorf("%w: this process or one of its parent processes belongs to a Claude Code session", ErrInSession)
		writeFailure(err)
		return err
	}

	if err := cfg.CheckRuntimeDir(); err != nil {
		err = fmt.Errorf("%w: %v", ErrRuntimeDir, err)
		writeFailure(err)
		return err
	}
	// Take the lock before Load so a second instance fails before it touches any
	// configuration.
	lockPath := cfg.AccountLock()
	lock, err := AcquireAccountLock(lockPath)
	if err != nil {
		writeFailure(err)
		return err
	}
	// The account's Broker may be running from the other runtime directory, so a
	// process resolving to the freed preferred path must still see it.
	if err := checkAccountLockAvailable(otherAccountLockPaths(cfg.AccountLockCandidates(), lockPath)); err != nil {
		lock.Release()
		writeFailure(err)
		return err
	}

	// A degraded run retries Load, so the fallback notice is written once.
	fallbackLogged := false

	// On failure it undoes everything it acquired except the account lock, which
	// the caller owns.
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
		if fb := paths.RuntimeDirFallback; fb != nil && !fallbackLogged {
			fallbackLogged = true
			deps.Logger.Warn(EvRuntimeDirFallback,
				"taken", fb.TakenPath,
				"owner_uid", fb.OwnerUID,
				"path", paths.RuntimeDir,
			)
		}
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
				return
			}
			level.Set(lv)
			d.Logger.Info(EvLogLevelChanged, "level", newVal.String)
		})

		sockPath := filepath.Join(paths.RuntimeDir, "broker.sock")
		var applySocketPerms func() error
		if opts.System {
			applySocketPerms = func() error {
				return applySocketPermsFor(cfg, sockPath, paths.RuntimeDir)
			}
			// The group is checked before anything binds, so a bad value fails
			// startup.
			cfg.Validate("broker.local_socket_group", validateSocketGroup)
			if err := checkSocketGroupAtStartup(cfg); err != nil {
				return nil, failWith("config", err)
			}
			watchSocketPerms(cfg, applySocketPerms, d.Logger)
		}

		assemblers := assemblersFor(opts)
		asm.LinkProtocolVersion = opts.LinkProtocolVersion
		for _, a := range assemblers {
			comps, err := a.Assemble(ctx, cfg, &d, asm)
			if err != nil {
				return nil, failWith("assemblers", fmt.Errorf("%s: %w", a.Name(), err))
			}
			assembled = append(assembled, comps...)
		}

		// System mode fails closed: without the system assembler's checker every
		// local call would be unguarded.
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

		nb, err := New(d)
		if err != nil {
			return nil, failWith("broker", err)
		}
		b = nb

		// A failed component stops the started ones and the remaining pending ones.
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

		var listenState *localpb.DegradedState
		if asm.Runner != nil {
			v, err := cfg.Get("broker.listen")
			if err != nil {
				return nil, failWith("config", fmt.Errorf("%w: %v", ErrInvalidConfig, err))
			}
			runnerRan = true
			if v.String != "" {
				// The low-port rule is checked here, not in Load, so a refusal
				// enters local-only degradation.
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
				notifyListenBound(started, v.String)
			}
		}

		// Only the account lock holder binds the local socket.
		if sock == nil {
			s, err := BindLocalSocket(sockPath, 0o600, d.Logger)
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
		// Report success only after the link listener and the local socket are
		// bound.
		writeReadyLine(opts.Ready, "ok")
		deps.Logger.Info(EvBrokerStarted, "socket", sockPath, "pid", os.Getpid(), "version", opts.Version)
		go serveLocal(b, sock.Listener(), deps.Logger)

		// Link starts after the local socket is ready; a local-only Broker still
		// starts it because it may be the dialing side of a peer.
		if asm.Runner != nil {
			asm.Runner.Start()
		}

		// While the retry owns binding, the config component must not bind the
		// same address from its subscription.
		var hasRebinder bool
		for _, c := range started {
			if _, ok := c.(interface{ rebind(string) error }); ok {
				hasRebinder = true
				break
			}
		}
		// The component that owns the link's listener deduplicates the rebind and
		// serializes it with the config subscription; without one, bind directly.
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
					// Enable the component's rebinding before reading broker.listen
					// again, so a change during the lift is either handled by the
					// subscription or seen by this read; the bound-address check
					// makes the two idempotent.
					notifyListenBound(started, addr)
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
					state := degradedStateFor(&startupFailure{
						step: "listen", addr: addr, cause: err, system: opts.System,
						err:   fmt.Errorf("%w: %s: %w", ErrListenUnavailable, addr, err),
						paths: paths,
					})
					if bound != "" && bound != addr {
						// A failed rebind leaves the old listener serving, so name it.
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
			// Cancel requests before link Close waits for its handlers, so a blocked
			// inbox write does not hold shutdown open.
			if liveDeps.Delivery != nil {
				liveDeps.Delivery.stop()
			}
			if asm.Runner != nil {
				asm.Runner.Close()
			}
			b.Stop()
			stopComponents(started)
			_ = sock.Close(true)
			// The configuration must close after the link has stopped and its
			// handlers returned: the offline reply reads broker.alias afresh and
			// would see an empty string.
			liveCfg.Close()
			lock.Release()
		}

		// Components that need the ready socket and the started link run now.
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

		if isReleaseVersion(deps.Version) {
			b.startUpdateChecks(ctx)
		}

		tick, ticker := checkTicks(opts.checkTick)
		if ticker != nil {
			defer ticker.Stop()
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
					ns, err := BindLocalSocket(sockPath, 0o600, deps.Logger)
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
				refresher.refresh(lock, lockPath, paths.RuntimeDir, sockPath, deps.Clock.Now(), touchLockFunc(opts.touchLock), deps.Logger)
				if opts.afterCheck != nil {
					opts.afterCheck()
				}
			}
		}
	}

	live, sf := startOnce(nil, config.PathSet{}, "", "")
	if sf == nil {
		return serveLive(live)
	}
	if !opts.AllowDegraded || !errors.Is(sf.err, config.ErrDataUnavailable) {
		writeFailure(sf.err)
		lock.Release()
		return sf.err
	}

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
		geteuid := effectiveGeteuid(baseDeps)
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
			if opts.System {
				if err := applyLocalSocketPerms(diagSockPath, diagPaths.RuntimeDir, diagGroup, diagMode); err != nil {
					deps.Logger.Error(evLocalSocketPermsFailed, "error", err.Error())
				}
			}
			return nil
		}
		s, err := BindLocalSocket(sockPath, 0o600, deps.Logger)
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

	rebindDiagSocket := func() {
		if diagSock == nil || diagSock.Present() {
			return
		}
		sockPath := deriveSockPath(diagPaths)
		if sockPath == "" {
			return
		}
		s, err := BindLocalSocket(sockPath, 0o600, deps.Logger)
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
	tick, ticker := checkTicks(opts.checkTick)
	if ticker != nil {
		defer ticker.Stop()
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
			refresher.refresh(lock, lockPath, diagPaths.RuntimeDir, deriveSockPath(diagPaths), deps.Clock.Now(), touchLockFunc(opts.touchLock), deps.Logger)
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
			// Entering local only logs degraded_entered with the final level, so no
			// fieldless degraded_left is logged here.
			return serveLive(live)
		}
		// A retry failure never exits while diagnostics only; an unparsable
		// configuration keeps the loop and updates the reason.
		if err := enterDiagnostics(sf); err != nil {
			return fatalDegraded(err)
		}
	}
}

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

func effectiveGeteuid(d Deps) func() int {
	if d.Geteuid != nil {
		return d.Geteuid
	}
	return os.Geteuid
}

// Stops in reverse start order, so a component stops before the ones started
// before it.
func stopComponents(comps []Component) {
	for i := len(comps) - 1; i >= 0; i-- {
		comps[i].Stop()
	}
}

func notifyListenBound(components []Component, address string) {
	for _, c := range components {
		if r, ok := c.(listenBoundReporter); ok {
			r.listenBound(address)
		}
	}
}

func checkTicks(ch <-chan time.Time) (<-chan time.Time, *time.Ticker) {
	if ch != nil {
		return ch, nil
	}
	t := time.NewTicker(time.Minute)
	return t.C, t
}

func touchLockFunc(fn func(*AccountLock, time.Time) error) func(*AccountLock, time.Time) error {
	if fn != nil {
		return fn
	}
	return func(l *AccountLock, now time.Time) error { return l.Touch(now) }
}

func suppressListenRebind(components []Component, suppress bool) {
	for _, c := range components {
		if s, ok := c.(interface{ suppressListenRebind(bool) }); ok {
			s.suppressListenRebind(suppress)
		}
	}
}

func serveLocal(b *Broker, ln net.Listener, log *slog.Logger) {
	err := b.Serve(ln)
	if err == nil || errors.Is(err, grpc.ErrServerStopped) || errors.Is(err, net.ErrClosed) {
		return
	}
	log.Warn(evLocalServeFailed, "error", err)
}

func writeReadyLine(w io.WriteCloser, text string) {
	if w == nil {
		return
	}
	_, _ = io.WriteString(w, strings.ReplaceAll(text, "\n", " ")+"\n")
	_ = w.Close()
}
