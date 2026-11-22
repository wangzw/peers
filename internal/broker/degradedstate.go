package broker

import (
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"sync"

	"google.golang.org/protobuf/proto"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/config"
)

// degradedHolder keeps the degraded state the diagnostics-only service reads.
// The service runs while the process has no *Broker, so the state cannot live
// on that type.
type degradedHolder struct {
	mu    sync.Mutex
	state *localpb.DegradedState
}

// set stores state and reports whether the reason changed.
func (h *degradedHolder) set(state *localpb.DegradedState) (changed bool) {
	var reason string
	if state != nil {
		reason = state.Reason
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	old := ""
	if h.state != nil {
		old = h.state.Reason
	}
	h.state = cloneDegraded(state)
	return old != reason
}

func (h *degradedHolder) get() *localpb.DegradedState {
	h.mu.Lock()
	defer h.mu.Unlock()
	return cloneDegraded(h.state)
}

func cloneDegraded(state *localpb.DegradedState) *localpb.DegradedState {
	if state == nil {
		return nil
	}
	return proto.Clone(state).(*localpb.DegradedState)
}

// setDegraded records the degraded state that Status, ListBrokers and
// CreatePairingKey report. A nil state means the Broker runs normally. The
// value is cloned so the caller keeps ownership of its copy. It reports
// whether the reported problem changed, so a retry can log a reason update
// instead of repeating the entry record.
func (b *Broker) setDegraded(state *localpb.DegradedState) (changed bool) {
	b.degradedMu.Lock()
	defer b.degradedMu.Unlock()
	oldStep, oldReason := "", ""
	if b.degraded != nil {
		oldStep, oldReason = b.degraded.Step, b.degraded.Reason
	}
	if state == nil {
		b.degraded = nil
		return oldStep != "" || oldReason != ""
	}
	b.degraded = cloneDegraded(state)
	return oldStep != state.Step || oldReason != state.Reason
}

// degradedState returns a clone of the current degraded state, or nil while
// the Broker runs normally.
func (b *Broker) degradedState() *localpb.DegradedState {
	b.degradedMu.Lock()
	defer b.degradedMu.Unlock()
	if b.degraded == nil {
		return nil
	}
	return cloneDegraded(b.degraded)
}

// startupFailure is one failed startup step. Run classifies it into a
// degraded level, or treats it as fatal when --allow-degraded was not given.
type startupFailure struct {
	step string
	err  error
	// cause is the unwrapped cause when err carries a classification
	// sentinel, so the degraded reason does not repeat the sentinel text.
	cause error

	// addr is the address a listen bind failed on.
	addr string

	// system records whether the Broker runs the system form, which decides
	// whether the repair hints need sudo.
	system bool

	// paths, socketGroup and socketMode describe the configuration that a
	// failure after Load can no longer read directly. A DataUnavailableError
	// from Load fills them; a later failure fills them from the loaded
	// configuration before it is closed.
	paths       config.PathSet
	socketGroup string
	socketMode  string
}

func (f *startupFailure) Error() string { return f.err.Error() }
func (f *startupFailure) Unwrap() error { return f.err }

// degradedStateFor renders the DegradedState a caller displays. The reason
// names no configuration value or secret; the fix is one line.
func degradedStateFor(f *startupFailure) *localpb.DegradedState {
	level := localpb.DegradedLevel_DEGRADED_LEVEL_DIAGNOSTICS_ONLY
	step := f.step
	var reason, fix string

	if errors.Is(f.err, ErrListenUnavailable) {
		level = localpb.DegradedLevel_DEGRADED_LEVEL_LOCAL_ONLY
		cause := f.cause
		if cause == nil {
			cause = f.err
		}
		reason = fmt.Sprintf("listen address %s is unavailable: %v", f.addr, cause)
		fix = listenFix(cause, f.paths, f.system)
	} else {
		kind, paths, ok := classifyDataUnavailable(f.err)
		if paths.DataDir != "" || paths.ConfigFile != "" || paths.AuditLog != "" {
			f.paths = paths
		}
		switch {
		case ok:
			step = kindStep(kind)
			reason = f.err.Error()
			fix = dataFix(kind, f.paths, f.system)
		case f.step == "config":
			// A configuration failure during a retry names the key and the
			// class only: the raw error can quote the configured value.
			reason = configReason(f.err)
			fix = genericFix(f.step, f.paths, f.system)
		default:
			reason = f.err.Error()
			fix = genericFix(f.step, f.paths, f.system)
		}
	}

	return &localpb.DegradedState{
		Level:  level,
		Step:   step,
		Reason: reason,
		Fix:    fix,
	}
}

func kindStep(kind dataKind) string {
	switch kind {
	case dataKindIdentity:
		return "identity"
	case dataKindPeers:
		return "peer table"
	case dataKindDirectory:
		return "directory"
	case dataKindAudit:
		return "audit log"
	case dataKindDataDir:
		return "data directory"
	default:
		return "data directory"
	}
}

// dataFix returns the repair hint for a data-directory class failure. Paths are
// not configuration values, so naming them is allowed. The design table puts
// the no-restart retry in the hint, since the Broker keeps retrying every
// minute.
func dataFix(kind dataKind, paths config.PathSet, system bool) string {
	target := paths.DataDir
	if kind == dataKindAudit && paths.AuditLog != "" {
		target = paths.AuditLog
	}
	if target == "" {
		target = "the data directory and the audit log"
	}
	return "check the owner and permissions of " + target + ", then run " + restartCommand(system) + "; otherwise the Broker retries every minute"
}

// restartCommand is the command that restarts the Broker at this level.
func restartCommand(system bool) string {
	if system {
		return "sudo peers service restart"
	}
	return "peers service restart"
}

// listenFix returns the repair hint for a listen bind failure. The low-port
// rule has its own remedy. A system Broker's commands need sudo.
func listenFix(err error, paths config.PathSet, system bool) string {
	sudo := ""
	if system {
		sudo = "sudo "
	}
	if errors.Is(err, config.ErrLowPortDenied) {
		if paths.ConfigFile != "" {
			return fmt.Sprintf("%schmod g-w %s, or set broker.listen to a port at or above 1024", sudo, paths.ConfigFile)
		}
		return "restrict the configuration file to root, or set broker.listen to a port at or above 1024"
	}
	if paths.ConfigFile != "" {
		edit := "edit broker.listen in " + paths.ConfigFile
		if system {
			edit += " with sudo"
		}
		return fmt.Sprintf("%speers config set broker.listen <free address>, or %s", sudo, edit)
	}
	return sudo + "peers config set broker.listen <free address>"
}

func genericFix(step string, paths config.PathSet, system bool) string {
	restart := restartCommand(system)
	switch step {
	case "config":
		if paths.ConfigFile != "" {
			return fmt.Sprintf("edit %s, correct the invalid value, and save the file; the Broker retries automatically", paths.ConfigFile)
		}
		return "correct the configuration file, then run " + restart
	default:
		return "inspect the startup_failed entry in the run log, then run " + restart
	}
}

// configReason names only the configuration key and the class of failure. The
// raw validation error can quote the configured value, which a degraded status
// must not do.
func configReason(err error) string {
	switch {
	case errors.Is(err, config.ErrInvalidValue):
		if key := invalidValueKey(err.Error()); key != "" {
			return "the configuration value for " + key + " is invalid"
		}
		return "a configuration value is invalid"
	case errors.Is(err, config.ErrInvalidCombination):
		return "the configuration has an invalid combination of values"
	case errors.Is(err, config.ErrTooManyAddresses):
		return "the configuration has too many advertised addresses"
	case strings.Contains(err.Error(), "parse error"):
		return "the configuration file cannot be parsed"
	default:
		return "the configuration file cannot be read"
	}
}

// invalidValueKey extracts the key from "config: invalid value: <key>: ...".
func invalidValueKey(text string) string {
	const marker = "invalid value: "
	i := strings.Index(text, marker)
	if i < 0 {
		return ""
	}
	rest := text[i+len(marker):]
	if j := strings.Index(rest, ":"); j >= 0 {
		rest = rest[:j]
	}
	return strings.TrimSpace(rest)
}

// deriveSockPath returns the local socket path from the best available path
// set. A failed Load leaves the package without paths, so the failure carries
// them.
func deriveSockPath(paths config.PathSet) string {
	if paths.RuntimeDir == "" {
		return ""
	}
	return filepath.Join(paths.RuntimeDir, "broker.sock")
}
