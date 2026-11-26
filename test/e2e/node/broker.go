package node

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/wangzw/peers/internal/broker/localpb"
)

const (
	// brokerStderrPrefix copies the startup-failure line prefix cmd/peers's
	// (*app).run writes; a product wording change must update this constant.
	brokerStderrPrefix  = "peers service run: "
	brokerProbeInterval = 50 * time.Millisecond
	brokerProbeTimeout  = 250 * time.Millisecond
	// defaultBrokerBound is the bound used when the caller passes 0: ten
	// seconds for the Broker to bind its socket, plus three.
	defaultBrokerBound = 13 * time.Second
	brokerKillWait     = 2 * time.Second
)

// BrokerSpec describes one Broker process to start.
type BrokerSpec struct {
	// Path is the peers binary.
	Path string
	// Args follow "service run --user" on the command line, for example
	// ["--listen", "127.0.0.1:41234"].
	Args []string
	// Env is the complete environment, normally built by BaseEnv.Build.
	Env []string
	// LogPath receives standard output and standard error, appended.
	LogPath string
	// Socket is the local socket used to decide that the Broker is ready.
	Socket string
}

// StartResult reports how StartBroker ended.
type StartResult struct {
	// Running reports whether the Broker answered on its local socket before
	// the bound expired.
	Running bool
	// PID is the Broker process ID.
	PID int
	// ExitCode is the exit code of a Broker that exited before becoming ready.
	ExitCode int
	// StderrLast is the last Broker failure line from its log, without the
	// prefix.
	StderrLast string
	// Addrs maps each address the one retry replaced to the address the
	// Broker actually used instead. It is nil when nothing was replaced.
	Addrs map[string]string
}

// Address returns the address the Broker used in place of requested: the
// replacement chosen by the retry, or requested when it was not replaced.
func (s StartResult) Address(requested string) string {
	if actual, ok := s.Addrs[requested]; ok {
		return actual
	}
	return requested
}

// ErrBrokerReaped reports that a signal was refused because the Broker process
// has already been waited for; its pid may name an unrelated process.
var ErrBrokerReaped = errors.New("node: broker process has already been reaped")

// BrokerProc is one started Broker process.
type BrokerProc struct {
	cmd     *exec.Cmd
	logPath string
	pid     int

	done chan struct{}

	// mu guards the lifecycle state a signal must not race with. SendSignal
	// holds it across the reaped check and the send, and the reaper sets reaped
	// and performs the wait that makes the pid reusable under the same mutex, so
	// no send can interleave with the reap.
	mu      sync.Mutex
	exit    int
	reaped  bool
	signals int

	traceSends bool
	sends      []SignalSend
}

// SignalSend is one signal a BrokerProc handed to procsig, with the handle's
// reaped mark at the moment of the send.
type SignalSend struct {
	// Sig is the signal that was sent.
	Sig syscall.Signal
	// Reaped reports whether the handle had already reaped the process.
	Reaped bool
}

// StartBroker starts the Broker and waits, until bound expires, for either the
// process to exit or its local socket to answer Hello. The process runs in its
// own session, so signals sent to the node agent do not reach it.
//
// A Broker that exits before becoming ready is not an error: the returned
// StartResult carries its exit code and the last failure line. An error is
// returned only when the process could not be started or bound expired.
func StartBroker(ctx context.Context, spec BrokerSpec, bound time.Duration) (*BrokerProc, StartResult, error) {
	if bound <= 0 {
		bound = defaultBrokerBound
	}

	logFile, err := os.OpenFile(spec.LogPath, os.O_CREATE|os.O_WRONLY|os.O_APPEND, 0o644)
	if err != nil {
		return nil, StartResult{}, fmt.Errorf("node: opening broker log %s: %w", spec.LogPath, err)
	}

	cmd := exec.Command(spec.Path, append([]string{"service", "run", "--user"}, spec.Args...)...)
	cmd.Env = spec.Env
	cmd.Stdout = logFile
	cmd.Stderr = logFile
	cmd.SysProcAttr = &syscall.SysProcAttr{Setsid: true}
	setDeathSignal(cmd.SysProcAttr)

	if err := cmd.Start(); err != nil {
		_ = logFile.Close()
		return nil, StartResult{}, fmt.Errorf("node: starting broker %s: %w", spec.Path, err)
	}
	_ = logFile.Close()

	proc := newBrokerProc(cmd, spec.LogPath)

	deadline := time.Now().Add(bound)
	ticker := time.NewTicker(brokerProbeInterval)
	defer ticker.Stop()

	for {
		select {
		case <-proc.done:
			return proc, StartResult{
				Running:    false,
				PID:        proc.pid,
				ExitCode:   proc.exitCode(),
				StderrLast: lastBrokerStderr(proc.logPath),
			}, nil
		case <-ticker.C:
			if brokerHello(ctx, spec.Socket) {
				return proc, StartResult{Running: true, PID: proc.pid}, nil
			}
			if time.Now().After(deadline) || ctx.Err() != nil {
				return nil, StartResult{}, proc.abandon(bound, spec.Socket)
			}
		case <-ctx.Done():
			return nil, StartResult{}, proc.abandon(bound, spec.Socket)
		}
	}
}

func newBrokerProc(cmd *exec.Cmd, logPath string) *BrokerProc {
	proc := &BrokerProc{
		cmd:     cmd,
		logPath: logPath,
		pid:     cmd.Process.Pid,
		done:    make(chan struct{}),
	}
	go proc.reap()
	return proc
}

func (b *BrokerProc) abandon(bound time.Duration, socket string) error {
	// Go through the lifecycle guard: a Broker reaped between the timeout and
	// here must not be signalled through a pid that may already be reused.
	_ = b.SendSignal(syscall.SIGKILL)
	select {
	case <-b.done:
	case <-time.After(brokerKillWait):
	}
	if socket == "" {
		return fmt.Errorf("node: broker process %d did not become ready within %s", b.pid, bound)
	}
	return fmt.Errorf("node: broker process %d did not answer on %s within %s", b.pid, socket, bound)
}

func brokerHello(ctx context.Context, socket string) bool {
	if socket == "" {
		return false
	}
	probeCtx, cancel := context.WithTimeout(ctx, brokerProbeTimeout)
	defer cancel()
	oracle, err := DialOracle(socket)
	if err != nil {
		return false
	}
	defer func() { _ = oracle.Close() }()
	_, err = oracle.Hello(probeCtx)
	if err == nil {
		return true
	}
	// A Broker that answers with a version mismatch is running; the probe
	// speaks the package's default version, which a test Broker may not.
	detail := ErrorDetailOf(err)
	return detail != nil && detail.GetCode() == localpb.ErrorCode_ERROR_CODE_VERSION_MISMATCH
}

func lastBrokerStderr(logPath string) string {
	file, err := os.Open(logPath)
	if err != nil {
		return ""
	}
	defer func() { _ = file.Close() }()

	last := ""
	scanner := bufio.NewScanner(file)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)
	for scanner.Scan() {
		line := strings.TrimRight(scanner.Text(), "\r")
		if rest, ok := strings.CutPrefix(line, brokerStderrPrefix); ok {
			last = rest
		}
	}
	return last
}

// PID returns the process ID of the Broker.
func (b *BrokerProc) PID() int { return b.pid }

// SendSignal sends sig unless the Broker has been reaped, in which case it
// returns ErrBrokerReaped. It holds the lifecycle mutex across its check and
// its send, so a signal can never reach a pid that has become reusable.
func (b *BrokerProc) SendSignal(sig syscall.Signal) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.reaped {
		return fmt.Errorf("%w (pid %d)", ErrBrokerReaped, b.pid)
	}
	return b.sendSignal(sig)
}

// Signal sends sig through SendSignal.
func (b *BrokerProc) Signal(sig syscall.Signal) error { return b.SendSignal(sig) }

// TraceSends makes the handle record every signal SendSignal hands to procsig,
// with the reaped mark at the moment of the send; sends made before the call
// are not recorded.
func (b *BrokerProc) TraceSends() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sends = nil
	b.traceSends = true
}

// SignalSends returns a copy of the signals recorded since TraceSends, each
// carrying the handle's reaped mark at the moment of the send.
func (b *BrokerProc) SignalSends() []SignalSend {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]SignalSend(nil), b.sends...)
}

// recordSend appends one signal hand-off to the trace when it is enabled. The
// caller holds b.mu, so Reaped records the check's view at the moment of the
// send.
func (b *BrokerProc) recordSend(sig syscall.Signal) {
	if !b.traceSends {
		return
	}
	b.sends = append(b.sends, SignalSend{Sig: sig, Reaped: b.reaped})
}

// SignalsSent reports how many signals SendSignal delivered.
func (b *BrokerProc) SignalsSent() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.signals
}

// Wait waits up to bound for the Broker to exit and returns its exit code. A
// process terminated by a signal reports -1.
func (b *BrokerProc) Wait(bound time.Duration) (int, error) {
	select {
	case <-b.done:
		b.mu.Lock()
		defer b.mu.Unlock()
		return b.exitCodeLocked(), nil
	case <-time.After(bound):
		return -1, fmt.Errorf("node: broker process %d did not exit within %s", b.pid, bound)
	}
}

// exitCode returns the exit code of the finished process. It must only be
// called after done is closed.
func (b *BrokerProc) exitCode() int {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.exitCodeLocked()
}

// exitCodeLocked returns the exit code recorded at the reap, or -1 when a
// signal terminated it. b.mu must be held, and done must be closed.
func (b *BrokerProc) exitCodeLocked() int {
	return b.exit
}

// interpretWait4 reads one wait4(WNOHANG) poll, the way the reaper on a
// platform without waitid(WNOWAIT) does. A zero wpid means the child is still
// running and EINTR that the poll was interrupted. When wpid names the child
// the poll has reaped it, and exit is the code the status encodes, as
// exec.ExitError.ExitCode reports it. Any other error leaves nothing to wait
// for, so the poll reports the reap with an unknown code.
func interpretWait4(pid, wpid int, err error, status syscall.WaitStatus) (reaped bool, exit int) {
	switch {
	case errors.Is(err, syscall.EINTR):
		return false, 0
	case err != nil:
		// ECHILD means another waiter reaped the child; any other error leaves
		// nothing to wait for either.
		return true, -1
	case wpid == 0:
		return false, 0
	case wpid == pid:
		return true, status.ExitStatus()
	default:
		return true, -1
	}
}

// Proc is one process as read from the operating system.
type Proc struct {
	// PID is the process ID.
	PID int
	// PPID is the parent process ID.
	PPID int
	// SID is the session ID.
	SID int
	// UID is the real user ID.
	UID int
	// Args is the command line, argv[0] included.
	Args []string
	// Env is the environment as a map, or nil when it could not be read, which
	// happens for processes of other users.
	Env map[string]string
}
