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
	// brokerStderrPrefix starts the line a Broker writes to standard error when
	// it fails to start.
	brokerStderrPrefix = "peers service run: "
	// brokerProbeInterval is how often StartBroker tries the local socket while
	// the Broker process is starting.
	brokerProbeInterval = 50 * time.Millisecond
	// brokerProbeTimeout bounds one readiness probe. A Broker that has not
	// created its socket yet fails the probe quickly.
	brokerProbeTimeout = 250 * time.Millisecond
	// defaultBrokerBound is the bound used when the caller passes 0: ten
	// seconds for the Broker to bind its socket, plus three.
	defaultBrokerBound = 13 * time.Second
	// brokerKillWait is how long StartBroker waits for a killed Broker to be
	// reaped before giving up.
	brokerKillWait = 2 * time.Second
)

// BrokerSpec describes one Broker process to start.
type BrokerSpec struct {
	// Path is the peers binary.
	Path string
	// Args follow "service run" on the command line, for example
	// ["--listen", "127.0.0.1:41234"].
	Args []string
	// Env is the complete environment, normally built by BaseEnv.Build.
	Env []string
	// LogPath receives standard output and standard error, appended.
	LogPath string
	// Socket is the local socket used to decide that the Broker is ready.
	Socket string
}

// StartResult reports how StartBroker ended. When the Broker process exited
// before answering on its local socket, Running is false and ExitCode and
// StderrLast describe the failure.
type StartResult struct {
	Running    bool
	PID        int
	ExitCode   int
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
// has already been waited for. After a process is reaped its pid may name an
// unrelated process, so no signal may be sent through that pid.
var ErrBrokerReaped = errors.New("node: broker process has already been reaped")

// BrokerProc is one started Broker process.
type BrokerProc struct {
	cmd     *exec.Cmd
	logPath string
	pid     int

	// done closes once the process has been reaped and its exit code recorded.
	done chan struct{}

	// mu guards the lifecycle state a signal must not race with. SendSignal
	// holds mu across its reaped check and its send; the reaper sets reaped, and
	// performs the wait that makes the pid reusable, under the same mutex. On
	// Linux the exit is observed out of the lock with waitid(WNOWAIT) and then
	// collected with cmd.Wait under the lock. Elsewhere there is no non-reaping
	// observation, so the reaper polls wait4(WNOHANG) under the lock instead.
	// Either way a child that has exited but not been reaped is a zombie whose
	// pid cannot be reused, and no send can interleave with the reap itself.
	mu      sync.Mutex
	exit    int
	reaped  bool
	signals int

	// traceSends, when set, makes sendSignal record every signal it hands to
	// procsig together with the reaped mark at that moment. A test enables it
	// with TraceSends to prove no signal went out after the reap.
	traceSends bool
	sends      []SignalSend
}

// SignalSend is one signal a BrokerProc handed to procsig, with the handle's
// reaped mark at the moment of the send. A test reads the records to assert
// that no signal went out through a pid that may already be reusable.
type SignalSend struct {
	Sig    syscall.Signal
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
	// The child holds its own descriptor; the parent copy is not needed.
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

// newBrokerProc builds the handle for a started command and begins reaping it
// in the background.
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

// abandon kills a Broker that never became ready and returns the error
// StartBroker reports for it.
func (b *BrokerProc) abandon(bound time.Duration, socket string) error {
	// Go through the lifecycle guard like every other signal: a Broker that
	// exited and was reaped between the timeout and here must not be signalled
	// through a pid that may already name another process.
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

// brokerHello reports whether the local socket accepts a Hello.
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

// lastBrokerStderr returns the last line of the log that starts with
// brokerStderrPrefix, without the prefix. It returns "" when the log has no
// such line or cannot be read.
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

// SendSignal sends sig unless the Broker has been reaped. It holds the
// lifecycle mutex across its check and its send, and the reaper sets the reaped
// mark and reaps the process under the same mutex, so a signal can never reach
// a pid that has become reusable. A process this handle has already reaped is
// refused with ErrBrokerReaped.
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
// along with the reaped mark at the moment of the send. A test calls it before
// the Broker is signalled and reads the records with SignalSends. Sends made
// before it is called are not recorded.
func (b *BrokerProc) TraceSends() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.sends = nil
	b.traceSends = true
}

// SignalSends returns the signals recorded since TraceSends. Each record
// carries the handle's reaped mark at the moment of the send, so a test can
// assert that none was handed to procsig after the reap.
func (b *BrokerProc) SignalSends() []SignalSend {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]SignalSend(nil), b.sends...)
}

// recordSend appends one signal hand-off to the trace when it is enabled. The
// caller holds b.mu, the lock SendSignal holds across its reaped check and its
// send, so Reaped records the check's view at the moment of the send.
func (b *BrokerProc) recordSend(sig syscall.Signal) {
	if !b.traceSends {
		return
	}
	b.sends = append(b.sends, SignalSend{Sig: sig, Reaped: b.reaped})
}

// SignalsSent reports how many signals SendSignal delivered. A test uses it to
// assert that a reaped Broker received none.
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
// running; EINTR means the poll was interrupted before it could look; ECHILD
// means another waiter reaped the child first. When wpid names the child, the
// poll has reaped it and exit is the exit code the status encodes, matching
// exec.ExitError.ExitCode: the process's own code, or -1 when a signal ended
// it. Any other error leaves nothing to wait for, so the poll reports the reap
// with an unknown code rather than spinning forever.
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

// Proc is one process as read from the operating system. Env is nil when the
// environment could not be read, which happens for processes of other users.
type Proc struct {
	PID  int
	PPID int
	SID  int
	UID  int
	Args []string
	Env  map[string]string
}
