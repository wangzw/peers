package broker

import (
	"errors"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"os"
	"sync"
	"syscall"
)

// LocalSocket is the Unix socket serving the local gRPC interface; it removes
// its path only when that path still names its own socket.
type LocalSocket struct {
	mu     sync.Mutex
	path   string
	ln     *net.UnixListener
	dev    uint64
	ino    uint64
	closed bool
}

// afterListen is a test seam that runs after each successful ListenUnix and
// before the socket's mode is set.
var afterListen = func() {}

const bindAttempts = 3

func attemptCountText(n int) string {
	if n == 1 {
		return "1 attempt"
	}
	return fmt.Sprintf("%d attempts", n)
}

// BindLocalSocket binds the local interface to path with file mode perm. A
// stale socket is replaced; any other file at path is left alone and reported
// as an error. The caller must hold the account lock.
func BindLocalSocket(path string, perm os.FileMode, logger *slog.Logger) (*LocalSocket, error) {
	if logger == nil {
		logger = slog.Default()
	}
	fi, err := os.Lstat(path)
	switch {
	case err == nil:
		if fi.Mode()&os.ModeSocket == 0 {
			return nil, fmt.Errorf("refusing to replace %s: not a socket", path)
		}
		if err := os.Remove(path); err != nil {
			return nil, fmt.Errorf("remove stale socket %s: %w", path, err)
		}
	case errors.Is(err, fs.ErrNotExist):
	default:
		return nil, fmt.Errorf("inspect %s: %w", path, err)
	}

	retried := false

	for attempt := 1; ; attempt++ {
		ln, err := net.ListenUnix("unix", &net.UnixAddr{Name: path, Net: "unix"})
		if err != nil {
			return nil, err
		}
		// The listener must never unlink the path on Close: by then another
		// Broker may own a freshly bound socket there.
		ln.SetUnlinkOnClose(false)
		afterListen()

		if err := os.Chmod(path, perm); err != nil {
			_ = ln.Close()
			if errors.Is(err, fs.ErrNotExist) && attempt < bindAttempts {
				logger.Info(EvLocalSocketBindRetry, "socket", path, "attempt", attempt)
				retried = true
				continue
			}
			_ = os.Remove(path)
			return nil, fmt.Errorf("cannot set mode on %s after %s: %w", path, attemptCountText(attempt), err)
		}
		fi, err := os.Lstat(path)
		if err != nil {
			_ = ln.Close()
			if errors.Is(err, fs.ErrNotExist) && attempt < bindAttempts {
				logger.Info(EvLocalSocketBindRetry, "socket", path, "attempt", attempt)
				retried = true
				continue
			}
			_ = os.Remove(path)
			return nil, fmt.Errorf("cannot stat socket %s after %s: %w", path, attemptCountText(attempt), err)
		}
		st, ok := fi.Sys().(*syscall.Stat_t)
		if !ok {
			_ = ln.Close()
			_ = os.Remove(path)
			return nil, fmt.Errorf("stat socket %s: missing file identity", path)
		}
		if retried {
			logger.Info(EvLocalSocketRebound, "socket", path)
		}
		return &LocalSocket{
			path: path,
			ln:   ln,
			dev:  uint64(st.Dev),
			ino:  uint64(st.Ino),
		}, nil
	}
}

// Listener returns the underlying listener so the caller can serve the local
// gRPC service on it.
func (s *LocalSocket) Listener() net.Listener {
	return s.ln
}

// Present reports whether path still names the socket this LocalSocket bound.
func (s *LocalSocket) Present() bool {
	fi, err := os.Lstat(s.path)
	if err != nil {
		return false
	}
	if fi.Mode()&os.ModeSocket == 0 {
		return false
	}
	st, ok := fi.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	return uint64(st.Dev) == s.dev && uint64(st.Ino) == s.ino
}

// Close closes the listener. When unlink is true and path still names this
// socket, it also removes the path. Close is safe to call more than once.
func (s *LocalSocket) Close(unlink bool) error {
	s.mu.Lock()
	ln := s.ln
	if !s.closed {
		s.closed = true
	}
	s.mu.Unlock()

	var closeErr error
	if ln != nil {
		closeErr = ln.Close()
		if errors.Is(closeErr, net.ErrClosed) {
			closeErr = nil
		}
	}
	if unlink && s.Present() {
		if err := os.Remove(s.path); err != nil && !errors.Is(err, fs.ErrNotExist) {
			if closeErr != nil {
				return errors.Join(closeErr, err)
			}
			return err
		}
	}
	return closeErr
}
