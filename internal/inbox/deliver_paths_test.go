package inbox

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"

	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// gDeliverTimeout bounds every delivery here, with margin above the vendored
// client's fixed 150 ms half-close delay on darwin: a shorter deadline can
// expire before CloseWrite and turn a delivery the receiver took into
// ErrUncertain.
const gDeliverTimeout = 2 * time.Second

const gNoFrameWait = 20 * time.Millisecond

// gWriteKeyFile publishes an inbox auth key the way a session would. The live
// pid with no start token outranks a dead owner, so the library would send the
// token if NoAuth were not set.
func gWriteKeyFile(t *testing.T, configDir string, pid int, socketPath, token string) {
	t.Helper()
	name, err := ccsock.KeyFileName(pid, socketPath)
	if err != nil {
		t.Fatalf("computing the key file name: %v", err)
	}
	dir := filepath.Join(configDir, "sessions")
	if err := os.MkdirAll(dir, 0o700); err != nil {
		t.Fatalf("creating %s: %v", dir, err)
	}
	body, err := json.Marshal(map[string]string{"peerToken": token})
	if err != nil {
		t.Fatalf("encoding the key file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
		t.Fatalf("writing the key file: %v", err)
	}
}

func gLibrarySend(t *testing.T, socketPath string, m ccsock.Message) error {
	t.Helper()
	client := &ccsock.Client{NoAuth: true, Timeout: gDeliverTimeout}
	_, err := client.SendToSocket(context.Background(), socketPath, m)
	return err
}

func gWantClass(t *testing.T, libErr error, prefix string, want error) {
	t.Helper()
	if libErr == nil {
		t.Fatalf("the library returned no error, want one with prefix %q", prefix)
	}
	if !strings.HasPrefix(libErr.Error(), prefix) {
		t.Fatalf("library error = %q, want prefix %q", libErr, prefix)
	}
	if got := classifySendError(libErr); got != want {
		t.Fatalf("classifySendError(%q) = %v, want the %v sentinel itself", libErr, got, want)
	}
}

// TestREQAgentMessaging024_NoAuthEvenWithKeyFile checks that Deliver sends no authentication line even when a matching key file exists.
func TestREQAgentMessaging024_NoAuthEvenWithKeyFile(t *testing.T) {
	cfg := t.TempDir()
	t.Setenv("CLAUDE_CONFIG_DIR", cfg)
	t.Setenv("CLAUDE_CODE_MESSAGING_SOCKET", "")
	t.Setenv("CLAUDE_CODE_MESSAGING_TOKEN", "")

	fake := fakesession.New(t, fakesession.Options{Name: "worker"})
	const token = "0123456789abcdef0123456789abcdef"
	gWriteKeyFile(t, cfg, os.Getpid(), fake.SocketPath(), token)

	// The key file must be discoverable, or the test cannot tell a missing
	// NoAuth from a token the library never found.
	if got, err := ccsock.LookupToken(fake.SocketPath()); err != nil || got != token {
		t.Fatalf("LookupToken = %q, %v; want the published token", got, err)
	}

	if err := Deliver(context.Background(), Session{SocketPath: fake.SocketPath()}, "sender", "hello", "msg-024-key", gDeliverTimeout); err != nil {
		t.Fatalf("Deliver returned %v, want nil", err)
	}
	frame, ok := fake.WaitFrame(gDeliverTimeout)
	if !ok {
		t.Fatalf("no frame arrived within %s", gDeliverTimeout)
	}
	if frame.Auth {
		t.Fatalf("receiver saw an auth line even though a matching key file exists: %q", frame.AuthRaw)
	}
	if len(frame.AuthRaw) != 0 {
		t.Fatalf("raw auth line = %q, want none", frame.AuthRaw)
	}
	if frame.Type != "user" {
		t.Fatalf("first frame type = %q, want user", frame.Type)
	}
	if frame.From != "" {
		t.Fatalf("from = %q, want empty", frame.From)
	}
}

// TestDeliver_SentinelsReturnedUnwrapped checks that each failure returns the sentinel itself rather than a wrapper.
func TestDeliver_SentinelsReturnedUnwrapped(t *testing.T) {
	ctx := context.Background()
	if err := Deliver(ctx, Session{}, "sender", "hello", "msg-empty-socket", gDeliverTimeout); err != ErrNotWritten {
		t.Fatalf("empty socket path: got %v, want the ErrNotWritten sentinel itself", err)
	}
	if err := Deliver(ctx, Session{SocketPath: "/nonexistent/inbox.sock"}, `bad"sig`, "hello", "msg-bad-sig", gDeliverTimeout); err != ErrBadSignature {
		t.Fatalf("bad signature: got %v, want the ErrBadSignature sentinel itself", err)
	}
	if _, err := Lookup(t.TempDir(), filepath.Join(t.TempDir(), "missing.sock")); err != ErrNoRecord {
		t.Fatalf("Lookup on an empty registry: got %v, want the ErrNoRecord sentinel itself", err)
	}

	fake := fakesession.New(t, fakesession.Options{Name: "worker"})
	err := Deliver(ctx, Session{SocketPath: fake.SocketPath()}, "sender", "", "msg-empty-text", gDeliverTimeout)
	if err != ErrNotWritten {
		t.Fatalf("empty message text: got %v, want the ErrNotWritten sentinel itself", err)
	}
	if _, ok := fake.WaitFrame(gNoFrameWait); ok {
		t.Fatal("an argument rejection still reached the socket")
	}
}

// TestDeliver_ClassificationAnchoredToLibrary checks that Deliver's error classification matches the vendored library's own errors.
func TestDeliver_ClassificationAnchoredToLibrary(t *testing.T) {
	t.Run("dial-failure", func(t *testing.T) {
		missing := filepath.Join(bShortTempDir(t), "absent.sock")
		m := ccsock.Message{Text: "hello", Priority: ccsock.PriorityNext, FromName: "sender", MsgID: "msg-dial"}
		libErr := gLibrarySend(t, missing, m)
		gWantClass(t, libErr, "ccsock: connecting to", ErrNotWritten)

		err := Deliver(context.Background(), Session{SocketPath: missing}, "sender", "hello", "msg-dial", gDeliverTimeout)
		if err != ErrNotWritten {
			t.Fatalf("Deliver returned %v, want the ErrNotWritten sentinel itself", err)
		}
	})

	t.Run("empty-text", func(t *testing.T) {
		fake := fakesession.New(t, fakesession.Options{Name: "worker"})
		m := ccsock.Message{Priority: ccsock.PriorityNext, FromName: "sender", MsgID: "msg-empty-text"}
		libErr := gLibrarySend(t, fake.SocketPath(), m)
		if libErr == nil || libErr.Error() != "ccsock: message text is empty" {
			t.Fatalf("library error = %v, want %q", libErr, "ccsock: message text is empty")
		}
		if got := classifySendError(libErr); got != ErrNotWritten {
			t.Fatalf("classifySendError(%q) = %v, want ErrNotWritten", libErr, got)
		}

		err := Deliver(context.Background(), Session{SocketPath: fake.SocketPath()}, "sender", "", "msg-empty-text", gDeliverTimeout)
		if err != ErrNotWritten {
			t.Fatalf("Deliver returned %v, want the ErrNotWritten sentinel itself", err)
		}
		if _, ok := fake.WaitFrame(gNoFrameWait); ok {
			t.Fatal("an empty message still reached the socket")
		}
	})

	t.Run("oversized-frame", func(t *testing.T) {
		fake := fakesession.New(t, fakesession.Options{Name: "worker"})
		text := strings.Repeat("a", 1<<20)
		m := ccsock.Message{Text: text, Priority: ccsock.PriorityNext, FromName: "sender", MsgID: "msg-big"}
		libErr := gLibrarySend(t, fake.SocketPath(), m)
		gWantClass(t, libErr, "ccsock: frame is", ErrNotWritten)

		err := Deliver(context.Background(), Session{SocketPath: fake.SocketPath()}, "sender", text, "msg-big", gDeliverTimeout)
		if err != ErrNotWritten {
			t.Fatalf("Deliver returned %v, want the ErrNotWritten sentinel itself", err)
		}
		if _, ok := fake.WaitFrame(gNoFrameWait); ok {
			t.Fatal("an oversized frame still reached the socket")
		}
	})

	t.Run("waiting-for-close", func(t *testing.T) {
		fake := fakesession.New(t, fakesession.Options{Name: "worker", Mode: fakesession.HoldOpen})
		m := ccsock.Message{Text: "hello", Priority: ccsock.PriorityNext, FromName: "sender", MsgID: "msg-hold"}
		libErr := gLibrarySend(t, fake.SocketPath(), m)
		if libErr == nil {
			t.Fatal("the library returned nil while the receiver held the connection open")
		}
		// On darwin the client's 150 ms half-close delay can consume the whole
		// deadline, in which case it reports the sending path instead; both mean
		// the frame may have arrived.
		if !strings.HasPrefix(libErr.Error(), "ccsock: waiting for") &&
			!strings.HasPrefix(libErr.Error(), "ccsock: sending to") {
			t.Fatalf("library error = %q, want a waiting or sending prefix", libErr)
		}
		if got := classifySendError(libErr); got != ErrUncertain {
			t.Fatalf("classifySendError(%q) = %v, want ErrUncertain", libErr, got)
		}

		err := Deliver(context.Background(), Session{SocketPath: fake.SocketPath()}, "sender", "hello", "msg-hold", gDeliverTimeout)
		if err != ErrUncertain {
			t.Fatalf("Deliver returned %v, want the ErrUncertain sentinel itself", err)
		}
	})

	t.Run("write-after-peer-close", func(t *testing.T) {
		fake := fakesession.New(t, fakesession.Options{Name: "worker", Mode: fakesession.CloseImmediately})
		// The text must exceed the socket send buffer but stay below the frame limit, so the write cannot finish before the peer closes and the library reports its writing path.
		text := strings.Repeat("a", 900*1024)
		m := ccsock.Message{Text: text, Priority: ccsock.PriorityNext, FromName: "sender", MsgID: "msg-write"}
		libErr := gLibrarySend(t, fake.SocketPath(), m)
		gWantClass(t, libErr, "ccsock: writing to", ErrUncertain)

		err := Deliver(context.Background(), Session{SocketPath: fake.SocketPath()}, "sender", text, "msg-write", gDeliverTimeout)
		if err != ErrUncertain {
			t.Fatalf("Deliver returned %v, want the ErrUncertain sentinel itself", err)
		}
	})
}
