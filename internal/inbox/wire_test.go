package inbox

import (
	"bytes"
	"context"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"

	ccsock "github.com/PeterSR/claude-code-socket-transport"
)

// bWireReceiver is a fake inbox socket that records the exact bytes a client
// writes. Unlike fakesession's CloseAfterRead mode, it keeps reading after the
// frame line and closes the connection only once the client half-closes. The
// client then completes its own half-close and its wait for the receiver to
// close. Closing as soon as the line has been read works on Linux, where the
// half-close races ahead of the close, but fails on macOS: the vendored client
// delays its half-close by 150 ms there, by which time the receiver is already
// gone, and CloseWrite returns ENOTCONN.
type bWireReceiver struct {
	path   string
	result chan bWireResult
}

type bWireResult struct {
	data []byte
	err  error
}

// bStartWireReceiver binds a socket at a short path and starts accepting one
// connection. The socket is closed and the directory removed when the test
// ends.
func bStartWireReceiver(t *testing.T) *bWireReceiver {
	t.Helper()
	dir := bShortTempDir(t)
	path := filepath.Join(dir, "inbox.sock")
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatalf("binding wire receiver socket: %v", err)
	}
	t.Cleanup(func() { _ = listener.Close() })

	receiver := &bWireReceiver{path: path, result: make(chan bWireResult, 1)}
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			receiver.result <- bWireResult{err: err}
			return
		}
		defer func() { _ = conn.Close() }()
		data, err := io.ReadAll(conn)
		receiver.result <- bWireResult{data: data, err: err}
	}()
	return receiver
}

// bSendWireRaw sends m through the transport library itself and returns the
// exact frame line the client wrote, without the trailing newline. The client
// is configured without authentication, so the frame is the only line.
func bSendWireRaw(t *testing.T, receiver *bWireReceiver, m ccsock.Message) []byte {
	t.Helper()
	client := &ccsock.Client{NoAuth: true, Timeout: bDeliverTimeout}
	if _, err := client.SendToSocket(context.Background(), receiver.path, m); err != nil {
		t.Fatalf("library send failed: %v", err)
	}
	select {
	case result := <-receiver.result:
		if result.err != nil {
			t.Fatalf("reading wire bytes: %v", result.err)
		}
		return bytes.TrimSuffix(result.data, []byte("\n"))
	case <-time.After(aWaitTimeout):
		t.Fatalf("no wire bytes received within %s", aWaitTimeout)
		return nil
	}
}
