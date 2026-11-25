package service

import (
	"errors"
	"net"
	"testing"
)

// TestSystemPeerUIDReportsAClosedSocket checks that the error of the raw
// connection reaches the caller instead of a zero account being treated as a
// real uid.
func TestSystemPeerUIDReportsAClosedSocket(t *testing.T) {
	sock := shortProbeSocket(t)
	lis, err := net.Listen("unix", sock)
	if err != nil {
		t.Fatalf("listen on %s: %v", sock, err)
	}
	t.Cleanup(func() { _ = lis.Close() })

	conn, err := net.Dial("unix", sock)
	if err != nil {
		t.Fatalf("dial %s: %v", sock, err)
	}
	uc := conn.(*net.UnixConn)
	if err := uc.Close(); err != nil {
		t.Fatalf("close: %v", err)
	}

	uid, err := systemPeerUID(uc)
	if !errors.Is(err, net.ErrClosed) {
		t.Fatalf("systemPeerUID error = %v, want net.ErrClosed", err)
	}
	if uid != 0 {
		t.Errorf("uid = %d, want 0 when the socket is closed", uid)
	}
}
