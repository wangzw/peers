package helper

import (
	"context"
	"errors"
	"io"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/wangzw/peers/internal/inbox"
)

// Compile-time checks that the inbox functions still have the signatures the helper package adapts.
var (
	_ func(string, string) (inbox.Session, error)                                       = inbox.Lookup
	_ func(inbox.Session) (inbox.Session, error)                                        = inbox.Reread
	_ func(string, func(int)) (func(), error)                                           = inbox.Watch
	_ func(context.Context, inbox.Session, string, string, string, time.Duration) error = inbox.Deliver
)

// TestInboxOpsLookupDelegates checks that Lookup returns the record's session fields.
func TestInboxOpsLookupDelegates(t *testing.T) {
	dir := t.TempDir()
	sessions := filepath.Join(dir, "sessions")
	if err := os.MkdirAll(sessions, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	record := `{"pid":4242,"name":"a","status":"idle","messagingSocketPath":"/x.sock"}`
	if err := os.WriteFile(filepath.Join(sessions, "4242.json"), []byte(record), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	s, err := InboxOps{}.Lookup(dir, "/x.sock")
	if err != nil {
		t.Fatalf("Lookup: %v", err)
	}
	if s.PID != 4242 {
		t.Errorf("PID = %d, want 4242", s.PID)
	}
	if s.Name != "a" {
		t.Errorf("Name = %q, want %q", s.Name, "a")
	}
}

// TestInboxOpsLookupMissingIsNoRecord checks that a missing record is reported as inbox.ErrNoRecord.
func TestInboxOpsLookupMissingIsNoRecord(t *testing.T) {
	dir := t.TempDir()
	if _, err := (InboxOps{}).Lookup(dir, "/x.sock"); !errors.Is(err, inbox.ErrNoRecord) {
		t.Fatalf("Lookup error = %v, want inbox.ErrNoRecord", err)
	}
}

// TestInboxOpsRereadDelegates checks that Reread returns the record's fields and reports ErrNoRecord for a missing record.
func TestInboxOpsRereadDelegates(t *testing.T) {
	dir := t.TempDir()
	sessions := filepath.Join(dir, "sessions")
	if err := os.MkdirAll(sessions, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}
	record := `{"pid":4242,"name":"a","status":"busy","messagingSocketPath":"/x.sock"}`
	if err := os.WriteFile(filepath.Join(sessions, "4242.json"), []byte(record), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}

	s, err := InboxOps{}.Reread(inbox.Session{ConfigDir: dir, PID: 4242})
	if err != nil {
		t.Fatalf("Reread: %v", err)
	}
	if s.Name != "a" || s.Status != "busy" || s.SocketPath != "/x.sock" {
		t.Fatalf("Reread = %+v, want the record's fields", s)
	}
	if _, err := (InboxOps{}).Reread(inbox.Session{ConfigDir: dir, PID: 9999}); !errors.Is(err, inbox.ErrNoRecord) {
		t.Fatalf("Reread of a missing record = %v, want inbox.ErrNoRecord", err)
	}
}

// TestInboxOpsWatchDelegates checks that Watch reports a new session record to its callback and returns a stop.
func TestInboxOpsWatchDelegates(t *testing.T) {
	dir := t.TempDir()
	sessions := filepath.Join(dir, "sessions")
	if err := os.MkdirAll(sessions, 0o755); err != nil {
		t.Fatalf("MkdirAll: %v", err)
	}

	var mu sync.Mutex
	seen := map[int]bool{}
	stop, err := InboxOps{}.Watch(dir, func(pid int) {
		mu.Lock()
		seen[pid] = true
		mu.Unlock()
	})
	if err != nil {
		t.Fatalf("Watch: %v", err)
	}
	if stop == nil {
		t.Fatal("Watch returned a nil stop")
	}
	defer stop()

	record := `{"pid":4343,"name":"b","status":"idle","messagingSocketPath":"/y.sock"}`
	if err := os.WriteFile(filepath.Join(sessions, "4343.json"), []byte(record), 0o644); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	deadline := time.Now().Add(processTimeout)
	for {
		mu.Lock()
		got := seen[4343]
		mu.Unlock()
		if got {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the watch did not report the new session record")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

// TestInboxOpsDeliverDelegates checks that Deliver passes the request to inbox.Deliver and returns its ErrBadSignature.
func TestInboxOpsDeliverDelegates(t *testing.T) {
	err := InboxOps{}.Deliver(context.Background(), inbox.Session{SocketPath: "/s"}, "", "text", "m1", time.Second)
	if !errors.Is(err, inbox.ErrBadSignature) {
		t.Fatalf("Deliver with an empty signature = %v, want inbox.ErrBadSignature", err)
	}
}

// TestStubSignatures pins the signatures of Serve, NewClient, NewPool and RunAccessCheck.
func TestStubSignatures(t *testing.T) {
	var (
		_ = (func(context.Context, io.Reader, io.Writer, SessionOps, Identity) error)(Serve)
		_ = (func(io.Reader, io.WriteCloser) *Client)(NewClient)
		_ = (func(PoolOptions) *Pool)(NewPool)
		_ = (func(io.Reader, io.Writer) error)(RunAccessCheck)
	)
}
