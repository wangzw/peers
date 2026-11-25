package broker

import (
	"context"
	"strings"
	"testing"
	"time"

	"google.golang.org/grpc/status"

	"github.com/wangzw/peers/internal/broker/localpb"
	"github.com/wangzw/peers/internal/inbox"
	"github.com/wangzw/peers/internal/testkit/fakesession"
)

// TestNotSentMessagePrefix pins the fixed prefix of a certainly-not-delivered
// error.
func TestNotSentMessagePrefix(t *testing.T) {
	err := notSent(localpb.ErrorCode_ERROR_CODE_NOT_FOUND, "x %d", 1)
	if err.Code != localpb.ErrorCode_ERROR_CODE_NOT_FOUND {
		t.Fatalf("Code = %v, want NOT_FOUND", err.Code)
	}
	if err.Message != "Not sent: x 1" {
		t.Fatalf("Message = %q, want %q", err.Message, "Not sent: x 1")
	}
	if len(err.Origin) != 0 {
		t.Fatalf("Origin = %x, want empty", err.Origin)
	}
}

// TestUnknownResultCode pins the code and prefix of an unknown result.
func TestUnknownResultCode(t *testing.T) {
	err := unknownResult("y")
	if err.Code != localpb.ErrorCode_ERROR_CODE_DELIVERY_UNKNOWN {
		t.Fatalf("Code = %v, want DELIVERY_UNKNOWN", err.Code)
	}
	const prefix = "Delivery unknown: "
	if len(err.Message) < len(prefix) || err.Message[:len(prefix)] != prefix {
		t.Fatalf("Message = %q, want prefix %q", err.Message, prefix)
	}
	if len(err.Origin) != 0 {
		t.Fatalf("Origin = %x, want empty", err.Origin)
	}
}

// TestNotSentToStatus checks that this unit's errors convert to one ErrorDetail
// with the same code and message and an empty fingerprint.
func TestNotSentToStatus(t *testing.T) {
	err := notSent(localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE, "z")
	st, ok := status.FromError(toStatus(err))
	if !ok {
		t.Fatalf("toStatus did not return a status error")
	}
	details := st.Details()
	if len(details) != 1 {
		t.Fatalf("details = %d, want exactly 1", len(details))
	}
	d, ok := details[0].(*localpb.ErrorDetail)
	if !ok {
		t.Fatalf("detail type = %T, want *localpb.ErrorDetail", details[0])
	}
	if d.Code != localpb.ErrorCode_ERROR_CODE_NOT_VISIBLE {
		t.Fatalf("detail code = %v, want NOT_VISIBLE", d.Code)
	}
	if d.Message != "Not sent: z" {
		t.Fatalf("detail message = %q, want %q", d.Message, "Not sent: z")
	}
	if len(d.OriginatingBrokerFingerprint) != 0 {
		t.Fatalf("detail fingerprint = %x, want empty", d.OriginatingBrokerFingerprint)
	}
}

// TestInboxPackageDelegates checks that the inboxWriter adapter passes the message
// id and text through to inbox.Deliver and reaches the session socket.
func TestInboxPackageDelegates(t *testing.T) {
	fake := fakesession.New(t, fakesession.Options{Name: "api-worker"})
	session, err := inbox.Lookup(fake.ConfigDir(), fake.SocketPath())
	if err != nil {
		t.Fatalf("inbox.Lookup: %v", err)
	}

	const msgID = "3f2a1c4e-5b6d-4e8f-9a0b-1c2d3e4f5a6b"
	if err := (inboxPackage{}).Deliver(context.Background(), session, "api-worker@hostA", "hello", msgID, 2*time.Second); err != nil {
		t.Fatalf("Deliver: %v", err)
	}

	frame, ok := fake.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("no frame reached the session")
	}
	if frame.MsgID != msgID {
		t.Fatalf("frame MsgID = %q, want %q", frame.MsgID, msgID)
	}
	if frame.FromName != "api-worker@hostA" {
		t.Fatalf("frame FromName = %q, want %q", frame.FromName, "api-worker@hostA")
	}
	if !strings.Contains(frame.Content, "hello") {
		t.Fatalf("frame Content = %q, want it to contain the text", frame.Content)
	}
}
