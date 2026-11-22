package fakesession

import "testing"

// TestWaitFrameN checks that WaitFrameN waits for a specific frame and that a
// later WaitFrame continues after the highest frame already observed.
func TestWaitFrameN(t *testing.T) {
	f := New(t, Options{PID: 8040})

	sendFrameLine(t, f, userFrameLine(t, "first", "sender", "one"))
	sendFrameLine(t, f, userFrameLine(t, "second", "sender", "two"))
	sendFrameLine(t, f, userFrameLine(t, "third", "sender", "three"))

	second, ok := f.WaitFrameN(2, eventTimeout)
	if !ok {
		t.Fatal("second frame not received")
	}
	if second.MsgID != "second" {
		t.Fatalf("WaitFrameN(2) msg id = %q, want second", second.MsgID)
	}

	third, ok := f.WaitFrame(eventTimeout)
	if !ok {
		t.Fatal("third frame not received")
	}
	if third.MsgID != "third" {
		t.Fatalf("WaitFrame after WaitFrameN(2) msg id = %q, want third", third.MsgID)
	}

	// Absolute access to an earlier frame still works and does not move the
	// cursor backwards.
	first, ok := f.WaitFrameN(1, eventTimeout)
	if !ok {
		t.Fatal("first frame not received")
	}
	if first.MsgID != "first" {
		t.Fatalf("WaitFrameN(1) msg id = %q, want first", first.MsgID)
	}
}
