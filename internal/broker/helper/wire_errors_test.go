package helper

import (
	"bufio"
	"bytes"
	"errors"
	"io"
	"testing"

	"google.golang.org/protobuf/types/known/wrapperspb"
)

// shortWriter reports one byte less written than it was given.
type shortWriter struct{}

func (shortWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

// TestMsgWriterMarshalErrorIsReturned checks that a message which fails to marshal is reported without writing any bytes.
func TestMsgWriterMarshalErrorIsReturned(t *testing.T) {
	// A proto string with invalid UTF-8 fails to marshal, which is not the
	// oversize refusal: the frame never reached the size check.
	msg := wrapperspb.String(string([]byte{0xff}))

	var buf bytes.Buffer
	err := newMsgWriter(&buf).write(msg)
	if err == nil {
		t.Fatal("write of an unmarshalable message returned nil")
	}
	if errors.Is(err, errOversize) {
		t.Fatalf("write error = %v, want a marshal error, not errOversize", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("wrote %d bytes of a message that failed to marshal, want 0", buf.Len())
	}
}

// TestMsgWriterShortWriteIsReported checks that a short write is reported as io.ErrShortWrite.
func TestMsgWriterShortWriteIsReported(t *testing.T) {
	err := newMsgWriter(shortWriter{}).write(wrapperspb.String("payload"))
	if !errors.Is(err, io.ErrShortWrite) {
		t.Fatalf("write error = %v, want io.ErrShortWrite", err)
	}
}

// TestMsgReaderReusesBufioReader checks that newMsgReader keeps a *bufio.Reader it is given and still decodes a frame.
func TestMsgReaderReusesBufioReader(t *testing.T) {
	var buf bytes.Buffer
	if err := newMsgWriter(&buf).write(wrapperspb.String("hello")); err != nil {
		t.Fatalf("write: %v", err)
	}

	br := bufio.NewReader(bytes.NewReader(buf.Bytes()))
	r := newMsgReader(br)
	if r.r != br {
		t.Fatal("newMsgReader wrapped a *bufio.Reader instead of using it directly")
	}

	var got wrapperspb.StringValue
	if err := r.read(&got); err != nil {
		t.Fatalf("read: %v", err)
	}
	if got.Value != "hello" {
		t.Fatalf("read value = %q, want %q", got.Value, "hello")
	}
}
