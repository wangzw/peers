package helper

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"sync"
	"testing"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/wrapperspb"
)

// TestWireConcurrentWritesDoNotInterleave checks that concurrent writes of large messages stay whole and unsplit.
func TestWireConcurrentWritesDoNotInterleave(t *testing.T) {
	const (
		writers   = 16
		perWriter = 50
		payload   = 64 << 10
		total     = writers * perWriter
	)

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	w := newMsgWriter(pw)

	var wg sync.WaitGroup
	errCh := make(chan error, writers)
	for g := 0; g < writers; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			fill := byte('a' + g)
			for i := 0; i < perWriter; i++ {
				n := uint64(g*perWriter + i)
				body := make([]byte, 8+payload)
				binary.BigEndian.PutUint64(body[:8], n)
				for j := 8; j < len(body); j++ {
					body[j] = fill
				}
				if err := w.write(wrapperspb.Bytes(body)); err != nil {
					errCh <- err
					return
				}
			}
		}(g)
	}
	go func() {
		wg.Wait()
		close(errCh)
	}()

	r := newMsgReader(pr)
	seen := make([]bool, total)
	for i := 0; i < total; i++ {
		var msg wrapperspb.BytesValue
		if err := r.read(&msg); err != nil {
			_ = pr.CloseWithError(err)
			t.Fatalf("read message %d: %v", i, err)
		}
		if got, want := len(msg.Value), 8+payload; got != want {
			_ = pr.CloseWithError(fmt.Errorf("message %d has length %d", i, got))
			t.Fatalf("message %d length = %d, want %d", i, got, want)
		}
		n := binary.BigEndian.Uint64(msg.Value[:8])
		if n >= total {
			_ = pr.CloseWithError(fmt.Errorf("message %d has sequence %d", i, n))
			t.Fatalf("message %d has sequence %d, want < %d", i, n, total)
		}
		if seen[n] {
			_ = pr.CloseWithError(fmt.Errorf("sequence %d repeated", n))
			t.Fatalf("sequence %d was read twice", n)
		}
		seen[n] = true
	}
	_ = pw.Close()

	for err := range errCh {
		t.Fatalf("write: %v", err)
	}
	for n, ok := range seen {
		if !ok {
			t.Fatalf("sequence %d was never read", n)
		}
	}
}

// TestWireOversizeRejected checks that the reader rejects a message above the size limit.
func TestWireOversizeRejected(t *testing.T) {
	var frame bytes.Buffer
	if _, err := protodelim.MarshalTo(&frame, wrapperspb.Bytes(make([]byte, maxMessageSize+1))); err != nil {
		t.Fatalf("marshal oversize frame: %v", err)
	}

	pr, pw := io.Pipe()
	defer func() { _ = pw.Close() }()
	go func() {
		_, _ = pw.Write(frame.Bytes())
		_ = pw.Close()
	}()

	r := newMsgReader(pr)
	var msg wrapperspb.BytesValue
	err := r.read(&msg)
	_ = pr.CloseWithError(err)

	if err == nil {
		t.Fatal("read of oversize message returned nil error")
	}
	if errors.Is(err, io.EOF) {
		t.Fatalf("read of oversize message returned io.EOF: %v", err)
	}
}

func bytesMessageOfSize(t *testing.T, size int) *wrapperspb.BytesValue {
	t.Helper()
	payload := size
	for {
		if payload < 0 {
			t.Fatalf("cannot build a message of exactly %d bytes", size)
		}
		msg := wrapperspb.Bytes(make([]byte, payload))
		got := proto.Size(msg)
		if got == size {
			return msg
		}
		payload -= got - size
	}
}

// TestWireMessageAtSizeLimitAccepted checks that a message exactly at the size limit round-trips.
func TestWireMessageAtSizeLimitAccepted(t *testing.T) {
	msg := bytesMessageOfSize(t, maxMessageSize)

	var buf bytes.Buffer
	if err := newMsgWriter(&buf).write(msg); err != nil {
		t.Fatalf("write at the size limit: %v", err)
	}

	var got wrapperspb.BytesValue
	if err := newMsgReader(bytes.NewReader(buf.Bytes())).read(&got); err != nil {
		t.Fatalf("read at the size limit: %v", err)
	}
	if size := proto.Size(&got); size != maxMessageSize {
		t.Fatalf("round-tripped size = %d, want %d", size, maxMessageSize)
	}
}

// TestWireMessageOverLimitNotWritten checks that an oversize message is refused before any byte is written.
func TestWireMessageOverLimitNotWritten(t *testing.T) {
	msg := bytesMessageOfSize(t, maxMessageSize+1)

	var buf bytes.Buffer
	err := newMsgWriter(&buf).write(msg)
	if !errors.Is(err, errOversize) {
		t.Fatalf("write over the size limit = %v, want errOversize", err)
	}
	if buf.Len() != 0 {
		t.Fatalf("wrote %d bytes of an oversize frame, want 0", buf.Len())
	}
}

// TestWireTruncatedMessage checks the io.ErrUnexpectedEOF and io.EOF outcomes at a truncated and an empty stream.
func TestWireTruncatedMessage(t *testing.T) {
	var buf bytes.Buffer
	if _, err := protodelim.MarshalTo(&buf, wrapperspb.Bytes(bytes.Repeat([]byte{'x'}, 1024))); err != nil {
		t.Fatalf("marshal: %v", err)
	}
	full := buf.Bytes()

	var msg wrapperspb.BytesValue

	r := newMsgReader(bytes.NewReader(full[:len(full)/2]))
	if err := r.read(&msg); !errors.Is(err, io.ErrUnexpectedEOF) {
		t.Fatalf("read of truncated message = %v, want io.ErrUnexpectedEOF", err)
	}

	r = newMsgReader(bytes.NewReader(nil))
	if err := r.read(&msg); !errors.Is(err, io.EOF) {
		t.Fatalf("read of empty stream = %v, want io.EOF", err)
	}
}
