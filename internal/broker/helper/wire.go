package helper

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"sync"

	"google.golang.org/protobuf/encoding/protodelim"
	"google.golang.org/protobuf/proto"
)

// maxMessageSize bounds one framed message so neither end of a pipe can make the other allocate an arbitrary amount of memory.
const maxMessageSize = 4 << 20

// errOversize reports a message whose encoded size exceeds maxMessageSize; a peer would reject it, so sending it can only break the connection.
var errOversize = errors.New("message exceeds the size limit")

// msgWriter writes length-delimited protobuf messages, holding mu so one message's bytes stay contiguous under concurrent callers.
type msgWriter struct {
	mu sync.Mutex
	w  io.Writer
}

func newMsgWriter(w io.Writer) *msgWriter {
	return &msgWriter{w: w}
}

func (m *msgWriter) write(msg proto.Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if size := proto.Size(msg); size > maxMessageSize {
		return fmt.Errorf("%w: %d bytes, limit %d", errOversize, size, maxMessageSize)
	}
	var buf bytes.Buffer
	if _, err := protodelim.MarshalTo(&buf, msg); err != nil {
		return err
	}
	n, err := m.w.Write(buf.Bytes())
	if err != nil {
		return err
	}
	if n != buf.Len() {
		return io.ErrShortWrite
	}
	return nil
}

type msgReader struct {
	r *bufio.Reader
}

func newMsgReader(r io.Reader) *msgReader {
	if br, ok := r.(*bufio.Reader); ok {
		return &msgReader{r: br}
	}
	return &msgReader{r: bufio.NewReader(r)}
}

// read returns io.EOF at a message boundary and io.ErrUnexpectedEOF in the middle of a message.
func (m *msgReader) read(msg proto.Message) error {
	opts := protodelim.UnmarshalOptions{MaxSize: maxMessageSize}
	return opts.UnmarshalFrom(m.r, msg)
}
