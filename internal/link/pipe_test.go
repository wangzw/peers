package link

import (
	"errors"
	"sync"
	"testing"

	"github.com/wangzw/peers/internal/link/linkpb"
)

var errPipeClosed = errors.New("link: pipe closed")

// pipeBuffer is how many frames one direction of a pipe buffers before Send
// blocks.
const pipeBuffer = 64

type pipeEnd struct {
	in  chan *linkpb.Frame
	out chan *linkpb.Frame

	mu         sync.Mutex
	sentFrames []*linkpb.Frame

	blockSend chan struct{}

	closed    chan struct{}
	closeOnce *sync.Once

	recvWait chan struct{}
	recvOnce *sync.Once
}

func newPipe(t testing.TB) (a, b *pipeEnd) {
	t.Helper()
	ab := make(chan *linkpb.Frame, pipeBuffer)
	ba := make(chan *linkpb.Frame, pipeBuffer)
	closed := make(chan struct{})
	closeOnce := new(sync.Once)
	a = &pipeEnd{
		in: ba, out: ab,
		closed: closed, closeOnce: closeOnce,
		recvWait: make(chan struct{}), recvOnce: new(sync.Once),
	}
	b = &pipeEnd{
		in: ab, out: ba,
		closed: closed, closeOnce: closeOnce,
		recvWait: make(chan struct{}), recvOnce: new(sync.Once),
	}
	t.Cleanup(a.closeBoth)
	return a, b
}

// Send records f and hands it to the peer, blocking while blockSend is set and
// not closed and while the peer's buffer is full.
func (p *pipeEnd) Send(f *linkpb.Frame) error {
	if p.blockSend != nil {
		select {
		case <-p.blockSend:
		case <-p.closed:
			return errPipeClosed
		}
	}
	select {
	case <-p.closed:
		return errPipeClosed
	default:
	}
	p.mu.Lock()
	p.sentFrames = append(p.sentFrames, cloneFrame(f))
	p.mu.Unlock()
	select {
	case p.out <- f:
		return nil
	case <-p.closed:
		return errPipeClosed
	}
}

// Recv returns the next frame from the peer and closes recvWaiting once a
// goroutine has to wait for one.
func (p *pipeEnd) Recv() (*linkpb.Frame, error) {
	select {
	case f := <-p.in:
		return f, nil
	default:
	}
	p.recvOnce.Do(func() { close(p.recvWait) })
	select {
	case f := <-p.in:
		return f, nil
	case <-p.closed:
		return nil, errPipeClosed
	}
}

// closeBoth closes both ends; it is safe to call more than once.
func (p *pipeEnd) closeBoth() {
	p.closeOnce.Do(func() { close(p.closed) })
}

func (p *pipeEnd) sent() []*linkpb.Frame {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*linkpb.Frame, len(p.sentFrames))
	copy(out, p.sentFrames)
	return out
}

func (p *pipeEnd) recvWaiting() <-chan struct{} {
	return p.recvWait
}

// cloneFrame copies a frame so a test can inspect it after it was handed to the
// stream.
func cloneFrame(f *linkpb.Frame) *linkpb.Frame {
	if f == nil {
		return nil
	}
	return &linkpb.Frame{
		Kind:      f.GetKind(),
		RequestId: f.GetRequestId(),
		IsReply:   f.GetIsReply(),
		Payload:   append([]byte(nil), f.GetPayload()...),
	}
}
