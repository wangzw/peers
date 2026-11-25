package linktest

import (
	"net"
	"sync"
	"testing"
)

const copyBufSize = 32 * 1024

// Proxy forwards TCP connections to a target address and lets a test inspect
// and interfere with the bytes. It listens on 127.0.0.1 with an ephemeral port
// and dials the target once per accepted connection.
type Proxy struct {
	target string
	ln     net.Listener

	mu       sync.Mutex
	closed   bool
	conns    map[*proxyConn]struct{}
	accepted int
	captured []byte
	buffered int
	paused   bool
	pauseCh  chan struct{}
	changed  chan struct{}

	wg sync.WaitGroup
}

type proxyConn struct {
	p      *Proxy
	client net.Conn
	target net.Conn
}

// NewProxy starts a proxy that forwards to target. It registers Close with the
// test's cleanup function.
func NewProxy(t testing.TB, target string) *Proxy {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("linktest: proxy listen: %v", err)
	}
	p := &Proxy{
		target:  target,
		ln:      ln,
		conns:   make(map[*proxyConn]struct{}),
		changed: make(chan struct{}),
	}
	p.wg.Add(1)
	go p.acceptLoop()
	t.Cleanup(p.Close)
	return p
}

// Addr returns the proxy's listening address in host:port form.
func (p *Proxy) Addr() string { return p.ln.Addr().String() }

// Pause stops both directions from forwarding. Bytes read while paused stay in
// the proxy, and the connections stay open. Pause is idempotent.
func (p *Proxy) Pause() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.paused || p.closed {
		return
	}
	p.paused = true
	p.pauseCh = make(chan struct{})
}

// Resume resumes forwarding after Pause.
func (p *Proxy) Resume() {
	p.mu.Lock()
	defer p.mu.Unlock()
	if !p.paused {
		return
	}
	p.paused = false
	close(p.pauseCh)
	p.pauseCh = nil
}

// CloseConns closes every current connection. The listener keeps accepting.
func (p *Proxy) CloseConns() {
	for _, pc := range p.snapshot() {
		pc.close()
	}
}

// Close closes the listener and every connection, then waits for the
// forwarding goroutines to exit. It is idempotent.
func (p *Proxy) Close() {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return
	}
	p.closed = true
	if p.paused {
		p.paused = false
		close(p.pauseCh)
		p.pauseCh = nil
	}
	p.notifyLocked()
	ln := p.ln
	p.mu.Unlock()

	_ = ln.Close()
	p.CloseConns()
	p.wg.Wait()
}

// Captured returns a copy of every byte the proxy has forwarded in either
// direction.
func (p *Proxy) Captured() []byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([]byte(nil), p.captured...)
}

// Accepted returns how many connections the proxy has accepted.
func (p *Proxy) Accepted() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.accepted
}

// WaitAccepted blocks until at least n connections have been accepted, failing
// the test once eventTimeout passes.
func (p *Proxy) WaitAccepted(t testing.TB, n int) {
	t.Helper()
	waitUntil(t, "accepted proxy connections", p.changedCh, func() bool {
		return p.Accepted() >= n
	})
}

func (p *Proxy) bufferedBytes() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.buffered
}

func (p *Proxy) waitBuffered(t testing.TB, cond func(int) bool) {
	t.Helper()
	waitUntil(t, "buffered proxy bytes", p.changedCh, func() bool {
		return cond(p.bufferedBytes())
	})
}

func (p *Proxy) changedCh() <-chan struct{} {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.changed
}

// notifyLocked wakes every waiter. The caller must hold p.mu.
func (p *Proxy) notifyLocked() {
	close(p.changed)
	p.changed = make(chan struct{})
}

func (p *Proxy) snapshot() []*proxyConn {
	p.mu.Lock()
	defer p.mu.Unlock()
	out := make([]*proxyConn, 0, len(p.conns))
	for pc := range p.conns {
		out = append(out, pc)
	}
	return out
}

func (p *Proxy) acceptLoop() {
	defer p.wg.Done()
	for {
		conn, err := p.ln.Accept()
		if err != nil {
			return
		}
		p.wg.Add(1)
		go p.serve(conn)
	}
}

func (p *Proxy) serve(client net.Conn) {
	defer p.wg.Done()
	target, err := net.Dial("tcp", p.target)
	if err != nil {
		_ = client.Close()
		return
	}
	pc := &proxyConn{p: p, client: client, target: target}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		pc.close()
		return
	}
	p.conns[pc] = struct{}{}
	p.accepted++
	p.notifyLocked()
	p.mu.Unlock()

	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		p.pump(client, target)
	}()
	go func() {
		defer wg.Done()
		p.pump(target, client)
	}()
	wg.Wait()

	p.mu.Lock()
	delete(p.conns, pc)
	p.mu.Unlock()
	pc.close()
}

// pump forwards src to dst, counting a chunk as buffered while it waits for a
// pause and as captured once written.
func (p *Proxy) pump(src, dst net.Conn) {
	buf := make([]byte, copyBufSize)
	for {
		n, err := src.Read(buf)
		if n > 0 {
			p.mu.Lock()
			p.buffered += n
			p.notifyLocked()
			p.mu.Unlock()

			p.waitResume()

			written := 0
			var writeErr error
			for written < n {
				var m int
				m, writeErr = dst.Write(buf[written:n])
				written += m
				if writeErr != nil {
					break
				}
			}

			p.mu.Lock()
			if written > 0 {
				p.captured = append(p.captured, buf[:written]...)
			}
			p.buffered -= n
			p.notifyLocked()
			p.mu.Unlock()

			if writeErr != nil {
				return
			}
		}
		if err != nil {
			return
		}
	}
}

func (p *Proxy) waitResume() {
	for {
		p.mu.Lock()
		ch := p.pauseCh
		p.mu.Unlock()
		if ch == nil {
			return
		}
		<-ch
	}
}

func (pc *proxyConn) close() {
	_ = pc.client.Close()
	_ = pc.target.Close()
}
