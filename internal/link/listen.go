package link

import "net"

func (l *Link) acceptStream(c *conn, t *trackedConn) {
	if d, ok := l.serveInbound(c, t); ok {
		l.runConn(c, d)
	}
}

// Listen binds address and starts accepting inbound links. An empty host part
// binds every local address; an empty address stops listening and leaves
// established links alone. A bind failure is returned unchanged and the
// previous listener keeps serving.
func (l *Link) Listen(address string) error {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return ErrClosed
	}
	if address == "" {
		old := l.lis
		l.lis = nil
		l.mu.Unlock()
		if old != nil {
			_ = old.Close()
		}
		return nil
	}
	l.mu.Unlock()

	ln, err := net.Listen("tcp", address)
	if err != nil {
		return err
	}

	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		_ = ln.Close()
		return ErrClosed
	}
	server := l.server
	if server == nil {
		server = l.newGRPCServer(l.acceptStream)
		l.server = server
	}
	tl := &trackingListener{Listener: ln, reg: l.unauth}
	old := l.lis
	l.lis = tl
	l.mu.Unlock()

	go func() {
		_ = server.Serve(tl)
	}()
	if old != nil {
		// Connections already accepted on the old listener stay open.
		_ = old.Close()
	}
	return nil
}

// Addr returns the address currently listened on, or nil when this Link does
// not listen.
func (l *Link) Addr() net.Addr {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lis == nil {
		return nil
	}
	return l.lis.Addr()
}

// Close stops listening, tears down every link and stops the notifier. It
// returns only after no callback or frame handler can run any more. It must not
// be called from a callback or a frame handler.
func (l *Link) Close() {
	l.mu.Lock()
	if l.closed {
		l.mu.Unlock()
		return
	}
	l.closed = true
	lis := l.lis
	l.lis = nil
	server := l.server
	var conns []*conn
	for _, rec := range l.records {
		if rec.conn != nil {
			conns = append(conns, rec.conn)
		}
	}
	l.mu.Unlock()

	l.cancelRoot()
	if lis != nil {
		_ = lis.Close()
	}
	if server != nil {
		server.Stop()
	}
	for _, c := range conns {
		c.closeAsync(FailConnectionLost)
	}
	l.wg.Wait()
	l.notifier.stop()
}
