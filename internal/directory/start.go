package directory

// Start registers the reconciliation frame handlers and link notifications,
// purges records that expired while the Broker was stopped, applies the
// configured public flag, and starts the maintenance loop. It returns ErrClosed
// after Close; calling it again on a started directory changes nothing.
func (d *Directory) Start() error {
	d.mu.Lock()
	if d.closed {
		d.mu.Unlock()
		return ErrClosed
	}
	if d.started {
		d.mu.Unlock()
		return nil
	}
	d.started = true
	// The own-record correction hook is installed only here, once the
	// directory is live.
	d.ownHook = d.reconcileOwn

	now := d.now()
	if n := d.st.purgeExpired(now); n > 0 {
		d.dirty = true
	}
	d.mu.Unlock()

	d.registerReconcile()
	d.trackLinks()
	d.startupCheck(now)
	go d.runLoop()
	return nil
}
