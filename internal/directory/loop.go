package directory

import "time"

// runLoop is the maintenance loop. It wakes at the next persist deadline, at
// the next reconciliation, or after maintainInterval, whichever comes first,
// and runs one maintenance pass. Close stops it through d.stop; the goroutine
// closes loopDone on the way out so that Close can wait for the loop.
func (d *Directory) runLoop() {
	defer close(d.loopDone)

	lastReconcile := d.now()
	for {
		now := d.now()
		wait := d.nextWake(now, lastReconcile)

		t := d.clk.NewTimer(wait)
		select {
		case <-t.C():
		case <-d.stop:
			t.Stop()
			return
		}

		lastReconcile = d.maintain(d.now(), lastReconcile)
	}
}

// nextWake returns how long the maintenance loop may sleep from now. The persist
// deadline is scheduled from the last maintenance write, not from the last wake,
// so a reconcile interval below maintainInterval cannot push a write two
// intervals out: without this, a 50-second interval could hold accepted records
// for 100 seconds while the documentation promises at most a minute.
func (d *Directory) nextWake(now, lastReconcile time.Time) time.Duration {
	wait := maintainInterval
	if next := lastReconcile.Add(d.settings.ReconcileInterval()).Sub(now); next < wait {
		wait = next
	}

	d.mu.RLock()
	lastPersist := d.lastMaintainPersist
	d.mu.RUnlock()
	if !lastPersist.IsZero() {
		if next := lastPersist.Add(maintainInterval).Sub(now); next < wait {
			wait = next
		}
	}

	if wait < 0 {
		wait = 0
	}
	return wait
}

// maintain runs one maintenance pass: it purges expired records, renews the
// local card and the active public topic declarations that are due, reconciles
// with every established peer once the interval has elapsed, and writes a
// pending change to disk. Records accepted from a link are written at most once
// per maintainInterval; a locally issued record and a revocation write at once
// through their own paths. The instant of the last reconciliation is returned so
// the loop can schedule the next wake.
func (d *Directory) maintain(now, lastReconcile time.Time) time.Time {
	d.mu.Lock()
	if n := d.st.purgeExpired(now); n > 0 {
		d.dirty = true
	}
	d.mu.Unlock()

	d.renewDue(now)

	if now.Sub(lastReconcile) >= d.settings.ReconcileInterval() {
		d.reconcileAll()
		lastReconcile = now
	}

	if d.persistDue(now) {
		d.persistIfDirty()
		d.noteMaintainPersist(now)
	}
	if d.afterMaintain != nil {
		d.afterMaintain()
	}
	return lastReconcile
}
