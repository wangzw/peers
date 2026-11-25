package directory

import "time"

// runLoop closes loopDone on return so Close can wait for it.
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

// nextWake schedules the persist deadline from the last maintenance write, so a
// reconcile interval below maintainInterval cannot push a write two intervals
// out.
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
