package pairing

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// lockedAliasConfig models the mutex config holds around one key. set takes
// that lock and runs the registered validator while holding it, exactly as
// config.Set and config's file reload do for broker.alias. It then commits the
// value and calls the subscriber after releasing the lock, mirroring the
// notification goroutine.
type lockedAliasConfig struct {
	mu        sync.Mutex
	alias     string
	validator func(newAlias string) error
	onCommit  func(newAlias string)
}

func (c *lockedAliasConfig) set(alias string) error {
	c.mu.Lock()
	if c.validator != nil {
		if err := c.validator(alias); err != nil {
			c.mu.Unlock()
			return err
		}
	}
	c.alias = alias
	c.mu.Unlock()
	if c.onCommit != nil {
		c.onCommit(alias)
	}
	return nil
}

// get reads the value through the same lock, standing in for config.Get.
func (c *lockedAliasConfig) get() (string, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.alias, nil
}

const (
	// lockOrderRounds is how many rounds each side runs. It is large enough
	// that the two sides overlap on every run, which is what exposes an
	// inverted lock order.
	lockOrderRounds = 100
	// roundSamples is how many uncontended rounds are timed per side to find
	// the normal cost of one round.
	roundSamples = 20
	// stallFactor multiplies the slower measured round to give the
	// no-progress window. A goroutine waiting on the other side's lock is
	// released after at most one round, so progress keeps resetting the
	// window while an inverted order stops both counters for good. 100 times
	// the measured round leaves room for the scheduler and for the other
	// side's critical section without approaching the total run time.
	stallFactor = 100
	// minStallWindow floors the derived window. When a round takes
	// microseconds the measured multiple is below the resolution of the
	// scheduler and the timer, so the window must not shrink further.
	minStallWindow = time.Second
	// maxStallWindow caps the derived window. 30 seconds is far above the
	// normal cost of one round and the scheduler jitter around it, while a
	// true deadlock still fails within an acceptable time. The cap matters
	// when the measurement phase is itself abnormally slow: without it a
	// single inflated round would stretch the no-progress window to an
	// undiagnosable length.
	maxStallWindow = 30 * time.Second
)

// TestAliasValidationKeepsTheConfigPairingLockOrder runs the broker.alias
// validator, which holds config's lock, next to handshake commits, which hold
// the peer table lock. The validator calls AliasConflict and ResolveAlias.
//
// The rule is config lock -> pairing lock, never the reverse: pairing must not
// call config while its table lock is held. On the pre-fix code the validator's
// ResolveAlias re-entered config.Get while config's lock was held, and a
// concurrent commit read the own alias through config.Get under the table lock,
// so this test deadlocked.
//
// The test fails on a stalled counter, not on total elapsed time. Each side
// records how many rounds it completed, and the test fails only when neither
// counter has moved for a window derived from the measured cost of one round.
// A slow machine that keeps completing rounds therefore cannot fail it, while
// a true lock inversion stops both counters and is reported with every
// goroutine's stack.
func TestAliasValidationKeepsTheConfigPairingLockOrder(t *testing.T) {
	cfg := &lockedAliasConfig{alias: "self"}
	s := newHSSettings(t)
	s.alias = "self"
	s.aliasFunc = cfg.get // reading the own alias takes config's lock
	p := hsNewPairing(t, s, &hsTransport{}, newHSClock())
	p.SetOwnAlias("self")

	cfg.validator = func(newAlias string) error {
		if p.AliasConflict(newAlias) {
			return fmt.Errorf("alias %q conflicts with a peer", newAlias)
		}
		if _, ok := p.ResolveAlias(newAlias); ok {
			return fmt.Errorf("alias %q resolves to a peer", newAlias)
		}
		return nil
	}

	type peerKey struct {
		fp    Fingerprint
		pub   []byte
		alias string
	}

	// Time one uncontended round on each side. The no-progress window is
	// derived from these measurements, never from the total run time, so load
	// that merely makes each round slower cannot fail the test.
	measureKeys := make([]peerKey, roundSamples)
	for i := range measureKeys {
		fp, pub := peerTestKey(t)
		measureKeys[i] = peerKey{fp: fp, pub: pub, alias: fmt.Sprintf("measure-%d", i)}
	}
	var measureErr error
	configRound := meanRoundDuration(roundSamples, func() {
		if err := cfg.set("measure-own"); err != nil && measureErr == nil {
			measureErr = err
		}
	})
	if measureErr != nil {
		t.Fatalf("measuring the validator round: %v", measureErr)
	}
	next := 0
	pairingRound := meanRoundDuration(roundSamples, func() {
		k := measureKeys[next]
		next++
		token, ok := p.peers.reserveAlias(k.alias, k.fp)
		if !ok {
			if measureErr == nil {
				measureErr = fmt.Errorf("reserve %q failed", k.alias)
			}
			return
		}
		entry := peerTestEntry(k.fp, k.pub, k.alias, RoleAcceptor)
		if _, err := commitHandshakeForTest(p, entry, token); err != nil && measureErr == nil {
			measureErr = fmt.Errorf("commit %q: %w", k.alias, err)
		}
	})
	if measureErr != nil {
		t.Fatalf("measuring the commit round: %v", measureErr)
	}

	slowest := configRound
	if pairingRound > slowest {
		slowest = pairingRound
	}
	stallWindow := stallFactor * slowest
	if stallWindow < minStallWindow {
		stallWindow = minStallWindow
	}
	if stallWindow > maxStallWindow {
		stallWindow = maxStallWindow
	}
	t.Logf("one uncontended round: config=%s pairing=%s; failing only after %s without progress (%d× the slower round, clamped to [%s, %s])",
		configRound, pairingRound, stallWindow, stallFactor, minStallWindow, maxStallWindow)

	keys := make([]peerKey, lockOrderRounds)
	for i := range keys {
		fp, pub := peerTestKey(t)
		keys[i] = peerKey{fp: fp, pub: pub, alias: fmt.Sprintf("peer-%d", i)}
	}

	var configRounds, pairingRounds atomic.Int64
	start := make(chan struct{})
	errs := make(chan error, 2)

	// Config side: hold config's lock, run the validator, commit the value.
	go func() {
		<-start
		for i := 0; i < lockOrderRounds; i++ {
			if err := cfg.set(fmt.Sprintf("own-%d", i)); err != nil {
				errs <- fmt.Errorf("validator round %d: %w", i, err)
				return
			}
			configRounds.Add(1)
		}
		errs <- nil
	}()

	// Pairing side: complete handshakes, each reserving and committing an
	// alias under the peer table lock.
	go func() {
		<-start
		for i := 0; i < lockOrderRounds; i++ {
			token, ok := p.peers.reserveAlias(keys[i].alias, keys[i].fp)
			if !ok {
				errs <- fmt.Errorf("reserve %q failed", keys[i].alias)
				return
			}
			entry := peerTestEntry(keys[i].fp, keys[i].pub, keys[i].alias, RoleAcceptor)
			if _, err := commitHandshakeForTest(p, entry, token); err != nil {
				errs <- fmt.Errorf("commit %q: %w", keys[i].alias, err)
				return
			}
			pairingRounds.Add(1)
		}
		errs <- nil
	}()

	checkEvery := stallWindow / 10
	if checkEvery < 10*time.Millisecond {
		checkEvery = 10 * time.Millisecond
	}
	ticker := time.NewTicker(checkEvery)
	defer ticker.Stop()

	close(start)
	lastConfig, lastPairing := configRounds.Load(), pairingRounds.Load()
	lastProgress := time.Now()
	finished := 0
	for finished < 2 {
		select {
		case err := <-errs:
			if err != nil {
				t.Fatalf("validator or commit failed: %v", err)
			}
			finished++
		case <-ticker.C:
			doneConfig, donePairing := configRounds.Load(), pairingRounds.Load()
			if doneConfig != lastConfig || donePairing != lastPairing {
				lastConfig, lastPairing = doneConfig, donePairing
				lastProgress = time.Now()
				continue
			}
			if since := time.Since(lastProgress); since < stallWindow {
				continue
			}
			t.Fatalf("no round completed for %s (config completed %d/%d rounds, pairing completed %d/%d rounds; one uncontended round measured config=%s pairing=%s, so the window is %d× the slower round clamped to [%s, %s]): config and pairing locks are inverted\nall goroutine stacks:\n%s",
				time.Since(lastProgress).Round(time.Millisecond), doneConfig, lockOrderRounds, donePairing, lockOrderRounds,
				configRound, pairingRound, stallFactor, minStallWindow, maxStallWindow, allGoroutineStacks())
		}
	}
}

// meanRoundDuration returns the mean duration of n calls to op.
func meanRoundDuration(n int, op func()) time.Duration {
	start := time.Now()
	for i := 0; i < n; i++ {
		op()
	}
	return time.Since(start) / time.Duration(n)
}

// allGoroutineStacks returns every goroutine's stack, growing the buffer until
// runtime.Stack reports that the whole trace fit.
func allGoroutineStacks() []byte {
	buf := make([]byte, 64<<10)
	for {
		n := runtime.Stack(buf, true)
		if n < len(buf) {
			return buf[:n]
		}
		buf = make([]byte, 2*len(buf))
	}
}
